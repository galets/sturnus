//! The session table: one candidate per session, in this instance's memory.
//!
//! A **pin** is a session's claim on a candidate, so the conversation's prompt
//! cache stays warm on that box (see `GLOSSARY.md`). The table is the whole
//! mechanism: `(alias, session key) → candidate`, touched on every turn, and
//! nothing else. There is no record of the boxes a session has left, because
//! overflow **re-pins**: when the pinned candidate is demoted the turn goes to
//! the next placement and that becomes the session's candidate, and nothing
//! moves it back when the window expires — nobody can say whether the cache it
//! left is still there, so a migration is accepted as the new state of affairs.
//!
//! The table is per instance and in-memory, which is what keeps this proxy free
//! of shared state: no Redis, no database, no control plane. Two replicas
//! behind a load balancer therefore place their own sessions, and one
//! session's turns land on whichever replica received them. The stateless
//! `provider/model` value echoed on every response is how a cooperating client
//! pins across replicas without this table at all.
//!
//! The clock is not owned here. Every method takes the reading it judged
//! recency against, so the table shares the one clock the demotion windows are
//! kept on ([`crate::clock::Clock`], via [`crate::tracker::Tracker::clock`])
//! and a test crosses the idle TTL by advancing that clock rather than
//! sleeping.

use std::collections::{HashMap, VecDeque};

use hyper::header::HeaderValue;

/// Default idle TTL for a pin: 30 minutes.
///
/// This is a bound on how long a session is plausibly still going, *not* a
/// claim that the upstream's prompt cache survived that long. What the cache
/// does was measured on the boxes this proxy runs in front of: a warm
/// 8348-token prompt re-prefills in 50ms against 9.05s cold on a DDR-based
/// box, and 58ms against 6.60s on a GPU one; the cache is still warm after
/// 120s of idle; and it is destroyed by the *next conversation on that box* —
/// a 53-token turn from another session costs the resident conversation a
/// full 9.03s re-prefill. So on a single-slot llama.cpp box the cache is not
/// on a timer at all, and on the hosted providers it is a 5–10 minute
/// inactivity window (OpenAI) or a 5-minute one (Anthropic). In no case does
/// a 30-minute TTL match a live cache, and the entry is kept that long for
/// two reasons that are not about the cache: a conversation that resumes
/// after a half-hour gap is the same conversation, and its next turn still
/// costs less on the box that already holds its system prompt and tool
/// schemas than on a cold one — provided nothing else ran there, which is
/// what the idle-preference rule in [`crate::router`] exists to keep true;
/// and a session that hops boxes every time its owner gets lunch never has a
/// box. Past 30 minutes a session is more likely dead than resting, and a
/// dead session holding one candidate at 100% of its traffic is the failure
/// mode the README names as the cost of affinity — so it places fresh.
pub const DEFAULT_IDLE_TTL_SECS: u64 = 1800;

/// Default LRU cap: 1024 sessions per instance.
///
/// The cap exists because the keys are untrusted caller input: any client can
/// invent session keys, so the table's size cannot be left to the traffic.
/// 1024 is roomy against what one sidecar actually has pinned — a pinned
/// entry is a live conversation, and a sidecar in front of a few boxes serves
/// tens of concurrent ones, including the chatty case of an agent that opens a
/// session per subagent. The cost of the bound is the worst case: a key is
/// capped at [`MAX_SESSION_KEY_BYTES`], so an entry is at most ~300 bytes of
/// key plus 16 bytes of pin plus map overhead, which is ~400 KB for the whole
/// table at the cap. Against a default request-buffer budget of 128 MB the
/// table can never be the thing that OOMs the pod; real keys (an OpenCode
/// `ses_…` id is 30 bytes) put the same table near 80 KB.
///
/// It is a memory bound, not a capacity, and the difference matters: measured
/// on a llama.cpp box, exactly one conversation is cached at a time — two
/// interleaved conversations fully evict each other — so the sessions that
/// are *warm* number about one per box, and the rest of the table is a record
/// of where conversations have been. Reading the cap as "this many
/// conversations are warm here" overstates what affinity buys.
pub const DEFAULT_TABLE_LIMIT: usize = 1024;

/// The longest session key the table accepts.
///
/// A session key is untrusted caller input and it becomes a map key, so its
/// length is bounded: 256 bytes is ~8x an OpenCode `ses_…` id (30 bytes) and
/// room for any provider's opaque id, while a caller that sprays long random
/// strings is refused a pin (the turn is placed) rather than paying for them
/// in the table.
pub const MAX_SESSION_KEY_BYTES: usize = 256;

/// What the affinity header asked for, discriminated by shape.
///
/// The value arrives under `x-session-affinity`, or under `x-session-id` when
/// that one is absent (see [`crate::server`]): the name changes nothing about
/// what a value means, only where it was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity<'a> {
    /// A `provider/model` pair: the caller naming one specific box. Checked
    /// before the session table and never entered into it, because it is not a
    /// session's claim — it is a request's instruction.
    ExplicitPin { provider: &'a str, model: &'a str },
    /// An opaque session key — an OpenCode `ses_…` id, for example. The first
    /// turn of a key is placed and pinned; every later turn rides the pin.
    SessionKey(&'a str),
}

/// Read the header's shape.
///
/// `None` means the header is absent, not printable ASCII, blank, or over the
/// [`MAX_SESSION_KEY_BYTES`] bound: every one of those falls back to placement,
/// as an unusable header always has. A value containing `/` is the explicit
/// pin, exactly as it was before session keys existed — including the case
/// where it names no candidate of the alias, which is not a pin either.
///
/// The name the value arrived under is not this function's business: it reads
/// the value it is handed, whether that came from `x-session-affinity` or from
/// `x-session-id` in its place.
pub fn classify(value: Option<&HeaderValue>) -> Option<Affinity<'_>> {
    let value = value?.to_str().ok()?;
    if let Some((provider, model)) = value.split_once('/') {
        return Some(Affinity::ExplicitPin { provider, model });
    }
    let key = value.trim();
    if key.is_empty() || key.len() > MAX_SESSION_KEY_BYTES {
        return None;
    }
    Some(Affinity::SessionKey(key))
}

/// One session's pin: the candidate it is bound to, and the clock reading of
/// the last turn that touched it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pin {
    stats_index: usize,
    last_used_ms: u64,
}

/// `(alias, session key)` as one map key.
///
/// Pins are keyed per alias, so the same session using a second alias places
/// fresh there and the two pins cannot interfere: a share is a property of an
/// alias's candidate list, and so is the box a session holds inside it.
///
/// The NUL separator is unambiguous — a header value is visible ASCII plus tab,
/// so it can never contain one — and one key per lookup keeps the request path
/// free of a nested map.
fn session_id(alias: &str, key: &str) -> String {
    let mut id = String::with_capacity(alias.len() + key.len() + 1);
    id.push_str(alias);
    id.push('\0');
    id.push_str(key);
    id
}

/// The pins this instance knows about, bounded by an idle TTL and an LRU cap.
///
/// Both bounds key off the same fact — the last turn that touched a session —
/// so the LRU order *is* the expiry order, and the front of the queue is the
/// next entry the TTL would drop. That is what makes the bound cheap: eviction
/// pops the front, and the sweep that keeps the reported size honest walks the
/// front and stops at the first entry that is still live.
///
/// The LRU is a `VecDeque` of keys beside the map, not an order-statistic tree:
/// at the cap a touch scans ≤ 1024 entries of a small string, which is noise
/// next to the upstream round trip it is pacing.
#[derive(Debug)]
pub struct SessionTable {
    pins: HashMap<String, Pin>,
    /// LRU order: front is the least recently used, back the most.
    order: VecDeque<String>,
    limit: usize,
    ttl_ms: u64,
}

impl Default for SessionTable {
    fn default() -> Self {
        Self::new(DEFAULT_TABLE_LIMIT, DEFAULT_IDLE_TTL_SECS)
    }
}

impl SessionTable {
    /// An empty table bounded by `limit` sessions and `idle_ttl_secs` of quiet.
    ///
    /// A `limit` of 0 pins nothing: every entry is evicted as it is inserted,
    /// so every turn places fresh. A `idle_ttl_secs` of 0 gives a pin no quiet
    /// to survive: the turn after the one that made it places fresh.
    pub fn new(limit: usize, idle_ttl_secs: u64) -> Self {
        Self {
            pins: HashMap::new(),
            order: VecDeque::new(),
            limit,
            ttl_ms: idle_ttl_secs.saturating_mul(1_000),
        }
    }

    /// The candidate this session is pinned to, touching it so it stays alive.
    ///
    /// `None` means no pin: a key never seen, one whose TTL has passed (the
    /// entry is dropped here, so the reported size follows the TTL), or one
    /// evicted by the cap. The caller answers a `None` with a placement, and
    /// records what that placement chose.
    pub fn lookup(&mut self, alias: &str, key: &str, now_ms: u64) -> Option<usize> {
        let id = session_id(alias, key);
        let pin = self.pins.get(&id).copied()?;
        if self.is_expired(pin, now_ms) {
            self.remove(&id);
            return None;
        }
        if let Some(pin) = self.pins.get_mut(&id) {
            pin.last_used_ms = now_ms;
        }
        self.move_to_back(&id);
        Some(pin.stats_index)
    }

    /// Bind this session to `stats_index`, replacing any pin it already had.
    ///
    /// One call covers both ways a pin comes into being: the first turn of a
    /// session (a fresh pick) and an overflow (a re-pin away from a demoted
    /// box). The table holds one candidate per session, so a re-pin leaves
    /// nothing behind to move the session back.
    pub fn pin(&mut self, alias: &str, key: &str, stats_index: usize, now_ms: u64) {
        let id = session_id(alias, key);
        self.sweep_expired(now_ms);
        let pin = Pin {
            stats_index,
            last_used_ms: now_ms,
        };
        if self.pins.insert(id.clone(), pin).is_some() {
            self.move_to_back(&id);
        } else {
            self.order.push_back(id);
        }
        while self.pins.len() > self.limit {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.pins.remove(&oldest);
        }
    }

    /// How many sessions are pinned to the candidate owning `stats_index`.
    ///
    /// `/status` renders this, so the number an operator reads is the one the
    /// request path consulted — the endpoint cannot drift from the router.
    pub fn pinned_count(&self, stats_index: usize) -> usize {
        self.pins
            .values()
            .filter(|pin| pin.stats_index == stats_index)
            .count()
    }

    /// Sessions in the table right now.
    pub fn len(&self) -> usize {
        self.pins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }

    /// The LRU cap this table was built with.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The idle TTL this table was built with, in whole seconds.
    pub fn idle_ttl_secs(&self) -> u64 {
        self.ttl_ms / 1_000
    }

    fn is_expired(&self, pin: Pin, now_ms: u64) -> bool {
        now_ms.saturating_sub(pin.last_used_ms) > self.ttl_ms
    }

    /// Drop the entries the TTL has passed, from the front in.
    ///
    /// Clippy suggests `while let Some(front) = self.order.front()`, and that
    /// form cannot compile: it holds the borrow of `order` across the
    /// `pop_front` below. The `loop` holds the same borrow only until the
    /// expiry decision, and clones a key only for an entry it is dropping.
    #[allow(clippy::while_let_loop)]
    fn sweep_expired(&mut self, now_ms: u64) {
        loop {
            let Some(front) = self.order.front() else {
                break;
            };
            let expired = match self.pins.get(front) {
                None => true,
                Some(pin) => self.is_expired(*pin, now_ms),
            };
            if !expired {
                break;
            }
            let id = front.clone();
            self.order.pop_front();
            self.pins.remove(&id);
        }
    }

    fn move_to_back(&mut self, id: &str) {
        if let Some(pos) = self.order.iter().position(|key| key == id) {
            if let Some(key) = self.order.remove(pos) {
                self.order.push_back(key);
            }
        }
    }

    fn remove(&mut self, id: &str) {
        self.pins.remove(id);
        if let Some(pos) = self.order.iter().position(|key| key == id) {
            self.order.remove(pos);
        }
    }
}

#[cfg(test)]
mod tests;
