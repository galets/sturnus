use crate::model_map::ResolvedCandidate;
use crate::tracker::Tracker;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

/// One counter per alias, walked over the weighted draw (no RNG).
#[derive(Debug)]
pub struct RoundRobinState {
    counters: HashMap<String, std::sync::atomic::AtomicUsize>,
}

impl Default for RoundRobinState {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinState {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    pub fn register_alias(&mut self, alias: String) {
        self.counters
            .entry(alias)
            .or_insert_with(|| std::sync::atomic::AtomicUsize::new(0));
    }

    fn next_tick(&self, alias: &str) -> u64 {
        self.counters
            .get(alias)
            .map_or(0, |c| c.fetch_add(1, Ordering::Relaxed) as u64)
    }
}

/// 2^64 / φ, the Weyl-sequence step. Odd, so `wrapping_mul` is a bijection.
///
/// The sequence is the deterministic stand-in for the uniform sample in a
/// weighted choice (cf. `rand::distributions::WeightedIndex`): consecutive
/// ticks land spread over the weight range rather than clumping, so equal
/// candidates alternate exactly and a 2:1 pair splits 2:1 over a long run of
/// picks. Deterministic means the split is reproducible in a test and in a
/// debug log, with no RNG to seed.
const WEYL_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// Why a turn landed on the candidate it did.
///
/// Every placement reports one of these, so "why this box, this turn?" is a
/// log line rather than an inference: the reason is the shape of the decision
/// the placement path actually made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// The caller demanded a specific `provider/model` and got it.
    ExplicitPin,
    /// The turn belongs to a session already pinned to this candidate.
    Pinned,
    /// A new session, placed on an idle candidate in proportion to weight.
    FreshByWeight,
    /// Placed away from the weight-preferred candidate because it was busy.
    OverflowBusy,
    /// Placed on a demoted candidate: either the session's pin was demoted, or
    /// every candidate of the alias is demoted and the highest-weight one
    /// serves so the caller sees the box's own error, not one we invented.
    OverflowDemoted,
}

impl Placement {
    pub fn as_str(self) -> &'static str {
        match self {
            Placement::ExplicitPin => "explicit-pin",
            Placement::Pinned => "pinned",
            Placement::FreshByWeight => "fresh-by-weight",
            Placement::OverflowBusy => "overflow-busy",
            Placement::OverflowDemoted => "overflow-demoted",
        }
    }
}

/// A placement decision: the candidate that serves the turn, and why.
#[derive(Debug)]
pub struct Placed<'a> {
    pub candidate: &'a ResolvedCandidate,
    pub reason: Placement,
}

/// One candidate's declared weight and the share that weight buys it in the
/// group it was scheduled with.
#[derive(Debug, Clone)]
pub struct ScheduledCandidate<'a> {
    pub candidate: &'a ResolvedCandidate,
    /// The declared integer weight. Never zero: config rejects it.
    pub weight: usize,
    /// `weight / total_weight`, in `0.0..=1.0`.
    pub share: f64,
}

/// The declared-weight schedule for one group of competing candidates.
///
/// Computed independently of any pick so callers can inspect the weights and
/// shares without consuming a tick. The pick path draws from this same
/// schedule, keeping the reported numbers and the routing decision in sync.
#[derive(Debug)]
pub struct PlacementSchedule<'a> {
    pub entries: Vec<ScheduledCandidate<'a>>,
    /// Sum of every entry's weight; the modulus of the weighted draw.
    pub total_weight: usize,
}

/// Every candidate's share on one line, e.g.
/// `cuda30/fast=66.7% llama30/slow=33.3%` — the shape the routing debug line
/// wants. `Display` rather than a built `String` so
/// `debug!(shares = %schedule)` costs nothing unless the callsite is enabled.
impl std::fmt::Display for PlacementSchedule<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, entry) in self.entries.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(
                f,
                "{}/{}={:.1}%",
                entry.candidate.provider_name,
                entry.candidate.model,
                entry.share * 100.0
            )?;
        }
        Ok(())
    }
}

/// The declared-weight schedule for `candidates`.
///
/// This is the whole allocation policy: a candidate's share of its group is
/// its weight over the group's total weight. Nothing measured — latency,
/// error rate, queue depth — scales it. A 19:1 split is written as
/// `weight = 19` beside `weight = 1`, not discovered by probing.
///
/// Pure: advances no counter and reserves nothing, so `/status` renders the
/// same arithmetic with no effect on routing.
pub fn placement_schedule(candidates: &[ResolvedCandidate]) -> PlacementSchedule<'_> {
    let total_weight: usize = candidates.iter().map(|c| c.weight).sum();

    let entries = candidates
        .iter()
        .map(|candidate| ScheduledCandidate {
            weight: candidate.weight,
            candidate,
            share: normalised_share(candidate.weight, total_weight),
        })
        .collect();

    PlacementSchedule {
        entries,
        total_weight,
    }
}

/// `weight / total_weight`, guarding the divide.
///
/// A zero total is unreachable today — every weight is at least 1 — but these
/// numbers are rendered by `/status` and the routing debug line, so a `NaN`
/// reaching an operator would be worse than a `0.0`.
fn normalised_share(weight: usize, total_weight: usize) -> f64 {
    if total_weight == 0 {
        return 0.0;
    }
    // Both are small counts, so the f64 conversion is exact.
    #[allow(clippy::cast_precision_loss)]
    let (weight, total_weight) = (weight as f64, total_weight as f64);
    weight / total_weight
}

/// Draw one candidate from `pool` in proportion to declared weight.
///
/// Golden-ratio Weyl sequence (`n·2^64/φ`) over the pool's total weight:
/// deterministic, equidistributed, and it stays equidistributed when the pool
/// is a filtered subset (idle candidates only, say).
pub fn pick_weighted<'a>(
    pool: &[&'a ResolvedCandidate],
    alias: &str,
    rr: &RoundRobinState,
) -> Option<&'a ResolvedCandidate> {
    // No tick is consumed for an empty pool, matching the old early return.
    let total_weight: usize = pool.iter().map(|c| c.weight).sum();
    if pool.is_empty() || total_weight == 0 {
        return None;
    }

    let spread = rr.next_tick(alias).wrapping_mul(WEYL_STEP);
    #[allow(clippy::cast_possible_truncation)]
    let pos = ((u128::from(spread) * total_weight as u128) >> 64) as usize;
    let mut acc = 0;
    for candidate in pool {
        acc += candidate.weight;
        if pos < acc {
            return Some(candidate);
        }
    }
    // pos < total_weight guarantees a hit above; last is a safe fallback.
    pool.last().copied()
}

/// Place a turn among `candidates` by declared weight, preferring an idle box.
///
/// Weight decides between candidates that are equally able to start now;
/// busyness decides who gets asked at all. A candidate with a prompt running
/// is passed over for an idle one whatever its weight, because the cheap
/// outcome for a new session is a box that starts it immediately and the
/// expensive one is a queue behind somebody else's prompt. When every
/// candidate is busy the least busy wins, and weight breaks that tie.
///
/// A session that is already pinned does not come through here: its next turn
/// queues behind the running prompt on its own box (the pin path in
/// `server.rs`), because re-prefilling a long conversation on a slower box is
/// the expensive outcome and queueing behind your own prompt is the cheap one.
///
/// The demotion input to eligibility comes in the next ticket; the pool is
/// every candidate of the alias until then.
pub fn place<'a>(
    candidates: &'a [ResolvedCandidate],
    alias: &str,
    rr: &RoundRobinState,
    tracker: &Tracker,
) -> Option<Placed<'a>> {
    // `min` over an empty candidate list is `None`, so a misconfigured alias
    // yields no placement and consumes no tick.
    let least_busy = candidates
        .iter()
        .map(|c| tracker.in_flight(c.stats_index))
        .min()?;
    let pool: Vec<&ResolvedCandidate> = candidates
        .iter()
        .filter(|c| tracker.in_flight(c.stats_index) == least_busy)
        .collect();

    let candidate = pick_weighted(&pool, alias, rr)?;
    Some(Placed {
        candidate,
        reason: if least_busy == 0 {
            Placement::FreshByWeight
        } else {
            Placement::OverflowBusy
        },
    })
}

#[cfg(test)]
mod tests;
