use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// Streaming TTFC and non-streaming response time are tracked separately;
/// blending them in one EWMA would distort routing for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyMode {
    Streaming,
    NonStreaming,
}

impl LatencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LatencyMode::Streaming => "streaming",
            LatencyMode::NonStreaming => "nonstreaming",
        }
    }
}

#[derive(Debug)]
pub struct CandidateStats {
    /// EWMA of streaming TTFC in milliseconds. u64::MAX = cold (no data).
    streaming_ewma_ms: AtomicU64,
    /// EWMA of non-streaming response time in milliseconds. u64::MAX = cold.
    nonstreaming_ewma_ms: AtomicU64,
    /// EWMA of request outcomes (success = 1.0, error = 0.0) as f64 bits.
    /// Starts optimistic at 1.0; recovers via probe traffic, not a clock.
    success_rate: AtomicU64,
    /// Requests currently outstanding against this candidate, held by
    /// [`InFlightGuard`]. A count of a *candidate*, not of an alias: one box
    /// reachable through several aliases has one queue, not one per alias.
    in_flight: AtomicUsize,
}

impl Default for CandidateStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateStats {
    pub fn new() -> Self {
        Self {
            streaming_ewma_ms: AtomicU64::new(u64::MAX),
            nonstreaming_ewma_ms: AtomicU64::new(u64::MAX),
            success_rate: AtomicU64::new(1.0_f64.to_bits()),
            in_flight: AtomicUsize::new(0),
        }
    }

    fn ewma_field(&self, mode: LatencyMode) -> &AtomicU64 {
        match mode {
            LatencyMode::Streaming => &self.streaming_ewma_ms,
            LatencyMode::NonStreaming => &self.nonstreaming_ewma_ms,
        }
    }

    pub fn ewma_ms(&self, mode: LatencyMode) -> u64 {
        self.ewma_field(mode).load(Ordering::Relaxed)
    }

    pub fn is_cold(&self, mode: LatencyMode) -> bool {
        self.ewma_ms(mode) == u64::MAX
    }

    pub fn success_rate(&self) -> f64 {
        f64::from_bits(self.success_rate.load(Ordering::Relaxed))
    }

    /// Requests outstanding against this candidate right now.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn record_outcome(&self, success: bool, alpha: f64) {
        let sample = if success { 1.0 } else { 0.0 };
        // Switch to `update` once MSRV reaches 1.95.
        #[allow(deprecated)]
        let _ = self
            .success_rate
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some((alpha * sample + (1.0 - alpha) * f64::from_bits(current)).to_bits())
            });
    }

    pub fn update_ewma(&self, mode: LatencyMode, observed_ms: u64, alpha: f64) {
        // Switch to `update` once MSRV reaches 1.95.
        #[allow(deprecated)]
        let _ = self
            .ewma_field(mode)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                // EWMA of positive millis; rounds to a bounded non-negative u64.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                Some(if old == u64::MAX {
                    observed_ms
                } else {
                    (alpha * observed_ms as f64 + (1.0 - alpha) * old as f64).round() as u64
                })
            });
    }
}

/// Per-candidate state stored in a flat `Vec`, addressed by a stable index
/// minted at `register` time. Indices are safe because `register` only
/// appends and the tracker is frozen behind `&Tracker` once moved into
/// `AppState`. Cross-alias sharing is preserved by `ModelMap` deduping
/// (provider, model) pairs to the same index. One `alpha` smooths both the
/// latency and success-rate EWMAs, so the signals react on the same timescale.
///
/// A slot is an `Arc` so that a request in flight can hold a handle on the
/// candidate serving it ([`InFlightGuard`]) without borrowing the tracker.
#[derive(Debug)]
pub struct Tracker {
    stats: Vec<Arc<CandidateStats>>,
    alpha: f64,
    error_threshold: f64,
    /// Serialises *scoring* a candidate on its queue with *reserving* a place
    /// in it, so a pick is visible to the next pick the instant it is made.
    ///
    /// Why it is needed: reading `in_flight` and making the reservation that
    /// answers it are two operations, and routing has to run them as one. Do
    /// them separately and a pair of requests picked on the same instant both
    /// read an idle alias and both land on the box the first one chose — the
    /// pile-up the queue adjustment exists to prevent, which is why the
    /// adjustment alone only narrowed the window from "the EWMA's reaction
    /// time" to "the microseconds between one pick's read and its increment".
    /// Holding this lock from [`crate::router::weighted_schedule`] through the
    /// pick to [`Tracker::reserve`] closes the window.
    ///
    /// Why one lock for the whole router rather than one per alias: what it
    /// protects is the per-*candidate* count, and a candidate is deliberately
    /// shared across aliases — `ModelMap` gives a (provider, model) pair one
    /// stats slot no matter how many aliases list it — so per-alias locks
    /// would let two aliases reserving against the same box read the same
    /// stale snapshot, which is the same race with a name change. Coarser than
    /// per-candidate as well, and that is affordable because the critical
    /// section is a pure computation over one alias's handful of candidates:
    /// build a small `Vec` of ratios, one `powf` each, one atomic add. No I/O,
    /// no allocation growth, no `.await`, once per request, in a proxy whose
    /// per-request budget is a model generation. Contending on it is not a
    /// load this process is capable of producing.
    ///
    /// A `std::sync::Mutex`, deliberately not the async one: the section it
    /// guards contains no `await` points, so an async lock would only move
    /// cost onto the executor and put a held lock in every signature between
    /// here and the upstream call.
    routing_lock: Mutex<()>,
}

/// Proof that the caller holds [`Tracker`]'s routing lock, returned by
/// [`Tracker::lock_routing`]. Held for the duration of one routing decision
/// and dropped when it ends; it is a plain synchronous guard because a routing
/// decision contains no `await` points.
#[derive(Debug)]
pub struct RoutingGuard<'a> {
    /// Held, never read: owning it for the length of one routing decision is
    /// the whole job, and dropping it is what releases the lock.
    _lock: MutexGuard<'a, ()>,
}

impl Tracker {
    pub fn new(alpha: f64, error_threshold: f64) -> Self {
        Self {
            stats: Vec::new(),
            alpha,
            error_threshold,
            routing_lock: Mutex::new(()),
        }
    }

    /// Allocates a new stats slot and returns its index. The index is stable
    /// for the lifetime of the tracker.
    pub fn register(&mut self) -> usize {
        let idx = self.stats.len();
        self.stats.push(Arc::new(CandidateStats::new()));
        idx
    }

    pub fn stats(&self, index: usize) -> &CandidateStats {
        &self.stats[index]
    }

    /// Requests outstanding against the candidate at `index` right now.
    ///
    /// A lock-free snapshot, which is what an observer wants. It is not what a
    /// routing decision needs: between this returning and the caller
    /// reserving, another request can reserve against the same count. Decisions
    /// read it as part of [`crate::router::weighted_schedule`] while holding
    /// [`Tracker::lock_routing`] and reserve with [`Tracker::reserve`] before
    /// releasing it, so they never route on a count they have already spent.
    pub fn in_flight(&self, index: usize) -> usize {
        self.stats[index].in_flight()
    }

    /// Take the tracker's routing lock for the duration of one routing
    /// decision: score the candidates on their queues, pick, then reserve with
    /// [`Tracker::reserve`]. Nothing is `await`ed while it is held, and it is
    /// never taken recursively — [`Tracker::dispatch`] is the non-decision path
    /// and takes it exactly once.
    pub(crate) fn lock_routing(&self) -> RoutingGuard<'_> {
        RoutingGuard {
            // Poisoned only if a request panicked inside a routing decision,
            // which left the counts precisely as it found them (the section's
            // one mutation is the reservation at its end). Answering every
            // later request with a routing failure would be the worse
            // outcome, so take the lock back.
            _lock: self
                .routing_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }

    /// Count a request against the candidate it was just dispatched to, and
    /// return the handle that owns that count.
    ///
    /// The count starts when a request is *committed* to a candidate — before
    /// the upstream is contacted — so it covers the whole wait on that box:
    /// connecting, uploading, time-to-first-byte and the stream itself. That
    /// is what load-aware routing needs: a count that only appeared at first
    /// byte would let a same-instant pair of requests both pick the same busy
    /// candidate, which is exactly the condition they should have avoided.
    /// There is no retry policy to place the count around — one request is
    /// dispatched to exactly one candidate — and a request that never gets a
    /// candidate (unknown alias, no healthy candidate, shed with 429) is never
    /// counted at all.
    ///
    /// The count is returned by [`InFlightGuard::drop`], so releasing it rides
    /// on the response rather than on the code paths that notice one: a
    /// connect failure, a timeout, an upstream 5xx and a caller that hangs up
    /// mid-stream all drop the guard, and nothing else has to remember to
    /// decrement.
    ///
    /// Reserves under the routing lock, so a request that never went through
    /// the router at all — one riding an `x-session-affinity` pin — is queued
    /// against its box the same way a routed one is, and the next pick sees it.
    /// A routing decision that has already scored the candidates holds the lock
    /// and calls [`Tracker::reserve`] instead.
    pub fn dispatch(&self, index: usize) -> InFlightGuard {
        let held = self.lock_routing();
        self.reserve(index, &held)
    }

    /// The reservation half of a routing decision, with the routing lock
    /// already held from the moment the candidates were scored. That is what
    /// makes the pick and the reservation one step: the counts the schedule was
    /// computed from cannot change until this request has been added to one of
    /// them, and the next decision takes the lock only after that.
    pub(crate) fn reserve(&self, index: usize, _held: &RoutingGuard<'_>) -> InFlightGuard {
        let stats = Arc::clone(&self.stats[index]);
        // `Relaxed` is enough between two decisions: the routing lock's
        // release/acquire pair is what makes one pick's increment visible to
        // the next. The atomic itself stays relaxed for the lock-free readers
        // (`/status`, the gauge) that only ever want the current value.
        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard { stats }
    }

    pub fn record_success(&self, index: usize, mode: LatencyMode, observed: Duration) {
        // request latency in millis always fits u64.
        #[allow(clippy::cast_possible_truncation)]
        let ms = observed.as_millis() as u64;
        self.stats[index].update_ewma(mode, ms, self.alpha);
        self.stats[index].record_outcome(true, self.alpha);
    }

    pub fn record_error(&self, index: usize) {
        self.stats[index].record_outcome(false, self.alpha);
    }

    /// Error-rate EWMA above `error_threshold`. Not consulted by routing —
    /// it only breaks session-affinity pins and labels `/status`.
    pub fn is_degraded(&self, index: usize) -> bool {
        1.0 - self.stats[index].success_rate() > self.error_threshold
    }
}

/// One unit of a candidate's in-flight count: the token a request carries from
/// the moment it is dispatched to that candidate until its response is over.
///
/// The guard is the only place the count is released, and it is released
/// exactly once, in `Drop`. That is what keeps the count honest on the paths
/// that are easy to forget: for a streaming response the guard is moved into
/// the relay stream, so the count comes back when the stream ends, when it
/// errors, *and* when the stream is dropped mid-flight because the caller hung
/// up or the connection died. There is deliberately no explicit release to
/// call, because an explicit release alongside the `Drop` is a double
/// decrement waiting to happen.
///
/// Not `Clone`: one dispatched request is one unit of load. The guard also
/// owns the candidate's stats slot, so a request that wants to do something
/// about the candidate when its response finishes — record an outcome against
/// it, say — can do it from this handle, from inside the stream, without
/// reaching back through the tracker.
#[derive(Debug)]
#[must_use = "the guard returns the in-flight count when dropped; hold it until the request is finished with"]
pub struct InFlightGuard {
    stats: Arc<CandidateStats>,
}

impl InFlightGuard {
    /// The candidate this request is being counted against.
    pub fn stats(&self) -> &CandidateStats {
        &self.stats
    }

    /// Requests this candidate has outstanding right now, this one included.
    pub fn in_flight(&self) -> usize {
        self.stats.in_flight()
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Saturating, not `fetch_sub`: every guard is born from one increment
        // and `Drop` runs once per guard, so it cannot underflow — but a wrap
        // to `usize::MAX` would poison every future routing decision, and a
        // stuck count is easier to live with than a poisoned router.
        // Switch to `update` once MSRV reaches 1.95.
        #[allow(deprecated)]
        let _ = self
            .stats
            .in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_sub(1))
            });
    }
}

#[cfg(test)]
mod tests;
