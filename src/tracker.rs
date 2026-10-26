use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;

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
    /// Starts optimistic at 1.0. Observability only: it is reported on
    /// `/status`, and it moves no share and breaks no pin. The health signal
    /// routing acts on is the demotion window, which is a timer.
    success_rate: AtomicU64,
    /// Requests currently outstanding against this candidate, held by
    /// [`InFlightGuard`]. A count of a *candidate*, not of an alias: one box
    /// reachable through several aliases has one queue, not one per alias.
    in_flight: AtomicUsize,
    /// The instant, on the tracker's clock, until which this candidate is
    /// demoted. `0` means never: the clock starts at `0`, so a deadline at or
    /// before the clock's own start has already passed and the candidate is
    /// eligible.
    demoted_until_ms: AtomicU64,
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
            demoted_until_ms: AtomicU64::new(0),
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

    /// Push the demotion deadline out to `until_ms`, on the tracker's clock.
    ///
    /// `fetch_max`, so a failure that lands while a window is already running
    /// extends it and a late reply to an older failure can never shorten it.
    fn demote_until(&self, until_ms: u64) {
        self.demoted_until_ms.fetch_max(until_ms, Ordering::Relaxed);
    }

    /// The demotion deadline, in clock milliseconds. `0` means never demoted.
    fn demoted_until_ms(&self) -> u64 {
        self.demoted_until_ms.load(Ordering::Relaxed)
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
///
/// The tracker owns the two things a routing decision reads that are not
/// declared in the config: the outstanding-turn count (busyness) and the
/// demotion window (eligibility). The EWMAs it also keeps are observability
/// only — see [`crate::router::placement_schedule`] for the allocation policy.
#[derive(Debug)]
pub struct Tracker {
    stats: Vec<Arc<CandidateStats>>,
    alpha: f64,
    /// Length of a demotion window, in milliseconds.
    demotion_ms: u64,
    clock: Clock,
}

impl Tracker {
    pub fn new(alpha: f64, demotion_secs: u64) -> Self {
        Self {
            stats: Vec::new(),
            alpha,
            demotion_ms: demotion_secs.saturating_mul(1_000),
            clock: Clock::new(),
        }
    }

    /// The clock the demotion deadlines are kept on. Its `advance` is the test
    /// seam for the end of a window; production code reads it only.
    pub fn clock(&self) -> &Clock {
        &self.clock
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
    pub fn in_flight(&self, index: usize) -> usize {
        self.stats[index].in_flight()
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
    /// candidate (unknown alias, an alias with no candidates, shed with 429)
    /// is never counted at all.
    ///
    /// The count is returned by [`InFlightGuard::drop`], so releasing it rides
    /// on the response rather than on the code paths that notice one: a
    /// connect failure, a timeout, an upstream 5xx and a caller that hangs up
    /// mid-stream all drop the guard, and nothing else has to remember to
    /// decrement.
    pub fn dispatch(&self, index: usize) -> InFlightGuard {
        let stats = Arc::clone(&self.stats[index]);
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

    /// Start (or extend) this candidate's demotion window, and return its
    /// length so the caller can log what it just cost the box.
    ///
    /// A transport failure — the request ended with no response from the box —
    /// is the only thing that calls this. An upstream 4xx/5xx is a box that
    /// answered, and answering is being up: `server.rs` relays that status and
    /// records it against the success-rate EWMA without demoting.
    ///
    /// The window is a timer, not a feedback loop: nothing probes the box
    /// during it, no traffic share is reserved for it, and it does not win
    /// traffic back gradually. When the clock passes the deadline the candidate
    /// is fully eligible at once, with nothing carried over from the demotion —
    /// its first new session is its probe, and that session eats the failure if
    /// the box is still down.
    pub fn demote(&self, index: usize) -> Duration {
        let until_ms = self.clock.now_ms().saturating_add(self.demotion_ms);
        self.stats[index].demote_until(until_ms);
        Duration::from_millis(self.demotion_ms)
    }

    /// Whether this candidate is demoted right now: standing to take on no new
    /// sessions, whatever its weight.
    pub fn is_demoted(&self, index: usize) -> bool {
        self.clock.now_ms() < self.stats[index].demoted_until_ms()
    }

    /// Time left in this candidate's demotion window, or `None` when it is
    /// eligible. `/status` reports it, so "why is this box getting nothing?"
    /// is a curl with a number in it.
    pub fn demotion_remaining(&self, index: usize) -> Option<Duration> {
        let until_ms = self.stats[index].demoted_until_ms();
        let now_ms = self.clock.now_ms();
        (now_ms < until_ms).then(|| Duration::from_millis(until_ms - now_ms))
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

/// Whole seconds left in a window, rounded up.
///
/// `9s` means the window ends within nine seconds, so a reading shown to an
/// operator never understates what is left.
pub fn secs_left(remaining: Duration) -> u64 {
    remaining.as_secs() + u64::from(remaining.subsec_millis() > 0)
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
