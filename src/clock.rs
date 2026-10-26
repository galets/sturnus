//! Monotonic time for the router's timers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Milliseconds since this clock was created, plus an offset that only ever
/// moves forward.
///
/// The router's health model is a window on a clock, not a counter of traffic:
/// a demotion ends when the clock says so, and nothing probes it back. Reading
/// that deadline off an `Instant` directly would make "the window has expired"
/// a test that waits out the whole window, so the reading carries an offset.
/// Production code never advances it — the offset stays `0` and the clock *is*
/// `Instant` — while a test crosses a window's end by moving the offset
/// forward, which is what lets one test assert both "no placements during the
/// window" and "fully eligible the moment it expires" without sleeping.
///
/// The offset is shared, so it is advanced through `&self`: the clock lives
/// behind the `Tracker` that `AppState` holds.
#[derive(Debug)]
pub struct Clock {
    epoch: Instant,
    offset_ms: AtomicU64,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            offset_ms: AtomicU64::new(0),
        }
    }

    /// The current reading, in milliseconds since this clock started.
    ///
    /// Saturating: a `u64` of milliseconds is ~584 million years, so the only
    /// way to reach the ceiling is to advance the clock deliberately, and a
    /// reading that clamps at the ceiling keeps every deadline in the past
    /// (everything eligible) rather than wrapping one into the far future.
    pub fn now_ms(&self) -> u64 {
        // A millisecond count of a process's uptime always fits u64 in
        // practice; the allow is for the u128 arithmetic `as_millis` returns.
        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ms = self.epoch.elapsed().as_millis() as u64;
        elapsed_ms.saturating_add(self.offset_ms.load(Ordering::Relaxed))
    }

    /// Move the reading forward by `by` without waiting for it.
    ///
    /// This is the test seam for a window's end. Production code has no
    /// business calling it: a clock that jumps forward is a clock that ends
    /// every demotion window early.
    pub fn advance(&self, by: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let ms = by.as_millis() as u64;
        self.offset_ms.fetch_add(ms, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests;
