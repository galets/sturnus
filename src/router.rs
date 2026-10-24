use crate::model_map::ResolvedCandidate;
use crate::tracker::{LatencyMode, Tracker};
use std::sync::atomic::Ordering;

/// One counter per alias, walked over the weighted schedule (no RNG).
#[derive(Debug)]
pub struct RoundRobinState {
    counters: std::collections::HashMap<String, std::sync::atomic::AtomicUsize>,
}

impl Default for RoundRobinState {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinState {
    pub fn new() -> Self {
        Self {
            counters: std::collections::HashMap::new(),
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

/// Default exploit sharpness (the `routing.exploit_k` default): traffic to
/// each candidate is proportional to `(best_effective / its_effective)^k`,
/// where effective latency folds the error rate in. Higher = stronger
/// preference for the best. Scale-invariant (depends on the ratio, not
/// absolute ms), so a fixed default generalises: a 2x-worse candidate gets
/// ~1/8 the traffic, a 5x-worse one ~1/125.
pub const EXPLOIT_K: f64 = 3.0;

/// Fastest candidate gets this many weight units; everyone else's weight is
/// scaled relative to it (but never below the `PROBE_FLOOR` share).
const WEIGHT_RESOLUTION: f64 = 1000.0;

/// Cold candidates probe at this fraction of the fastest candidate's rate.
const COLD_PROBE_WEIGHT: f64 = 0.25;

/// Minimum weight for any candidate, as a fraction of `WEIGHT_RESOLUTION`.
/// This is the probe rate for a hard-failing candidate, and it bounds both
/// sides of the same trade: at most ~this share of alias traffic eats
/// user-facing errors during an outage (there is no failover retry), and a
/// recovered candidate is re-detected within ~`1/PROBE_FLOOR` requests.
const PROBE_FLOOR: f64 = 0.01;

/// Success-rate floor inside effective latency: a fully failing candidate
/// gets a finite (huge) value rather than a division by zero.
const MIN_SUCCESS_RATE: f64 = 1e-3;

/// 2^64 / φ, the Weyl-sequence step. Odd, so `wrapping_mul` is a bijection.
const WEYL_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// One candidate's place in a [`WeightedSchedule`].
#[derive(Debug, Clone)]
pub struct ScheduledCandidate<'a> {
    pub candidate: &'a ResolvedCandidate,
    /// Integer weight in `WEIGHT_RESOLUTION` units; never below the probe floor.
    pub weight: usize,
    /// Normalised traffic share, `weight / total_weight`, in `0.0..=1.0`.
    pub share: f64,
    /// Effective latency (ms per successful response) the weight was derived
    /// from, including the guards applied to it. `None` while cold for this
    /// mode, which is why a cold candidate's weight is a probe, not a ranking.
    pub effective_latency_ms: Option<f64>,
}

/// The weighted schedule for one alias at one moment.
///
/// Computed independently of any pick so callers can inspect the weights and
/// shares without consuming a tick. The pick path draws from this same
/// schedule, keeping the reported numbers and the routing decision in sync.
#[derive(Debug)]
pub struct WeightedSchedule<'a> {
    pub entries: Vec<ScheduledCandidate<'a>>,
    /// Sum of every entry's weight; the modulus of the Weyl pick.
    pub total_weight: usize,
}

/// Every candidate's share on one line, e.g.
/// `groq/llama-3.3-70b=88.9% openai/gpt-4o-mini=11.1%` — the shape the routing
/// debug line wants. `Display` rather than a built `String` so
/// `debug!(shares = %schedule)` costs nothing unless the callsite is enabled.
impl std::fmt::Display for WeightedSchedule<'_> {
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

/// Compute the weighted schedule for `candidates` in `mode`.
///
/// Weights use *effective latency* — the latency EWMA over the success-rate
/// EWMA, i.e. expected time per successful response. Each candidate gets
/// traffic proportional to `(best_effective / its_effective)^k`, so slower
/// or erroring candidates keep a small but never-zero share. That share
/// doubles as the probe through which a recovered candidate wins traffic back.
///
/// Pure: reads the tracker but does not advance the round-robin counter, so
/// it can be called for observability without perturbing routing.
pub fn weighted_schedule<'a>(
    candidates: &'a [ResolvedCandidate],
    tracker: &Tracker,
    k: f64,
    mode: LatencyMode,
) -> WeightedSchedule<'a> {
    // Effective latency (ms per successful response); None = cold for `mode`.
    let effective: Vec<Option<f64>> = candidates
        .iter()
        .map(|c| {
            let stats = tracker.stats(c.stats_index);
            if stats.is_cold(mode) {
                None
            } else {
                // Guarded against a 0ms EWMA.
                #[allow(clippy::cast_precision_loss)]
                Some(stats.ewma_ms(mode).max(1) as f64 / stats.success_rate().max(MIN_SUCCESS_RATE))
            }
        })
        .collect();

    // Reference = best effective latency; infinite while everyone is cold.
    let best = effective
        .iter()
        .flatten()
        .copied()
        .fold(f64::INFINITY, f64::min);

    // Floor at the probe share so no candidate starves.
    let mut entries: Vec<ScheduledCandidate<'a>> = Vec::with_capacity(candidates.len());
    let mut total_weight: usize = 0;
    for (candidate, effective_latency) in candidates.iter().zip(&effective) {
        let raw_weight = match effective_latency {
            Some(latency) => (best / latency).powf(k),
            // Cold: fixed probe share (full when everyone is cold), scaled
            // by success rate so erroring cold candidates back off too.
            None => {
                let rate = tracker.stats(candidate.stats_index).success_rate();
                let base = if best.is_finite() {
                    COLD_PROBE_WEIGHT
                } else {
                    1.0
                };
                base * rate.max(MIN_SUCCESS_RATE).powf(k)
            }
        };
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let weight = (raw_weight * WEIGHT_RESOLUTION)
            .round()
            .max(WEIGHT_RESOLUTION * PROBE_FLOOR) as usize;
        entries.push(ScheduledCandidate {
            candidate,
            weight,
            share: 0.0,
            effective_latency_ms: *effective_latency,
        });
        total_weight += weight;
    }

    // Shares are only meaningful once the total is known.
    for entry in &mut entries {
        entry.share = normalised_share(entry.weight, total_weight);
    }

    WeightedSchedule {
        entries,
        total_weight,
    }
}

/// `weight / total_weight`, guarding the divide.
///
/// A zero total is unreachable today — every entry keeps at least the probe
/// floor — but these numbers are rendered by `/status` and the routing debug
/// line now, so a `NaN` reaching an operator would be worse than a `0.0`.
fn normalised_share(weight: usize, total_weight: usize) -> f64 {
    if total_weight == 0 {
        return 0.0;
    }
    // Both are small counts, so the f64 conversion is exact.
    #[allow(clippy::cast_precision_loss)]
    let (weight, total_weight) = (weight as f64, total_weight as f64);
    weight / total_weight
}

/// Draw a pick from a previously computed [`WeightedSchedule`].
///
/// Golden-ratio Weyl sequence (n·2^64/φ): a deterministic stand-in for the
/// uniform sample in weighted choice (cf. rand::distributions::WeightedIndex).
/// Stays equidistributed even when modes subsample the shared counter.
pub fn pick_from_schedule<'a>(
    schedule: &WeightedSchedule<'a>,
    alias: &str,
    rr: &RoundRobinState,
) -> Option<&'a ResolvedCandidate> {
    // No tick is consumed for an empty schedule, matching the old early return.
    if schedule.entries.is_empty() || schedule.total_weight == 0 {
        return None;
    }

    let spread = rr.next_tick(alias).wrapping_mul(WEYL_STEP);
    #[allow(clippy::cast_possible_truncation)]
    let pos = ((u128::from(spread) * schedule.total_weight as u128) >> 64) as usize;
    let mut acc = 0;
    for entry in &schedule.entries {
        acc += entry.weight;
        if pos < acc {
            return Some(entry.candidate);
        }
    }
    // pos < total_weight guarantees a hit above; last is a safe fallback.
    schedule.entries.last().map(|entry| entry.candidate)
}

/// Pick a candidate for `alias` by proportional weighting.
///
/// Builds the schedule with [`weighted_schedule`] and draws from it with
/// [`pick_from_schedule`], so routing consumes exactly the weights that are
/// inspectable through the schedule seam. A caller that also reports those
/// numbers (the proxy's debug line) calls the two seams itself rather than
/// computing the schedule twice.
pub fn select_candidate<'a>(
    alias: &str,
    candidates: &'a [ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    k: f64,
    mode: LatencyMode,
) -> Option<&'a ResolvedCandidate> {
    let schedule = weighted_schedule(candidates, tracker, k, mode);
    pick_from_schedule(&schedule, alias, rr)
}

#[cfg(test)]
mod tests;
