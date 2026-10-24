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
/// where effective latency folds the error rate and the outstanding load in.
/// Higher = stronger preference for the best. Scale-invariant (depends on the
/// ratio, not absolute ms), so a fixed default generalises: a 2x-worse
/// candidate gets ~1/8 the traffic, a 5x-worse one ~1/125.
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

/// Queue latency per outstanding request, in multiples of the candidate's
/// own effective latency: a candidate serving `n` requests is scored as
/// `effective * (1 + n * QUEUE_LATENCY_PER_REQUEST)`. Fixed at 1.0 — one
/// queued request costs one measured latency of expected wait — not a config
/// knob, for the same reason `PROBE_FLOOR` is not one: it is a property of
/// how a box serialises work, not a traffic preference. The reasoning lives
/// at the site the score is computed, in [`weighted_schedule`].
const QUEUE_LATENCY_PER_REQUEST: f64 = 1.0;

/// The queue multiplier for a candidate with `in_flight` requests
/// outstanding: exactly `1.0` when idle, so an alias with nothing in flight
/// routes on measured latency alone.
fn queue_factor(in_flight: usize) -> f64 {
    // A usize count of live requests is far below f64's exact-integer range.
    #[allow(clippy::cast_precision_loss)]
    {
        1.0 + QUEUE_LATENCY_PER_REQUEST * in_flight as f64
    }
}

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
    /// from, including the guards applied to it and the queue adjustment for
    /// outstanding load — the wait a request routed here can expect. `None`
    /// while cold for this mode, which is why a cold candidate's weight is a
    /// probe, not a ranking.
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
/// EWMA, i.e. expected time per successful response, adjusted for the load
/// already outstanding against each candidate (see the queue comment in the
/// body). Each candidate gets traffic proportional to
/// `(best_effective / its_effective)^k`, so slower or erroring candidates
/// keep a small but never-zero share. That share doubles as the probe
/// through which a recovered candidate wins traffic back.
///
/// Pure: reads the tracker but does not advance the round-robin counter and
/// does not reserve anything, so it can be called for observability without
/// perturbing routing — `/status` renders straight from it with no lock held.
///
/// What it reads is a snapshot: in-flight counts change underneath it as
/// requests are dispatched and released. That is fine for a caller that is
/// only *looking*. A caller that is about to *route* must not call it on its
/// own, because the counts it scored are spent by the time it has picked: hold
/// [`Tracker::lock_routing`] across schedule, pick and reserve, which is
/// exactly what [`select_and_dispatch`] does.
pub fn weighted_schedule<'a>(
    candidates: &'a [ResolvedCandidate],
    tracker: &Tracker,
    k: f64,
    mode: LatencyMode,
) -> WeightedSchedule<'a> {
    // Effective latency (ms per successful response), then the queue
    // adjustment; None = cold for `mode`.
    //
    // Why multiply by (1 + in_flight): the score a request should be routed
    // on is the wait IT can expect, not the latency this box happened to
    // measure last. A box with n requests outstanding is on track to spend
    // ~n effective latencies delivering them, and an arrival that queues
    // behind that work waits ~n·L before its own ~L of service — the classic
    // FCFS sojourn estimate, (n + 1)·L. Real providers batch rather than
    // serialise, so (1 + n) overstates the wait at high n; overstating is
    // the safe direction (traffic leaves a hot box early and re-probes it as
    // its guard drops), and it keeps the form scale-invariant like `k`
    // itself: one queued request costs one measured latency whether that
    // latency is 80ms or 8s.
    //
    // Why this shape and not an additive `L + n·C`: a constant per-request
    // penalty C cannot be right at every scale — 500ms is noise on a 30s
    // batch and catastrophe on a 20ms one — while a multiplicative factor
    // composes with the ratio-of-latencies weighting the share table already
    // rests on.
    //
    // Three properties this buys:
    // * A burst spreads. N equal idle candidates weigh equal; the first pick
    //   doubles its target's effective latency, dropping its weight by 2^k
    //   (8x at k=3) against every still-idle candidate, so the next pick
    //   lands elsewhere; when every candidate carries one request they are
    //   equal again and the Weyl walk simply cycles. Concurrency, not the
    //   EWMA's reaction time, is what reorders the candidates — and only if
    //   the first pick's request is already counted when the second scores,
    //   which is the job of the atomic select-and-reserve in
    //   [`select_and_dispatch`].
    // * Proportional share survives. The factor is applied per candidate
    //   inside the latency the existing ratio is taken on, so an all-idle
    //   alias multiplies every latency by 1.0 — an exact identity, the old
    //   arithmetic bit for bit — and a genuinely slower idle candidate keeps
    //   the share the share table promises. A busy candidate's share shrinking
    //   is not proportionality breaking, it is the load-dependent share the
    //   ratio was always meant to express; the probe floor is applied after
    //   the adjustment, so even slow *and* busy never goes to zero.
    // * It drains without oscillation. The only memory is the requests
    //   themselves: each response drops its guard (see `Tracker::dispatch`)
    //   and decays the factor by one step toward the identity, so the
    //   candidate that served a burst wins its share back smoothly as its
    //   queue empties. Nothing integrates, nothing overshoots: at in_flight
    //   0 the schedule is exactly the latency-only schedule, which is the
    //   fixed point any idle alias sits at.
    let effective: Vec<Option<f64>> = candidates
        .iter()
        .map(|c| {
            let stats = tracker.stats(c.stats_index);
            if stats.is_cold(mode) {
                None
            } else {
                // Guarded against a 0ms EWMA.
                #[allow(clippy::cast_precision_loss)]
                let measured =
                    stats.ewma_ms(mode).max(1) as f64 / stats.success_rate().max(MIN_SUCCESS_RATE);
                Some(measured * queue_factor(stats.in_flight()))
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
            // by success rate so erroring cold candidates back off too, and
            // by the same queue factor the warm path got — a cold candidate
            // that is already serving requests is not a free probe slot.
            // (Dividing the weight by (1+n)^k is the latency-domain factor
            // (L·(1+n)) carried into the weight domain, where a cold
            // candidate has no L of its own to inflate.)
            //
            // The floor clips that factor short: at the default sharpness a
            // cold 25% probe is 250 units idle, 31 with one request out, and
            // at or below the 10-unit floor from two on. So the queue term
            // backs a cold box off for its first request or two; past that its
            // share is the floor's, and load stops mattering to it — the warm
            // path has no such ceiling, which is why a busy candidate keeps
            // yielding while a cold one settles at its probe rate.
            None => {
                let stats = tracker.stats(candidate.stats_index);
                let rate = stats.success_rate();
                let base = if best.is_finite() {
                    COLD_PROBE_WEIGHT
                } else {
                    1.0
                };
                base * rate.max(MIN_SUCCESS_RATE).powf(k) / queue_factor(stats.in_flight()).powf(k)
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
///
/// This is the *scoring* half of a routing decision: it reserves nothing, so
/// the counts it scored can be spent by someone else before the caller has
/// dispatched. A request that is about to be dispatched goes through
/// [`select_and_dispatch`] instead, and only a caller that has no reservation
/// to make — the closed-loop simulation, and every test that wants the pick
/// without the load — should want this one.
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

/// One request's routing decision: the candidate it belongs to, the load it
/// reserved there, and the schedule it was picked from.
#[derive(Debug)]
pub struct Dispatch<'a> {
    /// The candidate this request is now on its way to.
    pub candidate: &'a ResolvedCandidate,
    /// This request's unit of that candidate's in-flight count. Kept by the
    /// caller until its response is over — `Tracker::dispatch` explains what
    /// the count covers and how it comes back.
    pub in_flight: crate::tracker::InFlightGuard,
    /// The schedule the pick was drawn from, i.e. the queue-adjusted weights
    /// and shares as they were *before* this request's own reservation. That
    /// is the view the decision was made on, which is the one worth logging
    /// and the one `/status` reports.
    pub schedule: WeightedSchedule<'a>,
}

/// Score, pick and reserve a candidate for `alias` as one indivisible step,
/// and return the request's in-flight guard along with its choice.
///
/// Indivisible is the whole point. Scoring reads each candidate's in-flight
/// count and reserving adds to one of them; done as two steps, two requests
/// picked at the same instant both read the same all-idle snapshot and both
/// take the box the first one liked, so the queue adjustment that was supposed
/// to spread the burst has already been evaluated and lost by the time the
/// first reservation lands. On a multi-threaded runtime that window is not
/// theoretical — it is the microseconds between a read and an increment, which
/// is comfortably shorter than the gap between two arrivals an agent firing a
/// burst produces.
///
/// So all three steps run inside [`Tracker::lock_routing`], a synchronous
/// lock that is never held across an `await` — the section contains no `await`
/// points at all, and the guard is dropped before this returns, so nothing
/// here can keep the executor or a downstream call waiting on it. The lock
/// lives on the tracker and covers the whole router rather than one alias;
/// both choices are justified at the field, `Tracker::routing_lock`.
///
/// The pure seams stay pure: [`weighted_schedule`] and
/// [`pick_from_schedule`] take no lock and reserve nothing, so `/status` keeps
/// rendering the same arithmetic with no reservation and no contention.
pub fn select_and_dispatch<'a>(
    alias: &str,
    candidates: &'a [ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    k: f64,
    mode: LatencyMode,
) -> Option<Dispatch<'a>> {
    let held = tracker.lock_routing();
    let schedule = weighted_schedule(candidates, tracker, k, mode);
    let candidate = pick_from_schedule(&schedule, alias, rr)?;
    let in_flight = tracker.reserve(candidate.stats_index, &held);
    Some(Dispatch {
        candidate,
        in_flight,
        schedule,
    })
}

#[cfg(test)]
mod tests;
