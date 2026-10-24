use super::*;
use crate::model_map::ProviderKind;
use crate::tracker::Tracker;
use std::time::Duration;

fn make_candidate(provider: &str, model: &str, stats_index: usize) -> ResolvedCandidate {
    ResolvedCandidate {
        provider_name: provider.to_string(),
        model: model.to_string(),
        base_url: "http://localhost".to_string(),
        api_key: None,
        kind: ProviderKind::ApiKey,
        stats_index,
        provider_header: hyper::header::HeaderValue::from_str(provider).unwrap(),
        affinity_header: hyper::header::HeaderValue::try_from(format!("{provider}/{model}"))
            .unwrap(),
        attribution_labels: None,
        extra_headers: Default::default(),
    }
}

fn setup(specs: &[(&str, &str)]) -> (Vec<ResolvedCandidate>, Tracker, RoundRobinState) {
    let mut tracker = Tracker::new(0.3, 0.5);
    let mut rr = RoundRobinState::new();
    rr.register_alias("test".to_string());
    let candidates: Vec<_> = specs
        .iter()
        .map(|(provider, model)| {
            let stats_index = tracker.register();
            make_candidate(provider, model, stats_index)
        })
        .collect();
    (candidates, tracker, rr)
}

#[test]
fn empty_candidates_returns_none() {
    let candidates: Vec<ResolvedCandidate> = vec![];
    let (_, tracker, rr) = setup(&[]);
    assert!(select_candidate(
        "test",
        &candidates,
        &tracker,
        &rr,
        EXPLOIT_K,
        LatencyMode::Streaming
    )
    .is_none());
}

#[test]
fn all_cold_round_robins() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

    let first = select_candidate(
        "test",
        &candidates,
        &tracker,
        &rr,
        EXPLOIT_K,
        LatencyMode::Streaming,
    )
    .unwrap();
    let second = select_candidate(
        "test",
        &candidates,
        &tracker,
        &rr,
        EXPLOIT_K,
        LatencyMode::Streaming,
    )
    .unwrap();
    assert_ne!(first.provider_name, second.provider_name);
}

#[test]
fn lowest_ewma_gets_the_large_majority() {
    let (candidates, tracker, rr) = setup(&[("slow", "m1"), ("fast", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(500),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );

    let n = 10_000;
    let mut fast = 0;
    for _ in 0..n {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            3.0,
            LatencyMode::Streaming,
        )
        .unwrap();
        if picked.provider_name == "fast" {
            fast += 1;
        }
    }
    let fast_share = f64::from(fast) / f64::from(n);
    assert!(
        fast_share > 0.95,
        "fast (5x quicker) should dominate at k=3, got {fast_share:.3}"
    );
}

#[test]
fn slower_candidate_keeps_a_proportional_share() {
    // Defining property of proportional routing: at k=3 a 2x-slower candidate
    // has weight (1/2)^3 = 0.125 against the fastest's 1, i.e. a normalised
    // share of 0.125 / 1.125 ≈ 0.111 — never 0 (winner-take-all) nor 0.5 (RR).
    let (candidates, tracker, rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );

    let n = 100_000;
    let mut slow = 0;
    for _ in 0..n {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            3.0,
            LatencyMode::Streaming,
        )
        .unwrap();
        if picked.provider_name == "slow" {
            slow += 1;
        }
    }
    let slow_share = f64::from(slow) / f64::from(n);
    assert!(
        (slow_share - 0.111).abs() < 0.02,
        "slow share {slow_share:.3} should be ≈0.111 (weight 0.125 normalised)"
    );
}

#[test]
fn alternating_modes_do_not_bias_the_weighted_walk() {
    // Modes share the alias tick counter, so each sees every other tick;
    // the Weyl sequence must stay unbiased under that subsampling.
    let (candidates, tracker, rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
    for mode in [LatencyMode::Streaming, LatencyMode::NonStreaming] {
        tracker.record_success(candidates[0].stats_index, mode, Duration::from_millis(100));
        tracker.record_success(candidates[1].stats_index, mode, Duration::from_millis(200));
    }

    let n = 50_000;
    let mut slow = [0u32; 2];
    for i in 0..2 * n {
        let mode = if i % 2 == 0 {
            LatencyMode::Streaming
        } else {
            LatencyMode::NonStreaming
        };
        let picked = select_candidate("test", &candidates, &tracker, &rr, 3.0, mode).unwrap();
        if picked.provider_name == "slow" {
            slow[i % 2] += 1;
        }
    }
    for (mode, count) in ["streaming", "nonstreaming"].iter().zip(slow) {
        #[allow(clippy::cast_precision_loss)]
        let share = f64::from(count) / n as f64;
        assert!(
            (share - 0.111).abs() < 0.02,
            "{mode} slow share {share:.3} should be ≈0.111 despite seeing only every other tick"
        );
    }
}

/// Majority provider over many weighted picks, for asserting which way the
/// split leans without depending on a single pick's position.
fn majority_provider(
    candidates: &[ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    mode: LatencyMode,
) -> String {
    let mut counts = std::collections::HashMap::new();
    for _ in 0..10_000 {
        let picked = select_candidate("test", candidates, tracker, rr, 3.0, mode).unwrap();
        *counts.entry(picked.provider_name.clone()).or_insert(0) += 1;
    }
    counts.into_iter().max_by_key(|(_, n)| *n).unwrap().0
}

/// Share of picks going to `provider` over `n` weighted selections.
fn share_of(
    provider: &str,
    n: u32,
    candidates: &[ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
) -> f64 {
    let mut hits = 0u32;
    for _ in 0..n {
        let picked =
            select_candidate("test", candidates, tracker, rr, 3.0, LatencyMode::Streaming).unwrap();
        if picked.provider_name == provider {
            hits += 1;
        }
    }
    f64::from(hits) / f64::from(n)
}

#[test]
fn erroring_candidate_gets_minimal_share_despite_speed() {
    // bad is 4x faster but failing hard: its effective latency blows up
    // and its share collapses to the probe floor.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(50),
    );

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }

    let bad_share = share_of("bad", 10_000, &candidates, &tracker, &rr);
    assert!(
        bad_share < 0.05,
        "hard-failing candidate should be near the floor, got {bad_share:.3}"
    );
    assert!(
        bad_share > 0.0,
        "the floor share must keep probing a failing candidate"
    );
}

#[test]
fn hard_failing_candidate_share_is_pinned_at_probe_floor() {
    // The floor bounds both sides of the trade: at most ~PROBE_FLOOR of
    // traffic eats errors during an outage, and recovery is detected
    // within ~1/PROBE_FLOOR requests. Expected share with two candidates:
    // 10 / (1000 + 10) ≈ 0.0099.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    for _ in 0..50 {
        tracker.record_error(candidates[1].stats_index);
    }

    let bad_share = share_of("bad", 100_000, &candidates, &tracker, &rr);
    assert!(
        (0.005..0.02).contains(&bad_share),
        "share should be pinned near PROBE_FLOOR, got {bad_share:.4}"
    );
}

#[test]
fn single_error_reduces_share_proportionally() {
    // Equal latency, one error at alpha 0.3: success EWMA 0.7, weight
    // 0.7^3 = 0.343, share 0.343/1.343 ≈ 0.26 — proportional, not a cliff.
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    tracker.record_error(candidates[1].stats_index);

    let b_share = share_of("b", 100_000, &candidates, &tracker, &rr);
    assert!(
        (b_share - 0.255).abs() < 0.02,
        "one error should cost a proportional share, got {b_share:.3}"
    );
}

#[test]
fn erroring_candidate_recovers_share_through_probes() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for c in &candidates {
        tracker.record_success(
            c.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(share_of("b", 10_000, &candidates, &tracker, &rr) < 0.05);

    // Probe successes rebuild the EWMA; the share follows.
    for _ in 0..20 {
        tracker.record_success(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    let recovered = share_of("b", 10_000, &candidates, &tracker, &rr);
    assert!(
        (recovered - 0.5).abs() < 0.05,
        "recovered candidate should win back its share, got {recovered:.3}"
    );
}

#[test]
fn cold_candidate_is_probed() {
    let (candidates, tracker, rr) = setup(&[("warm", "m1"), ("cold", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );

    // Cold weight 0.25 vs the fastest's 1.0: share 0.25 / 1.25 = 0.2.
    let mut cold_picks = 0;
    let n = 4000;
    for _ in 0..n {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            3.0,
            LatencyMode::Streaming,
        )
        .unwrap();
        if picked.provider_name == "cold" {
            cold_picks += 1;
        }
    }
    let cold_share = f64::from(cold_picks) / f64::from(n);
    assert!(
        (0.15..0.25).contains(&cold_share),
        "cold should probe at ~0.2, got {cold_share:.3}"
    );
}

#[test]
fn traffic_shifts_when_latency_increases() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(300),
    );
    assert_eq!(
        majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
        "a"
    );

    for _ in 0..20 {
        tracker.record_success(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(500),
        );
    }
    assert_eq!(
        majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
        "b"
    );
}

#[test]
fn cold_candidate_with_errors_probes_at_the_floor() {
    // Errors never warm the latency EWMA, so a failing candidate can stay
    // cold forever; its probe share must still scale down.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);

    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );

    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(tracker
        .stats(candidates[1].stats_index)
        .is_cold(LatencyMode::Streaming));

    let bad_share = share_of("bad", 10_000, &candidates, &tracker, &rr);
    assert!(
        bad_share < 0.05,
        "failing cold candidate should be near the floor, got {bad_share:.3}"
    );
}

#[test]
fn nonstreaming_routing_ignores_streaming_ewma() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);

    // a fast on streaming, slow on non-streaming; b the reverse.
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(50),
    );
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::NonStreaming,
        Duration::from_millis(5_000),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(5_000),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::NonStreaming,
        Duration::from_millis(50),
    );

    assert_eq!(
        majority_provider(&candidates, &tracker, &rr, LatencyMode::Streaming),
        "a"
    );
    assert_eq!(
        majority_provider(&candidates, &tracker, &rr, LatencyMode::NonStreaming),
        "b"
    );
}

/// Deterministic xorshift PRNG so the simulation is reproducible.
struct Rng(u64);
impl Rng {
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        #[allow(clippy::cast_precision_loss)]
        {
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }
}

/// Closed-loop run against a load-dependent upstream: candidate 0 ("gemini")
/// is usually fast (~600ms) but episodically throttles to ~3000ms; candidate
/// 1 ("openai") is a steady ~2000ms. Routes via the shipped `select_candidate`
/// and feeds observed latency back. Returns (picks, gemini-throttled flags).
///
/// Requests now hold their in-flight count for their own duration, so a pick
/// made while earlier requests are still outstanding sees their queues —
/// the concurrency the router scores on since the queue adjustment. Time is
/// quantised to ticks (one arrival per tick, a request held for
/// `ceil(latency / TICK_MS)` of them): a 600ms box releases its count before
/// the next arrival when it is healthy, so the idle baseline routes on
/// latency alone, while a throttled 3000ms one stays visibly busy for three
/// arrivals. The EWMA feedback itself is still synchronous at pick time;
/// that lag, not this, is where production staleness lives.
fn simulate_load_dependent() -> (Vec<usize>, Vec<bool>) {
    /// One arrival per tick; latencies in ms, holds quantised to whole ticks.
    const TICK_MS: u64 = 1_000;

    let mode = LatencyMode::NonStreaming;
    let mut tracker = Tracker::new(0.3, 0.5);
    let g = tracker.register();
    let o = tracker.register();
    let candidates = vec![
        make_candidate("gemini", "m1", g),
        make_candidate("openai", "m2", o),
    ];
    let mut rr = RoundRobinState::new();
    rr.register_alias("test".to_string());
    tracker.record_success(g, mode, Duration::from_millis(600));
    tracker.record_success(o, mode, Duration::from_millis(2000));

    let n = 12_000usize;
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut throttled = false;
    let mut picks = Vec::with_capacity(n);
    let mut flags = Vec::with_capacity(n);
    // (tick its guard comes back, guard) for every request still outstanding.
    let mut outstanding: Vec<(u64, crate::tracker::InFlightGuard)> = Vec::new();

    for i in 0..n {
        // Requests that finished before this arrival stop weighing on it.
        outstanding.retain(|(release, _)| *release > i as u64);

        // Two-state Markov throttle: rare onset, ~50-tick average duration.
        throttled = if throttled {
            rng.unit() >= 0.02
        } else {
            rng.unit() < 0.004
        };
        flags.push(throttled);

        let picked = select_candidate("test", &candidates, &tracker, &rr, EXPLOIT_K, mode).unwrap();
        let idx = usize::from(picked.provider_name == "openai");
        let latency = match idx {
            0 if throttled => 3000,
            0 => 600,
            _ => 2000,
        };
        tracker.record_success(
            candidates[idx].stats_index,
            mode,
            Duration::from_millis(latency),
        );
        let hold_ticks = latency.div_ceil(TICK_MS).max(1);
        let guard = tracker.dispatch(candidates[idx].stats_index);
        outstanding.push((i as u64 + hold_ticks, guard));
        picks.push(idx);
    }
    (picks, flags)
}

#[test]
fn does_not_over_route_to_a_load_dependent_upstream() {
    // The core value of proportional routing: when the fast provider briefly
    // throttles, traffic shifts to the slower one and back — it never herds
    // onto it beyond the actual throttle windows. Because the loser keeps a
    // small share, gemini stays sampled and its recovery is seen at once.
    // With the queue adjustment this also holds the other way: while gemini
    // is busy serving a throttle episode its own in-flight count moves the
    // traffic, and it wins the share straight back as that queue drains.
    let (picks, flags) = simulate_load_dependent();
    #[allow(clippy::cast_precision_loss)]
    let overrouting = picks
        .iter()
        .zip(&flags)
        .filter(|(&p, &throttled)| p == 1 && !throttled)
        .count() as f64
        / picks.len() as f64;
    assert!(
        overrouting < 0.06,
        "openai should be used only while gemini is throttled, got {overrouting:.3}"
    );
}

// --- Queue-adjusted latency -------------------------------------------------

/// One request per candidate, no exceptions: four equal candidates, four
/// picks, each pick holding its in-flight guard before the next runs — the
/// same-instant burst latency-only routing used to send to one box. The
/// assertion is on the distribution (exactly one per candidate), not on
/// "not all the same": two-on-one with two-untouched is still a queue.
#[test]
fn burst_of_picks_spreads_one_per_candidate() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2"), ("c", "m3"), ("d", "m4")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }

    // Guards live until the whole burst has been picked, mirroring the proxy:
    // each request is dispatched (counted) as it is committed to its box.
    let mut guards = Vec::new();
    let mut counts = std::collections::HashMap::new();
    for _ in 0..candidates.len() {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming,
        )
        .unwrap();
        guards.push(tracker.dispatch(picked.stats_index));
        *counts.entry(picked.provider_name.clone()).or_insert(0usize) += 1;
    }

    for candidate in &candidates {
        assert_eq!(
            counts.get(&candidate.provider_name).copied(),
            Some(1),
            "every equal candidate should carry exactly one of the burst, got {counts:?}",
        );
    }
    // The counts the picks saw were real: four boxes, one request each.
    for candidate in &candidates {
        assert_eq!(tracker.in_flight(candidate.stats_index), 1);
    }
}

/// The burst property holds before any latency is measured at all: all four
/// candidates cold, the probe share is the only weight, and one dispatch
/// still drops a candidate's probe by `(1+1)^k` against the idle ones.
#[test]
fn a_cold_burst_spreads_across_cold_candidates() {
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2"), ("c", "m3"), ("d", "m4")]);

    let mut guards = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..candidates.len() {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming,
        )
        .unwrap();
        assert!(
            seen.insert(picked.provider_name.clone()),
            "cold burst revisited {} before spreading",
            picked.provider_name
        );
        guards.push(tracker.dispatch(picked.stats_index));
    }
}

/// The adjustment is a function of the queue, not a grudge: dispatch three
/// requests onto one of two equal candidates and its score drops; release
/// the guards one at a time and the share walks monotonically back to the
/// exactly-idle baseline — the candidate that served a burst is not
/// starved, and at in_flight 0 the arithmetic is bit-identical to before.
#[test]
fn queue_adjustment_decays_as_in_flight_drains() {
    let (candidates, tracker, _rr) = setup(&[("busy", "m1"), ("idle", "m2")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }

    let baseline = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    assert_eq!(baseline.entries[0].weight, 1000);
    assert_eq!(baseline.entries[1].weight, 1000);

    let guards: Vec<_> = (0..3)
        .map(|_| tracker.dispatch(candidates[0].stats_index))
        .collect();

    let loaded = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    // (1 + 3) x 100ms of expected wait against an idle 100ms twin.
    assert_eq!(loaded.entries[0].effective_latency_ms, Some(400.0));
    assert_eq!(loaded.entries[1].effective_latency_ms, Some(100.0));
    assert_eq!(
        loaded.entries[0].weight, 16,
        "(100/400)^3 * 1000, the idle twin is the reference"
    );
    assert_eq!(loaded.entries[1].weight, 1000);
    assert!(loaded.entries[0].share < baseline.entries[0].share);

    // Draining one request at a time moves the share monotonically back.
    let mut guards = guards;
    let mut previous = loaded.entries[0].share;
    for expected_effective in [300.0, 200.0, 100.0] {
        let guard = guards.pop().expect("three guards");
        drop(guard);
        let draining = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
        assert_eq!(
            draining.entries[0].effective_latency_ms,
            Some(expected_effective)
        );
        assert!(
            draining.entries[0].share > previous,
            "share should recover as the queue drains"
        );
        previous = draining.entries[0].share;
    }

    // Fully drained: the identity is exact, not approximate — no memory of
    // the burst survives it, so nothing can starve a candidate that just
    // served one.
    let drained = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    assert_eq!(drained.total_weight, baseline.total_weight);
    for (drained, baseline) in drained.entries.iter().zip(&baseline.entries) {
        assert_eq!(drained.weight, baseline.weight);
        assert!((drained.share - baseline.share).abs() < 1e-12);
    }
}

/// The probe floor is applied after the queue adjustment: a candidate that
/// is both genuinely bad *and* busy keeps the same never-zero share it kept
/// before this ticket — the adjustment reorders scores, it never deletes a
/// candidate from its own schedule.
#[test]
fn a_busy_alias_still_floors_its_worst_candidate() {
    let (candidates, tracker, _rr) = setup(&[("good", "m1"), ("bad", "m2")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    for _ in 0..50 {
        tracker.record_error(candidates[1].stats_index);
    }

    // Three requests in flight on the healthy candidate inflate it to 400ms;
    // the hard-failing candidate (effective latency clamped to 1000x the
    // reference) is still hopeless next to that, and is still floored.
    let guards: Vec<_> = (0..3)
        .map(|_| tracker.dispatch(candidates[0].stats_index))
        .collect();
    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    assert_eq!(schedule.entries[0].effective_latency_ms, Some(400.0));
    assert_eq!(schedule.entries[1].weight, 10, "still at the probe floor");
    assert!(schedule.entries[1].share > 0.0);
    assert!(
        schedule.entries[1].share < schedule.entries[0].share,
        "and still ranked far below the healthy one"
    );
    drop(guards);
}

/// The queue adjustment reuses the one number `k` already scales: at
/// `k = 0` every warm ratio is 1, and it stays 1 with a queue — a
/// sharpness-0 alias splits evenly whether or not it is busy, exactly as
/// documented.
#[test]
fn zero_sharpness_blindly_queues_too() {
    let (candidates, tracker, _rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    let guards: Vec<_> = (0..4)
        .map(|_| tracker.dispatch(candidates[0].stats_index))
        .collect();

    let schedule = weighted_schedule(&candidates, &tracker, 0.0, LatencyMode::Streaming);
    assert_eq!(schedule.entries[0].effective_latency_ms, Some(500.0));
    assert_eq!(
        schedule.entries[0].weight, schedule.entries[1].weight,
        "k = 0 raises every ratio, queue-inflated or not, to the power 0"
    );
    drop(guards);
}

const ALL_COLD: &str = concat!(
    "ababaababbabaababbababbabaababbabaababaababbabaababbababbabaabab",
    "babaababaababbabaababbabaababaababbabaababbababbabaababbabaababa",
    "ababbabaababbababbabaababbabaababbababbabaababbabaababaababbabaa",
    "babbababbabaababbabaababaababbabaababbabaababaababbabaababbababb",
);

const MIXED_COLD: &str = concat!(
    "aaabaaaabaaaaaaabaaaabaabaaaabaaaaaaabaaaabaabaaaabaaaabaabaaaab",
    "aaaaaaabaaaabaabaaaabaaaaaaabaaaabaabaaaabaaaabaabaaaabaaaaaaaba",
    "aaabaabaaaabaaaabaabaaaabaaaaaaabaaaabaabaaaabaaaaaaabaaaabaabaa",
    "aabaaaabaabaaaabaaaaaaabaaaabaabaaaabaaaaaaabaaaabaabaaaabaaaaba",
);

const FLOORED: &str = concat!(
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaa",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
);

const DEGRADED: &str = concat!(
    "aaaaaaaabaaaaaaaaaaaabaaaaaaabaaaaaaaaaaaabaaaaaaabaaaabaaaaaaab",
    "aaaaaaaaaaaabaaaaaaabaaaaaaaaaaaabaaaaaaaaaaaabaaaaaaabaaaaaaaaa",
    "aaabaaaaaaabaaaabaaaaaaabaaaaaaaaaaaabaaaaaaabaaaaaaaaaaaabaaaaa",
    "aabaaaabaaaaaaabaaaaaaaaaaaabaaaaaaabaaaaaaaaaaaabaaaaaaaaaaaaba",
);

// --- Weighted schedule seam ------------------------------------------------

/// The fixed golden sequences below were captured from the implementation
/// immediately before the schedule/pick split. Each character is one pick:
/// `a` for the first/healthy candidate, `b` for the second. They must keep
/// matching exactly — the extraction is a pure refactor.
fn golden_trace(
    candidates: &[ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    mode: LatencyMode,
) -> String {
    (0..256)
        .map(|_| {
            let picked =
                select_candidate("test", candidates, tracker, rr, EXPLOIT_K, mode).unwrap();
            match picked.provider_name.as_str() {
                "a" | "warm" | "good" => 'a',
                "b" | "cold" | "bad" => 'b',
                other => panic!("unexpected provider {other}"),
            }
        })
        .collect()
}

#[test]
fn golden_picks_are_stable_across_the_schedule_seam() {
    // All cold: equal weights walk the aliases alternately.
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    assert_eq!(
        golden_trace(&candidates, &tracker, &rr, LatencyMode::Streaming),
        ALL_COLD,
        "all-cold picks changed"
    );

    // Mixed cold: the cold candidate probes at a quarter of the warm rate.
    let (candidates, tracker, rr) = setup(&[("warm", "m1"), ("cold", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    assert_eq!(
        golden_trace(&candidates, &tracker, &rr, LatencyMode::Streaming),
        MIXED_COLD,
        "cold-probe picks changed"
    );

    // Floored: a hard-failing candidate sits at the probe floor but is still
    // scheduled often enough to be sampled.
    let (candidates, tracker, rr) = setup(&[("good", "m1"), ("bad", "m2")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    for _ in 0..50 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(tracker.is_degraded(candidates[1].stats_index));
    assert!(!tracker
        .stats(candidates[1].stats_index)
        .is_cold(LatencyMode::Streaming));
    assert_eq!(
        golden_trace(&candidates, &tracker, &rr, LatencyMode::Streaming),
        FLOORED,
        "probe-floor picks changed"
    );

    // Degraded: two errors push it over the error threshold, but its share
    // stays proportional rather than collapsing to the floor.
    let (candidates, tracker, rr) = setup(&[("a", "m1"), ("b", "m2")]);
    for candidate in &candidates {
        tracker.record_success(
            candidate.stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
    }
    for _ in 0..2 {
        tracker.record_error(candidates[1].stats_index);
    }
    assert!(tracker.is_degraded(candidates[1].stats_index));
    assert_eq!(
        golden_trace(&candidates, &tracker, &rr, LatencyMode::Streaming),
        DEGRADED,
        "degraded-share picks changed"
    );
}

#[test]
fn weighted_schedule_reports_weight_and_normalised_share() {
    let (candidates, tracker, _rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );

    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);

    assert_eq!(schedule.entries.len(), 2);
    assert_eq!(schedule.total_weight, 1125);
    assert_eq!(schedule.entries[0].candidate.provider_name, "fast");
    assert_eq!(schedule.entries[0].weight, 1000);
    assert_eq!(schedule.entries[1].candidate.provider_name, "slow");
    assert_eq!(schedule.entries[1].weight, 125);
    assert!((schedule.entries[0].share - 1000.0 / 1125.0).abs() < 1e-12);
    assert!((schedule.entries[1].share - 125.0 / 1125.0).abs() < 1e-12);
    let share_sum: f64 = schedule.entries.iter().map(|entry| entry.share).sum();
    assert!((share_sum - 1.0).abs() < 1e-12);
}

#[test]
fn weighted_schedule_applies_cold_and_probe_floor_weights() {
    let (candidates, tracker, _rr) = setup(&[("warm", "m1"), ("cold", "m2"), ("failing", "m3")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[2].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    for _ in 0..50 {
        tracker.record_error(candidates[2].stats_index);
    }

    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);

    assert!(tracker
        .stats(candidates[1].stats_index)
        .is_cold(LatencyMode::Streaming));
    assert_eq!(schedule.entries[0].weight, 1000, "warm reference weight");
    assert_eq!(schedule.entries[1].weight, 250, "cold probe weight");
    assert_eq!(schedule.entries[2].weight, 10, "hard-failing weight floor");
    assert_eq!(schedule.total_weight, 1260);
    assert!(
        (schedule.entries[2].share - 10.0 / 1260.0).abs() < 1e-12,
        "the floored candidate must report its ~1% share"
    );
}

/// The weight and share of the worse candidate in a two-candidate alias whose
/// best candidate sits at 100ms: `failures` straight errors on top of one
/// success at `latency_ms`, at sharpness `k`. One success leaves the
/// success-rate EWMA at 1.0, so its effective latency is just `latency_ms`;
/// 50 failures clamp that rate to `MIN_SUCCESS_RATE`, i.e. 1000x the reference.
fn weight_and_share_of_worse_candidate(latency_ms: u64, failures: u32, k: f64) -> (usize, f64) {
    let (candidates, tracker, _rr) = setup(&[("best", "m1"), ("worse", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(latency_ms),
    );
    for _ in 0..failures {
        tracker.record_error(candidates[1].stats_index);
    }
    let schedule = weighted_schedule(&candidates, &tracker, k, LatencyMode::Streaming);
    (schedule.entries[1].weight, schedule.entries[1].share)
}

#[track_caller]
fn assert_row(actual: (usize, f64), weight: usize, share: f64) {
    let (got_weight, got_share) = actual;
    assert_eq!(got_weight, weight, "weight");
    assert!(
        (got_share - share).abs() < 1e-12,
        "share {got_share} != expected {share}"
    );
}

/// The share table in `README.md` and `config.example.toml`, row by row. Every
/// number in it is `round(1000 * (best / its_effective)^k)` floored at 10, over
/// the alias's total weight — floors included — so the docs cannot drift from
/// the arithmetic the router runs.
#[test]
fn documented_share_table_matches_the_weighting() {
    // 2x slower.
    assert_row(
        weight_and_share_of_worse_candidate(200, 0, EXPLOIT_K),
        125,
        125.0 / 1125.0,
    );
    assert_row(
        weight_and_share_of_worse_candidate(200, 0, 1.0),
        500,
        500.0 / 1500.0,
    );
    assert_row(weight_and_share_of_worse_candidate(200, 0, 0.0), 1000, 0.5);
    // 5x slower: at the default sharpness the raw 8 units are floored to 10.
    assert_row(
        weight_and_share_of_worse_candidate(500, 0, EXPLOIT_K),
        10,
        10.0 / 1010.0,
    );
    assert_row(
        weight_and_share_of_worse_candidate(500, 0, 1.0),
        200,
        200.0 / 1200.0,
    );
    assert_row(weight_and_share_of_worse_candidate(500, 0, 0.0), 1000, 0.5);
    // Hard-failing: effective latency clamps to 1000x the reference, so the
    // floor carries it at k >= 1 and k = 0 hands it half the alias anyway.
    assert_row(
        weight_and_share_of_worse_candidate(100, 50, EXPLOIT_K),
        10,
        10.0 / 1010.0,
    );
    assert_row(
        weight_and_share_of_worse_candidate(100, 50, 1.0),
        10,
        10.0 / 1010.0,
    );
    assert_row(weight_and_share_of_worse_candidate(100, 50, 0.0), 1000, 0.5);
}

/// At `k = 0` even the cold probe loses its success-rate scaling, since
/// `rate^0` is 1: an erroring cold candidate keeps the whole
/// `COLD_PROBE_WEIGHT` rather than backing off.
#[test]
fn zero_sharpness_flattens_the_cold_probe_too() {
    let (candidates, tracker, _rr) = setup(&[("best", "m1"), ("cold", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    for _ in 0..10 {
        tracker.record_error(candidates[1].stats_index);
    }

    assert_eq!(
        weighted_schedule(&candidates, &tracker, 0.0, LatencyMode::Streaming).entries[1].weight,
        250,
        "an erroring cold candidate must keep the full probe at k = 0"
    );
    assert!(
        weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming).entries[1]
            .weight
            < 250,
        "…while at the default sharpness it backs off"
    );
}

#[test]
fn select_candidate_draws_from_the_weighted_schedule() {
    let (candidates, tracker, rr) = setup(&[("fast", "m1"), ("slow", "m2")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );

    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    let mut schedule_rr = RoundRobinState::new();
    schedule_rr.register_alias("test".to_string());

    for _ in 0..128 {
        let picked = select_candidate(
            "test",
            &candidates,
            &tracker,
            &rr,
            EXPLOIT_K,
            LatencyMode::Streaming,
        )
        .unwrap();
        let scheduled =
            pick_from_schedule(&schedule, "test", &schedule_rr).expect("schedule is not empty");
        assert_eq!(picked.provider_name, scheduled.provider_name);
    }
}

// --- Reporting the schedule (`/status` and the routing debug line) ---------

#[test]
fn weighted_schedule_reports_the_effective_latency_it_weighted() {
    // The latency `/status` shows must be the one the weight was derived from,
    // so the schedule reports it instead of the caller recomputing the formula:
    // effective latency is the latency EWMA over the success-rate EWMA, so one
    // error at alpha 0.3 inflates a 200ms candidate to 200/0.7 ≈ 286ms while
    // its latency EWMA stays 200ms.
    let (candidates, tracker, _rr) = setup(&[("fast", "m1"), ("erroring", "m2"), ("cold", "m3")]);
    for _ in 0..2 {
        tracker.record_success(
            candidates[0].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(100),
        );
        tracker.record_success(
            candidates[1].stats_index,
            LatencyMode::Streaming,
            Duration::from_millis(200),
        );
    }
    tracker.record_error(candidates[1].stats_index);

    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);

    assert_eq!(schedule.entries[0].effective_latency_ms, Some(100.0));
    let erroring = schedule.entries[1]
        .effective_latency_ms
        .expect("a candidate with samples is warm");
    assert!(
        (erroring - 200.0 / (1.0 - 0.3)).abs() < 1e-9,
        "errors inflate effective latency: {erroring}"
    );
    assert_eq!(
        tracker
            .stats(candidates[1].stats_index)
            .ewma_ms(LatencyMode::Streaming),
        200,
        "…while the latency EWMA the endpoint shows next to it stays 200ms"
    );
    assert_eq!(schedule.entries[2].effective_latency_ms, None);
}

#[test]
fn shares_are_zero_rather_than_nan_for_an_empty_total() {
    // A zero total is unreachable through `weighted_schedule` — every entry
    // keeps at least the probe floor — but `/status` renders these numbers, so
    // the divide is guarded rather than trusted.
    assert!(
        !normalised_share(10, 0).is_nan(),
        "a NaN would reach the endpoint"
    );
    assert_eq!(normalised_share(10, 0), 0.0);
    assert_eq!(normalised_share(0, 0), 0.0);
    assert!((normalised_share(10, 1000) - 0.01).abs() < 1e-12);
}

#[test]
fn schedule_renders_every_share_for_the_debug_line() {
    let (candidates, tracker, _rr) = setup(&[("groq", "llama-3.3-70b"), ("openai", "gpt-4o-mini")]);
    tracker.record_success(
        candidates[0].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(100),
    );
    tracker.record_success(
        candidates[1].stats_index,
        LatencyMode::Streaming,
        Duration::from_millis(200),
    );

    let schedule = weighted_schedule(&candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    // 1000/1125 and 125/1125, i.e. 88.9% and 11.1%.
    assert_eq!(
        schedule.to_string(),
        "groq/llama-3.3-70b=88.9% openai/gpt-4o-mini=11.1%"
    );

    // An alias with no candidates renders empty; it never reaches a pick.
    let (no_candidates, tracker, _rr) = setup(&[]);
    let empty = weighted_schedule(&no_candidates, &tracker, EXPLOIT_K, LatencyMode::Streaming);
    assert_eq!(empty.to_string(), "");
}
