use super::*;
use crate::model_map::ProviderKind;
use crate::tracker::{InFlightGuard, Tracker};
use hyper::header::HeaderValue;

fn make_candidate(
    provider: &str,
    model: &str,
    weight: usize,
    stats_index: usize,
) -> ResolvedCandidate {
    ResolvedCandidate {
        provider_name: provider.to_string(),
        model: model.to_string(),
        base_url: "http://localhost".to_string(),
        api_key: None,
        kind: ProviderKind::ApiKey,
        stats_index,
        provider_header: HeaderValue::from_str(provider).unwrap(),
        affinity_header: HeaderValue::try_from(format!("{provider}/{model}")).unwrap(),
        weight,
        attribution_labels: None,
        extra_headers: Default::default(),
    }
}

/// Candidates in config order, each with its own stats slot, all idle.
fn setup(specs: &[(&str, usize)]) -> (Vec<ResolvedCandidate>, Tracker, RoundRobinState) {
    let mut tracker = Tracker::new(0.3, 0.5);
    let mut rr = RoundRobinState::new();
    rr.register_alias("test".to_string());
    let candidates: Vec<_> = specs
        .iter()
        .map(|(provider, weight)| {
            let stats_index = tracker.register();
            make_candidate(provider, "m", *weight, stats_index)
        })
        .collect();
    (candidates, tracker, rr)
}

/// The alias's current tick, read without advancing it.
fn tick(rr: &RoundRobinState) -> usize {
    rr.counters
        .get("test")
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0)
}

/// Count where a run of `n` consecutive picks lands, one slot per candidate.
fn tally(
    candidates: &[ResolvedCandidate],
    tracker: &Tracker,
    rr: &RoundRobinState,
    n: usize,
) -> Vec<usize> {
    let mut counts = vec![0; candidates.len()];
    for _ in 0..n {
        let placed = place(candidates, "test", rr, tracker).unwrap();
        let slot = candidates
            .iter()
            .position(|c| c.stats_index == placed.candidate.stats_index)
            .unwrap();
        counts[slot] += 1;
    }
    counts
}

/// Put `turns` prompts on `candidate`, for as long as the returned guards live.
fn busy(tracker: &Tracker, candidate: &ResolvedCandidate, turns: usize) -> Vec<InFlightGuard> {
    (0..turns)
        .map(|_| tracker.dispatch(candidate.stats_index))
        .collect()
}

#[test]
fn empty_candidates_returns_none() {
    let candidates: Vec<ResolvedCandidate> = vec![];
    let (_candidates, tracker, rr) = setup(&[]);
    assert!(place(&candidates, "test", &rr, &tracker).is_none());
    // The empty case consumes no tick: the alias's counter is untouched, so a
    // misconfigured alias cannot shift the spread of an alias that does work.
    assert_eq!(tick(&rr), 0);
}

#[test]
fn equal_weights_alternate_exactly() {
    let (candidates, tracker, rr) = setup(&[("a", 1), ("b", 1)]);

    let picks: Vec<&str> = (0..4)
        .map(|_| {
            place(&candidates, "test", &rr, &tracker)
                .unwrap()
                .candidate
                .provider_name
                .as_str()
        })
        .collect();
    assert_eq!(picks, vec!["a", "b", "a", "b"]);
}

#[test]
fn a_two_to_one_pair_splits_two_to_one() {
    // The share is the declared ratio, so the split over a long run of picks
    // is 2:1. The Weyl stride is low-discrepancy, not exact per window: over
    // 1000 picks the golden sequence stays well within a point of the ratio,
    // so the tolerance here is generous and the assertion is about the policy.
    let (candidates, tracker, rr) = setup(&[("fast", 2), ("slow", 1)]);
    let counts = tally(&candidates, &tracker, &rr, 1000);

    assert_eq!(counts.len(), 2);
    let fast_share = counts[0] as f64 / 1000.0;
    assert!(
        (fast_share - 2.0 / 3.0).abs() < 0.02,
        "expected ~66.7% on the weight-2 candidate, got {counts:?}"
    );
}

#[test]
fn a_nineteen_to_one_pair_is_the_ninety_five_percent_split_it_declares() {
    // The point of declared weights: a 95% share is written, not discovered.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    let counts = tally(&candidates, &tracker, &rr, 2000);

    let fast_share = counts[0] as f64 / 2000.0;
    assert!(
        (fast_share - 0.95).abs() < 0.02,
        "expected ~95% on the weight-19 candidate, got {counts:?}"
    );
}

#[test]
fn a_single_candidate_gets_everything() {
    let (candidates, tracker, rr) = setup(&[("only", 1)]);
    for _ in 0..5 {
        assert_eq!(
            place(&candidates, "test", &rr, &tracker)
                .unwrap()
                .candidate
                .provider_name,
            "only"
        );
    }
}

#[test]
fn the_reason_for_a_placement_onto_an_idle_box_is_fresh_by_weight() {
    let (candidates, tracker, rr) = setup(&[("a", 1), ("b", 1)]);
    assert_eq!(
        place(&candidates, "test", &rr, &tracker).unwrap().reason,
        Placement::FreshByWeight
    );
}

#[test]
fn the_schedule_reports_declared_weight_as_a_share() {
    let (candidates, _tracker, _rr) = setup(&[("a", 1), ("b", 1), ("c", 2)]);
    let schedule = placement_schedule(&candidates);

    assert_eq!(schedule.total_weight, 4);
    assert_eq!(schedule.entries[0].weight, 1);
    assert_eq!(schedule.entries[0].share, 0.25);
    assert_eq!(schedule.entries[1].share, 0.25);
    assert_eq!(schedule.entries[2].share, 0.5);
}

#[test]
fn the_schedule_is_pure_and_advances_nothing() {
    // `/status` renders this schedule on every scrape. It must not consume a
    // tick, or the endpoint would perturb the routing it claims to report.
    let (candidates, tracker, rr) = setup(&[("a", 1), ("b", 1)]);
    for _ in 0..8 {
        let _ = placement_schedule(&candidates);
    }
    assert_eq!(tick(&rr), 0);
    let first = place(&candidates, "test", &rr, &tracker).unwrap();
    assert_eq!(first.candidate.provider_name, "a");
}

const BOXES: [&str; 3] = ["a", "b", "c"];

/// The share table in `README.md` and `config.example.toml`, row by row. Every
/// number in those docs is `weight / sum(weights)`, so a change to the
/// weighting that moves one of them fails here rather than leaving the docs
/// describing arithmetic the code no longer runs.
#[test]
fn documented_share_table_matches_the_weighting() {
    let cases: &[(&[usize], &[f64])] = &[
        (&[1, 1], &[0.5, 0.5]),
        (&[2, 1], &[2.0 / 3.0, 1.0 / 3.0]),
        (&[3, 1], &[0.75, 0.25]),
        (&[19, 1], &[0.95, 0.05]),
        (&[1, 1, 2], &[0.25, 0.25, 0.5]),
        (&[1], &[1.0]),
    ];

    for (weights, expected) in cases {
        let specs: Vec<(&str, usize)> = weights
            .iter()
            .enumerate()
            .map(|(i, w)| (BOXES[i], *w))
            .collect();
        let (candidates, _tracker, _rr) = setup(&specs);
        let schedule = placement_schedule(&candidates);
        let shares: Vec<f64> = schedule.entries.iter().map(|e| e.share).collect();
        assert_eq!(&shares, expected, "weights {weights:?}");
    }
}

#[test]
fn display_renders_the_share_line_the_debug_log_wants() {
    let (candidates, _tracker, _rr) = setup(&[("cuda30", 2), ("llama30", 1)]);
    let schedule = placement_schedule(&candidates);
    assert_eq!(schedule.to_string(), "cuda30/m=66.7% llama30/m=33.3%");
}

#[test]
fn a_zero_total_weight_reports_zero_share_not_nan() {
    // Unreachable through config (weights are validated >= 1), but these
    // numbers are rendered, and a NaN reaching an operator is worse than 0.0.
    assert_eq!(normalised_share(0, 0), 0.0);
    assert_eq!(normalised_share(3, 6), 0.5);
}

// ---------------------------------------------------------------------------
// Busyness is a placement rule: it decides who is asked, never what a weight
// is worth.
// ---------------------------------------------------------------------------

#[test]
fn a_busy_box_loses_to_an_idle_one_even_at_weight_19() {
    // The rule in one line: the fast box running somebody's prompt does not
    // get a new session the slow box could start right now.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    let _guards = busy(&tracker, &candidates[0], 1);

    for _ in 0..5 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(placed.candidate.provider_name, "slow");
        // An idle box exists, so this is the ordinary case, not an overflow.
        assert_eq!(placed.reason, Placement::FreshByWeight);
    }
}

#[test]
fn a_finished_turn_leaves_its_box_idle_again() {
    // Busyness is the outstanding-turn count, so a stream that has ended is
    // idle: the weight gets its box back as soon as the prompt is over.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    let guards = busy(&tracker, &candidates[0], 1);
    assert_eq!(
        place(&candidates, "test", &rr, &tracker)
            .unwrap()
            .candidate
            .provider_name,
        "slow"
    );

    drop(guards);

    let counts = tally(&candidates, &tracker, &rr, 1000);
    let fast_share = counts[0] as f64 / 1000.0;
    assert!(
        (fast_share - 0.95).abs() < 0.02,
        "expected the weight back after the turn ended, got {counts:?}"
    );
}

#[test]
fn when_every_box_is_busy_the_least_busy_wins() {
    let (candidates, tracker, rr) = setup(&[("fast", 5), ("slow", 1)]);
    let _fast_guards = busy(&tracker, &candidates[0], 2);
    let _slow_guards = busy(&tracker, &candidates[1], 1);

    for _ in 0..4 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(
            placed.candidate.provider_name, "slow",
            "the box with one prompt beats the box with two, at a fifth the weight"
        );
        assert_eq!(placed.reason, Placement::OverflowBusy);
    }
}

#[test]
fn weight_breaks_a_tie_in_busyness() {
    // Both boxes are equally busy, so nothing but the declared weight is left
    // to decide, and it decides 19:1.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    let _g1 = busy(&tracker, &candidates[0], 1);
    let _g2 = busy(&tracker, &candidates[1], 1);

    let counts = tally(&candidates, &tracker, &rr, 2000);
    let fast_share = counts[0] as f64 / 2000.0;
    assert!(
        (fast_share - 0.95).abs() < 0.02,
        "expected weight to break the busyness tie 19:1, got {counts:?}"
    );
}

#[test]
fn equal_weight_and_equal_busyness_spread_deterministically() {
    // No RNG anywhere: the same run of ticks gives the same run of picks.
    let (candidates, tracker, rr) = setup(&[("a", 1), ("b", 1)]);
    let _g1 = busy(&tracker, &candidates[0], 1);
    let _g2 = busy(&tracker, &candidates[1], 1);

    let picks: Vec<&str> = (0..4)
        .map(|_| {
            place(&candidates, "test", &rr, &tracker)
                .unwrap()
                .candidate
                .provider_name
                .as_str()
        })
        .collect();
    assert_eq!(picks, vec!["a", "b", "a", "b"]);
}

#[test]
fn a_box_holding_one_more_turn_than_the_rest_is_not_the_least_busy() {
    // Three candidates, loads 0/1/1: the idle one takes everything.
    let (candidates, tracker, rr) = setup(&[("idle", 1), ("busy", 1), ("busier", 1)]);
    let _g1 = busy(&tracker, &candidates[1], 1);
    let _g2 = busy(&tracker, &candidates[2], 1);

    for _ in 0..3 {
        assert_eq!(
            place(&candidates, "test", &rr, &tracker)
                .unwrap()
                .candidate
                .provider_name,
            "idle"
        );
    }
}
