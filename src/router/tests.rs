use super::*;
use crate::model_map::ProviderKind;
use crate::tracker::{InFlightGuard, Tracker};
use hyper::header::HeaderValue;
use std::time::Duration;

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
    setup_window(specs, 10)
}

/// The same, with a chosen demotion window, so a test can ask for a
/// zero-length one.
fn setup_window(
    specs: &[(&str, usize)],
    demotion_secs: u64,
) -> (Vec<ResolvedCandidate>, Tracker, RoundRobinState) {
    let mut tracker = Tracker::new(0.3, demotion_secs);
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

/// The arithmetic those docs describe, on rows the test itself wrote down.
///
/// Every documented share is `weight / sum(weights)`, so a change to the
/// weighting fails here rather than leaving the docs describing arithmetic the
/// code no longer runs. The rows the docs actually print are checked by
/// `the_share_rows_in_the_readme_are_the_arithmetic_the_router_runs` and its
/// example-config twin, which read them out of the files; this test covers the
/// cases those tables do not list — three candidates, and a single one.
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

/// The share rows a docs file claims, as `(weights, documented shares)`.
///
/// `README.md` writes the table as Markdown and `config.example.toml` writes
/// the same four rows as comments; both are read here as the text an operator
/// reads, so the docs cannot drift from the router while the test keeps
/// passing on a private copy of the numbers.
fn documented_share_rows(file: &str) -> Vec<(Vec<usize>, Vec<String>)> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {} to check its share table: {e}",
            path.display()
        )
    });
    share_rows_in(&text)
}

/// Pick the share rows out of a docs file's text.
///
/// A row is two integers around a colon — `19 : 1` — followed by one
/// percentage per weight. Pipes and `#` are stripped so the Markdown table and
/// the commented table parse the same way, and a line with no percentages
/// (a header, a separator, prose about a 95% share) is not a row.
fn share_rows_in(text: &str) -> Vec<(Vec<usize>, Vec<String>)> {
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.replace(['|', '#'], " ");
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let Some(colon) = tokens.iter().position(|token| *token == ":") else {
            continue;
        };
        let first = colon
            .checked_sub(1)
            .and_then(|i| tokens[i].parse::<usize>().ok());
        let second = tokens
            .get(colon + 1)
            .and_then(|token| token.parse::<usize>().ok());
        let (Some(first), Some(second)) = (first, second) else {
            continue;
        };
        let shares: Vec<String> = tokens[colon + 2..]
            .iter()
            .filter(|token| token.ends_with('%'))
            .map(|token| (*token).to_string())
            .collect();
        if shares.is_empty() {
            continue;
        }
        rows.push((vec![first, second], shares));
    }
    rows
}

/// Recompute every documented row with [`placement_schedule`] and compare at
/// the precision the doc writes: `66.7%` is the arithmetic rounded to one
/// decimal, `50%` to none. A row that disagrees is a doc bug, so the message
/// names the file, the row, and the number the router actually gives.
fn assert_share_rows_match_the_weighting(rows: &[(Vec<usize>, Vec<String>)], file: &str) {
    let pairs: Vec<Vec<usize>> = rows.iter().map(|(weights, _)| weights.clone()).collect();
    assert_eq!(
        pairs,
        vec![vec![1, 1], vec![2, 1], vec![3, 1], vec![19, 1]],
        "{file} is expected to carry the share rows 1:1, 2:1, 3:1 and 19:1, in that order; \
         the test reads them as `w : w` followed by one percentage per weight"
    );

    for (weights, documented) in rows {
        let specs: Vec<(&str, usize)> = weights
            .iter()
            .enumerate()
            .map(|(i, weight)| (BOXES[i], *weight))
            .collect();
        let (candidates, _tracker, _rr) = setup(&specs);
        let schedule = placement_schedule(&candidates);
        assert_eq!(
            documented.len(),
            schedule.entries.len(),
            "{file} gives weights {weights:?} {} shares, but that alias has {} candidates",
            documented.len(),
            schedule.entries.len()
        );

        for (entry, doc) in schedule.entries.iter().zip(documented) {
            let decimals = doc
                .trim_end_matches('%')
                .split('.')
                .nth(1)
                .map_or(0, |part| part.len());
            let computed = format!("{:.1$}%", entry.share * 100.0, decimals);
            let share = entry.share * 100.0;
            let weight = entry.weight;
            let total = schedule.total_weight;
            assert_eq!(
                computed, *doc,
                "{file} documents weights {weights:?} as {documented:?}, but \
                 placement_schedule gives {share:.0$}% for weight {weight} of a \
                 total weight {total} — fix the doc, not this test",
                decimals
            );
        }
    }
}

#[test]
fn the_share_rows_in_the_readme_are_the_arithmetic_the_router_runs() {
    assert_share_rows_match_the_weighting(&documented_share_rows("README.md"), "README.md");
}

#[test]
fn the_share_rows_in_the_example_config_are_the_arithmetic_the_router_runs() {
    assert_share_rows_match_the_weighting(
        &documented_share_rows("config.example.toml"),
        "config.example.toml",
    );
}

#[test]
fn display_renders_the_share_line_the_debug_log_wants() {
    let (candidates, _tracker, _rr) = setup(&[("cuda30", 2), ("llama30", 1)]);
    let schedule = placement_schedule(&candidates);
    assert_eq!(schedule.to_string(), "cuda30/m=66.7% llama30/m=33.3%");
}

#[test]
fn the_load_line_names_every_candidate_s_weight_and_state() {
    // The debug line's `load=` field: every candidate of the alias, its
    // declared weight, its outstanding turns, and the busy/idle state the
    // placement rule acted on — the inputs, not just the answer.
    let (candidates, tracker, _rr) = setup(&[("cuda30", 2), ("llama30", 1)]);
    let schedule = placement_schedule(&candidates);
    assert_eq!(
        scheduled_load(&schedule, &tracker).to_string(),
        "cuda30/m=w2,t0,idle llama30/m=w1,t0,idle",
        "an idle alias reads idle everywhere"
    );

    // One prompt on the weight-2 box: the state the next placement reads.
    let guards = busy(&tracker, &candidates[0], 1);
    assert_eq!(
        scheduled_load(&schedule, &tracker).to_string(),
        "cuda30/m=w2,t1,busy llama30/m=w1,t0,idle"
    );

    // The turns come back with the guards, and the line follows.
    drop(guards);
    assert_eq!(
        scheduled_load(&schedule, &tracker).to_string(),
        "cuda30/m=w2,t0,idle llama30/m=w1,t0,idle"
    );
}

#[test]
fn the_load_line_carries_the_same_numbers_the_placement_read() {
    // The load field is not a second opinion: it reads the same counts
    // `place` compares, so the reason on the line and the load beside it are
    // one decision's inputs.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    let heavy = busy(&tracker, &candidates[0], 2);
    let placed = place(&candidates, "test", &rr, &tracker).unwrap();
    let schedule = placement_schedule(&candidates);
    let load = scheduled_load(&schedule, &tracker).to_string();

    // One box busy, one idle: the idle box takes the session, and the line
    // shows the turns that cost the heavy box its weight.
    assert_eq!(
        placed.reason,
        Placement::FreshByWeight,
        "an idle box existed: {placed:?}"
    );
    assert_eq!(placed.candidate.provider_name, "slow");
    assert!(
        load.contains("fast/m=w19,t2,busy"),
        "the turns the placement compared are on the line: {load}"
    );
    assert!(
        load.contains("slow/m=w1,t0,idle"),
        "and so is the box that won: {load}"
    );

    // Every box busy is the other reason, and the counts beside it are the
    // ones that decided it.
    let light = busy(&tracker, &candidates[1], 1);
    let placed = place(&candidates, "test", &rr, &tracker).unwrap();
    let load = scheduled_load(&schedule, &tracker).to_string();
    assert_eq!(
        placed.reason,
        Placement::OverflowBusy,
        "nothing was idle: {placed:?}"
    );
    assert!(
        load.contains("fast/m=w19,t2,busy") && load.contains("slow/m=w1,t1,busy"),
        "the least-busy comparison is on the line: {load}"
    );

    drop(heavy);
    drop(light);
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

// ---------------------------------------------------------------------------
// Eligibility: a demoted candidate takes no new sessions for its window. The
// window is a timer, so the tests move the tracker's clock rather than
// sleeping, and the share assertions are exact — a demoted box's share is
// zero, not small.
// ---------------------------------------------------------------------------

/// Put `candidate` in a demotion window, for as long as the tracker's clock
/// has not advanced past it.
fn demote(tracker: &Tracker, candidate: &ResolvedCandidate) {
    tracker.demote(candidate.stats_index);
}

#[test]
fn a_demoted_box_takes_no_placements_for_its_window() {
    // The weight-19 box is demoted. Its share during the window is not
    // reduced, it is zero: 1000 picks and not one of them lands on it.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    demote(&tracker, &candidates[0]);

    let counts = tally(&candidates, &tracker, &rr, 1000);
    assert_eq!(
        counts[0], 0,
        "a demoted candidate takes no new placements: {counts:?}"
    );
    assert_eq!(counts[1], 1000, "its share goes to the eligible box");
}

#[test]
fn a_demoted_box_loses_to_a_box_at_a_nineteenth_the_weight() {
    // Eligibility is a gate, not a discount: the box at weight 1 serves every
    // pick while the weight-19 box is demoted.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    demote(&tracker, &candidates[1]);

    for _ in 0..5 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(placed.candidate.provider_name, "fast");
        // An eligible box exists, so this is the ordinary case, not an overflow.
        assert_eq!(placed.reason, Placement::FreshByWeight);
    }
}

#[test]
fn a_demoted_light_box_keeps_no_probe_share() {
    // The point of "nothing probes it back": no slice of the alias is held
    // aside for the demoted box so its recovery can be noticed. Over 2000
    // picks the weight-1 demoted box gets exactly nothing, and the healthy
    // box gets all of it — not 19/20 of it.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    demote(&tracker, &candidates[1]);

    let counts = tally(&candidates, &tracker, &rr, 2000);
    assert_eq!(counts[1], 0, "no probe share is reserved: {counts:?}");
    assert_eq!(counts[0], 2000, "the eligible box takes the whole alias");
}

#[test]
fn a_demoted_box_is_fully_eligible_the_moment_its_window_ends() {
    // One window, one step back: zero placements right up to the deadline, the
    // full declared weight the instant it passes. No ramp, no probe traffic,
    // no recovery ladder — and nothing carried over from the demotion.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    demote(&tracker, &candidates[0]);

    assert_eq!(tally(&candidates, &tracker, &rr, 200), [0, 200]);

    tracker.clock().advance(Duration::from_secs(10));

    let counts = tally(&candidates, &tracker, &rr, 1000);
    let fast_share = counts[0] as f64 / 1000.0;
    assert!(
        (fast_share - 0.95).abs() < 0.02,
        "expected the full declared weight the moment the window ended, got {counts:?}"
    );
}

#[test]
fn a_window_that_has_not_expired_yet_is_not_a_ramp() {
    // The other side of the same rule: 9.999s in, the box is still at zero.
    // There is no partial eligibility to observe on the way to the deadline.
    let (candidates, tracker, rr) = setup(&[("fast", 19), ("slow", 1)]);
    demote(&tracker, &candidates[0]);

    tracker.clock().advance(Duration::from_millis(9_999));

    assert_eq!(tally(&candidates, &tracker, &rr, 200), [0, 200]);
}

#[test]
fn when_every_candidate_is_demoted_the_highest_weight_one_serves() {
    // Every box is down. The turn still goes somewhere: the heaviest demoted
    // box serves it so the caller sees that box's own answer rather than a
    // 502 the proxy invented.
    let (candidates, tracker, rr) = setup(&[("heavy", 5), ("light", 1)]);
    demote(&tracker, &candidates[0]);
    demote(&tracker, &candidates[1]);

    for _ in 0..4 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(
            placed.candidate.provider_name, "heavy",
            "the highest-weight demoted candidate serves the turn"
        );
        assert_eq!(placed.reason, Placement::OverflowDemoted);
    }
}

#[test]
fn the_all_demoted_fallback_ignores_busyness() {
    // Every box is demoted, so queueing behind one is not the thing being
    // weighed: the caller needs the box's own answer, and the heaviest box is
    // the one the alias is mostly made of. Busyness is a rule for choosing
    // between boxes that can take a session.
    let (candidates, tracker, rr) = setup(&[("heavy", 19), ("light", 1)]);
    let _guards = busy(&tracker, &candidates[0], 2);
    demote(&tracker, &candidates[0]);
    demote(&tracker, &candidates[1]);

    for _ in 0..3 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(placed.candidate.provider_name, "heavy");
        assert_eq!(placed.reason, Placement::OverflowDemoted);
    }
}

#[test]
fn equal_weight_demoted_boxes_take_turns_in_the_all_demoted_case() {
    // No RNG, same as every other placement: two equally heavy demoted boxes
    // alternate over the alias's ticks.
    let (candidates, tracker, rr) = setup(&[("a", 1), ("b", 1)]);
    demote(&tracker, &candidates[0]);
    demote(&tracker, &candidates[1]);

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
fn a_demoted_idle_box_does_not_make_a_busy_eligible_box_lose() {
    // Eligibility is applied before busyness: the idle box is demoted, so the
    // only box that can be asked is the busy one, and it is asked.
    let (candidates, tracker, rr) = setup(&[("demoted", 19), ("busy", 1)]);
    demote(&tracker, &candidates[0]);
    let _guards = busy(&tracker, &candidates[1], 1);

    for _ in 0..3 {
        let placed = place(&candidates, "test", &rr, &tracker).unwrap();
        assert_eq!(placed.candidate.provider_name, "busy");
        assert_eq!(placed.reason, Placement::OverflowBusy);
    }
}

#[test]
fn demotion_report_names_the_boxes_a_placement_skipped() {
    // The debug line's `demoted=` field: the boxes that were passed over, and
    // how much of their window is left. Whole seconds, rounded up.
    let (candidates, tracker, _rr) = setup(&[("a", 1), ("b", 1)]);
    assert_eq!(
        demotion_report(&candidates, &tracker),
        "",
        "a placement that skipped nothing says nothing"
    );

    demote(&tracker, &candidates[1]);
    assert_eq!(demotion_report(&candidates, &tracker), "b/m=10s");

    tracker.clock().advance(Duration::from_secs(1));
    assert_eq!(demotion_report(&candidates, &tracker), "b/m=9s");
}

#[test]
fn a_zero_length_window_leaves_the_alias_untouched() {
    // `routing.demotion_secs = 0` asks for no demotion at all, and gets it:
    // every window has ended before the next placement is asked, so the
    // declared split stands.
    let (candidates, tracker, rr) = setup_window(&[("fast", 19), ("slow", 1)], 0);
    for candidate in &candidates {
        tracker.demote(candidate.stats_index);
    }

    let counts = tally(&candidates, &tracker, &rr, 1000);
    let fast_share = counts[0] as f64 / 1000.0;
    assert!(
        (fast_share - 0.95).abs() < 0.02,
        "a zero window demotes nobody, so the declared split stands: {counts:?}"
    );
}
