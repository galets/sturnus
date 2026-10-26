use super::*;

/// The window left, in milliseconds, and `0` when the candidate is eligible.
///
/// Read with a tolerance in these tests: the clock is real, so a reading taken
/// a microsecond after a demotion is a microsecond short of the full window.
fn remaining_ms(tracker: &Tracker, index: usize) -> u64 {
    tracker.demotion_remaining(index).map_or(0, |left| {
        u64::try_from(left.as_millis()).unwrap_or(u64::MAX)
    })
}

/// Assert the window left is `want`, allowing for the real milliseconds that
/// elapse between two readings of a real clock.
fn assert_window_left(got: u64, want: u64) {
    assert!(
        (want - 25..=want).contains(&got),
        "expected {want}ms of window (give or take a tick), got {got}ms"
    );
}

#[test]
fn ewma_first_observation_sets_directly() {
    let stats = CandidateStats::new();
    assert!(stats.is_cold(LatencyMode::Streaming));
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert!(!stats.is_cold(LatencyMode::Streaming));
}

#[test]
fn ewma_blends_subsequent_observations() {
    let stats = CandidateStats::new();
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    stats.update_ewma(LatencyMode::Streaming, 200, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 130);
}

#[test]
fn ewma_converges_toward_constant_signal() {
    let stats = CandidateStats::new();
    for _ in 0..50 {
        stats.update_ewma(LatencyMode::Streaming, 500, 0.3);
    }
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 500);
}

#[test]
fn streaming_and_nonstreaming_ewmas_are_independent() {
    let stats = CandidateStats::new();
    stats.update_ewma(LatencyMode::Streaming, 100, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert!(stats.is_cold(LatencyMode::NonStreaming));

    stats.update_ewma(LatencyMode::NonStreaming, 5_000, 0.3);
    assert_eq!(stats.ewma_ms(LatencyMode::Streaming), 100);
    assert_eq!(stats.ewma_ms(LatencyMode::NonStreaming), 5_000);
}

#[test]
fn success_rate_starts_optimistic() {
    let stats = CandidateStats::new();
    assert!((stats.success_rate() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn success_rate_blends_like_an_ewma() {
    let stats = CandidateStats::new();
    stats.record_outcome(false, 0.3);
    assert!((stats.success_rate() - 0.7).abs() < 1e-12);
    stats.record_outcome(true, 0.3);
    assert!((stats.success_rate() - 0.79).abs() < 1e-12);
}

/// A transport failure starts a window of the configured length, and the
/// window is the whole health model: it ends on the clock, in one step.
#[test]
fn a_transport_failure_demotes_for_the_configured_window() {
    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();
    assert!(
        !tracker.is_demoted(idx),
        "a box is eligible before anything happens"
    );

    let window = tracker.demote(idx);
    assert_eq!(
        window,
        Duration::from_secs(10),
        "the window is what the config declared"
    );
    assert!(tracker.is_demoted(idx), "the failure demotes the box");
    assert_window_left(remaining_ms(&tracker, idx), 10_000);

    // Time passes, the window ticks down, and nothing about the box changes.
    tracker.clock().advance(Duration::from_secs(4));
    assert!(tracker.is_demoted(idx));
    assert_window_left(remaining_ms(&tracker, idx), 6_000);

    // The last millisecond of the window is the whole recovery: no ramp, no
    // probe share, no ladder to climb.
    tracker.clock().advance(Duration::from_millis(6_000));
    assert!(
        !tracker.is_demoted(idx),
        "the window is over, so the box is eligible at full weight again"
    );
    assert_eq!(tracker.demotion_remaining(idx), None);
}

/// A demotion is a timer, not a feedback loop: it records nothing against the
/// box, so there is no state for the next placement to carry.
#[test]
fn a_demotion_carries_no_state_into_the_next_placement() {
    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();

    tracker.demote(idx);
    tracker.clock().advance(Duration::from_secs(10));

    assert_eq!(
        tracker.stats(idx).success_rate(),
        1.0,
        "a demotion must not touch the success-rate EWMA: the box's own \
         outcomes are the only thing that moves it"
    );
    assert_eq!(
        tracker.in_flight(idx),
        0,
        "a demotion must not touch the outstanding-turn count"
    );
    assert!(
        tracker.stats(idx).is_cold(LatencyMode::Streaming),
        "a demotion must not touch the latency EWMA"
    );
}

/// A failure that lands while a window is already running restarts it from
/// now: the deadline only ever moves forward, so a window can never be
/// shortened by the arrival of another failure.
#[test]
fn a_second_failure_restarts_the_window_rather_than_stacking_it() {
    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();

    tracker.demote(idx);
    tracker.clock().advance(Duration::from_secs(6));
    assert_window_left(remaining_ms(&tracker, idx), 4_000);

    // One window per box, not a count of failures: the second failure restarts
    // the same window rather than stacking a second one on it.
    tracker.demote(idx);
    assert_window_left(remaining_ms(&tracker, idx), 10_000);
}

/// `routing.demotion_secs = 0` is the one way to ask for no demotion at all:
/// the window is zero long, so the box is eligible the instant it is demoted.
#[test]
fn a_zero_length_window_demotes_for_no_time() {
    let mut tracker = Tracker::new(0.3, 0);
    let idx = tracker.register();

    tracker.demote(idx);

    assert!(
        !tracker.is_demoted(idx),
        "a window of 0 seconds has already ended"
    );
    assert_eq!(tracker.demotion_remaining(idx), None);
}

/// The success-rate EWMA keeps being recorded, because `/status` still reports
/// it: the operator notices a box that has started failing by reading it.
#[test]
fn the_success_rate_ewma_is_still_recorded() {
    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();

    tracker.record_error(idx);
    tracker.record_error(idx);

    assert!(
        1.0 - tracker.stats(idx).success_rate() > 0.5,
        "two failures should push the error-rate EWMA past a half"
    );
    assert!(
        !tracker.is_demoted(idx),
        "and none of that demotes the box: only a transport failure does"
    );
}

#[test]
fn record_success_updates_ewma_for_correct_mode() {
    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();
    tracker.record_success(idx, LatencyMode::Streaming, Duration::from_millis(150));
    assert_eq!(tracker.stats(idx).ewma_ms(LatencyMode::Streaming), 150);
    assert!(tracker.stats(idx).is_cold(LatencyMode::NonStreaming));
    assert!((tracker.stats(idx).success_rate() - 1.0).abs() < f64::EPSILON);
}

/// The whole contract of a dispatch in one place: counting starts at
/// `dispatch`, one guard returns exactly one count, and a candidate's count
/// says nothing about any other candidate's.
#[test]
fn dispatch_counts_until_its_guard_is_dropped() {
    let mut tracker = Tracker::new(0.3, 10);
    let busy = tracker.register();
    let idle = tracker.register();
    assert_eq!(tracker.in_flight(busy), 0, "nothing is in flight yet");

    let first = tracker.dispatch(busy);
    assert_eq!(tracker.in_flight(busy), 1, "dispatch counts the request");
    assert_eq!(
        first.in_flight(),
        1,
        "the guard sees its own request counted"
    );

    let second = tracker.dispatch(busy);
    assert_eq!(
        tracker.in_flight(busy),
        2,
        "concurrent requests count separately"
    );
    assert_eq!(tracker.in_flight(idle), 0, "the count is per candidate");

    drop(second);
    assert_eq!(tracker.in_flight(busy), 1, "one guard, one count");
    drop(first);
    assert_eq!(
        tracker.in_flight(busy),
        0,
        "the last guard empties the slot"
    );
    assert_eq!(
        tracker.in_flight(idle),
        0,
        "the idle candidate was never touched"
    );
}

/// A guard dropped twice is impossible (it is not `Clone` and `Drop` runs
/// once), but the count is only correct if concurrent dispatches also neither
/// lose an increment nor an decrement. 8 threads hold a shifting window of
/// guards, so increments and releases genuinely interleave.
#[test]
fn concurrent_dispatch_and_release_conserve_the_count() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 2_000;

    let mut tracker = Tracker::new(0.3, 10);
    let idx = tracker.register();
    let tracker = Arc::new(tracker);
    let start = Arc::new(std::sync::Barrier::new(THREADS));

    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let tracker = Arc::clone(&tracker);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                // Deliberately leaves guards alive while other threads are
                // still dispatching and dropping.
                let mut held = Vec::new();
                for _ in 0..ROUNDS {
                    held.push(tracker.dispatch(idx));
                    if held.len() == 4 {
                        // Returning one guard mid-loop is the point: releases
                        // interleave with other threads' dispatches.
                        drop(held.remove(0));
                    }
                }
                held.len()
            })
        })
        .collect();

    let still_held: usize = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .sum();
    // Every guard was moved into `held`, and `held` is dropped when its thread
    // ends, which the joins above have waited for.
    assert_eq!(
        tracker.in_flight(idx),
        0,
        "{still_held} guards were handed back to their threads; none may leak"
    );
}
