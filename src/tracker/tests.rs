use super::*;

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

#[test]
fn degraded_when_error_ewma_breaches_threshold() {
    let mut tracker = Tracker::new(0.3, 0.5);
    let idx = tracker.register();
    assert!(!tracker.is_degraded(idx));

    // At alpha 0.3 the error EWMA is 0.3 after one error, 0.51 after two.
    tracker.record_error(idx);
    assert!(!tracker.is_degraded(idx));
    tracker.record_error(idx);
    assert!(tracker.is_degraded(idx));

    tracker.record_success(idx, LatencyMode::Streaming, Duration::from_millis(100));
    assert!(!tracker.is_degraded(idx));
}

#[test]
fn record_success_updates_ewma_for_correct_mode() {
    let mut tracker = Tracker::new(0.3, 0.5);
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
    let mut tracker = Tracker::new(0.3, 0.5);
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

    let mut tracker = Tracker::new(0.3, 0.5);
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
