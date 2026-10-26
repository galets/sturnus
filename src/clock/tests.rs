use super::*;

#[test]
fn a_clock_reads_milliseconds_since_it_started_and_never_goes_back() {
    let clock = Clock::new();
    let first = clock.now_ms();
    for _ in 0..1_000 {
        assert!(clock.now_ms() >= first);
    }
}

/// The whole reason the offset exists: a test crosses the end of a demotion
/// window by moving the reading, not by waiting for it.
#[test]
fn advancing_moves_the_reading_forward_without_waiting() {
    let clock = Clock::new();
    let before = clock.now_ms();

    clock.advance(Duration::from_secs(10));

    assert!(
        clock.now_ms() >= before + 10_000,
        "a 10s advance should move the reading by at least 10_000ms"
    );
}

#[test]
fn advances_accumulate() {
    let clock = Clock::new();
    let before = clock.now_ms();

    for _ in 0..4 {
        clock.advance(Duration::from_millis(250));
    }

    assert!(clock.now_ms() >= before + 1_000);
}
