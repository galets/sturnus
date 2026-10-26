use super::*;

/// Pin `key` under `alias` to `stats_index` at `now_ms`, and assert the table
/// agrees about where the session now is.
fn assert_pinned(
    table: &mut SessionTable,
    alias: &str,
    key: &str,
    stats_index: usize,
    now_ms: u64,
) {
    table.pin(alias, key, stats_index, now_ms);
    assert_eq!(
        table.lookup(alias, key, now_ms),
        Some(stats_index),
        "{alias}/{key} should be pinned to {stats_index}"
    );
}

// ---------------------------------------------------------------------------
// The header's two shapes.
// ---------------------------------------------------------------------------

#[test]
fn a_value_containing_a_slash_is_an_explicit_pin() {
    let value = HeaderValue::from_static("openai/gpt-4o-mini");
    assert_eq!(
        classify(Some(&value)),
        Some(Affinity::ExplicitPin {
            provider: "openai",
            model: "gpt-4o-mini"
        })
    );
}

/// The shape is the whole discrimination: an OpenCode session id has no slash,
/// so it is a key, and a `provider/model` has one, so it names a box.
#[test]
fn a_value_without_a_slash_is_a_session_key() {
    let value = HeaderValue::from_static("ses_01J9XQ2ZK4M8N7P3Q5R6T9VW");
    assert_eq!(
        classify(Some(&value)),
        Some(Affinity::SessionKey("ses_01J9XQ2ZK4M8N7P3Q5R6T9VW"))
    );
}

#[test]
fn an_absent_header_is_no_affinity() {
    assert_eq!(classify(None), None);
}

/// A blank header is not a session with an empty name: it is no header, and
/// the turn is placed.
#[test]
fn a_blank_value_is_no_affinity() {
    for raw in ["", " ", "   \t "] {
        let value = HeaderValue::from_str(raw).unwrap();
        assert_eq!(classify(Some(&value)), None, "blank value {raw:?}");
    }
}

/// A key is untrusted input that becomes a map key, so an over-long one buys
/// the caller a placement, not a table entry.
#[test]
fn an_over_long_session_key_is_no_affinity() {
    let key = "s".repeat(MAX_SESSION_KEY_BYTES);
    let value = HeaderValue::from_str(&key).unwrap();
    assert_eq!(
        classify(Some(&value)),
        Some(Affinity::SessionKey(&key[..])),
        "the bound is inclusive"
    );

    let too_long = "s".repeat(MAX_SESSION_KEY_BYTES + 1);
    let value = HeaderValue::from_str(&too_long).unwrap();
    assert_eq!(classify(Some(&value)), None, "one over the bound");
}

#[test]
fn a_header_that_is_not_printable_is_no_affinity() {
    let value = HeaderValue::from_bytes(b"ses_\xff\xfe").unwrap();
    assert_eq!(classify(Some(&value)), None);
}

/// The explicit shape wins on shape alone, whatever it names: an unusable pin
/// is handled by the caller (it falls back to placement), never by reading it
/// as a session key.
#[test]
fn the_slash_shape_is_checked_before_anything_else() {
    for raw in ["/model", "provider/", "a/b/c", " /x "] {
        let value = HeaderValue::from_str(raw).unwrap();
        let Some(Affinity::ExplicitPin { provider, model }) = classify(Some(&value)) else {
            panic!("{raw:?} should read as an explicit pin");
        };
        assert_eq!(format!("{provider}/{model}"), raw, "{raw:?}");
    }
}

// ---------------------------------------------------------------------------
// The table: one candidate per session, keyed per alias, bounded twice.
// ---------------------------------------------------------------------------

#[test]
fn a_key_never_seen_has_no_pin() {
    let mut table = SessionTable::default();
    assert_eq!(table.lookup("fast", "ses_new", 0), None);
    assert!(table.is_empty());
}

#[test]
fn a_pinned_session_gets_the_candidate_it_was_pinned_to() {
    let mut table = SessionTable::default();
    assert_pinned(&mut table, "fast", "ses_a", 3, 0);
    assert_eq!(table.lookup("fast", "ses_a", 1), Some(3));
    assert_eq!(table.len(), 1);
}

/// Pins are keyed per alias, so the same session on a second alias places
/// fresh, and the two pins cannot interfere with each other.
#[test]
fn the_same_key_on_two_aliases_pins_independently() {
    let mut table = SessionTable::default();
    assert_pinned(&mut table, "fast", "ses_a", 0, 0);
    // The second alias has never seen this session: fresh pick.
    assert_eq!(table.lookup("cheap", "ses_a", 0), None);
    assert_pinned(&mut table, "cheap", "ses_a", 1, 0);

    assert_eq!(table.lookup("fast", "ses_a", 0), Some(0), "fast's pin");
    assert_eq!(
        table.lookup("cheap", "ses_a", 0),
        Some(1),
        "cheap's pin is its own"
    );
    assert_eq!(table.len(), 2, "two sessions, one per alias");
}

/// Overflow re-pins: the table holds one candidate per session, so a re-pin
/// leaves nothing behind that could move the session back.
#[test]
fn a_re_pin_replaces_the_sessions_candidate_and_leaves_nothing_behind() {
    let mut table = SessionTable::default();
    assert_pinned(&mut table, "fast", "ses_a", 0, 0);

    table.pin("fast", "ses_a", 1, 10);

    assert_eq!(table.lookup("fast", "ses_a", 10), Some(1));
    assert_eq!(table.len(), 1, "one session, one candidate");
    assert_eq!(table.pinned_count(0), 0, "the box it left holds nothing");
    assert_eq!(table.pinned_count(1), 1);
}

#[test]
fn a_pin_that_has_gone_quiet_for_the_ttl_places_fresh() {
    let ttl_ms = DEFAULT_IDLE_TTL_SECS * 1_000;
    let mut table = SessionTable::default();
    table.pin("fast", "ses_a", 0, 0);

    // The TTL is inclusive: at exactly the bound the session is still itself.
    assert_eq!(table.lookup("fast", "ses_a", ttl_ms), Some(0));

    // One millisecond over, the entry is gone, and it leaves the table. A fresh
    // table, because the lookup above was a turn: it refreshed the session.
    let mut table = SessionTable::default();
    table.pin("fast", "ses_a", 0, 0);
    assert_eq!(table.lookup("fast", "ses_a", ttl_ms + 1), None);
    assert!(table.is_empty(), "an expired pin does not linger");
}

/// A turn is what keeps a session alive: the TTL measures quiet, not age.
#[test]
fn a_turn_refreshes_a_session_past_where_its_ttl_would_have_taken_it() {
    let ttl_ms = DEFAULT_IDLE_TTL_SECS * 1_000;
    let mut table = SessionTable::default();
    table.pin("fast", "ses_a", 0, 0);

    for turn in 1..20 {
        let now_ms = turn * (ttl_ms - 1);
        assert_eq!(
            table.lookup("fast", "ses_a", now_ms),
            Some(0),
            "a session that keeps talking outlives the TTL"
        );
    }
}

#[test]
fn the_cap_evicts_the_least_recently_used_session() {
    let mut table = SessionTable::new(2, 1800);
    assert_pinned(&mut table, "fast", "ses_1", 0, 0);
    assert_pinned(&mut table, "fast", "ses_2", 1, 0);
    assert_pinned(&mut table, "fast", "ses_3", 2, 0);

    assert_eq!(table.lookup("fast", "ses_1", 0), None, "the oldest is gone");
    assert_eq!(table.lookup("fast", "ses_2", 0), Some(1));
    assert_eq!(table.lookup("fast", "ses_3", 0), Some(2));
    assert_eq!(table.len(), 2, "the cap is a cap");
}

/// The LRU is honest: a turn moves a session out of the eviction queue, so the
/// session that is being used is the one that survives.
#[test]
fn a_hit_saves_a_session_from_the_eviction_queue() {
    let mut table = SessionTable::new(3, 1800);
    for (index, key) in ["ses_1", "ses_2", "ses_3"].iter().enumerate() {
        table.pin("fast", key, index, 0);
    }
    assert_eq!(table.lookup("fast", "ses_1", 1), Some(0), "touch ses_1");

    table.pin("fast", "ses_4", 3, 2);

    assert_eq!(
        table.lookup("fast", "ses_1", 2),
        Some(0),
        "touched, so kept"
    );
    assert_eq!(
        table.lookup("fast", "ses_2", 2),
        None,
        "the new oldest, evicted"
    );
    assert_eq!(table.lookup("fast", "ses_3", 2), Some(2));
    assert_eq!(table.lookup("fast", "ses_4", 2), Some(3));
}

/// The cap is the bound on a map whose keys are untrusted caller input: a
/// caller that invents keys cannot grow the table past the cap.
#[test]
fn the_table_stays_at_the_cap_under_a_stream_of_invented_keys() {
    let mut table = SessionTable::default();
    for index in 0..5_000usize {
        let now_ms = u64::try_from(index).unwrap();
        table.pin("fast", &format!("ses_{index}"), index % 3, now_ms);
    }

    assert_eq!(table.len(), DEFAULT_TABLE_LIMIT);
    assert_eq!(
        table.order.len(),
        table.pins.len(),
        "the LRU matches the map"
    );
    assert_eq!(
        table.lookup("fast", "ses_4999", 5_000),
        Some(4_999 % 3),
        "the newest key is always in"
    );
    assert_eq!(
        table.lookup("fast", "ses_0", 5_000),
        None,
        "the oldest is not"
    );
}

#[test]
fn a_zero_cap_pins_nothing() {
    let mut table = SessionTable::new(0, 1800);
    table.pin("fast", "ses_a", 0, 0);

    assert_eq!(table.lookup("fast", "ses_a", 0), None);
    assert!(table.is_empty());
}

/// A TTL of zero gives a pin no quiet to survive: the turn after the one that
/// made it places fresh.
#[test]
fn a_zero_ttl_gives_the_next_turn_a_fresh_pick() {
    let mut table = SessionTable::new(1024, 0);
    table.pin("fast", "ses_a", 0, 0);
    assert_eq!(
        table.lookup("fast", "ses_a", 0),
        Some(0),
        "the turn that made the pin is the pin"
    );

    assert_eq!(table.lookup("fast", "ses_a", 1), None);
}

#[test]
fn the_sweep_keeps_the_reported_size_at_the_live_sessions() {
    let ttl_ms = 1_800_000;
    let mut table = SessionTable::default();
    table.pin("fast", "ses_old", 0, 0);
    table.pin("fast", "ses_old2", 0, 0);
    assert_eq!(table.len(), 2);

    // The sweep runs on insert, from the front in: both are past the TTL.
    table.pin("fast", "ses_new", 1, ttl_ms + 1);

    assert_eq!(table.len(), 1, "expired sessions leave the reported size");
    assert_eq!(table.pinned_count(0), 0);
    assert_eq!(table.pinned_count(1), 1);
}

#[test]
fn pinned_count_counts_the_sessions_each_candidate_holds() {
    let mut table = SessionTable::default();
    table.pin("fast", "ses_a", 0, 0);
    table.pin("fast", "ses_b", 0, 10);
    table.pin("fast", "ses_c", 1, 20);

    assert_eq!(table.pinned_count(0), 2);
    assert_eq!(table.pinned_count(1), 1);
    assert_eq!(table.pinned_count(2), 0, "a candidate nothing is pinned to");
}

#[test]
fn the_defaults_are_the_numbers_the_docs_carry() {
    let table = SessionTable::default();
    assert_eq!(table.limit(), DEFAULT_TABLE_LIMIT);
    assert_eq!(table.idle_ttl_secs(), DEFAULT_IDLE_TTL_SECS);
    assert!(table.is_empty());
}
