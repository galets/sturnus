//! Session pins, end to end: one conversation sticks to one candidate, so the
//! conversation's cache stays warm.
//!
//! Every test here drives real HTTP at a real proxy listener over raw-socket
//! mock upstreams, exactly as `tests/demotion.rs` does, and reads the pin off
//! the two surfaces an operator and a client actually have: the
//! `x-session-affinity` header echoed on every response, and the session
//! counts on `/status`. The identity is the request header's *shape* — a value
//! containing `/` names a box outright, anything else is a session key — and
//! the shape is read from `x-session-affinity` or, when that name is absent,
//! from `x-session-id`, so the tests cover both names, which one wins when a
//! caller sends both, and what the routing decision logs about it.
//!
//! The rules under test are the ones the glossary states: a session key's
//! first turn is a **fresh pick** by weight and busyness, every later turn
//! rides the pin busy or not, pins are keyed per alias, and overflow
//! **re-pins** — a session pushed off a demoted box follows the new box
//! afterwards, and nothing moves it back when the old box's window expires.
//! The table is per instance, bounded by an idle TTL and an LRU cap; both are
//! crossed here through the injectable clock and a small configured cap rather
//! than by sleeping.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::session::SessionTable;
use sturnus::tracker::Tracker;

mod common;
use common::{capture_logs, wait_for_log, CapturedLogs};

/// How long a test waits for a count to land. Generous: these are local
/// sockets, and the point is to fail loudly rather than flake.
const SETTLE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Configs
// ---------------------------------------------------------------------------

/// Two candidates for the alias `fast` at the weights given, as a 19:1 pair so
/// weight alone always names `alpha` and every placement that is not alpha is
/// a placement the test caused.
fn alpha_and_beta(port_alpha: u16, port_beta: u16) -> String {
    format!(
        r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_alpha}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_beta}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "m-alpha", weight = 19 }},
  {{ provider = "beta", model = "m-beta", weight = 1 }},
]
"#
    )
}

/// The same two boxes at equal weight, so a run of fresh picks alternates
/// exactly and where each new session landed is arithmetic.
fn even_pair(port_alpha: u16, port_beta: u16) -> String {
    format!(
        r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_alpha}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_beta}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "m-alpha", weight = 1 }},
  {{ provider = "beta", model = "m-beta", weight = 1 }},
]
"#
    )
}

/// The same pair, seen through two aliases with the weights reversed: the
/// candidate a session holds is a property of the alias it was placed under.
fn two_aliases(port_alpha: u16, port_beta: u16) -> String {
    format!(
        r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_alpha}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_beta}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "m-alpha", weight = 19 }},
  {{ provider = "beta", model = "m-beta", weight = 1 }},
]
cheap = [
  {{ provider = "beta", model = "m-beta", weight = 19 }},
  {{ provider = "alpha", model = "m-alpha", weight = 1 }},
]
"#
    )
}

/// `AppState` as `main` builds it, with the table's bounds taken from the
/// config's `[routing]` table so a test that sets them exercises the plumbing.
fn state_for(config_toml: &str, client: reqwest::Client) -> Arc<AppState> {
    sturnus::init_crypto();
    let config: sturnus::config::Config = toml::from_str(config_toml).unwrap();

    let mut tracker = Tracker::new(config.routing.ewma_alpha, config.routing.demotion_secs);
    let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        sessions: Mutex::new(SessionTable::new(
            config.routing.session_table_limit,
            config.routing.session_ttl_secs,
        )),
        client,
        gcp_token_provider: None,
        budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
        metrics: Metrics::new(),
        shutting_down: AtomicBool::new(false),
        api_keys: None,
    })
}

fn start_server(state: Arc<AppState>) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = TcpListener::from_std(listener).unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(run_server(
        listener,
        state,
        async {
            let _ = rx.await;
        },
        Duration::from_secs(1),
    ));
    (addr, tx)
}

/// The table's bounds, appended to a config as a `[routing]` block.
fn with_routing(config_toml: &str, routing: &str) -> String {
    format!("{config_toml}\n[routing]\n{routing}\n")
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

fn caller_client() -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::new()
}

fn request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": []})
}

fn streaming_request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": [], "stream": true})
}

/// One turn through the proxy carrying `headers` as its affinity headers, and
/// returning its status, the box that served it, and the `x-session-affinity`
/// value echoed back.
///
/// The echo is the stateless half of affinity — a cooperating client can pin
/// across replicas with it and never touch this instance's table — so every
/// test here reads it, and a response that stopped echoing it fails.
///
/// The headers are passed through so a test can name the header it sends:
/// `x-session-affinity` is the name this proxy echoes, and `x-session-id` is
/// accepted in its place.
async fn turn_with(
    addr: std::net::SocketAddr,
    alias: &str,
    headers: &[(&str, &str)],
) -> (u16, String, String) {
    let mut req = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&request(alias));
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    let resp = req.send().await.unwrap();
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .map(|value| value.to_str().unwrap().to_string())
            .unwrap_or_default()
    };
    (
        resp.status().as_u16(),
        header("x-sturnus-provider"),
        header("x-session-affinity"),
    )
}

/// One turn under `x-session-affinity`, as a cooperating client sends it.
async fn turn(
    addr: std::net::SocketAddr,
    alias: &str,
    session: Option<&str>,
) -> (u16, String, String) {
    let headers: Vec<(&str, &str)> = session
        .map(|value| vec![("x-session-affinity", value)])
        .unwrap_or_default();
    turn_with(addr, alias, &headers).await
}

/// One turn carrying both affinity header names at once.
async fn turn_under_both(
    addr: std::net::SocketAddr,
    alias: &str,
    affinity: &str,
    session_id: &str,
) -> (u16, String, String) {
    turn_with(
        addr,
        alias,
        &[
            ("x-session-affinity", affinity),
            ("x-session-id", session_id),
        ],
    )
    .await
}

/// A streamed turn, returned as the response whose body is still arriving.
async fn streaming_turn(
    addr: std::net::SocketAddr,
    alias: &str,
    session: &str,
) -> (reqwest::Response, String) {
    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("x-session-affinity", session)
        .json(&streaming_request(alias))
        .send()
        .await
        .unwrap();
    let provider = resp
        .headers()
        .get("x-sturnus-provider")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    (resp, provider)
}

/// Read a streamed response to the end, and report whether it was cut short.
async fn drain<S>(stream: S) -> bool
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>>,
{
    tokio::pin!(stream);
    let mut failed = false;
    while let Some(next) = stream.next().await {
        if next.is_err() {
            failed = true;
            break;
        }
    }
    failed
}

// ---------------------------------------------------------------------------
// Mock upstreams
// ---------------------------------------------------------------------------

/// Mock upstream that answers every request immediately.
///
/// Each connection is served in a loop, because the proxy's client keeps a
/// connection alive and reuses it.
async fn eager_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                loop {
                    // `read_request` reports a peer that has closed as an
                    // empty header block, which ends this connection's loop.
                    let Ok(head) = common::read_request(&mut conn).await else {
                        return;
                    };
                    if head.is_empty() {
                        return;
                    }
                    common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
                }
            });
        }
    });
    (port, handle)
}

/// Mock upstream that hangs up on the first `hangups` requests — accepted,
/// read, closed with no response bytes, which is a transport failure the proxy
/// cannot relay — and answers every request after that. That is what makes the
/// overflow rule testable over real HTTP: the same box is unreachable long
/// enough to be demoted, and then able to answer again.
async fn hangup_then_status(hangups: usize) -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            let served = Arc::clone(&served);
            tokio::spawn(async move {
                loop {
                    let Ok(head) = common::read_request(&mut conn).await else {
                        return;
                    };
                    if head.is_empty() {
                        return;
                    }
                    if served.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < hangups {
                        conn.shutdown().await.ok();
                        return;
                    }
                    common::write_ok(&mut conn, r#"{"result":"the box's own"}"#).await;
                }
            });
        }
    });
    (port, handle)
}

/// Mock upstream that answers every request with an SSE stream and then holds
/// it open: one chunk, then it waits for a permit from `release` before
/// finishing the body. That hold is the window in which the candidate is
/// provably mid-prompt.
async fn held_sse_upstream() -> (u16, Arc<Semaphore>, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let release = Arc::new(Semaphore::new(0));
    let upstream_release = Arc::clone(&release);
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            let release = Arc::clone(&upstream_release);
            tokio::spawn(async move {
                if common::read_request(&mut conn).await.is_err() {
                    return;
                }
                conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .await
                .unwrap();
                conn.write_all(b"9\r\ndata: a\n\n\r\n").await.ok();
                conn.flush().await.ok();
                let Ok(permit) = release.acquire().await else {
                    return;
                };
                permit.forget();
                conn.write_all(b"9\r\ndata: z\n\n\r\n").await.ok();
                conn.write_all(b"0\r\n\r\n").await.ok();
                conn.flush().await.ok();
                conn.shutdown().await.ok();
            });
        }
    });
    (port, release, handle)
}

// ---------------------------------------------------------------------------
// Reading the table
// ---------------------------------------------------------------------------

/// `GET /status` parsed as JSON.
async fn status_body(addr: std::net::SocketAddr) -> serde_json::Value {
    let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

/// The `/status` row for `provider`.
async fn status_row(addr: std::net::SocketAddr, provider: &str) -> serde_json::Value {
    let body = status_body(addr).await;
    body["candidates"]
        .as_array()
        .expect("candidates array")
        .iter()
        .find(|row| row["provider"] == provider)
        .unwrap_or_else(|| panic!("no /status row for {provider}: {body}"))
        .clone()
}

/// The sessions pinned to `provider`, as `/status` reports it.
async fn sessions_pinned(addr: std::net::SocketAddr, provider: &str) -> usize {
    usize::try_from(
        status_row(addr, provider).await["sessions_pinned"]
            .as_u64()
            .unwrap(),
    )
    .unwrap()
}

/// The table's own block on `/status`: `(pinned, limit, idle_ttl_secs)`.
async fn sessions_block(addr: std::net::SocketAddr) -> (usize, usize, u64) {
    let body = status_body(addr).await;
    let sessions = &body["sessions"];
    (
        usize::try_from(sessions["pinned"].as_u64().unwrap()).unwrap(),
        usize::try_from(sessions["limit"].as_u64().unwrap()).unwrap(),
        sessions["idle_ttl_secs"].as_u64().unwrap(),
    )
}

/// The live in-flight count for `provider`, straight from the tracker.
fn in_flight(state: &AppState, provider: &str) -> usize {
    let (_, candidate) = state
        .model_map
        .iter()
        .find(|(_, candidate)| candidate.provider_name == provider)
        .unwrap();
    state.tracker.in_flight(candidate.stats_index)
}

/// Wait until the candidate's count is `want`, failing with what it is stuck on.
async fn wait_for_in_flight(state: &Arc<AppState>, provider: &str, want: usize) {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        if in_flight(state, provider) == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "in_flight({provider}) stuck at {}",
            in_flight(state, provider)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// A session key is placed once, then rides the pin
// ---------------------------------------------------------------------------

/// The ticket's headline: the first turn of a session key is a placement — here
/// the weight-preferred box — and every later turn of that key lands on the
/// same candidate. The response keeps echoing `provider/model`, so a
/// cooperating client can pin statelessly across replicas.
#[tokio::test]
async fn a_session_key_is_placed_once_and_every_later_turn_rides_the_pin() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    for turn_number in 1..=4 {
        let (status, provider, echo) = turn(addr, "fast", Some("ses_steady")).await;
        assert_eq!(status, 200, "turn {turn_number} was served");
        assert_eq!(
            (provider.as_str(), echo.as_str()),
            ("alpha", "alpha/m-alpha"),
            "turn {turn_number}: placed on alpha by weight, and pinned there"
        );
    }

    // The pin is visible where the routing decision is: one session, on one box.
    assert_eq!(sessions_pinned(addr, "alpha").await, 1);
    assert_eq!(sessions_pinned(addr, "beta").await, 0);
    assert_eq!(sessions_block(addr).await, (1, 1024, 1800));

    alpha_up.abort();
    beta_up.abort();
}

/// A pinned turn is not moved by busyness: it queues behind the running prompt
/// on its own box, because re-prefilling a long conversation on the other box
/// is the expensive outcome and queueing behind your own prompt is the cheap
/// one. The turn is streamed so its headers arrive while the first prompt is
/// still held.
#[tokio::test]
async fn a_pinned_turn_stays_on_its_box_while_that_box_is_mid_prompt() {
    let (alpha_port, release, alpha_up) = held_sse_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // The session's first turn: placed on alpha by weight, and held mid-prompt.
    let (first, provider) = streaming_turn(addr, "fast", "ses_midprompt").await;
    assert_eq!(provider, "alpha");
    let mut first_stream = first.bytes_stream();
    first_stream.next().await.unwrap().unwrap();
    wait_for_in_flight(&state, "alpha", 1).await;

    // The same session, next turn. beta is idle and alpha is busy, and the pin
    // outranks that.
    let (second, provider) = streaming_turn(addr, "fast", "ses_midprompt").await;
    assert_eq!(
        provider, "alpha",
        "a pinned turn does not move for busyness"
    );
    let mut second_stream = second.bytes_stream();
    let chunk = second_stream.next().await.unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&chunk).contains("data: a"),
        "the queued turn starts being answered: {chunk:?}"
    );
    assert_eq!(
        in_flight(&state, "alpha"),
        2,
        "both of the session's turns are queued on its own box"
    );
    assert_eq!(in_flight(&state, "beta"), 0, "beta was never touched");

    release.add_permits(2);
    assert!(!drain(first_stream).await, "the held stream ends cleanly");
    assert!(!drain(second_stream).await, "the queued turn ends cleanly");

    alpha_up.abort();
    beta_up.abort();
}

/// Two distinct session keys over a two-candidate alias place independently per
/// weight — at equal weights the fresh picks alternate exactly — and each key
/// sticks for its own life.
#[tokio::test]
async fn two_session_keys_place_independently_per_weight_and_each_sticks() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&even_pair(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // Four fresh picks over a 1:1 alias: the declared split, 2 and 2.
    let keys = ["ses_one", "ses_two", "ses_three", "ses_four"];
    let mut placed = Vec::new();
    for key in keys {
        placed.push(turn(addr, "fast", Some(key)).await.1);
    }
    assert_eq!(
        placed,
        vec!["alpha", "beta", "alpha", "beta"],
        "equal weights alternate, one session per pick"
    );

    // Each key keeps its own box for the rest of its life.
    for (index, key) in keys.into_iter().enumerate() {
        for turn_number in 2..=4 {
            let (_, provider, _) = turn(addr, "fast", Some(key)).await;
            assert_eq!(
                provider, placed[index],
                "{key} turn {turn_number} stays on the box that took its first turn"
            );
        }
    }

    assert_eq!(sessions_pinned(addr, "alpha").await, 2);
    assert_eq!(sessions_pinned(addr, "beta").await, 2);
    assert_eq!(sessions_block(addr).await.0, 4);

    alpha_up.abort();
    beta_up.abort();
}

/// A new session key gets a fresh pick: a second key is not dragged onto the
/// first key's candidate. The proof is busyness — a fresh pick goes where it
/// can start now, which is not the box the first session is holding mid-prompt.
#[tokio::test]
async fn a_new_session_key_is_a_fresh_pick_not_the_first_keys_box() {
    let (alpha_port, release, alpha_up) = held_sse_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // A parent session, pinned to alpha by weight, running a prompt there.
    let (parent, provider) = streaming_turn(addr, "fast", "ses_parent").await;
    assert_eq!(provider, "alpha");
    let mut parent_stream = parent.bytes_stream();
    parent_stream.next().await.unwrap().unwrap();
    wait_for_in_flight(&state, "alpha", 1).await;

    // A subagent's conversation is a separate session key, so it picks fresh,
    // independently of the session it descends from.
    let (status, provider, echo) = turn(addr, "fast", Some("ses_subagent")).await;
    assert_eq!(status, 200);
    assert_eq!(
        (provider.as_str(), echo.as_str()),
        ("beta", "beta/m-beta"),
        "the new key goes to the box it can start on, not to its parent's"
    );

    release.add_permits(1);
    assert!(
        !drain(parent_stream).await,
        "the parent's prompt ends cleanly"
    );

    alpha_up.abort();
    beta_up.abort();
}

/// Pins are keyed per alias: the same session key on a second alias places
/// fresh there, and the two pins do not interfere.
#[tokio::test]
async fn the_same_session_key_on_a_second_alias_places_fresh_and_the_pins_do_not_interfere() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&two_aliases(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // `fast` is mostly alpha, `cheap` is mostly beta: the same key, two boxes.
    let (status, provider, _) = turn(addr, "fast", Some("ses_both")).await;
    assert_eq!((status, provider.as_str()), (200, "alpha"));
    let (status, provider, echo) = turn(addr, "cheap", Some("ses_both")).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "the second alias has never seen this session, so it places fresh"
    );

    // Each alias keeps its own pin, turn after turn.
    for turn_number in 2..=4 {
        let (_, provider, _) = turn(addr, "fast", Some("ses_both")).await;
        assert_eq!(provider, "alpha", "fast turn {turn_number}");
        let (_, provider, _) = turn(addr, "cheap", Some("ses_both")).await;
        assert_eq!(provider, "beta", "cheap turn {turn_number}");
    }

    // Two sessions in the table, one per alias, and neither box holds both.
    assert_eq!(sessions_block(addr).await.0, 2);
    assert_eq!(sessions_pinned(addr, "alpha").await, 1);
    assert_eq!(sessions_pinned(addr, "beta").await, 1);

    alpha_up.abort();
    beta_up.abort();
}

// ---------------------------------------------------------------------------
// The explicit shape, and the header that is no instruction
// ---------------------------------------------------------------------------

/// A value containing `/` is an explicit `provider/model` pin, checked before
/// the session table, exactly as today: it wins over the box the session is
/// pinned to, and it does not move the session's own pin.
#[tokio::test]
async fn an_explicit_pin_is_checked_before_the_session_table_and_leaves_the_pin_alone() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // The session is pinned to alpha by weight.
    let (_, provider, _) = turn(addr, "fast", Some("ses_steady")).await;
    assert_eq!(provider, "alpha");

    // The same header name, explicit shape: the caller names beta, and gets it,
    // over the session's pin and over the weight.
    let (status, provider, echo) = turn(addr, "fast", Some("beta/m-beta")).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "an explicit pin is honored, and it is not a session key"
    );

    // The session is still alpha's: an explicit turn is a request's instruction,
    // not a session's claim, so it re-pins nothing.
    for turn_number in 2..=4 {
        let (_, provider, echo) = turn(addr, "fast", Some("ses_steady")).await;
        assert_eq!(
            (provider.as_str(), echo.as_str()),
            ("alpha", "alpha/m-alpha"),
            "turn {turn_number}: the session's own pin survived the explicit turn"
        );
    }

    // One session in the table, on alpha. The explicit turn added nothing.
    assert_eq!(sessions_block(addr).await.0, 1);
    assert_eq!(sessions_pinned(addr, "alpha").await, 1);
    assert_eq!(sessions_pinned(addr, "beta").await, 0);

    alpha_up.abort();
    beta_up.abort();
}

/// A malformed, unusable or absent header falls back to placement, as today:
/// the alias's weight-preferred box serves, and no session is pinned.
#[tokio::test]
async fn a_malformed_unusable_or_absent_header_falls_back_to_placement() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // Every one of these is no instruction: the turn is placed, and weight
    // (19:1) always names alpha.
    let long_key = "s".repeat(300);
    let cases: Vec<Option<&str>> = vec![
        None,
        Some(""),
        Some("   "),
        Some("gamma/m-gamma"),
        Some("/m-alpha"),
        Some("alpha/"),
        Some(long_key.as_str()),
    ];
    for case in &cases {
        let label = case.unwrap_or("<absent>");
        let (status, provider, echo) = turn(addr, "fast", *case).await;
        assert_eq!(
            (status, provider.as_str()),
            (200, "alpha"),
            "{label:?} should fall through to placement"
        );
        // The echo is the box that served, whatever the caller sent.
        assert_eq!(echo, "alpha/m-alpha", "{label:?}");
    }

    // None of them pinned a session: an anonymous turn is not a claim on a box.
    assert_eq!(sessions_block(addr).await.0, 0);
    assert_eq!(sessions_pinned(addr, "alpha").await, 0);

    alpha_up.abort();
    beta_up.abort();
}

// ---------------------------------------------------------------------------
// `x-session-id` in place of `x-session-affinity`
// ---------------------------------------------------------------------------

/// A caller that carries its conversation id under `x-session-id` gets the same
/// affinity as one that sends it under `x-session-affinity`: the first turn is a
/// fresh pick, every later turn rides the pin, and the response keeps echoing
/// `x-session-affinity` — the name this proxy answers with.
#[tokio::test]
async fn an_x_session_id_header_pins_the_session_like_the_affinity_header() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    for turn_number in 1..=4 {
        let (status, provider, echo) =
            turn_with(addr, "fast", &[("x-session-id", "ses_by_id")]).await;
        assert_eq!(status, 200, "turn {turn_number} was served");
        assert_eq!(
            (provider.as_str(), echo.as_str()),
            ("alpha", "alpha/m-alpha"),
            "turn {turn_number}: placed on alpha by weight, and pinned there"
        );
    }

    // One session, on one box, from a header the pinning tests never sent.
    assert_eq!(sessions_pinned(addr, "alpha").await, 1);
    assert_eq!(sessions_pinned(addr, "beta").await, 0);
    assert_eq!(sessions_block(addr).await.0, 1);

    alpha_up.abort();
    beta_up.abort();
}

/// The fallback is a drop-in, so its other shape is a drop-in too: a
/// `provider/model` under `x-session-id` names a box outright, is honored over
/// a session's own pin, and pins nothing of its own.
#[tokio::test]
async fn an_x_session_id_naming_a_box_is_an_explicit_pin() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // The session is pinned to alpha by weight, under the primary name.
    let (_, provider, _) = turn(addr, "fast", Some("ses_steady")).await;
    assert_eq!(provider, "alpha");

    // `x-session-id` carrying the explicit shape: the caller names beta, and
    // gets it, over the session's pin and over the weight.
    let (status, provider, echo) =
        turn_with(addr, "fast", &[("x-session-id", "beta/m-beta")]).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "the fallback header carries an explicit pin as well as a session key"
    );

    // The session is still alpha's: an explicit turn re-pins nothing, and the
    // box named under the fallback name is not a session.
    let (_, provider, _) = turn(addr, "fast", Some("ses_steady")).await;
    assert_eq!(provider, "alpha");
    assert_eq!(
        sessions_block(addr).await.0,
        1,
        "only the session's own pin"
    );
    assert_eq!(sessions_pinned(addr, "beta").await, 0);

    alpha_up.abort();
    beta_up.abort();
}

/// Both names present: `x-session-affinity` is the one read, and the
/// `x-session-id` beside it is not consulted at all — it pins nothing, and
/// every turn keeps going where the header this proxy answers with says.
#[tokio::test]
async fn the_affinity_header_is_read_in_preference_to_a_session_id() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // The affinity header names beta outright; the session id beside it asks
    // for a pin, which is never looked at.
    for turn_number in 1..=3 {
        let (status, provider, echo) =
            turn_under_both(addr, "fast", "beta/m-beta", "ses_ignored").await;
        assert_eq!(
            (status, provider.as_str(), echo.as_str()),
            (200, "beta", "beta/m-beta"),
            "turn {turn_number}: the primary header decides, alone"
        );
    }

    // The ignored name pinned nothing, so the table is empty.
    assert_eq!(sessions_block(addr).await.0, 0);
    assert_eq!(sessions_pinned(addr, "beta").await, 0);

    alpha_up.abort();
    beta_up.abort();
}

/// A `x-session-id` with no usable value is no instruction, exactly as an
/// unusable `x-session-affinity` value: the turn is placed and nothing pins.
#[tokio::test]
async fn an_unusable_x_session_id_falls_back_to_placement() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    let long_key = "s".repeat(300);
    for case in ["", "   ", long_key.as_str()] {
        let (status, provider, echo) = turn_with(addr, "fast", &[("x-session-id", case)]).await;
        assert_eq!(
            (status, provider.as_str()),
            (200, "alpha"),
            "{case:?} should fall through to placement"
        );
        assert_eq!(echo, "alpha/m-alpha", "{case:?}");
    }

    assert_eq!(sessions_block(addr).await.0, 0);

    alpha_up.abort();
    beta_up.abort();
}

// ---------------------------------------------------------------------------
// Overflow re-pins
// ---------------------------------------------------------------------------

/// Overflow re-pins: when the pinned candidate is demoted the turn goes to the
/// next placement and that becomes the session's candidate, and no turn returns
/// to the old candidate when its demotion window expires. Nobody can say
/// whether the cache it left is still there, so the migration is the new state
/// of affairs.
#[tokio::test]
async fn overflow_on_demotion_re_pins_and_nothing_moves_the_session_back() {
    let (alpha_port, alpha_up) = hangup_then_status(1).await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // Turn 1: placed on alpha by weight, and alpha hangs up — a transport
    // failure, so alpha's window starts. The session is pinned to alpha.
    let (status, _, _) = turn(addr, "fast", Some("ses_overflow")).await;
    assert_eq!(
        status, 502,
        "the box the session was placed on never answered"
    );
    assert_eq!(
        status_row(addr, "alpha").await["demotion_secs_left"],
        10,
        "alpha is standing down"
    );

    // Turn 2: the pinned box is demoted, so this turn is placed, and beta —
    // at a nineteenth the weight — takes it. The echo says beta, because beta
    // is the session's box now.
    let (status, provider, echo) = turn(addr, "fast", Some("ses_overflow")).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "overflow serves, and re-pins"
    );

    // Turn 3: the session follows the new box.
    let (status, provider, _) = turn(addr, "fast", Some("ses_overflow")).await;
    assert_eq!((status, provider.as_str()), (200, "beta"));

    // Cross the end of alpha's window without waiting for it. alpha is fully
    // eligible again at its full declared weight — and the session stays on
    // beta, which is what "a migration is a new state of affairs" means.
    state.tracker.clock().advance(Duration::from_secs(10));
    assert!(
        status_row(addr, "alpha").await["demotion_secs_left"].is_null(),
        "alpha's window is over"
    );
    for turn_number in 4..=6 {
        let (status, provider, echo) = turn(addr, "fast", Some("ses_overflow")).await;
        assert_eq!(
            (status, provider.as_str(), echo.as_str()),
            (200, "beta", "beta/m-beta"),
            "turn {turn_number}: nothing moves the session back to alpha"
        );
    }

    // A new session key, placed after the window, goes to alpha at full weight:
    // the re-pin is the session's, not the alias's.
    let (status, provider, _) = turn(addr, "fast", Some("ses_new")).await;
    assert_eq!(
        (status, provider.as_str()),
        (200, "alpha"),
        "the alias is untouched by one session's migration"
    );

    assert_eq!(
        sessions_pinned(addr, "beta").await,
        1,
        "the migrated session"
    );
    assert_eq!(sessions_pinned(addr, "alpha").await, 1, "the new one");
    assert_eq!(sessions_block(addr).await.0, 2, "one candidate per session");

    alpha_up.abort();
    beta_up.abort();
}

// ---------------------------------------------------------------------------
// The table is bounded: an idle TTL and an LRU cap
// ---------------------------------------------------------------------------

/// A session that has been quiet for `routing.session_ttl_secs` places fresh,
/// and leaves the table. The clock is the injectable one, so the TTL is crossed
/// here by advancing it rather than by sleeping.
#[tokio::test]
async fn a_session_quiet_past_the_idle_ttl_places_fresh() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(
        &with_routing(&even_pair(alpha_port, beta_port), "session_ttl_secs = 30"),
        caller_client(),
    );
    let (addr, _shutdown) = start_server(state.clone());

    // The first turn is alpha's (the first pick of a 1:1 alias), and it pins.
    let (_, provider, _) = turn(addr, "fast", Some("ses_quiet")).await;
    assert_eq!(provider, "alpha");
    assert_eq!(sessions_pinned(addr, "alpha").await, 1);
    assert_eq!(
        sessions_block(addr).await,
        (1, 1024, 30),
        "the configured TTL"
    );

    // 31 seconds of quiet: the pin is gone, so this turn is a fresh pick — and
    // the fresh pick is beta's, which is what a pin that survived would not be.
    state.tracker.clock().advance(Duration::from_secs(31));
    let (status, provider, echo) = turn(addr, "fast", Some("ses_quiet")).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "an expired session places fresh"
    );

    // The expired entry left the table, and the fresh pick is in it.
    assert_eq!(sessions_block(addr).await.0, 1);
    assert_eq!(sessions_pinned(addr, "alpha").await, 0);
    assert_eq!(sessions_pinned(addr, "beta").await, 1);

    alpha_up.abort();
    beta_up.abort();
}

/// A session evicted by `routing.session_table_limit` places fresh, and the cap
/// is the bound on a table whose keys are caller input.
#[tokio::test]
async fn a_session_evicted_at_the_lru_cap_places_fresh() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(
        &with_routing(
            &even_pair(alpha_port, beta_port),
            "session_table_limit = 2\nsession_ttl_secs = 1800",
        ),
        caller_client(),
    );
    let (addr, _shutdown) = start_server(state);

    // Three keys into a table of two: the first is evicted, and the cap holds.
    let (_, first, _) = turn(addr, "fast", Some("ses_1")).await;
    let (_, second, _) = turn(addr, "fast", Some("ses_2")).await;
    let (_, third, _) = turn(addr, "fast", Some("ses_3")).await;
    assert_eq!(
        (first.as_str(), second.as_str(), third.as_str()),
        ("alpha", "beta", "alpha"),
        "three fresh picks over a 1:1 alias"
    );
    assert_eq!(sessions_block(addr).await, (2, 2, 1800), "the cap is a cap");

    // ses_1 is out of the table, so its next turn places fresh — and it lands on
    // beta, the box it was not pinned to.
    let (status, provider, echo) = turn(addr, "fast", Some("ses_1")).await;
    assert_eq!(
        (status, provider.as_str(), echo.as_str()),
        (200, "beta", "beta/m-beta"),
        "an evicted session places fresh"
    );

    // The two sessions still in the table keep their own boxes: the eviction
    // took the least recently used, and a turn is what keeps a session in line.
    let (_, provider, _) = turn(addr, "fast", Some("ses_3")).await;
    assert_eq!(provider, "alpha", "ses_3 is still pinned");
    let (_, provider, _) = turn(addr, "fast", Some("ses_2")).await;
    assert_eq!(provider, "alpha", "ses_2 was evicted, so it placed fresh");

    assert_eq!(sessions_block(addr).await.0, 2, "the cap held throughout");
    assert_eq!(sessions_pinned(addr, "alpha").await, 2);
    assert_eq!(sessions_pinned(addr, "beta").await, 0);

    alpha_up.abort();
    beta_up.abort();
}

/// `/status` reports the table's size and the number of sessions pinned to each
/// candidate, from the same table the request path consults.
#[tokio::test]
async fn status_reports_the_table_size_and_who_is_pinned_to_what() {
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&even_pair(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    // Three sessions: two on alpha, one on beta — the alternating picks a 1:1
    // alias makes.
    let mut placed = Vec::new();
    for key in ["ses_one", "ses_two", "ses_three"] {
        placed.push(turn(addr, "fast", Some(key)).await.1);
    }
    assert_eq!(placed, vec!["alpha", "beta", "alpha"]);

    let alpha = status_row(addr, "alpha").await;
    let beta = status_row(addr, "beta").await;
    assert_eq!(alpha["sessions_pinned"], 2, "{alpha}");
    assert_eq!(beta["sessions_pinned"], 1, "{beta}");
    assert_eq!(sessions_block(addr).await, (3, 1024, 1800));

    // A pin is a session's claim, not a turn's: the counts survive a turn.
    let (_, provider, _) = turn(addr, "fast", Some("ses_one")).await;
    assert_eq!(provider, "alpha");
    assert_eq!(
        sessions_block(addr).await.0,
        3,
        "a pinned turn adds no session"
    );

    alpha_up.abort();
    beta_up.abort();
}

// ---------------------------------------------------------------------------
// The pin is auditable
// ---------------------------------------------------------------------------

/// A session key's first turn logs the placement it made, and every later turn
/// logs `reason="pinned"` beside the key the pin is stored under — so a pinned
/// turn is never mistaken for a weighted decision, and "why this box for this
/// conversation?" is answerable from the log.
#[tokio::test(flavor = "current_thread")]
async fn a_session_key_logs_the_fresh_pick_then_the_pin() {
    let logs = CapturedLogs::default();
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    capture_logs(logs.clone(), async move {
        assert_eq!(turn(addr, "fast", Some("ses_auditable")).await.1, "alpha");
        assert_eq!(turn(addr, "fast", Some("ses_auditable")).await.1, "alpha");

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="pinned""#)
        })
        .await;

        let fresh = text
            .lines()
            .find(|line| line.contains(r#"reason="fresh-by-weight""#))
            .unwrap_or_else(|| panic!("no fresh-by-weight line in:\n{text}"));
        assert!(
            fresh.contains(r#"session="ses_auditable""#),
            "the pick that made the pin names the session: {fresh}"
        );

        let pinned = text
            .lines()
            .find(|line| line.contains(r#"reason="pinned""#))
            .unwrap_or_else(|| panic!("no pinned line in:\n{text}"));
        assert!(
            pinned.contains(r#"session="ses_auditable""#) && pinned.contains("provider=alpha"),
            "the pinned turn names the session and its box: {pinned}"
        );
        assert!(
            !pinned.contains(r#"reason="fresh-by-weight""#),
            "a pinned turn is not a weighted placement:\n{text}"
        );
    })
    .await;

    alpha_up.abort();
    beta_up.abort();
}

/// A turn with no session key logs no session, so the field reads as it did
/// before session pins existed.
#[tokio::test(flavor = "current_thread")]
async fn a_turn_with_no_session_key_logs_no_session() {
    let logs = CapturedLogs::default();
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    capture_logs(logs.clone(), async move {
        assert_eq!(turn(addr, "fast", None).await.1, "alpha");

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains("routing decision")
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains("routing decision"))
            .unwrap_or_else(|| panic!("no routing decision line in:\n{text}"));
        assert!(
            line.contains(r#"session="""#),
            "an anonymous turn names no session: {line}"
        );
    })
    .await;

    alpha_up.abort();
    beta_up.abort();
}

/// The routing decision names the header it read, so "which of my headers is
/// this proxy listening to?" is answerable from the same line that answers
/// "why this box?": the name it used, `x-session-id` when only that one was
/// present, and nothing when the request carried neither.
#[tokio::test(flavor = "current_thread")]
async fn the_routing_decision_logs_which_affinity_header_it_read() {
    let logs = CapturedLogs::default();
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    capture_logs(logs.clone(), async move {
        turn_with(addr, "fast", &[("x-session-affinity", "ses_log_primary")]).await;
        turn_with(addr, "fast", &[("x-session-id", "ses_log_fallback")]).await;
        turn_under_both(addr, "fast", "beta/m-beta", "ses_log_both").await;
        turn_with(addr, "fast", &[]).await;

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"affinity_header="" session="""#)
        })
        .await;

        let line_with = |needle: &str| {
            text.lines()
                .find(|line| line.contains("routing decision") && line.contains(needle))
                .unwrap_or_else(|| panic!("no routing decision line for {needle:?} in:\n{text}"))
                .to_string()
        };

        let primary = line_with(r#"session="ses_log_primary""#);
        assert!(
            primary.contains(r#"affinity_header="x-session-affinity""#),
            "the primary name names itself: {primary}"
        );

        let fallback = line_with(r#"session="ses_log_fallback""#);
        assert!(
            fallback.contains(r#"affinity_header="x-session-id""#),
            "the fallback name names itself: {fallback}"
        );

        // Both names present: the line says the primary one was the one read,
        // and the session id beside it is not in the line at all.
        let both = line_with(r#"reason="explicit-pin""#);
        assert!(
            both.contains(r#"affinity_header="x-session-affinity""#)
                && both.contains(r#"session="""#),
            "the primary name wins, and the ignored session id logs nothing: {both}"
        );

        // The anonymous turn is the line that names no header and no session.
        let anonymous = line_with(r#"affinity_header="" session="""#);
        assert!(
            anonymous.contains(r#"affinity_header="""#),
            "a turn that carried neither header names no header: {anonymous}"
        );
    })
    .await;

    alpha_up.abort();
    beta_up.abort();
}

/// A header that carries no usable instruction still names itself in the log,
/// and the reason on that line is the placement it fell through to: the field
/// says what was read, the reason says what was decided.
#[tokio::test(flavor = "current_thread")]
async fn an_unusable_header_names_itself_beside_the_placement_it_fell_through_to() {
    let logs = CapturedLogs::default();
    let (alpha_port, alpha_up) = eager_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state);

    capture_logs(logs.clone(), async move {
        // Blank under the fallback name: no session, so the turn is placed, and
        // weight (19:1) names alpha.
        assert_eq!(
            turn_with(addr, "fast", &[("x-session-id", "   ")]).await.1,
            "alpha"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains("routing decision")
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains("routing decision"))
            .unwrap_or_else(|| panic!("no routing decision line in:\n{text}"));
        assert!(
            line.contains(r#"affinity_header="x-session-id""#)
                && line.contains(r#"reason="fresh-by-weight""#)
                && line.contains(r#"session="""#),
            "the unusable header names itself beside the placement it fell to: {line}"
        );
    })
    .await;

    alpha_up.abort();
    beta_up.abort();
}
