//! Observability of the routing decision: the declared weights `/status`
//! reports, and the debug lines that make a routed request readable next to a
//! pinned one.
//!
//! The numbers asserted here are the declared weights and the shares they
//! imply, from `src/router.rs`:
//!
//! ```text
//! share = weight / sum(weights)
//! ```
//!
//! Nothing measured enters that arithmetic. The second test here is the one
//! that proves it: a candidate that is warmed, then fails fifty times in a
//! row, keeps exactly the share its weight declares.
//!
//! The last section is the demotion window in that same line: the box a
//! placement skipped and the time left on its window, and the reason a turn
//! went to a demoted box because the alias had nothing else. The window itself
//! — a transport failure, zero placements, full weight again at its end — is
//! `tests/demotion.rs`.
//!
//! The section after that is the same decision read from the other three
//! surfaces: the load every candidate was in when the placement was made
//! (`load=` on the debug line), the numbers `/status` reports for a box that
//! has carried real traffic, the placement-reason counter, and the
//! sessions-pinned gauge. The rule they hold the code to is that the number an
//! operator reads is the number the placement path used, so the endpoint, the
//! gauge and the router cannot drift apart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Semaphore};

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::{self, Placement, RoundRobinState};
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::session::SessionTable;
use sturnus::tracker::{secs_left, LatencyMode, Tracker};

mod common;
use common::{capture_logs, wait_for_log, CapturedLogs};

/// One alias, two candidates at declared weights 2 and 1. The upstream
/// addresses are only used by the tests that actually proxy something.
const TWO_CANDIDATES: &str = r#"
[provider.fast]
base_url = "http://127.0.0.1:9"
api_key = "fake"

[provider.slow]
base_url = "http://127.0.0.1:9"
api_key = "fake"

[model]
test = [
  { provider = "fast", model = "m-fast", weight = 2 },
  { provider = "slow", model = "m-slow", weight = 1 },
]
"#;

fn state_from_config(config_toml: &str) -> Arc<AppState> {
    sturnus::init_crypto();
    let config: sturnus::config::Config = toml::from_str(config_toml).unwrap();

    let mut tracker = Tracker::new(0.3, 10);
    let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    let metrics = Metrics::new();
    // As `main` does: every candidate's series exists at `0` from startup, so
    // "nothing happened" and "nobody looked" are distinguishable on `/metrics`.
    let triples: Vec<(&str, &str, &str)> = model_map
        .iter()
        .map(|(alias, c)| (alias, c.provider_name.as_str(), c.model.as_str()))
        .collect();
    metrics.init_zero(&triples);

    Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        sessions: Mutex::new(SessionTable::default()),
        client: reqwest::Client::new(),
        gcp_token_provider: None,
        budget: BufferBudget::new(128 * 1024, 32 * 1024 * 1024),
        metrics,
        shutting_down: AtomicBool::new(false),
        api_keys: None,
    })
}

/// Serve `state` until the returned sender is dropped or fired.
async fn start_server(state: Arc<AppState>) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(run_server(
        listener,
        state,
        async move {
            let _ = rx.await;
        },
        Duration::from_secs(1),
    ));
    (addr, tx)
}

/// Warm both latency EWMAs of `provider` to `ms` (one sample at alpha 0.3
/// leaves the success-rate EWMA at 1.0).
fn warm(state: &AppState, provider: &str, ms: u64) {
    let candidate = state
        .model_map
        .get("test")
        .unwrap()
        .iter()
        .find(|c| c.provider_name == provider)
        .unwrap();
    for mode in [LatencyMode::Streaming, LatencyMode::NonStreaming] {
        state
            .tracker
            .record_success(candidate.stats_index, mode, Duration::from_millis(ms));
    }
}

/// `times` failures and no successes against `provider`.
fn fail(state: &AppState, provider: &str, times: usize) {
    let candidate = state
        .model_map
        .get("test")
        .unwrap()
        .iter()
        .find(|c| c.provider_name == provider)
        .unwrap();
    for _ in 0..times {
        state.tracker.record_error(candidate.stats_index);
    }
}

/// `GET /status` parsed as JSON.
async fn status_body(state: Arc<AppState>) -> serde_json::Value {
    let (addr, _shutdown) = start_server(state).await;
    let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.starts_with("application/json"),
        "expected JSON, got {content_type}"
    );
    resp.json().await.unwrap()
}

/// The `/status` row for `provider`.
fn row<'a>(body: &'a serde_json::Value, provider: &str) -> &'a serde_json::Value {
    body["candidates"]
        .as_array()
        .expect("candidates array")
        .iter()
        .find(|row| row["provider"] == provider)
        .unwrap_or_else(|| panic!("no /status row for {provider}: {body}"))
}

fn share(row: &serde_json::Value) -> f64 {
    row["routing"]["share"].as_f64().unwrap()
}

/// The declared weight is what the row reports, and the share is that weight
/// over the alias's total.
#[tokio::test]
async fn status_reports_declared_weight_and_the_share_it_buys() {
    let state = state_from_config(TWO_CANDIDATES);

    let body = status_body(state).await;

    let fast = row(&body, "fast");
    let slow = row(&body, "slow");
    assert_eq!(fast["alias"], "test");
    assert_eq!(fast["model"], "m-fast");

    assert_eq!(fast["routing"]["weight"], 2);
    assert_eq!(slow["routing"]["weight"], 1);
    assert!((share(fast) - 2.0 / 3.0).abs() < 1e-9, "fast share: {fast}");
    assert!((share(slow) - 1.0 / 3.0).abs() < 1e-9, "slow share: {slow}");
    assert!(
        (share(fast) + share(slow) - 1.0).abs() < 1e-9,
        "shares must be normalised"
    );

    // Live load is reported beside the health fields, not as a schedule number.
    assert_eq!(fast["in_flight"], 0, "nothing is in flight: {fast}");
}

/// The measured allocator is gone: warming a candidate to 100ms, warming its
/// twin to 400ms, and then failing the twin fifty times moves no share at all.
///
/// This is the non-vacuity test for "the declared weight is the sole
/// allocator". Under the arithmetic this design deleted, the same inputs moved
/// the slow candidate's share from 33.3% to ~1% (latency) and then to the
/// probe floor (errors); here the numbers cannot budge.
#[tokio::test]
async fn measured_latency_and_errors_do_not_move_the_share() {
    let state = state_from_config(TWO_CANDIDATES);
    let before = status_body(state.clone()).await;
    let (fast_before, slow_before) = (share(row(&before, "fast")), share(row(&before, "slow")));

    warm(&state, "fast", 100);
    warm(&state, "slow", 400);
    fail(&state, "slow", 50);

    let after = status_body(state).await;
    let (fast_after, slow_after) = (share(row(&after, "fast")), share(row(&after, "slow")));

    assert_eq!(
        fast_before, fast_after,
        "a declared share moved because latency did"
    );
    assert_eq!(
        slow_before, slow_after,
        "a declared share moved because errors did"
    );
    assert!((fast_after - 2.0 / 3.0).abs() < 1e-9);

    // The measurements are still reported — the operator can see the box that
    // got slow, which is the detection this design hands back to a human.
    let slow = row(&after, "slow");
    assert_eq!(slow["observed"]["streaming_ewma_ms"], 400);
    assert!(slow["observed"]["error_rate"].as_f64().unwrap() > 0.9);
}

/// The measured numbers say out loud that they do not route, so a reader of
/// `/status` cannot conclude from a 400ms EWMA that the router knows about it.
#[tokio::test]
async fn the_measured_numbers_are_labelled_as_not_used_for_routing() {
    let body = status_body(state_from_config(TWO_CANDIDATES)).await;
    let observed = &row(&body, "fast")["observed"];

    assert_eq!(
        observed["note"].as_str().unwrap(),
        "measured, not used for routing: allocation is by declared weight"
    );
    // Cold: no sample yet, so the field is null rather than a sentinel.
    assert!(observed["streaming_ewma_ms"].is_null(), "{observed}");
}

/// Mock upstream that keeps answering, so whichever candidate the router picks
/// gets a response.
async fn eager_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
            common::read_once(&mut conn).await;
            common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
        }
    });
    (port, handle)
}

/// Two live upstreams for the alias `fast`, weights 2 and 1.
async fn two_candidate_state() -> (Arc<AppState>, Vec<tokio::task::JoinHandle<()>>) {
    let (port_alpha, alpha) = eager_upstream().await;
    let (port_beta, beta) = eager_upstream().await;
    let state = state_from_config(&format!(
        r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_alpha}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_beta}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "m-alpha", weight = 2 }},
  {{ provider = "beta", model = "m-beta", weight = 1 }},
]
"#
    ));
    (state, vec![alpha, beta])
}

/// The routing decision is auditable at debug: which alias, which candidate
/// won, why, and what every candidate's declared weight buys it.
#[tokio::test(flavor = "current_thread")]
async fn routed_request_logs_the_reason_and_the_declared_shares() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state).await;

    capture_logs(logs.clone(), async move {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let chosen = resp
            .headers()
            .get("x-sturnus-provider")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains("routing decision")
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains("routing decision"))
            .unwrap_or_else(|| panic!("no routing decision line in:\n{text}"));

        assert!(line.contains("alias=fast"), "alias missing: {line}");
        assert!(
            line.contains(&format!("provider={chosen}")),
            "the chosen candidate ({chosen}) should be logged: {line}"
        );
        assert!(
            line.contains(r#"reason="fresh-by-weight""#),
            "the placement reason is missing: {line}"
        );
        // Declared weights 2:1, so the shares are 66.7% and 33.3% — the split
        // the config declares, not one derived from measured latency.
        assert!(
            line.contains("alpha/m-alpha=66.7%"),
            "alpha's declared share missing: {line}"
        );
        assert!(
            line.contains("beta/m-beta=33.3%"),
            "beta's declared share missing: {line}"
        );
        // Nothing is demoted, so the skip report is empty: a line that names no
        // skipped box reads exactly as it did before demotion existed.
        assert!(
            line.ends_with("demoted="),
            "an unskipped placement should report no demoted box: {line}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

/// A request that rides an explicit pin bypasses the placement path, and its
/// log line names the pin as the reason rather than pretending to be a
/// weighted choice.
#[tokio::test(flavor = "current_thread")]
async fn an_explicit_pin_logs_the_pin_as_its_reason() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state).await;

    capture_logs(logs.clone(), async move {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("x-session-affinity", "beta/m-beta")
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("x-sturnus-provider")
                .unwrap()
                .to_str()
                .unwrap(),
            "beta",
            "the pin must win over the weight-preferred candidate"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="explicit-pin""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="explicit-pin""#))
            .unwrap_or_else(|| panic!("no explicit-pin line in:\n{text}"));
        assert!(line.contains("alias=fast"), "alias missing: {line}");
        assert!(
            line.contains("provider=beta"),
            "the pinned provider should be named: {line}"
        );
        assert!(
            line.contains("model=m-beta"),
            "the pinned model should be named: {line}"
        );
        assert!(
            !line.contains(r#"reason="fresh-by-weight""#),
            "a pin is not a weighted placement:\n{text}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

// ---------------------------------------------------------------------------
// The demotion window, in the same line: which boxes a placement skipped, and
// how much of each window is left.
// ---------------------------------------------------------------------------

/// Start a demotion window on `provider`, as a transport failure would.
fn demote(state: &AppState, provider: &str) {
    let candidate = state
        .model_map
        .get("fast")
        .unwrap()
        .iter()
        .find(|c| c.provider_name == provider)
        .unwrap();
    state.tracker.demote(candidate.stats_index);
}

/// The routing debug line names the box a placement skipped, with the time
/// left on its window, so "why is this box getting nothing?" is answerable
/// from the log alone.
#[tokio::test(flavor = "current_thread")]
async fn the_routing_debug_line_names_the_demoted_box_a_placement_skipped() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    demote(&state, "alpha");
    let (addr, _shutdown) = start_server(state).await;

    capture_logs(logs.clone(), async move {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("x-sturnus-provider")
                .unwrap()
                .to_str()
                .unwrap(),
            "beta",
            "the weight-2 box is demoted, so the weight-1 box takes the turn"
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
            line.contains("provider=beta"),
            "the box that served is named: {line}"
        );
        // The skipped box, and the seconds left on its window.
        assert!(
            line.contains("alpha/m-alpha=10s"),
            "the skipped, demoted box should be named with its window: {line}"
        );
        assert!(
            !line.contains("beta/m-beta=10s"),
            "the box that served was not skipped: {line}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

/// When every candidate is demoted the turn still goes out, on the heaviest
/// demoted box, and the line says so: `overflow-demoted` is a placement reason
/// an operator can grep for, distinct from the busy overflow and from a
/// weighted choice.
#[tokio::test(flavor = "current_thread")]
async fn a_placement_onto_the_heaviest_demoted_box_logs_that_reason() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    demote(&state, "alpha");
    demote(&state, "beta");
    let (addr, _shutdown) = start_server(state).await;

    capture_logs(logs.clone(), async move {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "a demoted box still answers its own turns"
        );
        assert_eq!(
            resp.headers()
                .get("x-sturnus-provider")
                .unwrap()
                .to_str()
                .unwrap(),
            "alpha",
            "the heaviest demoted box serves"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="overflow-demoted""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="overflow-demoted""#))
            .unwrap_or_else(|| panic!("no overflow-demoted line in:\n{text}"));
        assert!(
            line.contains("provider=alpha"),
            "the box that served is named: {line}"
        );
        // Both windows are on the line, so the reader can see there was no
        // eligible box to skip to.
        assert!(
            line.contains("alpha/m-alpha=10s") && line.contains("beta/m-beta=10s"),
            "every demoted box should be named: {line}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

/// A demotion is not a pin-breaker, and the line says which box a pinned turn
/// rode while that box sat out the placement path.
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_turn_on_a_demoted_box_logs_the_pin_and_the_window_it_skipped() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    demote(&state, "beta");
    let (addr, _shutdown) = start_server(state).await;

    capture_logs(logs.clone(), async move {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("x-session-affinity", "beta/m-beta")
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("x-sturnus-provider")
                .unwrap()
                .to_str()
                .unwrap(),
            "beta",
            "the pin is honored while beta is demoted"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="explicit-pin""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="explicit-pin""#))
            .unwrap_or_else(|| panic!("no explicit-pin line in:\n{text}"));
        assert!(
            line.contains("provider=beta"),
            "the pinned box is named: {line}"
        );
        assert!(
            line.contains("beta/m-beta=10s"),
            "the window the placement path would have skipped is on the line: {line}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

// ---------------------------------------------------------------------------
// The same numbers on the other two surfaces: `/status`, `/metrics`, and the
// load the debug line carries.
//
// The rule these tests hold the code to is one sentence: the number an
// operator reads is the number the placement path used. `/status` renders
// what the tracker and the session table know, the two gauges are written
// from those same live counts at scrape time, and the placement counter is
// incremented at the seam that made the decision — so no surface can drift
// from the router, and a scrape cannot invent a number.
// ---------------------------------------------------------------------------

/// How long a count is allowed to take landing. Generous: these are local
/// sockets, and the point is to fail loudly rather than flake.
const SETTLE: Duration = Duration::from_secs(5);

/// A caller-side client.
fn caller_client() -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::new()
}

/// `GET` an observability path as text.
async fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let resp = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap()
}

/// `GET /status` parsed as JSON, from a server the test is already driving.
async fn status_json(addr: std::net::SocketAddr) -> serde_json::Value {
    serde_json::from_str(&get(addr, "/status").await).unwrap()
}

/// The stats slot of `provider` in the `fast` alias.
fn index_of(state: &AppState, provider: &str) -> usize {
    state
        .model_map
        .iter()
        .find(|(_, candidate)| candidate.provider_name == provider)
        .unwrap_or_else(|| panic!("no candidate {provider}"))
        .1
        .stats_index
}

/// The live outstanding-turn count for `provider`.
fn in_flight(state: &AppState, provider: &str) -> usize {
    state.tracker.in_flight(index_of(state, provider))
}

/// Wait until `provider`'s outstanding count is `want`, failing with what it
/// is stuck on. Proving a turn is running is what makes every "and then it
/// went back to zero" assertion below mean something.
async fn wait_for_in_flight(state: &Arc<AppState>, provider: &str, want: usize) {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let got = in_flight(state, provider);
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{provider}'s outstanding count never reached {want}; it is stuck at {got}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A row's number, at a (possibly nested) path, failing with the row.
fn number(row: &serde_json::Value, path: &[&str]) -> i64 {
    let mut node = row;
    for key in path {
        node = &node[key];
    }
    node.as_i64().unwrap_or_else(|| {
        panic!("{} should be an integer: {row}", path.join("."));
    })
}

/// A live count, in the shape a `/status` field is compared against.
fn want(count: usize) -> i64 {
    i64::try_from(count).unwrap()
}

/// One non-streaming turn through the proxy: its status and the box that
/// served it.
async fn turn(addr: std::net::SocketAddr, alias: &str, session: Option<&str>) -> (u16, String) {
    let mut req = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&serde_json::json!({"model": alias, "messages": []}));
    if let Some(session) = session {
        req = req.header("x-session-affinity", session);
    }
    let resp = req.send().await.unwrap();
    (
        resp.status().as_u16(),
        resp.headers()
            .get("x-sturnus-provider")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
    )
}

/// One streaming turn: the box that served it, and its body as a stream.
///
/// The first chunk is read here, so a caller that gets this back has proof
/// the turn is running on that box — the state every load assertion below
/// needs.
async fn streaming_turn(
    addr: std::net::SocketAddr,
    alias: &str,
    session: Option<&str>,
) -> (
    impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>>,
    String,
) {
    let mut req = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&serde_json::json!({"model": alias, "messages": [], "stream": true}));
    if let Some(session) = session {
        req = req.header("x-session-affinity", session);
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let provider = resp
        .headers()
        .get("x-sturnus-provider")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();
    (stream, provider)
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

/// The exported value of one series, by name and exact label set.
fn series_value(metrics: &str, name: &str, labels: &[(&str, &str)]) -> Option<i64> {
    metrics.lines().find_map(|line| {
        let (series, value) = line.split_once(' ')?;
        series.split_once('{')?.0.eq(name).then_some(())?;
        labels
            .iter()
            .all(|(key, want)| series.contains(&format!("{key}=\"{want}\"")))
            .then_some(())?;
        value.trim().parse::<i64>().ok()
    })
}

/// The sum of every series of one metric name: the whole traffic the metric
/// has seen, across every label set.
fn series_sum(metrics: &str, name: &str) -> i64 {
    metrics
        .lines()
        .filter_map(|line| {
            let (series, value) = line.split_once(' ')?;
            series.split_once('{')?.0.eq(name).then_some(())?;
            value.trim().parse::<i64>().ok()
        })
        .sum()
}

/// The `load=` field of the routing debug line, as it should read: one
/// candidate per entry, in schedule order.
fn load_field(entries: &[(&str, &str, usize, usize)]) -> String {
    entries
        .iter()
        .map(|(provider, model, weight, turns)| {
            format!(
                "{provider}/{model}=w{weight},t{turns},{}",
                if *turns > 0 { "busy" } else { "idle" }
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Mock upstream that answers with an SSE stream and holds it open until a
/// permit is available: the window in which a turn is provably running on the
/// box behind it.
async fn held_sse_upstream() -> (u16, Arc<Semaphore>, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncWriteExt;

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
                .ok();
                conn.flush().await.ok();
                conn.write_all(b"9\r\ndata: a\n\n\r\n").await.ok();
                conn.flush().await.ok();
                // Held open until the test lets this one finish.
                let Ok(permit) = release.acquire().await else {
                    return;
                };
                permit.forget();
                conn.write_all(b"0\r\n\r\n").await.ok();
                conn.flush().await.ok();
                conn.shutdown().await.ok();
            });
        }
    });
    (port, release, handle)
}

/// Two candidates for `fast` (alpha at weight 2, beta at 1), each behind a
/// stream it holds open, so the test decides when a turn stops running.
async fn two_held_state() -> (
    Arc<AppState>,
    Vec<tokio::task::JoinHandle<()>>,
    [Arc<Semaphore>; 2],
) {
    let (port_alpha, alpha_release, alpha) = held_sse_upstream().await;
    let (port_beta, beta_release, beta) = held_sse_upstream().await;
    let state = state_from_config(&format!(
        r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_alpha}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_beta}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "m-alpha", weight = 2 }},
  {{ provider = "beta", model = "m-beta", weight = 1 }},
]
"#
    ));
    (state, vec![alpha, beta], [alpha_release, beta_release])
}

/// `/status` reports the numbers the placement path used, read from the same
/// tracker, session table and schedule the router consults — driven here by
/// real turns over HTTP, so the endpoint is checked against traffic rather
/// than against an empty process.
#[tokio::test]
async fn status_reports_the_numbers_the_placement_path_used() {
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state.clone()).await;

    // Real turns: two session keys, and a second turn of the first one.
    for session in ["s1", "s2", "s1"] {
        let (status, _) = turn(addr, "fast", Some(session)).await;
        assert_eq!(status, 200);
    }

    let body = status_json(addr).await;
    let schedule = router::placement_schedule(state.model_map.get("fast").unwrap());
    // Snapshot the table's per-candidate counts in one scoped lock: the awaits
    // below must not run with the guard held, and a pin count is a reading of
    // the table as it stood, which is exactly what the endpoint reports.
    let pinned: Vec<(String, usize)> = {
        let table = state.sessions.lock().unwrap();
        schedule
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.candidate.provider_name.clone(),
                    table.pinned_count(entry.candidate.stats_index),
                )
            })
            .collect()
    };

    for entry in &schedule.entries {
        let index = entry.candidate.stats_index;
        let row = row(&body, &entry.candidate.provider_name);

        // The allocation numbers are the schedule's, verbatim: the same
        // arithmetic the weighted draw is run against.
        assert_eq!(
            number(row, &["routing", "weight"]),
            i64::try_from(entry.weight).unwrap(),
            "{row}"
        );
        assert!(
            (share(row) - entry.share).abs() < 1e-9,
            "the reported share is the schedule's: {row}"
        );

        // The live numbers are the tracker's and the table's, as of now.
        assert_eq!(
            number(row, &["in_flight"]),
            want(state.tracker.in_flight(index)),
            "{row}"
        );
        assert_eq!(
            number(row, &["turns_served"]),
            want(state.tracker.turns_served(index)),
            "{row}"
        );
        assert_eq!(
            number(row, &["sessions_pinned"]),
            want(
                pinned
                    .iter()
                    .find(|(provider, _)| *provider == entry.candidate.provider_name)
                    .map(|(_, count)| *count)
                    .unwrap()
            ),
            "{row}"
        );
        assert_eq!(
            row["busy"],
            serde_json::json!(state.tracker.in_flight(index) > 0),
            "busy is the state the placement rule reads: {row}"
        );
        assert!(
            row["demotion_secs_left"].is_null(),
            "nothing has failed, so nothing is stood down: {row}"
        );
        // The health state tracks the turns, and the turns are the ones the
        // placement path dispatched: a box this alias has served is warm, a
        // box it has not is cold. (A 2:1 pair over three picks is all one box:
        // the Weyl stride is low-discrepancy, not exact per window.)
        assert_eq!(
            row["status"],
            serde_json::json!(if state.tracker.turns_served(index) > 0 {
                "warm"
            } else {
                "cold"
            }),
            "the health state follows the turns this box has served: {row}"
        );
    }

    // Every dispatched turn landed on exactly one box, and the two
    // `turns_served` fields are where all of them went.
    let served: i64 = schedule
        .entries
        .iter()
        .map(|entry| want(state.tracker.turns_served(entry.candidate.stats_index)))
        .sum();
    assert_eq!(served, 3, "three turns, counted once each, on one box each");
    assert_eq!(
        body["sessions"]["pinned"], 2,
        "two session keys, two pins, whatever the turns did"
    );

    // The demotion window is the same kind of reading: the seconds `/status`
    // reports are the seconds the eligibility gate is enforcing, and the
    // declared share is untouched by it.
    demote(&state, "alpha");
    let body = status_json(addr).await;
    let alpha = row(&body, "alpha");
    let left = state
        .tracker
        .demotion_remaining(index_of(&state, "alpha"))
        .expect("alpha was just demoted");
    assert_eq!(
        number(alpha, &["demotion_secs_left"]),
        i64::try_from(secs_left(left)).unwrap(),
        "{alpha}"
    );
    assert_eq!(alpha["status"], "demoted", "{alpha}");
    assert!(
        row(&body, "beta")["demotion_secs_left"].is_null(),
        "beta is eligible: {}",
        row(&body, "beta")
    );
    assert!(
        (share(alpha) - 2.0 / 3.0).abs() < 1e-9,
        "a demotion is a window, not a discount: {alpha}"
    );

    for upstream in upstreams {
        upstream.abort();
    }
}

/// The placement-reason counter counts at the seam that decided, so the five
/// reasons are a queryable record of what the router did, and a turn that
/// rode a pin is visible as a pin.
#[tokio::test]
async fn the_placement_reason_counter_counts_the_reasons_the_router_gave() {
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state.clone()).await;

    // Zero-initialised, as `main` leaves it: every reason is a series at `0`
    // before any traffic, so "this never happened" is not read as "no data".
    let metrics = get(addr, "/metrics").await;
    for provider in [("alpha", "m-alpha"), ("beta", "m-beta")] {
        for reason in Placement::ALL {
            assert_eq!(
                series_value(
                    &metrics,
                    "sturnus_placements_total",
                    &[
                        ("alias", "fast"),
                        ("provider", provider.0),
                        ("model", provider.1),
                        ("reason", reason.as_str()),
                    ],
                ),
                Some(0),
                "{} should be exported at 0 for {}/{} before any traffic",
                reason.as_str(),
                provider.0,
                provider.1
            );
        }
    }
    assert_eq!(series_sum(&metrics, "sturnus_placements_total"), 0);

    // One fresh pick, which creates a pin; a second turn riding that pin; a
    // third turn naming a box outright.
    let (_, first) = turn(addr, "fast", Some("s1")).await;
    let (_, second) = turn(addr, "fast", Some("s1")).await;
    let (_, third) = turn(addr, "fast", Some("beta/m-beta")).await;
    assert_eq!(first, second, "a session's turns stay on its box");
    assert_eq!(
        third, "beta",
        "the box the caller named is the box that served"
    );

    let metrics = get(addr, "/metrics").await;
    let count = |provider: &str, model: &str, reason: &str| {
        series_value(
            &metrics,
            "sturnus_placements_total",
            &[
                ("alias", "fast"),
                ("provider", provider),
                ("model", model),
                ("reason", reason),
            ],
        )
    };

    // The pick that made the pin, on the box that served it.
    let (first_provider, first_model) = if first == "alpha" {
        ("alpha", "m-alpha")
    } else {
        ("beta", "m-beta")
    };
    assert_eq!(
        count(first_provider, first_model, "fresh-by-weight"),
        Some(1)
    );
    // The turn that queued behind its own session's first pick.
    assert_eq!(count(first_provider, first_model, "pinned"), Some(1));
    // The turn the caller named outright.
    assert_eq!(count("beta", "m-beta", "explicit-pin"), Some(1));

    // Nothing else was counted: three turns, three placements, one series each.
    assert_eq!(
        series_sum(&metrics, "sturnus_placements_total"),
        3,
        "every proxied request counts exactly one placement\n{metrics}"
    );

    for upstream in upstreams {
        upstream.abort();
    }
}

/// The sessions-pinned gauge is written from the live table at scrape time,
/// exactly as the in-flight gauge is written from the tracker: it agrees with
/// `/status`, and a pin the idle TTL has passed leaves `0` behind rather than
/// a count that sticks.
#[tokio::test]
async fn the_sessions_pinned_gauge_is_written_from_the_live_table() {
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state.clone()).await;

    // Zero-initialised: an idle candidate is an observable zero.
    let metrics = get(addr, "/metrics").await;
    for provider in [("alpha", "m-alpha"), ("beta", "m-beta")] {
        assert_eq!(
            series_value(
                &metrics,
                "sturnus_sessions_pinned",
                &[
                    ("alias", "fast"),
                    ("provider", provider.0),
                    ("model", provider.1),
                ],
            ),
            Some(0),
            "no session is pinned to {} yet",
            provider.0
        );
    }

    // Two sessions, and a second turn of one of them.
    for session in ["s1", "s2", "s1"] {
        turn(addr, "fast", Some(session)).await;
    }

    let metrics = get(addr, "/metrics").await;
    let body = status_json(addr).await;
    // Snapshot the table's counts in one scoped lock, so no guard is held
    // across the awaits below.
    let pinned: Vec<(&str, i64)> = {
        let table = state.sessions.lock().unwrap();
        [("alpha", "m-alpha"), ("beta", "m-beta")]
            .into_iter()
            .map(|(provider, _)| {
                (
                    provider,
                    want(table.pinned_count(index_of(&state, provider))),
                )
            })
            .collect()
    };
    for provider in [("alpha", "m-alpha"), ("beta", "m-beta")] {
        let live = pinned
            .iter()
            .find(|(name, _)| *name == provider.0)
            .map(|(_, count)| *count)
            .unwrap();
        let gauge = series_value(
            &metrics,
            "sturnus_sessions_pinned",
            &[
                ("alias", "fast"),
                ("provider", provider.0),
                ("model", provider.1),
            ],
        );
        assert_eq!(gauge, Some(live), "the gauge is the table's own count");
        assert_eq!(
            number(row(&body, provider.0), &["sessions_pinned"]),
            live,
            "and so is `/status`: the two surfaces cannot disagree"
        );
    }
    assert_eq!(
        series_sum(&metrics, "sturnus_sessions_pinned"),
        2,
        "two sessions, each pinned to one box"
    );

    // Cross the idle TTL on the shared clock, then drive one more turn: the
    // request path sweeps the expired pins, and the next scrape follows.
    state.tracker.clock().advance(Duration::from_secs(1801));
    turn(addr, "fast", Some("s3")).await;

    let metrics = get(addr, "/metrics").await;
    let table = state.sessions.lock().unwrap();
    assert_eq!(
        table.len(),
        1,
        "the two expired pins are gone, only the new session's stands"
    );
    for provider in [("alpha", "m-alpha"), ("beta", "m-beta")] {
        assert_eq!(
            series_value(
                &metrics,
                "sturnus_sessions_pinned",
                &[
                    ("alias", "fast"),
                    ("provider", provider.0),
                    ("model", provider.1),
                ],
            ),
            Some(want(table.pinned_count(index_of(&state, provider.0)))),
            "a dropped pin must not leave the gauge set"
        );
    }
    assert_eq!(series_sum(&metrics, "sturnus_sessions_pinned"), 1);
    drop(table);

    for upstream in upstreams {
        upstream.abort();
    }
}

/// The debug line carries the inputs the busyness rule read: every candidate's
/// declared weight, its outstanding turns, and its busy/idle state — beside
/// the shares the weight half drew from.
#[tokio::test(flavor = "current_thread")]
async fn the_routing_debug_line_carries_every_candidate_s_weight_and_load() {
    let logs = CapturedLogs::default();
    let (state, upstreams, releases) = two_held_state().await;
    let (addr, _shutdown) = start_server(state.clone()).await;

    capture_logs(logs.clone(), async move {
        let state = state.clone();

        // One prompt running on the weight-2 box, named outright so the test
        // chooses which box is busy rather than asking the router to.
        let (held, provider) = streaming_turn(addr, "fast", Some("alpha/m-alpha")).await;
        assert_eq!(provider, "alpha");
        wait_for_in_flight(&state, "alpha", 1).await;

        // A new session, placed while that prompt runs: the line it logs shows
        // the state the placement acted on, not just the answer.
        let (moved, provider) = streaming_turn(addr, "fast", None).await;
        assert_eq!(provider, "beta", "the idle box takes the session");

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="fresh-by-weight""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="fresh-by-weight""#))
            .unwrap_or_else(|| panic!("no fresh-by-weight line in:\n{text}"));

        let expected = load_field(&[("alpha", "m-alpha", 2, 1), ("beta", "m-beta", 1, 0)]);
        assert!(
            line.contains(&format!("load={expected}")),
            "the load the placement read should be on the line: {line}"
        );
        assert!(
            line.contains("shares=alpha/m-alpha=66.7% beta/m-beta=33.3%"),
            "the declared shares are still on the line: {line}"
        );

        releases[0].add_permits(1);
        releases[1].add_permits(1);
        assert!(
            !drain(held).await,
            "the held stream ends cleanly once released"
        );
        assert!(!drain(moved).await, "the placed stream ends cleanly");
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

/// The busy-preference rule, readable in the log: the turn that moved because
/// every box was running a prompt logs `overflow-busy`, and the turn that
/// queued behind its own session's prompt logs `pinned` — the two outcomes
/// the rule is defined by, named in the line that produced them.
#[tokio::test(flavor = "current_thread")]
async fn the_busy_preference_logs_overflow_busy_and_a_queued_turn_logs_pinned() {
    let logs = CapturedLogs::default();
    let (state, upstreams, releases) = two_held_state().await;
    let (addr, _shutdown) = start_server(state.clone()).await;

    capture_logs(logs.clone(), async move {
        let state = state.clone();

        // 1. A session's first turn: placed on the weight-2 box, and pinned.
        let (first, provider) = streaming_turn(addr, "fast", Some("s1")).await;
        assert_eq!(provider, "alpha", "the weight-2 box takes a fresh pick");
        wait_for_in_flight(&state, "alpha", 1).await;

        // 2. A new session with alpha busy: the idle box takes it.
        let (second, provider) = streaming_turn(addr, "fast", None).await;
        assert_eq!(provider, "beta");
        wait_for_in_flight(&state, "beta", 1).await;

        // 3. A new session with every box busy: the least busy group competes,
        // and weight breaks the tie. This is the turn that moved.
        let (third, provider) = streaming_turn(addr, "fast", None).await;
        assert_eq!(
            provider, "alpha",
            "weight breaks the tie between two busy boxes"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="overflow-busy""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="overflow-busy""#))
            .unwrap_or_else(|| panic!("no overflow-busy line in:\n{text}"));
        assert!(
            line.contains("provider=alpha"),
            "the box that served is named: {line}"
        );
        let expected = load_field(&[("alpha", "m-alpha", 2, 1), ("beta", "m-beta", 1, 1)]);
        assert!(
            line.contains(&format!("load={expected}")),
            "every candidate was busy, and the line says so: {line}"
        );

        // 4. The same session, next turn: alpha is mid-prompt and beta is
        // idle, and the turn does not move — it queues behind its own prompt.
        let (fourth, provider) = streaming_turn(addr, "fast", Some("s1")).await;
        assert_eq!(provider, "alpha", "a pinned turn is not moved by busyness");

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| {
            t.contains(r#"reason="pinned""#)
        })
        .await;
        let line = text
            .lines()
            .find(|line| line.contains(r#"reason="pinned""#))
            .unwrap_or_else(|| panic!("no pinned line in:\n{text}"));
        assert!(
            line.contains("provider=alpha") && line.contains(r#"session="s1""#),
            "the box it queued behind and the session that owns it: {line}"
        );
        let expected = load_field(&[("alpha", "m-alpha", 2, 2), ("beta", "m-beta", 1, 1)]);
        assert!(
            line.contains(&format!("load={expected}")),
            "the line shows the queue the turn joined: {line}"
        );

        // Let all four streams finish, so nothing is left holding a count.
        releases[0].add_permits(3);
        releases[1].add_permits(1);
        for stream in [first, second, third, fourth] {
            assert!(
                !drain(stream).await,
                "every stream ends cleanly once released"
            );
        }
        wait_for_in_flight(&state, "alpha", 0).await;
        wait_for_in_flight(&state, "beta", 0).await;
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}
