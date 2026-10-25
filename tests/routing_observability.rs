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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::tracker::{LatencyMode, Tracker};

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

    let mut tracker = Tracker::new(0.3, 0.5);
    let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        client: reqwest::Client::new(),
        gcp_token_provider: None,
        budget: BufferBudget::new(128 * 1024, 32 * 1024 * 1024),
        metrics: Metrics::new(),
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
