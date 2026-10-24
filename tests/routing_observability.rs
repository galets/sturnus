//! Observability of the routing decision: the weights `/status` reports, and
//! the debug lines that make a routed request readable next to a pinned one.
//!
//! The numbers asserted here are derived from the weighting formula in
//! `src/router.rs`, not from whatever the endpoint happened to print:
//!
//! ```text
//! weight = round( (best_effective / its_effective)^exploit_k * 1000 )
//!          clamped to at least 1000 * 0.01 = 10   (the probe floor)
//! share  = weight / sum(weights)
//! ```
//!
//! with `effective = latency_ewma / success_rate_ewma` for the request's
//! latency mode. `exploit_k` defaults to 3.0.

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

/// One alias, two candidates. The upstream addresses are only used by the
/// tests that actually proxy something.
const TWO_CANDIDATES: &str = r#"
[provider.fast]
base_url = "http://127.0.0.1:9"
api_key = "fake"

[provider.slow]
base_url = "http://127.0.0.1:9"
api_key = "fake"

[model]
test = [
  { provider = "fast", model = "m-fast" },
  { provider = "slow", model = "m-slow" },
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
        exploit_k: 3.0,
        gcp_token_provider: None,
        budget: BufferBudget::new(128 * 1024, 32 * 1024 * 1024),
        metrics: Metrics::new(),
        shutting_down: AtomicBool::new(false),
        api_keys: None,
    })
}

/// Serve `state` until the returned sender is dropped or fired.
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

/// One sample sets the latency EWMA to exactly that sample (a cold EWMA takes
/// the first observation whole) and leaves the success-rate EWMA at 1.0, so
/// the effective latency is exactly `ms` in both modes.
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

/// `times` failures and no successes: at alpha 0.3 the success-rate EWMA is
/// `0.7^times`, so 50 of them land under the `MIN_SUCCESS_RATE` clamp.
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
    let (addr, _shutdown) = start_server(state);
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

/// A 2×-slower candidate gets `(1/2)^3 = 0.125` of the fastest candidate's
/// weight, i.e. `0.125 / 1.125 = 1/9 ≈ 11.1%` of the traffic — the ~11% the
/// ticket calls for, exactly. Both modes are warmed identically, so the two
/// mode-specific schedules come out the same.
#[tokio::test]
async fn status_reports_weight_share_and_effective_latency() {
    let state = state_from_config(TWO_CANDIDATES);
    warm(&state, "fast", 100);
    warm(&state, "slow", 200);

    let body = status_body(state).await;

    let fast = row(&body, "fast");
    let slow = row(&body, "slow");
    assert_eq!(fast["alias"], "test");
    assert_eq!(fast["model"], "m-fast");
    assert_eq!(fast["status"], "warm");
    // The pre-existing numbers are untouched.
    assert_eq!(fast["streaming_ewma_ms"], 100);
    assert_eq!(fast["nonstreaming_ewma_ms"], 100);
    assert_eq!(fast["error_rate"], 0.0);

    for mode in ["streaming", "nonstreaming"] {
        let fast_route = &fast["routing"][mode];
        let slow_route = &slow["routing"][mode];

        assert_eq!(fast_route["weight"], 1000, "(100/100)^3 * 1000");
        assert_eq!(slow_route["weight"], 125, "(100/200)^3 * 1000");
        assert_eq!(fast_route["effective_latency_ms"], 100.0);
        assert_eq!(slow_route["effective_latency_ms"], 200.0);

        let fast_share = fast_route["share"].as_f64().unwrap();
        let slow_share = slow_route["share"].as_f64().unwrap();
        assert!(
            (fast_share - 1000.0 / 1125.0).abs() < 1e-9,
            "{mode} fast share {fast_share} should be 1000/1125 = 8/9"
        );
        assert!(
            (slow_share - 125.0 / 1125.0).abs() < 1e-9,
            "{mode} slow share {slow_share} should be 125/1125 = 1/9 ≈ 11%"
        );
        assert!(
            (fast_share + slow_share - 1.0).abs() < 1e-9,
            "shares must be normalised, sum was {}",
            fast_share + slow_share
        );
    }
}

/// A candidate that has stopped working entirely is not dropped from the
/// schedule: it keeps the probe floor. Equal-latency candidates, then 50
/// straight failures on "slow" — its success rate is clamped to
/// `MIN_SUCCESS_RATE` (1e-3), so its effective latency is 100_000ms and its
/// raw weight `(100/100_000)^3 * 1000 ≈ 1e-6` rounds to nothing and is clamped
/// to `WEIGHT_RESOLUTION * PROBE_FLOOR` = 10. Share = `10 / 1010` ≈ 0.99%.
#[tokio::test]
async fn status_shows_a_candidate_sitting_at_the_probe_floor() {
    let state = state_from_config(TWO_CANDIDATES);
    warm(&state, "fast", 100);
    warm(&state, "slow", 100);
    fail(&state, "slow", 50);

    let body = status_body(state).await;

    let fast = row(&body, "fast");
    let slow = row(&body, "slow");
    assert_eq!(slow["status"], "degraded", "error EWMA is over threshold");

    for mode in ["streaming", "nonstreaming"] {
        let slow_route = &slow["routing"][mode];
        assert_eq!(slow_route["weight"], 10, "clamped to the probe floor");
        let share = slow_route["share"].as_f64().unwrap();
        assert!(
            (share - 10.0 / 1010.0).abs() < 1e-9,
            "{mode} floored share {share} should be 10/1010"
        );
        assert!(
            (0.009..=0.011).contains(&share),
            "a floored candidate should read ≈1% in {mode}, got {share}"
        );
        // Still scheduled, still probing: it is how it wins traffic back.
        assert!(
            fast["routing"][mode]["share"].as_f64().unwrap() > 0.98,
            "the healthy candidate keeps the rest"
        );
    }
}

/// A candidate with no latency data yet is not starved either: it probes at
/// `COLD_PROBE_WEIGHT` (0.25) of the best candidate's rate, so 250 of
/// `1000 + 250` units = 20%, and it reports no effective latency.
#[tokio::test]
async fn status_shows_a_cold_candidate_as_a_probe() {
    let state = state_from_config(TWO_CANDIDATES);
    warm(&state, "fast", 100);

    let body = status_body(state).await;

    let fast = row(&body, "fast");
    let slow = row(&body, "slow");
    assert_eq!(fast["status"], "warm");
    assert_eq!(slow["status"], "cold");
    assert!(slow["streaming_ewma_ms"].is_null());

    for mode in ["streaming", "nonstreaming"] {
        let fast_route = &fast["routing"][mode];
        let slow_route = &slow["routing"][mode];
        assert_eq!(
            fast_route["weight"], 1000,
            "the only warm candidate is the reference"
        );
        assert!(
            slow_route["effective_latency_ms"].is_null(),
            "a cold candidate has no effective latency to report"
        );
        assert_eq!(slow_route["weight"], 250, "cold probe weight");
        let share = slow_route["share"].as_f64().unwrap();
        assert!(
            (share - 0.2).abs() < 1e-9,
            "{mode} cold share {share} should be 250/1250 = 0.2"
        );
    }
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

/// Two candidates at a 1:2 latency ratio for the alias `fast`.
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
  {{ provider = "alpha", model = "m-alpha" }},
  {{ provider = "beta", model = "m-beta" }},
]
"#
    ));
    let candidates = state.model_map.get("fast").unwrap();
    let alpha_idx = candidates
        .iter()
        .find(|c| c.provider_name == "alpha")
        .unwrap()
        .stats_index;
    let beta_idx = candidates
        .iter()
        .find(|c| c.provider_name == "beta")
        .unwrap()
        .stats_index;
    for mode in [LatencyMode::Streaming, LatencyMode::NonStreaming] {
        state
            .tracker
            .record_success(alpha_idx, mode, Duration::from_millis(100));
        state
            .tracker
            .record_success(beta_idx, mode, Duration::from_millis(200));
    }
    (state, vec![alpha, beta])
}

/// The routing decision is auditable at debug: which alias, which candidate
/// won, and what every candidate was worth.
#[tokio::test(flavor = "current_thread")]
async fn routed_request_logs_every_candidates_share() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state);

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
        // Every candidate's share at the default sharpness: 1000/1125 and
        // 125/1125, i.e. 88.9% and 11.1%.
        assert!(
            line.contains("alpha/m-alpha=88.9%"),
            "alpha's share missing: {line}"
        );
        assert!(
            line.contains("beta/m-beta=11.1%"),
            "beta's share missing: {line}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}

/// A request that rides a session-affinity pin never reaches the router, and
/// its log line says so rather than pretending to be a decision.
#[tokio::test(flavor = "current_thread")]
async fn honored_pin_logs_a_pin_not_a_routing_decision() {
    let logs = CapturedLogs::default();
    let (state, upstreams) = two_candidate_state().await;
    let (addr, _shutdown) = start_server(state);

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
            "the pin must win over the faster candidate"
        );

        let text = wait_for_log(&logs, Duration::from_secs(3), |t| t.contains("pin honored")).await;
        let line = text
            .lines()
            .find(|line| line.contains("pin honored"))
            .unwrap_or_else(|| panic!("no pin line in:\n{text}"));
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
            !text.contains("routing decision"),
            "a pin is not a routing decision:\n{text}"
        );
        assert!(
            !text.contains("shares="),
            "a pinned request consulted no schedule:\n{text}"
        );
    })
    .await;

    for upstream in upstreams {
        upstream.abort();
    }
}
