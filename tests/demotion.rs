//! The demotion window, end to end: a box that cannot be reached takes no new
//! sessions for `routing.demotion_secs` and is then fully eligible again in one
//! step.
//!
//! Every test here drives real HTTP at a real proxy listener over raw-socket
//! mock upstreams, and reads the window off `/status` — the surface an operator
//! actually curls. The clock is the injectable one (`sturnus::clock::Clock`),
//! so crossing a window's end is one `advance` call rather than a sleep, and
//! one test can assert both halves of the rule: nothing during the window,
//! everything the moment it expires.
//!
//! The distinction the whole feature turns on is visible in the first two
//! tests: a box that never answered (connect refused, read timeout) is demoted;
//! a box that answered `500` is not, and keeps every placement its weight
//! declares.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::tracker::Tracker;

mod common;

/// Two candidates for the alias `fast`: `alpha` at declared weight 19, `beta`
/// at 1. Weight alone always names alpha, so every placement that is not alpha
/// is a placement the demotion caused.
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

/// The same two boxes with a window of three seconds instead of the default
/// ten: the config value is what the window is built from.
fn alpha_and_beta_window(port_alpha: u16, port_beta: u16, secs: u64) -> String {
    format!(
        "{}\n[routing]\ndemotion_secs = {secs}\n",
        alpha_and_beta(port_alpha, port_beta)
    )
}

/// `AppState` as `main` builds it, with the window length taken from the
/// config's `[routing]` table so a test that sets `demotion_secs` exercises the
/// same plumbing.
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

// ---------------------------------------------------------------------------
// Requests and mock upstreams
// ---------------------------------------------------------------------------

/// A caller-side client. `init_crypto` comes first: a reqwest client needs a
/// process rustls provider, and this one is built before the state that would
/// otherwise install it.
fn caller_client() -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::new()
}

/// The proxy's own upstream client, with a short read timeout: what a box that
/// takes the request and never answers looks like to the router.
fn proxy_client(read_timeout: Duration) -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(read_timeout)
        .build()
        .unwrap()
}

/// A non-streaming request for `alias`.
fn request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": []})
}

/// POST one request through the proxy and return its status and the box that
/// served it.
///
/// Every mock here answers, so the `x-sturnus-provider` header the proxy sets
/// on a relayed response is always present; a transport failure is the one
/// case with no header, and those tests assert the status instead.
async fn served_by(addr: std::net::SocketAddr, alias: &str) -> (u16, String) {
    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&request(alias))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let provider = resp
        .headers()
        .get("x-sturnus-provider")
        .map(|value| value.to_str().unwrap().to_string())
        .unwrap_or_default();
    (status, provider)
}

/// A port nothing is listening on: connect is refused immediately, which is the
/// cheapest way to be unreachable.
async fn refused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Mock upstream that answers every request immediately.
///
/// Each connection is served in a loop, because the proxy's client keeps a
/// connection alive and reuses it: a mock that read one request per connection
/// and then stopped reading would leave the proxy's second request on that
/// connection unread.
async fn eager_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
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

/// Mock upstream that reads a request and then never answers it, so the only
/// way the request ends is the proxy's own read timeout.
async fn silent_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                common::read_request(&mut conn).await.ok();
                // Never responds: the request can only end in a timeout.
                tokio::time::sleep(Duration::from_secs(3600)).await;
            });
        }
    });
    (port, handle)
}

/// Mock upstream that answers every request with `status_line` — a box that is
/// up, and unhappy.
async fn status_upstream(status_line: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
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
                    common::write_status(&mut conn, status_line, r#"{"error":"upstream said no"}"#)
                        .await;
                }
            });
        }
    });
    (port, handle)
}

/// Mock upstream that hangs up on the first `hangups` requests — accepted,
/// read, closed with no response bytes, which is a transport failure the proxy
/// cannot relay — and answers every request after that with `status_line`.
///
/// That shape is what makes the all-demoted rule testable over real HTTP: the
/// same box is unreachable long enough to be demoted and then able to answer,
/// so the response the caller gets during the window is provably the box's own.
async fn hangup_then_status(
    hangups: usize,
    status_line: &'static str,
) -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handle = tokio::spawn(async move {
        loop {
            let (mut conn, _) = listener.accept().await.unwrap();
            let served = Arc::clone(&served);
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
                    if served.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < hangups {
                        conn.shutdown().await.ok();
                        return;
                    }
                    common::write_status(&mut conn, status_line, r#"{"error":"the box's own"}"#)
                        .await;
                }
            });
        }
    });
    (port, handle)
}

// ---------------------------------------------------------------------------
// Reading the window
// ---------------------------------------------------------------------------

/// The `/status` row for `provider`.
async fn status_row(addr: std::net::SocketAddr, provider: &str) -> serde_json::Value {
    let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    body["candidates"]
        .as_array()
        .expect("candidates array")
        .iter()
        .find(|row| row["provider"] == provider)
        .unwrap_or_else(|| panic!("no /status row for {provider}: {body}"))
        .clone()
}

/// The seconds `/status` reports left in `provider`'s window, `None` when the
/// row says the candidate is eligible.
async fn demotion_secs_left(addr: std::net::SocketAddr, provider: &str) -> Option<u64> {
    let row = status_row(addr, provider).await;
    assert_eq!(
        row["status"], "demoted",
        "{provider} should read as demoted: {row}"
    );
    row["demotion_secs_left"].as_u64()
}

/// The index of `provider` inside the alias `fast`.
fn stats_index(state: &AppState, provider: &str) -> usize {
    state
        .model_map
        .get("fast")
        .unwrap()
        .iter()
        .find(|candidate| candidate.provider_name == provider)
        .unwrap()
        .stats_index
}

/// Transport failures counted against one candidate, from the Prometheus
/// counter. This is how a test names the box a request reached when the
/// response is a proxy-written `502` and so carries no provider header.
fn transport_errors(state: &AppState, provider: &str) -> u64 {
    state
        .metrics
        .errors_total
        .with_label_values(&["fast", provider, &format!("m-{}", provider)])
        .get()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The ticket's headline: a refused connection starts a window, and for that
/// window the box takes no new sessions at all.
///
/// The box is the weight-19 one, so every placement it is skipped for is the
/// rule working against the weight: a demoted candidate holds no share, not
/// even a probe slice, and the alias's traffic goes to the box at a
/// nineteenth the weight.
#[tokio::test]
async fn a_connect_failure_demotes_the_box_for_the_configured_window() {
    let alpha_port = refused_port().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // Weight 19 to 1, so the first turn is alpha's: it is unreachable, and the
    // caller gets the 502 the proxy has to write because there is nothing to relay.
    let (status, _) = served_by(addr, "fast").await;
    assert_eq!(status, 502, "an unreachable box is a bad gateway");
    assert_eq!(
        transport_errors(&state, "alpha"),
        1,
        "the failure belongs to alpha"
    );

    let left = demotion_secs_left(addr, "alpha").await;
    assert_eq!(
        left,
        Some(10),
        "the default window is ten seconds, and /status says so"
    );

    // Every turn of the next ten requests is beta's: alpha is in the running
    // for none of them, whatever its weight buys it.
    for turn in 0..10 {
        let (status, provider) = served_by(addr, "fast").await;
        assert_eq!(status, 200, "turn {turn} should be served: {provider}");
        assert_eq!(
            provider, "beta",
            "a demoted box takes no placements, not even a probe share"
        );
    }
    assert_eq!(
        transport_errors(&state, "alpha"),
        1,
        "alpha was not contacted again during its window"
    );

    // The eligible box is untouched by any of this.
    let beta = status_row(addr, "beta").await;
    assert_eq!(beta["status"], "warm", "beta served ten turns: {beta}");
    assert!(
        beta["demotion_secs_left"].is_null(),
        "beta has no window: {beta}"
    );

    beta_up.abort();
}

/// A read timeout is the other transport failure a self-hosted box produces
/// most often: it accepted the connection, took the request, and stopped.
#[tokio::test]
async fn a_read_timeout_demotes_the_box() {
    let (alpha_port, alpha_up) = silent_upstream().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(
        &alpha_and_beta(alpha_port, beta_port),
        // Long enough that reading the window can never lose a race against
        // the timeout, short enough that the test stays quick.
        proxy_client(Duration::from_millis(600)),
    );
    let (addr, _shutdown) = start_server(state.clone());

    let (status, _) = served_by(addr, "fast").await;
    assert_eq!(status, 502, "a box that never answers is an upstream error");

    assert_eq!(
        demotion_secs_left(addr, "alpha").await,
        Some(10),
        "a timeout starts the same window a refused connection does"
    );

    let (status, provider) = served_by(addr, "fast").await;
    assert_eq!((status, provider.as_str()), (200, "beta"));

    alpha_up.abort();
    beta_up.abort();
}

/// A box that answered is a box that is up. `500` is relayed to the caller
/// verbatim and costs the box nothing: it keeps every placement its weight
/// declares, which is the same claim the first test makes in reverse.
#[tokio::test]
async fn an_upstream_500_leaves_the_box_in_full_possession_of_its_placements() {
    let (alpha_port, alpha_up) = status_upstream("500 Internal Server Error").await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    for turn in 0..10 {
        let (status, provider) = served_by(addr, "fast").await;
        assert_eq!(
            (status, provider.as_str()),
            (500, "alpha"),
            "turn {turn}: the upstream's own status, from the box that answered"
        );
    }

    let alpha = status_row(addr, "alpha").await;
    assert_ne!(
        alpha["status"], "demoted",
        "a box that answers 500 ten times is not demoted: {alpha}"
    );
    assert!(
        alpha["demotion_secs_left"].is_null(),
        "an upstream status starts no window: {alpha}"
    );
    assert_eq!(
        alpha["routing"]["weight"], 19,
        "and it keeps the weight it declared: {alpha}"
    );

    // The success-rate EWMA is still recorded, because it is reported.
    assert!(
        alpha["observed"]["error_rate"].as_f64().unwrap() > 0.9,
        "the failures are visible to an operator: {alpha}"
    );
    assert_eq!(
        transport_errors(&state, "alpha"),
        0,
        "an upstream 500 is not a transport failure"
    );

    alpha_up.abort();
    beta_up.abort();
}

/// The window is a timer, not a feedback loop: when the clock passes its end
/// the box is fully eligible at once.
///
/// The turn immediately after the window ends is alpha's, at its full declared
/// weight — no probe slice, no ramp, nothing carried over from the demotion.
/// The ten turns after that split 19:1 as the config declares, which is the
/// part a ladder would get wrong.
#[tokio::test]
async fn a_demoted_box_is_fully_eligible_at_its_full_weight_when_its_window_ends() {
    // Alpha hangs up on the first request, then answers: unreachable long
    // enough to be demoted, and able to serve the moment its window ends.
    let (alpha_port, alpha_up) = hangup_then_status(1, "200 OK").await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let (status, _) = served_by(addr, "fast").await;
    assert_eq!(status, 502, "the box hung up mid-request");
    assert_eq!(demotion_secs_left(addr, "alpha").await, Some(10));

    // Inside the window the weight-1 box has the alias to itself.
    let (_, provider) = served_by(addr, "fast").await;
    assert_eq!(provider, "beta");

    // Cross the end of the window without waiting for it.
    state.tracker.clock().advance(Duration::from_secs(10));
    let alpha = status_row(addr, "alpha").await;
    assert!(
        alpha["demotion_secs_left"].is_null(),
        "the window is over: {alpha}"
    );

    // The very next turn is alpha's, at full weight: the first new session is
    // its probe, and it is a full-sized session.
    let (status, provider) = served_by(addr, "fast").await;
    assert_eq!(
        (status, provider.as_str()),
        (200, "alpha"),
        "an expired window leaves nothing behind"
    );

    // And the split is the declared one straight away, not one climbing back
    // toward it.
    let mut to_alpha = 0usize;
    for _ in 0..10 {
        if served_by(addr, "fast").await.1 == "alpha" {
            to_alpha += 1;
        }
    }
    assert!(
        to_alpha >= 9,
        "a 19:1 alias is 9 or 10 alpha turns out of 10, not a recovery ramp: {to_alpha}"
    );

    alpha_up.abort();
    beta_up.abort();
}

/// Every candidate demoted: the turn still goes out, on the heaviest demoted
/// box, and the caller receives that box's own response.
///
/// The status is the upstream's `503` with the upstream's own body, not a
/// proxy-written `502`: a dead box's error is more informative than one the
/// proxy invented, so the proxy keeps out of the way.
#[tokio::test]
async fn when_every_box_is_demoted_the_heaviest_one_serves_and_answers_in_its_own_voice() {
    // Both boxes hang up once, which demotes both, then answer 503.
    let (alpha_port, alpha_up) = hangup_then_status(1, "503 Service Unavailable").await;
    let (beta_port, beta_up) = hangup_then_status(1, "503 Service Unavailable").await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // Weight 19 takes the first turn, then weight 1 takes the second: both
    // boxes are now demoted.
    assert_eq!(served_by(addr, "fast").await.0, 502);
    assert_eq!(served_by(addr, "fast").await.0, 502);
    assert_eq!(demotion_secs_left(addr, "alpha").await, Some(10));
    assert_eq!(demotion_secs_left(addr, "beta").await, Some(10));

    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&request("fast"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "the caller gets the box's own status, not a proxy-invented 502"
    );
    assert_eq!(
        resp.headers()
            .get("x-sturnus-provider")
            .unwrap()
            .to_str()
            .unwrap(),
        "alpha",
        "the heaviest demoted box serves the turn"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("the box's own"),
        "the upstream's own body is relayed: {body}"
    );

    alpha_up.abort();
    beta_up.abort();
}

/// The window length is the config's, not a constant: `demotion_secs = 3` is a
/// three-second window, and `/status` counts down from three.
#[tokio::test]
async fn the_window_is_the_length_the_config_declares() {
    let alpha_port = refused_port().await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(
        &alpha_and_beta_window(alpha_port, beta_port, 3),
        caller_client(),
    );
    let (addr, _shutdown) = start_server(state.clone());

    assert_eq!(served_by(addr, "fast").await.0, 502);
    assert_eq!(
        demotion_secs_left(addr, "alpha").await,
        Some(3),
        "three seconds, as configured"
    );

    // Still inside it: beta has the alias.
    assert_eq!(served_by(addr, "fast").await.1, "beta");

    // Two seconds in, the window is still running; the third second ends it.
    state.tracker.clock().advance(Duration::from_secs(2));
    assert_eq!(demotion_secs_left(addr, "alpha").await, Some(1));
    assert_eq!(served_by(addr, "fast").await.1, "beta");

    state.tracker.clock().advance(Duration::from_secs(2));
    let (status, _) = served_by(addr, "fast").await;
    assert_eq!(status, 502, "alpha is in the running again");
    assert_eq!(
        transport_errors(&state, "alpha"),
        2,
        "and it was alpha that was reached"
    );

    beta_up.abort();
}

/// A window of zero seconds is a config that asked for no demotion at all: the
/// box is eligible again the moment the failure is recorded, so the alias is
/// left exactly as its weights declare.
#[tokio::test]
async fn a_zero_length_window_demotes_no_one() {
    let (alpha_port, alpha_up) = hangup_then_status(1, "200 OK").await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(
        &alpha_and_beta_window(alpha_port, beta_port, 0),
        caller_client(),
    );
    let (addr, _shutdown) = start_server(state.clone());

    assert_eq!(served_by(addr, "fast").await.0, 502);
    let alpha = status_row(addr, "alpha").await;
    assert!(
        alpha["demotion_secs_left"].is_null(),
        "a zero window has already expired: {alpha}"
    );

    // The next turn is alpha's again: nothing was skipped on the way.
    let (status, provider) = served_by(addr, "fast").await;
    assert_eq!((status, provider.as_str()), (200, "alpha"));

    alpha_up.abort();
    beta_up.abort();
}

/// A demotion is a placement gate, not a pin-breaker: a session that named its
/// box keeps riding it while that box is demoted, because the operator's choice
/// outranks the proxy's opinion of the box.
#[tokio::test]
async fn a_demoted_box_keeps_serving_the_sessions_pinned_to_it() {
    let (alpha_port, alpha_up) = hangup_then_status(1, "200 OK").await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    // The pin is what put the session on alpha, and the hangup demotes it.
    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("x-session-affinity", "alpha/m-alpha")
        .json(&request("fast"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "the pinned box hung up on this turn");
    assert_eq!(demotion_secs_left(addr, "alpha").await, Some(10));

    // The next turn of the same session still goes to alpha.
    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("x-session-affinity", "alpha/m-alpha")
        .json(&request("fast"))
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
        "alpha",
        "a demotion does not break a pin"
    );

    // A new session, with no pin, is placed somewhere else.
    assert_eq!(served_by(addr, "fast").await.1, "beta");

    alpha_up.abort();
    beta_up.abort();
}

/// The window is per candidate: demoting one box of an alias leaves the other's
/// placements and reported state alone, and the two rows of `/status` say
/// different things.
#[tokio::test]
async fn a_demotion_is_a_property_of_one_candidate() {
    let (alpha_port, alpha_up) = hangup_then_status(1, "200 OK").await;
    let (beta_port, beta_up) = eager_upstream().await;
    let state = state_for(&alpha_and_beta(alpha_port, beta_port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    assert_eq!(served_by(addr, "fast").await.0, 502);

    let alpha = status_row(addr, "alpha").await;
    let beta = status_row(addr, "beta").await;
    assert_eq!(alpha["status"], "demoted");
    // Beta has not been contacted at all, so it is cold: the window is alpha's
    // and alpha's alone.
    assert_eq!(beta["status"], "cold");
    assert_eq!(alpha["demotion_secs_left"], 10);
    assert!(beta["demotion_secs_left"].is_null());
    // Both rows still report the weights they declared: the window is a gate on
    // placements, not a discount on the schedule.
    assert_eq!(alpha["routing"]["weight"], 19);
    assert_eq!(beta["routing"]["weight"], 1);

    // The demoted box's index is the one carrying the window; the other
    // candidate's is untouched.
    assert!(state.tracker.is_demoted(stats_index(&state, "alpha")));
    assert!(!state.tracker.is_demoted(stats_index(&state, "beta")));

    alpha_up.abort();
    beta_up.abort();
}
