//! The per-candidate in-flight count: what it counts, and the fact that every
//! way a request can end also returns it.
//!
//! Each test drives a request to a real proxy listener against a raw-socket
//! mock upstream, and reads the count straight off the tracker (which is what
//! `/status` and `sturnus_in_flight_requests` are rendered from). The shape is
//! always the same: get the request into a state where it is provably
//! outstanding — assert `1` — then end it by whatever path is under test and
//! assert `0`. Waiting for `1` first is what keeps the `0` honest: a count that
//! was never incremented would pass the second half of every test here for the
//! wrong reason.
//!
//! The paths covered are the ones a counter is usually wrong on: a caller that
//! hangs up mid-stream, a stream that stalls or dies mid-body, a connect
//! failure, an upstream timeout, an upstream 5xx, and normal completion.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::tracker::Tracker;

mod common;

/// How long a count is allowed to take coming back. Generous: these are local
/// sockets, but the point is to fail loudly rather than flake.
const SETTLE: Duration = Duration::from_secs(5);

/// One alias, one candidate, at `port`.
fn one_candidate(port: u16) -> String {
    format!(
        r#"
[provider.test]
base_url = "http://127.0.0.1:{port}"
api_key = "fake"

[model]
fast = [{{ provider = "test", model = "test-model" }}]
"#
    )
}

/// The same single box, reachable through two aliases: the count is a property
/// of the box, so both of its rows report one number rather than one each.
fn shared_across_aliases(port: u16) -> String {
    format!(
        r#"
[provider.test]
base_url = "http://127.0.0.1:{port}"
api_key = "fake"

[model]
fast = [{{ provider = "test", model = "test-model" }}]
cheap = [{{ provider = "test", model = "test-model" }}]
"#
    )
}

fn state_for(config_toml: &str, client: reqwest::Client) -> Arc<AppState> {
    sturnus::init_crypto();
    let config: sturnus::config::Config = toml::from_str(config_toml).unwrap();

    let mut tracker = Tracker::new(0.3, 0.5);
    let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
    let mut rr_state = RoundRobinState::new();
    for alias in config.model.keys() {
        rr_state.register_alias(alias.clone());
    }

    let metrics = Metrics::new();
    // As `main` does: an idle candidate exports a `0` series rather than no
    // series, so "nothing in flight" and "never looked" are distinguishable.
    let triples: Vec<(&str, &str, &str)> = model_map
        .iter()
        .map(|(alias, c)| (alias, c.provider_name.as_str(), c.model.as_str()))
        .collect();
    metrics.init_zero(&triples);

    Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        client,
        exploit_k: 3.0,
        gcp_token_provider: None,
        budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
        metrics,
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
// Reading the count
// ---------------------------------------------------------------------------

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
        let got = in_flight(state, provider);
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{provider}'s in-flight count never reached {want}; it is stuck at {got}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let resp = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap()
}

/// `GET /status`, for the one row of `alias`.
async fn status_row(addr: std::net::SocketAddr, alias: &str) -> serde_json::Value {
    let body: serde_json::Value = serde_json::from_str(&get(addr, "/status").await).unwrap();
    body["candidates"]
        .as_array()
        .expect("candidates array")
        .iter()
        .find(|row| row["alias"] == alias)
        .unwrap_or_else(|| panic!("no /status row for {alias}: {body}"))
        .clone()
}

/// The exported value of `sturnus_in_flight_requests` for one label set.
fn gauge_value(metrics: &str, alias: &str, provider: &str, model: &str) -> Option<f64> {
    metrics.lines().find_map(|line| {
        let (labels, value) = line.split_once(' ')?;
        let name = labels.split_once('{')?.0;
        if name != "sturnus_in_flight_requests" {
            return None;
        }
        for (key, want) in [("alias", alias), ("provider", provider), ("model", model)] {
            if !labels.contains(&format!("{key}=\"{want}\"")) {
                return None;
            }
        }
        value.trim().parse().ok()
    })
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

/// The proxy's own upstream client, with the timeouts `main` configures.
/// `read_timeout` resets on each successful read, so it is what fires while a
/// candidate is stalled before or during a body.
fn proxy_client(read_timeout: Duration) -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(read_timeout)
        .build()
        .unwrap()
}

/// POST a chat completion through the proxy.
async fn proxy_post(
    addr: std::net::SocketAddr,
    body: serde_json::Value,
) -> Result<reqwest::Response, reqwest::Error> {
    caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&body)
        .send()
        .await
}

/// A non-streaming request body for `alias`.
fn request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": []})
}

/// A streaming one.
fn streaming_request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": [], "stream": true})
}

/// Chunked SSE framing: the response stays open until the mock ends it.
async fn sse_head(conn: &mut TcpStream) {
    conn.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
    )
    .await
    .unwrap();
    conn.flush().await.unwrap();
}

async fn sse_chunk(conn: &mut TcpStream, data: &str) {
    conn.write_all(format!("{:x}\r\n{}\r\n", data.len(), data).as_bytes())
        .await
        .ok();
    conn.flush().await.ok();
}

/// Terminate the chunked body and close, so both sides are done with it.
async fn sse_end(conn: &mut TcpStream) {
    conn.write_all(b"0\r\n\r\n").await.ok();
    conn.flush().await.ok();
    conn.shutdown().await.ok();
}

/// Read a streamed response to the end. Returns the bytes as text and whether
/// the stream was cut short by an error instead of ending cleanly.
async fn drain<S>(stream: S) -> (String, bool)
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>>,
{
    tokio::pin!(stream);
    let mut text = String::new();
    let mut failed = false;
    while let Some(next) = stream.next().await {
        match next {
            Ok(chunk) => text.push_str(&String::from_utf8_lossy(&chunk)),
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    (text, failed)
}

/// Mock upstream that answers every request with an SSE stream and then holds
/// it open: one chunk, then it waits for a permit from `release` before
/// finishing the body. That hold is the window in which a request is provably
/// in flight, and how the test chooses to end the request — caller hangs up, or
/// the release comes — is up to the test.
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
                sse_head(&mut conn).await;
                sse_chunk(&mut conn, "data: a\n\n").await;
                // Held open until the test lets this one finish.
                let _ = release.acquire().await;
                sse_chunk(&mut conn, "data: z\n\n").await;
                sse_end(&mut conn).await;
            });
        }
    });
    (port, release, handle)
}

/// Mock upstream that reads a request and then never answers it, so the
/// candidate is working on a request that will only ever end in a timeout.
async fn silent_upstream() -> (u16, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let (reached_tx, reached_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        common::read_request(&mut conn).await.unwrap();
        let _ = reached_tx.send(());
        // Never responds: the only way this request ends is a timeout.
        tokio::time::sleep(Duration::from_secs(3600)).await;
    });
    (port, reached_rx, handle)
}

/// Mock upstream that reads a request, waits to be released, then answers with
/// `status_line` — a candidate that took the request and answered badly.
async fn error_status_upstream(
    status_line: &'static str,
) -> (
    u16,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (listener, port) = common::bind().await;
    let (reached_tx, reached_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        common::read_request(&mut conn).await.unwrap();
        let _ = reached_tx.send(());
        let _ = release_rx.await;
        common::write_status(&mut conn, status_line, r#"{"error":"overloaded"}"#).await;
    });
    (port, reached_rx, release_tx, handle)
}

/// Mock upstream that starts an SSE stream and then stalls mid-body forever:
/// the shape of a box that answered promptly and then died generation-wise.
async fn stalling_sse_upstream() -> (u16, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let (reached_tx, reached_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        common::read_request(&mut conn).await.unwrap();
        let _ = reached_tx.send(());
        sse_head(&mut conn).await;
        sse_chunk(&mut conn, "data: a\n\n").await;
        tokio::time::sleep(Duration::from_secs(3600)).await;
    });
    (port, reached_rx, handle)
}

/// Mock upstream that starts an SSE stream, declares a chunk bigger than it
/// sends, and hangs up: the body is undecodable, so the relay fails mid-flight.
async fn broken_sse_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        common::read_request(&mut conn).await.unwrap();
        sse_head(&mut conn).await;
        // A first chunk that arrives whole, so the relay is genuinely under
        // way before the body is cut out from under it.
        sse_chunk(&mut conn, "data: a\n\n").await;
        // Declares 16 bytes of chunk, sends 12, then closes mid-chunk.
        conn.write_all(b"10\r\ndata: part").await.ok();
        conn.flush().await.ok();
        conn.shutdown().await.ok();
    });
    (port, handle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The ticket's headline case: a caller that stops reading and hangs up while a
/// stream is open. The relay stream is dropped mid-flight, and dropping it is
/// what returns the count — there is no "stream ended" for anyone to notice.
#[tokio::test]
async fn a_stream_dropped_mid_flight_returns_the_in_flight_count() {
    let (port, release, upstream) = held_sse_upstream().await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let mut stream = resp.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&first).contains("data: a"),
        "the stream should have started"
    );

    wait_for_in_flight(&state, "test", 1).await;

    // The caller hangs up mid-stream: the response body is dropped, and the
    // upstream then pushes on into a caller that is gone.
    drop(stream);
    release.add_permits(1);

    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// Normal completion, seen from both observability surfaces: while the stream
/// is open the count reads 1 on the tracker, on `/status` and on `/metrics`,
/// and all three read 0 once the body has been relayed to the end.
#[tokio::test]
async fn a_streamed_response_reports_its_count_and_then_lets_it_go() {
    let (port, release, upstream) = held_sse_upstream().await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let mut stream = resp.bytes_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&first).contains("data: a"),
        "the stream should have started"
    );
    wait_for_in_flight(&state, "test", 1).await;

    let row = status_row(addr, "fast").await;
    assert_eq!(
        row["in_flight"], 1,
        "/status should show the outstanding request: {row}"
    );
    let metrics = get(addr, "/metrics").await;
    assert_eq!(
        gauge_value(&metrics, "fast", "test", "test-model"),
        Some(1.0),
        "the gauge should carry the live count"
    );

    release.add_permits(1);
    let (body, failed) = drain(stream).await;
    assert!(!failed, "the stream should end cleanly: {body}");
    assert!(
        body.contains("data: z"),
        "the rest of the stream is relayed too: {body}"
    );

    wait_for_in_flight(&state, "test", 0).await;
    let row = status_row(addr, "fast").await;
    assert_eq!(row["in_flight"], 0, "the count is back to idle: {row}");
    let metrics = get(addr, "/metrics").await;
    assert_eq!(
        gauge_value(&metrics, "fast", "test", "test-model"),
        Some(0.0),
        "a finished request must not leave the gauge set"
    );
    upstream.abort();
}

/// A request riding an `x-session-affinity` pin never reaches the router, so it
/// is easy to count only the routed half of traffic. Both are dispatched.
#[tokio::test]
async fn a_pinned_request_is_counted_too() {
    let (port, release, upstream) = held_sse_upstream().await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = caller_client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .header("x-session-affinity", "test/test-model")
        .json(&streaming_request("fast"))
        .send()
        .await
        .unwrap();
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();

    wait_for_in_flight(&state, "test", 1).await;
    release.add_permits(1);
    let (_, failed) = drain(stream).await;
    assert!(!failed, "the stream should end cleanly");
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// Two concurrent streams are two units of load, and each comes back on its
/// own: a count that double counted, or a release that ran twice, would end
/// this test somewhere other than `0`.
#[tokio::test]
async fn two_concurrent_streams_count_both_and_release_both() {
    let (port, release, upstream) = held_sse_upstream().await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let first = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let mut first_stream = first.bytes_stream();
    first_stream.next().await.unwrap().unwrap();
    wait_for_in_flight(&state, "test", 1).await;

    let second = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let mut second_stream = second.bytes_stream();
    second_stream.next().await.unwrap().unwrap();
    wait_for_in_flight(&state, "test", 2).await;

    release.add_permits(2);
    let (_, failed_first) = drain(first_stream).await;
    let (_, failed_second) = drain(second_stream).await;
    assert!(!failed_first && !failed_second, "both streams end cleanly");

    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// A candidate reachable through two aliases has one queue, not one per alias:
/// one request against the box reads `1` on both of its rows and on both label
/// sets, which is also why `sum()` over the alias label counts a shared box
/// once per alias it is reachable through.
#[tokio::test]
async fn a_candidate_shared_by_two_aliases_reports_one_count() {
    let (port, release, upstream) = held_sse_upstream().await;
    let state = state_for(&shared_across_aliases(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, streaming_request("cheap")).await.unwrap();
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();
    wait_for_in_flight(&state, "test", 1).await;

    for alias in ["fast", "cheap"] {
        let row = status_row(addr, alias).await;
        assert_eq!(
            row["in_flight"], 1,
            "{alias} reaches the same box, so it shows the same queue: {row}"
        );
    }
    let metrics = get(addr, "/metrics").await;
    for alias in ["fast", "cheap"] {
        assert_eq!(
            gauge_value(&metrics, alias, "test", "test-model"),
            Some(1.0),
            "both label sets of the shared candidate report the one count"
        );
    }

    release.add_permits(1);
    drain(stream).await;
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// A candidate that refuses the connection was still the candidate the request
/// was dispatched to — the error counter says so — and its count comes back
/// with the failure rather than staying behind.
#[tokio::test]
async fn a_connect_failure_returns_the_in_flight_count() {
    // A port nothing is listening on: connect is refused immediately.
    let refused = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    let state = state_for(&one_candidate(refused), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, request("fast")).await.unwrap();
    assert_eq!(resp.status(), 502, "a refused upstream is a bad gateway");

    let errors = state
        .metrics
        .errors_total
        .with_label_values(&["fast", "test", "test-model"])
        .get();
    assert_eq!(errors, 1, "the failure belongs to the candidate");
    wait_for_in_flight(&state, "test", 0).await;
}

/// An upstream 5xx is a completed response, not a leak: the count is returned
/// once its body has been read, and relayed to the caller either way.
#[tokio::test]
async fn an_upstream_error_status_returns_the_in_flight_count() {
    let (port, reached, release, upstream) = error_status_upstream("503 Service Unavailable").await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let pending = tokio::spawn(async move { proxy_post(addr, request("fast")).await });

    // The candidate has the request and has not answered: it is working on it.
    reached.await.unwrap();
    wait_for_in_flight(&state, "test", 1).await;

    release.send(()).unwrap();
    let resp = pending.await.unwrap().unwrap();
    assert_eq!(resp.status(), 503, "the upstream answer is relayed as-is");
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// A candidate that takes the request and never answers: the proxy's own
/// timeout ends it, and ends the count with it.
#[tokio::test]
async fn an_upstream_that_never_answers_returns_its_count_on_timeout() {
    let (port, reached, upstream) = silent_upstream().await;
    // Long enough that reading "the request is outstanding" can never lose a
    // race against the timeout, short enough that the test stays quick.
    let state = state_for(
        &one_candidate(port),
        proxy_client(Duration::from_millis(600)),
    );
    let (addr, _shutdown) = start_server(state.clone());

    let pending = tokio::spawn(async move { proxy_post(addr, request("fast")).await });

    reached.await.unwrap();
    wait_for_in_flight(&state, "test", 1).await;

    let resp = pending.await.unwrap().unwrap();
    assert_eq!(resp.status(), 502, "a timeout is an upstream error");
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// The failure mode a self-hosted box is most likely to have: it starts a
/// stream, then stops producing. The relay turns that into its terminating SSE
/// error event, and the count goes back with the stream.
#[tokio::test]
async fn a_stream_that_stalls_mid_flight_returns_its_count() {
    let (port, reached, upstream) = stalling_sse_upstream().await;
    // `read_timeout` resets on each successful read, so it fires while the
    // upstream is stalled mid-body: what a stalled generation looks like to
    // the proxy.
    let state = state_for(
        &one_candidate(port),
        proxy_client(Duration::from_millis(600)),
    );
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let mut stream = resp.bytes_stream();
    stream.next().await.unwrap().unwrap();
    reached.await.unwrap();

    wait_for_in_flight(&state, "test", 1).await;

    // The stalled upstream surfaces as the relay's own SSE error event, then
    // the response ends; either way the count must not survive it.
    let (body, _failed) = drain(stream).await;
    assert!(
        body.contains("proxy_error"),
        "the caller should still be told the stream died: {body}"
    );
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// Same, for a stream the upstream breaks rather than stalls: an undecodable
/// body mid-flight ends the relay, and the count.
#[tokio::test]
async fn a_stream_broken_mid_flight_returns_its_count() {
    let (port, upstream) = broken_sse_upstream().await;
    let state = state_for(&one_candidate(port), caller_client());
    let (addr, _shutdown) = start_server(state.clone());

    let resp = proxy_post(addr, streaming_request("fast")).await.unwrap();
    let stream = resp.bytes_stream();

    let (body, _failed) = drain(stream).await;
    assert!(
        body.contains("proxy_error"),
        "a broken stream still ends with an SSE error event: {body}"
    );
    wait_for_in_flight(&state, "test", 0).await;
    upstream.abort();
}

/// The gauge is zero-initialised, so an idle candidate is an observable zero
/// rather than a missing series that looks like "no data".
#[tokio::test]
async fn an_idle_candidate_exports_a_zero_gauge() {
    let state = state_for(&one_candidate(9999), caller_client());
    let (addr, _shutdown) = start_server(state);

    let metrics = get(addr, "/metrics").await;
    assert_eq!(
        gauge_value(&metrics, "fast", "test", "test-model"),
        Some(0.0),
        "the series should exist before any traffic"
    );
}
