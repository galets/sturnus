//! Regression tests for how caller-initiated disconnects are logged.
//!
//! A caller that hangs up mid-request, shuts down its write half, or closes an
//! idle keep-alive connection is normal traffic and must not fill the journal
//! with ERROR lines. A genuine connection fault (here: a write to a connection
//! the caller reset) must still be reported at ERROR.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tracing_subscriber::fmt::MakeWriter;

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::tracker::Tracker;

mod common;

/// A `MakeWriter` that keeps every formatted log line in memory.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `body` under a debug-level subscriber that captures into `logs`.
async fn capture_logs<F, T>(logs: CapturedLogs, body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(logs)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    body.await
}

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
        budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
        metrics: Metrics::new(),
        shutting_down: AtomicBool::new(false),
        api_keys: None,
    })
}

fn test_state(upstream_port: u16) -> Arc<AppState> {
    state_from_config(&format!(
        r#"
[provider.test]
base_url = "http://127.0.0.1:{upstream_port}"
api_key = "fake"

[model]
fast = [{{ provider = "test", model = "test-model" }}]
"#
    ))
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

/// Poll the shared log buffer until `predicate` holds, up to `timeout`.
async fn wait_for_log<F>(logs: &CapturedLogs, timeout: Duration, predicate: F) -> String
where
    F: Fn(&str) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let text = logs.text();
        if predicate(&text) {
            return text;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("log line not seen within {timeout:?}; captured logs:\n{text}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A caller that drops after sending only part of a request head: the peer
/// hangs up mid-message, which hyper reports as "connection closed before
/// message completed".
#[tokio::test]
async fn mid_request_disconnect_logs_no_error() {
    let logs = CapturedLogs::default();
    let state = test_state(9999);
    let (addr, _shutdown) = start_server(state.clone());

    capture_logs(logs.clone(), async move {
        let mut client = TcpStream::connect(addr).await.unwrap();
        let caller = client.local_addr().unwrap();
        client
            .write_all(b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 128\r\n")
            .await
            .unwrap();
        // Hang up mid-head without completing the blank line.
        client.shutdown().await.unwrap();
        drop(client);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while state.metrics.client_disconnects_total.get() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "benign disconnect was not counted; logs:\n{}",
                logs.text()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let captured = wait_for_log(&logs, Duration::from_secs(1), |text| {
            text.contains("client disconnected")
        })
        .await;

        assert!(
            !captured.contains("ERROR"),
            "a caller hanging up mid-request must not log at ERROR:\n{captured}"
        );
        assert!(
            captured.contains(&caller.to_string()),
            "the benign disconnect should be visible at debug with the caller address ({caller}):\n{captured}"
        );
    })
    .await;
}

/// A caller that hangs up after declaring a body but sending only part of it.
/// The body read fails with the same incomplete-message error.
#[tokio::test]
async fn truncated_body_disconnect_quiet() {
    let logs = CapturedLogs::default();
    let state = test_state(9999);
    let (addr, _shutdown) = start_server(state.clone());

    capture_logs(logs.clone(), async move {
        let mut client = TcpStream::connect(addr).await.unwrap();
        let caller = client.local_addr().unwrap();
        client
            .write_all(
                b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 128\r\n\r\n{\"model\":",
            )
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        drop(client);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while state.metrics.client_disconnects_total.get() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "benign disconnect was not counted; logs:\n{}",
                logs.text()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let captured = wait_for_log(&logs, Duration::from_secs(1), |text| {
            text.contains("client disconnected")
        })
        .await;
        assert!(
            !captured.contains("ERROR"),
            "a caller that truncates its request body must not log at ERROR:\n{captured}"
        );
        assert!(
            !captured.contains("WARN"),
            "a caller that truncates its request body should be quiet:\n{captured}"
        );
        assert!(
            captured.contains(&caller.to_string()),
            "the benign disconnect should name the caller ({caller}):\n{captured}"
        );
        // Give a stray second report (handler + connection task) time to show up.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            state.metrics.client_disconnects_total.get(),
            1,
            "a disconnect must be counted exactly once:\n{}",
            logs.text()
        );
    })
    .await;
}

/// A caller that closes an idle keep-alive connection after a completed
/// response: hyper reports a clean EOF, not a fault.
#[tokio::test]
async fn idle_keep_alive_close_logs_no_error() {
    let logs = CapturedLogs::default();
    let state = test_state(9999);
    let (addr, _shutdown) = start_server(state.clone());

    capture_logs(logs.clone(), async move {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        // Read the complete response so the connection is idle, then close.
        let mut response = Vec::new();
        let mut buf = [0u8; 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while !String::from_utf8_lossy(&response).contains(r#"{"status":"ok"}"#) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "health response never arrived"
            );
            let n = client.read(&mut buf).await.unwrap();
            assert_ne!(n, 0, "server closed before sending the response");
            response.extend_from_slice(&buf[..n]);
        }
        drop(client);

        // Give the server's connection task time to observe the EOF.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let captured = logs.text();
        assert!(
            !captured.contains("ERROR"),
            "closing an idle keep-alive connection must not log at ERROR:\n{captured}"
        );
        assert_eq!(
            state.metrics.client_disconnects_total.get(),
            0,
            "a clean idle close is not a disconnect to count:\n{captured}"
        );
    })
    .await;
}

/// Mock upstream that accepts one request, then waits for the test to release
/// it before responding — leaving a window in which the caller can vanish.
async fn slow_upstream() -> (
    u16,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (listener, port) = common::bind().await;
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        common::read_request(&mut conn).await.unwrap();
        let _ = ready_tx.send(());
        let _ = release_rx.await;
        common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
    });
    (port, ready_rx, release_tx, handle)
}

/// A genuine connection fault — the caller resets the connection while the
/// server is writing the response — must still be reported at ERROR.
#[tokio::test]
#[allow(deprecated)] // SO_LINGER is exactly how a test forces a reset.
async fn response_write_failure_logs_error() {
    let logs = CapturedLogs::default();
    let (upstream_port, upstream_ready, upstream_release, upstream_handle) = slow_upstream().await;
    let state = test_state(upstream_port);
    let (addr, _shutdown) = start_server(state);

    capture_logs(logs.clone(), async move {
        let mut client = TcpStream::connect(addr).await.unwrap();
        let body = br#"{"model":"fast","messages":[]}"#;
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();

        // The request reached the upstream; now reset the caller side so the
        // response write fails.
        upstream_ready.await.unwrap();
        client.set_linger(Some(Duration::ZERO)).unwrap();
        drop(client);
        let _ = upstream_release.send(());

        let captured = wait_for_log(&logs, Duration::from_secs(3), |text| {
            text.contains("connection error")
        })
        .await;

        assert!(
            captured.contains("ERROR"),
            "a failed response write must still log at ERROR:\n{captured}"
        );
    })
    .await;

    upstream_handle.abort();
}
