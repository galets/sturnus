//! The same-instant burst, end to end: concurrent streaming requests against
//! an alias of equal-latency candidates spread evenly over its boxes.
//!
//! Why this needs the real dispatcher and not the router's unit tests: the
//! property lives in the gap between two statements of one routing decision —
//! read every candidate's in-flight count, then add this request to one of
//! them. A single-threaded test cannot exercise that gap no matter how it
//! orders its calls, because one thread is never inside two picks at once. So
//! this drives a proxy listener on a multi-threaded runtime over real
//! connections, which is the situation the queue adjustment is for: an agent
//! firing a fan-out of parallel calls, not a client with one request in flight.
//!
//! Every mock box holds its stream open until the test releases it, so a burst
//! is provably concurrent rather than merely fast: when the distribution is
//! read, every request in the burst is outstanding at the same instant. That is
//! what makes a lopsided answer a routing fault and not a timing artefact — a
//! candidate at zero while another carries two of the burst is a pick that
//! routed onto a queue it could not see.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

use common::CapturedLogs;

mod common;

/// How long anything is allowed to take to settle. Local sockets; generous on
/// purpose, so a failure means a real fault rather than a slow machine.
const SETTLE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// The alias and its mock boxes
// ---------------------------------------------------------------------------

/// One alias, `n` candidates, one held-stream mock box per provider (`box0` ..
/// `boxN-1`). Every box is identical in every way the router can see — same
/// model name, same latency, nothing warmed — so the only thing that can tell
/// two of them apart is who is already carrying a request.
fn alias_config(ports: &[u16], alias: &str) -> String {
    let mut toml = String::new();
    for (i, port) in ports.iter().enumerate() {
        toml.push_str(&format!(
            r#"
[provider.box{i}]
base_url = "http://127.0.0.1:{port}"
api_key = "fake"
"#
        ));
    }
    let candidates: Vec<String> = (0..ports.len())
        .map(|i| format!("{{ provider = \"box{i}\", model = \"m\" }}"))
        .collect();
    toml.push_str(&format!(
        "\n[model]\n{alias} = [{}]\n",
        candidates.join(", ")
    ));
    toml
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

    Arc::new(AppState {
        model_map,
        tracker,
        rr_state,
        client,
        exploit_k: 3.0,
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

/// One mock box: takes a request, starts an SSE stream, then holds it open
/// until the test hands over a permit. The hold is the window in which the
/// request is provably outstanding, and the test owns it — nothing about a held
/// stream depends on how fast the router was.
///
/// Returns the port, a counter of requests the box has taken, and the permit
/// semaphore that finishes its held streams. A permit is spent on one stream
/// and forgotten, so releasing k permits finishes exactly k streams.
async fn held_box() -> (u16, Arc<AtomicUsize>, Arc<Semaphore>) {
    let (listener, port) = common::bind().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream_release = Arc::clone(&release);
    tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            let release = Arc::clone(&upstream_release);
            let hits = Arc::clone(&upstream_hits);
            tokio::spawn(async move {
                if common::read_request(&mut conn).await.is_err() {
                    return;
                }
                hits.fetch_add(1, Ordering::Relaxed);
                sse_head(&mut conn).await;
                sse_chunk(&mut conn, "data: a\n\n").await;
                let Ok(permit) = release.acquire().await else {
                    return;
                };
                permit.forget();
                sse_chunk(&mut conn, "data: z\n\n").await;
                sse_end(&mut conn).await;
            });
        }
    });
    (port, hits, release)
}

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

// ---------------------------------------------------------------------------
// Firing requests, reading the distribution
// ---------------------------------------------------------------------------

/// A caller-side client. `init_crypto` comes first: a reqwest client needs a
/// process rustls provider, and this one is built before the state that would
/// otherwise install it.
fn caller_client() -> reqwest::Client {
    sturnus::init_crypto();
    reqwest::Client::new()
}

fn streaming_request(alias: &str) -> serde_json::Value {
    serde_json::json!({"model": alias, "messages": [], "stream": true})
}

/// The alias under test, its boxes, and the proxy in front of them.
struct Boxes {
    /// The alias under test, also the handle this test's routing decisions are
    /// found by in the shared log buffer.
    alias: String,
    state: Arc<AppState>,
    addr: std::net::SocketAddr,
    hits: Vec<Arc<AtomicUsize>>,
    releases: Vec<Arc<Semaphore>>,
    /// Held so the server stops when the test's `Boxes` does.
    _server: oneshot::Sender<()>,
    /// The burst's open responses. Held by the caller: hanging up ends a
    /// request, and an ended request stops being load.
    burst: Vec<reqwest::Response>,
}

impl Boxes {
    async fn start(count: usize, alias: &str) -> Self {
        let mut ports = Vec::new();
        let mut hits = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..count {
            let (port, hit, release) = held_box().await;
            ports.push(port);
            hits.push(hit);
            releases.push(release);
        }
        let state = state_for(&alias_config(&ports, alias), caller_client());
        let (addr, _shutdown) = start_server(state.clone());
        Self {
            alias: alias.to_string(),
            state,
            addr,
            hits,
            releases,
            _server: _shutdown,
            burst: Vec::new(),
        }
    }

    /// Fire `n` streaming requests at the alias together — `n` separate
    /// connections, every handshake and body in flight at once, driven by one
    /// `join_all` so nothing between them is sequential — and return the
    /// in-flight distribution the alias settled on, per box in provider order.
    ///
    /// The responses stay unread and unconsumed on purpose: the burst has to
    /// still be outstanding when the distribution is read.
    async fn burst(&mut self, n: usize) -> Vec<usize> {
        // What the boxes had taken before this burst: their counters are
        // cumulative, and the wait below is on this burst's arrivals only.
        let arrived_before: usize = self
            .hits
            .iter()
            .map(|hit| hit.load(Ordering::Relaxed))
            .sum();

        let client = caller_client();
        let sends: Vec<_> = (0..n)
            .map(|_| {
                client
                    .post(format!("http://{}/v1/chat/completions", self.addr))
                    .json(&streaming_request(&self.alias))
                    .send()
            })
            .collect();
        let responses = futures_util::future::join_all(sends).await;
        for resp in responses {
            let resp = resp.unwrap();
            assert_eq!(resp.status(), 200, "a held upstream answers 200");
            self.burst.push(resp);
        }

        // Every request of the burst is now outstanding: counted against the
        // candidate it was dispatched to, and no stream can have finished
        // because nothing has been released.
        self.wait_for_burst(n, arrived_before).await
    }

    /// Poll until the alias has `n` requests outstanding and each of them has
    /// reached the box it was dispatched to. The tracker counts a request the
    /// moment it is committed to a candidate, which is before the upstream sees
    /// it, so waiting on the count alone would sample a distribution while part
    /// of the burst was still on the wire to its box.
    async fn wait_for_burst(&self, n: usize, arrived_before: usize) -> Vec<usize> {
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            let queue = self.in_flight_by_box();
            let arrived: usize = self
                .hits
                .iter()
                .map(|hit| hit.load(Ordering::Relaxed))
                .sum();
            if queue.iter().sum::<usize>() == n && arrived == arrived_before + n {
                return queue;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the burst never settled: {n} requests outstanding expected, the tracker says \
                 {queue:?} and the boxes have taken {} of them",
                arrived - arrived_before,
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The live in-flight count of every candidate, in provider order.
    fn in_flight_by_box(&self) -> Vec<usize> {
        let mut counts: Vec<(String, usize)> = self
            .state
            .model_map
            .iter()
            .map(|(_, candidate)| {
                (
                    candidate.provider_name.clone(),
                    self.state.tracker.in_flight(candidate.stats_index),
                )
            })
            .collect();
        counts.sort_unstable_by_key(|(provider, _)| provider.clone());
        counts.into_iter().map(|(_, count)| count).collect()
    }

    /// `GET /status` → `in_flight` per candidate, in provider order. Reading
    /// the same distribution off the endpoint is the point: an operator
    /// debugging a burst has to see the queue the router routed on.
    async fn in_flight_from_status(&self) -> Vec<u64> {
        let text = reqwest::get(format!("http://{}/status", self.addr))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        let mut rows: Vec<(String, u64)> = body["candidates"]
            .as_array()
            .expect("candidates array")
            .iter()
            .map(|row| {
                (
                    row["provider"].as_str().unwrap_or_default().to_string(),
                    row["in_flight"].as_u64().unwrap_or(u64::MAX),
                )
            })
            .collect();
        rows.sort_unstable_by_key(|(provider, _)| provider.clone());
        rows.into_iter().map(|(_, count)| count).collect()
    }

    /// Let every held stream finish, read the responses to their ends (which is
    /// what lets each relay complete and return its count), and wait for the
    /// alias to be idle again.
    async fn settle(&mut self) {
        // One permit finishes one held stream, so hand out as many as the burst
        // had requests; the extras are never acquired.
        let held = self.burst.len();
        for release in &self.releases {
            release.add_permits(held);
        }

        let burst = std::mem::take(&mut self.burst);
        let drain = async move {
            for response in burst {
                let mut stream = response.bytes_stream();
                while let Some(next) = stream.next().await {
                    let _ = next.unwrap();
                }
            }
        };
        tokio::time::timeout(SETTLE, drain)
            .await
            .expect("a released stream should reach its end, not hang");

        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            let queue = self.in_flight_by_box();
            if queue.iter().all(|count| *count == 0) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the in-flight counts never came back to zero: {queue:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Logging, and what it proves
// ---------------------------------------------------------------------------

/// Run the proxy at the debug level it runs at in production, capturing every
/// line.
///
/// Not for its own sake: a routing decision logs the shares it drew from, and
/// the shares are the queue-adjusted weights, so reading them back is the only
/// way to see inside a decision the proxy has already made.
///
/// It matters twice over. The window this fix closes is a few instructions wide
/// — read every candidate's in-flight count, then add this request to one of
/// them. Do those two things with a log line in between, as this file's
/// predecessor did, and that line costs more than both of them together: with a
/// subscriber installed, the picks of a burst land inside one another's window
/// and the alias gets a lopsided split. With no subscriber installed the
/// callsite is a no-op, and the same broken code races without ever showing it.
/// So install the subscriber, and assert on what it recorded.
fn proxy_logs() -> &'static CapturedLogs {
    static LOGS: std::sync::OnceLock<CapturedLogs> = std::sync::OnceLock::new();
    let logs = LOGS.get_or_init(CapturedLogs::default);
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(logs.clone())
        .try_init();
    logs
}

/// The shares each routing decision logged, oldest first — one entry per
/// request the proxy dispatched.
fn logged_shares(logs: &CapturedLogs, alias: &str) -> Vec<String> {
    let handle = format!("alias={alias}");
    logs.text()
        .lines()
        .filter(|line| line.contains("routing decision") && line.contains(&handle))
        .filter_map(|line| {
            line.split_once("shares=")
                .map(|(_, shares)| shares.trim().to_string())
        })
        .collect()
}

/// Every pick of a burst reads a snapshot of the alias's load, and under a
/// reservation taken as the pick is made, a later pick's snapshot has to be
/// one request heavier than an earlier one's. So for a pair — the idle alias,
/// then an alias carrying one request — the two logged share sets cannot match,
/// and a match is the race written down: the second pick scored counts the
/// first had already spent.
///
/// Sound for a pair only, and only because the two snapshots differ: a load
/// that sits evenly on the alias renders as the same even split whether it is
/// four boxes carrying one request each or none at all, so equality means
/// nothing once a burst has been round the alias more than once. The bigger
/// burst therefore asserts the distribution and leaves this to the pair.
fn assert_each_pick_saw_its_own_snapshot(alias: &str, expected: usize) {
    let shares = logged_shares(proxy_logs(), alias);
    assert_eq!(
        shares.len(),
        expected,
        "one routing decision per request of the burst"
    );
    let mut unique = shares.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        shares.len(),
        "every pick of the burst has to score its own in-flight snapshot; two of them scored \
         {shares:?}, which is only possible if one read counts another had already reserved"
    );
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// The CHANGELOG's headline, end to end: a same-instant pair of streaming
/// requests against an alias of two interchangeable boxes splits one per box,
/// instead of both queueing behind whichever box the EWMA happened to like.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_same_instant_pair_splits_one_per_box() {
    let mut boxes = Boxes::start(2, "pair").await;
    proxy_logs();

    // One request per candidate: with a full alias and an empty one, "at most
    // one per candidate" and "one per candidate" are the same statement, so a
    // box left at zero is by itself the failure.
    let queue = boxes.burst(2).await;
    assert_eq!(
        queue,
        vec![1; 2],
        "a same-instant pair against two equal candidates must split one per box; the in-flight \
         distribution was {queue:?} (spread {}, so a box is carrying work its twin was free for)",
        queue.iter().max().unwrap_or(&0) - queue.iter().min().unwrap_or(&0),
    );
    assert_eq!(
        boxes.in_flight_from_status().await,
        vec![1; 2],
        "/status must show the one-per-box queue the router just routed on"
    );
    assert_each_pick_saw_its_own_snapshot("pair", 2);

    boxes.settle().await;
}

/// The same property with a burst larger than the alias, which is what an agent
/// fanning out parallel calls actually produces: eight concurrent requests
/// against four equal candidates, and the queue still lands perfectly even —
/// two per box, no box skipped, none holding three while another has room.
///
/// Both tests assert exact equality because that is what the router promises an
/// alias of interchangeable boxes: once a pick has been made, that box costs
/// `(1 + 1)^k` = 8x what its idle twins cost at the default sharpness, so the
/// next pick has eight times less reason to go there. The exact split for these
/// two bursts is the deterministic Weyl walk's doing too, and it is pinned
/// without any concurrency in sight — `router::tests` reproduces the pick
/// sequences for equal candidates — so a lopsided distribution here means a
/// pick scored a queue that was not there yet, not that a coin came up wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_twice_the_size_of_its_alias_still_splits_evenly() {
    let mut boxes = Boxes::start(4, "eight").await;
    proxy_logs();

    let queue = boxes.burst(8).await;
    assert_eq!(
        queue,
        vec![2; 4],
        "eight same-instant requests against four equal candidates must split two per box; the \
         in-flight distribution was {queue:?} (spread {}, so a box is holding more than its even \
         share while another has room)",
        queue.iter().max().unwrap_or(&0) - queue.iter().min().unwrap_or(&0),
    );
    assert_eq!(
        boxes.in_flight_from_status().await,
        vec![2; 4],
        "/status must show the even queue distribution the router just routed on"
    );

    boxes.settle().await;
}
