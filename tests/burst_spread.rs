//! The same-instant burst, end to end: concurrent streaming requests against
//! an alias whose boxes the router cannot tell apart spread evenly over them.
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
//! # What the alias's state is while a burst is placed, and how that is made
//! true rather than hoped for
//!
//! The assertions below are exact — one request per box, two per box — so they
//! are only honest if the alias is in a known state when the picks are made.
//! Two things had to be built to make that a property of the harness rather
//! than a coin flip, and the file's predecessor had neither:
//!
//! * **The boxes are warmed to one identical sample before the burst**
//!   ([`Boxes::warm`]), so "interchangeable boxes" is a state the test puts
//!   there and not an assumption about boxes that have never answered.
//! * **No box sends its first byte until the whole burst has been placed**
//!   ([`held_box`]'s first-byte gate), and that is what freezes the state.
//!   `record_success` fires at time-to-first-byte ([`sturnus::server`]), while
//!   the stream is still being held, so a box that answers at once warms itself
//!   mid-burst: the first request of the burst rewrites its box's latency EWMA
//!   and the second pick is made on an alias that is no longer the one the test
//!   described. That is what the predecessor's "nothing is warmed" precondition
//!   silently lost, and it is why its split was flaky ~2.5–10% of runs: the
//!   router's answer was *correct* for the state it saw (one warmed box against
//!   one cold one is 1000 to 250, i.e. 80% on the warmed box) and the assertion
//!   was asserting about a different state.
//!
//! With the gate held, the alias's latency state cannot change at all while the
//! burst is being placed — the only thing that moves it is a first byte, and
//! every first byte is waiting on the test. So the distribution read after the
//! burst is the distribution the Weyl walk produces over a known state, which
//! is a deterministic thing to assert on, and the harness has no sleeps in it
//! as the mechanism: it warms a state, polls until the burst has arrived, and
//! reads what the router did.
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
use sturnus::tracker::{LatencyMode, Tracker};

use common::CapturedLogs;

mod common;

/// How long anything is allowed to take to settle. Local sockets; generous on
/// purpose, so a failure means a real fault rather than a slow machine.
const SETTLE: Duration = Duration::from_secs(5);

/// The latency every box of a warmed alias is sampled at. The number itself is
/// arbitrary — what matters is that it is the *same* number on every box, so
/// the router's only way to tell two of them apart is who is already carrying
/// a request.
const WARM: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// The alias and its mock boxes
// ---------------------------------------------------------------------------

/// One alias, `n` candidates, one held-stream mock box per provider (`box0` ..
/// `boxN-1`). Every box is identical in every way the router can see — same
/// model name, same latency, and [`Boxes::warm`] puts the same latency EWMA on
/// every one of them — so the only thing that can tell two of them apart is who
/// is already carrying a request.
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

/// One mock box: takes a request, sends the response head, then waits on two
/// gates the test owns.
///
/// The **first-byte gate** is the one that makes this file's precondition true.
/// The proxy records a candidate's latency at the first byte of the body, so a
/// box that answers immediately warms itself while the burst is still being
/// placed, and the alias the later picks score is not the alias the test
/// described. Holding the first byte means no pick in a burst can be made on a
/// state the test did not put there.
///
/// The **finish gate** is the window in which the request is provably
/// outstanding: the test owns it, and nothing about a held stream depends on
/// how fast the router was.
///
/// Returns the port, a counter of requests the box has taken, and the two
/// permit semaphores. A permit is spent on one stream and forgotten, so
/// releasing k permits finishes exactly k streams — which is why the test hands
/// out exactly as many as it has streams waiting, per box.
async fn held_box() -> (u16, Arc<AtomicUsize>, Arc<Semaphore>, Arc<Semaphore>) {
    let (listener, port) = common::bind().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let first_byte = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream_first_byte = Arc::clone(&first_byte);
    let upstream_finish = Arc::clone(&finish);
    tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            let first_byte = Arc::clone(&upstream_first_byte);
            let finish = Arc::clone(&upstream_finish);
            let hits = Arc::clone(&upstream_hits);
            tokio::spawn(async move {
                if common::read_request(&mut conn).await.is_err() {
                    return;
                }
                hits.fetch_add(1, Ordering::Relaxed);
                sse_head(&mut conn).await;
                // Nothing has been recorded for this request yet: the proxy is
                // waiting on the byte the test has not allowed out.
                let Ok(permit) = first_byte.acquire().await else {
                    return;
                };
                permit.forget();
                sse_chunk(&mut conn, "data: a\n\n").await;
                let Ok(permit) = finish.acquire().await else {
                    return;
                };
                permit.forget();
                sse_chunk(&mut conn, "data: z\n\n").await;
                sse_end(&mut conn).await;
            });
        }
    });
    (port, hits, first_byte, finish)
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
    /// Provider name per box, in creation order (`box0`, `box1`, …). The
    /// distribution the tests read is in provider-name order, which is not the
    /// same ordering once an alias has ten boxes, so permits are matched to
    /// boxes by name rather than by position.
    providers: Vec<String>,
    hits: Vec<Arc<AtomicUsize>>,
    first_bytes: Vec<Arc<Semaphore>>,
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
        let mut first_bytes = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..count {
            let (port, hit, first_byte, release) = held_box().await;
            ports.push(port);
            hits.push(hit);
            first_bytes.push(first_byte);
            releases.push(release);
        }
        let providers = (0..count).map(|i| format!("box{i}")).collect();
        let state = state_for(&alias_config(&ports, alias), caller_client());
        let (addr, _shutdown) = start_server(state.clone());
        Self {
            alias: alias.to_string(),
            state,
            addr,
            providers,
            hits,
            first_bytes,
            releases,
            _server: _shutdown,
            burst: Vec::new(),
        }
    }

    /// The alias's candidates' stats slots, in provider-name order — the order
    /// every distribution in this file is read in.
    fn stats_indices(&self) -> Vec<usize> {
        self.in_provider_order(|candidate| candidate.stats_index)
    }

    /// Map the alias's candidates, in provider-name order, through `f`.
    fn in_provider_order<T>(
        &self,
        mut f: impl FnMut(&sturnus::model_map::ResolvedCandidate) -> T,
    ) -> Vec<T> {
        let mut rows: Vec<(String, T)> = self
            .state
            .model_map
            .iter()
            .filter(|(alias, _)| *alias == self.alias)
            .map(|(_, candidate)| (candidate.provider_name.clone(), f(candidate)))
            .collect();
        rows.sort_unstable_by_key(|(provider, _)| provider.clone());
        rows.into_iter().map(|(_, value)| value).collect()
    }

    /// Warm the given boxes (indices in provider order) to one identical
    /// latency sample, so the router sees boxes it cannot tell apart on
    /// latency and the only asymmetry in the alias is the one the burst
    /// creates.
    ///
    /// Recording the sample on the tracker is the honest way to get there: the
    /// alternative the predecessor relied on was "assume nothing has been
    /// warmed", which the first request of the burst itself broke. Warming a
    /// subset is how a leader with a real EWMA lead is built — see
    /// [`a_warmed_leader_shares_a_burst_with_its_cold_twin`].
    fn warm(&self, boxes: &[usize]) {
        let stats = self.stats_indices();
        for i in boxes {
            self.state
                .tracker
                .record_success(stats[*i], LatencyMode::Streaming, WARM);
        }
    }

    /// Warm every box of the alias to the same sample.
    fn warm_all(&self) {
        let all: Vec<usize> = (0..self.stats_indices().len()).collect();
        self.warm(&all);
    }

    /// Fire `n` streaming requests at the alias together — `n` separate
    /// connections, every handshake and body in flight at once, driven by one
    /// `join_all` so nothing between them is sequential — and return the
    /// in-flight distribution the alias settled on, per box in provider order.
    ///
    /// The responses stay unread and unconsumed on purpose: the burst has to
    /// still be outstanding when the distribution is read.
    ///
    /// The sends are spawned rather than awaited in place because the
    /// distribution has to be read *between* "every request has been placed"
    /// and "every request has been answered": the caller's `send()` resolves at
    /// the first byte, which no box is allowed to send until
    /// [`Boxes::open_first_bytes`] hands the alias back to the upstream. So the
    /// burst is placed on a frozen alias, read, and only then released.
    async fn burst(&mut self, n: usize) -> Vec<usize> {
        // What the boxes had taken before this burst: their counters are
        // cumulative, and the waits below are on this burst's arrivals only.
        let arrived_before: Vec<usize> = self.arrivals();

        // The address and body are taken out of `self` first: a spawned future
        // has to be `'static`, so it cannot borrow from the alias it is about to
        // fire at.
        let addr = self.addr;
        let body = streaming_request(&self.alias);
        let client = caller_client();
        let sends: Vec<_> = (0..n)
            .map(|_| {
                let client = client.clone();
                let body = body.clone();
                tokio::spawn(async move {
                    client
                        .post(format!("http://{addr}/v1/chat/completions"))
                        .json(&body)
                        .send()
                        .await
                })
            })
            .collect();

        // Every request of the burst is now outstanding: counted against the
        // candidate it was dispatched to, and no stream can have finished
        // because nothing has been released.
        let queue = self.wait_for_burst(n, &arrived_before).await;

        // …and the alias is exactly the state it was warmed to: a latency
        // sample is recorded at the first byte, and every box is still holding
        // its. Open the gates now, and collect the responses.
        self.open_first_bytes(&arrived_before);
        for send in sends {
            let resp = send.await.unwrap().unwrap();
            assert_eq!(resp.status(), 200, "a held upstream answers 200");
            self.burst.push(resp);
        }
        queue
    }

    /// Requests each box has taken, in creation order.
    fn arrivals(&self) -> Vec<usize> {
        self.hits
            .iter()
            .map(|hit| hit.load(Ordering::Relaxed))
            .collect()
    }

    /// Let exactly as many first bytes out per box as this burst left waiting
    /// on that box, so no permit is left over to release a later burst's stream
    /// early.
    fn open_first_bytes(&self, arrived_before: &[usize]) {
        for (i, hit) in self.hits.iter().enumerate() {
            let arrived = hit.load(Ordering::Relaxed) - arrived_before[i];
            if arrived > 0 {
                self.first_bytes[i].add_permits(arrived);
            }
        }
    }

    /// Poll until the alias has `n` requests outstanding and each of them has
    /// reached the box it was dispatched to. The tracker counts a request the
    /// moment it is committed to a candidate, which is before the upstream sees
    /// it, so waiting on the count alone would sample a distribution while part
    /// of the burst was still on the wire to its box.
    async fn wait_for_burst(&self, n: usize, arrived_before: &[usize]) -> Vec<usize> {
        let before: usize = arrived_before.iter().sum();
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            let queue = self.in_flight_by_box();
            let arrived: usize = self.arrivals().iter().sum();
            if queue.iter().sum::<usize>() == n && arrived == before + n {
                return queue;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the burst never settled: {n} requests outstanding expected, the tracker says \
                 {queue:?} and the boxes have taken {} of them",
                arrived - before,
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The live in-flight count of every candidate, in provider order.
    fn in_flight_by_box(&self) -> Vec<usize> {
        self.in_provider_order(|candidate| self.state.tracker.in_flight(candidate.stats_index))
    }

    /// The same counts, paired with the box they belong to, so a release can be
    /// matched to the box that is holding the stream.
    fn in_flight_by_provider(&self) -> Vec<(String, usize)> {
        self.in_provider_order(|candidate| {
            (
                candidate.provider_name.clone(),
                self.state.tracker.in_flight(candidate.stats_index),
            )
        })
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
        // One permit finishes one held stream, so each box gets exactly the
        // number of streams it is holding — its own live count. Handing every
        // box the size of the whole burst, as this file's predecessor did,
        // leaves permits sitting on boxes that had nothing to release, and the
        // next burst on the same alias finds its streams finishing by
        // themselves: the surplus spends the release before the test asks for
        // it, and the burst that is supposed to be outstanding is not.
        let held = self.in_flight_by_provider();
        for (provider, count) in &held {
            if let Some(i) = self.providers.iter().position(|p| p == provider) {
                if *count > 0 {
                    self.releases[i].add_permits(*count);
                }
            }
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
/// one request heavier than an earlier one's. So for a pair — the alias with
/// nothing on it, then the alias carrying one request — the two logged share
/// sets cannot match, and a match is the race written down: the second pick
/// scored counts the first had already spent.
///
/// The premise this rests on is that a reservation *changes* the shares it
/// reports, and that is only guaranteed by a scoring reference that does not
/// move with the candidate being scored. Both boxes here are warmed to the same
/// sample, so the second pick's snapshot is 125/1000 against the first's
/// 1000/1000. Scored against the queue-inflated minimum this pair happens to
/// separate as well, and for a reason that has nothing to do with the leader:
/// the reservation hands the minimum to the idle twin, so the leader falls to
/// 125 against that twin's 1000. Byte-identical shares under the moving
/// reference need a reserving box that is the alias's *sole warm candidate*. A
/// warmed leader beside a cold twin is that minimum at every queue depth, so
/// its own `(1 + n)` sits on both sides of its ratio and divides out, and the
/// shares read 80.0%/20.0% at one request in flight and at none — the pair
/// landing `[2, 0]`, four requests `[3, 1]`. That is the alias this file's own
/// `a_warmed_leader_shares_a_burst_with_its_cold_twin` drives over HTTP, and
/// the one `router::tests::a_warmed_leader_pays_for_its_queue_against_a_cold_probe`
/// drives at the seam: it fails at `9484fd8`, with the leader holding the full
/// 1000 units at one request in flight. That is why the pair is
/// warmed: not to make the split happen, which the Weyl walk does anyway, but
/// to make the line the assertion reads mean what the assertion says.
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
/// "Interchangeable" is the state the test builds — both boxes warmed to the
/// same sample, no box allowed to answer while the pair is placed — and it is
/// the state the pair splits one per box *because of*. A pair whose boxes are
/// not interchangeable, where one has a real lead, is the next test's subject,
/// and there the router is right to prefer one box.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_same_instant_pair_splits_one_per_box() {
    let mut boxes = Boxes::start(2, "pair").await;
    proxy_logs();
    boxes.warm_all();

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

    // A second burst on the same alias, which is what the file is safe to do
    // now that `settle` releases exactly the streams each box is holding: a
    // box left with a spare permit finishes this burst's stream on its own,
    // the request stops being load mid-burst, and the distribution read below
    // is a fiction.
    boxes.warm_all();
    let queue = boxes.burst(2).await;
    assert_eq!(
        queue,
        vec![1; 2],
        "a second same-instant pair on the same alias must split one per box too; the in-flight \
         distribution was {queue:?}",
    );
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
    boxes.warm_all();

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

/// The case the scoring reference is for, end to end: one box with a real
/// latency lead (100ms, warmed) and one with no latency data at all, four
/// concurrent requests. The leader is the alias's best box, so it is also the
/// minimum the weights are taken against — and that is exactly why scoring
/// against the *queue-adjusted* minimum made its queue invisible: at one
/// request in flight its adjusted latency is the alias's best, so its weight
/// stayed the full 1000 units, twice the cold box's 250-unit probe, and the
/// burst piled onto it. Measured at `9484fd8` this alias lands `[3, 1]`, in
/// every run; the same alias over the fixed reference lands `[2, 2]`.
///
/// Normalised against the load-free 100ms, one request costs the leader 8x of
/// its weight (125 units) and the cold twin's probe outranks it, so the four
/// requests go two per box.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_warmed_leader_shares_a_burst_with_its_cold_twin() {
    let mut boxes = Boxes::start(2, "leader").await;
    proxy_logs();
    // Only the leader is warmed: the cold twin is the situation a real alias is
    // in the moment it starts taking traffic, and the one where a leader's
    // queue was invisible.
    boxes.warm(&[0]);

    let queue = boxes.burst(4).await;
    assert_eq!(
        queue,
        vec![2; 2],
        "a warmed leader carrying a request must lose weight to its cold twin, not take the \
         whole burst; the in-flight distribution was {queue:?}",
    );
    assert_eq!(
        boxes.in_flight_from_status().await,
        vec![2; 2],
        "/status must show the leader sharing the burst with the box it was probed against"
    );

    boxes.settle().await;
}
