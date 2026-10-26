use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::body::Body as _;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn, Instrument};

use crate::auth::ApiKeys;
use crate::gcp_auth::GcpTokenProvider;
use crate::metrics::Metrics;
use crate::model_map::{ModelMap, ResolvedCandidate};
use crate::proxy::{self, UpstreamLatency};
use crate::router::{self, Placed, Placement, RoundRobinState};
use crate::session::{self, SessionTable};
use crate::tracker::{self, LatencyMode, Tracker};

pub struct AppState {
    pub model_map: ModelMap,
    pub tracker: Tracker,
    pub rr_state: RoundRobinState,
    /// The sessions this instance has pinned to a candidate. Per instance and
    /// in-memory: several replicas behind a load balancer each place their own
    /// sessions, and nothing is shared between them.
    pub sessions: Mutex<SessionTable>,
    pub client: reqwest::Client,
    pub gcp_token_provider: Option<GcpTokenProvider>,
    pub budget: BufferBudget,
    pub metrics: Metrics,
    pub shutting_down: AtomicBool,
    /// When set, every proxied request must present one of these keys.
    /// Observability routes stay open so probes and scrapes need no secrets.
    pub api_keys: Option<ApiKeys>,
}

impl AppState {
    /// The candidate this session is pinned to, touching the entry so it stays
    /// alive.
    ///
    /// `None` means the turn is placed: no pin, an expired pin, an evicted one,
    /// or a pin whose box is demoted. The demoted case is the one that matters
    /// for the model: busyness never moves a pinned turn (it queues behind its
    /// own prompt), but a box that is standing down takes no *new* sessions,
    /// and a session overflowed off it is a new session to that box. The
    /// placement that follows re-pins, and nothing remembers the box it left.
    fn pinned_candidate<'a>(
        &self,
        alias: &str,
        key: &str,
        candidates: &'a [ResolvedCandidate],
    ) -> Option<&'a ResolvedCandidate> {
        let now_ms = self.tracker.clock().now_ms();
        let index = self.sessions.lock().ok()?.lookup(alias, key, now_ms)?;
        candidates
            .iter()
            .find(|candidate| candidate.stats_index == index && !self.tracker.is_demoted(index))
    }

    /// Record that this session's turns belong to the candidate at
    /// `stats_index`, replacing whatever it was pinned to.
    fn pin_session(&self, alias: &str, key: &str, stats_index: usize) {
        let now_ms = self.tracker.clock().now_ms();
        // A poisoned table means a request panicked while holding it; dropping
        // the pin costs a session its affinity, which is what an absent pin
        // already means, so the lock is not worth a panic of its own.
        if let Ok(mut table) = self.sessions.lock() {
            table.pin(alias, key, stats_index, now_ms);
        }
    }
}

/// Caps how much memory all in-flight requests may spend buffering bodies,
/// and the size of any single body. The one place that decides admission:
/// too big for the cap → 413, momentarily over budget → 429.
pub struct BufferBudget {
    semaphore: Arc<tokio::sync::Semaphore>,
    permits: usize,
    pub max_body_bytes: usize,
}

impl BufferBudget {
    pub fn new(budget_bytes: usize, max_body_bytes: usize) -> Self {
        let permits = (budget_bytes / 1024).clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            permits,
            max_body_bytes: max_body_bytes.min(permits * 1024),
        }
    }

    /// Reserve permits for `bytes` of buffer (1 KB granularity, at least
    /// one), or None if the budget is exhausted.
    pub fn reserve(&self, bytes: usize) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let want = u32::try_from(bytes.div_ceil(1024).max(1)).unwrap_or(u32::MAX);
        self.semaphore.clone().try_acquire_many_owned(want).ok()
    }

    /// The budget's total size in KB-permits.
    pub fn permits(&self) -> usize {
        self.permits
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

type BoxBody = http_body_util::Either<
    http_body_util::Full<Bytes>,
    http_body_util::StreamBody<
        futures_util::stream::BoxStream<'static, Result<hyper::body::Frame<Bytes>, Infallible>>,
    >,
>;

fn json_error(status: hyper::StatusCode, message: &str) -> Response<BoxBody> {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "invalid_request_error",
        }
    });
    // serializing a static JSON value can't fail.
    #[allow(clippy::expect_used)]
    let bytes = Bytes::from(serde_json::to_vec(&body).expect("serialize static JSON value"));
    json_response(status, bytes)
}

/// The answer to a request that failed the API key gate. No key material is
/// ever echoed back.
fn access_denied() -> Response<BoxBody> {
    let mut resp = json_error(
        hyper::StatusCode::UNAUTHORIZED,
        "missing or invalid API key",
    );
    resp.headers_mut().insert(
        hyper::header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer"),
    );
    resp
}

pub async fn handle_request(
    mut req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
    remote: std::net::SocketAddr,
) -> Result<Response<BoxBody>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    debug!(%method, %path, "incoming request");

    if method == hyper::Method::GET {
        if let Some(resp) = observability_get(&path, &state) {
            return Ok(resp);
        }
        return Ok(json_error(hyper::StatusCode::NOT_FOUND, "not found"));
    }

    if method != hyper::Method::POST {
        return Ok(json_error(
            hyper::StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }

    // The gate sits ahead of everything else that costs anything: a rejected
    // request neither reads a body nor reaches an upstream. Logged at debug,
    // in line with the other client-error responses: an unauthenticated
    // scanner must not be able to write the log at its own rate.
    if let Some(api_keys) = state.api_keys.as_ref() {
        if !api_keys.is_authorized(req.headers()) {
            debug!("rejected a request without a valid API key");
            return Ok(access_denied());
        }
    }

    let affinity_header = req.headers_mut().remove("x-session-affinity");

    let max_body = state.budget.max_body_bytes;

    // We have to reserve the request size before buffering it
    // We 413 if too large or 429 if we don't have memory for it just now.
    let Some(declared_len) = req
        .body()
        .size_hint()
        .exact()
        .and_then(|n| usize::try_from(n).ok())
    else {
        return Ok(json_error(
            hyper::StatusCode::LENGTH_REQUIRED,
            "Content-Length is required",
        ));
    };
    if declared_len > max_body {
        return Ok(json_error(
            hyper::StatusCode::PAYLOAD_TOO_LARGE,
            &format!("request body too large (max {} MB)", max_body / 1024 / 1024),
        ));
    }
    let Some(permit) = state.budget.reserve(declared_len) else {
        state.metrics.buffer_rejections_total.inc();
        let mut resp = json_error(
            hyper::StatusCode::TOO_MANY_REQUESTS,
            "buffer budget exhausted, retry shortly",
        );
        resp.headers_mut().insert(
            hyper::header::RETRY_AFTER,
            hyper::header::HeaderValue::from_static("1"),
        );
        return Ok(resp);
    };

    let body_bytes = match Limited::new(req, max_body).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => {
            return Ok(json_error(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                &format!("request body too large (max {} MB)", max_body / 1024 / 1024),
            ));
        }
        Err(e) => {
            // A caller that hangs up while uploading is not a fault: its EOF
            // arrives here wrapped in hyper's "error reading a body" error.
            if is_benign_disconnect(e.as_ref()) {
                state.metrics.client_disconnects_total.inc();
                debug!(remote = %remote, "client disconnected while sending the request body");
            } else {
                warn!("failed to read request body: {e}");
            }
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                "failed to read request body",
            ));
        }
    };

    let body: crate::body::RawBody = match serde_json::from_slice(&body_bytes) {
        Ok(b) => b,
        Err(e) if e.classify() == serde_json::error::Category::Data => {
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                "request body must be a JSON object",
            ));
        }
        Err(e) => {
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                &format!("request body is not valid JSON: {e}"),
            ));
        }
    };

    let Some(alias) = body.get_as::<String>("model") else {
        return Ok(json_error(
            hyper::StatusCode::BAD_REQUEST,
            "missing 'model' field in request body",
        ));
    };

    let Some(candidates) = state.model_map.get(&alias) else {
        let available = state.model_map.alias_names();
        return Ok(json_error(
            hyper::StatusCode::BAD_REQUEST,
            &format!(
                "unknown model alias '{}', available: {:?}",
                alias, available
            ),
        ));
    };

    // The header's shape says what it asks for: a `provider/model` value names
    // one box outright, anything else is a session key. An absent,
    // unprintable, blank or over-long value is no instruction at all, and the
    // turn is placed.
    let affinity = session::classify(affinity_header.as_ref());
    let session_key = match affinity {
        Some(session::Affinity::SessionKey(key)) => Some(key),
        _ => None,
    };

    // Needed before routing: mode selects which EWMA the router compares.
    let is_streaming = body.get_as::<bool>("stream").unwrap_or(false);
    let latency_mode = if is_streaming {
        LatencyMode::Streaming
    } else {
        LatencyMode::NonStreaming
    };

    let placed: Option<Placed<'_>> = match affinity {
        // An explicit pin is checked before the session table and honored
        // whatever the box is doing: the caller named this box, and a transport
        // failure against it is reported, not routed around. It bypasses
        // placement, so a demoted box keeps serving the sessions pinned to it.
        Some(session::Affinity::ExplicitPin { provider, model }) => candidates
            .iter()
            .find(|c| c.provider_name == provider && c.model == model)
            .map(|candidate| Placed {
                candidate,
                reason: Placement::ExplicitPin,
            })
            // A pin naming no candidate of this alias is not a pin: the turn is
            // placed, as it was before the header existed.
            .or_else(|| place_turn(&state, candidates, &alias, session_key)),
        // A session key: every turn after the first rides the pin, busy or not.
        Some(session::Affinity::SessionKey(key)) => state
            .pinned_candidate(&alias, key, candidates)
            .map(|candidate| Placed {
                candidate,
                reason: Placement::Pinned,
            })
            // No pin, an expired one, an evicted one, or a demoted box: this
            // turn is placed, and the box it lands on becomes the session's.
            .or_else(|| place_turn(&state, candidates, &alias, Some(key))),
        None => place_turn(&state, candidates, &alias, None),
    };
    let Some(placed) = placed else {
        // Reachable only when the alias has no candidate to place at all:
        // every candidate demoted still serves a turn (see `router::place`),
        // so this is a config shape, not a box being down.
        return Ok(json_error(
            hyper::StatusCode::BAD_GATEWAY,
            &format!("no candidate with a non-zero weight for model alias '{alias}'"),
        ));
    };

    // One line per request naming why this box: the reason the placement path
    // produced, beside the declared weights it drew from and every candidate's
    // load at that moment — the same numbers `/status` reports — and beside
    // the boxes it skipped because they are demoted, with the time left on
    // each. Gated so the schedule is not built for a request the log level
    // will not show.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let schedule = router::placement_schedule(candidates);
        debug!(
            alias = %alias,
            provider = %placed.candidate.provider_name,
            model = %placed.candidate.model,
            mode = latency_mode.as_str(),
            reason = placed.reason.as_str(),
            // The session key, when the request carried one: the identity the
            // pin is stored under, so "why this box for this conversation?" is
            // answerable from the log. Empty when the turn had no session.
            session = session_key.unwrap_or(""),
            shares = %&schedule,
            // Every candidate's weight, outstanding turns and busy/idle state
            // as the placement read them: the inputs the busyness rule used,
            // so the reason on this line is checkable from the same line.
            load = %router::scheduled_load(&schedule, &state.tracker),
            demoted = %router::demotion_report(candidates, &state.tracker),
            "routing decision"
        );
    }

    // The same reason, counted rather than logged: every proxied request
    // increments one series per (alias, candidate, reason), so "where did this
    // alias's traffic go, and why?" is a query, and a turn that rode a pin is
    // visible as a pin rather than blending into the weighted picks. Counted
    // outside the debug gate, because the count is not a log's business.
    state
        .metrics
        .placements_total
        .with_label_values(&[
            alias.as_str(),
            placed.candidate.provider_name.as_str(),
            placed.candidate.model.as_str(),
            placed.reason.as_str(),
        ])
        .inc();

    let candidate = placed.candidate;

    let stats_index = candidate.stats_index;

    // The request belongs to this candidate from here on, and the returned
    // guard is what gives that back: it is moved into the relay stream for a
    // streamed response and released when `forward_request` returns otherwise,
    // so every way a request can end also ends its count. `Tracker::dispatch`
    // explains why the count starts before the upstream is contacted.
    let in_flight = state.tracker.dispatch(stats_index);

    info!(
        alias = %alias,
        provider = %candidate.provider_name,
        model = %candidate.model,
        streaming = is_streaming,
        "routing request"
    );

    let outbound = match proxy::prepare_outbound(
        body,
        &candidate.model,
        candidate.attribution_labels.as_deref(),
        Some(permit),
    ) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "failed to serialize outbound body");
            return Ok(json_error(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to serialize outbound body",
            ));
        }
    };
    // Only the outbound copy is held while waiting on the upstream.
    drop(body_bytes);

    let result = proxy::forward_request(
        &state.client,
        candidate,
        in_flight,
        &path,
        outbound,
        is_streaming,
        state.budget.max_body_bytes,
        state.gcp_token_provider.as_ref(),
    )
    .await;

    let provider: &str = &candidate.provider_name;
    let model: &str = &candidate.model;

    match result {
        Ok(proxy_result) => {
            let status = proxy_result.status;

            // Variant carries what was measured, so mode, metric and duration agree.
            let (observed_mode, latency, latency_metric) = match proxy_result.latency {
                UpstreamLatency::Ttfc(d) => {
                    (LatencyMode::Streaming, d, &state.metrics.ttfc_seconds)
                }
                UpstreamLatency::Total(d) => {
                    (LatencyMode::NonStreaming, d, &state.metrics.latency_seconds)
                }
            };

            if status.is_success() {
                state
                    .tracker
                    .record_success(stats_index, observed_mode, latency);
                debug!(
                    provider = %provider,
                    model = %model,
                    status = %status,
                    mode = observed_mode.as_str(),
                    latency_ms = latency.as_millis(),
                    "response received"
                );
            } else {
                warn!(
                    provider = %provider,
                    model = %model,
                    status = %status,
                    mode = observed_mode.as_str(),
                    latency_ms = latency.as_millis(),
                    "upstream returned error status"
                );
                state.tracker.record_error(stats_index);
            }

            let status_str = status.as_u16().to_string();
            state
                .metrics
                .requests_total
                .with_label_values(&[alias.as_str(), provider, model, status_str.as_str()])
                .inc();
            latency_metric
                .with_label_values(&[alias.as_str(), provider, model])
                .observe(latency.as_secs_f64());

            let body = proxy::into_hyper_body(proxy_result.body);
            let mut resp = Response::new(body);
            *resp.status_mut() = proxy_result.status;
            let headers = resp.headers_mut();
            headers.extend(proxy_result.headers);
            headers.insert("x-sturnus-provider", candidate.provider_header.clone());
            headers.insert("x-session-affinity", candidate.affinity_header.clone());
            Ok(resp)
        }
        Err(e) => {
            // The box never answered: connect refused, DNS, TLS, a connect
            // timeout, a read timeout. That is the one thing a stand-down
            // window is for, so the candidate is demoted. The caller gets the
            // `502` the proxy has to write, because there is no upstream
            // response to relay.
            let window = state.tracker.demote(stats_index);
            warn!(
                provider = %provider,
                model = %model,
                error = %e,
                demoted_for_secs = window.as_secs(),
                "upstream request failed, candidate demoted"
            );

            state.tracker.record_error(stats_index);
            state
                .metrics
                .errors_total
                .with_label_values(&[alias.as_str(), provider, model])
                .inc();

            Ok(json_error(
                hyper::StatusCode::BAD_GATEWAY,
                &format!("upstream error: {e}"),
            ))
        }
    }
}

/// Place a turn, and pin the session it belongs to to whatever it lands on.
///
/// One call covers both ways a pin comes into being: the first turn of a
/// session key (a fresh pick, by weight and busyness) and a turn overflowed off
/// a demoted box (a re-pin). Recording is what makes a placement a pin, so the
/// table always holds the box the session's last turn actually used.
///
/// A turn with no session key pins nothing: an anonymous request is not a
/// session's claim on a box, and a caller that names its box outright is
/// already pinned by the header.
fn place_turn<'a>(
    state: &AppState,
    candidates: &'a [ResolvedCandidate],
    alias: &str,
    session_key: Option<&str>,
) -> Option<Placed<'a>> {
    let placed = router::place(candidates, alias, &state.rr_state, &state.tracker)?;
    if let Some(key) = session_key {
        state.pin_session(alias, key, placed.candidate.stats_index);
    }
    Some(placed)
}

fn json_response(status: hyper::StatusCode, body: Bytes) -> Response<BoxBody> {
    let mut resp = Response::new(http_body_util::Either::Left(http_body_util::Full::new(
        body,
    )));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

/// Observability GETs shared by every listener; `None` = not an observability path.
fn observability_get(path: &str, state: &AppState) -> Option<Response<BoxBody>> {
    match path {
        "/health" | "/healthz" => {
            if state.shutting_down.load(Ordering::Relaxed) {
                let body = Bytes::from(r#"{"status":"shutting_down"}"#);
                Some(json_response(hyper::StatusCode::SERVICE_UNAVAILABLE, body))
            } else {
                let body = Bytes::from(r#"{"status":"ok"}"#);
                Some(json_response(hyper::StatusCode::OK, body))
            }
        }
        "/status" => Some(build_status_response(state)),
        "/metrics" => Some(encode_metrics_response(state)),
        _ => None,
    }
}

fn encode_metrics_response(state: &AppState) -> Response<BoxBody> {
    // The in-flight gauge mirrors counts that live in the tracker, so it is
    // written from them at scrape time rather than incremented alongside them:
    // the exported number then cannot drift from the real count, and cannot
    // stick above zero after a request is over. The sessions-pinned gauge is
    // written the same way, from the live session table.
    state.metrics.set_in_flight(&in_flight_samples(state));
    state
        .metrics
        .set_sessions_pinned(&sessions_pinned_samples(state));
    match state.metrics.encode() {
        Ok(buf) => {
            let body = Bytes::from(buf);
            let mut resp = Response::new(http_body_util::Either::Left(http_body_util::Full::new(
                body,
            )));
            resp.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );
            resp
        }
        Err(e) => {
            error!(error = %e, "failed to encode metrics");
            json_error(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to encode metrics",
            )
        }
    }
}

/// One `(alias, provider, model, count)` row per candidate, for the in-flight
/// gauge. A candidate reachable through several aliases appears once per alias,
/// each row carrying the same count: it is one box with that many requests
/// outstanding, not one queue per alias.
fn in_flight_samples(state: &AppState) -> Vec<(&str, &str, &str, usize)> {
    state
        .model_map
        .iter()
        .map(|(alias, candidate)| {
            (
                alias,
                candidate.provider_name.as_str(),
                candidate.model.as_str(),
                state.tracker.in_flight(candidate.stats_index),
            )
        })
        .collect()
}

/// One `(alias, provider, model, count)` row per candidate, for the
/// sessions-pinned gauge, read from the live table at scrape time exactly as
/// [`in_flight_samples`] reads the tracker. The exported number is the one the
/// request path consulted, so it cannot drift from the table and cannot stick
/// above zero after a session's pin expires or is evicted.
///
/// A candidate reachable through several aliases appears once per alias with
/// the same count, as the in-flight gauge does: the pins are the box's, not
/// the alias's. A poisoned table is read as it stands, as `/status` reads it.
fn sessions_pinned_samples(state: &AppState) -> Vec<(&str, &str, &str, usize)> {
    let table = match state.sessions.lock() {
        Ok(table) => table,
        Err(poisoned) => poisoned.into_inner(),
    };
    let sessions: &SessionTable = &table;
    state
        .model_map
        .iter()
        .map(|(alias, candidate)| {
            (
                alias,
                candidate.provider_name.as_str(),
                candidate.model.as_str(),
                sessions.pinned_count(candidate.stats_index),
            )
        })
        .collect()
}

/// Metrics-listener handler: observability GETs only; never proxies.
pub async fn handle_observability_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody>, Infallible> {
    if req.method() == hyper::Method::GET {
        if let Some(resp) = observability_get(req.uri().path(), &state) {
            return Ok(resp);
        }
    }
    Ok(json_error(hyper::StatusCode::NOT_FOUND, "not found"))
}

/// Which surface a listener serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerRole {
    /// Full router: the proxy plus the observability endpoints.
    Full,
    /// Observability only (`/metrics`, `/health`, `/status`); never proxies.
    Observability,
}

pub async fn run_server(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
    shutdown_timeout: Duration,
) {
    run_server_with_role(
        listener,
        state,
        ServerRole::Full,
        shutdown,
        shutdown_timeout,
    )
    .await;
}

/// Run the proxy and optional observability listener. Observability is
/// stopped only after the proxy has drained, so scrapes survive the drain.
pub async fn run_servers(
    listener: TcpListener,
    metrics_listener: Option<TcpListener>,
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
    shutdown_timeout: Duration,
) {
    let Some(metrics_listener) = metrics_listener else {
        return run_server(listener, state, shutdown, shutdown_timeout).await;
    };

    let (obs_tx, obs_rx) = tokio::sync::oneshot::channel::<()>();
    let observability = tokio::spawn(run_server_with_role(
        metrics_listener,
        state.clone(),
        ServerRole::Observability,
        async move {
            let _ = obs_rx.await;
        },
        shutdown_timeout,
    ));

    run_server_with_role(
        listener,
        state,
        ServerRole::Full,
        shutdown,
        shutdown_timeout,
    )
    .await;

    let _ = obs_tx.send(());
    let _ = observability.await;
}

pub async fn run_server_with_role(
    listener: TcpListener,
    state: Arc<AppState>,
    role: ServerRole,
    shutdown: impl Future<Output = ()>,
    shutdown_timeout: Duration,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, remote) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "accept error");
                        continue;
                    }
                };
                let io = TokioIo::new(stream);
                let state = state.clone();
                // The connection task needs the metrics registry after the
                // service closure takes `state`.
                let conn_state = state.clone();

                let conn = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| {
                        let state = state.clone();
                        async move {
                            let span = crate::trace::request_span(req.headers());
                            match role {
                                ServerRole::Full => {
                                    handle_request(req, state, remote).instrument(span).await
                                }
                                ServerRole::Observability => {
                                    handle_observability_request(req, state).instrument(span).await
                                }
                            }
                        }
                    }));

                let conn = graceful.watch(conn);

                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        if is_benign_disconnect(&e) {
                            conn_state.metrics.client_disconnects_total.inc();
                            debug!(remote = %remote, "client disconnected before the request completed");
                        } else {
                            error!(remote = %remote, error = %e, "connection error");
                        }
                    }
                });
            }
            _ = &mut shutdown => {
                info!("shutting down, draining connections");
                state.shutting_down.store(true, Ordering::Relaxed);
                break;
            }
        }
    }

    drop(listener);

    let drain = graceful.shutdown();
    tokio::select! {
        _ = drain => {
            info!("all connections drained");
        }
        _ = tokio::time::sleep(shutdown_timeout) => {
            warn!(timeout_secs = shutdown_timeout.as_secs(), "drain timeout reached, dropping remaining connections");
        }
    }
}

/// Whether a connection error is a caller hanging up rather than a fault.
///
/// hyper reports EOF in the middle of a message as an `IncompleteMessage`
/// error. That marker may sit at the top of the chain (a mid-head EOF on the
/// connection or a half-close while the response is written) or arrive
/// wrapped as an `UnexpectedEof` I/O error (a request body that stops short
/// of its declared length). Neither carries a connection fault, so both are
/// the normal shape of a caller that hangs up mid-request, shuts down its
/// write half while waiting for the response, or exits while a response is in
/// flight. Real faults — a failed write, a reset while sending a response —
/// surface as other I/O or protocol errors and stay at ERROR.
fn is_benign_disconnect(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(err) = current {
        let incomplete_message = err
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_incomplete_message);
        let truncated_body = err
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::UnexpectedEof);
        if incomplete_message || truncated_body {
            return true;
        }
        current = err.source();
    }
    false
}

/// One candidate's allocation numbers for one alias, taken straight from the
/// schedule the picker draws from: the declared weight and the share it buys.
fn routing_report(entry: &router::ScheduledCandidate<'_>) -> serde_json::Value {
    serde_json::json!({
        "weight": entry.weight,
        "share": entry.share,
    })
}

/// What the router measures but deliberately does not route on. Reported so an
/// operator can see a box that has quietly gotten slow — the detection the
/// measured allocator used to do automatically is, in this design, a human
/// reading a number and changing a `weight`.
fn observed_report(state: &AppState, stats_index: usize) -> serde_json::Value {
    let stats = state.tracker.stats(stats_index);
    let streaming_ewma = stats.ewma_ms(LatencyMode::Streaming);
    let nonstreaming_ewma = stats.ewma_ms(LatencyMode::NonStreaming);
    serde_json::json!({
        "note": "measured, not used for routing: allocation is by declared weight",
        "streaming_ewma_ms": (streaming_ewma != u64::MAX).then_some(streaming_ewma),
        "nonstreaming_ewma_ms": (nonstreaming_ewma != u64::MAX).then_some(nonstreaming_ewma),
        "error_rate": 1.0 - stats.success_rate(),
    })
}

/// `GET /status`: every candidate as the router currently sees it — its health
/// (cold/warm/demoted) and, when demoted, the seconds left in its window, the
/// declared weight and traffic share it holds in each alias that can use it,
/// how many requests it is serving right now, how many sessions this instance
/// has pinned to it, and the latencies and error rate the router measures
/// without routing on. Weights
/// come from the same [`router::placement_schedule`] the picker draws from, so
/// the reported numbers and the routing decision cannot drift apart.
///
/// A candidate reachable through several aliases appears once per alias, since
/// its share is relative to the candidates it competes with there.
fn build_status_response(state: &AppState) -> Response<BoxBody> {
    // Aliases sorted so repeated curls diff cleanly; the map itself is a HashMap.
    let mut aliases = state.model_map.alias_names();
    aliases.sort_unstable();

    // The session table, read once for the whole render: the counts an operator
    // reads are the ones the request path consulted, so the endpoint cannot
    // drift from the router. A poisoned table is read as it stands — the
    // invariant it protects is affinity, and a stale count is worth less than
    // a `/status` that stopped answering.
    let table = match state.sessions.lock() {
        Ok(table) => table,
        Err(poisoned) => poisoned.into_inner(),
    };
    let sessions: &SessionTable = &table;

    let mut candidates = Vec::new();
    for alias in aliases {
        // `alias` came from this very map, so the lookup cannot miss.
        let Some(group) = state.model_map.get(alias) else {
            continue;
        };
        let schedule = router::placement_schedule(group);

        for entry in &schedule.entries {
            let candidate = entry.candidate;
            let stats_index = candidate.stats_index;
            let streaming_ewma = state
                .tracker
                .stats(stats_index)
                .ewma_ms(LatencyMode::Streaming);
            let nonstreaming_ewma = state
                .tracker
                .stats(stats_index)
                .ewma_ms(LatencyMode::NonStreaming);
            let any_warm = streaming_ewma != u64::MAX || nonstreaming_ewma != u64::MAX;
            // Demotion outranks the latency state: "this box takes no new
            // sessions for 7s" is the actionable fact, and it is the one a
            // weight change is not the answer to.
            let demotion_left = state.tracker.demotion_remaining(stats_index);
            let status = if demotion_left.is_some() {
                "demoted"
            } else if !any_warm {
                "cold"
            } else {
                "warm"
            };

            candidates.push(serde_json::json!({
                "alias": alias,
                "provider": candidate.provider_name,
                "model": candidate.model,
                "status": status,
                // Whole seconds left in the demotion window, `null` when the
                // candidate is eligible. Rounds up, so a reading never
                // understates what is left.
                "demotion_secs_left": demotion_left.map(tracker::secs_left),
                // Live load, not a schedule number: it is one count per box, so
                // it sits beside the health fields rather than in `routing`.
                // `busy` is the word the placement rule uses — a box with a
                // prompt running is passed over for an idle one — so the same
                // word an operator sees in a log line is readable here.
                "in_flight": state.tracker.in_flight(stats_index),
                "busy": state.tracker.in_flight(stats_index) > 0,
                // Cumulative since startup, from the same dispatch that
                // increments `in_flight`: this box has been given this many
                // turns, and a turn that failed at the transport level is one
                // of them. Not a routing input — placement reads the
                // outstanding count, never this total.
                "turns_served": state.tracker.turns_served(stats_index),
                // Sessions this instance has pinned to this box, from the same
                // table the request path consults. It is a count of sessions,
                // not of turns: `in_flight` is what is running here now, this is
                // whose conversations come back here.
                "sessions_pinned": sessions.pinned_count(stats_index),
                "routing": routing_report(entry),
                "observed": observed_report(state, stats_index),
            }));
        }
    }
    let body = serde_json::json!({
        "candidates": candidates,
        // The whole table: how many sessions this instance is holding pins for,
        // the cap it holds them to, and the quiet that drops a pin. Per
        // instance — a second replica answers this question about its own
        // sessions, and nothing is shared between them.
        "sessions": {
            "pinned": sessions.len(),
            "limit": sessions.limit(),
            "idle_ttl_secs": sessions.idle_ttl_secs(),
        },
    });
    // serializing the status object can't fail.
    #[allow(clippy::expect_used)]
    let bytes = Bytes::from(serde_json::to_vec_pretty(&body).expect("serialize status JSON"));
    json_response(hyper::StatusCode::OK, bytes)
}
