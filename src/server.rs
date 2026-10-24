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
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn, Instrument};

use crate::auth::ApiKeys;
use crate::gcp_auth::GcpTokenProvider;
use crate::metrics::Metrics;
use crate::model_map::ModelMap;
use crate::proxy::{self, UpstreamLatency};
use crate::router::{self, RoundRobinState};
use crate::tracker::{LatencyMode, Tracker};

pub struct AppState {
    pub model_map: ModelMap,
    pub tracker: Tracker,
    pub rr_state: RoundRobinState,
    pub client: reqwest::Client,
    pub exploit_k: f64,
    pub gcp_token_provider: Option<GcpTokenProvider>,
    pub budget: BufferBudget,
    pub metrics: Metrics,
    pub shutting_down: AtomicBool,
    /// When set, every proxied request must present one of these keys.
    /// Observability routes stay open so probes and scrapes need no secrets.
    pub api_keys: Option<ApiKeys>,
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

    let affinity: Option<(&str, &str)> = affinity_header
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .and_then(|string| string.split_once('/'));

    // A pin is honored until the pinned candidate's error-rate EWMA breaches
    // `error_threshold`; routing weights never consult the threshold.
    let pinned = affinity.and_then(|(provider, model)| {
        let found = candidates
            .iter()
            .find(|c| c.provider_name == provider && c.model == model)?;
        if state.tracker.is_degraded(found.stats_index) {
            debug!(provider = %provider, model = %model, "affinity candidate above error threshold, re-routing");
            None
        } else {
            Some(found)
        }
    });

    // Needed before routing: mode selects which EWMA the router compares.
    let is_streaming = body.get_as::<bool>("stream").unwrap_or(false);
    let latency_mode = if is_streaming {
        LatencyMode::Streaming
    } else {
        LatencyMode::NonStreaming
    };

    let candidate = if let Some(c) = pinned {
        // A pin bypasses the router entirely, so log it as a pin: at debug a
        // pinned request must not read like a weighted decision it never made.
        debug!(
            alias = %alias,
            provider = %c.provider_name,
            model = %c.model,
            mode = latency_mode.as_str(),
            "session affinity pin honored, router bypassed"
        );
        c
    } else {
        // The schedule is built here rather than inside the picker so the
        // weights behind this choice can be logged: the same numbers `/status`
        // reports, from the same seam.
        let schedule =
            router::weighted_schedule(candidates, &state.tracker, state.exploit_k, latency_mode);
        let Some(c) = router::pick_from_schedule(&schedule, &alias, &state.rr_state) else {
            return Ok(json_error(
                hyper::StatusCode::BAD_GATEWAY,
                &format!("no healthy candidate for model alias '{alias}'"),
            ));
        };
        debug!(
            alias = %alias,
            provider = %c.provider_name,
            model = %c.model,
            mode = latency_mode.as_str(),
            shares = %schedule,
            "routing decision"
        );
        c
    };

    let stats_index = candidate.stats_index;

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
            warn!(
                provider = %provider,
                model = %model,
                error = %e,
                "upstream request failed"
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

/// One candidate's live routing numbers for one latency mode, taken straight
/// from the schedule the picker consumes: what the router would do right now
/// if a request of this mode arrived.
fn routing_report(entry: &router::ScheduledCandidate<'_>) -> serde_json::Value {
    serde_json::json!({
        // `null` while cold: there is no measured latency to weigh yet, so the
        // weight is a probe share rather than a ranking.
        "effective_latency_ms": entry.effective_latency_ms,
        "weight": entry.weight,
        "share": entry.share,
    })
}

/// `GET /status`: every candidate as the router currently sees it — its health
/// (cold/warm/degraded), its measured latencies and error rate, and the weight
/// and traffic share those numbers buy it in each alias that can use it, per
/// latency mode. Weights come from the same [`router::weighted_schedule`] the
/// picker draws from, so the reported numbers and the routing decision cannot
/// drift apart.
///
/// A candidate reachable through several aliases appears once per alias, since
/// its share is relative to the candidates it competes with there.
fn build_status_response(state: &AppState) -> Response<BoxBody> {
    // Aliases sorted so repeated curls diff cleanly; the map itself is a HashMap.
    let mut aliases = state.model_map.alias_names();
    aliases.sort_unstable();

    let mut candidates = Vec::new();
    for alias in aliases {
        // `alias` came from this very map, so the lookup cannot miss.
        let Some(group) = state.model_map.get(alias) else {
            continue;
        };
        let streaming = router::weighted_schedule(
            group,
            &state.tracker,
            state.exploit_k,
            LatencyMode::Streaming,
        );
        let nonstreaming = router::weighted_schedule(
            group,
            &state.tracker,
            state.exploit_k,
            LatencyMode::NonStreaming,
        );

        // Both schedules walk the same slice, so their entries line up.
        for (streaming_entry, nonstreaming_entry) in
            streaming.entries.iter().zip(&nonstreaming.entries)
        {
            let candidate = streaming_entry.candidate;
            let stats = state.tracker.stats(candidate.stats_index);
            let streaming_ewma = stats.ewma_ms(LatencyMode::Streaming);
            let nonstreaming_ewma = stats.ewma_ms(LatencyMode::NonStreaming);
            let any_warm = streaming_ewma != u64::MAX || nonstreaming_ewma != u64::MAX;
            let status = if !any_warm {
                "cold"
            } else if state.tracker.is_degraded(candidate.stats_index) {
                "degraded"
            } else {
                "warm"
            };

            candidates.push(serde_json::json!({
                "alias": alias,
                "provider": candidate.provider_name,
                "model": candidate.model,
                "status": status,
                "streaming_ewma_ms": (streaming_ewma != u64::MAX).then_some(streaming_ewma),
                "nonstreaming_ewma_ms": (nonstreaming_ewma != u64::MAX).then_some(nonstreaming_ewma),
                "error_rate": 1.0 - stats.success_rate(),
                "routing": {
                    "streaming": routing_report(streaming_entry),
                    "nonstreaming": routing_report(nonstreaming_entry),
                },
            }));
        }
    }

    let body = serde_json::json!({
        "candidates": candidates,
    });
    // serializing the status object can't fail.
    #[allow(clippy::expect_used)]
    let bytes = Bytes::from(serde_json::to_vec_pretty(&body).expect("serialize status JSON"));
    json_response(hyper::StatusCode::OK, bytes)
}
