# Changelog

All notable changes to this project are documented here.

The project was renamed from **llmrouter** to **sturnus** in 5.0.0. The previous
name collided with other projects, and as the project matured it made sense to
rename it.

Releases up to and including 4.3.0 were published under the `llmrouter` name;
their history lives in the git log and tags.

## [Unreleased]

### Added

- **`api_keys`** — an optional top-level config list that gates the proxy. When
  it is present, a proxied `POST` must present one of the keys in
  `Authorization: Bearer <key>` or be answered with `401` + `WWW-Authenticate:
  Bearer`, before its body is read or an upstream is contacted. The observability
  routes (`/health`, `/healthz`, `/status`, `/metrics`) stay keyless so container
  health probes and Prometheus scrapes keep working, and the keys are never
  logged or relayed upstream. Leaving it unset changes nothing. A present-but-empty
  list fails at startup rather than quietly disabling the gate, as does any entry
  that is blank, whitespace-padded, or non-ASCII — those are keys no client could
  ever present.
- **`/status` now reports the router's arithmetic, not just its inputs.** Each
  candidate carries `weight`, the integer weight it holds in its alias, and
  `share`, the fraction of that alias's traffic it should be getting right now,
  next to the `effective_latency_ms` both were derived from — under
  `routing.streaming` and `routing.nonstreaming`, beside the latency EWMAs and
  error rate that were already there. So "why is this box getting nothing?" is a
  curl rather than an inference from a log excerpt, and the common non-answer is
  visible too: a `share` of ~1% is the probe floor keeping a bad box sampled,
  never `0`. The numbers come from the same weighted schedule the picker draws
  from, so the endpoint and the routing decision cannot drift apart. Candidates
  reachable through several aliases are now listed once per alias, since a share
  only means something relative to the candidates it competes with there. At
  `debug` a routed request logs the alias, the candidate that won, and every
  candidate's share; a request riding an `x-session-affinity` pin instead logs
  that it bypassed the router, with the provider and model, so a pin is not
  mistaken for a weighted decision.
- **`routing.exploit_k` is documented next to the other routing knobs.** The key
  was accepted and validated but appeared in neither the README's config block
  nor `config.example.toml`, so the main lever for an underutilized candidate
  had to be rediscovered by reading the weighting code. Both now carry it with
  its 3.0 default and a share table — a 2x-slower, a 5x-slower and a
  hard-failing candidate, at 3.0, 1.0 and 0.0 — every number derived from the
  arithmetic the router runs, probe floor included, and pinned by a test
  (`documented_share_table_matches_the_weighting`). The README also states the
  sharpness-0 caveat: an even split that stops counting errors, so a
  hard-failing candidate keeps half the alias's traffic, and what config
  *cannot* do — disable or bound session affinity, set a per-candidate weight,
  or move the probe floor and cold probe share.
- **How many requests a candidate has outstanding right now.** `/status` lists
  `in_flight` per candidate and `/metrics` exports
  `sturnus_in_flight_requests`, a gauge labelled by `alias`, `provider` and
  `model`. A request belongs to its candidate from the moment it is committed to
  that candidate — before the upstream is contacted, so connecting, uploading,
  time-to-first-byte and the stream itself are all covered — and the count is
  given back by the guard the request travels with, when its response ends,
  errors, or is dropped mid-stream. A caller that hangs up, a connect failure, a
  timeout and an upstream 5xx therefore all return the count; none of them has to
  be remembered, and the number cannot stick above zero. The gauge is written
  from those live counts on every scrape rather than incremented per request, so
  the exported value cannot drift from the count. Zero-initialised at startup
  alongside the failure counters, so an idle candidate reads `0` rather than no
  series. The point is to make queueing visible before any routing change reacts
  to it: an agent that fires a same-instant pair of requests shows both of them
  waiting on one box while the alias's other candidate sits at zero, which
  latency-only routing notices only once latency has already degraded. A box
  reachable through several aliases reports the same number under each of its
  label sets, because it is one box's queue.

### Changed

- **Routing now scores a candidate on the wait a request can expect, not on
  the latency it happened to measure last.** A candidate with `n` requests
  outstanding is weighted as if its effective latency were `(1 + n)` times
  larger — the classic first-come-first-served sojourn estimate, `n` queued
  latencies plus its own — so routing one request to a box immediately lowers
  that box's score and the next pick goes elsewhere. Reading a candidate's
  outstanding count and adding to it is one step rather than two: a pick scores
  the alias and reserves its own place in it under one lock, so two requests
  placed in the same instant never both see an idle box. A same-instant pair of
  requests against interchangeable boxes now splits one per box instead of
  queueing both on the current EWMA winner, while a genuinely slower box is
  still avoided rather than load-balanced to. The adjustment is the exact
  identity at `in_flight = 0`, so an idle alias routes on measured latency
  alone and the proportional-share property is untouched: the share table
  (2x-slower 11.1% at the default sharpness, probe floor 1%) is the split of
  a *waiting* alias, and the floor still applies after the adjustment, so a
  candidate that is slow *and* busy keeps its shrinking-but-nonzero share.
  Nothing oscillates either — the adjustment's only memory is the requests
  themselves, and each response that finishes decays one step back toward the
  idle arithmetic, so a box that served a burst wins its share smoothly back
  as its queue drains. `/status` (`effective_latency_ms`) and the `debug`
  `routing decision` line now render the queue-adjusted numbers, so the
  inflation is visible on the very line that routed away from the busy box.
  The per-request multiplier is fixed at one measured latency, like the probe
  floor a property of how a box serialises work rather than a tuning surface;
  `routing.exploit_k` remains the knob for how hard the resulting ratios are
  exploited.
- **Breaking (bare-metal):** the default config location is now
  `$HOME/.config/sturnus.toml` rather than `./config.toml` in the working
  directory. `--config` is unchanged and still overrides it. If `HOME` is
  unset and no `--config` is given, startup fails naming both fixes, rather
  than reading a path relative to wherever the process was started. The
  container images are unaffected — they pass `--config /config.toml`
  themselves, so the documented `-v ./config.toml:/config.toml` mount keeps
  working.
- A repeated `--config` now replaces the earlier value instead of being
  rejected, so `docker run <image> --config /other.toml` overrides the
  image's built-in path.
- The config path actually loaded is logged at startup.

### Fixed

- **Benign caller disconnects no longer log at ERROR.** A caller that hangs up
  mid-request now appears at `debug` with its address and in the new
  `sturnus_client_disconnects_total` counter instead of filling the journal
  with `connection closed before message completed`. The previous filter
  inspected the error's source chain for an I/O error, which hyper's
  incomplete-message error does not carry, so it never matched. Genuine
  connection faults — a failed write while sending a response, for example —
  still log at `error`.

## [5.3.0] - 2026-08-25

### Added

- **Per-provider request headers** via a new `headers` map on any provider,
  sent on every outbound request to it. This covers upstream options carried as
  headers rather than body fields, such as pinning a Vertex provider to
  provisioned throughput with `X-Vertex-AI-LLM-Request-Type`. Headers sturnus
  sets itself, those framing or routing the request, and the hop-by-hop set fail
  startup rather than being silently dropped, as do two keys differing only in
  case.

## [5.2.1] - 2026-08-25

### Fixed

- **Vertex AI multi-region locations** now resolve to their Representative
  Endpoint hosts (`aiplatform.us.rep.googleapis.com`,
  `aiplatform.eu.rep.googleapis.com`). Previously `location = "us"` or `"eu"`
  built a regional hostname that returns `400 Invalid hostname`, so reaching a
  multi-region required an explicit `base_url` override.

## [5.2.0] - 2026-07-14

### Added

- **Optional separate metrics listener** via a new `metrics_listen` config key.
  When set (e.g. `metrics_listen = "0.0.0.0:4040"`), the observability endpoints
  (`/metrics`, `/health`, `/healthz`, `/status`) are served on a second socket
  that never exposes the proxy, so `listen` can bind loopback for the
  credential-bearing proxy while Prometheus still scrapes metrics off a routable
  address. On shutdown this listener stays up until the proxy has finished
  draining, so the final scrape window isn't lost.

## [5.1.0] - 2026-07-14

### Added

- **healthcheck subcommand** added to support exec probe healthchecks.

## [5.0.0] - 2026-06-17

Project renamed from `llmrouter` to `sturnus`. This is a breaking release: the
binary, crate, container image, Prometheus metric names, response header, and
environment variables all change name, and the routing config fields deprecated
since 4.1/4.2 are removed.

### Changed (breaking)

- **Prometheus metrics renamed** `llmrouter_* → sturnus_*`: `sturnus_requests_total`,
  `sturnus_ttfc_seconds`, `sturnus_latency_seconds`, `sturnus_errors_total`,
  `sturnus_buffer_rejections_total`. Update dashboards, alerts, and recording rules.
- **Response header renamed** `x-llmrouter-provider → x-sturnus-provider`. Update any
  client keying off the old header.
- **Environment variables renamed** `LLMROUTER_LOG_FORMAT → STURNUS_LOG_FORMAT`; the
  default `RUST_LOG` filter is now `sturnus=info`.
- **Binary, crate, and container image renamed** to `sturnus`; the image is published
  to `ghcr.io/sturnus-dev/sturnus` and releases to `github.com/sturnus-dev/sturnus`.

### Removed

- **Deprecated `routing` fields dropped**: `explore_ratio` (no-op since 4.1.0),
  `error_decay_secs` and `max_error_window_entries` (no-ops since 4.2.0). They were
  already ignored; a config that still sets them now loads with the keys ignored
  rather than logging a deprecation warning.

### Migration

- Rename the metric prefix in any Grafana/Prometheus dashboards and alert rules
  (`llmrouter_` → `sturnus_`).
- Replace reads of `x-llmrouter-provider` with `x-sturnus-provider`.
- Update the binary/image name and any `LLMROUTER_LOG_FORMAT` env var.
- Existing `config.toml` files still load. The removed `routing` fields
  (`explore_ratio`, `error_decay_secs`, `max_error_window_entries`) are now silently
  ignored; delete them to keep configs tidy.
