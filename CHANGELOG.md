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

### Changed

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
