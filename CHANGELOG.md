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
  candidate carries `weight`, the integer weight it declares in its alias, and
  `share`, the fraction of that alias's traffic that weight buys — under
  `routing`, beside the latency EWMAs and error rate that were already there,
  which now sit under `observed` with a note that they do not route. So "why is
  this box getting this share?" is a curl rather than an inference from a log
  excerpt, and the arithmetic on show is the whole policy:
  `share = weight / sum(weights)`. The numbers come from the same placement
  schedule the picker draws from, so the endpoint and the routing decision
  cannot drift apart, and rendering them consumes no pick — a scrape cannot
  perturb the routing it reports. Candidates reachable through several aliases
  are now listed once per alias, since a share only means something relative to
  the candidates it competes with there. At `debug` a routed request logs the
  alias, the candidate that won, the reason (`fresh-by-weight`), and every
  candidate's declared share; a request riding an `x-session-affinity` pin logs
  the same line with `reason="explicit-pin"`, so a pin is not mistaken for a
  weighted decision.
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
  nothing in the router acts on today: the count is visible before any placement
  rule consults it. A box
  reachable through several aliases reports the same number under each of its
  label sets, because it is one box's queue.

- **A box that cannot be reached stands down for a window.** A request that
  never got a response from a candidate — connect refused, DNS failure, TLS
  failure, connect timeout, read timeout — starts a per-candidate demotion
  window of `routing.demotion_secs` (default 10), and during it the candidate
  takes no new placements: its share is exactly zero, with no probe traffic and
  no slice held aside for it. When the window ends the candidate is fully
  eligible again at its full declared weight, in one step — no ramp, no
  recovery ladder, nothing carried over from the demotion, because the first
  new session placed there *is* the probe. A second failure restarts the window
  rather than stacking it. A box that answers is a box that is up: an upstream
  `4xx` or `5xx` is relayed to the caller verbatim and starts no window, so a
  box returning `500` to everything keeps its full share and the fix is a
  `weight`. When every candidate of an alias is demoted the turn still goes
  out, on the heaviest demoted one, and the caller receives that upstream's own
  response — a dead box's own error is more informative than a `502` the proxy
  invented. `routing.demotion_secs = 0` asks for no window at all. The window
  is readable where the routing decision is: `/status` reports
  `demotion_secs_left` per candidate and a `demoted` status, and the `debug`
  routing line carries `demoted=provider/model=9s` beside the shares, naming the
  boxes a placement skipped.
- **A new session is placed on an idle box.** A candidate with a prompt running
  is passed over for one that is idle whatever their weights — the cheap
  outcome for a new session is a box that starts it immediately, the expensive
  one is a queue behind somebody else's prompt — and when every candidate is
  busy the least busy wins, with weight as the tiebreak. Busyness is a
  placement rule only: a turn of a session that is already pinned stays on its
  box and queues behind that box's running prompt. `/status` shows `busy`
  beside `in_flight`, and the placement reason says which half of the rule ran
  (`fresh-by-weight` when an idle box existed, `overflow-busy` when none did).
- **A session key pins a conversation to one box.** `x-session-affinity` names
  two different things, and the router reads it as the shape it finds. A value
  containing a slash is a `provider/model` **pin**: it names a box outright, it
  is routed like any other request, and it is stateless — the header carries the
  whole decision, so it works across replicas with nothing shared. Any other
  usable value is a **session key**: a conversation id from the caller, which is
  not routed at all. Its first turn is placed by the normal rules, and every
  later turn of that session rides to the same box, busy or not — the box that
  has already paid for the prompt is the box worth queuing behind. A key with no
  live pin — new, quiet past `routing.session_ttl_secs` (default 1800), evicted
  at `routing.session_table_limit` (default 1024, least recently used first), or
  pinned to a box that has since been demoted — places fresh, and the box it
  lands on becomes the pin. A demotion therefore
  **re-pins**: the session follows its new box, and nothing moves it back when
  the old box's window expires. Pins are keyed per alias, because two aliases may
  name two models and a box that cached one prompt has not cached the other.
  Nothing is ever refused for a header the router cannot use: an empty value, a
  leading or trailing slash, a model that is not a candidate of this alias, and
  an absent header all place normally. The table is in-process memory, so a
  session is pinned per instance — behind two replicas each places the session
  on its own first turn, and the pin only holds once traffic is sticky above
  them; the startup log says so. `/status` reports the table (`sessions_pinned`
  per candidate, and `sessions` with `pinned`, `limit`, `idle_ttl_secs`), and
  the `debug` routing line carries `session=` beside the box, with
  `reason="pinned"` on the turns that rode a pin.
- **The inputs to a placement are logged beside its outcome.** The `debug`
  routing line gains `load=`, listing every candidate of the alias as
  `provider/model=w3,t1,busy` — the weight it declares, the turns it has
  outstanding right now, and whether it is busy — so "why did this subagent land
  on the slow box?" is answered by the one line that logged the decision, rather
  than by cross-referencing a share, a gauge and a demotion window from three
  places. The line is built inside the same `debug` gate as the rest of it, so a
  process not logging at `debug` pays nothing, and it is rendered from the same
  placement schedule the picker drew from: the numbers on show are the ones the
  decision used.
- **How many turns a candidate has been given in total.** `/status` adds
  `turns_served` per candidate, incremented at the same seam that takes a turn
  out of `in_flight`, so it agrees with `in_flight` about what a dispatch was. It
  counts turns that failed — the box was given that turn — and it is not an input
  to placement: the router compares outstanding turns, never this total, so a box
  that has carried a lot is not penalised for having carried it. Read beside
  `share`, it shows a box whose traffic has drifted from what its weight declares.
- **`/metrics` exports the placement rule's own audit trail.**
  `sturnus_placements_total` counts turns placed, labelled by `alias`, `provider`,
  `model` and `reason`, so `sum by (reason) (sturnus_placements_total{alias="…"})`
  answers how an alias is being split in practice over a window of traffic rather
  than in one log line; a `reason` climbing where it should be flat is the shape
  of a box that is busy, demoted, or pinned. `sturnus_sessions_pinned` is a gauge
  of how many sessions this instance currently pins to each candidate — sessions,
  not turns, and per instance. Both are written from the live router state (the
  counter at the seam that decided, the gauge from the pin table at scrape time),
  and both are zero-initialised at startup, so an idle alias reads `0` rather than
  no series.

### Changed

- **Breaking: a session-affinity pin is no longer broken by an error rate, and
  `routing.error_threshold` is gone.** The threshold was the one place a
  measured number reached the routing path: a pinned candidate whose
  error-rate EWMA passed it had its pin dropped. Health is binary now — a box
  that answered is up, a box that did not is stood down for
  `routing.demotion_secs` — and an explicit `provider/model` pin is honored
  whatever the pinned box's error rate, so a config that sets the key fails at
  startup naming what replaced it:

  ```text
  routing.error_threshold is no longer used: a transport failure (connect, DNS,
  TLS, connect timeout, read timeout) demotes a candidate for
  `routing.demotion_secs` seconds (default 10), an upstream 4xx/5xx is relayed
  and does not demote, and a pin is honored whatever the pinned box's error
  rate; remove error_threshold
  ```

  The migration is to delete the key. A pin that used to break after roughly
  two consecutive errors now holds, which is the point: the conversation's
  cache stays warm, and a box that is genuinely unreachable is handled by the
  demotion window for new placements. The success-rate EWMA is still recorded
  and shown on `/status`, under `observed`, as observability.
- **Breaking: an alias's traffic is divided by declared `weight`, not by
  measured latency.** Every candidate of an alias takes an integer `weight`
  (default 1) and its share of that alias is `weight / sum(weights)`; nothing
  measured — latency, error rate, queue depth — scales it. The measured
  allocator is out of the routing path: exploit sharpness, effective-latency
  weighting, the probe floor and the cold-probe share. A config that still sets
  `routing.exploit_k` therefore fails at startup, naming the key and what
  replaced it, rather than loading and tuning nothing:

  ```text
  routing.exploit_k is no longer used: traffic is allocated by each candidate's
  declared `weight` (an integer, default 1) in the [model] alias list; remove exploit_k
  ```

  The migration is to write the split down. The box that a 2x latency gap and
  `exploit_k = 3.0` gave 11% of an alias is `weight = 19` beside `weight = 1` to
  give it 95%. Latency and success-rate EWMAs are still recorded and `/status`
  still shows them, under `observed`, labelled as not used for routing: noticing
  a box that has quietly gotten slow is, in this design, a human reading a
  number and changing a `weight`. A candidate that should get nothing is left
  out of the alias's list — `weight = 0` is a startup error naming the alias and
  the candidate, because the weighted draw has no way to express "never". Picks
  come from a golden-ratio Weyl sequence over the alias's counter, so equal
  weights alternate exactly and a declared ratio holds over a long run, with no
  RNG to seed.
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

- **A Linux binary built from source no longer depends on the C library of the
  machine that built it.** `cargo build --release` — the command the README
  presented as *the* release build, labelled "static binary with LTO" — links
  against the build host's glibc and stamps that host's symbol versions into
  the binary. Installed on an older host it complains on every start, before
  the process has done anything:
  `weak version 'GLIBC_2.39' not found (required by /usr/local/bin/sturnus)`.
  Nothing in sturnus can explain or suppress that line, because the dynamic
  loader emits it before `main`. The documented Linux release build is now the
  static musl target — the target the container images and the published
  release assets were already built from, so downloading a release asset
  changes nothing. `scripts/build-static.sh` builds x86_64 and aarch64 with the
  toolchain prerequisites stated up front, and `scripts/check-static.sh` fails
  any binary that declares `libc.so.6`, `libm.so.6`, or `libgcc_s.so.1`, or
  names a program interpreter. The release pipeline now runs that check on its
  own Linux artifacts as it publishes them, and fails if an expected
  architecture's binary is absent, so a mistyped artifact pattern can no longer
  drop a platform quietly. The README separates the two builds it previously
  conflated — a host build, for developing and testing on the machine you are
  on, and the static target for a binary you intend to install on another
  Linux — and says what `cargo install` really produces: a binary bound to the
  host that compiled it, with the static target named as the way to opt out.
  `cargo build --release`, `cargo test`, and `cargo install` keep their current
  behaviour, in CI and for everyone who never intends to ship a binary.
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
