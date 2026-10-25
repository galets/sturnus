# sturnus

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/sturnus-dev/sturnus)](https://github.com/sturnus-dev/sturnus/releases)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fsturnus--dev%2Fsturnus-blue?logo=docker)](https://ghcr.io/sturnus-dev/sturnus)

**Spread one model alias across LLM providers, in the proportions you declare. A single static binary, zero infrastructure.**

LLM providers have variable latency and availability that can break production features. sturnus is a lightweight sidecar that sits beside your app, exposes an OpenAI-compatible API, and divides each alias's traffic across its providers by the weights you declare: 19:1 on the fast box is written `weight = 19` beside `weight = 1`, not discovered by probing.

## Quick start

sturnus needs a config file — copy `config.example.toml` and add your providers. It reads `~/.config/sturnus.toml` by default; `--config <path>` reads one from anywhere.

**Docker** — best for production deployments and Kubernetes sidecars:

```bash
docker run -v ./config.toml:/config.toml \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

**Download** — best for installing on a host. Grab the asset for your architecture from the latest [release](https://github.com/sturnus-dev/sturnus/releases/latest):

```bash
curl -fL -o sturnus \
  https://github.com/sturnus-dev/sturnus/releases/latest/download/sturnus-x86_64-unknown-linux-musl
chmod +x sturnus
./sturnus --help
```

The Linux assets (`sturnus-x86_64-unknown-linux-musl`, `sturnus-aarch64-unknown-linux-musl`) are static: they declare no C library dependency, so they run on any glibc Linux regardless of which Linux built them. macOS assets are `sturnus-x86_64-apple-darwin` and `sturnus-aarch64-apple-darwin`.

**`cargo install`** — best for local testing if you have a Rust toolchain:

```bash
cargo install sturnus
sturnus --config config.toml
```

`cargo install` compiles on your machine against your machine's C library, so the binary it leaves in `~/.cargo/bin` is bound to that host's glibc and its neighbours. That is fine where you built it. To produce a binary you can copy onto another Linux, build the static target — see [Building](#building).

Then point any OpenAI-compatible SDK at sturnus — the only change is the base URL:

```diff
- client = OpenAI(base_url="https://api.openai.com/v1", api_key="sk-...")
+ client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
response = client.chat.completions.create(
    model="fast",  # an alias: sturnus picks one of its candidates by weight
    messages=[{"role": "user", "content": "Hello"}],
)
```

## Features

- **Declared-weight routing** — every candidate of an alias declares an integer `weight` (default 1) and gets `weight / sum(weights)` of that alias's traffic. The split is the arithmetic the config states, drawn deterministically, and nothing measured — latency, error rate, queue depth — scales it. Latency and success-rate EWMAs are still recorded and shown on `/status`, labelled as not used for routing.
- **Session affinity** — a stateless `x-session-affinity` header pins follow-up requests to the same provider across pods.
- **Transparent passthrough** — only the `model` field is rewritten: the request body is otherwise forwarded byte-for-byte, preserving key order, number precision, and formatting. Responses, including SSE `text/event-stream` chunks, are relayed untouched as they arrive.
- **Memory-bounded** — request buffers are capped per request and in aggregate; bursts beyond the memory budget shed load with `429` + `Retry-After` instead of OOMing the pod.
- **Vertex AI support** — GKE Workload Identity auth via the metadata server, with automatic token refresh.
- **Optional caller authentication** — `api_keys` puts a bearer-key gate in front of the proxy, answering unauthenticated requests with `401` while leaving health and metrics routes keyless.
- **Zero infrastructure** — a single static binary; no Redis, database, or control plane.

## Why sturnus

Most LLM gateways are either a hosted SaaS you route all your traffic (and keys) through, or a large application with a significant surface area. sturnus is the opposite — **a single static binary** with a small auditable surface area, MIT-licensed and running entirely inside your infrastructure. It speaks the OpenAI API, so any OpenAI-compatible SDK works by changing one base URL. The core capability of sturnus is routing one model across several providers by declared weights — something that most gateways put behind an enterprise tier. Each sidecar routes independently from what it observes locally, so there is no shared state to run.

If you need a full LLMOps platform (spend tracking, prompt management, a UI, dozens of integrations), sturnus is not that.

<details>
<summary><b>Design choices &amp; deliberate omissions</b></summary>

sturnus has a bounded scope by design and has some deliberate omissions:

- **No request-level failover or retries.** sturnus is a transparent proxy: it surfaces upstream errors to the client verbatim rather than silently retrying within a black box. An error is recorded against the provider that produced it and shows up on `/status`, but it does not move traffic — the individual failed request is returned as-is. Client SDKs (OpenAI, Anthropic, LangChain, etc.) already ship mature, configurable retry and backoff; configure it there and let the caller decide what a failure is worth retrying.
- **Declared weights, not cost or quality-based.** Routing divides an alias by the weights you declare, and every model routed under that alias should be largely interchangeable. sturnus never trades quality or cost for speed, and it never measures its way to a preference you could have written down: a box is fast because the operator says it is fast.

</details>

## Contents

- [Configuration](#configuration)
- [Endpoints](#endpoints)
- [Observability](#observability)
- [Session affinity](#session-affinity)
- [How routing works](#how-routing-works)
- [Docker](#docker)
- [Building](#building)

## Configuration

sturnus reads `~/.config/sturnus.toml` unless `--config <path>` (`-c`) names a file. A repeated `--config` replaces the earlier one, which is how the container image's built-in `--config /config.toml` stays overridable. Startup fails, rather than running unconfigured, if the file is missing — or if `HOME` is unset and no `--config` was given.

```toml
listen = "127.0.0.1:4000"

# Optional: a second listener serving /metrics, /health, /healthz, /status
# Bind it to a routable address so Prometheus can scrape it.
# metrics_listen = "0.0.0.0:4040"

# Providers: where to send requests
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "${OPENAI_API_KEY}"

# Vertex AI via GKE Workload Identity (no API key needed)
[provider.vertex]
vertex_ai = { project_id = "my-gcp-project", location = "us-central1" }

# Model map: aliases the client uses → provider+model candidates
# Each candidate declares an integer `weight` (default 1); its share of the
# alias is its weight over the sum of the alias's weights.
[model]
fast = [
  { provider = "openai", model = "gpt-4o-mini", weight = 3 },
  { provider = "vertex", model = "google/gemini-2.5-flash" },
]

[routing]
ewma_alpha = 0.3          # smoothing for the latency and success-rate EWMAs, which /status reports and routing ignores
error_threshold = 0.5      # error-rate EWMA above which a session-affinity pin is broken
```

See [`config.example.toml`](config.example.toml) for all providers (Groq, Azure, Google AI Studio, Anthropic, local OpenAI-compatible) and options.

A config that still sets `routing.exploit_k` — the sharpness of the measured allocator this design replaced — fails at startup naming `weight` as its replacement, rather than loading and tuning nothing.

Environment variables in `${VAR}` syntax are interpolated at config load time. Where they're available in an `.env` file (`KEY=VALUE` per line), pass it with `--env-file`:

```bash
sturnus --env-file /secrets/.env
```

<details>
<summary><b>Per-provider request headers</b></summary>

Any provider takes a `headers` map, sent on every outbound request to it. Useful for upstream routing controls such as Vertex provisioned throughput:

```toml
[provider.vertex-pt]
vertex_ai = { project_id = "my-project", location = "us-central1" }
headers = { "X-Vertex-AI-LLM-Request-Type" = "dedicated" }
```

Headers are per provider, not per model alias, so pointing two aliases at the same provider gives both the same headers; use separate providers to vary them.

</details>

<details>
<summary><b>Requiring an API key</b></summary>

`api_keys` gates the proxy itself: every proxied `POST` must present one of the listed keys, and is otherwise answered with `401`.

```toml
api_keys = ["${STURNUS_API_KEY}", "old-key-still-rotating"]
```

Clients pass it exactly as they pass a provider key — with the OpenAI SDK it is the `api_key` argument.

- The only accepted form is `Authorization: Bearer <key>`, matched exactly. Comparison reads both sides fully, so a wrong guess costs the same time however close it was.
- The `GET` observability routes (`/health`, `/healthz`, `/status`, `/metrics`) stay keyless, so container health probes and Prometheus scrapes need no secrets.
- The keys gate the proxy and nothing else: they are never logged, and never relayed upstream — the provider's own `api_key` authenticates the outbound request.
- Startup refuses a key it could never use — blank, whitespace-padded, or non-ASCII — and refuses `api_keys = []` outright rather than treating it as no gate. Leaving `api_keys` unset keeps the proxy open, which is the right default on loopback.

</details>

<details>
<summary><b>Vertex billing attribution</b></summary>

For Vertex providers, sturnus can inject sidecar-controlled `labels` into outbound requests so the resulting spend shows up tagged in GCP Billing Export. The labels live in a top-level `[attribution]` block (typically deployment identity sourced from env vars) and are merged into each request body for any Vertex provider that opts in:

```toml
[attribution]
service = "${SERVICE_NAME}"
owner = "${OWNER}"
env = "${ENV}"

[provider.vertex]
vertex_ai = { project_id = "my-project", location = "us-central1", attribution = true }
```

Sidecar keys take precedence over any client-supplied `labels` keys with the same name; disjoint client keys are preserved. The feature is currently scoped to Vertex only. Keys and values must conform to Vertex naming rules (`[a-z][a-z0-9_-]{0,62}`).

</details>

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/chat/completions` | Proxied to upstream (model alias resolved) |
| POST | `/v1/embeddings` | Proxied to upstream (model alias resolved) |
| GET | `/health` | Returns `{"status":"ok"}` |
| GET | `/status` | Per candidate: cold/warm/degraded state, the declared weight and the traffic share it buys, the live in-flight count, and the latency EWMAs and error rate that are measured but not routed on |
| GET | `/metrics` | Prometheus metrics (see below) |

The `POST` proxy routes are served only on `listen`. The `GET` observability routes are served on `listen` and, when configured, on `metrics_listen`. When `api_keys` is configured, the `POST` routes additionally require `Authorization: Bearer <key>`; the observability routes never do.

## Observability

### Metrics

Prometheus metrics on `/metrics`, all labelled by `alias`, `provider`, `model`:

| Metric | Type | Meaning |
|--------|------|---------|
| `sturnus_requests_total` | counter | Completed responses, additionally labelled by `status_code` (includes upstream 4xx/5xx) |
| `sturnus_ttfc_seconds` | histogram | Streaming time-to-first-chunk (streaming requests only) |
| `sturnus_latency_seconds` | histogram | Non-streaming full response time (non-streaming requests only) |
| `sturnus_errors_total` | counter | Transport failures that never produced a response (timeout, connect, DNS) |
| `sturnus_in_flight_requests` | gauge | Requests outstanding against the candidate right now: counted from the moment it is chosen until its response ends, errors, or is dropped mid-flight |
| `sturnus_buffer_rejections_total` | counter | Requests shed with `429` because the aggregate buffer budget was full (no per-alias labels) |
| `sturnus_client_disconnects_total` | counter | Callers that hung up before their request completed (no per-alias labels). Benign: logged at debug, not ERROR |

Connection failures and the in-flight gauge are zero-initialised at startup, so a missing series is never mistaken for "no errors" and an idle candidate reads `0` rather than nothing. The in-flight gauge is written from the live counts on every scrape instead of being incremented per request, so the exported number cannot drift from the real count or stick above zero after a request is over. It counts a *candidate*, not an alias: a box reachable through several aliases reports the same number under each of their label sets.

### Status

`GET /status` answers "why is this box getting this share?" in one curl, rather than as an inference from a log excerpt. Each candidate of each alias is listed with its health, what the config declares for it, and what it has been measured doing:

```json
{
  "candidates": [
    {
      "alias": "fast",
      "in_flight": 2,
      "model": "llama-3.3-70b",
      "observed": {
        "error_rate": 0.0,
        "nonstreaming_ewma_ms": 134,
        "note": "measured, not used for routing: allocation is by declared weight",
        "streaming_ewma_ms": 131
      },
      "provider": "groq",
      "routing": { "share": 0.6, "weight": 3 },
      "status": "warm"
    },
    {
      "alias": "fast",
      "in_flight": 0,
      "model": "gpt-4o-mini",
      "observed": {
        "error_rate": 0.0,
        "nonstreaming_ewma_ms": 201,
        "note": "measured, not used for routing: allocation is by declared weight",
        "streaming_ewma_ms": 200
      },
      "provider": "openai",
      "routing": { "share": 0.2, "weight": 1 },
      "status": "warm"
    },
    {
      "alias": "fast",
      "in_flight": 0,
      "model": "qwen3-32b",
      "observed": {
        "error_rate": 0.7599,
        "nonstreaming_ewma_ms": 113,
        "note": "measured, not used for routing: allocation is by declared weight",
        "streaming_ewma_ms": null
      },
      "provider": "local",
      "routing": { "share": 0.2, "weight": 1 },
      "status": "degraded"
    }
  ]
}
```

- **`status`** — `cold` (no latency sample in either mode yet), `warm`, or `degraded` (its error-rate EWMA has passed `error_threshold`, which is also what breaks session-affinity pins to it).
- **`in_flight`** — requests this candidate has outstanding right now, counted from the moment it is chosen (by the router or by an `x-session-affinity` pin) until its response ends, errors, or is dropped mid-flight, so the wait it covers includes connecting, uploading, TTFB and the stream itself. It is one count per box, so a candidate listed under several aliases shows the same number in every entry, and `0` means nothing is in flight. Read it against `share`: several requests queued on a candidate whose share says it should be getting a fifth of the alias is congestion the router does not act on — busyness is not an input to placement yet, so the queue is visible, not routed around.
- **`routing`** — the whole allocation policy, as declared: `weight` is the integer the config gave this candidate in this alias, and `share` is that weight over the alias's total weight. Nothing measured scales it. The `local` row above is the point of the design: it is `degraded` with a 76% error-rate EWMA, and its share is exactly what its weight declares, because a share is not something the router discovers.
- **`observed`** — what the box has been measured doing: the streaming TTFC and non-streaming response-time EWMAs (`null` while cold in that mode) and the error-rate EWMA, under a `note` saying they are not used for routing. Seeing a box that has quietly gotten slow is, in this design, a human reading a number and changing a `weight`.
- **Weights are per alias.** A candidate reachable through several aliases appears once per alias, because its share is relative to whoever it competes with there.
- The numbers come from the same placement schedule the picker draws from, so `/status` and the routing decision cannot drift apart, and rendering them consumes no pick. They describe what the router would do *now*: a request riding an `x-session-affinity` pin bypasses the placement path entirely and follows none of it.

### Logging

Structured logging via `tracing`: coloured text on a terminal (respecting `NO_COLOR`), newline-delimited JSON when piped or redirected. Set the format with `--log-format <auto|pretty|json>` (or `STURNUS_LOG_FORMAT`) and the level with `RUST_LOG` (default `sturnus=info`).

Each request gets a span with a `request_id`; a client-supplied W3C `traceparent` propagates as `trace_id` and `parent_span_id` for cross-service correlation.

A caller that hangs up mid-request (closing the write half or the whole connection) is normal traffic, not a fault: it is logged at `debug` with the caller's address and counted in `sturnus_client_disconnects_total`, while genuine connection faults (such as a failed response write) stay at `error`.

At `debug` each routed request also logs the decision behind its `INFO routing request` line: the alias, the candidate that won, why it won, and every candidate's declared share of the alias — the same numbers `/status` reports:

```text
DEBUG routing decision alias=fast provider=groq model=llama-3.3-70b mode="nonstreaming" reason="fresh-by-weight" shares=groq/llama-3.3-70b=60.0% openai/gpt-4o-mini=20.0% local/qwen3-32b=20.0%
```

## Session affinity

Every response includes an `x-session-affinity` header (e.g. `openai/gpt-4o-mini`). Pass it back on subsequent requests to pin to the same provider — useful for multi-turn conversations where context is provider-specific:

```python
response = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Hello"}],
)
affinity = response.headers["x-session-affinity"]  # e.g. "openai/gpt-4o-mini"

response = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Follow-up"}],
    extra_headers={"x-session-affinity": affinity},
)
```

Fully stateless — works across pods with no shared state. The pin is honored until the pinned candidate's error-rate EWMA breaches `error_threshold` (at the default smoothing, roughly two consecutive errors), at which point the header is ignored and a new provider is selected — check the updated `x-session-affinity` in the response. Unknown or malformed headers fall back to normal routing.

A honored pin bypasses the placement path entirely, and its `debug` line says so — the same `routing decision` line, with `reason="explicit-pin"` and the alias, provider and model — rather than looking like the `reason="fresh-by-weight"` decision a routed request logs.

## How routing works

1. Client sends `POST /v1/chat/completions` with `"model": "fast"`.
2. Sidecar looks up the `fast` alias and reads each candidate's declared `weight` (an integer, default 1). A candidate's share of the alias is `its weight / sum of weights` — that is the entire allocation policy, and nothing measured — latency, error rate, queue depth — scales it.
3. A deterministic low-discrepancy sequence (the golden-ratio Weyl sequence) over the alias's pick counter turns those weights into picks: equal weights alternate exactly, a 2:1 pair splits 2:1 over a long run, and there is no RNG to seed, so a split is reproducible in a test and in a debug log.
4. The `model` field is rewritten to the real model name, auth headers are set, and the request is forwarded.
5. TTFC is measured at first chunk arrival and fed into the latency EWMA; the response status (any non-2xx counts as an error, including upstream 4xx) feeds the success-rate EWMA. Both are reported on `/status`, and neither feeds the pick.

The share is written, not discovered: the 95% box is `weight = 19` beside `weight = 1`. The trade is explicit — the router does not notice on its own that a box has gotten slow or started erroring, because it is not looking. `/status` reports the latency and error-rate EWMAs so an operator can notice, and the fix is a `weight`.

### The share a weight buys

For an alias of two candidates:

| weights | preferred box | other box |
|---------|---------------|-----------|
| 1 : 1   | 50%           | 50%       |
| 2 : 1   | 66.7%         | 33.3%     |
| 3 : 1   | 75%           | 25%       |
| 19 : 1  | 95%           | 5%        |

Every number is `weight / sum(weights)`, and `documented_share_table_matches_the_weighting` pins these rows against the arithmetic the router runs, so a change to the weighting fails a test rather than leaving this table describing arithmetic the code no longer runs. Weights allocate picks, and the Weyl stride is low-discrepancy rather than exact per window: over a short run the observed split drifts around the declared share, and `/status` reports the declared one.

A candidate that should never serve is left out of the alias's list. `weight = 0` is a startup error naming the alias and the candidate, because the weighted draw has no way to express "never".

### What config cannot do

The routing keys are `ewma_alpha` (the smoothing of the EWMAs `/status` reports) and `error_threshold` (the error-rate EWMA above which a session-affinity pin is broken), alongside the timeouts and the body-size caps. There is no key for:

- **Making a measured number count.** Latency, error rate and queue depth are recorded and reported, and no weight is scaled by any of them. There is no sharpness knob to turn, and `routing.exploit_k` — the knob this design removed — is a startup error naming `weight` as its replacement, not a key that loads and does nothing.
- **Disabling or bounding session affinity.** A caller-supplied `x-session-affinity` pin is honored for as long as it stays under `error_threshold` — there is no turn limit, and no setting ignores the header. A handful of long-lived sessions can therefore hold one candidate at 100% of their own traffic however idle it looks to the router.
- **A floor, a ceiling, or a preference a box carries.** Weight is declared per candidate inside one alias's list, so the same box can be `weight = 3` in one alias and `weight = 1` in another; there is no global preference, no floor keeping a broken box sampled, and no ceiling on a healthy one. The list is the policy.

## Docker

`listen` carries the proxy and its provider credentials, so bind it as narrowly as the deployment allows:

- **Kubernetes native sidecar** (recommended): the app container shares the pod's network namespace and reaches sturnus on `127.0.0.1:4000`, so keep `listen = "127.0.0.1:4000"` — bound to loopback, the credential-bearing proxy is unreachable from other pods entirely. To scrape metrics, set `metrics_listen = "0.0.0.0:4040"`: it serves only `/metrics`, `/health`, `/healthz` and `/status` (never the proxy) on the routable pod IP, and stays up until the proxy has finished draining so the final scrape window isn't lost. Containers in a pod share ports, so pick a metrics port the app doesn't use.
- **Standalone Docker** with the client in a *separate* container: it reaches sturnus over the container's IP, which a `127.0.0.1` bind refuses, so `listen` must be `0.0.0.0:4000`.

On Kubernetes, run sturnus as a [native sidecar](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/) — an init container with `restartPolicy: Always` (stable since v1.29). It then starts before the app container and is terminated after it, so the proxy is ready for the app's first request and stays up while the app drains.

Memory needs no tuning: the aggregate request-buffer budget defaults to half the container's memory limit (read from cgroups at startup, logged with its source), so a small sidecar sheds excess load with `429`s rather than getting OOM-killed. Override with `routing.max_buffered_bytes` if you want a different ceiling.

The image is published as a multi-arch (amd64/arm64) scratch container to `ghcr.io/sturnus-dev/sturnus`. Tags follow semver: `:latest`, `:5.0`, `:5.0.0`.

The image starts with `--config /config.toml` built in, so mounting the file there is all a container needs — the `~/.config/sturnus.toml` host default does not apply inside it. A `--config` you pass replaces the built-in one.

To inject secrets via a mounted `.env` file:

```bash
docker run -v ./config.toml:/config.toml \
  -v ./secrets.env:/secrets/.env:ro \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest --env-file /secrets/.env
```

<details>
<summary><b>Vertex credentials outside GKE</b></summary>

On GKE, workload identity is picked up automatically. Elsewhere, supply credentials one of two ways.

A service account key, pointed to by `GOOGLE_APPLICATION_CREDENTIALS` (recommended for production):

```bash
docker run -v ./config.toml:/config.toml \
  -v ./sa-key.json:/sa-key.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/sa-key.json \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

Or gcloud ADC for local dev, mounted to `$HOME/.config/gcloud/` (the image sets `HOME=/root`):

```bash
docker run -v ./config.toml:/config.toml \
  -v ~/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json:ro \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

</details>

## Building

### Host build — for developing on the machine you are on

```bash
cargo build            # debug
cargo build --release  # release, with LTO
cargo test
```

This builds for the machine you are on and links against *its* C library, stamping that library's symbol versions into the binary. It runs on that glibc and newer — which is exactly what you want for developing and testing locally, and what you do not want to install on a machine you cannot rebuild on. Copied to an older host, the dynamic loader complains on every start, before the process has done anything, and no log line, config option, or preflight check can explain it, because none of the program has run yet:

```
/usr/local/bin/sturnus: /lib/x86_64-linux-gnu/libc.so.6: weak version `GLIBC_2.39' not found (required by /usr/local/bin/sturnus)
```

### Release build — for installing on another Linux

Build the static musl target, which carries no C library dependency at all:

```bash
sudo apt-get install musl-tools                 # Debian/Ubuntu; provides musl-gcc
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl

scripts/check-static.sh target/x86_64-unknown-linux-musl/release/sturnus
```

`scripts/check-static.sh` is the proof, not a formality: it fails if the binary declares `libc.so.6`, `libm.so.6`, or `libgcc_s.so.1`, or names a program interpreter. The release pipeline runs it on the Linux assets before publishing them.

Both architectures in one command, prerequisites checked and each result verified:

```bash
scripts/build-static.sh all                     # x86_64 and aarch64
```

Building `aarch64` on an `x86_64` host needs a cross C compiler for ring's C code: `sudo apt-get install gcc-aarch64-linux-gnu`, or a musl cross toolchain providing `aarch64-linux-musl-gcc`. Building each architecture on its own architecture needs only `musl-tools`.

The published release assets and both container images are built from these targets, and the image runs from `scratch` — no C library on the filesystem at all.

### Run tests

```bash
cargo test
```

Tests run against the host build; nothing here changes what `cargo test`, `cargo install`, or a plain `cargo build --release` does.

## License

MIT
