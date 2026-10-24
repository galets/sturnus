# sturnus

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/sturnus-dev/sturnus)](https://github.com/sturnus-dev/sturnus/releases)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fsturnus--dev%2Fsturnus-blue?logo=docker)](https://ghcr.io/sturnus-dev/sturnus)

**Automatic latency-based routing across LLM providers. A single static binary, zero infrastructure.**

LLM providers have variable latency and availability that can break production features. sturnus is a lightweight sidecar that sits beside your app, exposes an OpenAI-compatible API, and automatically shifts traffic to whichever provider is fastest and available right now.

## Quick start

sturnus needs a config file — copy `config.example.toml` and add your providers. It reads `~/.config/sturnus.toml` by default; `--config <path>` reads one from anywhere.

**Docker** — best for production deployments and Kubernetes sidecars:

```bash
docker run -v ./config.toml:/config.toml \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

**`cargo install`** — best for local testing if you have a Rust toolchain:

```bash
cargo install sturnus
sturnus --config config.toml
```

Prebuilt static binaries for Linux and macOS (x86_64 and aarch64) are attached to every [release](https://github.com/sturnus-dev/sturnus/releases/latest).

Then point any OpenAI-compatible SDK at sturnus — the only change is the base URL:

```diff
- client = OpenAI(base_url="https://api.openai.com/v1", api_key="sk-...")
+ client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
response = client.chat.completions.create(
    model="fast",  # resolved by sturnus to the fastest available candidate
    messages=[{"role": "user", "content": "Hello"}],
)
```

## Features

- **Latency- and error-aware routing** — the fastest healthy provider gets the bulk of traffic, while slower or erroring ones keep a small, shrinking share. That share doubles as a probe, so a recovered provider wins its traffic back automatically, with no thresholds to trip.
- **Session affinity** — a stateless `x-session-affinity` header pins follow-up requests to the same provider across pods.
- **Transparent passthrough** — only the `model` field is rewritten: the request body is otherwise forwarded byte-for-byte, preserving key order, number precision, and formatting. Responses, including SSE `text/event-stream` chunks, are relayed untouched as they arrive.
- **Memory-bounded** — request buffers are capped per request and in aggregate; bursts beyond the memory budget shed load with `429` + `Retry-After` instead of OOMing the pod.
- **Vertex AI support** — GKE Workload Identity auth via the metadata server, with automatic token refresh.
- **Optional caller authentication** — `api_keys` puts a bearer-key gate in front of the proxy, answering unauthenticated requests with `401` while leaving health and metrics routes keyless.
- **Zero infrastructure** — a single static binary; no Redis, database, or control plane.

## Why sturnus

Most LLM gateways are either a hosted SaaS you route all your traffic (and keys) through, or a large application with a significant surface area. sturnus is the opposite — **a single static binary** with a small auditable surface area, MIT-licensed and running entirely inside your infrastructure. It speaks the OpenAI API, so any OpenAI-compatible SDK works by changing one base URL. The core capability of sturnus is automatic latency-based routing across providers — something that most gateways put behind an enterprise tier. Each sidecar routes independently from what it observes locally, so there is no shared state to run.

If you need a full LLMOps platform (spend tracking, prompt management, a UI, dozens of integrations), sturnus is not that.

<details>
<summary><b>Design choices &amp; deliberate omissions</b></summary>

sturnus has a bounded scope by design and has some deliberate omissions:

- **No request-level failover or retries.** sturnus is a transparent proxy: it surfaces upstream errors to the client verbatim rather than silently retrying within a black box. Error responses still feed the routing signal, so a flaky provider is quickly deprioritized for subsequent traffic — but the individual failed request is returned as-is. Client SDKs (OpenAI, Anthropic, LangChain, etc.) already ship mature, configurable retry and backoff; configure it there and let sturnus steer those retries toward the healthiest provider.
- **Latency-based, not cost or quality-based.** Routing optimizes time-to-first-chunk *within an alias*, and every model routed under that alias should be largely interchangeable. sturnus never trades quality or cost for speed — it just picks the fastest among options you've already deemed equivalent.

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
[model]
fast = [
  { provider = "openai", model = "gpt-4o-mini" },
  { provider = "vertex", model = "google/gemini-2.5-flash" },
]

[routing]
ewma_alpha = 0.3          # smoothing for the latency and success-rate EWMAs (higher = more reactive)
exploit_k = 3.0            # exploit sharpness: how strongly the best candidate is preferred (0.0 = even split; see How routing works)
error_threshold = 0.5      # error-rate EWMA above which a session-affinity pin is broken (routing weights are unaffected)
```

See [`config.example.toml`](config.example.toml) for all providers (Groq, Azure, Google AI Studio, Anthropic, local OpenAI-compatible) and options.

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
| GET | `/status` | Per candidate: cold/warm/degraded state, latency EWMAs, error rate, requests in flight, and the queue-adjusted weight and traffic share the router would assign right now |
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

`GET /status` answers "why is this box getting nothing?" in one curl, rather than as an inference from a log excerpt. Each candidate of each alias is listed with its health, what it has been measured doing, and what those numbers buy it in the router's schedule right now:

```json
{
  "candidates": [
    {
      "alias": "fast",
      "error_rate": 0.0,
      "in_flight": 2,
      "model": "llama-3.3-70b",
      "nonstreaming_ewma_ms": 134,
      "provider": "groq",
      "routing": {
        "nonstreaming": { "effective_latency_ms": 402.0, "share": 0.10390689941812137, "weight": 125 },
        "streaming":    { "effective_latency_ms": 393.0, "share": 0.11558669001751314, "weight": 132 }
      },
      "status": "warm",
      "streaming_ewma_ms": 131
    },
    {
      "alias": "fast",
      "error_rate": 0.0,
      "in_flight": 0,
      "model": "gpt-4o-mini",
      "nonstreaming_ewma_ms": 201,
      "provider": "openai",
      "routing": {
        "nonstreaming": { "effective_latency_ms": 201.0, "share": 0.8312551953449709, "weight": 1000 },
        "streaming":    { "effective_latency_ms": 200.0, "share": 0.8756567425569177, "weight": 1000 }
      },
      "status": "warm",
      "streaming_ewma_ms": 200
    },
    {
      "alias": "fast",
      "error_rate": 0.7599,
      "in_flight": 0,
      "model": "qwen3-32b",
      "nonstreaming_ewma_ms": 113,
      "provider": "local",
      "routing": {
        "nonstreaming": { "effective_latency_ms": 470.63723448563115, "share": 0.06483790523690773, "weight": 78 },
        "streaming":    { "effective_latency_ms": null, "share": 0.008756567425569177, "weight": 10 }
      },
      "status": "degraded",
      "streaming_ewma_ms": null
    }
  ]
}
```

- **`status`** — `cold` (no latency sample in either mode yet), `warm`, or `degraded` (its error-rate EWMA has passed `error_threshold`, which is also what breaks session-affinity pins to it).
- **`in_flight`** — requests this candidate has outstanding right now, counted from the moment it is chosen (by the router or by an `x-session-affinity` pin) until its response ends, errors, or is dropped mid-flight, so the wait it covers includes connecting, uploading, TTFB and the stream itself. It is one count per box, so a candidate listed under several aliases shows the same number in every entry, and `0` means nothing is in flight. It is also an *input* to routing, not just a reading: each outstanding request adds one of the candidate's own effective latencies to the wait the next request can expect there, which is why `groq` above — the fastest box when idle — is currently worth only ~10% of the alias. As its two responses finish, the inflation decays request by request and its share walks back on its own.
- **`routing.<mode>`** — the numbers a request of that mode is routed on, since streaming TTFC and non-streaming response time keep separate EWMAs. `weight` is `(best_effective / its_effective)^exploit_k` scaled so the best candidate in the alias gets 1000, `share` is that weight over the alias's total weight, and `effective_latency_ms` is the queue-adjusted latency the weight came from: the latency EWMA over the success-rate EWMA, times `(1 + in_flight)`. So it exceeds the latency EWMA both for erroring candidates (`local` in `nonstreaming`) and for busy ones (`groq`, at 3× its 131/134ms EWMAs because it carries two requests), and it is `null` while cold — nothing measured to adjust.
- **A `share` near 0.01 is the probe floor, not starvation.** Every weight is floored at 1% of the best candidate's (`PROBE_FLOOR`), which is what keeps a broken box sampled and lets it win traffic back; the floor applies after the queue adjustment, so even a candidate that is slow *and* busy is never deleted from its own schedule. A `share` of exactly 0 never happens; `local` above sits at the floor in `streaming` (no successes there).
- **Weights are per alias.** A candidate reachable through several aliases appears once per alias, because its share is relative to whoever it competes with there.
- The numbers come from the same weighted schedule the picker draws from, so `/status` and the routing decision cannot drift apart. They describe what the router would do *now*: a request riding an `x-session-affinity` pin bypasses the router entirely and follows none of it.

### Logging

Structured logging via `tracing`: coloured text on a terminal (respecting `NO_COLOR`), newline-delimited JSON when piped or redirected. Set the format with `--log-format <auto|pretty|json>` (or `STURNUS_LOG_FORMAT`) and the level with `RUST_LOG` (default `sturnus=info`).

Each request gets a span with a `request_id`; a client-supplied W3C `traceparent` propagates as `trace_id` and `parent_span_id` for cross-service correlation.

A caller that hangs up mid-request (closing the write half or the whole connection) is normal traffic, not a fault: it is logged at `debug` with the caller's address and counted in `sturnus_client_disconnects_total`, while genuine connection faults (such as a failed response write) stay at `error`.

At `debug` each routed request also logs the decision behind its `INFO routing request` line: the alias, the candidate that won, and every candidate's share of the alias in that request's latency mode — the same queue-adjusted numbers `/status` reports, so a busy candidate's shrunken share is visible on the very line that routed away from it:

```text
DEBUG routing decision alias=fast provider=openai model=gpt-4o-mini mode="nonstreaming" shares=groq/llama-3.3-70b=10.4% openai/gpt-4o-mini=83.1% local/qwen3-32b=6.5%
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

A honored pin bypasses the router completely, and says so at `debug` (`session affinity pin honored, router bypassed` with the alias, provider and model) rather than looking like the weighted `routing decision` a routed request logs.

## How routing works

1. Client sends `POST /v1/chat/completions` with `"model": "fast"`.
2. Sidecar looks up the `fast` alias and computes each candidate's **effective latency**: its latency EWMA divided by its success-rate EWMA. A candidate erroring with probability `p` needs ~`1/(1-p)` attempts per success, so errors inflate effective latency the same way slowness does.
3. The effective latency is then adjusted for the load the candidate is already carrying: it is multiplied by `(1 + in_flight)`, its number of requests outstanding. A box with `n` requests on track to take one effective latency each is expected to make an arrival queue for ~`n` of them before serving its own — the FCFS sojourn estimate `(n + 1)·L` — so the router scores the wait a request can *expect*, not the latency it happened to measure last. Nothing in flight, nothing adjusted: at `in_flight = 0` this is exactly the previous arithmetic, and a same-instant pair of requests no longer piles onto one box, because the first pick halves that box's score against every idle twin.
4. Each candidate is weighted by `(best_effective / its_effective)^k` — `k` is `routing.exploit_k`, default 3.0 — so the best gets the bulk of traffic and worse ones a shrinking-but-nonzero share. A deterministic low-discrepancy sequence (golden-ratio Weyl sequence) turns those weights into picks.
5. Because worse candidates always keep a small share, their EWMAs stay fresh — a provider that recovers (faster responses *or* errors stopping) wins traffic back automatically; a cold candidate (no latency data yet) probes at a quarter of the best candidate's rate, scaled by its success rate, until its first samples land. A busy candidate recovers its share too, and much faster: each response that finishes takes one step off the queue adjustment, so the box that served a burst is attractive again as soon as its queue drains.
6. The `model` field is rewritten to the real model name, auth headers are set, and the request is forwarded.
7. TTFC is measured at first chunk arrival and fed back into the EWMA; the response status (any non-2xx counts as an error, including upstream 4xx) feeds the success-rate EWMA.

The best provider is exploited heavily while worse ones keep enough traffic to stay measured. A candidate's probe share shrinks with how bad it looks but is floored at 1%, so re-detecting a recovered provider costs at most ~100 requests — and during an outage at most ~1% of an alias's traffic is spent on the failing candidate.

### Exploit sharpness: `routing.exploit_k`

`k` is the one knob that changes how an alias's traffic is divided. It acts on the *ratio* of effective latencies, so it behaves identically whether candidates differ by 20ms or 2s. A candidate's share is its weight over its alias's total weight, where `weight = round(1000 * (best_effective / its_effective)^k)` floored at 10 units (1% of the best candidate's). Effective latencies here are the queue-adjusted ones, so the table is the split of a *waiting* alias; a candidate carrying requests is scored `(1 + in_flight)`x slower than the table shows and its share shrinks accordingly until its responses finish. For an alias of two idle candidates the table is:

| worse candidate | k = 3.0 (default) | k = 1.0 | k = 0.0 |
|-----------------|-------------------|---------|---------|
| 2x slower | 11.1% | 33.3% | 50% |
| 5x slower | 1.0% | 16.7% | 50% |
| hard-failing | 1.0% | 1.0% | 50% |

Lower it when a healthy candidate is starved by a gap you consider noise; raise it when you want the best candidate to take as close to all of the traffic as the floor allows. The three 1.0% entries are the floor rather than the formula — their raw weights are 8, 1 and 0 units, each lifted to 10 — which is what guarantees every candidate stays measured.

**k = 0 is an even split, not a safe one.** Raising every ratio to the power 0 makes each warm candidate weigh exactly 1000, so errors stop counting entirely: a hard-failing candidate keeps half the traffic of a two-candidate alias, and the probe floor never engages because no weight comes near it. The cold probe flattens with it, since `rate^0` is 1 — a candidate failing from the start still probes at the full quarter rate. What survives is outside the weighting: `error_threshold` still breaks `x-session-affinity` pins to that candidate and still reports it `degraded` on `/status`, but routing weights never consult the threshold.

### What config cannot do

The routing keys are `ewma_alpha`, `exploit_k` and `error_threshold`, alongside the timeouts and the body-size caps. There is no key for:

- **Disabling or bounding session affinity.** A caller-supplied `x-session-affinity` pin is honored for as long as it stays under `error_threshold` — there is no turn limit, and no setting ignores the header. A handful of long-lived sessions can therefore hold one candidate at 100% of their own traffic however idle it looks to the router.
- **A per-candidate weight.** A candidate is a `provider` and a `model`; shares come only from what each has been observed doing. To prefer a provider, make it faster or route it under its own alias.
- **The probe floor and the cold probe share.** Both are fixed: 1% of the best candidate's weight, a quarter of its rate. They bound how much traffic an outage eats and how fast a recovery is noticed, so they are not a tuning surface.
- **The queue adjustment.** A candidate with `n` requests outstanding is scored as if its effective latency were `(1 + n)` times larger — each in-flight request costs one measured latency of expected wait. That multiplier is fixed at 1.0 because it states how a box serialises work, not how much you like it; disabling it would put latency-only routing back, where the first request of a burst decides where the whole burst goes. `exploit_k` remains the knob for how hard the resulting ratios are exploited.

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

```bash
# Development
cargo build

# Release (static binary with LTO)
cargo build --release

# Run tests
cargo test
```

## License

MIT
