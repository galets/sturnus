# 0002. Pin the box, not the llama.cpp slot

- **Status:** Accepted
- **Date:** 2026-10-26

## Context

llama.cpp keeps KV cache per *slot*, and a box can run several slots
(`--parallel N`). A router that pins a session to a box is therefore coarser
than the cache it is trying to protect: two sessions pinned to the same box can
evict each other, and a turn that lands on a different slot re-prefills the
conversation. [ADR 0001](0001-declared-weights-over-measured-latency.md) bought
cache affinity with a box-level pin, so the gap is a real question about the
design, and steering a slot from sturnus would mean injecting a request-body
field — a deliberate break with the transparency that proxy is built on.

The question was measured against the deployment's own stack (nginx →
llama-swap → llama-server) rather than argued from upstream documentation. The
instrument was llama.cpp's own response: `timings.cache_n` and
`usage.prompt_tokens_details.cached_tokens` arrive through both hops, so the
server reports what it reused.

The measurements, on the two boxes this proxy runs in front of:

| | DDR box | GPU box |
|---|---|---|
| cold prefill, 8348 tokens | 9070.9 ms (920 tok/s) | 6645.7 ms (1256 tok/s) |
| the same prompt warm | 50.7 ms | 69.1 ms |
| conversations cached at once | 1 | 1 |
| 3 concurrent distinct conversations | serialized | 6.78s / 13.38s / 19.98s walls |

A 53-token turn from a second session destroys the resident conversation's
8342-token cache, and that conversation pays a full 9030.2 ms on its next turn.
Idle does not evict: the cache is warm after 120 s of quiet. On the OpenAI
endpoint the deployed llama-server runs, `slot_id`, `slot_n`, `cache_id`,
`prompt_cache_key` and `grammar` are inert — accepted with `200`, never
validated, no effect — while OpenAI's own fields validate with llama.cpp's
`400`s relayed verbatim, which is also the proof that llama-swap forwards the
body unmodified. `/slots` and `/props` are unreachable through the swap.

## Decision

sturnus pins the box and does not steer the slot. No slot field is injected, no
per-provider transparency opt-in exists, and `/status` reports no slot mapping.

The evidence for that is the granularity match: a box caches exactly one
conversation, so a box-level pin is already as fine as the cache. There is
nothing finer for the router to chase, and the router cannot see slots through a
model-swapping proxy, so a reported slot mapping would be a guess.

## Consequences

**Bought.** The proxy stays transparent: it rewrites `model` and nothing else,
so a request body that reaches it is the body that reaches the box, and no
provider needs an opt-in flag. The pin's value is now a number rather than an
assumption: 9.05 s on the DDR box and 6.60 s on the GPU box per 8348 tokens,
against 50–70 ms warm.

**Given up.** If a box is ever started with `--parallel N`, `N > 1`, and a
`--cache-ram` budget that fits N conversations, box-level pins become coarser
than the cache and this decision has to be revisited. The trigger is
observable, not theoretical: concurrent conversations stop serializing, and
interleaved conversations keep `cache_n` near the prompt length. The follow-up
is written (`.scratch/priority-and-pins/issues/08-slot-steering-if-parallel-gt-1.md`
in the working tree) and names the llama-swap opt-in
(`models.<id>.filters.setParams`) that would carry the field.

**Accepted.** One conversation per box is warm, so a second concurrent session
on a box costs the first a full re-prefill — 9.03 s paid by a conversation that
did nothing but send 53 tokens. The router does not cure that; it avoids it, by
placing a new session on an idle box and keeping a pinned session on its own.
A box that runs one slot is a box that serves one conversation at a time, and
the weights an operator declares are shares of a box's *sessions*, not of its
turns.

**Accepted.** The measurements are of a deployment, not of llama.cpp. They
justify the design as it runs here; a box started with different launch args
changes the answer, which is why the trigger above is written as a measurement
to take rather than a fact to remember.
