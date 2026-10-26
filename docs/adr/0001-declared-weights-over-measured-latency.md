# 0001. Declared weights replace measured-latency routing

- **Status:** Accepted
- **Date:** 2026-10-25

## Context

sturnus used to divide an alias by what it measured. Each candidate's latency
and error-rate EWMA fed an allocator, and the operator tuned the sharpness of
that allocator with `routing.exploit_k`: a box answering twice as slowly lost
share, a box whose error rate drifted lost share, and a box that had been
demoted was fed a probe share so its recovery could be noticed. The split an
operator saw was the output of that arithmetic, and it moved on its own.

That design has two properties that turned out to be wrong for the traffic this
proxy is deployed in front of.

The first is legibility. The share a box got was a function of sharpness,
effective latency, a queue tax, a probe floor and a cold-probe share. Nobody
could say what an alias would do, and a routing change could not be tested
against a stated expectation — the expectation was "whatever the allocator
computes". The starvation this router is being changed to fix was invisible for
exactly that reason: the arithmetic was doing something, and nothing on show
said what.

The second is affinity. A coding tool's conversation is worth keeping on the box
that has already paid for its prompt, and a measured allocator moves traffic by
design. A box that got briefly slow pulled new sessions onto itself, and an
error-rate threshold broke a pinned conversation outright. The router was
re-deriving a preference the operator had already stated, and the thing it cost
was the cache.

## Decision

Traffic is divided by declared weight. Every candidate of an alias declares an
integer `weight` (default 1) and its share of that alias is `weight /
sum(weights)`. Nothing measured — latency, error rate, queue depth — scales it.
Picks come from a deterministic low-discrepancy sequence over the alias's
counter, so equal weights alternate exactly and a declared ratio holds over a
long run, with no RNG to seed.

Around that arithmetic, three rules, in order:

- **Eligibility is a gate.** A candidate that failed at the transport level —
  connect, DNS, TLS, connect timeout, read timeout — takes no new sessions for
  `routing.demotion_secs`, and its share during that window is exactly zero.
  Nothing probes it back; its first new session is the probe.
- **An idle box is preferred.** A candidate with a prompt running is passed over
  for an idle one whatever its weight; when every candidate is busy the least
  busy group competes and weight breaks the tie.
- **A session stays put.** A session key's first turn is placed and pinned, and
  every later turn rides that pin, busy or not. A demotion re-pins the session
  to its new box, permanently.

The latency and success-rate EWMAs are still recorded and still reported on
`/status`, under a note that they are not used for routing. `routing.exploit_k`
and `routing.error_threshold` are gone, and a config that sets either fails at
startup naming the key and what replaced it, rather than loading and tuning
nothing.

## Consequences

**Given up: the router no longer notices a box that has gotten slow.** That is
the trade, and it is total. A box whose latency triples keeps its full share. A
box that answers `500` to everything keeps serving everything, because it
answered. The measurement still exists, and `/status` shows it, so the
detection is a human reading a number and changing a `weight`. The router will
not do it, and no key will make it do it again.

**Bought: a split that is exactly what you wrote.** A 95% share is written as
`weight = 19` beside `weight = 1`, not discovered by probing. The config carries
the whole of the allocation policy, so the share table in the README and the
example config is arithmetic rather than an approximation — and a test reads
those rows out of the docs and recomputes them, so the docs cannot drift from
the code. The same determinism makes a routing decision reproducible in a test
and in a debug line, which is what makes "why this box, this turn?" answerable
at all.

**Bought: cache affinity.** A pinned conversation is not moved by a measurement,
so the box that has paid for the prompt keeps the conversation. The cost is
stated where the feature is: pins are per instance, bounded by an idle TTL and a
table cap, and a demotion moves a session for good.

**Accepted: the operator owns the split.** Weights are a claim about the boxes,
and a stale claim is a real misallocation with no feedback loop to correct it.
Adding a box means writing its share down; a box that gets slower means editing
a number. A candidate that should get nothing is left out of the alias's list —
`weight = 0` is a startup error, because the weighted draw has no way to express
"never".

**Accepted: health is binary.** A transport failure stands a box down for a
window and the window ends in one step, at full weight. There is no ramp, no
probe share and no recovery ladder, so the first session placed on a box that is
still down eats that failure. The one case that is not a stand-down is an
upstream `4xx`/`5xx`, which is relayed to the caller verbatim.

**Migration.** A config that sets `routing.exploit_k` or
`routing.error_threshold` stops at startup. Delete the keys and write the split
as `weight`. A pin that used to break after roughly two consecutive errors now
holds.
