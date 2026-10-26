# Glossary

## Routing

**Alias** — the model name a caller asks for, standing for a set of candidates.
The caller never names a candidate.

**Candidate** — one provider and model pair, reachable through one or more
aliases. A candidate is a box's queue, not an alias's: the same pair reached
through two aliases is one box.

**Weight** — an operator-declared integer preference for a candidate within an
alias, defaulting to 1. A candidate's share of that alias is its weight over
the sum of the alias's weights. Weight is declared, never measured: a box that
is fast because the operator says it is fast is a box with a high weight. It is
a preference among the candidates that are eligible and equally able to start
now — a box that is stood down, or one with a prompt running, is passed over
whatever its weight buys it.

**Turn** — one request from a caller to an alias.

**Session** — one conversation as the caller understands it. A new chat window
is a new session. A subagent's conversation is a separate session.

**Pin** — the binding of one session to one candidate for the session's
lifetime, so that the conversation's cache stays warm. A pin is a session's
claim on a candidate, not a request's. Two things make one: a **session key**
whose first turn is placed and then bound, and an **explicit pin**, where the
caller names the box outright. A pin is bounded — a session quiet past the idle
TTL, or evicted at the table's cap, places fresh — and it lives in one instance,
so replicas place their own sessions.

**Session key** — the identity a caller gives a conversation, by which a pin is
found again. A new chat window is a new key, so it is a fresh pick. Pins are
keyed per alias, so the same session inside a second alias places fresh there.

**Explicit pin** — a caller naming one `provider/model` outright. It is checked
before the session table and honored whatever that box is doing, including a
box that is demoted: it is a request's instruction, not a session's claim, so it
neither creates a pin nor is moved by one.

**Fresh pick** — a new session choosing a candidate by weight and busyness,
independently of any session it descends from.

**Placement** — the choice of candidate for a session that does not have one
yet. A candidate that is demoted is not asked at all; among the rest, a
candidate with a prompt running is passed over for one that is idle, and weight
decides between candidates that are equally able to start. Placement is a
decision about sessions, never about the turns of a session already placed.

**Busy** — a candidate with at least one prompt running: from the moment a turn
is committed to it until that turn's response is over. A candidate with nothing
running is idle. Busy is a state, not a load figure.

**Least busy** — the candidate carrying the fewest prompts, used only when
every eligible candidate is busy: the new session goes to the least busy group,
and weight decides within it.

**Eligibility** — a candidate's standing to take on new sessions right now. It
is a gate, not a discount: a candidate without it is not asked at all, so its
share while it lacks it is exactly zero, and no part of the alias is held aside
for it.

**Demotion** — the loss of eligibility after a transport failure: a turn that
never got any answer from the box. It lasts a fixed window and ends in one
step, at full standing, with nothing carried over. Nothing probes a demoted
candidate back, and its first new session is the probe. A candidate that
answered is not demoted, whatever it answered — an upstream error is a box that
is up.

**Overflow** — a turn served by a candidate other than the one its session is
pinned to, because the pinned candidate is demoted. Overflow **re-pins**: the
new candidate becomes the session's candidate, the old one is forgotten, and
nothing moves the session back. A migrated session is a new state of affairs,
not a detour, because nobody can say whether the cache it left is still there.

**Probe floor** — the traffic share kept for a candidate that is getting
nothing, so that its recovery can be noticed. Not part of this model: recovery
is not noticed, it happens.
