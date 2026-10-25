# Glossary

## Routing

**Alias** — the model name a caller asks for, standing for a set of candidates.
The caller never names a candidate.

**Candidate** — one provider and model pair, reachable through one or more
aliases. A candidate is a box's queue, not an alias's: the same pair reached
through two aliases is one box.

**Weight** — a caller-declared integer preference for a candidate within an
alias, defaulting to 1. Traffic divides in proportion to weights. Weight is
declared, never measured: a box that is fast because the operator says it is
fast is a box with a high weight.

**Turn** — one request from a caller to an alias.

**Session** — one conversation as the caller understands it. A new chat window
is a new session. A subagent's conversation is a separate session.

**Pin** — the binding of one session to one candidate for the session's
lifetime, so that the conversation's cache stays warm. A pin is a session's
claim on a candidate, not a request's.

**Fresh pick** — a new session choosing a candidate by weight, independently of
any session it descends from.

**Placement** — the choice of candidate for a new session. Weight is the
preference; a candidate with a prompt running is passed over for one that is
idle. Placement is a decision about sessions, never about the turns of a
session already placed.

**Busy** — a candidate with at least one prompt running: from the moment a turn
is committed to it until that turn's response is over. A candidate with nothing
running is idle. Busy is a state, not a load figure.

**Eligibility** — a candidate's standing to take on new sessions right now.

**Demotion** — temporary loss of eligibility after a transport failure. A
demoted candidate takes no new sessions until it regains eligibility on its
own; nothing probes it back.

**Saturation** — a candidate carrying as many outstanding turns as it is
allowed. A saturated candidate is not demoted; it simply cannot take more.

**Overflow** — a turn served by a candidate other than the one its session is
pinned to, because the pinned candidate is demoted. Overflow **re-pins**: the
new candidate becomes the session's candidate, the old one is forgotten, and
nothing moves the session back. A migrated session is a new state of affairs,
not a detour, because nobody can say whether the cache it left is still there.

**Probe floor** — the traffic share kept for a candidate that is getting
nothing, so that its recovery can be noticed. Not part of this model: recovery
is not noticed, it happens.
