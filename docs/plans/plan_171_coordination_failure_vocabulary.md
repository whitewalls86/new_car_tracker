# Plan 171: One Failure Vocabulary for the Deploy and Maintenance Path

## Status

**Backlog, written 2026-09-02**, out of [Plan 162](plan_162_testing_census_and_restructure.md)
Stage 6c, which fixed two instances of this defect and found there was no
contract to fix the rest against.

**This is a production defect, not a testing gap.** Stage 6c's assertion half
belongs to Plan 162 because the missing guard was a test; this half is about
what the operator reads at 06:15, and it belongs on its own number. The
distinction matters because the two have different evidence: a test either
exists or does not, whereas this is judged by whether the next unfamiliar
failure on this path is diagnosable from its own output.

[`docs/PLANS.md`](../PLANS.md) owns priority and effort; this document does not
choose them.

## The measurement

Taken 2026-09-02, against the tree at Plan 162 Stage 6c.

**Two operator entry points drive one state machine.** `scripts/redeploy.sh`
runs a deploy; `scripts/host_maintenance.py` runs a host window. Between them
they call eight endpoints served by `ops/routers/deploy.py` and
`ops/routers/coordination.py`:

| Entry point | Endpoints it drives |
|---|---|
| `redeploy.sh` | `/deploy/start`, `/coordination/begin-drain`, `/coordination/authorize`, `/coordination/begin-validation`, `/deploy/complete` |
| `host_maintenance.py` | `/coordination/status`, `/coordination/drain-status`, `/coordination/request`, `/coordination/begin-drain`, `/coordination/authorize`, `/coordination/begin-validation`, `/coordination/complete` |

**Eight private handlers stand behind them**, and seven return a status string:
`_set_intent`, `_request`, `_transition`, `_cancel`, `_authorize`, `_complete`,
`_submit_host_evidence`. The eighth, `_intent_release`, returns a bare `bool`.

**The vocabulary already exists and is nowhere declared.** Counted across both
files:

| status | returns |
|---|---|
| `error` | 19 |
| `conflict` | 14 |
| `ok` | 8 |
| `invalid` | 7 |
| `unavailable` | 2 *(added by Stage 6c)* |
| `locked` | 2 |
| `stale` | 1 |
| `blocked` | 1 |

Nothing declares that set. Each route hand-writes its own `if`/`elif` chain over
whichever subset it happens to know about, and `error` — the largest bucket by
some way — is the undifferentiated one that everything else falls into.

**Eleven `503`s across the two files, six of them `"Database unavailable."`**
After Stage 6c two of those six are correct, because `/deploy/start` and
`/coordination/request` now separate an unreachable Postgres from one that
refused a write. The other four are still returned for failures that are not a
database being unavailable: `/deploy/complete`, `/coordination/begin-drain`,
`/coordination/begin-validation` and `/coordination/cancel`. `_status()`'s is
ambiguous — it catches everything a read can raise.

**Information is lost at all three layers, and that is the actual defect.**

| Layer | What it collapses to |
|---|---|
| Handler | `"error"`, for a constraint violation, a missing row, a policy refusal and an unreachable database alike |
| Route | `503 "Database unavailable."` |
| Script | `redeploy.sh`: exit 22 with no body, until Stage 6c's `_post_ops`. `host_maintenance.py`: exit 1, for everything |

No single layer is unreasonable on its own. The defect is that each one discards
what the layer below it knew, so by the time it reaches a terminal there is
nothing left to read.

## The incident record

Two production failures, one day apart, wearing the same face:

1. **2026-09-01, the deploy.** `POST /deploy/start {"targets":["dashboard"]}`
   returned `503 "Database unavailable."` Postgres was healthy throughout. The
   real cause was a `CHECK` constraint that no `(targets, scope)` pair from a
   surface-less service could satisfy.
2. **2026-09-01, merging Plan 162 Stage 7.** The identical symptom on the
   identical endpoint, for an unrelated reason: psycopg2 counts placeholders
   inside comments, so a statement expected four parameters where the caller
   passed three. It was found by seven Layer 4 failures in CI and diagnosed from
   a log.

Both are recorded in [Plan 162's Stage 6c
section](plan_162_testing_census_and_restructure.md). **The second is the
stronger evidence, because it arrived from outside the stage that predicted the
first.** Two unrelated defects were indistinguishable at the response, in the
log, and to the operator.

Stage 6c fixed the two *write* paths and gave `redeploy.sh` a POST helper that
prints the response body. It deliberately did not touch the release path, the
transitions, or `host_maintenance.py`'s exit codes — there was no contract to
fix them against, and inventing one inside a testing stage would have been the
wrong place for it.

## What this is not, recorded so it is not re-argued

**Not repo-wide.** There are 42 `HTTPException` call sites in `ops/routers/`.
The status-string layer this plan is about exists in two files. Every other
router raises directly and has no intermediate vocabulary to reconcile.

**Not a cross-service HTTP standard.** [Plan
106](plan_106_code_review_cleanup.md) C1 asked for exactly that shape of
uniformity — `/ready` should return 200 everywhere — and `docs/PLANS.md` records
that **it inverted**: `archiver` and `dbt_runner` now return 503 deliberately as
Plan 131's job-in-flight contract. Uniformity for its own sake across services
was tried here and was wrong. This plan covers one surface with an incident
behind it and does not generalise past it.

**Not a register item.** [Plan 163](plan_163_documented_code_quality_fixes.md)
excludes "anything with an incident behind it" and "anything that is really a
refactor… if an item grows a design question, it leaves this register and
becomes a plan. Say so in writing when it happens." This has two incidents and
one design question. This section is that writing.

**Not new HTTP status codes.** The codes are mostly fine. What is missing is
that a status is produced in one place, mapped in another, and read in a third,
with nothing keeping the three in step.

## Design

### The convention may already exist — check Plan 134 before inventing one

[Plan 134](plan_134_archiver_endpoint_failure_contract.md) is the same genus
running the other way, and it is further along. Its endpoints **under**-signal —
an archiver processor returns a summary dict, the route returns it with a `200`,
and `raise_for_status()` passes on a run that failed. This surface
**mis**-signals: it collapses distinct causes into one `503` that names the
wrong component. Different direction, different consumer — DAGs there, operator
scripts here — but the same sentence describes both: *the HTTP layer discarded
what the layer below it knew.*

134 has already settled a shape for the answer:
`HTTPException(500, detail=dict(result, failure_reason=reason))`, which
`dbt_runner/app.py:214` was already using and which Plan 131 Stage 5 applied to
three `/pack/bronze/*` routes. Plan 162 Stage 6c's `500` carrying the SQL cause
landed on the same shape without knowing it.

**So Stage 0 starts by reading 134's contract, not by drafting one.** A second
failure vocabulary three months after the first would be the defect this plan is
about, committed at the level of plans. If 134's shape fits, this plan adopts it
and shrinks to the mapping and the assertion.

It is not, however, a stage of 134. That plan is mid-flight — Stage 1 deployed
2026-08-30 and observing to 2026-09-06 — on a different service, and widening a
plan under observation to a second surface would spoil the observation it is
running.

### The vocabulary is declared once and asserted, not documented

A contract nothing checks is the shortcut this plan exists to close, so the
deliverable is not a paragraph in a runbook. The repository already has the
shape twice: `SERVICE_CONTRACTS` is checked for exact coverage against
`docker-compose.yml`, and Plan 162 Stage 3 gave the two health-sensor censuses
one declared source.

**The assertion is static, because both sides are literals.** Collect the status
strings each handler can return; collect the statuses each route maps; fail in
either direction — a handler that can return something its route does not
handle, or a route branching on a status nothing produces. That is the same
mechanical read as `test_every_compose_service_has_exactly_one_contract_and_no_contract_is_stale`,
and it is what makes the next handler inherit the contract rather than
re-deriving it.

### `_intent_release`'s `bool` is the visible shortcut

Seven of eight handlers converged on a status string. The eighth returns
`True`/`False`, collapsing five distinct outcomes:

1. the singleton `coordination_state` row is missing
2. **a non-deploy coordination holds the record** — `row[1] != "none" and row[0] != "deploy"`
3. `CLEAR_DEPLOY_INTENT` matched no row
4. `RELEASE_DEPLOY_COORDINATION` matched no row
5. an exception of any kind

**Case 2 is not an error.** It is the facade correctly refusing to release
someone else's host maintenance — a `409`, and the request path already
discriminates its equivalent as `locked`. Rendering a deliberate policy refusal
as `503 "Database unavailable."` is a second kind of lie on top of the masking
one.

### The release path fails more quietly than the request path

Worth stating because it inverts the obvious priority. `_prepare_coordination`
runs before any container is recreated, so a failed request aborts under
`set -e` with `MUTATED=0`, and decision 3's trap releases intent and the fleet
resumes by itself — loud and self-healing.

The release runs in the exit trap as
`curl … || echo "Warning: failed to signal /deploy/complete"`. The `||` swallows
it, the script's exit code is unchanged — **so a successful deploy that fails to
release still exits 0** — and the Telegram alert only fires on a non-zero exit.
Deploy intent stays set, coordination stays non-`none`, every gated DAG stays
parked, and the operator's entire output is the word `Warning`.

### The scripts are the last layer and need the same words

`redeploy.sh` gained `_post_ops` in Stage 6c, which prints status and body on
failure. What remains there: `/deploy/complete` does not go through it, and the
authorize poll reads only `%{http_code}` with `-o /dev/null`, so it cannot print
a body it never captured.

`host_maintenance.py` is in better shape at the message level — `api_request`
raises `MaintenanceError` carrying the HTTP code *and* the decoded body. Its gap
is the exit code: every failure prints `ERROR: …` and returns 1, so a runbook
step, a wrapper or a future automation cannot tell "another coordination holds
the record, wait" from "the ops API is down, escalate" without parsing English.

### What "a good error code system" means here, precisely

Three properties, and none of them is a new HTTP code:

1. A status is decided once, at the layer that knows why, and never widened on
   the way up.
2. Every route maps every status its handler can produce, and this is asserted
   rather than reviewed.
3. The operator's terminal shows the cause — in the body, and in an exit code
   that distinguishes refusal from failure.

## Stages

### Stage 0 — Decide the vocabulary

Enumerate what the eight handlers return today (the table above is the start,
not the answer — it counts literals, it does not judge them). Decide the member
set, each member's HTTP rendering, and the two scripts' exit codes. Fold
`locked`, `blocked` and `stale` in or keep them, with a reason either way.

The deliverable is one table, agreed before any code moves. **Safe stopping
point:** if the set turns out to be what exists minus `error`, that is a result
worth recording and Stage 2 gets much smaller.

### Stage 1 — Declare it and assert it

One module holding the set and its HTTP mapping; one test asserting both
directions of coverage across the two routers. The test lands before the
conversion so the conversion is graded by it.

### Stage 2 — Convert the handlers

`_intent_release` first, because it is the outlier, the quietest failure and the
one carrying a mislabelled policy refusal. Then `_transition`, which serves
`begin-drain` and `begin-validation` and is the remaining pair on `redeploy.sh`'s
path. Then the rest, which are on `host_maintenance.py`'s path only.

### Stage 3 — The script end

`/deploy/complete` through `_post_ops`; the authorize poll capturing a body;
`host_maintenance.py` exiting with a code that separates refusal from failure.

## Files

- `ops/routers/deploy.py`, `ops/routers/coordination.py` — the eight handlers
  and their routes
- a new module for the declared set — location decided in Stage 1
- `tests/ops/routers/` — the coverage assertion
- `scripts/redeploy.sh`, `scripts/host_maintenance.py` — the operator end
- `docs/TESTING.md` — only if the assertion needs a row; decided in Stage 1

## Out of scope

- **Every other `ops` router.** `admin`, `snapshots`, `auth`, `info`, `scrape`
  and `users` raise `HTTPException` directly and have no status layer to
  reconcile. Nothing observed says they need one.
- **Every other service.** See Plan 106 C1's inversion above.
- **`shared/db.py`'s failure classification.** `UNREACHABLE_ERRORS`,
  `db_failure_cause` and the `exc_info=True` logging landed in Plan 162 Stage 6c
  and are this plan's input, not its work.
- **Which transitions are legal.** Plan 142 owns the state machine's semantics.
  This plan changes only how a refusal or a failure is reported.
- **The Airflow sensors** that read `coordination_state`. They read the row, not
  the HTTP surface.

## Success criteria

1. Every status a handler can return is a member of one declared set, and a test
   fails when it is not.
2. Every route maps every status its handler can produce, and a test fails when
   it does not.
3. No route renders a failure the database *answered* as `503 "Database
   unavailable."`
4. A deploy whose release fails is not silent: it names the cause and does not
   exit 0.
5. `host_maintenance.py` exits with a code that distinguishes a refusal from an
   unreachable API.

## Intersections

### Plan 162 — the testing census

Stage 6c is where this was found and it fixed the two write paths. This plan
finishes what that stage's own argument implies and its exit criteria did not
cover. Nothing here re-opens Stage 6c or changes its evidence.

### Plan 142 — planned host maintenance

Owns `coordination_state`, the transition table and
`scripts/host_maintenance.py`. It is in closeout and owes no code, so its
artifacts are input. This plan reports failures differently; it does not make a
different set of transitions legal.

### Plan 158 — the redeploy.sh decisions

Decision 7 bounded the drain wait and printed evidence on expiry; decision 8
(Stage 6c) made a failing POST print the body. Stage 3 here is the third
instalment of the same argument: a failure that costs the wait and the diagnosis
is two failures.

### Plan 134 — the archiver endpoint failure contract

The nearest sibling, and the reason Stage 0 is a reading task before it is a
drafting one. See [the design section](#the-convention-may-already-exist--check-plan-134-before-inventing-one).
Its `failure_reason` shape is a candidate answer here; its plan boundary is not
one to cross while it is observing.

### Plan 163 — the code quality register

Where this would have gone had it no incident behind it. It has two, so it is
here instead — which is the register's own rule, applied.
