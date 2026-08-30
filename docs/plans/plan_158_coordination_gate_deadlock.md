# Plan 158: The Coordination Drain Waits For An Observation That Cannot Be Written

## Status

In the build order, written 2026-08-30, found while deploying
[Plan 147](plan_147_scrape_state_ownership.md) Stage 2. The deploy hung for
nineteen minutes with every production DAG parked, and would have hung
indefinitely; it was aborted by hand.

Priority **90 (critical)** proposed. Effort **S** — the code change is XS, the
tests and the deploy evidence are the work. Position in the build order is not
this document's to choose; see [`docs/PLANS.md`](../PLANS.md).

This is a **defect in a safety mechanism**, which is the worst place to have
one: the machinery that exists to make deploys safe is the machinery that
stopped a deploy and stalled the pipeline.

## The problem

`scripts/redeploy.sh` cannot reliably deploy. Whether it works is a race with
the DAG schedule, and losing the race hangs forever rather than failing.

Observed on 2026-08-30, deploying `ops` and `processing` for Plan 147 Stage 2:

| Time (UTC) | Event |
|---|---|
| 05:56:34 | `redeploy.sh ops processing` declares deploy intent, generation 17 |
| 05:56:34 | Coordination enters `draining` |
| 06:00:00 | Seven scheduled DAGs fire and park on the gate sensor |
| 06:04 → 06:15 | `airflow_gate_observations` climbs 6 → 8 → 9; `oldest` pinned at 06:00:00.69 |
| 06:15 | Aborted by hand. Intent released, generation 18, fleet recovered |

The blocker count **rises**. `orphan_checker` fires every five minutes and each
new run adds one. Nothing in the loop can ever reduce it.

Both images had already been rebuilt. Nothing was recreated, so the abort took
the `MUTATED=0` branch of `_on_exit` and released cleanly — the one part of
this that behaved exactly as designed.

## Root cause: the write is below the return that always fires

`_DeployIntentSensor.poke()`, [`airflow/dags/sensors.py:55`](../../airflow/dags/sensors.py):

```python
row = hook.get_first(
    """SELECT di.intent, cs.phase, ... , cs.generation
         FROM deploy_intent di CROSS JOIN coordination_state cs ...""")
if row is None or row[0] != "none":
    return False                    # ← always taken during a deploy
...
blocked = row[1] in {"requested", "draining", "active", "validating"} and row[2]
if blocked:
    ...
    hook.run("""INSERT INTO coordination_gate_observations ...""")
    return False
```

`row[0]` is `deploy_intent.intent`. `redeploy.sh` sets it to `pending` before
it begins draining. So during **every** deploy the first branch returns, and
the `INSERT` beneath it is unreachable.

The drain then waits on exactly that table.
`gate_observation_query` ([`ops/coordination_drain.py:132`](../../ops/coordination_drain.py))
counts DAG runs in `queued`/`running` with no matching row in
`coordination_gate_observations` for the current generation, and
`_authorize` refuses while that count is non-zero.

### The cycle

1. `redeploy.sh` sets `deploy_intent.intent = 'pending'`, then polls authorize.
2. Authorize requires `airflow_gate_observations == 0`.
3. The count is non-zero because gated DAG runs are `running`.
4. Those runs are `running` because the sensor holds them while intent is
   `pending` — it is a `mode="reschedule"` sensor, so the run stays alive.
5. The sensor cannot record the observation that would clear them, because the
   `intent != "none"` return fires first.

Every arrow is load-bearing. Breaking any one of them ends the deadlock.

### The confirming evidence

`public.coordination_gate_observations` is **empty for generation 17 and for
every generation that has ever existed.** The `INSERT` has never executed once
in production. A mechanism that has never run is not a mechanism.

### Why it has not bitten before, and why that is not luck to rely on

The count is zero whenever no affected DAG run is in `queued`/`running` at the
instant intent is declared. Runs are normally short, so most declarations land
in a gap and authorize returns 200 immediately — generation 16 went active at
02:52 UTC, in the quietest part of the night. Deploy during a gap and it works;
overlap a fire and it hangs.

That is a **race whose losing branch is unbounded**, not an intermittent
slowdown. Roughly, the wider the DAG schedule's duty cycle, the likelier the
loss; `orphan_checker` alone fires twelve times an hour.

### How it got here

Commit `4ec8d35` (*"feat: gate Airflow admission by coordination scope"*, Plan
142 Stage 1, 2026-08-24) extended an existing sensor. The original body was:

```python
row = hook.get_first("SELECT intent FROM deploy_intent LIMIT 1")
```

— block while intent is not `none`, and nothing else. Stage 1 added the
coordination-scope query and the observation write **beneath** that
pre-existing return, without noticing the older guard already covered the
deploy case. The new code was correct in isolation and dead in composition.

Worth stating plainly because it is the reusable lesson: the defect is not in
either half, it is in the seam, and no test covered the seam because each half
had tests of its own.

## Design

### The fix — record the observation before the intent check

The observation means *"this run has seen the gate and is holding."* That is
true whether the hold comes from deploy intent or from coordination phase, so
it must be recorded before either return.

```python
row = hook.get_first(...)          # unchanged
if row is None:
    return False

blocked_by_coordination = (
    row[1] in {"requested", "draining", "active", "validating"} and row[2]
)
if blocked_by_coordination:
    _record_observation(hook, row[3], self.coordination_dag_id, context)

if row[0] != "none" or blocked_by_coordination:
    return False
return True
```

Admission behaviour is **unchanged**: the sensor still returns `False` in every
case it returns `False` today. The only difference is that a run parked by
deploy intent now leaves a trace, which is what the drain is asking for.

### The guardrail — bound the authorize wait

`_prepare_coordination` ([`scripts/redeploy.sh:135`](../../scripts/redeploy.sh))
polls authorize in a `while :` loop with no timeout and no escape. That is what
turned a defect into a nineteen-minute silent hang.

The script's own decision 2 already argues the principle — *"A health gate, not
`sleep 10`… Every recreated, pollable service must reach `healthy` or the
deploy fails loudly"* — and the drain loop is the one wait that does not follow
it. Bound it, and on expiry fail loudly, print the drain evidence naming the
blocking sources, and exit through the `MUTATED=0` release path.

This is defence in depth, not the fix. It converts an unbounded hang into a
failed deploy with a diagnosis attached. **Both halves are wanted:** with only
the guardrail the deploy still cannot proceed, and with only the fix the next
unrelated stuck source hangs the script again — which is precisely what the
stray container did earlier in the same session.

## Non-goals

- **Not a redesign of the coordination state machine.** Generations, phases and
  scopes are sound; one write is in the wrong place.
- **Not a change to what the drain counts.** Excluding gate-parked runs from the
  count would be treating the symptom, and would need to distinguish "parked"
  from "working" — which is the very thing the observation records.
- **Not a change to admission.** No work is admitted during a deploy that is not
  admitted today. A fix that let DAGs run during a deploy would be a worse
  defect than the one it replaced.
- Not a change to `mode="reschedule"`. Plan 142 chose it deliberately so a
  waiting sensor does not hold a pool slot.

## Rejected alternative — clear intent before draining

Have `redeploy.sh` leave `intent = 'none'` until authorization succeeds, so the
sensor reaches its coordination branch and writes the observation.

Rejected because intent is the **admission boundary**. The comment in
`poke()` says so directly: *"Request fixes the immutable scope and is the
admission boundary. Do not admit another run merely because the operator has
not yet asked for the first drain read."* Deferring intent would open a window
in which new DAG runs are admitted after a deploy has been declared — trading a
visible hang for an invisible race against a fleet mid-mutation.

The observation write does not need intent to be `none`. It needs to be above
the line that returns.

## Stages

### Stage 1 — Make the observation reachable

The sensor change above, plus tests. Ships independently and fixes the defect
on its own.

Deploying it is subject to the very race it fixes, so it must go out in a
declared quiet window, or behind an operator hold on the affected DAGs — see
Stage 0 below, which is why Stage 0 is first.

### Stage 0 — Write down the escape hatch first

Before any code, add to [`runbook_host_maintenance.md`](../runbooks/runbook_host_maintenance.md)
the recognition and recovery for this state, because it is live in production
right now and will be until Stage 1 deploys:

- **Recognise it:** `GET /coordination/drain-status` shows
  `drained: false`, `blockers: ['airflow_gate_observations']`, and a count that
  does not fall between reads.
- **Recover:** interrupt `redeploy.sh`. With nothing recreated, `_on_exit`
  releases intent and the fleet resumes by itself. If the trap does not fire,
  `curl -X POST http://localhost:8060/deploy/complete`.
- **Avoid:** pause the affected DAGs and let in-flight runs finish before
  declaring intent.

Stage 0 is documentation only and carries no deploy, so it is not subject to
the defect it describes.

### Evidence — Stage 0, 2026-08-30

Shipped as *When the drain never drains — the gate deadlock*, the closing
subsection of §11 of
[`runbook_host_maintenance.md`](../runbooks/runbook_host_maintenance.md), plus a
pointer to it from §9.

**§11, not §9, on purpose.** §9 is the `maintenance` Airflow pool, which the
runbook is at pains to distinguish from coordination — §10's own note warns that
`/coordination/status` reads `phase=none` throughout a pool hold. This defect is
entirely coordination: intent, generations, `drain-status`. §11 already teaches
the exact reads it needs, and it teaches one of them wrongly for this case: its
interpretation list says *"`known` with a positive count names real admitted
work and its oldest start"*, which is precisely the false reading that cost
nineteen minutes on 2026-08-30. The runs the count names were parked, not
working. The new subsection sits below that list and corrects it for the one
source where it does not hold.

The subsection is the closing one in §11 rather than mid-section so its heading
does not capture §11's trailing release-after-failure prose. §9's pointer is a
blockquote beside the existing note about the sensor's 600s timeout, because a
pool-side reader arriving at "the fleet is quiet, is that healthy?" should find
this from there.

| Item | Covered by |
|---|---|
| Recognise | `drain-status` JSON with `drained: false` and `blockers: ['airflow_gate_observations']`; two reads five minutes apart; the count rises rather than falls; `oldest_started_at` pinned; the empty-table confirmation for the generation |
| Recover | `Ctrl-C`, why `MUTATED=0` is guaranteed there, `_on_exit`'s expected output, the `/deploy/complete` fallback, and the generation increment as proof the release landed |
| Avoid | pause the affected DAGs, the query that names the runs that would park, and why pausing beats timing a gap |
| Why it hangs | `_prepare_coordination`'s unbounded `while :`, contrasted with the script's own 300s health gate; the operator is the timeout until Stage 2 |
| Incident | the 2026-08-30 timeline, and that the `INSERT` has never executed for any generation |

`tests/test_planning_docs.py`: **33 passed** before the change and 33 after —
the relevant one is `test_no_markdown_link_in_docs_is_dangling`, which resolves
the four new relative links (this plan, `sensors.py`, `redeploy.sh` twice).

No deploy, by design: Stage 0 changes documentation only, so it is not subject
to the defect it describes — which is the whole reason it goes first.


### Stage 2 — Bound the authorize wait

The `redeploy.sh` guardrail, its timeout asserted in
`tests/test_deploy_script.py` alongside the existing health-gate timeout
relationship.

### Stage 3 — Prove it against the case that fails today

Deliberately declare a deploy overlapping a DAG fire — the exact shape that hung
on 2026-08-30 — and show authorization is reached. Then confirm
`coordination_gate_observations` is non-empty for that generation, which is the
first time in the mechanism's life it will be.

## Tests

1. During a **deploy-intent** drain, a gated DAG run writes exactly one
   observation row per `(generation, dag_id, run_id)`. This is the test whose
   absence allowed the defect; it must fail against today's sensor.
2. The drain's `airflow_gate_observations` count reaches zero once every live
   affected run has poked at least once.
3. The sensor still returns `False` in every case it does today — deploy intent
   pending, coordination draining, and both at once. Admission is unchanged.
4. An observation for generation N does not satisfy generation N+1.
5. A run poking repeatedly produces one row, not one per poke (the
   `ON CONFLICT` path).
6. `redeploy.sh`'s authorize wait expires, fails loudly with the blocking
   sources named, and releases intent through the `MUTATED=0` path.

## Intersections

### Plan 142 — this is its mechanism, and its Stage 1 introduced the defect

[Plan 142](plan_142_planned_host_maintenance.md) owns the coordination gate.
The defect arrived with its Stage 1 and does not invalidate its design; the
scoped drain, the generation counter and the admission boundary all work. It
does mean **Plan 142's maintenance window cannot be run until this is fixed** —
the window depends on declaring intent and waiting for a drain, which is the
operation that hangs.

Plan 142's Stage 4 runbook already recorded that two images were stale and
needed deploying before the window. That deploy would have hit this.

### Plan 147 — blocked on this

[Plan 147](plan_147_scrape_state_ownership.md) Stage 2 is merged to master
(`4b2426b`) and **not deployed**; its deploy is what surfaced this. Stage 1's
`V048` is applied and inert, so production is in a consistent, safe state: the
new columns exist, `last_detail_fetched_at` is null everywhere, and queue
behaviour is exactly what `V040` produced. There is no urgency to force the
Stage 2 deploy past this defect.

### Plan 152 — the same session produced its evidence too

The first blocker on 2026-08-30 was not this one. A `cartracker-archiver-run-*`
one-shot container from Plan 145 Stage 6 had printed its results at
2026-08-29 15:32 and then never exited — 0.00% CPU, sleeping, six threads, the
classic non-daemon connection-pool shape — and `container_processes` counted it
as live work for fourteen hours.

That is [Plan 152](plan_152_scheduled_worker_lifecycle.md)'s subject, not
this one's, and it is fresh evidence for a row whose trigger is otherwise
speculative: a one-shot that outlives its work does not merely waste a
container, it blocks every deploy. Removing it cleared that blocker and revealed
this one underneath.

### An observability gap, recorded but not claimed here

That container's healthcheck had failed **1,727 consecutive times** across
fourteen hours with nothing surfacing it. Whatever covers container health did
not raise a container that had been unhealthy since the previous afternoon.
That is a third defect, separate from both of the above, and it is written down
here only so it is not lost — it wants its own plan.

## Success criteria

1. A deploy declared while gated DAG runs are live reaches authorization.
2. `coordination_gate_observations` is non-empty after any drain that overlapped
   a live affected run.
3. No deploy waits unboundedly: the authorize wait either succeeds, or fails
   loudly naming the blocking source, and always releases intent when nothing
   was mutated.
4. Admission during a deploy is unchanged — no DAG run starts work that would
   not start today.
