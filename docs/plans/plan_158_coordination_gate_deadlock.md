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


### Evidence — Stage 1, 2026-08-30

Shipped in [`airflow/dags/sensors.py`](../../airflow/dags/sensors.py) as the
design above, with one addition: the `INSERT` is now the module-level constant
`GATE_OBSERVATION_SQL`, consumed by a module-level `_record_observation`. That
mirrors `coordination_drain`'s own query builders, which are module-level for
exactly this reason — so `tests/integration/sql` can execute the real
statement rather than a copy of it that is free to drift.

**The seam test failed first, which was the point.** Written against the sensor
as it stood at `220395a`, five of the new unit tests failed and all of them
failed the same way — `assert set() == {(17, 'orphan_checker', ...)}`, nothing
written. The twenty-four truth-table cases passed unchanged, before and after:
admission was already correct and the fix leaves it alone.

**Admission is unchanged, proved exhaustively.** `test_admission_is_unchanged_by_
the_observation_write` parametrises the full cross product of intent ×
phase × intersects — three intents, five phases, both scope answers, thirty
cases — and asserts against the formula the old code computed:

| intent | phase / scope | poke | before | after |
|---|---|---|---|---|
| `!= none` | any | `False` | ✔ | ✔ |
| `none` | blocking phase ∧ intersects | `False` | ✔ | ✔ |
| `none` | otherwise | `True` | ✔ | ✔ |

A missing coordination row still fails closed, and a run with no discoverable
`run_id` still blocks and now still writes nothing — the pre-existing guard is
carried into `_record_observation` unchanged.

**End to end against real Postgres.** Flyway's 48 migrations applied to a
throwaway container, the 2026-08-30 shape reconstructed — `intent='pending'`,
`phase='draining'`, `scope=['processing']`, generation 158, two live affected
DAG runs — and the real sensor driven through a psycopg2-backed hook, so the
jsonb `?|` read, the `INSERT` and `gate_observation_query` all executed for
real. Three pokes per run, as the reschedule sensor would:

| | blockers before | blockers after | observation rows |
|---|---|---|---|
| sensor at `220395a` | 2 | **2** | **0** |
| sensor after Stage 1 | 2 | **0** | 2 |

The first row is the deadlock: the count does not move, and would not have
moved however long `redeploy.sh` polled. Six pokes produced two rows, not six.
Re-running the drain query at generation 159 returns to 2, so a released
generation is not authorized by the previous one's observations. No poke in
either run returned `True`.

| Plan §Tests item | Covered by |
|---|---|
| 1 — one row per `(generation, dag_id, run_id)` during a deploy-intent drain | `test_a_run_parked_by_deploy_intent_records_exactly_one_observation`, `test_each_live_run_of_a_dag_is_observed_under_its_own_key` |
| 2 — the drain's count reaches zero | `test_every_live_affected_run_leaves_the_key_the_drain_looks_up` (the key contract) and `test_gate_observation_count_falls_to_zero_as_live_runs_observe` (the real query, real Postgres) |
| 3 — admission unchanged | `test_admission_is_unchanged_by_the_observation_write`, plus the fail-closed and unblocked cases |
| 4 — generation N does not satisfy N+1 | `test_an_observation_is_written_against_the_generation_it_saw`, `test_an_observation_does_not_satisfy_the_next_generation` |
| 5 — repeated pokes, one row | `test_repeated_pokes_write_one_key_not_one_per_poke`, `test_repeated_observation_of_one_run_keeps_a_single_row` |

Two further tests guard the note the plan makes about drift:
`test_the_gate_read_selects_the_columns_this_row_supplies` ties the canned row's
column order to the real `SELECT`, and
`test_the_drain_and_the_sensor_read_the_same_declaration` asserts, for every
surface, that the DAGs `gate_observation_query` filters on are exactly the DAGs
whose sensor `intersects` term would fire. The `ADMISSION_SURFACES` agreement
the plan asks for is now checked rather than remembered.

**On why the unit tests do not use a database.** The gate `SELECT` cannot run
outside Postgres, so its row is supplied directly; the writes are recorded.
Executing Postgres SQL against a substitute engine would have tested the
substitute. Everything that must meet real SQL is in
`tests/integration/sql/test_ops_queries.py`, beside the existing
`test_gate_observation_query_resolves`, and runs in CI.

Suite: **3115 passed**, 470 deselected (`-m "not integration"`), up from 3095 —
twenty new unit cases. `tests/integration/sql/`: **38 passed** against the
throwaway Postgres, up from 35. Ruff clean.

**Not deployed, deliberately.** Stage 1's own deploy is subject to the race it
fixes and needs the quiet window or the operator hold Stage 0 wrote down; that
is Stage 3's business. `scripts/redeploy.sh` is untouched — the unbounded
authorize poll is Stage 2.

### Stage 2 — Bound the authorize wait

The `redeploy.sh` guardrail, its timeout asserted in
`tests/test_deploy_script.py` alongside the existing health-gate timeout
relationship.

### Evidence — Stage 2, 2026-08-30

Shipped in [`scripts/redeploy.sh`](../../scripts/redeploy.sh) as **decision 7**,
written into the header beside the six Plan 144 decisions it belongs with. The
`while :` is now bounded by `DRAIN_TIMEOUT` (`DEPLOY_DRAIN_TIMEOUT`, default
600s), checked on the 409 branch before the next sleep, and expiry calls a new
`_dump_drain_evidence` before returning 1.

**Ordering: this stage ran before Stage 3, which the plan asks for the other
way round.** Stage 3 was to supply the first measurement of a healthy drain and
so the number this stage needs; it cannot run yet, because Stage 1 is merged and
not deployed and its own deploy is subject to the race. The number is therefore
*derived rather than measured*, which is the same trade decision 2 makes for the
health gate, and the derivation is asserted rather than remembered:

| | |
|---|---|
| Floor | 360s — the tightest gated DAG schedule (`*/5`, `orphan_checker` and `results_processing`) plus `deploy_intent_sensor`'s 60s `poke_interval`, because a run parked on the gate records its observation only on its *next* poke |
| Default | 600s — Plan 142 D9's stated expectation for deploy intent, *"a short service replacement — expected duration under ten minutes"*, and 240s of headroom over the floor |
| Ceiling | none needed; expiry is cheap and the deploy is retryable |

`test_the_drain_timeout_survives_one_fire_and_park_cycle` computes the floor
from the DAG files and `sensors.py` rather than restating it, so tightening a
schedule or slowing the sensor past the bound fails CI instead of manufacturing
a deploy failure. Stage 3 may lower the default once it has a real measurement;
nothing here depends on 600 specifically.

**Failing in the wrong direction is cheap, and that governed the choice.** A
timeout that is too long costs parked DAGs; too short costs a failed deploy that
released intent cleanly and can be re-run, or raised with an environment
variable the error message names. Given no measurement, err long.

**`host_maintenance.py`'s identical wait is deliberately left unbounded.**
`wait_until_active` polls the same drain with no deadline, and that is correct
there: package and reboot duration are variable and an operator is watching.
Deploy intent is not that. The asymmetry is written into decision 7 so the next
reader does not "fix" the other one.

**Driven for real, not only asserted.** The definitions above `# --- main ---`
were sourced against a stub ops API that answers `/coordination/authorize` with
409 forever and serves the 2026-08-30 `drain-status` shape, with
`DEPLOY_DRAIN_TIMEOUT=8`:

```
Beginning scoped coordination drain (waiting up to 8s)...
  In-scope work is still draining (0s of 8s); retrying in 2s.
  ... 2s ... 4s ... 6s ...
ERROR: in-scope work did not drain within 8s.

--- coordination drain evidence (http://localhost:8060/coordination/drain-status) ---
Blocking sources: airflow_gate_observations
  airflow_gate_observations: status=known count=8 oldest_started_at=2026-08-30T06:00:00.690000+00:00
{ ...the full evidence document... }
Nothing has been recreated, so deploy intent is released on exit
...
Signalling deploy complete...
```

Exit code 1, and the stub recorded exactly one `POST /deploy/complete` — the
`MUTATED=0` release path in decision 3, reached without the operator. That is
the whole of the change: the nineteen minutes of 2026-08-30 become eight
seconds and a diagnosis. With nothing listening on `OPS_URL`,
`_dump_drain_evidence` says so and returns 0 rather than dying under `set -e`,
so a dead ops API still reaches the release path.

The elapsed drain time is now printed on the success line too, which is the
measurement Stage 3 will read off a real deploy.

| Plan §Tests item | Covered by |
|---|---|
| 6 — the wait expires | `test_the_authorize_wait_is_bounded` (the bound is consulted on the *retry* branch, before sleeping — a deadline only reachable after success is not a deadline) |
| 6 — fails loudly with the blocking sources named | `test_drain_expiry_names_the_blocking_sources`, which lifts the embedded formatter out of the script and **executes** it on the 2026-08-30 document, so the real program meets the real shape: blockers named, counts and `oldest_started_at` printed, an `unknown` source's `reason` printed, drained sources not listed as blockers, full document dumped |
| 6 — releases intent through the `MUTATED=0` path | `test_drain_expiry_leaves_nothing_mutated_so_intent_releases` asserts every `MUTATED=1` is preceded by a `_prepare_coordination` call, so expiry cannot strand a half-deployed fleet; confirmed live by the single `/deploy/complete` above |
| — the number is derived | `test_the_drain_timeout_survives_one_fire_and_park_cycle` |

All four failed against `b7b90ac`'s script and pass after — the same discipline
Stage 1 used, and for the same reason: a guardrail nobody has watched fail is
not a guardrail. `tests/test_deploy_script.py`: **32 passed**, up from 28. Ruff
clean.

**Not deployed.** Stage 2 changes only `scripts/redeploy.sh`, which the
production VM reads from the checkout, so it takes effect on the next `git pull`
there and carries no image build. Stage 3 is the first run that exercises it.

### Stage 3 — Prove it against the case that fails today

Show that a drain held open across a DAG fire now reaches authorization, and
that `coordination_gate_observations` is non-empty for that generation — the
first time in the mechanism's life it will be.

**Do not wait for a lucky overlap.** The 2026-08-30 hang was not caused by
declaring intent shortly before a fire; it was caused by a *second* blocker
holding the drain open across one. Intent was declared at 05:56:34 and the only
blocker was a stray `container_processes` one-off; without it the drain would
have authorized in the gap before 06:00 and nothing would have been learned.
Reproducing the failure therefore means reproducing the hold, not the timing.

#### The decoy protocol

Hold the drain open deliberately with a one-off that does nothing, let a DAG
fire park runs against it, and read the observation table before releasing.

```bash
cd /opt/cartracker

# 1. A decoy that blocks the drain and touches nothing.
docker compose run -d --no-deps --name plan158-decoy dbt_test sleep 900

# 2. Declare intent. With no drift to deploy this recreates nothing and exits
#    MUTATED=0 — a full coordination exercise with zero fleet change.
bash scripts/redeploy.sh ops processing

# 3. Expect blockers ['container_processes'], count 1.
curl -s localhost:8060/coordination/drain-status | python3 -m json.tool

# 4. After a */5 fire parks runs — THE PROOF:
docker exec cartracker-postgres psql -U cartracker -d cartracker -At -c \
  "SELECT count(*) FROM public.coordination_gate_observations;"

# 5. Release the decoy; the drain should authorize on the next poll.
docker rm -f plan158-decoy
```

**Step 4 is the experiment.** Rows appearing *while the decoy still holds the
drain* is what distinguishes fixed from broken; at exactly that point on
2026-08-30 the table was empty. Steps 5 and 6 only confirm the drain then
clears.

Why `dbt_test` and why `docker compose run`, both load-bearing:

- `oneoff_processes` ([`container_health/collector.py:67`](../../container_health/collector.py))
  counts a container only if it carries `com.docker.compose.oneoff=True`, which
  `docker compose run` sets and `docker compose up` does not.
- The service label is looked up in `SERVICE_CONTRACTS`, and **an unrecognised
  service raises, making the whole source return `unknown` — which fails
  closed.** An arbitrary `docker run` would hang the drain for the wrong reason
  and prove nothing. `dbt_test` is a declared `one_shot` whose `analytics`
  surface intersects the deploy scope.
- The command is not part of the identification, so overriding it with `sleep`
  gives a blocker that touches no data. `april-processor` also qualifies and is
  deliberately not used — it is Plan 145's recovery worker.
- `--no-deps` keeps the decoy from starting anything else.

#### Bounding the experiment

Every gated DAG is parked from step 2 to step 5, so keep it to a single fire
cycle — six to eight minutes, not the nineteen the incident ran. The `sleep 900`
is headroom, not a duration; the decoy is removed by hand well before it
expires.

**Abort condition:** if after step 5 the blocker becomes
`airflow_gate_observations` and its count does not fall across two reads, the
fix did not take. Interrupt `redeploy.sh`; with nothing mutated `_on_exit`
releases intent and the fleet resumes, exactly as it did on 2026-08-30. That
path is written up in
[`runbook_host_maintenance.md`](../runbooks/runbook_host_maintenance.md) by
Stage 0, which is why Stage 0 came first.

#### This stage runs before Stage 2, deliberately

Stage 2 guards against *other* sources blocking a drain; it changes nothing
about what this stage measures. Running Stage 3 first also supplies the number
Stage 2 needs — the elapsed time between releasing the decoy and reaching
authorization is the first measurement of a healthy drain with observations
actually being written, and a timeout chosen before that is a guess.

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
