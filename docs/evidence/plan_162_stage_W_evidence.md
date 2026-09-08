# Plan 162 Stage W — evidence

**Issue:** CAR-82 · **Measured and demonstrated:** 2026-09-07

Everything bulky about Stage W: the design that was built and rejected, the
census that replaced it, the five mutations watched failing, and the two limits
this stage states rather than closes.

---

## 1. The detour, and why it was wrong

**The first implementation was a registry** — `tests/contracts/closed_set_contracts.py`,
a curated tuple of cross-module contracts, each naming a producer, a consumer
and how to derive each side, with one parametrized meta-test asserting
`accepted ⊆ emitted`. It worked, it was demonstrated failing against the
reintroduced defect, and it was committed as `ca74ba3` (reachable in the
reflog; the branch was reset to `a723022` and rebuilt).

**It was rejected for being the thing this plan exists against, and the
objection is exact.** `DORMANT_SUITES` and `DECLARED_SKIPS` are lists too, and
they work — because each is compared against a **derived population**:
directories on disk, skips pytest actually reported. Both directions close, so
an unlisted member fails. The contract registry had no derived population at
all. Nothing could tell you a third contract existed and was unregistered. It
was one instance and no mechanism, repeated at n=2.

**The evidence that it was wrong is stronger than the argument.** The rebuild's
first derived census immediately found a third instance sitting two files from
one the registry covered: `airflow/dags/sensors.py:95` tests a coordination
phase against `{"active", "draining", "requested", "validating"}`, a vocabulary
owned by a Flyway `CHECK` constraint in `V043__coordination_state.sql`. The
only thing guarding it was `test_coordination_admission.py` asserting that the
literal string appears in `sensors.py`'s *source text* — the paraphrase shape
`docs/TESTING.md` warns about, which passes forever.

**A second wrong answer, recorded because it shaped the third.** The census was
also wrong the first time. It counted *closed sets in production* — 110 of
them, 104 with a member restated somewhere under `tests/` — and asked who
restates them. That population is undifferentiable: counting cross-module
"consumers" structurally gives 87 of 110, dominated by `ok`, `error`, `year`,
`price`. `SOLVER_OUTCOMES` reads as a contract with 41 other modules. From that
measurement a curated list looks inevitable, and it is not — the subject was
wrong, not the conclusion. The right subject is the **guard**: where does a
module test an incoming value against a locally written literal vocabulary.

---

## 2. The census that replaced it

**262 literal comparisons** across `production_python_files()` (the raw count is
297; `if __name__ == "__main__"` and friends are noise and were removed by
reading the output rather than trusting the number). They are three buckets, and
only one is reachable.

**Bucket 1 — owned outside this repository. Unreachable.**

```
scripts/audit_git_refs.py:177       git(...,"config","--get","fetch.prune").strip() != "true"
scripts/build_public_recaps.py:326  if token.type == "heading_open"
airflow/dags/notifications.py:71    str(getattr(state,'value',state)) == "failed"
```

git's config output, markdown-it's token types, Airflow's `TaskInstanceState`.
No artifact here owns them, so there is nothing to derive from and a rule could
only be a list.

**Bucket 2 — owned by the module's own package. Reachable, different rule.**

```
ops/routers/coordination.py:553-636  if result == "conflict" / "invalid" / "ok"
```

`result` comes from a function in `ops/`. One package holds both ends, so
nothing can drift. The repair would be "name it as a constant" — a style rule
with a large blast radius and no defect behind it.

**Bucket 3 — owned by another artifact in this repository. The defect class.**

The owner corpus was derived rather than guessed: **18 `CHECK (<column> IN
(...))` columns** across `db/migrations/`, holding **14 distinct
vocabularies**.

| | |
|---|---:|
| Comparisons naming a CHECK-constrained column | 83 |
| …whose literal is actually a **member** of that column's vocabulary | **33** |
| …whose literal belongs to something else, correctly untouched | 50 |

**The membership test is what makes the rule usable, and it was found by
watching the alternative fail.** Scoping by column name alone produced 9 false
positives — `result.status == "ok"` in `ops/routers/deploy.py` read as a claim
about `artifacts_queue.status`. An earlier attempt fixed that by deriving which
tables each module's `.sql` files touch, which worked (`coordination_release.py`
resolves to no table, so its `status == "success"` fell out of scope) but cost a
module→constant→file→table chain and **missed 11 sites in
`scripts/host_maintenance.py`**, which reads the same vocabulary over HTTP.
Inverting the test from "stale literal" to "member literal" removed all 9 false
positives on its own and recovered the 11, so the table machinery was deleted.
That is the whole discriminator: a literal that is not in the vocabulary cannot
be a copy of it.

---

## 3. What shipped, and why it is two rules and not one

`shared/db_vocabularies.py` declares each of the 14 vocabularies once, as a
`StrEnum` — a member *is* a `str`, so a comparison against a row is unchanged,
psycopg2 needs no cast, and iterating the class gives the whole set.

* **`test_every_check_constrained_column_has_one_declared_vocabulary`** compares
  that module to the migrations in both directions and then requires equality.
* **`test_no_module_retypes_a_database_vocabulary_it_could_import`** fails a
  comparison against a bare literal that is a member of a constrained column's
  vocabulary.

**Neither works alone, and the mutation below shows it rather than asserting
it.** Renaming a value in the migration leaves the second rule green — the old
literal stops being a member, so the comparison stops being in scope. The first
rule is what fails in that instant; the second is what makes the repair reach
past one file.

All **33** sites were repaired across 8 modules: `ops/app.py`,
`ops/coordination_drain.py`, `ops/coordination_metrics.py`,
`ops/coordination_release.py`, `ops/routers/coordination.py`,
`ops/routers/users.py`, `processing/routers/batch.py`,
`scripts/host_maintenance.py`.

The third rule covers the instance the stage came from, where the owner is a
service rather than a constraint — see §4.4.

---

## 4. The five mutations, watched failing

### 4.1 A migration renames a value

```
$ sed -i "s/'none', 'requested', 'draining', .../'none', 'requested', 'drain_started', .../" \
    db/migrations/V043__coordination_state.sql
E  AssertionError: coordination_state.phase: the migration permits ['active',
   'drain_started', 'none', 'requested', 'validating'] and CoordinationPhase
   declares ['active', 'draining', 'none', 'requested', 'validating']. The
   migration is the owner -- Postgres rejects a write outside it -- so this
   module is what moves.
```

**The retype rule passed during this**, which is the vacuity documented above,
observed rather than argued.

### 4.2 The same rename, half-finished

Updating `CoordinationPhase` to match V043 **still fails**, because
`V044__coordination_state_events.sql` carries the same vocabulary on two more
columns and was not touched:

```
E  AssertionError: coordination_state_events.phase: the migration permits
   ['active', 'draining', ...] and CoordinationPhase declares
   ['active', 'drain_started', ...]
```

Three columns in two migrations share one vocabulary, and the rule holds them
together. This was not designed for; it fell out of keying the corpus by
`(table, column)`.

### 4.3 The rename, completed

With both migrations and the enum renamed, the three rules pass and **no call
site changed** — the diff is two migration files and one enum member. That is
what the second rule bought.

Running the whole suite under the rename: **8 tests failed**, in
`tests/ops/test_coordination_metrics.py`, `tests/ops/routers/test_coordination.py`
and `tests/scripts/test_host_maintenance.py`. The tests hold their own copies.
§5.

### 4.4 The Stage P defect, reintroduced

```
$ sed -i 's/acceptable = {"exported"}/acceptable = {"created"}/' \
    airflow/dags/export_ci_lake_snapshot.py
E  AssertionError: airflow/dags/export_ci_lake_snapshot.py accepts ['created'],
   which archiver/processors/export_ci_lake_snapshot.py never returns. It
   returns ['audited', 'coverage_failed', 'export_failed', 'exported', 'planned'].
```

Neither half is written in the test. The service is derived from the DAG's own
`ARCHIVER_URL` constant and the module from the shared basename.

### 4.5 The reader hole, reintroduced

The single-instance repair read `status=` keyword literals and saw four of the
exporter's five statuses; `coverage_failed` reaches the constructor through a
local (`status=failure_status`, from `_selector_failure_status`) and was
invisible, while
`tests/integration/airflow/test_export_ci_lake_snapshot_dag.py:57` seeded it on
the authority of a docstring. Removing the two lines that resolve the local:

```
E  AssertionError: archiver/processors/export_ci_lake_snapshot.py passes a
   status this reader cannot follow to a literal: ['line 722:
   status=failure_status', 'line 664: status=failure_status']. The emitted set
   it returns is short, and a short set makes the check below fail on a status
   the service really does return.
```

An incomplete emitted set only makes the subset check *stricter*, so this guard
is not load-bearing for correctness. It is there because a reader that quietly
narrows is this stage's own subject wearing the instrument's uniform.

### 4.6 A new constraint with no vocabulary

```
E  AssertionError: 1 CHECK-constrained column(s) with no vocabulary in
   shared/db_vocabularies.py: [('notification_channels', 'delivery')].
```

This is the direction the rejected registry did not have.

---

## 5. Two limits, stated rather than closed

**The tests retype these vocabularies too — 115 comparisons and 366 seeds
across 49 test modules.** The heaviest are
`tests/scripts/oneoff/test_reconcile_april_detail.py` (84),
`tests/ops/routers/test_coordination.py` (38) and
`tests/scripts/test_host_maintenance.py` (31).

**This is duplication, not the both-halves defect, and the repair above is what
made the difference.** CAR-82's instance was a test that supplied the seed *and*
the expectation, so it passed for any string. After the 33 production repairs,
production's half comes from `shared/db_vocabularies.py`: a test seeding
`phase="draining"` supplies one half and the enum supplies the other, and when
they disagree the test **fails** — 8 of them did, in §4.3. That is a rename
cost, not silent drift. Closing the remaining 481 is a stage of its own and is
not pretended to be done here.

**`sensors.py` is found, analysed and not closed.** Its two guards read the
coordination phases positionally out of a query result (`row[0]`, `row[1]`), so
no column name appears in the subject and the retype rule cannot link them.
It cannot import `shared` either — the container mounts only `airflow/dags`,
`airflow/sql` and `airflow/plugins`. The link is derivable: `sensors.py` loads
`airflow/sql/deploy_intent_gate.sql`, whose `SELECT` list maps `row[0]` to
`di.intent` and `row[1]` to `cs.phase`, and that column order is already
asserted against a real Postgres by
`tests/integration/sql/test_airflow_dag_queries.py`. **The loose alternative was
tried and rejected**: checking `airflow/dags/` literals against any CHECK
vocabulary by membership alone links `notifications.py`'s Airflow-owned
`state == "failed"` to `silver_observation_events.event_type`, a false positive
in a population of six. Positional resolution through the `SELECT` list is the
way to close it and is the next stage's, not this one's.

---

## 6. Suite

`pytest tests/ -q -m "not integration"` → **3,794 passed, 694 deselected**,
against a baseline of 3,789 collected. Five tests added, none removed.
`ruff check .` clean.
