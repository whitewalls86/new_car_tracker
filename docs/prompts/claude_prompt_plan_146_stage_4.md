# Plan 146 Stage 4 — make the invariant a test

Read `docs/plans/plan_146_planning_system.md` first — it is the source of
truth, and its **Stage 4** section is the specification. Then read
`docs/PLANS.md`, whose header states the rules this test enforces, and the
Stage 3 entry at the end of `docs/planning/plans_decision_log.md`, which records
the decisions this stage inherits.

Work continues on branch **`plan-146-planning-system`**. Stages 0-3 are
committed there. Do not branch again; merge to `master` when this stage passes.

## What already happened, so you do not redo it

| Stage | Commit | What landed |
|---|---|---|
| 0 | `9fb5408` | `scripts/audit_plan_state_history.py` and `docs/planning/plan_state_reconciliation.md` — every plan's true state, settled against its document and git |
| 1 | `353336a` | 25 backfilled archive rows; the archive went 83 → 108 rows, newest-first, with `*(observed)*` / `*(corroborated)*` / `*(inferred)*` provenance labels |
| 2 | `0c08382` | `docs/PLANS.md` rewritten: 232 → 169 lines, five tables, every row carrying its own exit condition |
| 3a | `f5416dd` | `docs/` became five directories — 98 files, all recorded as renames, no content edits |
| 3b | `b494798` | 163 relative links inside `docs/` and 88 path references outside it rewritten; `audit_plan_state_history.py` updated |

The layout Stage 3 built:

```
docs/
  ARCHITECTURE.md
  PLANS.md              index -- stays at root
  plans/                79 plan documents, flat, one stable path forever
  planning/             completed_plans.md, plans_decision_log.md,
                        plan_state_reconciliation.md
  runbooks/             3
  prompts/              9, including this one
  reference/            case study, code review, health report, debug bundle, svg
```

**Directories encode kind, not state.** Nothing in this test may assume
otherwise — no path pattern that changes when a plan completes.

## The task

Write `tests/test_planning_docs.py`. It asserts, per the plan document:

1. every plan document appears in **exactly one** of the five tables —
   closeout, build order, backlog, superseded in `PLANS.md`; archive in
   `planning/completed_plans.md`
2. no plan number appears in two
3. every closeout row has a `Lands` date that parses, and a gate
4. every backlog row has a trigger
5. every build-order row's `Blocked by` names a real plan or a date
6. `PLANS.md` is under its stated 250-line budget
7. no markdown link anywhere in `docs/` is dangling

**All seven hold right now.** This stage does not fix data; it freezes a
structure that is already correct. If an assertion fails on the first run, the
test is wrong before the docs are — check it against the measurements below.

The shape to copy is `TestServiceHealthCoverage` in
`tests/test_observability_config.py:461`: coverage asserted, not enumerated.
Its docstring explains why, and the argument is identical here. **No deny-list
of plan numbers.** A test you can silence by appending to a list is the defect
it was meant to catch.

Derive the paths from **one repo-root-relative constant block** at module
scope, the way `scripts/audit_plan_state_history.py:54-59` now does
(`INDEX`, `PLANS_DIR`, `ARCHIVE`). Stage 6 will move something else eventually;
it should have one place to edit.

## The five sharp edges, measured

Every one of these will break a reasonable first draft. They are facts about
the data, not suggestions.

### 1. The archive's `Plan` column is often not an integer

109 table lines, 108 rows, and **23 of them do not start with a plain number**:

```
| V029 | ...                  a schema-version row, not a plan
| Silver flush | ...          a named workstream
| 62 + 63 | ...              two plans, one row
| 60+75 |, | 54+58 | ...     ditto, no spaces
| 14.1 |, | 14.5 |, | 25.4 | sub-plans of a parent
```

`int(cell)` raises on all of them. Decide deliberately: parse `62 + 63` as
both 62 and 63 (it is genuinely both), treat `14.1` as a sub-plan of 14, and
skip the non-numeric ones. Whatever you choose, a row that does not parse must
**fail loudly** rather than be silently dropped — silent dropping is how the
old "Plan inventory" covered 30 of 72 files and said nothing about the other 42.

### 2. A plan legitimately occupies several build-order rows

Plan 139 holds rows 14 and 16 — **Stage C** and **Stage D**, distinct
executable slices with different `Blocked by` values. Assertion 2 means *no
plan number in two of the five tables*. It does not mean one row per plan; the
build order is staged work and that is the design.

### 3. Plan number and plan document are not one-to-one

78 of the 79 documents carry a number, and they resolve to **73 distinct plan
numbers**:

| Number | Documents |
|---|---|
| 110 | `implementation_plan_110_storage_layout_hygiene.md`, `plan_110_html_storage_optimization.md` |
| 120 | `implementation_plan_120_ci_lake_snapshot_delivery.md`, `plan_120_ci_lake_snapshot_delivery.md` |
| 123 | `plan_123_dbt_incrementalization_and_resource_governance.md`, `plan_123_dbt_resource_baseline.md` |
| 125 | `plan_125_catalog_decision_report.md`, `plan_125_duckdb_to_iceberg_migration.md`, `plan_125_portability_audit.md` |

And `plan_v018_schema_migration.md` has no number at all. So assertion 1 keys
on the **plan number a document declares**, not on the filename — several
documents can share one row. Say that in a docstring; the next reader will
assume one-to-one otherwise.

The converse does not hold either and must not be asserted: **61 table rows
have no document**, nearly all of them archive rows for plans finished before
plan documents existed, plus the six whose documents were deleted (Stage 0
found them; `--deleted` still lists them).

### 4. The dangling-link check must follow links, not filenames

This is the distinction that decides whether the test survives. Documents that
plans *propose* and nobody has written — `docs/governance_inventory.md`,
`docs/table_registration_standard.md`, `docs/dbt_spark_adapter_decision.md`,
`docs/staging_environment_decision.md`, `docs/runbook_lakehouse.md` — appear as
**backticked filenames in deliverables lists**, never as markdown links. They
are descriptions of future work. A test that flags them is a test somebody
disables inside a week.

Match markdown link syntax only, skip `http`/`mailto`/anchors, resolve relative
to the containing file's directory, and strip `#fragments` before the
existence check. Both a naive and a careful scan are clean today, so any hit is
a real regression.

Two traps that are one careless commit away:

- fenced code blocks in `docs/prompts/` contain regexes that include link
  punctuation. They are escaped today, so nothing matches. Consider skipping
  fenced blocks and code spans anyway.
- `plans_decision_log.md:630` had to be reworded during Stage 3 because it
  quoted link syntax inside backticks and tripped the checker. If you skip code
  spans, that reword becomes unnecessary — but leave it, it reads better.

### 5. Three references are broken by decision, and two of them are outside `docs/`

Stage 3 decided each one and wrote it down in the decision log. Do not
re-litigate, and do not let the test flag them:

- `.env.example` cites `docs/runbook_lakehouse.md`, which nobody has written.
  Left alone deliberately: no plan promises it, and inventing the file to
  satisfy a grep is worse than the stale pointer.
- `plan_105_vm_migration.md`'s two links — to a `provision_oracle_vm.py` no
  commit on any ref has ever contained, and to a `.claude/` memory file outside
  the repo on a machine that no longer exists — are now **backticked prose**,
  precisely so this test does not see them.
- `docs/prompts/claude_prompt_plan_146_stage_3.md` deliberately keeps flat
  `docs/plan_*.md` paths in prose. It describes the layout as it stood before
  the move. Prose, not links; the test should not care, but do not "fix" it.

## Measurements to check your parser against

If your numbers differ, your parser is wrong — none of this data changed in
Stage 3.

| Thing | Value |
|---|---|
| `PLANS.md` | 169 lines of a 250 budget |
| Closeout rows | 3 — Plans 135, 129, 123; lands 2026-08-23, 2026-09-01, 2026-09-01; all three have gates |
| Build-order rows | 18, of which 9 have `Blocked by` `--`, 2 name a date, 7 name a plan |
| Backlog rows | 14, all with triggers |
| Superseded rows | 14, all with a `Superseded by` |
| Archive rows | 108, of which 23 have a non-integer `Plan` cell |
| Plan documents | 79 files, 78 numbered, 73 distinct numbers |
| Documents in no table | **none** |
| Numbers in two tables | **none** (139 twice in the build order is one table) |

`Blocked by` `--` is not a violation. It means nothing blocks the row, which is
most of the build order.

## Column headers are frozen

Stage 2 wrote them and Stage 4 reads them. Do not rename a column to make
parsing easier — the test exists to constrain the documents, and a test that
edits its own input to pass is worthless.

```
closeout     | Plan | Lands | Gate — what removes this row |
build order  | Order | Plan | Title | Next executable slice | Workable? | Blocked by | Priority | Effort | Depends on / safe stopping point |
backlog      | Plan | Title | Priority | Effort | Trigger |
superseded   | Plan | Title | Superseded by |
archive      | Plan | Description | Date |
```

Note the em dash in the closeout header and the question mark in `Workable?`.

## Do not repeat the performance mistake next door

`tests/test_observability_config.py` re-parses `docker-compose.yml` **81 times
in one run** — 3.28s of a 3.41s suite, because helpers read from disk on every
call and one test does it inside a comprehension. That file is 37 KB.
`PLANS.md` plus the archive plus 79 documents is a comparable amount of I/O,
and assertion 7 walks every markdown file in `docs/`.

Read each file once, at module scope or behind `@lru_cache`. This test should
add well under a second.

## Verification before you hand back

```bash
# the new test, alone and verbose
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -v

# it must be fast
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py --durations=10

# full suite -- 2380 passed, 401 deselected, ~12s before this stage
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest -q -m "not integration"

# the sweep tool still agrees: 3 never-used (44, 85, 104), 0 unrecorded
python scripts/audit_plan_state_history.py --coverage
```

Then prove the test can fail, and say so in the handoff. A structural
assertion nobody has watched fail is a structural assertion that does not work
yet. Break each of the seven deliberately in the working tree, confirm the
message names the offending plan or file, and revert.

## Scope

Writing the test, and only whatever `PLANS.md` or archive edits the test proves
necessary. **Do not** edit plan document content, re-order the build order, or
resolve the plan-number collision between `plan_82_user_management.md` and the
`plan_82_self_hosted_runner.md` that lives only on `origin/fix/import-errors` —
this test reads the working tree, so that collision is out of scope by
construction.

Stages 5 and 6 are skills that automate the edits this test now constrains.
They are deliberately later: automating a structure still under argument
encodes the argument.
