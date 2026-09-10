# Plan 162 Stage AG — measurements and findings

The readings behind [Stage AG](../plans/plan_162_testing_census_and_restructure.md#stage-ag-rules-live-in-a-directory-and-an-unregistered-one-cannot-exist),
which moved the rule declaration from a table to a directory and closed
[G30](../TESTING.md#the-gap-list).

**The plan document holds what the numbers mean; this file holds the numbers,
the recipe for each, and the four defects the work surfaced.** Three of those
defects were in the instrument rather than in the repository, which is why they
are recorded at length: each one had been reporting a verdict about something
other than what its description claimed.

The short version: **registering a rule set found that six of the mutations
already proving it had never run a rule**, and the rule written to police
membership shipped with an escape hatch 614 definitions wide.

## The boundary, and the partition it produced

**Recipe.** AST walk of every `def test_*` under `tests/`, keyed per module,
cross-referenced against the test names in `docs/TESTING.md`'s `Asserted by`
column as `_asserted_rule_names()` parses it. Run on this machine against
`4711fb5` before the move.

Top level of `tests/` held **427 definitions in 16 modules**, 72 of them named
in the column and 80 carrying a mutation.

| Module | Defs | Rowed | Mutated | Verdict |
|---|---:|---:|---:|---|
| `test_testing_contract.py` | 71 | 58 | 66 | rule |
| `test_planning_docs.py` | 45 | 2 | 2 | rule |
| `test_image_keep_set.py` | 11 | 0 | 0 | rule |
| `test_maintenance_running_set.py` | 10 | 0 | 0 | rule |
| `test_env_example_wiring.py` | 8 | 8 | 8 | rule |
| `test_declared_skips.py` | 7 | 0 | 0 | rule |
| `test_readme_contract.py` | 5 | 0 | 0 | rule |
| `test_ci_compose_parity.py` | 4 | 4 | 4 | rule |
| `test_observability_config.py` | 132 | 0 | 0 | config |
| `test_lakehouse_compose_config.py` | 46 | 0 | 0 | config |
| `test_deploy_script.py` | 39 | 0 | 0 | config |
| `test_caddy_public_routes.py` | 17 | 0 | 0 | config |
| `test_mlflow_compose_config.py` | 15 | 0 | 0 | config |
| `test_ops_content_mount.py` | 7 | 0 | 0 | config |
| `test_pack_worker_compose_config.py` | 6 | 0 | 0 | config |
| `test_dashboard_base_path.py` | 4 | 0 | 0 | config |

**161 moved, 266 stayed.** The staying set is named artifact by artifact in
`docs/TESTING.md`'s Layer 0 section, because *"the rest"* is not a reason.

**Shape could not have supplied the definition.** Measured the same way: *"a
test that asserts about the repository rather than exercising its code"* scopes
**411 of the 421** definitions at the top level as it stood when the stage was
written — sweeping in 132 observability-config tests and 39 deploy-script tests.

**`test_deploy_script.py` is the boundary's hardest case and it stayed.** Its own
docstring says it asserts *"properties of `docker-compose.yml` and of the shared
exemption file that the script reads at runtime"*, and two config modules import
`load_health_exemptions` from it. It is a shared Layer 0 helper, not a guardrail.
That import now crosses the directory boundary, which is noted in the contract
rather than repaired by relocating the reader away from the script whose runtime
behaviour it describes.

### The seed, and why it is 89 rather than the stage's ~83

The stage estimated `~83`, which was `155 - 72` against counts taken on
2026-09-09 before Stages Z and AA landed. Re-measured at implementation time the
directory holds 161 definitions of which 72 were already rowed:

| | Owed | Drained to |
|---|---:|---:|
| Rows in the `Asserted by` column | **89** | 0 |
| Mutations in the harness | **81** | 0 |

The harness grew from **92 entries to 177**. Eight of the 89 already carried a
mutation — the waiver-hygiene checks, which is exactly the list
`test_every_asserted_rule_names_a_real_test` gave as its reason for declining the
reverse direction.

## Finding 1 — six mutations had never run their rule

**Recipe.** For each `MUTATIONS` entry, resolve its node the way `_pytest` does
(`node if "::" in node else f"{TEST}::{node}"`), then check the name is defined
in that module with its enclosing classes. Then `python -m pytest <node>` and
read the exit code.

Four of Stage Q's entries and two more named a **bare** node for a rule that does
not live in `test_testing_contract.py`:

| Node as written | Rule actually lives in |
|---|---|
| `test_the_ci_override_is_the_whole_difference` | `test_ci_compose_parity.py` |
| `test_ci_runs_productions_flyway_command` | `test_ci_compose_parity.py` |
| `test_no_heavy_job_declares_its_own_services` | `test_ci_compose_parity.py` |
| `test_every_heavy_job_starts_the_compose_services` | `test_ci_compose_parity.py` |
| `test_every_gap_a_stage_claims_exists` | `test_planning_docs.py` |
| `test_the_gap_claim_corpus_is_not_empty` | `test_planning_docs.py` |

A bare node resolves to `TEST::<name>`, which does not exist. Reproduced:

```
$ python -m pytest "tests/rules/test_testing_contract.py::test_the_ci_override_is_the_whole_difference" -q
ERROR: not found: ...::test_the_ci_override_is_the_whole_difference
no tests ran in 0.05s
$ echo $?
4
```

`caught = code != 0` read exit 4 as proof. **All six had reported CAUGHT since
the day each was written**, and Stage Q's record credits one of them with finding
a real defect — that rule's bug was found by running the test directly, not
through the harness.

**The repair is `caught = code == 1`.** pytest exits 1 when a test fails and
something else when it could not run one: 4 for a node it cannot find, 5 for
nothing collected, 2 for a module that would not import. The loop now reports
`NO RUN (<code>)` separately from `MISSED`, so *the rule noticed* and *the
harness never asked it* can no longer be confused. This also closes
mechanically the false CAUGHT Stage AF found by reading output — an
`UNDOCUMENTED` append that landed outside its tuple and exited on a
`SyntaxError`.

**A second, quieter version of the same defect:** 23 further nodes named
`module::name` for tests defined inside classes, which pytest also cannot select.
They are now class-qualified. That is the same blindness Stage Q found in
`test_every_asserted_rule_names_a_real_test`, which read `tree.body` instead of
`ast.walk` and could not see a rule inside a class — arriving in the instrument
rather than in a rule.

## Finding 2 — the SQL rule caught the harness mid-proof

Two of the five new mutations for `test_testing_contract.py`'s own rules were
written with their payloads typed out as SQL:

```python
"SELECT role FROM authorized_users WHERE email_hash = %s"
```

`scripts/` is production Python, so the next baseline run failed on the harness
itself:

```
FAILED tests/rules/test_testing_contract.py::test_no_production_module_holds_a_sql_statement
  Not waived:
    scripts/verify_testing_contract_mutations.py:1926
    scripts/verify_testing_contract_mutations.py:1927
```

The file had anticipated this once already — `_statement()` reads test SQL out of
the file that owns it, and its docstring says *"this harness may not type SQL
either"*. `_production_statement()` is the production half, added beside it.
Reading rather than typing is also the better mutation: what gets filed twice is
the real statement rather than a plausible-looking stand-in.

One consequence worth stating: the duplicate-statement entry uses `_write` rather
than `_edit`, because an `_edit` **anchor** into a `.sql` file is itself a SQL
literal in this module. The anchor rule reads only the path and the anchor, so
the computed replacements stay checkable.

## Finding 3 — the engine exemption admitted 614 definitions to exempt one

`test_every_asserted_rule_lives_in_the_rules_directory` shipped keyed on the
**layer of the directory**: a rule could live outside `tests/rules/` if
`_layer_of` put its directory at Layer 2 or deeper. The reasoning is sound — a
rule needing a database cannot sit at Layer 0 — and the predicate is not its
consequence.

**Recipe.** For every directory holding a `test_*.py`, take `_layer_of` and count
the definitions in those at Layer ≥ 2.

| Layer | Directory | Defs |
|---:|---|---:|
| 2 | `tests/integration/sql` | 246 |
| 4 | `tests/integration/archiver` | 86 |
| 4 | `tests/integration/ops` | 74 |
| 4 | `tests/integration/processing` | 58 |
| 3 | `tests/integration/dbt` | 47 |
| 4 | `tests/integration/scripts` | 35 |
| 4 | `tests/integration/airflow` | 31 |
| 4 | `tests/integration/scraper` | 12 |
| 4 | `tests/integration/container_health` | 8 |
| 4 | `tests/integration/lakehouse` | 7 |
| 4 | `tests/integration/dbt_runner` | 6 |
| 4 | `tests/integration/shared` | 4 |
| | **12 directories** | **614** |

**614 admitted, 1 needed.** A purely static repository-wide rule written into
`tests/integration/ops/test_auth.py` would have been registered, exempt from the
directory requirement, and invisible to the rule built to catch exactly that —
the next Stage Q landing inside the hatch.

**The argument against it was already written, in the tuple that should have been
read.** `ENGINE_BOUND` in the harness:

```python
#: The nodes whose assertion cannot be reached without a live, Flyway-migrated
#: Postgres. **Per node and not per module**, because the module holding this
#: one also holds a test that needs no engine at all and is in ``TESTS`` above:
#: "which layer this lives at" is not a property of the file.
```

Stage AF had settled the grain question and the first version of this rule
ignored it.

**The exemption now reads `ENGINE_BOUND`**, parsed from the harness source the
way `MUTATIONS` already is. No registry is added, because the registry exists
and is load-bearing for another reason: the harness provisions a throwaway
`postgres:16` for its entries, so a name added to it stops being proved and
reports `UNPROVEN HERE`, and a name removed from it is run against an
unmigrated database and fails every `PREPARE`. **Surface: 614 → 1.** An
unreadable or emptied tuple yields an empty set, so the failure direction is
**closed** — the engine-bound rule is reported misplaced rather than every rule
being silently exempt — and a mutation proves that direction.

**The floor moved, and that is the finding rather than a side effect.**
`test_there_is_something_to_check` takes no fixture and needs no database; it was
claiming the engine exemption purely by sharing a module with
`test_every_test_statement_plans_against_the_migrated_schema`, which does. It now
lives in `tests/rules/`, and `_checkable()` became
`checkable_test_statements()` there, read from both layers so the floor and the
planner cannot disagree about what the corpus is. The harness's `TESTS` baseline
collapsed from a module list plus a hand-picked node to the single directory.

## Finding 4 — pytest's rewrite cache was producing wrong verdicts

pytest caches rewritten assertions under a key of **(mtime, size)**. The
mutation for `test_the_rules_directory_is_not_empty` changes
`rglob("test_*.py")` to `rglob("rule_*.py")` — **identical byte length**.
Restored in the same second, the key is unchanged, so the *next* child run
imports the **mutated** bytecode from a tree that is already back to normal.

Observed, in the run immediately after the exemption was tightened:

```
CAUGHT         the rules directory's glob stops matching, ...
*** MISSED *** a rule joins the directory with no row, ...
restored: 2 failed, 182 passed in 21.26s
```

The working tree at that moment was byte-for-byte correct — `git diff --numstat`
reported no content change in any of the files involved. The MISSED entry and
both restore failures were the stale bytecode, not the tree.

**This is the same class as the false CAUGHT and cannot be left to convention**,
because a same-length mutation is a perfectly reasonable thing to write. `_edit`,
`_write` and `_delete` now delete the rewritten bytecode for every file they
touch, and so does the restore in the `finally` block. Both directions matter:
a stale cache can mask the mutation *or* mask the restore.

**It is also why the verification below was run twice.** The failure is
order-dependent, so one clean run is not evidence.

## Three MISSED entries, all three the entry's fault

Each was checked to fail on **its own** assertion rather than merely to fail,
which is Stage AF's method. Three did not, and in every case the rule was right:

| Entry | Why it was MISSED |
|---|---|
| `test_the_four_paused_and_on_demand_images_are_all_in_it` | Dropped `on-demand` from `PROTECTED_CLASSES`, which only `test_every_entry_carries_a_protected_class` reads. `keep_set()` asks whether a container holds the image, so the derivation's answer never moved. Re-anchored on a Compose `profiles:` key. |
| `test_the_solver_and_its_redis_are_both_kept` | Same error: reclassified `redis-trawl` in the manifest where the rule reads `profiles:`. Re-anchored the same way. |
| `test_no_live_heading_is_silently_skipped` | Used `## Notes to self`, and `notes` is one of `IGNORED_HEADING_PREFIXES` — the rule correctly passed. The entry had encoded half its own subject: a heading is unclassified only when the script neither maps **nor ignores** it. Now `## Parked`. |

## Two mutation entries that record a choice rather than a payload

**The staleness rule moves the deadline instead of deleting a recap.** Deleting
the newest recap is the truer mutation on the day it is written and a different
one a week later: the file named in the anchor becomes interior as recaps
accumulate, and the entry would quietly start proving
`test_the_recap_series_has_no_interior_gap`. The anchor rule cannot see that kind
of decay — it checks the file exists, not that the mutation still means what it
says. Moving the boundary is date-independent. It is made unconditional on
`within_grace` deliberately, because a mutation that only fires Monday through
Wednesday reports MISSED on a Thursday.

**The archive-bulk rule truncates via a computed `_write`.** No single anchor can
take 124 archive rows below the live-table count, so the payload keeps the first
35 lines — header plus the five newest rows. Rows are prepended, so that slice
stays stable as the archive grows.

## Verification

**The harness, twice in a row:**

```
engine: provisioning a throwaway postgres:16 on port 55432
baseline: 184 passed in 21.05s
engine: ready, and the engine-bound baseline passes
restored: 184 passed in 21.22s
engine: throwaway database destroyed
177 CAUGHT, 0 MISSED, 0 NO RUN, 0 UNPROVEN — exit 0
```

**Locally:** 3,920 unit tests pass, `ruff` clean.

**Demonstrated rather than asserted**, both directions, then restored:

```
$ # a rule added to tests/rules/ with no row
AssertionError: these tests live under tests/rules and are named in no row of
docs/TESTING.md's 'Asserted by' column, so nothing obliges them to have been
watched fail:
    tests/rules/test_demo_unregistered.py::test_a_rule_that_joined_no_row

$ # a registered rule living outside the directory
AssertionError: docs/TESTING.md names these as asserting a rule, and they do not
live under tests/rules:
    test_the_first_entry_line_is_service_then_reason (tests/test_deploy_script.py)
```

**Observed in CI** on run
[`34418730747`](https://github.com/whitewalls86/new_car_tracker/actions/runs/34418730747)
for [#407](https://github.com/whitewalls86/new_car_tracker/pull/407) — all
fourteen jobs green, `Documentation tests` scope-skipped. `Unit tests (pytest)`
reported **3,919 passed, 1 skipped, 714 deselected**, with each new rule named in
the log:

```
tests/rules/test_testing_contract.py::test_there_is_something_to_check PASSED
tests/rules/test_testing_contract.py::test_the_rules_directory_is_not_empty PASSED
tests/rules/test_testing_contract.py::test_every_test_in_the_rules_directory_is_named_in_the_contract PASSED
tests/rules/test_testing_contract.py::test_every_asserted_rule_lives_in_the_rules_directory PASSED
...
================================ Declared skips ================================
1 declared skip(s) accepted:
```

**That last line is the one thing only CI could prove.** The declared-skips
registry key was re-pointed to
`tests/rules/test_planning_docs.py::TestWeeklyRecaps::test_every_sha_a_recap_names_is_a_real_commit`,
and the hook compares it against the nodeid pytest reports at runtime — a string
comparison no static rule checks, and one the gate cannot exercise locally
because it is off by default on a full clone (where that test does not skip).
`SQL execution coverage` passing is the matching live check for
`check_sql_execution_coverage.py`'s re-pointed import.

## What is not covered

**`Documentation tests` scope-skips on a changeset this wide**, so the
`pytest --noconftest tests/rules/test_planning_docs.py` step this stage
re-pointed did not run in CI. Run locally with the same flags and `PYTHONPATH`:
54 passed. The first docs-only changeset after this merges is where it is
exercised for real.

**Two test names collide across the new boundary.**
`test_every_entry_carries_a_reason` and `test_every_entry_names_a_real_service`
are defined in both `tests/rules/test_maintenance_running_set.py` and
`tests/test_deploy_script.py`. The mutation nodes are class-qualified and
unambiguous, but the column reader matches bare names, so deleting a
rules-directory copy would leave its row satisfied by the namesake. That is the
weakness `test_every_asserted_rule_names_a_real_test`'s own comment already
admits to — *"a name could be satisfied by a same-named test in another module"* —
and renaming a module out of scope to dodge it was declined.

**The residual the directory cannot close.** A rule that is neither registered
nor in `tests/rules/` is reachable by no mechanism here. The directory speaks
only for what joined it, and shape was measured and rejected above. That is
written into the rule's docstring and into G30 rather than left implied.
