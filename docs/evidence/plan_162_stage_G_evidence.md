# Plan 162 Stage G — separating production scripts from spent ones

**Legacy:** Stage 5b · **Issue:** CAR-55 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage G. It carries the summary; the sections below are the detail.

---

#### The classification needed a third step the design did not name

The stage's design section above says the bucket falls out of a join —
docstring plan number against `completed_plans.md`, overridden by the
binding-reference grep. That is two of the three steps actually required.

**Nine Python scripts declare no plan, not five.** Four beyond the design's
list — `diff_log_analysis.py`, `estimate_recompression_savings.py`,
`recompress_bronze_html.py` and `rewrite_parquet_layout.py` — so the residual
the design called "four files to read" was really nine.

**What closed the gap was a reverse join: the script's own name, grepped back
through the archive and every plan document.** It settled six of the nine with
no judgement at all. `estimate_recompression_savings.py` is the clearest case —
Plan 116's archive row names the script outright, so the archive already
contained the answer, read from the other end. Only three files
(`backfill_unlisted_silver.py`, `diff_semantic_duplicate_html.py`,
`audit_normalized_parquet_layout_once.py`) were genuinely read by hand.

**The reverse join belongs in the method, not only in this section.** A script
that declares its plan is the easy case; a script that does not is exactly the
one whose classification a future reader will have to reconstruct, and the
archive is where the answer already lives.

#### Three claims in the design section above were wrong

Recorded rather than quietly fixed, because the stage's own thesis is that an
unasserted claim goes false without anyone noticing.

**"No deploy surface is edited" is wrong by one.** `docker-compose.yml`
defines a profile-gated `april-processor` service whose documented invocation
is `python -m scripts.reconcile_april_detail`. One comment line was edited. The
20-reference count across 11 surfaces also missed four:
`docker-compose.lakehouse.a3.yml`, `maintenance-running-set.txt`,
`healthcheck-exemptions.txt`, and `.claude/settings.json`, which is what binds
`public_surface_gate.py` through a `PreToolUse` hook.

**There are 39 Python scripts, not 35.**

**`scripts/ops/` found no members and was not created.** The maintenance
bucket's rule was "human-invoked, durable, named in a live runbook", and
`host_maintenance.py` is the only script that satisfies it — but
`ops/routers/coordination.py:14` does `from scripts.host_maintenance import
HOST_VALIDATION_GATES` at module import, so the binding-reference override
keeps it in production. Shipped as a two-bucket split. The third bucket is not
deferred; it was dissolved by the same kind of override that dissolved the
coupling finding, and a future script that is durable, human-invoked and
imported by nothing can revive it.

#### The coupling finding ran the other way a second time

The design section records a coupling worry that the archive join dissolved:
two production scripts importing scripts that *looked* spent, where the owning
plans turned out to be open. The inverse case is the one it did not anticipate.

`lake_snapshot_common.py` and `seed_lake_snapshot.py` both belong to Plan 120,
which **is** archived, and neither is invoked by a workflow step. The stated
rule — archived owning plan, no binding reference — puts both in `oneoff/`.
They are imported by `download_lake_snapshot.py`, which an ops route documents,
and by `preflight_local_lakehouse_snapshot.py`, which a Compose file names. So
**a full import walk, not the reference grep alone, is what keeps them in
production**; the rule as written would have moved them and broken two
production imports. Nothing in the shipped split crosses a bucket boundary,
verified in both directions across all 39 scripts.

#### Three repairs found on the way

**`verify_testing_contract_mutations.py` could not run at all.** Stage F closed
G4 and deleted its row, and reworded G14 from "54 of 76" to "56 of 76", leaving
three mutations anchored on text that no longer existed. The script aborted on
its own staleness guard before reaching them — **the identical failure this
plan already records against Stage B**, where Stage B's deletions stranded
mutations on the removed G1 and G2 rows. Re-anchored to G6 and G14 as written.
The guard worked twice; what has not been established is a habit of running the
verifier after deleting a gap row.

**Two moved test files anchored their paths by counting parents.**
`test_audit_sectioned_html_storage.py` computed a fixture directory as
`__file__.parent.parent` and broke outright, twelve failures.
`test_estimate_recompression_savings.py` was the worse of the two: its
subprocess cases pass `cwd=Path(__file__).parents[2]` and assert only
`returncode != 0`, so "the script is not where I looked" and "argparse
rejected the flags" are the same result — a wrong `cwd` would have left both
cases **passing for the wrong reason**, invisibly. Both now resolve `tests/` by
name.

**One markdown link, and the line it drew.** Historical documents were
deliberately not rewritten: a prose mention of `scripts/x.py` inside an
archived plan records where the file sat when that plan ran, and editing it
manufactures history. A markdown *link* is a different object — it is a promise
that the target resolves, and `test_no_markdown_link_in_docs_is_dangling`
asserts it. One link in `plan_147_scrape_state_ownership.md` was repointed and
the prose around it left alone. Code, config and the two Plan 145 runbooks were
rewritten because they have to execute.

#### What the contract gained

Two assertions, both proven able to fail by new entries in the mutation
verifier, which now catches 23 of 23:

- `test_every_script_directory_is_classified` — both directions: a script
  directory the contract places nowhere, and a bucket the contract describes
  that does not exist.
- `test_every_unmeasured_script_bucket_is_omitted_from_coverage` — the prose
  and `[tool.coverage.run] omit` are one statement. The dangerous direction is
  the second: a bucket coverage omits while the contract calls it measured is
  code that silently stopped being graded.

`docs/TESTING.md` gained a *Where scripts sit* section and one row in *Where
the newer suites sit*. One row rather than two, because one new test directory
exists. `test_every_test_directory_is_assigned_a_layer` would in fact have been
satisfied without it — `_layer_of` inherits from the nearest declared ancestor,
so `tests/scripts/oneoff/` reads as Layer 1 through `tests/scripts/` — and the
row was added anyway, because a bucket that exists to be classified should say
its own layer rather than inherit one.

#### The CI zones compose, which was a second pass

`ci_change_scope.py` first shipped here with two mutually exclusive scopes,
`docs_only` and `oneoff_only`, and a changeset spanning both fell through to
the full workflow — a spent script edited alongside the note explaining why,
which is an ordinary shape of change in this repository and the one the
classifier helped with least.

It now maps a changeset to the job groups it needs, and the zones compose:

| changeset | `docs_tests` | `unit` | `heavy` |
|---|---|---|---|
| `docs/` only | yes | -- | -- |
| `scripts/oneoff/` only | -- | yes | -- |
| both | -- | yes | -- |
| anything unclassified | -- | yes | yes |
| empty, malformed, or no base sha | -- | yes | yes |

`docs_tests` fires only when the unit suite is not running, because
`pytest tests/` already contains `test_planning_docs.py` — the documentation
job is a substitute for the unit run, never an addition to it. Moving the
decision into the classifier also took the double negative out of six job
conditions, which now read `needs.changes.outputs.heavy == 'true'`.

**Fail-open is stated once.** The workflow's shell defaults are `unit=true
heavy=true`, and every path that fails to classify leaves them standing.
`test_paths_in_neither_zone_can_never_narrow_the_run` asserts that as a
property over every mixture of zones, comparing against the whole decision row
rather than the `heavy` flag alone.

The question this raised and declined — classifying the incremental diff rather
than the cumulative one — is scoped for Stage P
[above](../plans/plan_162_testing_census_and_restructure.md#stage-p-inherits-a-question-stage-g-raised-and-declined).

#### What was deliberately not done

- **`tests/integration/scripts/` was not split**, though all three of its files
  are Plan 145's. `test_every_integration_suite_is_invoked_by_a_ci_step`
  derives the invoked set from the literal path in each `ci.yml` step, so a new
  subdirectory would have needed its own step or a `DORMANT_SUITES` entry —
  cost for no coverage benefit, since the integration jobs are not what the
  ratchet measures.
- **`verify_testing_contract_mutations.py` stayed at `scripts/`** although
  Plan 161 has archived. Plan 162 edits it, and this stage edited it twice. The
  rule "the owning plan archived" is beaten by "an open plan maintains it",
  which is the one place the archive join needed a human answer rather than a
  better query.
- **`docs/PLANS.md` was moved through the `plans` skill**, not edited here.
  Row 1 keeps its build-order position; only its slice pointer advanced.
