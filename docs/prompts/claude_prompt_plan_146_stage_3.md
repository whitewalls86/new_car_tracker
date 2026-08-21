# Plan 146 Stage 3 — give `docs/` a hierarchy

Read `docs/plan_146_planning_system.md` first — it is the source of truth, and
its **Stage 3** section specifies the layout. Then read `docs/PLANS.md`, which
Stage 2 rewrote and whose header states the rules this stage must not break.

Work continues on branch **`plan-146-planning-system`**. Stages 0, 1 and 2 are
committed there. Do not branch again; do not merge to `master` until Stage 4
passes.

## What already happened, so you do not redo it

| Stage | Commit | What landed |
|---|---|---|
| 0 | `9fb5408` | `scripts/audit_plan_state_history.py` and `docs/plan_state_reconciliation.md` — every plan's true state, settled against its document and git |
| 1 | *(second commit)* | 25 backfilled archive rows; `docs/completed_plans.md` went 83 → 108 rows, newest-first, with `*(observed)*` / `*(corroborated)*` / `*(inferred)*` provenance labels |
| 2 | *(third commit)* | `docs/PLANS.md` rewritten: 232 → 169 lines, five tables, every row carrying its own exit condition. Watch list, plan inventory, paused-or-blocked and the duplicate Completed table are gone |

**Stage 0's findings are load-bearing and surprising**, so skim
`plan_state_reconciliation.md` before touching anything: 24 plan documents were
in no status table, 9 plans had no document *and* no archive row, and Plan 65 —
the auth stack that is the reason the site has authentication — shipped at
`eb96c41` and vanished from the record for four months when its document was
deleted the next day.

## The task

`docs/` is 98 files and zero directories. Give it one, exactly as the plan
specifies:

```
docs/
  ARCHITECTURE.md
  PLANS.md              index
  plans/                79 plan documents, flat
  planning/             completed_plans.md, plans_decision_log.md,
                        plan_state_reconciliation.md
  runbooks/             3 files
  prompts/              7 claude_prompt_* files, including this one
  reference/            LINKEDIN_CASE_STUDY.md, CODE_REVIEW_2026-03-20.md,
                        pipeline_health_report_2026-03-27.md, DEBUG_BUNDLE.md,
                        cover-image.svg
```

Counts are measured, not estimated: 79 files match
`(implementation_)?plan_[0-9]+_*` or `plan_v018_*`; 7 match `claude_prompt_*`;
3 match `runbook_*`.

**`ARCHITECTURE.md` and `PLANS.md` stay at `docs/` root.** They are the two
entry points and every external reference to them should keep working.

### The rule this stage exists to demonstrate

**Directories encode *kind*, not *state*.** Do not create `plans/active/` and
`plans/completed/`, however tempting. State changes; a completion would then
move a file, break every inbound link, and put the same fact in two places —
the path and the index — which is the exact defect Plan 146 exists to remove.
A plan document keeps one stable path forever. `PLANS.md` and the archive say
what state it is in.

This is rule 5 from the plan document again: key on something that does not
move.

## Use `git mv`

History matters here — Stage 1's entire method was reading it, and Stage 6 will
read it again. `git mv` preserves it; delete-and-recreate does not.

## The link surface, measured

This is the expensive half of the stage. Do not start renaming until you have
read this list.

**Inside `docs/`:** 238 relative markdown links across 70 distinct targets.
Every one of them currently assumes a flat directory. A link from
`plans/plan_136_*.md` to `plan_140_*.md` still resolves after the move (same
directory); a link from `plans/plan_136_*.md` to `PLANS.md` or
`completed_plans.md` does not.

**Outside `docs/`:** roughly 40 files reference `docs/plan_*.md` by path, in
places a markdown link checker will never look:

- `docker-compose.yml` and all five `docker-compose.lakehouse*.yml` — comments
- `grafana/provisioning/alerting/rules.yml` — **inside alert `description`
  fields**, which reach a human during an incident
- `grafana/dashboards/infrastructure.json` — inside a panel description
- `healthcheck-exemptions.txt`, `deploy-followers.txt`, `.env.example`
- `tests/test_deploy_script.py` — inside assertion messages
- `container_health/docker_api.py`, `shared/iceberg_catalog.py`,
  `shared/mlflow_provenance.py`, `archiver/processors/lake_snapshot_export.py`
- eight `scripts/*.py` docstrings, four `dbt/` model and macro comments

Find them with:

```bash
grep -rn 'docs/[A-Za-z0-9_.-]*\.md' --exclude-dir=.git --exclude-dir=__pycache__ \
  --exclude-dir=target .
```

An alert description that points at a moved file is worse than one that points
nowhere, because it is read under time pressure.

## Three things that are already broken — do not report them as regressions

Stage 4's dangling-link test will find these. Decide what to do with each and
write the decision down; do not silently "fix" them by inventing files.

1. `docs/plan_123_dbt_resource_baseline.md` links to
   `../../../.claude/projects/c--Users-mille-PycharmProjects-cartracker-scraper/memory/reference_server_ssh.md`
   — a path outside the repo, on a machine that no longer exists. **The only
   genuinely dangling relative link in `docs/` today.**
2. `docker-compose.yml:175` cites `docs/plan_128_challenge_pages.md`. The file
   is `plan_128_false_block_detection.md`.
3. Several plan documents cite files that were *proposed* and never written —
   `docs/governance_inventory.md`, `docs/table_registration_standard.md`,
   `docs/dbt_spark_adapter_decision.md`, `docs/staging_environment_decision.md`,
   `docs/runbook_lakehouse.md`. These are **deliverables described in a plan**,
   not broken links. Stage 4's test must not flag them, which means the test
   needs to distinguish "a markdown link that resolves nowhere" from "a
   backticked filename in prose". Keep that distinction in mind while moving
   things; it is the difference between a useful test and one that gets
   disabled.

## Order of work

1. `git mv` into the new directories. One commit, mechanical, no content edits.
2. Rewrite links. Second commit. Inside `docs/` first, then everything outside.
3. Verify, then a third commit only if something needed fixing.

Doing 1 and 2 as separate commits means the review can read the rename without
the 238-link diff on top of it.

## Verification before you hand back

```bash
# no plan document left at docs/ root
ls docs/*.md            # expect exactly ARCHITECTURE.md and PLANS.md

# every relative markdown link in docs/ resolves
python - <<'PY'
import pathlib, re
bad = []
for f in pathlib.Path('docs').rglob('*.md'):
    for m in re.finditer(r'\]\((?!https?:|#)([^)#]+)', f.read_text(encoding='utf-8')):
        if not (f.parent / m.group(1)).exists():
            bad.append(f"{f}: {m.group(1)}")
print('\n'.join(bad) or 'all links resolve')
PY

# nothing outside docs/ still points at a flat docs/plan_*.md
grep -rn 'docs/plan_[0-9]' --exclude-dir=.git --exclude-dir=__pycache__ \
  --exclude-dir=target . | grep -v 'docs/plans/'

# the sweep tool still works -- it hardcodes docs/PLANS.md and
# docs/completed_plans.md, both of which move or stay by design
python scripts/audit_plan_state_history.py --coverage
```

That last one matters: `scripts/audit_plan_state_history.py` has `INDEX =
"docs/PLANS.md"` at module scope and reads `docs/completed_plans.md` in two
functions. `PLANS.md` stays put; `completed_plans.md` moves to
`docs/planning/`. **Update the tool, and re-run all three of its modes** —
`--coverage` should still report 4 unrecorded (5, 52, 55, 56) and 3 never-used
(44, 85, 104). Any other number means the move broke the parser.

## What Stage 4 needs from you

Stage 4 writes `tests/test_planning_docs.py`, which asserts:

- every plan document appears in **exactly one** of the five tables
  (closeout, build order, backlog, superseded in `PLANS.md`; archive in
  `completed_plans.md`)
- no plan number appears in two
- every closeout row has a `Lands` date that parses, and a gate
- every backlog row has a trigger
- every build-order row's `Blocked by` names a real plan or a date
- `PLANS.md` is under its stated 250-line budget
- no markdown link anywhere in `docs/` is dangling

So: leave the tables' column headers exactly as Stage 2 wrote them, and make
sure the new paths are derivable from a repo-root-relative constant rather than
scattered through the test.

## Scope

Moving files and fixing references. **Do not** edit plan document content, do
not re-order the build order, do not resolve the plan-number collision between
`plan_82_user_management.md` and the `plan_82_self_hosted_runner.md` that lives
only on `origin/fix/import-errors` — Stage 4's test reads the working tree, so
that collision is out of scope by construction.
