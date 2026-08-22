# Plan 146 Stage 6 — a skill for the weekly recap

**This prompt is the entry point. It carries the measurements; the files below
carry the design.** Nothing here restates them, so read them — but read only
what is named. Loading `tests/test_planning_docs.py` and
`scripts/audit_plan_state_history.py` whole costs 15k tokens against the ~19k
the actual week's analysis needs, which is how a session spends more on
ceremony than on work.

| Read | Why |
|---|---|
| `docs/plans/plan_146_planning_system.md` **lines 273-312** | Stage 6's specification. The authority — if it and this prompt disagree, it wins |
| `docs/planning/plans_decision_log.md` **lines 711-909** | Stage 5's prose/state boundary, and why recaps got their own directory |
| `.claude/skills/plans/SKILL.md` | Stage 5's skill, whole. The format to mirror and the boundary to invert |
| `docs/PLANS.md` **lines 1-34** | The header rules a recap reads. The tables below them are derived, not read |
| `tests/test_planning_docs.py` **lines 1-31 and 521-569** | The module's argument, and the link check that will cover `docs/recaps/` |
| `scripts/audit_plan_state_history.py` **lines 110-181** | `revisions()`, `plans_in_section()`, `state_map()` — reuse these |

Work continues on branch **`plan-146-planning-system`**. Stages 0-5 are
committed there, the most recent being `ae0f2c5`. **Commit Stage 6's work on
that branch**, the way every stage before it did. Do not branch again, and do
not merge or otherwise integrate the branch anywhere — that is the maintainer's
decision, and no document, this one included, can make it for them. Leave the
branch checked out where you found it when you hand back.

## What already happened, so you do not redo it

| Stage | Commit | What landed |
|---|---|---|
| 0 | `9fb5408` | `scripts/audit_plan_state_history.py` and `docs/planning/plan_state_reconciliation.md` — every plan's true state, settled against its document and git |
| 1 | `353336a` | 25 backfilled archive rows; the archive went 83 → 108 rows, newest-first, with `*(observed)*` / `*(corroborated)*` / `*(inferred)*` provenance labels |
| 2 | `0c08382` | `docs/PLANS.md` rewritten: 232 → 169 lines, five tables, every row carrying its own exit condition |
| 3 | `f5416dd`, `b494798` | `docs/` became five directories; 251 references rewritten; directories encode kind, not state |
| 4 | `9ed8f30` | `tests/test_planning_docs.py` — 18 assertions, 0.11s, no deny-list |
| 5 | `937dd43` | `.claude/skills/plans/SKILL.md`; the test grew to 27 assertions; six of seven known mutations closed, the seventh recorded |

## The task

A `plan-week` skill that reads a window of commits, maps each to the plan it
belongs to, opens those plan documents for the *why*, and writes a recap.
Commits say what changed; plan documents say what it was for. A recap from
either alone is a changelog or a wish list.

**It writes prose and never state.** Stage 5's skill moves rows and never
composes a sentence; this one composes sentences and never moves a row. A tool
that does both can move a row because its own summary said so.

Output shape, window, and what a recap must contain are specified in
`plan_146` lines 273-312 — `docs/recaps/`, one file per week, Monday to Sunday,
last complete week only. The reasoning and the two hazards that shape creates
are in the decision log at lines 863-909. **Read both rather than trusting this
paragraph.** What follows is only what neither of them knows.

## The sharp edge that decides this stage

**Read the history. Do not fingerprint it.**

The plan calls attribution *"the whole difficulty"* and prescribes a layered
string heuristic: *"the same layered attribution `audit_plan_state_history.py`
uses — subject, then branch name, then the plan documents touched by the
diff."* Every part of that is wrong, and measurement says so.

**The script has no layered attributor to reuse.** `mentioned_numbers()` reads
subjects only. `ever_mentioned()` reads subjects, bodies, index history and the
contents of `docs/` — but it answers *"was this plan number ever real?"*, a
different question from *"what happened this week?"*.

**The diff layer contributes nothing.** Over the last 30 days, 170 commits:

| Layer | Attributes | Running total |
|---|---:|---:|
| commit subject names a plan | 146 | 146 |
| commit **body** names a plan | +6 | 152 |
| containing branch names a plan | +18 | 170 |
| plan documents touched by the diff | **+0** | 170 |

Zero. Work commits touch code, not plan documents — `2cfdb73` changed 24 files
and not one was under `docs/plans/`.

**And the branch layer evaporates.** All 18 commits it rescues are also on
`master`; `git branch --contains` finds them only because their plan-named
branches still exist among this repo's 76 refs. Delete those branches — which
merging is supposed to do — and the layer returns nothing. The same window
recapped today yields 0 unattributed and in three months yields 18.

### So read the commits instead, and here is what that costs

Measured on the real window, 2026-08-10 to 08-16, 42 non-merge commits:

| What you read | Tokens | Verdict |
|---|---:|---|
| subjects only | **~0.9k** | what the heuristic sees. Too thin to write from |
| subjects + `--stat` | ~3.5k | adds every file touched and its +/- |
| subjects + **bodies** | ~15.5k | adds the author's own rationale |
| **subjects + bodies + `--stat`** | **~19k** | **this is the tier to build on** |
| full patches | ~262k | 300x, and ~106k of it is markdown |
| full patches, code only | ~162k | still does not fit a busy week |

**~19k for a week is nothing, and it is genuinely reading the history rather
than pattern-matching metadata.** It carries the author's stated rationale and
the shape of every change, and unlike a branch name it does not evaporate. Full
patches are ~262k for a quiet week and roughly double for a busy one — and
106k of that is markdown, which is this plan's own documents being re-read at
enormous cost.

### The reframe that follows

**Attribution is for linking to the *why*, not for knowing what happened.** A
plan number is not a prerequisite for recapping a commit. `fix(tests): read
counters outside the builtins.open patch` needs none — its subject and `--stat`
say what it did. A plan number buys one thing: the right to open
`docs/plans/plan_NNN_*.md` and say why the work mattered.

- **Read subjects, bodies and `--stat` for the whole window.** Durable, and it
  does not care whether a branch still exists.
- **Attribute where the text says so.** 86% of subjects do it unaided. Take a
  branch name as a *hint*, never a dependency, and never treat its absence as
  failure.
- **Escalate to a full diff only for a commit you genuinely cannot read**, and
  treat it as a cost: the two subject-unattributed commits in this window are
  **~15.5k tokens** between them, so "just pull the diff for the unclear ones"
  is not automatically cheap.
- **A commit with no plan is recapped anyway**, under what it did, and listed
  as unattributed because the link to a *why* is missing — not the work.

Re-derive any of it:

```bash
git log --all --since='2026-08-10 00:00:00' --until='2026-08-16 23:59:59' \
        --no-merges --pretty='%h %ad %s%n%b' --date=short --stat | wc -c
```

## The other edges, measured

### 1. The unattributed list is a required section

*"An unattributed commit is the summary's version of a table row with no exit
condition: invisible, and therefore permanent."* Present even when empty, and
it names shas and subjects rather than a count. A recap that quietly covers 40
of 42 commits is the old "Plan inventory" again — the section that covered 30
of 72 plan documents and said nothing about the other 42.

### 2. Merges double-count, and the lifetime figure understates the present

Thirteen of the window's 55 commits are merges; 232 of the repo's 1,082 are. A
merge and the commits it brings in are the same work counted twice — and
`Merge pull request #NNN from feature/plan-142-x` attributes cleanly by branch
name, which makes merges the easiest thing to attribute and the most misleading
thing to count. Decide whether the recap counts them, follows them, or ignores
them, and say which.

The plan's own figure — *"conventional prefixes cover 11 commits of 1,041, and
subject-line mentions cover 298"* — is a **lifetime** number. Over the last 30
days subjects alone reach **146 of 170, 86%**. The convention improved; the
plan was measured before it did, and building to its 29% builds for a repo that
no longer exists.

### 3. Pin the week boundary to one clock

`git log --since/--until` reads author dates in local time, so a commit at
23:40 on a Sunday lands in either week depending on where it was made.
`audit_plan_state_history.py` uses `--date=short`, which is local. Follow it or
do not, but write down which.

### 4. Do not borrow the archive's provenance vocabulary

`*(observed)*`, `*(corroborated)*` and `*(inferred)*` are **the archive's**
labels, with a specific meaning about how a reconstructed completion date was
recovered, and Stage 5's skill is already forbidden from emitting them. A recap
that reuses those three words for its own uncertainty makes them look like
generic hedging and devalues 25 backfilled rows. Mark uncertainty in the
recap's own words.

### 5. What the test will and will not do for you

The link check walks **every** markdown file under `docs/`, so `docs/recaps/`
is covered the moment it exists: a recap that gets a plan document's path wrong
fails the suite. That is free.

What it does not do: a recap is not a plan document, contributes nothing to
coverage, and naming a plan in a recap does not put that plan in a table.
Nothing asserts a recap is accurate, complete, or written at all. If you want
that, build it — a structural check that a recap has its required sections is
cheap. **A check that every commit in the window appears somewhere in the recap
is the one worth thinking hardest about**, and note it is a property of a
generated artifact rather than of the repo, which is a different kind of test
from everything now in that file.

If you add assertions, **watch each one fail first.** Stage 5 applied seven
mutations before writing a line of skill, and all eighteen of Stage 4's
assertions passed on every one.

### 6. This is the second skill, so the shape is settled

`.gitignore` reads `.claude/*` with `!.claude/skills/`, so a new skill
directory is tracked automatically and `settings.local.json` stays local.

## Measurements to check your work against

| Thing | Value |
|---|---|
| `PLANS.md` | 169 lines of a 250 budget — Stage 6 must not change it |
| Closeout / build order / backlog / superseded rows | 3 / 18 / 14 / 14 |
| Archive rows | 108, and the index says so; both are checked |
| Test | 27 assertions, 0.11s, 394 markdown links scanned |
| Full suite | 2407 passed, 401 deselected, ~12s |
| Sweep | 3 never-used (44, 85, 104), 0 unrecorded |
| Last complete week on 2026-08-21 | Mon 2026-08-10 .. Sun 2026-08-16, **55 commits** |
| Commits deferred to the next window | **114** — the tail is twice the window |

## What the skill must not do

- **move a row, or edit any of the five tables.** That is Stage 5's skill, and
  the separation is the entire reason Stage 6 is its own stage.
- **change a plan document.** Stage 5's may touch a status marker; this one may
  not touch anything. It reads plan documents and writes elsewhere.
- **drop a commit it could not attribute.** Name it.
- **emit `*(observed)*`, `*(corroborated)*` or `*(inferred)*`.**
- **regenerate an existing recap silently.** A recap is a dated record of what
  was knowable that day.
- **grow a list of special cases.** If a commit needs an exception, the rule is
  wrong.

## Verification

```bash
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -v
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest -q -m "not integration"
python scripts/audit_plan_state_history.py --coverage
```

Then **run the skill for real and read what it wrote.** On 2026-08-21 that is
the week of **2026-08-10 to 2026-08-16** — 55 commits, 13 of them merges —
written to `docs/recaps/2026-08-16.md`. Check four things by hand, because no
test will:

1. **Every commit in the window is either attributed or listed as
   unattributed.** The denominator must reconcile exactly:

   ```bash
   git rev-list --count --all --since='2026-08-10 00:00:00' \
                              --until='2026-08-16 23:59:59'   # 55
   ```

2. **The recap says what it deferred.** 114 commits sit after the window end.
   If the recap does not say so, they are invisible, and invisible is the one
   thing this plan does not permit.

3. **A plan the recap names is described from its plan document, not from its
   commit subjects.** If it reads like `git log` with better grammar, the *why*
   half is missing and the skill is a changelog.

4. **Nothing in `docs/PLANS.md`, `docs/planning/completed_plans.md` or any plan
   document changed.** `git status` proves it. A recap that moved a row is the
   failure this stage was separated to prevent.

## Scope

The skill, `docs/recaps/` and its first real recap, the decision-log entry
recording what you decided about attribution and about regeneration, and
whatever test extensions you choose — each watched failing first.

**Do not** re-open Stage 5's skill, re-order the build order, re-litigate
Stage 3's directory layout, or resolve the plan-number collision between
`plan_82_user_management.md` and the `plan_82_self_hosted_runner.md` that lives
only on `origin/fix/import-errors`.

Stage 6 is the last stage. When it lands, Plan 146 owes a closeout row with a
gate, or an archive row — and Stage 5's `plans` skill is what writes it.
