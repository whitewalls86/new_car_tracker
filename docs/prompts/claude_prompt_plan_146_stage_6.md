# Plan 146 Stage 6 — a skill for the weekly recap

Read `docs/plans/plan_146_planning_system.md` first — it is the source of
truth, and its **Stage 6** section is the specification. Then read
`.claude/skills/plans/SKILL.md`, which is Stage 5's skill and the thing this
one must not become; `docs/PLANS.md`, whose header states the structure a recap
reads; and `tests/test_planning_docs.py`, which will link-check whatever you
write. Finally read the last two entries of `docs/planning/plans_decision_log.md`
— Stage 5's records what was decided about prose, and by omission what was left.

Work continues on branch **`plan-146-planning-system`**. Stages 0-5 are
committed there, the most recent being `937dd43`. **Commit Stage 6's work on
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
belongs to, opens those plan documents for the *why*, and writes a recap of
what actually happened. Commits say what changed; plan documents say what it
was for. A recap from either alone is a changelog or a wish list.

**It writes prose and never state.** Stage 5's skill moves rows and never
composes a sentence; this one composes sentences and never moves a row. The two
must not be merged: a tool that both summarises work *and* moves rows between
tables can move a row because its own summary said so, which is a
self-confirming record.

### Where it writes — the maintainer changed this on 2026-08-21

The plan document as written says the output is *"a dated entry appended to the
decision log, not a new surface."* **That is superseded.** The maintainer wants
a browsable record, so:

- output goes to **`docs/recaps/`**, a sixth directory under `docs/`
- **one file per window**, named for the window's end date: `docs/recaps/2026-08-16.md`
- **`recaps/` only.** The decision log goes back to holding decisions, not
  events. Two surfaces asserting what happened in one week is the duplication
  Plan 146 removed, so do not also append a recap or a pointer to the log.

`recaps/` encodes kind, not state, so it is consistent with Stage 3's rule.
Stage 5 already corrected the plan document's Stage 6 section to say all this;
if it disagrees with this prompt, the plan document wins and this prompt is the
bug.

### The window — Monday to Sunday, and only complete weeks

A week runs **Monday through Sunday**, and the skill recaps **the last complete
week — never a partial one**. Run it on Friday 2026-08-21 and it recaps
**Monday 2026-08-10 through Sunday 2026-08-16**, writes
`docs/recaps/2026-08-16.md`, and leaves Monday the 17th onward for next week's
file.

That deferral is deliberate, and it is also this skill's sharpest reporting
obligation. Measured on 2026-08-21:

| | |
|---|---:|
| Commits inside the window, 2026-08-10 .. 08-16 | **55** (13 merges, 42 non-merge) |
| Commits after the window end, not yet recapped | **114** |

**The tail is twice the window.** A recap that silently ignores 114 commits
because they fall outside its boundaries is indistinguishable from a recap that
lost them — the same defect as an unattributed commit, one level up. So the
recap **states its own window and says how many commits fall after it**, and
the file is named for the window's end date so the deferral is visible on the
filesystem rather than only in the prose.

**Missed weeks are the failure mode this shape creates.** One file per week
plus last-complete-week-only means that not running the skill for a fortnight
leaves a hole nothing announces. Before writing, read what `docs/recaps/`
already holds; if the newest recap is older than the week before last, either
recap each missing week in turn or name them and stop. Do not write the current
week on top of a gap.

**Pin the boundary to one clock and say which.** `git log --since/--until`
reads author dates in local time, so a commit at 23:40 on a Sunday lands in
either week depending on where it was made.
`scripts/audit_plan_state_history.py` uses `--date=short`, which is local; if
you follow it, write that down, and if you do not, write down what you chose
instead.

## The sharp edge that decides this stage

**The plan prescribes an attribution method that measurement shows does not
work, and credits a tool that does not implement it.**

The plan says the skill needs *"the same layered attribution
`audit_plan_state_history.py` uses — subject, then branch name, then the plan
documents touched by the diff."* Two problems, both checkable:

**1. The script does not do this.** `mentioned_numbers()` reads commit
*subjects* only. `ever_mentioned()` reads subjects, bodies, every revision of
the index and the current contents of `docs/` — but it answers *"was this plan
number ever real?"*, which is a different question from *"which plan does this
commit belong to?"*. There is no layered attributor in the repo to reuse. You
are building it, not borrowing it.

**2. The diff layer contributes nothing.** Measured over the last 30 days:

| Layer | Commits it attributes | Running total of 170 |
|---|---:|---:|
| commit subject names a plan | 146 | 146 |
| commit **body** names a plan | +6 | 152 |
| **containing branch** names a plan | +18 | **170** |
| plan documents touched by the diff | **+0** | 170 |

**Zero.** Work commits touch code, not plan documents — `2cfdb73` changed 24
files and not one was under `docs/plans/`. The layer the plan names third is
the layer that finds nothing, and the layer it names second is the one that
closes the gap to zero.

Re-derive it before you trust it:

```bash
git log --all --since='30 days ago' --pretty='%H%x01%s' | wc -l
git log --all --since='30 days ago' --pretty=%s | grep -icE '\bplan[ _-]?[0-9]{1,3}\b'
```

### And the layer that works has a shelf life

All 18 commits the branch layer rescues **are also on `master`**. `git branch
--contains` finds them only because their plan-named branches still exist among
this repo's 76 refs. Delete those branches — which merging is supposed to do —
and `--contains` returns `master` alone and the layer returns nothing.

So the same window recapped today yields **0 unattributed** and recapped in
three months yields **18**. That is not a bug you can fix; branch names are not
durable history. It is a property you must design around, and the honest
design is that **a recap records its attribution at the time it was written**
and is never silently regenerated. Decide this in writing.

## The other edges, measured

### 1. "Say which commits it could not attribute" is the whole point

The plan is explicit: the skill must **say which commits it could not
attribute** rather than dropping them. *"An unattributed commit is the
summary's version of a table row with no exit condition: invisible, and
therefore permanent."*

That is the same argument Stage 4 is built on and it is not decoration. A recap
that quietly covers 86 of 114 commits is the "Plan inventory" again — the
section that covered 30 of 72 plan documents and said nothing whatsoever about
the other 42.

So the unattributed list is a **required section**, present even when empty,
and it names shas and subjects rather than a count.

### 2. Merges double-count, and the lifetime figure understates the present

Thirteen of the window's 55 commits are merges, and 232 of the repo's 1,082
are. A merge and the commits it brings in are the same work counted twice — and
`Merge pull request #NNN from feature/plan-142-x` attributes cleanly by branch
name, which makes merges the easiest thing to attribute and the most misleading
thing to count. Decide whether the recap counts merges, follows them, or
ignores them, and say which.

The plan's own attribution figure — *"conventional prefixes cover 11 commits of
1,041, and subject-line mentions cover 298"* — is a **lifetime** number and
badly understates the present. Over the last 30 days, subjects alone reach
**146 of 170, 86%**. The convention improved; the plan was measured before it
did, and building to its 29% would be building for a repo that no longer
exists.

### 3. Machinery that genuinely is reusable

Unlike the attribution claim, this part is real.
`scripts/audit_plan_state_history.py` already reconstructs *what moved between
states* by walking the index's own history:

- `revisions()` — `(sha, date)` for every revision of `PLANS.md`, oldest first
- `state_map(text)` — plan number → canonical state for one revision
- `plans_in_section(body)` — the plan numbers one `##` section claims

Diff two `state_map`s across the window's endpoints and you have the recap's
"what moved between states" section, derived rather than asserted. Use these.
Do not re-implement them, and if you need to change one, check
`--coverage` still reports 3 never-used (44, 85, 104) and 0 unrecorded
afterwards.

### 4. Do not borrow the archive's provenance vocabulary

The plan says a recap should distinguish *"what it observed from what it
guessed"*, and it should. But `*(observed)*`, `*(corroborated)*` and
`*(inferred)*` are **the archive's** labels, with a specific meaning about how
a reconstructed completion date was recovered. Stage 5's skill is already
forbidden from emitting them. A recap that reuses those three words for its own
uncertainty makes the archive's labels look like generic hedging and devalues
25 backfilled rows.

Mark uncertainty in the recap's own words. Do not reach for those three.

### 5. What the test will and will not do for you

`tests/test_planning_docs.py` walks **every** markdown file under `docs/`, so
`docs/recaps/*.md` is link-checked the moment it exists: a recap linking
`../plans/plan_141_structured_log_ingestion_contract.md` fails the suite if it
gets the path wrong. That is free and worth having.

What it does **not** do: a recap is not a plan document, contributes nothing to
coverage, and naming a plan in a recap does not put that plan in a table. There
is no assertion anywhere that a recap is accurate, complete, or written at all.
If you want one, build it — a structural check that a recap has its required
sections is cheap. **A check that every commit in the window appears somewhere
in the recap is the one worth thinking hardest about**, and note that it is a
property of a generated artifact rather than of the repo, which is a different
kind of test from everything now in that file.

If you add assertions, **watch each one fail first.** Stage 5 applied seven
mutations to the working tree before writing a line of skill, and all eighteen
of Stage 4's assertions passed on every one. A test nobody has seen fail is not
yet working.

### 6. This is the second skill, so the shape is settled

`.claude/skills/plans/SKILL.md` is the format: YAML frontmatter with `name` and
`description`, the description written so the harness can tell when the skill
applies. Note that `.gitignore` now reads `.claude/*` with `!.claude/skills/`,
so a new skill directory is tracked automatically and `settings.local.json`
stays local.

Read Stage 5's skill for its boundary table and its "splice, never reflow"
rule. You need neither — you are appending a new file, not editing a table —
but the *shape* of that boundary is what kept it honest, and this skill needs
the mirror image of it.

## Measurements to check your work against

| Thing | Value |
|---|---|
| `PLANS.md` | 169 lines of a 250 budget — Stage 6 must not change it |
| Closeout / build order / backlog / superseded rows | 3 / 18 / 14 / 14 |
| Archive rows | 108, and the index says so; both are checked |
| Test | 27 assertions, 0.11s, 394 markdown links scanned |
| Full suite | 2407 passed, 401 deselected, ~12s |
| Sweep | 3 never-used (44, 85, 104), 0 unrecorded |
| Last complete week on 2026-08-21 | Mon 2026-08-10 .. Sun 2026-08-16, 55 commits |
| Commits deferred to the next window | 114 |

## What the skill must not do

- **move a row, or edit any of the five tables.** That is Stage 5's skill, and
  the separation is the entire reason Stage 6 is its own stage.
- **change a plan document.** Stage 5's skill may touch a status marker; this
  one may not touch anything. It reads plan documents for the *why* and writes
  somewhere else.
- **drop a commit it could not attribute.** Name it.
- **emit `*(observed)*`, `*(corroborated)*` or `*(inferred)*`.**
- **regenerate an existing recap silently.** A recap is a dated record of what
  was knowable that day.
- **grow a list of special cases.** If a commit needs an exception, the
  attribution rule is wrong.

## Verification

```bash
# the invariant, unchanged
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -v

# full suite -- 2407 passed, 401 deselected, ~12s before this stage
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest -q -m "not integration"

# the sweep still agrees
python scripts/audit_plan_state_history.py --coverage
```

Then **run the skill for real and read what it wrote.** On 2026-08-21 that is
the week of **2026-08-10 to 2026-08-16** — 55 commits, 13 of them merges —
written to `docs/recaps/2026-08-16.md`. Check four things by hand, because no
test will:

1. **Every commit in the window is either attributed or listed as
   unattributed.** Count them; the denominator must reconcile exactly:

   ```bash
   git rev-list --count --all --since='2026-08-10 00:00:00' \
                              --until='2026-08-16 23:59:59'   # 55
   ```

2. **The recap says what it deferred.** 114 commits sit after the window end
   and belong to next week's file. If the recap does not say so, those commits
   are invisible, and invisible is the one thing this plan does not permit.

3. **A plan the recap names is described from its plan document, not from its
   commit subjects.** If the recap reads like `git log` with better grammar,
   the *why* half is missing and the skill is a changelog.

4. **Nothing in `docs/PLANS.md`, `docs/planning/completed_plans.md` or any
   plan document changed.** `git status` proves it. A recap that moved a row is
   the failure this stage was separated to prevent.

## Scope

The skill, `docs/recaps/` and its first real recap, the decision-log entry
recording what you decided about attribution and about regeneration, and
whatever test extensions you choose to add — with each new assertion watched
failing first.

**Do not** re-open Stage 5's skill, re-order the build order, re-litigate
Stage 3's directory layout, or resolve the plan-number collision between
`plan_82_user_management.md` and the `plan_82_self_hosted_runner.md` that lives
only on `origin/fix/import-errors`.

Stage 6 is the last stage. When it lands, Plan 146 owes a closeout row with a
gate, or an archive row — and the `plans` skill from Stage 5 is what writes it.
