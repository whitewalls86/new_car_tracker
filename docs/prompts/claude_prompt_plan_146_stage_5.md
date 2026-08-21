# Plan 146 Stage 5 — a skill for the edits

Read `docs/plans/plan_146_planning_system.md` first — it is the source of
truth, and its **Stage 5** section is the specification. Then read
`docs/PLANS.md`, whose header states the rules the skill must preserve, and
`tests/test_planning_docs.py`, which is the definition of "correct" you are
building against. Finally skim the Stage 4 entry at the end of
`docs/planning/plans_decision_log.md` — it records what the test does and, by
omission, what it does not.

Work continues on branch **`plan-146-planning-system`**. Stages 0-4 are
committed there, the most recent being `9ed8f30`. **Commit Stage 5's work on
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
| 4 | `9ed8f30` | `tests/test_planning_docs.py` — 18 assertions, 0.11s, no deny-list. Seven structural rules, each proven able to fail |

## The task

A `plans` skill that performs the routine state transitions and **nothing
else**. It writes **state, never prose**: it will not author a plan, summarise
a result, or decide an order.

This is the first skill in this repo — `.claude/` holds only
`settings.local.json` today. It goes at `.claude/skills/plans/SKILL.md`, with
YAML frontmatter carrying `name` and `description`, the description written so
the harness can tell when the skill applies. A working example of the format lives
outside this repo at
`/Users/andrewkafene/Documents/housing-data/.claude/skills/process-notes/SKILL.md`,
if that checkout is still on the machine.

The operations the plan names:

1. **move a plan between states** — backlog → build order, build order →
   closeout, anything → superseded
2. **add a closeout row** — with its `Lands` date and its gate
3. **archive a completed plan** — remove its row, prepend to the archive
4. **record a soak result** — see the contradiction below before building this

**Correctness is defined by Stage 4's test passing after every operation the
skill performs.** That is the plan's own criterion, and it is the acceptance
bar for this stage.

## The measured reason this stage exists

Stage 1's sweep reconstructed 122 state transitions across 35 days. **92 of
them are a row disappearing** — `archive → absent`, `backlog → absent`,
`build → absent` — rather than moving anywhere.

Strip the two bulk events (45 archive rows vanishing in one revision on
2026-04-07, and 14 on 2026-08-21 when Stage 2 removed the index's duplicate
Completed table) and **33 rows still vanished, one and two at a time, across 16
separate days.** No single accident produced that — it is the shape of doing
this by hand, sixteen times. Reproduce it with a script and you have automated
the leak.

You can re-derive all of this:

```bash
python scripts/audit_plan_state_history.py --json
```

## The sharp edge that decides this stage

**Stage 4's test is necessary, not sufficient, and a skill is exactly the tool
that finds the difference.** A human editing by hand makes plausible mistakes.
An automated editor makes systematic ones, and it makes them in whichever
direction the test happens not to look.

Seven mutations were applied to the working tree and the full test file was run
against each. **All eighteen assertions passed on all seven.**

| # | Mutation | Test result |
|---|---|---|
| A | a superseded row's `Superseded by` emptied | passes |
| B | archive rows reordered, breaking newest-first | passes |
| C | `Order` values duplicated (`3` twice) and jumped to `99` | passes |
| D | an archive `Date` cell set to `sometime in August` | passes |
| E | `[112](plans/plan_113_production_adaptive_refresh.md)` — link text and target disagree | passes |
| F | the `**88**` backlog row deleted outright | passes |
| G | one backlog row duplicated within the backlog | passes |

**F is the dangerous one.** Six index rows name a plan that has no document:
`**88**`, `**87**`, `**5**`, `**52**`, `**55**`, `**56**`. Coverage keys on
plan documents, so a documentless row has nothing asserting it exists. Delete
it and every test still passes — which is precisely how Plans 5, 52, 55 and 56
were lost in the first place, and Stage 0 recovered them from index history.
**A skill that rewrites tables can drop one of these six and nothing will say
so.**

C and B are the ones automation reaches for: renumbering `Order` after an
insert, and choosing where a new archive row goes.

**Decide, in writing, which of these the skill closes and how.** Two honest
routes, and you may take both:

- make the skill structurally incapable — read rows, mutate the list, re-render
  the whole table, so a dropped row requires a bug rather than an omission
- extend `tests/test_planning_docs.py`. B, C, D and F are all cheaply
  checkable, and F is checkable by asserting the *count* of documentless rows
  never falls silently — though note that a count is a number to edit, which is
  the deny-list failure mode wearing a different hat. Think about it before
  reaching for it.

If you extend the test, extend it **first**, watch each new assertion fail,
and only then build the skill. A skill written against a test you have not seen
fail is a skill whose green run means nothing.

## The other five edges, measured

### 1. The plan document is the authority, and 17 of them say nothing

`PLANS.md` states it plainly: *"If this index and a plan document disagree, the
plan document wins."* But status is written three different ways and often not
at all:

| Form | Count |
|---|---|
| a `## Status` section | 25 |
| a `**Status:**` line in the first dozen lines | 37 |
| neither | 17 |

**And the authority is wrong right now.** `docs/plans/plan_146_planning_system.md`
opens with **"Draft — not started."** while Stages 0-4 sit committed on the
branch.
The index says Plan 146 is row 1 of the build order with Stages 0-4 done. By
`PLANS.md`'s own rule the document wins, so the index is currently claiming
something its authority contradicts.

So: **does the skill touch plan documents?** The plan says each operation
"touches two or three files", which implies it does. Answer it deliberately.
If yes, it must handle three formats and the absence of all three, and it must
not invent a `## Status` section in a document that has never had one. If no,
say so and say what closes the gap instead. Do not leave it implied.

Fixing Plan 146's own stale status is fair game and probably the skill's first
real test.

### 2. The archive is prepend-only, and the index counts it by hand

`docs/planning/completed_plans.md` is newest-first. A new row goes at the
**top**, immediately after `|------|-------------|------|` on line 29 — not
appended, which is where a naive writer puts it.

`docs/PLANS.md:163` reads *"108 rows, newest first"*. That number is
hand-maintained, it is the only hard-coded count in the index, and nothing
checks it. Archive a plan without updating it and the index is wrong the
moment the skill succeeds.

Dates carry provenance labels — `*(observed)*`, `*(corroborated)*`,
`*(inferred)*` — and a date the skill writes today is none of those. It is
recorded at the time, which is the unmarked default. **Do not let the skill
emit a provenance label**; those exist to mark reconstructed history, and a
tool that applies them to a date it just observed devalues every genuine one.

### 3. Cell formats the parser already depends on

Stage 2 froze the column headers and Stage 4 reads them. The em dash in
`Gate — what removes this row` and the question mark in `Workable?` are load-
bearing; renaming either fails the test, which is the point.

The `Plan` cell takes exactly two forms, optionally followed by a bold stage
marker:

```
| [135](plans/plan_135_storage_observability.md) |
| [140](plans/plan_140_service_health_contract.md) **Stage 4** |
| **88** |
```

Links are relative to `docs/`, so `plans/plan_NNN_*.md` from `PLANS.md` and
`../plans/plan_NNN_*.md` from the archive. Plan 139 legitimately holds two
build-order rows, Stage C and Stage D — **one plan, several rows, one table**
is the design, and a skill that "deduplicates" the build order breaks it.

Cells may contain **escaped pipes**: build-order row 4 quotes the LogQL
fragment `\|= "403"`. Split on every `|` and that row reads one column short.
Stage 4 hit this; do not hit it again.

### 4. The line budget is a real constraint on what the skill may write

`PLANS.md` is 169 lines against a stated 250. Rows are single lines and some
are long, so the budget binds on row *count*, not prose. A skill that adds
context to a row it is moving will find the ceiling. It should not be adding
context at all — that is prose, and prose is Stage 6's job.

### 5. "Record a soak result" contradicts "never prose"

The plan lists it as an operation and forbids the thing it requires two
paragraphs later: *"it will not author a plan, summarise a result, or decide an
order."* A soak result is a summary.

Resolve it, do not paper over it. The defensible reading is that the skill
transcribes a result the human supplies **verbatim** and performs the state
change that follows — the human writes the sentence, the skill moves the row
and updates the count. Anything where the skill composes the sentence belongs
to Stage 6, and merging the two is the self-confirming record the plan warns
about: a tool that both summarises work and moves rows can move a row because
its own summary said so.

Whatever you decide, write it in the decision log.

## Measurements to check your work against

None of this changed in Stage 4.

| Thing | Value |
|---|---|
| `PLANS.md` | 169 lines of a 250 budget |
| Closeout / build order / backlog / superseded rows | 3 / 18 / 14 / 14 |
| Archive rows | 108, prepend-only, newest first |
| Index rows naming a documentless plan | 6 — plans 88, 87, 5, 52, 55, 56 |
| Plan documents | 79 files, 78 numbered, 73 distinct numbers |
| Test | 18 assertions, 0.11s, 394 markdown links scanned |
| Full suite | 2398 passed, 401 deselected, ~12s |

## What the skill must not do

- **decide an order.** It may insert a row where it is told; it may not choose
  a priority, an effort or a build-order position.
- **author or summarise.** Titles, gates, triggers and descriptions come from
  the human. The skill places them.
- **edit plan document content** beyond a status marker, if you decide it
  touches status at all.
- **grow a list of special cases.** If a plan needs an exception, the structure
  is wrong; that is the argument Stage 4 is built on and it applies here.
- **run the transition and the summary in one operation.** Stage 6 is separate
  on purpose.

## Verification

```bash
# the invariant, unchanged and still fast
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -v
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py --durations=10

# full suite -- 2398 passed, 401 deselected, ~12s before this stage
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest -q -m "not integration"

# the sweep still agrees: 3 never-used (44, 85, 104), 0 unrecorded
python scripts/audit_plan_state_history.py --coverage
```

Then **exercise the skill on the real files and prove the round trip.** Pick a
transition the repo actually owes — Plan 146's own stale status, or Plan 140
Stage 4, or a closeout row whose gate has closed. Run the operation, run the
test, then `git diff` and read every changed line. The diff should contain the
row that moved and nothing else; a skill that reflows a table it was not asked
to touch will bury the next real change in noise.

Do at least one **destructive rehearsal**: run an operation on a documentless
row (`**88**` is the obvious candidate), confirm the row survives intact, and
say so in the handoff. That is mutation F, and it is the one the test cannot
catch for you.

## Scope

The skill, the decision-log entry recording what you decided about plan-document
status and about "record a soak result", and whatever test extensions you choose
to add — with each new assertion watched failing first.

**Do not** write Stage 6's weekly-summary skill, re-order the build order,
re-litigate Stage 3's directory layout, or resolve the plan-number collision
between `plan_82_user_management.md` and the `plan_82_self_hosted_runner.md`
that lives only on `origin/fix/import-errors`.

Stage 6 comes after this one, reads the structure Stages 2-4 settled, and
reports the transitions this skill performs.
