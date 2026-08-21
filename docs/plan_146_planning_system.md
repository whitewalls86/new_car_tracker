# Plan 146: The Planning System

## Status

**Draft — not started.** Priority **92 (critical-adjacent)**, and the
**maintainer's #1 priority as of 2026-08-21**. Effort **M** (1-2 weeks).

It holds row 1 over Plan 136's score of 98 because 136 has no executable step
until ~2026-09-09 and this does — which is the build order's own rule working
as designed rather than an exception to it. The score reflects dependency
leverage: every other plan's status is tracked by the thing this fixes.

Effort was S before Stage 1 was added; a history sweep across 1,041 commits
with a three-tier provenance rule is not a day's work.

This plan is about the planning documents themselves. It exists because the
2026-08-21 Plan 144 closeout could not answer *"is Plan 135 done?"* from the
index, and because answering it took reading four tables that disagreed.

## The defect

`PLANS.md` has **four** surfaces that assert a plan's status, and no rule about
which one wins. On 2026-08-21 they disagreed:

| Plan | `completed_plans.md` | `PLANS.md` "Completed" | "Operational watch list" | "Paused or blocked" |
|---|---|---|---|---|
| 135 | absent | absent | **"Complete 2026-08-18"** | — |
| 131 | present | absent | **"Complete"** | — |
| 114 | absent | **present** | — | **"follow-on"** |
| 143, 133, 128, 115 | absent | present | — | — |

Read that table twice. **Plan 135 is declared complete in prose and recorded in
neither completion record.** Plan 114 is completed and blocked at the same
time. Plan 131's completion is in the archive but not the index, so the index
says it is being watched and the archive says it is finished.

Three more measurements from the same pass:

- **The "Plan inventory" covers 30 of 72 `plan_*.md` files.** It is not an
  inventory. Nothing tells you which 42 are missing, so its silence is
  indistinguishable from a plan not existing.
- **The "Operational watch list" has no dates and no exit criteria.** Three of
  its six rows had been asking for closeout that had already happened — for
  weeks, in the case of Plan 115, whose bookkeeping completed 2026-07-01.
- **"Paused or blocked" duplicates the build order's `Blocked by` column** and
  contains a row (114 follow-on) whose parent plan is complete.

### Why it got this way, which matters for the fix

Every one of these surfaces was **added in good faith to solve a real problem**,
and none was given a removal rule. A watch list is a good idea. An inventory is
a good idea. The failure is not that they exist; it is that a row can enter a
table and nothing ever makes it leave. `PLANS.md` grew from an index to 683
lines the same way — a paragraph of history per departed closeout row, a
sequencing argument per ordering decision, a deploy narrative per deploy.

**A table without an eviction rule is a leak.** That is the one-sentence
diagnosis, and every fix below follows from it.

## Out of scope

- **Rewriting plan documents.** Each plan's own file stays exactly as it is.
  This plan changes the index and the rules, not the content it points at.
- **Deciding what to build next.** The build order's ordering is Plan 117's and
  the maintainer's; this plan only changes how it is recorded.
- **A tool that writes plans.** The skill in Stage 3 edits *state* — moving a
  plan between tables, archiving it — and never authors prose.

## The design

### One fact, one owner

| Fact | Owner | Everything else |
|---|---|---|
| What a plan is, why, and its evidence | **the plan's own `docs/plan_NNN_*.md`** | links to it |
| Which plans are active, in what order | **`PLANS.md` build order** | — |
| What is waiting on evidence | **`PLANS.md` closeout** | — |
| What is finished | **`completed_plans.md`** | — |
| Why the order is what it is; what happened when | **`plans_decision_log.md`** | — |

`PLANS.md` becomes an index that *derives* from plan documents rather than
competing with them. If the index and a plan document disagree, **the plan
document wins**, and the index is the bug.

### Every plan is in exactly one state

```
        trigger fires              work starts           code lands
Backlog ──────────────► Build order ──────────► Closeout ──────────► Archive
   ▲                         │                      │                (completed_plans.md)
   └─────────────────────────┴──────────────────────┘
              trigger un-fires, or plan superseded
```

Four states, four tables, **one row per plan across all of them**. That is the
invariant, and it is the thing that was broken: Plan 114 was in two, Plan 135 in
none.

- **Backlog** — no trigger has fired. Absorbs today's "Paused or blocked",
  which is the same idea with a different name; its `Resume when` column is a
  trigger by another word.
- **Build order** — active. Keeps the `Workable?` and `Blocked by` columns
  added 2026-08-21.
- **Closeout** — code is deployed, evidence is pending. **Every row needs a
  `Lands` date and a removal gate**, which is what today's watch list lacks.
- **Archive** — `completed_plans.md`, one row, newest first.

The "Operational watch list" does not survive as a table. A plan whose
behaviour needs watching is either *in closeout* (there is a specific thing to
observe, by a date) or *archived* (there is not, and monitoring is an
operational concern rather than a plan). Plans 129, 131 and 135 split cleanly
that way: 129 has a live backfill to watch, 131 and 135 are finished.

The "Plan inventory" does not survive either. A partial list of plan files is
worse than no list, because its omissions look like non-existence. `ls docs/`
is complete and free.

### Rules that make it stick

1. **Every table row carries its own exit condition.** Closeout rows name a
   date and a gate. Backlog rows name a trigger. Build-order rows name the row
   or date that unblocks them.
2. **A row leaves the moment its condition is met**, and its result goes to the
   plan document — *only* there. No summary paragraph stays behind. This is the
   rule that `PLANS.md`'s closeout section broke twenty lines' worth.
3. **Narrative goes to `plans_decision_log.md`**, never inline. Reasoning that
   sits next to an index drifts from it, and the reader cannot tell which is
   current — on 2026-08-21 a paragraph under the build order asserted
   `ct-403-log-spike` was diurnal while the row above it recorded the
   disproof.
4. **`PLANS.md` has a line budget.** It is an index; if it exceeds ~250 lines
   something has become narrative. State the budget in the file so exceeding it
   is visible.
5. **Cross-references key on plan numbers, never row ordinals.** Demonstrated
   while adding this plan: inserting one row at position 4 shifted fifteen rows
   and silently invalidated six `Blocked by` cells that said things like
   `[Row 8]`. Plan numbers are stable; positions are not. Stage 4's test should
   reject a reference that names a position.

## Stages

### Stage 0 — Reconcile the contradictions *(blocking, and the only stage with a right answer)*

Before restructuring anything, settle each disputed plan against its own
document and the production evidence. Deliverable is a table of
plan → true state → where the evidence is. **Plans 129, 131, 135, 130 and 114
are the known disputes**; the sweep may find more, because nothing has ever
checked.

Do not skip ahead. Restructuring on top of contradictory data preserves the
contradictions in a tidier shape, and backfilling on top of them propagates
them into the archive, where they become much harder to notice.

### Stage 1 — Backfill the record from git *(mechanical)*

Stage 0 settles what is *contradictory*. This settles what is *absent* — the
completion dates, entry dates and archive rows that were never written down.
The history is richer than expected, measured 2026-08-21:

| Signal | Measurement |
|---|---|
| Commits | 1,041, of which **298 name a plan** and 221 are PR merges |
| Per-plan dates | Every `docs/plan_NNN_*.md` yields a creation date and a last-touched date |
| **State timeline** | Walking `PLANS.md`'s own history and recording which `##` section each plan sits in at each revision reconstructs its **transitions** |

That last row is the useful one. Plan 135 — the plan declared complete in prose
and recorded in neither completion record — resolves cleanly: git puts it in
`Current closeout` at `b76fb44` (2026-08-17) and `8267e5c` (2026-08-18), which
corroborates the 2026-08-18 date the prose asserts. The record was lost; the
evidence was not.

**Method.** For each revision of `PLANS.md`, parse section membership per plan
number. Emit a per-plan timeline of `(date, state)` transitions. A plan's
completion date is the commit where it entered Completed or the archive.
Reconcile that against the plan document's own claimed dates, and write the
missing archive rows.

**The one rule this stage must not break: mark derived facts as derived.**
Three tiers, and they must stay visually distinct in the output:

1. **Observed** — a dated transition in `PLANS.md`'s history. A fact.
2. **Corroborated** — a date the plan document claims *and* git supports, as
   with Plan 135. A fact with two sources.
3. **Inferred** — no transition was ever recorded, so the plan's last-touched
   date is used as a proxy. **This is a guess**, and writing it unmarked would
   manufacture history, which is a worse defect than the missing record it
   replaces.

Known limits, so the sweep is not mistaken for completeness. Conventional
commit prefixes (`docs(plan-146)`) appear in only 11 commits and are recent, so
message parsing is not a reliable signal — file history is. And the archive's
oldest entries (Plans 0-7) predate plan documents entirely, so nothing can be
backfilled for them; they stay as they are.

### Stage 2 — Collapse to four tables

Fold "Paused or blocked" into Backlog with a `Trigger` column. Delete the Plan
inventory. Move `PLANS.md`'s "Completed" table into `completed_plans.md` and
leave a link. Retire the watch list, routing its rows to closeout or archive
per Stage 0. Add `Lands` and `Gate` columns to closeout.

### Stage 3 — Give `docs/` a hierarchy

`docs/` is **98 files and zero directories**. Seventy-eight are plan documents;
the rest are seven `claude_prompt_*` session prompts, three runbooks,
`ARCHITECTURE.md`, an SVG, and one-offs like a March code review and a pipeline
health report. It is the same defect as the tables: things accreted and nothing
said where they go.

Proposed layout:

```
docs/
  ARCHITECTURE.md
  PLANS.md              index
  plans/                all 78 plan documents, flat
  planning/             completed_plans.md, decision_log.md
  runbooks/
  prompts/              claude_prompt_*
  reference/            case study, code review, health report, debug bundle, assets
```

**Directories encode *kind*, not *state*.** The tempting split is
`plans/active/` and `plans/completed/`, and it is wrong: state changes, so
every completion would move a file, break every inbound link, and put the same
fact in two places — the path and the index — which is the defect this plan
exists to fix. Plan documents keep one stable path forever; `PLANS.md` and the
archive say what state they are in. This is rule 5 again: key on something that
does not move.

The cost is one large rename that breaks every relative link in the repo. That
is mechanical, `git mv` preserves history, and Stage 4's test should assert
that no markdown link in `docs/` is dangling — a check worth having
permanently, since it would have caught the `see below` that dangled for a day
after a section moved on 2026-08-21.

Do this **after** Stage 2. Collapsing tables while paths are moving means every
conflict is two problems at once.

### Stage 4 — Make the invariant a test

`tests/test_planning_docs.py`, in the shape this repo already uses for
`TestServiceHealthCoverage` — a deny-list-free structural assertion:

- every `docs/plan_NNN_*.md` appears in **exactly one** of the four tables
- no plan number appears in two
- every closeout row has a `Lands` date that parses, and a gate
- every backlog row has a trigger
- every build-order row's `Blocked by` names a real row or a date
- `PLANS.md` is under its stated line budget
- no markdown link anywhere in `docs/` is dangling

This is the stage that matters. Rules 1-5 above are conventions, and this
project's own history is that conventions without a failing test decay — that
is why `TestServiceHealthCoverage` exists, and it is the same argument here.
A CI failure is what makes a leak impossible rather than merely discouraged.

### Stage 5 — A skill for the edits

Once the structure is fixed and tested, the routine operations are small and
repetitive: move a plan between states, add a closeout row, archive a completed
plan, record a soak result. Each touches two or three files in a fixed pattern
and is exactly the kind of thing done inconsistently by hand.

A `plans` skill should perform those transitions and nothing else. It writes
**state, never prose** — it will not author a plan, summarise a result, or
decide an order. Its correctness is defined by Stage 4's test passing after
every operation it performs.

Sequencing note: the skill comes **last on purpose.** A skill that automates a
structure still being argued about encodes the argument.

## Success criteria

| Metric | Gate |
|--------|------|
| Contradictions | Zero plans in two tables; zero plans in none |
| "Is plan N done?" | Answerable from one table, in one lookup |
| Table hygiene | Every row carries a date or trigger that would remove it |
| Enforcement | The invariant is a CI test, not a convention |
| `PLANS.md` size | Under its stated budget, and the budget is in the file |
| `docs/` layout | Every file is in a directory named for its kind; no directory names a state |
| Skill | Every transition it performs leaves Stage 4's test green |
| Backfill | Every filled-in date is labelled observed, corroborated or inferred |

## Verification

Stage 4's test is the verification — it either passes against the real docs or
names the row that breaks it.

One human check the test cannot make: hand someone the index cold and ask
*"what should I work on, and what is Plan 135's status?"* Both answers should
come from one table each, without reading a plan document. That question is
what failed on 2026-08-21 and is the reason this plan exists.
