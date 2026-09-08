---
name: plan-start
description: "Put a drafted plan into the build order — from the backlog when its trigger fires, or straight from a fresh draft with no row yet — through an interview: design, rejected alternatives, lettered stages each with an exit, the order table, estimates, and how those stages chunk into issues. Use when the user says start this plan, begin work on it, or move it into the build order. This skill is close-out's mirror: it gathers and proposes first and writes nothing until the user approves, then writes the build-order row via the plans skill, the Linear project, and the issue set the chunking answer produced. It never chooses a priority, a position, an estimate or a cycle assignment — those are the user's, asked for rather than derived."
---

# Starting a plan

This is the second of the two writing passes `docs/PLAN_DOCUMENT.md` splits
apart: **draft**, generated and skimmed, and **start**, argued line by line.
This skill is the second half, and it is always its own invocation — `plan-draft`
names it and stops, even when the two happen back to back in one sitting. What
must never happen is skipping the review pass a separate invocation forces into
view.

## Why this skill stops in the middle

`close-out` states the reason and it holds here unchanged: *"a thing that
summarises work and moves rows can move a row because its own summary said
so, and the record then confirms itself."* This skill designs a plan, sizes
it, and writes its issue set — three things that must not happen in one
motion. It gathers and proposes with **every file untouched**, stops, and
writes only what the user approved.

## What this skill produces, and what it deliberately does not

| Writes | Never |
|---|---|
| `## Design` and `## Stages`, added to an existing document | anything already in the document |
| the build-order row, via `plans` (operation 1 or 6 — see Phase 3) | a row it wrote itself |
| the Linear project, if none exists for this plan | a priority, a position, an estimate, a cycle assignment — chosen by this skill |
| one issue per deploy-requiring stage, one issue per bundled run of locally-verified stages — the grouping the deploy question derives and the user confirms | any issue set before that grouping is confirmed and approved |

## Phase 1 — Gather

Read, write nothing:

1. **The plan document.** `## What this plan is for` and `## The case` must
   already exist — `plan-draft`'s job. Confirm it has **neither** `## Design`
   nor `## Stages` yet. If either exists, this plan has already been started;
   stop and say so rather than re-running the interview over existing work.
2. **Which entry point this is.** Exactly one of:
   - **from the backlog** — a row exists in `docs/PLANS.md`'s backlog table,
     naming a trigger, and the user is saying that trigger fired;
   - **from a fresh draft** — no row exists anywhere for this plan number.
     `plan-draft`'s build-order answer leaves exactly this: two sections
     written, no row anywhere.

   This determines which `plans` operation Phase 3 uses. Getting it wrong
   there is not recoverable by editing — confirm it here.
3. **Whether a Linear project already exists** for this plan —
   `list_projects(query: "Plan NNN")`. Mid-cycle work through `ticket-now` can
   create one before a plan is ever started; reuse it rather than duplicating.

## Phase 2 — Interview, propose, then stop

1. **Design.** The agreed shape and the alternatives rejected on the way to
   it — this is what makes `## Design` worth reading months later.
2. **Stages.** For each: a letter, allocated in the order the stage was
   thought of and never as a suffix of another; what it delivers; a mandatory
   exit condition; and **whether that exit can only be verified with the
   stage's code actually running in production, not merely merged.** All code
   reaches the VM the same way — merged to `master`, pulled, deployed — so
   every stage merges regardless; the question is narrower: does *proving this
   exit* require that last, separately-gated deploy step, or does it verify
   locally (tests, a disposable clone, a dry run) before a PR ever opens?
   **Refuse to move past this step while any stage lacks an exit** — an order
   table pointing at a stage with no exit is not a plan that can be started.
3. **The order table.** Number the stages `1..N` in the order they will be
   worked. The letter and the number are independent — Plan 172's own table
   reads `A, H, B, C…` for exactly this reason, and a later-discovered stage
   takes the next letter wherever it lands in the order.
4. **Estimates.** Ask for each stage's estimate on the Fibonacci scale (`1`
   under half a day, `2` about a day, `3` two to three days, `5` most of a
   week). **Never derive one from the plan's effort label** — `ticket-now`
   already records why that number means something different.
5. **Chunking — derived from the deploy question, then confirmed with the
   Questions interface.** The grouping is not chosen from a menu of generic
   shapes; it falls out of what step 2 already asked, in order-table order:
   - **A stage whose exit needs production gets its own issue.** Its PR is the
     natural proof artifact for that issue, and bundling it with a neighbor
     would let the issue read "done" on the neighbor's local proof alone.
   - **A run of consecutive stages that all verify locally bundles into one
     issue**, covering every stage in that run as its own `## Stage X`
     section, with no internal `blockedBy` chain. A plan whose stages are
     entirely local — this one, so far — collapses to a single issue, which is
     the shape `CAR-70` reached by hand before this rule existed to name it.
   - The deploy-requiring issues and the local-run issues interleave in order
     order, and a deploy-requiring issue's `blockedBy` still names its
     immediate predecessor issue, whichever kind it is.

   Present the computed grouping — which issues, which stages each covers, and
   why each split landed where it did — as a proposal, not a blank question.
   The Questions interface asks the user to **confirm it or name an
   adjustment**, rather than picking a shape from nothing; a plan with an
   idiosyncratic reason to split further, or bundle two production-gated
   stages that will land in one PR anyway, can still say so.

   **Do not skip the deploy question per stage to shortcut straight to a
   guess.** The grouping is only as trustworthy as that per-stage answer;
   inferring "probably needs a deploy" from a stage's description is exactly
   the kind of derivation Phase 2 exists to replace with an asked answer.
6. **Build-order values.** Ask for what `plans` will need and do not invent
   any of them: position, `Workable?`, `Blocked by`, priority, effort. For a
   backlog-entry plan, also confirm which values carry over from the existing
   row versus which change now that it is being sequenced.
7. **Present everything, then stop.** Design, the stage table with exits and
   estimates, the chunking answer, the resulting issue set (titles, which
   stages each covers, `blockedBy` relationships, estimates), and the exact
   `plans` row. This is the one approval stop this skill has. Do not write
   anything, and do not call `plans` or create a project or issue, until the
   user approves.

## Phase 3 — Write, in this order

Only after approval, and only what was approved.

1. **`## Design` and `## Stages`** into the plan document, additive — edit
   nothing already there.
2. **The build-order row, via `plans`.**
   - **Backlog entry:** operation 1, moving the existing row from backlog to
     build order.
   - **Fresh-draft entry:** operation 6, inserting the new row — no source row
     exists to move. Let operation 6 run its own preflight; do not run
     `tests/test_planning_docs.py` yourself first, the same reason
     `plan-draft` does not duplicate it before a backlog insert.
3. **The Linear project**, only if Phase 1 found none — named `Plan NNN —
   Title`.
4. **The issue set**, matching the approved grouping from Phase 2 exactly, one
   issue per group in order-table order:
   - **a single-stage group (a deploy-requiring stage, or an adjustment that
     split one out on its own)** — title `Plan NNN Stage <Letter>: <outcome,
     imperative>`. `## Exit` is that stage's exit condition, taken verbatim
     from the interview, never recomposed at ticket time.
   - **a multi-stage group (a bundled run of locally-verified stages)** —
     title `Plan NNN Stages <letters>: <the group's outcome>` — the shape
     `CAR-70` grew into by hand, letters appended as stages are added. `##
     Exit` becomes one `### Stage X` subsection per covered stage, each with
     that stage's own checklist, exactly as this plan's own tracking issue
     reads today.

   Every issue's `blockedBy` names its immediate predecessor **issue** in
   order-table order, not its predecessor stage — a bundled issue's internal
   stages carry no `blockedBy` among themselves. `save_issue` takes
   `blockedBy` directly, so the chain needs no GraphQL.

   Every issue otherwise follows `ticket-now`'s six-field shape:
   `## Outcome`, `## Canonical plan` (this plan, the stage or stages it
   covers), `## Why now`, `## Exit`, `## Blocked by`, `## Evidence
   destination` (`Plan NNN Record`).
5. **Estimates**, per issue — the sum of its covered stages' estimates for a
   bundled issue, the stage's own estimate for a one-per-stage issue.
6. **Status `Backlog` on every created issue, and no cycle on any of them** —
   `fill-cycle` is the only thing that spends the cycle budget, and none of
   this work has been through seeding yet. `blockedBy` communicates sequencing
   on its own; a status of `Ready` would claim it is already pullable, which
   is a decision this skill does not make.
7. **Always pass `assignee: "me"` to every `save_issue` call.** Left to
   Linear's own defaults this has been inconsistent — issues created without
   it have sometimes landed unassigned. Set it explicitly on every issue this
   skill creates, the same rule `ticket-now` follows.
8. **Exactly one issue is unblocked at creation** — whichever one contains the
   order table's `next` stage. Every other issue this skill creates carries at
   least one `blockedBy`.

If any step fails, stop and report — do not carry on to the next. A plan
document with `## Stages` written and no build-order row is a contradiction the
next reader has to untangle.

## The workspace

Verified 2026-09-03; re-read rather than trusting these values if anything
disagrees.

- Team **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Estimation is **Fibonacci**, enabled 2026-08-25. Never derive an estimate
  from a plan's effort label — see Phase 2, step 4.
- Projects are named `Plan NNN — Title`, one per plan. Created here only when
  Phase 1 finds none.
- New issues are created `Backlog`, with no cycle. `fill-cycle` seeds them into
  one later.

## After writing

Report:

- the plan document's `## Design` and `## Stages`, and where they were added;
- which `plans` operation ran — 1 or 6 — and the resulting row;
- whether a project was created or reused, and its name;
- the full issue set: identifier, title, which stage(s) it covers, its
  estimate, its `blockedBy`, and which one was left unblocked;
- which values were supplied by the user and which were approved from a
  proposal, naming the source of each;
- anything left outstanding.

## What this skill must never do

- **write before the approval stop.** Design, stages, the `plans` row, the
  project, and every issue all wait for it together.
- **skip the per-stage deploy question, or infer its answer from a stage's
  description.** The grouping is only as trustworthy as that asked answer, and
  it is mandatory for every stage, every time.
- **derive an estimate from a plan's effort label**, or invent a position,
  priority, `Blocked by`, or a grouping adjustment the user did not supply or
  approve.
- **edit anything already in the plan document.** `## Design` and `## Stages`
  are additive only.
- **write a `docs/PLANS.md` row directly.** Always through `plans`, and always
  the operation that matches the actual entry point.
- **create a duplicate Linear project** for a plan that already has one from
  earlier `ticket-now` work.
- **assign a cycle to any issue it creates**, or mark one `Ready`. That is
  `fill-cycle`'s decision, made later, from the backlog this skill fills.
- **run itself twice on one plan.** A document that already carries
  `## Design` or `## Stages` has already been started; that is Phase 1's stop,
  not a case to proceed past.
- **leave `assignee` unset.** Pass `"me"` on every issue this skill creates.
