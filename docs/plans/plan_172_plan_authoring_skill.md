# Plan 172: The Plan-Authoring Skill, and the Two Sentences Every Plan Owes

## Status

**Build order, ahead of [Plan 138](plan_138_public_surface_refresh.md).** Priority
**80**, effort **S** — one skill, two sections, one generator change and one
assertion, with no production surface and no migration.

**80 rather than the 72 first proposed, and the rubric is what moved it.** 72 put
this in the 55-74 *medium* band, "strategic platform work with no immediate
incident". It belongs in **75-89 — high**, which reads "removes a known defect,
unlocks another plan, or closes a time-sensitive public/operational gap": this
does the first two. The defect is measured rather than asserted, and the plan it
unlocks is Plan 138 Stage 9.

**The score is not what puts it ahead of Plan 138, whose score is higher.** The
index's own rule is that "the build order is authoritative when scores are
close: it accounts for dependencies". Plan 138 Stage 9 cannot be *finished* until
the published window carries the section this plan creates — see
[the intersection below](#plan-138--the-public-surface-refresh).

Raised 2026-09-02 out of [Plan 138](plan_138_public_surface_refresh.md) Stage 1d,
from a defect measured rather than suspected. See [the measurement](#the-measurement).

This plan is a sibling of [Plan 146](plan_146_planning_system.md), which owns the
`plans`, `close-out` and `plan-week` skills and is in **closeout** with a gate
dated 2026-09-14. It is deliberately not a Plan 146 stage: adding one would
reopen a plan whose exit condition is a four-week observation already running,
and the cost of reopening it is the observation, not the edit.

## The defect

**The public landing page republishes an internal work pointer, and it churns
because churning is that pointer's job.**

Plan 138 Stage 1d projects `docs/PLANS.md`'s **Default build order** onto the
public page. For each of the first four rows it publishes the plan's title and
the **Next executable slice** cell, as the item's `summary`. That cell is the
index's answer to "what is the next thing anyone could pick up", so it is
rewritten every time a stage lands — which is exactly as often as the work moves.

### The measurement

Measured 2026-09-02 by replaying `master`'s first-parent line over the previous
60 days: 96 commits touch `docs/PLANS.md`, 79 of them comparable against a
predecessor. For each, the published top four was reconstructed as the generator
builds it — the first four rows of the build order, `MAX_ITEMS = 4`.

| Outcome | Commits |
|---|---:|
| Published copy unchanged | 20 |
| **Only a slice cell was rewritten** | **35** |
| **Which plans are in the top four changed** | **24** |
| **Published copy changed** | **59 of 79 — 75%** |

Slice-cell rewrites, by plan: 138 nine times, 162 eight, 142 six, 136 four, 140
three, 145 three, 141 and 144 twice, and 147, 134 and 161 once each. **Plan 138's
own public sentence was rewritten roughly every six days.**

**The 35 and the 24 are different problems and only the first is this plan's.**
A membership change means the top four genuinely reordered; the public list is
supposed to move then, and a reader is better served by it moving. What this plan
removes is the 35 — a public sentence rewritten because a stage landed, when
nothing a public reader cares about changed.

### The register is the real defect, and the rate is its symptom

The same generator already publishes two lists in two different voices, from two
different sources. Measured against the artifact on 2026-09-02:

> **Completed** — *"A deploy could wait forever on a signal that was never sent,
> and looked like it was working while it hung. The signal is now recorded
> correctly, and a wait that cannot finish fails with the reason named instead of
> hanging."*

> **Planned** — *"Stage 8 (CAR-52) — the Layer 2 suite that asserts nothing:
> `tests/integration/sql/test_dashboard_queries.py` is 25 tests and 0 assertions,
> so a renamed mart column passes green and `KeyError`s in production."*

The first is written for a stranger. The second is a work instruction carrying a
file path, a ticket identifier and a stage number — close to what
[`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md) §4 bars from the feed, and reaching
the page only because §4's rule about *what may be shown* never said the
**source** had to be public copy in the first place.

**The completed side is in the right voice because Stage 1d already built the
mechanism for it.** Its own words: *"A plan says how it wants to be described in
public, and extraction is the fallback."* That is the `## Public summary` section,
preferred over the archive cell, with the extraction kept as a fallback and every
plan it had to fall back for named as a shrinking worklist. **This plan is that
same mechanism, applied to the other list, plus the thing Stage 1d did not need
and this does: something that writes the section when the plan is created.**

## Why a skill, and not a template or a checklist

The completed-side section works because the `close-out` skill writes it at the
one moment a person is already thinking about what the plan accomplished. There
is no equivalent moment for the planned side, because **nothing in this
repository currently writes a plan.** Plans are authored by hand into
`docs/plans/`, and `docs/PLANS.md` is edited afterwards by the `plans` skill,
which is explicit that it *"writes state and never prose"* and *"does not author
one mid-transition."*

So the gap is real and it is upstream of the public page: there is a skill for
moving a plan between tables, a skill for closing one out, a skill for filing its
issues and a skill for recapping its commits — and none for opening one. A
section that only a convention asks for is a section 94 of 98 plan documents do
not have, which is the measured state today.

**Writing this document demonstrated the gap it describes, on the first
attempt.** The file was created and `tests/test_planning_docs.py` immediately
failed: `test_every_plan_document_appears_in_a_table` requires a plan document to
be claimed by a row before it may exist at all. So opening a plan is already a
two-artifact, two-owner operation — the document is prose, the row is state
belonging to the `plans` skill, and the row needs a priority, an effort and a
trigger that must be decided outside either of them. Nothing sequences that
today; the test is the only thing that notices, and it notices *after* the
document is written rather than guiding its creation. **That sequence is the
skill's actual job**, and it sharpens Q4 below from a boundary question into a
concrete one: the skill authors the prose and must hand off to `plans` for the
row rather than writing it.

## Design

### Two sections, one at each end of the plan's life

| Section | Written | Tense | Answers |
|---|---|---|---|
| **A** — construction | When the plan document is created | Present | *What is this plan, and why does it exist?* |
| **B** — closeout | When the archive row is written | Past | *What was this plan, and what did it accomplish?* |

**B already exists.** It is today's `## Public summary`, written by the
`close-out` skill, present in 4 of 98 plan documents — all four of them completed
plans. This plan does not invent B; it renames it if the naming question below is
answered that way, and updates `close-out` to say plainly that B is *what
happened*, not *what it was for*.

A plan in the build order publishes A. A plan in the archive publishes B. **No
plan publishes both**, because no plan is in both tables — `PLANS.md`'s **Current
closeout** table is deliberately not an input to the generator, so a plan whose
work has landed but whose evidence is pending appears in neither list. That is
already the operational form of §4's rule, and it means the two sections never
compete for the same slot.

### What "not edited during" means, stated precisely

The value of A is that it does not move when a stage lands. The rule is therefore
about **what may cause an edit**, not about freezing the text:

> **A is not edited because the work progressed.** A stage landing, a stage being
> added, a slice being re-pointed, a ticket being filed or closed — none of these
> touch A. A is edited only when the plan's *purpose* changes, which is a
> different and much rarer event, and when it does happen A **must** be edited or
> it publishes a claim the plan has stopped trying to meet.

**An absolute freeze would be wrong, and this repository has the counter-examples.**
`docs/plans/plan_121_staging_environment.md` is titled *Staging Environment* and
its scope has since broadened past that. Four plans sit in the **Superseded**
table — 73, 106, 103 and 107 — *"replaced, not delivered"*, which is a plan
ceasing to be what it said it was. A rule of "never edit" would have kept a false
sentence on a public page in every one of those cases, so the rule names the
trigger instead.

### The skill guarantees A for new plans; an assertion guarantees it for published ones

These are different populations and they need different mechanisms:

- **New plans** — the plan-authoring skill writes A as part of creating the
  document. Nothing else has to remember.
- **The 94 existing plan documents without A** — a skill cannot reach backwards,
  and a build failure demanding 94 sections would be a migration nobody finishes.
  Stage 1d's answer applies unchanged: **fall back to the slice cell and name the
  worklist.** The page degrades to exactly today's behaviour for a plan with no A.
- **Plans that are actually published** — only the first four build-order rows
  reach the page, so the worklist that matters is never 94. It is **four**, plus
  each new entrant to the top four. That is small enough to assert, and
  `tests/test_planning_docs.py` already parses these tables and is where the
  assertion belongs.

**The assertion is the part that bites, and it is bounded by construction.** A
skill instruction is advice; a test that fails when a plan reaches the top four
without an A is the thing that would actually have caught the state this plan was
raised from. None of the current top four — 162, 134, 138, 164 — has one.

### Open questions — recorded here, not answered

Following [`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md)'s convention: answering
these by omission is how the defect above was produced in the first place.

| # | Question | Settled by |
|---|---|---|
| **Q1** | **What are the two headings called?** `## Public summary` is in the tree, in four documents and in one generator constant, and it is ambiguous once there are two. Candidates: keep `## Public summary` for B and add `## What this plan is for` for A; or rename both to name their end explicitly. The names should make the "not edited during" rule read as obvious rather than as a rule to remember | Stage 0 |
| **Q2** | **Does A have a length cap, and is it the same one?** B is capped at `MAX_SUMMARY_CHARS = 320`, and that cap exists to make an *unauthored* archive cell fail loudly rather than push a paragraph onto the page. A is authored by construction, so the cap is doing different work and may want a different number — or none | Stage 0 |
| **Q3** | **Does the public list still say which stage is next, anywhere?** Dropping the slice from the feed entirely is the smallest change available and kills all 35 rewrites; keeping a coarse progress signal beside a stable sentence is more informative and reintroduces a moving field. This is a question about what the page is *for*, and Plan 138 Stage 9 owns the answer | Plan 138 Stage 9 |
| **Q4** | **Is the plan-authoring skill also where a plan's number, filename and backlog row come from?** Those are `plans`-skill territory, and a skill that writes prose must not start writing state. The seam is real and this plan does not assume where it falls | Stage 1 |

## Stages

### Stage 0 — Name the two sections and their rules

Answer Q1 and Q2. Write the two headings, their tense, their audience and the
"not edited because the work progressed" trigger rule into a single place that
both skills and the generator can point at. **No code, and no plan documents
edited.** The output is the decision.

**Exit:** Q1 and Q2 are answered in this document; the trigger rule is written
once, not restated in three files.

### Stage 1 — The plan-authoring skill

Build the skill that creates a plan document, with A written as part of creating
it rather than as a step someone may skip. It owns prose and never state: it does
not edit `docs/PLANS.md`, choose a priority or an effort, move a row, or file an
issue — those belong to `plans`, `fill-cycle` and `ticket-now`, and Q4 is the
seam this stage settles.

**Exit:**
1. The skill produces a plan document carrying section A.
2. It refuses to produce one without A, rather than emitting a placeholder.
3. It writes no table row, no status marker and no Linear issue.
4. Its boundaries are stated in the same register as the sibling skills'.

### Stage 2 — Teach `close-out` that B is what happened

Update the `close-out` skill so B is written as *what the plan accomplished*
rather than *what it set out to do*, and so it is written from evidence in hand
at closeout rather than from A. Where A exists, closeout is the moment the voice
changes from present to past — and it is a rewrite, not an edit of A.

**Exit:** the skill says which section it writes, in which tense, from what
evidence; and it says explicitly that it does not touch A.

### Stage 3 — The assertion

Add to `tests/test_planning_docs.py`: every plan in the build order's published
window carries section A. The window is the generator's own `MAX_ITEMS`, read
rather than restated, so the test cannot drift from what is actually published.

**Exit:**
1. The assertion fails when a plan reaches the top four without A.
2. It is verified by mutation, not by passing once — remove an A, see it fail.
3. The four plans currently in the window have an A, or the assertion lands with
   a named, dated waiver and this plan's success criteria are not claimed met.

## Files

| Path | Change |
|---|---|
| `.claude/skills/<name>/SKILL.md` | New — the plan-authoring skill |
| `.claude/skills/close-out/SKILL.md` | Stage 2 — B is what happened |
| `docs/plans/plan_138_public_surface_refresh.md` | Stage 9 is Plan 138's, not this plan's |
| `tests/test_planning_docs.py` | Stage 3 — the published-window assertion |
| `docs/plans/plan_*.md` | Section A, four documents, as they enter the window |

## Out of scope

- **Backfilling A across 98 plan documents.** The fallback exists so this is
  never necessary, and a migration nobody finishes is worse than a fallback that
  works.
- **The generator change itself.** Reading A for planned rows is Plan 138 Stage 9;
  this plan supplies the field and the thing that writes it.
- **The 24 membership changes.** A stable sentence per plan does not stop the top
  four reordering, and it should not. See [the measurement](#the-measurement).
- **Choosing this plan's own priority, effort or build-order position.** The
  `plans` skill owns those.

## Success criteria

1. A plan created through the skill carries A without anyone remembering to ask.
2. `close-out` writes B in the past tense, from evidence, and leaves A alone.
3. Every plan in the published window carries A, asserted rather than reviewed.
4. **The measurement is re-run and the slice-only figure has fallen.** 35 of 79
   is the before-number; the after-number is taken the same way, over a window of
   comparable length, and recorded here. A plan about churn that never re-measures
   its own churn is the failure mode this document is built to avoid.

## Intersections

### Plan 138 — the public surface refresh

Supplies the defect and consumes the output. Plan 138 Stage 9 changes the
generator to prefer A for planned rows; this plan makes A exist.

**This plan blocks Stage 9's completion, which is why it sits ahead of Plan 138
in the build order.** Stage 9's *code* may land first and is harmless if it does
— its fallback is today's behaviour. But Stage 9's exit flips
[`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md) §4 and the `plans` skill to say the
build order's slice cell is no longer published copy, and until the published
window carries A that sentence is false: the fallback means the slice cell is
still what reaches the page. Plan 138 would be publishing a rule about its own
surface that its own surface contradicts.

**This plan's own row demonstrates the point on the day it is filed.** Entering
the build order at position 3 puts it inside the published window, so its
*Next executable slice* cell becomes public copy immediately — the exact
mechanism it exists to retire — and it displaces Plan 164 out of the window,
which is a membership change of the kind [the measurement](#the-measurement)
counts as legitimate.

### Plan 146 — the planning system

Owns `plans`, `close-out` and `plan-week`, and owns the ruling this plan inherits:
*"a public-copy column in it would be a change to someone else's table; a plan's
own document is the plan's to write in."* That is why A is a section in the plan
document and not a tenth column in the build order. Plan 146 is in closeout; this
plan's Stage 2 edits a skill Plan 146 built, which is a change to its artifact
and not to its scope.

### Plan 149 — the Linear execution layer

`ticket-now` and `fill-cycle` create issues from plans. A plan created by this
skill is one those can already read; nothing here changes the issue shape. The
seam in Q4 is the same seam Plan 149 already draws between prose and state.
