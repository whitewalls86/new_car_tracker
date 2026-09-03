# Plan 172: A Plan Document Answers Four Questions and Is Structured for None of Them

## What this plan is for

**The planning documents** — Every plan in this project has a written document,
and those documents have quietly become three things at once: an argument, a work
list, and a dump of everything that happened. This plan gives each of those a
place, so a plan can be read for whether it is worth building, or for what to do
next, without wading through the other two.

## The case

Raised 2026-09-02 out of [Plan 138](plan_138_public_surface_refresh.md) Stage 1d
as a defect in one field on one public page — the build order's **Next executable
slice** cell, republished to the landing page and rewritten every time a stage
landed. Rescoped the same day, once [the measurement](#the-measurement) showed
the churning field was a symptom.

**This is the other half of [Plan 146](plan_146_planning_system.md).** Plan 146
rebuilt the *index* around one rule — every plan sits in exactly one table, and
every row carries the condition that removes it — and enforced it with
`tests/test_planning_docs.py`. It never reached the documents the index points
at. Those are still written the way they were before 146 existed.

It stays a sibling plan rather than becoming a 146 stage. Plan 146 is in closeout
with a gate dated 2026-09-14 whose exit condition is a four-week observation
already running; reopening it would cost the observation, not the edit.

**This document states no status of its own.** That is
[the contract it proposes](#the-index-owns-state-the-document-owns-content),
applied to itself: [`docs/PLANS.md`](../PLANS.md) owns which table this plan is
in, and its priority, effort and position. The effort recorded when it was
filed — **S** — was sized against a much smaller scope and no longer holds.

### The index answers a question its documents do not

`PLANS.md` states that **"if this index and a plan document disagree, the plan
document wins."** The index is therefore the derived artifact and the document is
the authority — and the document is the one with no structure.

A plan sits in one of four live states, and each state is a different question:

| State | The row carries | The document must answer |
|---|---|---|
| backlog | a **Trigger** | *Should I build this?* |
| build order | **Next executable slice**, **Workable?**, **Blocked by** | *What am I building today?* |
| closeout | a **Lands** date and a **Gate** | *What do I need to check, and when?* |
| archive | a **Description** and a date | *What did I build, how, and what did it take?* |

**The row and the document are answering the same question at two grains.** Today
only the row answers it. The document holds all four answers at once, in one
heading namespace, in the order they were written.

There is a fifth state, **superseded**, which is not a step in that sequence but
a way out of it. It is [handled separately below](#superseded-is-an-exit-not-a-step).

### The measurement

Taken 2026-09-02 across the 98 numbered plan documents in `docs/plans/`.

**A stage is not a unit of anything.** 291 sections across 57 documents open with
`Stage`, `Gate`, `Phase`, `Track`, `Step` or `Wave`:

| | lines |
|---|---:|
| minimum | 3 |
| median | **33** |
| 75th percentile | 91 |
| 90th percentile | 230 |
| maximum | **2,190** |

Mean 96, standard deviation 191. The smallest is three lines (`Gate for Plan
90`); the largest is longer than the median whole document. A point estimate
cannot calibrate against a unit with no grain.

**Only 4% of them say when they are done.** 14 of 291 declare an exit condition.
Meanwhile `ticket-now`'s contract instructs the reader to *derive* `Outcome`,
`Exit` and `Evidence destination` from the plan document. Both `ticket-now` and
`fill-cycle` are reading a field that is almost never there, so it is invented at
ticket-writing time — the worst available moment, because whoever writes it is
about to do the work and already has an answer in mind.

**Six vocabularies, and four documents use two of them at once.** 203 `Stage N`,
58 `Phase N`, 25 `Gate A`, 17 `Track A`, 17 `Step N`, 16 `Stage Na`, 9 `Stage A`.
Plans 138, 145 and 147 use `Stage` and `Gate` together; Plan 137 uses `Stage` and
`Wave`.

**Stage identifiers decay into letters because they are positions.** Plan 138 was
filed with 7 stages and has 22. Its identifiers now read 0, 0b, 1, 1b, 1c, 1e,
1f, 1h, 2, 3, 3b, 3c, 3d, 4, 5, 6, 7, 8, 9. Plan 147 went 5 → 9, Plan 162 0 → 9,
Plan 149 4 → 5. The letters are not a taxonomy. They are what happens when one
identifier is simultaneously a **name** — frozen the moment it appears in
`CAR-52`, a PR title, a commit message and the published slice cell — and a
**sort key**, which is the thing you need to change when work is discovered
mid-plan. Naming wins, so insertion degrades into suffixes.

**Evidence has no home, so it colonizes the stage namespace.** Evidence-shaped
headings appear at four depths: `##` 25 times, `###` 77, `####` 41, `#####` 4.
**Ten of Plan 138's twenty-two `Stage`/`Gate` headings are evidence headings**,
and `Stage 2 evidence` appears twice. Plan 162's `##`-level `Stage` headings run
7, 8, 12, 11, 6b, 6c, 5b, 10, 3 — the order they were written, not the order they
will be worked.

**So the working documents grow without bound.** Lines at the creating commit
against today:

| Plan | Filed | Today | Growth | Commits |
|---|---:|---:|---:|---:|
| 162 | 136 | **2,965** | **21.8×** | 41 |
| 138 | 534 | 2,986 | 5.6× | 36 |
| 145 | 520 | 2,485 | 4.8× | 35 |
| 161 | 232 | 774 | 3.3× | 12 |
| 147 | 289 | 920 | 3.2× | 13 |
| 158 | 301 | 721 | 2.4× | 7 |

The median plan document is 269 lines. The two currently in the published window
are 2,986 and 2,965 — eleven times the median — and evidence is 35–60% of every
worked document.

### The lifecycle already exists in the data; nothing enforces it

Grouped by the table each plan currently sits in:

| State | n | median lines | median stage headings |
|---|---:|---:|---:|
| backlog | 16 | 155 | **0** |
| build order | 22 | 285 | 5 |
| closeout | 6 | **637** | 6 |
| archived | 41 | 298 | 0 |
| superseded | 13 | 122 | 0 |

The shape is right: a backlog plan is short and has no stages, a build-order plan
has stages, a closeout plan is the largest because the record has arrived.

**The backlog median hides a split, and it is a recent regression.** The six
short backlog plans — 64, 66, 70, 79, 108 and 122, at 17 to 77 lines — all
predate August. Every backlog plan written since then arrives fully designed: 165
at 205 lines with 3 stages, 166 at 189/3, 167 at 167/3, 169 at 394/5, 171 at
324/4.

Plan 171 sits in the backlog at 324 lines with four stages, answering *what am I
building today* from a table whose question is *should I build this*. That work
is either re-done at the start pass or ticketed without ever having been reviewed
as a commitment.

### Why a set of skills, and not a template

Nothing in this repository writes a plan. `plans` moves rows and is explicit that
it *"writes state and never prose"*; `close-out` writes evidence at the end;
`fill-cycle` and `ticket-now` read plans into Linear. There is a skill for every
operation on a plan except the ones that create and shape one.

A template would be advice. The measurements above are what advice produces: the
convention for an exit condition exists and is followed 4% of the time.

## Design

### The document is a ratchet, and each state adds one section

Nothing is rewritten when a plan advances. A plan in the archive still has to
answer why it was worth building, so the earlier answers stay.

| Section | Added at | Answers | Written by | Attention |
|---|---|---|---|---|
| `## What this plan is for` | draft | *what is this, to a stranger* | `plan-draft` | none |
| `## The case` | draft | **Should I build this?** | `plan-draft` | skim |
| `## Design`, `## Stages` | start | **What am I building today?** | `plan-start` | **all of it** |
| `## The checks` | landing in closeout | **What do I need to check, and when?** | `stage-close` | some |
| `## Record` | as stages close | **What did I build, how, what did it take?** | `note-evidence`, `stage-close` | per entry |
| `## Public summary` | archive | *what did it do, to a stranger* | `close-out` | one sentence |

**`## The case` is mandatory and its shape is not.** It has to exist and it has
to say why the plan is worth building; it is often prose carried over from the
work that produced the idea. Requiring a measurement would be wrong — a plan
drafted from an idea frequently has none yet, and demanding one would produce
invented numbers, which is the failure this whole document is written against.

**The split between the first two rows and the third is the whole point.** A
draft is generated and skimmed; it costs the reader one decision. The start pass
is a commitment and is argued line by line. Today both happen in one act, at
draft time, which is why recent backlog plans arrive with stages nobody has
agreed to.

**The split is between two pieces of work, not two sittings.** Drafting and
starting are frequently one thought — this plan was raised from a measurement and
went to build-order position 3 the same day, and so did Plans 168 and 170. What
must never happen is *skipping* the start: stages written without the commitment
conversation.

**But it is always a separate invocation, and that is the trigger for the review
pass.** `plan-draft` never runs `plan-start` for you; it names it and stops. A
review that follows automatically from drafting is one nobody can tell happened,
and the whole value of the split is that the high-attention half is visible.

### The index owns state; the document owns content

**A plan document under this contract carries no status marker.** No `## Status`
section, no `**Status:**` line. Which table a plan is in, its priority, its
effort and its position are `PLANS.md`'s, and repeating them in the document
creates the one disagreement the index cannot arbitrate.

`PLANS.md`'s rule — *"if this index and a plan document disagree, the plan
document wins"* — is stated too broadly today. It becomes: **the index owns
state, the document owns content.** A document that claims no state cannot
contradict the index about state, and the rule keeps its force everywhere it was
actually needed.

What `## Status` holds today is three different things, and only the first is
duplication:

1. the state word, which churns on every transition and belongs to the index;
2. the plan's **origin** — when it was raised and out of what — which never
   changes and exists nowhere else;
3. an **argument about the plan's nature or position** — why it is its own plan,
   why it sits above a higher-scored row.

The second and third are *should I build this*, so they move to `## The case`.
The first goes away.

**`plans` gets simpler rather than harder.** It maintains status markers today
across three forms in 79 documents, and already has a *"neither — **nothing.**
Report it"* branch covering 17 documents that have none. New-contract documents
join that branch: the existing gap becomes the intended path, and no new code
appears.

### Where a drafted plan lands, and why capacity is not a trigger

Measured 2026-09-02: **15 of 22 build-order rows wait on nothing but capacity and
position** — their `Blocked by` cell is `--`. And **not one of the sixteen backlog
triggers is about capacity.** Every one names an event: *"Plan 152 lands"*,
*"Plan 125 is complete"*, *"any new public mutation surface or auth-boundary
change"*, *"IP flagging or single-host throughput becomes the measured
constraint"*, *"Plan 136 Stage 3's verdict lands (2026-09-17)"*.

So the two tables already mean different things and neither is a waiting room:

- **The build order is the priority queue.** A plan whose only obstacle is your
  time belongs in it, at the position its priority earns. That is what those 15
  rows are, and it is correct.
- **The backlog holds plans whose answer to *should I build this* is not yet**,
  with the thing that would change the answer named as the trigger.

**"Fewer than X plans ahead of me" is not a trigger**, and a contract that
required one would turn the backlog into a second ordered queue and put the same
sequencing decision in two tables.

Therefore the draft's destination is **a question, not a default** — and it is
the *should I build this* question itself, asked once at the end of the draft:

| Answer | Lands in | Costs |
|---|---|---|
| yes, sequence it | build order — `plan-start` is offered next, and is its own invocation | position, priority, effort, and every stage's exit |
| yes, but not until X | backlog | one sentence: X |
| I don't know yet | backlog | one sentence: what would tell you |

**The backlog row's trigger is also the trigger for the review pass.** A backlog
plan has been drafted and not reviewed; when its trigger fires, what happens is
`plan-start`. So the trigger answers two questions with one sentence — *what
would make this worth building*, and *what should make me sit down and read it
properly* — and those are the same question.

**The row is written once, for the state the plan actually lands in.** A plan
going straight to the build order never gets a backlog row; writing one and
removing it minutes later would put a transition in the record that never
happened, which is the history
[`plan_state_reconciliation.md`](../planning/plan_state_reconciliation.md) exists
to keep honest.

### A stage is lettered, the order is numbered, and the letter means nothing

This is [Plan 146](plan_146_planning_system.md)'s rule applied one level down.
`PLANS.md` already says: *"Cross-references use plan numbers, never row
positions. Positions shift when a row is inserted; plan numbers do not."* The
index has an `Order` column that renumbers freely and a `Plan` column that never
does. A plan document has one identifier doing both jobs, so insertion has
nowhere to go and produces `1b`.

Two wrong answers were tried on the way to the right one, and both are worth
recording because each is locally reasonable:

- **Two numbers** — keep `Stage 8` as a name, put the order in a table beside it.
  This puts two integer namespaces in one table, so *"let's do stage 2"* means
  either the second row or Stage 2, which are different stages. The original
  defect wearing a hat.
- **Slugs** — `draft`, `assertion`, `plans-open`. Sayable and self-describing,
  and that is the flaw: **a name that encodes content goes stale exactly the way
  a name that encodes position does.** A stage called `assertion` that turns into
  something else is a lie in every ticket that already named it. Slugs also do
  not transfer between plans, invite near-duplicates, and are long.

**An identifier must be opaque.** It should carry nothing that can later become
false — not a position, not a description. So:

- **A stage is identified by a letter**, allocated in the order stages are
  thought of: A, B, C. Allocated once, never reused, never renamed. Work
  discovered mid-plan becomes Stage H and goes wherever the order puts it.
- **The order of work is the numbered `Order` column** in a table at the top of
  `## Stages`, rewritten freely, because nothing outside the document points at a
  position.
- **One vocabulary: `Stage`.** `Gate`, `Phase`, `Track`, `Step` and `Wave` are
  retired for new plans.

**Letters out of order are the point, not a defect.** An `Order` column reading
1, 2, 3 beside stages A, H, B says plainly that H was added later and belongs
early — and it says it without a single cell being renumbered or a single ticket
being wrong.

**This is not a new convention; it is the one the repository keeps reaching
for.** [Plan 139](plan_139_test_suite_maintenance.md) already runs Stage A
through Stage H, and `PLANS.md` already carries two build-order rows for its
Stage C and Stage D. The corpus reaches for letters every time it wants an
identifier that is not a position — 25 `Gate A`-style headings, 17 `Track`, 3
`Wave`. What it did each time was change the *noun* as well, which is where six
vocabularies came from. Keeping `Stage` and taking the letters merges the two
instincts and drops nothing.

Past Z, use AA, AB. A plan reaching that has 27 stages and the alphabet running
out is a signal it should have been two plans; it is not a case to engineer for.

#### Nothing already sequenced is converted, and the number is why

**Measured 2026-09-02: 695 stage references across the 18 build-order documents
that have any, spanning 138 distinct identifiers**, plus 49 cross-document
references in 15 files — including [`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md)
(16) and [`docs/TESTING.md`](../TESTING.md) (9), which are contracts rather than
plans. Plan 138 alone holds 258 references and 44 identifiers, spanning `Stage`,
`Gate` **and** `Phase`, and carries both `Gate 1d` and `Stage 1d`.

The Linear side is the small half — 30 issue titles, of which 5 are open. The
documents are not, and four facts settled it:

- **A sweep edits evidence prose.** Those 695 references sit inside recorded
  accounts of what happened, in files of 2,986 and 2,965 lines. A wrong regex
  match silently falsifies the record, which is the thing this plan exists to
  protect.
- **Only 5 of the 18 have the defect.** Suffixes appear in 138, 162, 150, 168 and
  170. The other 13 are clean sequential numbers and do not have the problem
  letters solve.
- **Git history is unconvertible either way.** Commit messages name `Stage 6c`
  and cannot be rewritten, so a mapping table is needed regardless — and if the
  mapping table exists, conversion buys much less than it appears to.
- **Converting destroys a signal.** `Plan 138 Stage 3` reads as a document under
  the old convention and `Plan 172 Stage C` as one under this, with no extra
  marker needed. A sweep makes everything letters and that distinction is gone
  permanently.

So: **new plans get letters; sequenced plans keep their numbers.** A plan already
sequenced may be converted if and when it is being rewritten wholesale for other
reasons — as a deliberate act, with the old-to-new mapping recorded in its own
document. Never as a sweep.

Existing documents therefore keep their identifiers, including the ugly ones.
`1h` is a name in `CAR-59` and in merged commits. A name may be ugly; it may not
change.

### The order table, and the words it may hold

**`Order` is numbered and rewritten freely; `Stage` is lettered and never
changes.** Only that first column holds an integer, so the two namespaces cannot
collide.

`State` takes exactly one of five values:

| Value | Means |
|---|---|
| `—` | not started |
| `next` | the one stage that is workable now. Exactly one per plan |
| `blocked` | workable order reached, but something outside the plan is in the way |
| `done` | its issue closed and its `## Record` entry written |
| `canceled` | rejected, superseded or made unnecessary. The letter is not reused |

**In-progress is deliberately absent.** Linear holds that, and a document field
tracking it would need editing twice per stage instead of once. `canceled`
matters because it is how a stage that changed shape is recorded without a letter
ever being reused or renamed.

**State is edited once per stage, monotonically.** That is a ratchet rather than
churn, and it is the difference between this and the `Next executable slice`
cell, which is retyped every time work moves.

It also leaves a door open that this plan does not walk through: once the
document knows which stage is `next`, the index's slice cell is **derivable**
rather than hand-typed, and so is a closeout `Gate`. It is
[out of scope](#out-of-scope) here and named so the shape chosen now does not
foreclose it.

### `## The checks` — what a closeout plan owes

A plan in closeout has deployed and is waiting on evidence. `PLANS.md`'s row
carries a `Lands` date and a one-sentence gate; the document carries the rest,
because a gate sentence in an index holding thirty other rows cannot say what was
deployed, what reading to take, or where the reading goes.

`## The checks` is written when the plan enters closeout and holds, per check:
what was deployed and when, what is being watched, the date the answer is due,
and which `## Record` entry will receive it. The row's `Gate` cell is the
one-line summary of it, and the index remains where the *date* lives so nothing
has to scan documents to find what is due.

### The record has one section, one depth, one shape

- **One `## Record` section, last in the document.** Nothing evidence-shaped
  appears anywhere else — not in a stage section, not at `##`, not at `#####`.
- **One `###` entry per closed stage**, chronological, oldest first. A record
  reads forward.
- **Bulky artifacts go to `docs/evidence/`**, named
  `plan_NNN_stage_X_evidence.md`, with the entry linking them. That directory
  already exists and holds three files; today the rule for reaching it is "this
  one felt too big to inline," and the three names are all different shapes.
- **Therefore the Linear `Evidence destination` field has one answer** —
  `plan_NNN §Record` — instead of varying per issue.

The working read is the top of the document and stays short. The record is at the
bottom and is allowed to be long.

### Superseded is an exit, not a step

Thirteen plans sit in the **Superseded** table — *"replaced, not delivered"* —
and they are not a fifth step in the ratchet. A superseded plan never reaches
`## Record` or `## Public summary`, because there is nothing it accomplished.

It owes one section, `## Superseded`, carrying three things: **the date**, **a
link to the plan or plans that replaced it**, and **why**. That mirrors the
Superseded table's `Superseded by` column and gives the reason a home, since the
column has room for a name and not an argument.

A plan may be superseded from any live state, which is why this is a branch off
the ratchet rather than a position in it.

### The two public sections

Both are public copy on the landing page, and they sit at opposite ends of the
document's life:

| | `## What this plan is for` | `## Public summary` |
|---|---|---|
| Written | at draft | at archive |
| Tense | present | past |
| Answers | what is this, and why does it exist | what did it accomplish |
| Feeds | the **planned** list | the **completed** list |
| Cap | 320 characters | 320 characters |

**`## Public summary` is not renamed.** It exists today in four documents and one
generator constant, and it means the completed one in every current use. Renaming
it to `## What this plan did` would buy symmetry for four documents, one
constant, ~8 fixtures in `tests/scripts/test_build_public_roadmap.py` and an edit
to Plan 138's generator while three of Plan 138's stages are in flight. The
symmetry is worth less than the clean seam.

**No plan publishes both**, because no plan is in both tables — the closeout
table is not an input to the generator.

The rule that keeps the first one stable is written **once**, in the contract
Stage A creates, and pointed at from everywhere else:

> **`## What this plan is for` is not edited because the work progressed.** A
> stage landing, a stage being added, a slice being re-pointed, a ticket being
> filed or closed — none of these touch it. It is edited only when the plan's
> *purpose* changes, which is rarer, and when that happens it **must** be edited
> or it publishes a claim the plan has stopped trying to meet.

An absolute freeze would be wrong and this repository has the counter-examples:
`plan_121_staging_environment.md` has outgrown its title, and four plans sit in
the **Superseded** table having ceased to be what they said they were.

### What the contract binds, and the waiver list that carries the rest

The contract governs every plan written from now on. Reaching backwards is
bounded by two decisions:

**The archive and the superseded table are out.** Fifty-four documents describe
work that is finished or abandoned; rewriting them changes a record to match a
convention invented afterwards, which is the opposite of what a record is for.

**The live tables are in** — backlog, build order and closeout, **44 plans.**
Measured 2026-09-02, **43 of them lack `## What this plan is for`**, and the four
documents that carry `## Public summary` today are all archived, so there is no
overlap to lean on.

**The waiver list is how that gets carried, and the pattern is Plan 162's.**
[Plan 162](plan_162_testing_census_and_restructure.md) runs `ROUTE_WAIVERS`,
`ENCODING_WAIVERS` and `LAYER_2_WAIVERS`, all currently empty, and records that
*"every waiver that remains was opened by Stage 7 itself."* The same shape here:
the assertion lands with 43 named entries and bites immediately on every new
plan, and each plan touched drops off the list. The migration becomes countable
and visible rather than a build failure nobody can land or a rule nobody applies.

The same list carries the six closeout plans that owe `## The checks`, and
[Plan 163](plan_163_documented_code_quality_fixes.md) and
[Plan 117](plan_117_storage_and_adaptive_refresh_roadmap.md), whose shapes do not
fit stages at all — 163 is a bag of independent XS fixes and 117 is an umbrella
over six other plans. **A waiver is better than a plan *kind* here**, because a
kind is a permanent exemption and a waiver is an embarrassment with a name and a
date on it.

**The reading this takes, and the one thing to confirm.** What reaches backwards
is the **public section**, the **checks** for closeout plans, and the waiver list
that tracks them. `## Design` and `## Stages` with an exit each do **not** reach
backwards: applying them to the 22 build-order plans would mean re-running
`plan-start` on every active plan, which is a different and much larger piece of
work than this plan claims.

### One skill per transition

| Skill | Moment | Writes | Never |
|---|---|---|---|
| **`plan-draft`** *(new)* | an idea, mid-work | the plan number and filename; the document's first two sections; asks where it lands; hands a **backlog** row to `plans` | stages, a design, a row it wrote itself, a ticket, `plan-start` |
| **`plan-start`** *(new)* | the plan becomes work | the interview; design, lettered stages each with an exit, the order table; a **build-order** row via `plans`; the Linear project and **one issue per stage, chained `blockedBy` in the order table's order** | a row it wrote itself, a priority, a position, an estimate, a cycle assignment |
| **`note-evidence`** *(new)* | during the work | one `## Record` entry | Linear, `PLANS.md`, a status marker |
| **`stage-close`** *(new)* | one issue done | a `## Record` entry, the cost comment, the order table's `State` cell, Linear `Done` | move a `PLANS.md` row |
| **`close-out`** *(narrowed)* | the **plan** stops being work | the gate, the row move via `plans`, the archive, `## Public summary` | close a stage |

**`plan-start` is the mirror of `close-out`, and naming it that way is what fixes
its scope.** They bracket a plan's working life and they have the same shape:
gather, interview, propose, **stop**, then write across three records — the plan
document, `PLANS.md` via `plans`, and Linear. `close-out` ends with an issue set
to `Done` and a cost recorded; `plan-start` begins with issues created and
estimates set. Neither writes before an approval, and neither decides the thing
that is the user's to decide.

**Writing tickets belongs here specifically because the interview is here.** The
argument against a plan-to-tickets skill was that its source is empty: 4% of
stages state an exit, so `ticket-now` and `fill-cycle` invent the field they are
told to derive. That objection does not survive the interview. An issue written
by `plan-start` carries an exit that was argued sixty seconds earlier, which is
the only moment in a plan's life when that field is trustworthy.

**Every stage gets an issue, chained.** `plan-start` creates one issue per stage
and sets each one's `blockedBy` to its predecessor in the order table, so the
board carries the plan's whole shape and its sequence. Three things follow:

- **The board becomes a true projection.** Plan 149's rule is that Linear
  projects repository planning. A projection of an eight-stage plan that shows
  one issue does not project it; it hides the shape and leaves the sequence in a
  document the board cannot see.
- **The chain and the order table are the same fact, checkable against each
  other.** Exactly one issue per plan is unblocked at any time, and it is the
  stage the order table calls `next`.
- **Nothing has to create the next issue.** Closing one stage unblocks the next
  through a relation Linear already holds, so `stage-close` stays a closing skill
  and never grows an opening half.

**This does not touch Plan 149's cycle budget.** The issues are created in
`Backlog` status with no cycle — the case `ticket-now` already documents — and
`fill-cycle` remains the only thing that puts an issue into a cycle. Creating
work and committing to work stay separate, which is the same separation as
drafting and starting one level down.

**The staleness objection does not survive examination.** Plan 138 went from 7
stages to 22, which looks like an argument against writing 8 tickets up front.
But that growth is the defect this plan removes: stages accreted because nothing
forced them to be thought through, and the interview is what forces it. A stage
that genuinely changes afterwards is `canceled`, on the board and in the order
table, which is a visible correction rather than a sequence only one person can
see.

**`close-out` is being used to close stages because there is nothing else, and
its own text says so** — *"Most closeouts touch the first two and leave the third
alone"*, with the `PLANS.md` consequence *"nothing — this is the common case and
should be proposed without apology."* The common case is carrying the whole
apparatus of a plan-level transition. Splitting it is not new machinery; it is
naming the branch that already exists.

**A draft that stops in the backlog has exactly one approval stop: the trigger.**
The `plans` skill requires the user to supply a backlog row's trigger, and the
trigger is the answer to *should I build this* — so the one thing a low-attention
draft cannot generate is the one thing the row needs. A draft the user follows
with `plan-start` spends far more, and should: that is the commitment.

### `plans` is missing the operation that opens a plan

Its five operations are all **moves** — between states, into closeout, into the
archive, a soak result, a slice pointer. Every one starts by reading a source row
and removing it. **There is no operation for a plan that has no row yet**, which
is why opening a plan is a hand-edit to `PLANS.md` today, and why
`tests/test_planning_docs.py` is the only thing that notices — after the document
is written rather than while it is being created.

So `plans` grows one operation and two refusals:

- **Operation 6, open a plan.** Insert one row into the backlog or the build
  order for a plan that is in no table. Same splice-never-reflow mechanics; a
  build-order insert renumbers below it exactly as operation 1 does. Every value
  still arrives from outside.
- **Refuse a build-order row whose document has no stages with exits.** This is
  what makes the ratchet real: it is the difference between a contract and a
  convention, and it is not authoring — `plans` already refuses to close out a
  plan that still owes code.
- **Refuse a backlog row whose document has stages.** The Plan 171 case.

Neither refusal composes a value. Both are stops, which is the shape that skill
already uses when it is handed something that does not add up.

### The questions this plan settled

Recorded with their answers, following
[`PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md)'s convention of writing the decision
down rather than letting omission make it.

| # | Question | Answer |
|---|---|---|
| **Q1** | Is `## Public summary` renamed for symmetry? | **No.** Keeping it costs nothing and keeps this plan out of Plan 138's generator |
| **Q2** | Does `## The checks` earn its own section? | **Yes.** A gate sentence in the index cannot hold what was deployed, what to watch, and where the reading goes |
| **Q3** | How far back does the contract reach? | The backlog, the build order and closeout — **44 plans, 43 needing the public section.** The archive and superseded are out. `## Design`/`## Stages` do not reach backwards |
| **Q4** | Do sequenced plans get lettered? | **No sweep.** 695 references across 18 documents; only 5 have the defect; git history is unconvertible; converting destroys the old/new signal |
| **Q5** | What about plans that do not fit a stage shape? | A **waiver list**, Plan 162 style — named, dated, shrinking. A kind is a permanent exemption; a waiver is an embarrassment |
| **Q6** | Does the assertion cover the completed window too? | **Yes.** All four pass today, so it is free, and it turns the generator's Gate 1d warning into a failure for published rows |
| **Q7** | Where does `## Status` sit? | **It does not.** The index owns state, the document owns content; origin and framing move to `## The case` |
| **Q8** | What does a superseded plan owe? | `## Superseded` — a date, a link to what replaced it, and why |
| **Q9** | Does `plan-draft` allocate the number and filename? | **Yes**, both. The filename is the one slug that survives, because a rename costs inbound links |
| **Q10** | What is the order table's `State` vocabulary? | `—`, `next`, `blocked`, `done`, `canceled`. In-progress lives in Linear |
| **Q11** | Does `## The case` have a required shape? | **No.** Required to exist, free in form. Demanding a measurement would produce invented ones |
| **Q12** | What is the naming form under `docs/evidence/`? | `plan_NNN_stage_X_evidence.md` |

## Stages

**`Order` is numbered and rewritten freely; `Stage` is lettered and never
changes.** Stage H was the last of the eight thought of and belongs second in the
order. Putting it there moved one row and renamed nothing, and the letters
reading A, H, B, C … are what say so at a glance.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a) | `docs/PLAN_DOCUMENT.md`, the contract | `done` | CAR-70 |
| 2 | [**H**](#stage-h) | `plans` operation 6, and two refusals | `done` | CAR-70 |
| 3 | [**B**](#stage-b) | the `plan-draft` skill | `next` | — |
| 4 | [**C**](#stage-c) | the `plan-start` skill | `—` | — |
| 5 | [**D**](#stage-d) | the assertion and the waiver list | `—` | — |
| 6 | [**E**](#stage-e) | `close-out` split in two | `—` | — |
| 7 | [**F**](#stage-f) | the `note-evidence` skill | `—` | — |
| 8 | [**G**](#stage-g) | the after-numbers | `—` | — |

Stages B and D are what [Plan 138](plan_138_public_surface_refresh.md) Stage 9
waits on; the rest may follow at any pace.

### Stage A

**The plan-document contract.** Write `docs/PLAN_DOCUMENT.md`, a sibling of
[`docs/TESTING.md`](../TESTING.md) and
[`docs/PUBLIC_SURFACE.md`](../PUBLIC_SURFACE.md), which are both contracts of
exactly this shape: the rules, and what asserts them. It states the four
questions and the superseded exit, the section ratchet and its fixed order, the
absence of a status marker, stage lettering and the single vocabulary, the order
table's `State` vocabulary, the record's shape, depth and `docs/evidence/`
naming, and the two public sections with the freeze rule.

Every answer in [the settled-questions table](#the-questions-this-plan-settled)
lands here. No code, and no existing plan document edited.

**Exit:** the contract exists; every rule in it is stated exactly once; the
skills, the generator and the test point at it rather than restating it.

### Stage H

**`plans` grows the operation that opens a plan.** Add operation 6: insert one
row, into the backlog or the build order, for a plan that is in no table. Same
splice-never-reflow mechanics as operation 1, and a build-order insert renumbers
below it in the same way. Add the two refusals. Record that a new-contract
document has no status marker, so the existing *"neither — report it"* branch is
the intended path rather than a gap.

**Exit:** a plan with no row can be opened into either table through `plans`; the
two refusals each fire against a deliberate mutation;
`tests/test_planning_docs.py` and `build_public_roadmap.py --check` pass after an
insert into the published window.

### Stage B

**The `plan-draft` skill.** It turns an idea into a plan document: allocates the
number and filename, writes the first two sections, asks where the plan lands,
and either hands a backlog row to `plans` or names `plan-start` without running
it. It owns prose and never state.

**Exit:**
1. It produces a document carrying `## What this plan is for` and `## The case`.
2. It refuses to produce one without the first, rather than emitting a
   placeholder.
3. It writes no stages and no design — those are `plan-start`'s, whether that
   happens in the same session or months later.
4. It writes no status marker, no table row itself and no Linear issue.
5. If the plan stops in the backlog, its one approval stop is the trigger.
6. A plan going to the build order never gets a backlog row on the way.

### Stage C

**The `plan-start` skill.** The mirror of `close-out`. It puts a plan into the
build order — from the backlog when its trigger fires, or straight from a draft —
through an interview: design, rejected alternatives, lettered stages each with an
exit, and the order table. Then the build-order row via `plans`, and the Linear
project and issue set.

It has `close-out`'s shape, including its approval stop. It hands position,
priority and effort to `plans` and chooses none of them, and it asks for
estimates rather than deriving them — `ticket-now` already records why an
estimate derived from a plan's effort label is wrong.

**Exit:**
1. It adds `## Design` and `## Stages` to an existing document, and edits nothing
   already there.
2. Every stage it writes carries an exit condition, and it refuses to hand a row
   to `plans` while any lacks one.
3. Stage letters are allocated in order of conception, never as a suffix of
   another stage.
4. It creates one issue per stage, each `Exit` taken from the stage just agreed
   rather than composed at ticket time, and each `blockedBy` its predecessor in
   the order table. `save_issue` takes `blockedBy` directly, so the chain needs
   no GraphQL.
5. Exactly one of those issues is unblocked, and it is the order table's `next`.
6. Every issue is created with no cycle, so `fill-cycle` remains the only thing
   that spends the cycle budget.
7. It writes nothing before the approval stop.
8. It runs correctly both ways — on a backlog plan with a row to move, and on a
   fresh draft with no row yet.

### Stage D

**The assertion and the waiver list.** Add to `tests/test_planning_docs.py`:
every plan in the build order's published window carries
`## What this plan is for` within its cap, and every plan in the archive's
published window carries `## Public summary`. Both windows are read from the
generator's `MAX_ITEMS` rather than restated — `tests/test_planning_docs.py`
already does a deferred `from scripts import ...` at line 1150, and the CI job
sets `PYTHONPATH` for exactly that reason.

Then widen to the 44 live plans behind a named, dated waiver list holding the 43
that do not yet carry the section, plus the six closeout plans owing
`## The checks`, plus Plans 163 and 117.

The four plans in the planned window today are **162, 134, 172 and 138**; this
plan's own row displaced 164 out of it on the day it was filed. The four in the
completed window are **161, 139, 147 and 158**, and all four already carry their
section, so that half of the assertion passes on arrival.

**Exit:**
1. The assertion fails when a plan reaches either window without its section.
2. Verified by mutation — remove one, watch it fail — not by passing once.
3. The waiver list is named and dated, and every entry says which plan it covers.
4. The four planned-window plans carry the section, so no waiver covers a plan
   that is actually published.

### Stage E

**`stage-close`, split out of `close-out`.** Move the per-issue half into its own
skill: the `## Record` entry, the cost comment, the public-surface question, the
order table's `State` cell, Linear `Done`. It cannot move a `PLANS.md` row.
`close-out` is narrowed to the plan-level transition and says which skill closes
a stage.

**Exit:** each skill's description names the grain it operates on; neither can
perform the other's write; `close-out` no longer proposes "nothing" as its common
case, because that case is no longer its job.

### Stage F

**The `note-evidence` skill.** It records a measurement while the work is
happening — a soak reading, a production number, a baseline — into `## Record` at
the contract's shape, before it is lost or has to be re-run.

**Exit:** it appends one entry and touches nothing else; it writes no Linear, no
`PLANS.md` and no status marker; a bulky artifact goes to `docs/evidence/` as
`plan_NNN_stage_X_evidence.md` with the entry linking it.

### Stage G

**Re-measure.** Re-run [the measurement](#the-measurement) the same way, over a
window of comparable length, and record the result in `## Record`. The
before-numbers are in this document: 35 slice-only rewrites in 79 comparable
commits, 4% of stages with an exit, evidence at four heading depths, 43 of 44
live plans without a public section.

**Exit:** the after-numbers are taken and recorded, including the ones that did
not move.

## Files

| Path | Change |
|---|---|
| `docs/PLAN_DOCUMENT.md` | New — Stage A |
| `.claude/skills/plans/SKILL.md` | Stage H — operation 6 and two refusals |
| `.claude/skills/plan-draft/SKILL.md` | New — Stage B |
| `.claude/skills/plan-start/SKILL.md` | New — Stage C |
| `tests/test_planning_docs.py` | Stage D — both window assertions and the waiver list |
| `.claude/skills/stage-close/SKILL.md` | New — Stage E |
| `.claude/skills/close-out/SKILL.md` | Narrowed to plan-level, Stage E |
| `.claude/skills/note-evidence/SKILL.md` | New — Stage F |
| `docs/plans/plan_*.md` | `## What this plan is for`, as each waiver is retired |
| `docs/PUBLIC_SURFACE.md` | §4 points at the contract for the two public sections |

## Out of scope

- **Rewriting the archive and the superseded table.** Fifty-four documents
  describe finished or abandoned work; rewriting them to match a convention
  invented afterwards is the opposite of what a record is for.
- **Sweeping the sequenced plans into letters.** 695 references across 18
  documents, inside evidence prose, with git history unconvertible regardless.
- **Backfilling `## Design` and `## Stages` into the 22 build-order plans.** That
  is re-running `plan-start` on every active plan and is a larger piece of work
  than this plan claims.
- **Deriving the index from the document.** Making `Next executable slice` and a
  closeout `Gate` generated rather than typed is the natural consequence of the
  order table, and it is a larger change touching `plans`, the generator and
  [Plan 138](plan_138_public_surface_refresh.md) Stage 9. Named so the shape
  chosen here does not foreclose it.
- **The generator change that reads the new section for planned rows.** That is
  Plan 138 Stage 9; this plan supplies the field and the thing that writes it.
- **Choosing this plan's priority, effort or build-order position.** The `plans`
  skill and its user own those.

## Success criteria

1. A plan created through `plan-draft` carries its public section and its case,
   and carries no stages and no status marker, without anyone remembering to ask.
2. A plan reaches the build order only through a conversation in which every
   stage got an exit condition, and its issue set carries those exits verbatim.
3. Evidence has one section, one depth and one shape, and the Linear
   `Evidence destination` field has one answer.
4. Closing a stage and closing a plan are different operations with different
   skills, and neither can perform the other's write.
5. Both published windows are asserted rather than reviewed, and the waiver list
   is shorter than 43.
6. **Stage G's numbers are taken.** A plan about documents that churn which never
   re-measures its own churn is the failure mode this document exists to avoid.

## Intersections

### Plan 146 — the planning system

The plan this one completes. Plan 146 gave the index a rule and a test; the
documents it indexes got neither. Stage E edits `close-out` and Stage H edits
`plans`, both of which Plan 146 built — changes to its artifacts, not to its
scope. Plan 146 is in closeout with a gate dated 2026-09-14 and is not reopened.

### Plan 138 — the public surface refresh

Supplies the original defect and consumes two of this plan's stages. Plan 138
Stage 9 changes the generator to publish what a plan is *for* rather than which
stage is next; Stages B and D here make that field exist and hold it in the
published window. Stage 9's code may land first and is a no-op until then.

Q1 and Q4 are both answered in ways that keep this plan out of Plan 138's
generator and out of its 258 stage references while three of its stages are in
flight.

### Plan 149 — the Linear execution layer

`ticket-now` and `fill-cycle` derive four of their six issue fields from plan
documents, and [the measurement](#the-measurement) shows the field they most need
is present 4% of the time. This plan changes neither skill; it makes what they
read actually be there.

**It does add a third writer of Linear issues, and the reason it does not move
Plan 149's measure is structural rather than a promise.** Stage 2 measures
"issues added after cycle start" — 62% in Cycle 1 — and that measure counts
**cycle membership, not issue creation**. `plan-start` assigns no cycle, so the
number it could distort is one it cannot reach.

The three do not overlap. `fill-cycle` puts existing issues into a cycle;
`ticket-now` catches work discovered mid-task that has no issue yet; `plan-start`
writes a plan's whole issue set at the moment its stages are agreed. Only the
third has an `Exit` it did not have to invent.

### Plan 162 — the testing census

Supplies the waiver-list pattern this plan adopts wholesale: a named list that
must shrink, currently empty, with every remaining entry traceable to the stage
that opened it. Plan 162 is also one of the two documents whose 187 stage
references argued against a lettering sweep.

## Record

### Stage A

Landed `docs/PLAN_DOCUMENT.md`: the four questions, the section ratchet, the
no-status-marker rule, stage lettering and the single `Stage` vocabulary, the
order table's `State` vocabulary, the record's shape and `docs/evidence/`
naming, and the two public sections with the freeze rule. No code and no
existing plan document were edited. Verified by `tests/test_planning_docs.py`
(35 passed) and `scripts/build_public_roadmap.py --check` on
`car-70-plan-172-stage-a-contract`.

### Stage H

Added `plans` operation 6 — open a plan currently in no table — plus its two
document-shape refusals, to `.claude/skills/plans/SKILL.md`. Operations 1–5
were left unedited apart from the shared preamble and after-every-operation
sections.

Validated in a disposable clone
(`cartracker-plan172-stage-h-forward-test-20260903-1`), never committed:

- **Insert:** a no-row plan document was spliced into build-order position 2,
  inside the published window. `tests/test_planning_docs.py` moved from one
  expected failure (the document present in no table) to 35 passed.
  `build_public_roadmap.py --check` correctly reported staleness after the
  insert, and came back clean after regeneration.
- **Backlog refusal:** a document with a `## Stages` section and a
  `### Stage A` heading, attempted as a backlog insert, was refused before any
  edit to `docs/PLANS.md` — confirmed unmodified afterward.
- **Build-order refusal:** a document whose only stage carried no `Exit:` line,
  attempted as a build-order insert, was refused the same way — confirmed
  unmodified afterward.

Both refusals were exercised by applying the gate text in `SKILL.md` directly
against the test documents, not by a script standing in for it.
