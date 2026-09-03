# Plan 149: Linear as the Weekly Execution Layer

## Status

**CLOSEOUT — Stage 0 complete 2026-08-24.** Priority **72 (medium)**. Effort
**XS plus three one-week cycles of observation**.

This is a bounded workflow experiment, not a migration of the planning system.
The repository remains authoritative throughout. If the experiment does not
reduce planning friction without creating state drift, remove the Linear
workspace projection and keep the repository unchanged.

## Problem

The repository's planning system is strong at durable reasoning and increasingly
awkward as a daily work surface.

- Plan documents own problems, designs, decisions, gates and evidence.
- [`PLANS.md`](../PLANS.md) owns plan state, dependency order and the next
  executable slice.
- Weekly recaps own what happened.

Those are useful separations, but none answers *"what am I actively doing
today, what is waiting on a soak, and what can I pull next without rereading the
whole build table?"* The build order now contains 18 slices across operational
debt, public presentation, lakehouse migration, infrastructure and streaming.
The current week can legitimately move through several of them as higher rows
enter timed observation windows.

The failure mode is not missing plans. It is losing the immediate execution
state inside a comprehensive roadmap:

- an executable slice, PR review and production soak are visually equivalent
  rows in Markdown;
- switching to the next safe item during a 24- or 48-hour soak requires
  reconstructing the dependency boundary;
- recent velocity is visible only after a recap is written;
- a small task can be either a distraction from the build order or the correct
  next item while a higher row is blocked, and the current view makes the
  distinction expensive to recover repeatedly.

A Jira-style replacement would solve visibility by duplicating the entire
planning system. That is worse than the present problem. This plan tests a much
smaller claim: **Linear can be a disposable projection of the next one to two
weeks while the repository remains the system of record.**

## Decision

Use Linear for weekly execution only.

| Fact | Authority |
|---|---|
| What a plan is and why it exists | `docs/plans/plan_NNN_*.md` |
| Plan state, dependencies and build order | `docs/PLANS.md` |
| Current slice, assignee, review and soak status | Linear |
| What actually happened | `docs/recaps/YYYY-MM-DD.md` |

Linear may link to repository facts. It must not become their second owner.

### Mapping

| Repository concept | Linear concept | Rule |
|---|---|---|
| Active plan | Project | Create only when work enters the near-term execution horizon |
| Next executable slice | Issue | One independently finishable outcome with an explicit exit |
| Implementation steps | Sub-issues or checklist | Use sub-issues only when pieces can move independently |
| Monday-to-Sunday work | One-week cycle | A cycle is a commitment view, not a release |
| Dependency | Blocked/blocking relation | The plan document remains authoritative if the relation disagrees |
| PR | GitHub-linked issue | Branch/PR automation may move execution status only |
| Production evidence window | `Soaking` status | Carries the end time and exact gate in the issue |
| Finished slice | `Done` | Does not by itself close, archive or move a plan in `PLANS.md` |

### Workflow

One team: **Cartracker**.

```text
Backlog -> Ready -> In Progress -> In Review -> Soaking -> Done
                                      \-> Canceled
```

- **Backlog:** captured execution work not selected for the current horizon.
- **Ready:** ordered, unblocked and safe to pull.
- **In Progress:** actively changing code, docs or production state.
- **In Review:** a PR or explicit review gate is open.
- **Soaking:** implementation is complete and a time/evidence gate is running.
- **Done:** the issue's exit condition is met and recorded in its plan.
- **Canceled:** the slice was rejected, superseded or made unnecessary. The
  reason belongs in the canonical plan when it changes that plan's design.

Do not create separate statuses for every plan stage, environment or failure
mode. Those are issue fields, labels or plan evidence, not workflow states.

### Issue contract

Every execution issue contains exactly these fields:

1. **Outcome:** one sentence describing the finished state.
2. **Canonical plan:** link to the plan document and stage/section.
3. **Why now:** the build-order position or the higher-row gate that makes this
   safe filler.
4. **Exit:** observable checks that make the issue Done.
5. **Blocked by:** issue relation plus the plan's actual gate in words.
6. **Evidence destination:** the plan section that receives deploy/soak facts.

Do not paste the plan's problem statement, architecture, rejected alternatives,
runbook or historical evidence into Linear. If an issue needs that context, it
links to it.

## Scope

### In scope

- one Linear workspace and one `Cartracker` team;
- the workflow above;
- one-week cycles beginning Monday;
- GitHub integration for PR linking and execution-status automation;
- projects only for plans in the current one-to-two-week horizon;
- a first cycle seeded from the top of the authoritative build order;
- three completed cycles of measured use;
- an export before any decision to keep or remove the system.

### Out of scope

- importing completed plans, old issues, weekly recaps or git history;
- copying the full backlog or all 18 build-order rows into Linear;
- replacing `PLANS.md`, plan documents, the completion archive or recaps;
- allowing Linear status changes to edit repository planning state;
- two-way GitHub Issues synchronization;
- automations that create or reorder plan rows;
- team ceremonies, story-point accounting or sprint-performance targets;
- paying for a Linear tier before the free-tier constraint is actually reached
  or a paid feature is shown to remove measured friction.

## Stage 0 — Issue 1, bootstrap the experiment

Create this as the first Linear issue on **2026-08-25**.

### Title

```text
Plan 149 Stage 0: bootstrap Linear as the execution projection
```

### Description

```markdown
## Outcome

Cartracker has a minimal Linear execution board for the current week without
duplicating repository planning authority.

## Canonical plan

Plan 149: `docs/plans/plan_149_linear_execution_layer.md`, Stage 0.

## Why now

The build order contains multiple executable slices and timed soak gates. A
one-week execution view should make safe context switching visible before the
next operational tranche begins.

## Exit

- [ ] One `Cartracker` team exists.
- [ ] Workflow is Backlog, Ready, In Progress, In Review, Soaking, Done,
      Canceled.
- [ ] One-week cycles begin Monday.
- [ ] GitHub is connected for PR linking; no two-way GitHub Issues sync.
- [ ] Plan 149 is the first project and this is Issue 1.
- [ ] The first-cycle issues are created from Stage 1's approved seed list.
- [ ] Every seeded issue links its canonical plan and states its exit gate.
- [ ] No repository plan state was changed by Linear setup.

## Evidence destination

Record the resulting team, workflow, cycle dates, integration settings and
seeded issue IDs under Plan 149 Stage 0 evidence.
```

Issue 1 is intentionally the bootstrap issue, even though some of its checklist
creates the system around itself. Create the workspace/team, create this issue
immediately, then finish configuration through the issue so the experiment has
an execution record from its first meaningful action.

## Stage 0 evidence

Recorded on **2026-08-24**, ahead of Cycle 1's 2026-08-31 start.

### Workspace and team

- Workspace: `cartracker` (`https://linear.app/cartracker`).
- Team: `Cartracker` (key `CAR`), timezone America/Chicago.

### Workflow

`Backlog → Ready → In Progress → In Review → Soaking → Done → Canceled`.
Soaking sits under Linear's **Started** category, not Completed — a Soaking
issue is not finished and continues to count in cycle progress. Putting it
under Completed would silently falsify the rollover and state-corrections
measures.

### Cycles

One-week cycles beginning Monday, no cooldown, three upcoming visible.
`cycleLockToActive` and `cycleIssueAutoAssignStarted` are both **off**, so
moving an issue to In Progress mid-cycle does not silently add it to the
current cycle — the "issues added after cycle start" measure would be
unreadable otherwise.

Cycle 1 runs **2026-08-25 → 2026-08-31**, six days rather than seven.

Corrected 2026-08-25 from an original 2026-08-31 → 2026-09-07. The workspace
was bootstrapped on Monday 2026-08-24 at 20:49 UTC, and Linear generated the
first cycle from the *following* Monday, so the week in which the board was
actually being used fell outside every cycle. CAR-7's implementation merged as
PR #243 on 2026-08-25 at 05:48 UTC — Cycle 1 work completed before Cycle 1
existed.

Two consequences are recorded rather than fixed:

- **Monday 2026-08-24 is not in any cycle.** `cycleUpdate` refuses a start date
  before today, so the day cannot be recovered in Linear. It is not lost from
  the record: `docs/recaps/` is the authority for what happened and is built
  from git, not from Linear.
- **Cycle 1's measures undercount.** Work finished before the cycle opened, and
  the cycle is a day short. Read Cycle 1 as calibration, not as a baseline
  comparable to Cycles 2 and 3.

Cycles 2 and 3 were shifted back one week to restore the Monday grid, so only
the first cycle is irregular. Shortening Cycle 1's end date was rejected by
`cycleUpdate` but accepted through the Linear UI — the two clients do not
enforce the same constraints.

### GitHub integration

Connected to `whitewalls86/new_car_tracker` (browser-only: OAuth + org
authorization is not scriptable). PR-linking automations set at
`Settings → Teams → Cartracker → Workflows & automations → Pull request and
commit automations`:

| Trigger | Set to |
|---|---|
| On draft PR open | No action |
| On PR or commit open | In Progress |
| On PR review request or activity | In Review |
| On PR ready for merge | No action |
| On PR or commit merge | Soaking |

Corrected 2026-08-25 from a table that recorded neither the real trigger names
nor the real targets. Two things were wrong, and one of them mattered:

- **Merge was set to `Done`, not `Soaking`.** That is the auto-transition this
  plan's own deviation note forbids. It never fired, so no issue was falsely
  completed — but had the integration been working, CAR-7 would have jumped to
  `Done` on 2026-08-25 and skipped its soak entirely. Now set to `Soaking`.
- **`On PR or commit open` is `In Progress`, not the `In Review` the old table
  claimed.** Kept deliberately, against the intuition that an open PR means
  review.

  The trigger fires on **commits as well as PR opens**, and this project
  pushes many commits before a PR exists. The issue is genuinely in progress
  through that window, so `In Review` would claim a review gate that is not
  open — on the first commit of a branch, days early.

  The cost is that opening a PR does not move an issue to `In Review` either.
  Since `On PR review request or activity` will rarely fire for a single
  maintainer who does not request review from himself, **reaching `In Review`
  is normally a manual move.** That is a real gap and may be revisited: if
  Linear ever separates the commit and PR-open triggers, PR-open belongs on
  `In Review`, because opening a PR *is* this project's review gate.

  `On draft PR open` stays `No action` — drafts are not part of the normal
  flow here.

GitHub Issues Sync remains OFF (the no-two-way-sync rule).

**Deviation from Decision → Workflow.** The plan describes PR-merge behavior
conditional on whether the issue requires production evidence. Linear's PR
automations are per-team, not per-issue, so the conditional has no home. The
chosen tradeoff: merge → Soaking unconditionally; no-soak issues are moved to
Done by hand. A false Done is on the forbidden-automation list; a false
Soaking is one manual click.

**No automation has ever fired.** Every status transition on CAR-6, CAR-7,
CAR-10 and CAR-11 through 2026-08-25 was made by hand — six of six. CAR-7 sat
in `In Review` for nine hours after PR #243 merged. PRs *do* attach to issues,
so the integration is connected; only the status automations are inert.

Measured 2026-08-25 on the Plan 141 branch, which carries `car-10` in its name.
Two probes, one positive and one negative:

| Probe | Result |
|---|---|
| PR #244 opened, with no issue identifier in its title or body | **attachment created on CAR-10** |
| CAR-10 set to `Ready`, then a commit pushed to that branch | **no transition** — 9 reads over 3 minutes, `updatedAt` unmoved |

**Branch-name linkage works.** PR #244 attached with the branch name as the only
possible mechanism, so Linear's suggested `gitBranchName` is the convention to
use — the older `feature/plan-NNN-*` style does not establish the relationship.

**The status automations are inert, and the cause is not yet known.** Three
explanations were eliminated rather than confirmed:

- *Not the trigger configuration.* The probes ran **after** the settings above
  were corrected, so the automations were configured correctly and still did
  nothing.
- *Not a missing link.* Attachments form correctly on every issue tested.
- *Not an unconnected GitHub account.* The GitHub identity is connected to the
  Linear profile.

The PR-open trigger could not be tested this round: CAR-10 was already
`In Progress`, which is that trigger's target, so a successful fire would have
been indistinguishable from no fire. The merge trigger remains untested, though
CAR-7 sitting nine hours in `In Review` after PR #243 merged is strong evidence
against it.

**Accepted as manual for now.** Status transitions are performed by hand, which
is what the `ticket-now`, `fill-cycle` and `stage-close` skills already assume —
none of them depends on an automation firing. The cost is that a stalled issue
is invisible until someone looks, which is how CAR-7 sat in the wrong state for
most of a day. If this is revisited, the next unexplored surfaces are the
GitHub App's installation permissions and whether Linear receives the PR
webhook events at all.

This does not block Stage 2. A board whose states are moved by hand still
answers the Stage 2 measures; it only makes "state corrections" a more
prominent one.

#### Corrected 2026-08-29 — the automations do fire, and the missing piece was linkage

Everything above is the record of what was true on 2026-08-25. It is wrong
about the mechanism, and the counter-evidence is exact.

| Event | Time |
|---|---|
| PR #265 merged | 2026-08-28 **04:09:21Z** |
| CAR-21 → `Soaking` | 2026-08-28 **04:09:23Z** |
| PR #266 merged | 2026-08-28 **05:07:10Z** |
| CAR-21 → `Soaking` | 2026-08-28 **05:07:12Z** |

Two seconds, twice, from CAR-21's own state history. **The merge → `Soaking`
automation works.**

CAR-22 is the negative control, on the same plan in the same week: PRs #278 and
#279 were opened and merged for it, and the issue has **zero attachments and no
automated transition**. It sat in `Ready` from 2026-08-26 until it was moved by
hand on 2026-08-30 — through the whole Stage 6 build, ordering trial, repack and
retire.

The difference is how the PR named its issue:

| PR | Named the issue by | Attached | Automation |
|---|---|---|---|
| #265, #266 | `(CAR-21)` in the title | yes | **fired** |
| #244 | Linear's suggested branch name | yes | untested — the target state was already held |
| #278, #279 | neither: branch `plan-145-stage-6`, title `Plan 145 Stage 6: …` | **no** | none |

The 2026-08-25 probe eliminated three explanations and missed the fourth:
**attachment and status automation are separate mechanisms, and only attachment
was ever demonstrated.** PR #244 attaching by branch name was read as proof the
integration was live, when it proved only half of it. This project's own
`plan-NNN-*` branch convention establishes neither.

**Working rule: every PR title carries `(CAR-NN)`.** That is the entire fix, and
it is the whole difference between CAR-21 and CAR-22.

#### The merge automation is wrong for a multi-PR issue

CAR-21 needed six PRs. Its state history reads `In Progress → Soaking → In
Progress → Soaking → In Progress → Done`. Both `Soaking` transitions were the
automation obeying its own rule correctly; both returns were hand corrections.

The deviation note above anticipated that a **no-soak** issue would need a manual
`Done`. It did not anticipate that **any issue whose work spans more than one
PR** gets flipped to `Soaking` mid-build, once per merge. Given how work is
actually sliced here, that is the dominant source of state corrections rather
than an edge case — and it argues for smaller issues, not for disabling the
automation.

### Seeded issues (Cycle 1)

Derived from `PLANS.md` as it stood on 2026-08-24. Filler-order judgement: the
handoff draft suggested Plan 147 (row 9) plus Plan 139 Stage C (row 13) as the
two fillers. Strict build-order priority sequencing kept Plan 147 (it feeds
row 2's held set) but swapped Plan 145 (row 7) in for Plan 139 Stage C.

| Issue | Plan / stage | Est. | Blocked by |
|---|---|---:|---|
| CAR-6 | Plan 149 Stage 0 bootstrap | — | — |
| CAR-7 | Plan 142 Stage 1 drain contract | 2 | — |
| CAR-8 | Plan 136 Stage 3b (baseline + pin + 48h soak) | 3 | 2026-08-25 |
| CAR-9 | Plan 142 item 3 Phase B (maintenance-pool window) | 2 | CAR-8 |
| CAR-10 | Plan 141 (freeze fixtures, fix ct-403-log-spike) | 3 | — |
| CAR-11 | Plan 140 Stage 4 (demote http_health_sensor) | 1 | — |
| CAR-12 | Plan 147 Stage 1 (V043 expand + view rebuild) | 3 | — |
| CAR-13 | Plan 145 (close Stage 0d/0e, build backfill write path) | 3 | — |

Also created and immediately canceled: CAR-1 through CAR-4 (Linear's default
onboarding issues) and CAR-5 (a session-artifact duplicate of Issue 1 from the
pre-restart bootstrap attempt).

### Not changed by this bootstrap

`docs/PLANS.md` was not edited. No plan document had its state altered by
Linear setup. This section, and the Cycle measures table below, are the only
edits.

## Stage 1 — Seed only the current horizon

After the build-order placement of this plan is approved, derive the first
cycle from `PLANS.md` as it stands that morning. Do not freeze today's exact
rows into this document: the top gate can change overnight.

Seed to a **points budget calibrated on measured completion** — currently ~30,
from Cycle 1's ~30 points in 6 days. **Revised 2026-08-29**; this read
"no more than eight issues" until Cycle 1 supplied a velocity number and showed
that an eight-issue seed leaves most of the cycle to arrive unplanned. See
[Cycle 1 findings](#the-eight-issue-cap-is-measured-in-the-wrong-unit) for the
evidence and the caveat that travels with the number.

The budget sizes the cycle. The composition is what keeps it honest, and it is
unchanged:

- the currently active top slice;
- the next two unblocked slices;
- any higher-priority slice whose timed gate ends early enough inside the cycle
  to start the slice. **Sharpened 2026-08-31**: the gate closing inside the cycle
  is necessary and not sufficient. Plan 134's Stage 1 window closed 2026-09-06
  against a cycle ending 2026-09-07, and its next slice is three deploys at
  48-hour intervals — five hours of cycle buys none of it, so the row belongs to
  the following cycle despite being the highest-priority row on the board;
- at most two safe fillers that become pullable only while a higher item soaks;
- Plan 149's own bootstrap/measurement work.

Create no project for a blocked plan outside that horizon. A board that begins
by mirroring the whole roadmap has already failed the experiment.

Estimates are optional in Cycle 1. If used, they describe execution size only:

| Estimate | Meaning |
|---:|---|
| 1 | less than half a day |
| 2 | roughly one day |
| 3 | two to three days |
| 5 | most of a week |

Do not estimate plan-document effort labels into issue points mechanically.
One plan may produce several independently measured issues.

**The day-mapping in that table is wrong by roughly 3x** — measured against
Cycle 1, which delivered about 20 estimated working days in 6 calendar days. It
is left standing rather than silently re-scaled: Cycle 1 is a 6-day cycle
dominated by one compute-bound plan, and re-anchoring the whole scale on it
would replace a known-wrong number with a differently-wrong one. Recalibrate,
or drop the day column and keep points as relative size, once Cycle 2 supplies a
second point.

## Stage 2 — Run three cycles

For three Monday-to-Sunday cycles:

1. Select issues from the build order and explicit soak-safe fillers.
2. Link every implementation branch and PR to its issue.
3. Move work to `Soaking` only when implementation is complete and the issue
   names the evidence and end time it awaits.
4. Close the issue only after its evidence is written to the canonical plan.
5. Write the normal repository weekly recap. Linear supplies execution facts;
   it does not replace the git-and-plan read.
6. Record the measures below in this document after the cycle closes.

### Measures

| Measure | Question |
|---|---|
| Time to choose next work | Did the board remove repeated roadmap reconstruction? |
| Issues added after cycle start | Is unplanned work visible, or merely normalized? |
| Issue rollover | Are slices too large or priorities unstable? |
| State corrections | How often did Linear disagree with repository truth? |
| Duplicate edits | How often was the same fact maintained in both places? |
| Recap effort | Did the board make the weekly recap easier without becoming its source? |

The first three cycles are calibration, not a velocity target. Do not use
Linear's capacity prediction to raise commitments until three completed cycles
exist, and do not treat issue count as comparable when issue sizes differ.

## Cycle 1 findings — a mid-cycle read, 2026-08-29

Taken with two days left in the cycle, because these change how Cycle 2 is
seeded and waiting would mean seeding it on the assumptions this read
overturns. **The measures table below is not filled from this read** — that
one waits for the close, as designed.

### The shape of the cycle

| | |
|---|---|
| Window | 2026-08-25 05:00Z → 2026-08-31 05:00Z (6 days) |
| Issues carrying the cycle | **21**, 42 points |
| Seeded | 8 — CAR-6…CAR-13; the Stage 1 cap held exactly |
| **Added after the start** | **13 — 62% of the cycle** |
| Done at this read | 15 issues, ~30 points |
| Open at this read | CAR-22 `In Progress`; CAR-12, 17, 23, 24, 25 `Ready` |
| Plan 145 alone | 6 issues, 15 points |

Also created and canceled without ever entering the cycle: CAR-27 (folded into
[Plan 156](plan_156_block_page_detection.md)) and CAR-28 (fixed under Plan 145
Stage 5b).

The seed predicted the cycle's **start** accurately and its **content** not at
all. Plan 145 entered the seed as a single 3-point issue to "close Stage 0d/0e
and build the backfill write path", then revised its method three times and
generated five further issues as it went. That is not a seeding failure — it is
what a plan does when measurement contradicts it — but it means a seed list
describes an opening position, never a commitment.

### The eight-issue cap is measured in the wrong unit

Stage 1 caps seeding at eight issues. The cap held (exactly 8 seeded) and the
cycle still ran to 21, because `ticket-now` exists precisely to add. Eight was
chosen before any velocity data existed; there is now a number, and it is
**~30 points completed in 6 days**.

Seeding ~15 points into a cycle that absorbs 30 guarantees the remainder
arrives as unplanned work, which is what makes "issues added after cycle start"
unreadable at 62%. **The cap therefore becomes a points budget calibrated on
measured completion — currently ~30 — and the issue count falls out of it.**

What actually protects the design is the *selection* rule, not the count: top of
the build order, the five Stage 1 categories, and fillers that name the item
they soak behind. Fourteen issues drawn from six build-order rows does not
mirror a 19-row build order and a 16-row backlog; the failure the cap guards
against is a board that holds work nobody could pull.

**Carry this caveat with the number.** Cycle 1's 30 points were Plan 145 —
compute-bound and code-bound work, which is what moves fastest here. Plan 147 is
four sequential production deploys with verification between them, Plan 134
opens a seven-day observation, and Plan 142's window is wall-clock. **Velocity
does not compress a seven-day window.** Expect a calibrated cycle to hold slots
that consume calendar without consuming days.

### The estimate scale is off by roughly 3x

Fourteen estimated issues completed, 30 points. Read against Stage 1's own
table — 1 is under half a day, 2 about a day, 3 two to three days — that is
**about 20 working days of estimated work delivered in 6 calendar days**.

Either the day-mapping is recalibrated to how this project actually moves, or
the descriptions come off the table and points become relative-size only. Left
as it is, the scale makes a cycle unreadable against its own commitment.

### CAR-28 became a second owner of a fact — the forbidden shape

The issue contract says not to paste a plan's problem statement, measurements
or rejected alternatives into Linear. CAR-28 holds roughly 4,000 words of
measured evidence: both compression bench tests, the full ordering-trial result
table, the May/June/July extrapolation and its caveats. It was **canceled on
2026-08-27** and was **still being edited on 2026-08-30**. The same result also
lives in Plan 145 §*Evidence — the ordering trial*.

That is the "Duplicate edits" measure firing, on a canceled issue, and it is
precisely the failure Stage 3's **Remove** criterion names. It is worth being
clear about why it happened rather than treating it as carelessness: CAR-28 was
a *question* that outlived its issue, and Linear was where the question was
being asked. The rule survives the case — the durable copy belongs in the plan
document, and the issue should be reduced to a pointer — but "an open question
with no plan section yet" is a real gap in the issue contract.

## Cycle measures

Filled in *after* each cycle closes, from a real read against the board plus
the repository — not before. Rows below track the six measures defined above.

Cycle 1 was read on **2026-08-31**, after it closed itself at
`endsAt 2026-08-31T05:00:00Z`. The two qualitative rows are the maintainer's,
supplied that day; the four countable rows are from Linear's cycle history
reconciled against the issues themselves.

| Measure | Cycle 1 (2026-08-25 → 2026-08-31, 6d) | Cycle 2 (2026-08-31 → 2026-09-07) | Cycle 3 (2026-09-07 → 2026-09-14) |
|---|---|---|---|
| Time to choose next work | **Better, with friction that has a named cause.** Maintainer, 2026-08-31: knowing what to do next was no longer the problem; the friction was "the gap between seeded work and output". That gap is the row below, measured — 17 points seeded against 48 delivered — so this is the same finding felt rather than a second one | — | — |
| Issues added after cycle start | **17 of 25 issues (68%); 31 of 48 points (65%).** Seeded 8 issues / 17 points. This is the measurement that turned the eight-issue cap into a points budget | — | — |
| Issue rollover | **2 of 25 issues (8%), 4 of 48 points.** CAR-17 (`Ready`) and CAR-31 (`Soaking`), both moved to Cycle 2 by Linear automatically at the close — no manual action, and none required. Neither is an oversized slice: CAR-31 is a seven-day observation window and CAR-17 waits on four Plan 142 Stage 2/3 defects. Rollover here measures gating, not slicing | — | — |
| State corrections | **Partial — three known, no exhaustive count.** CAR-21 was flipped to `Soaking` twice by the merge automation mid-build and hand-corrected back twice; CAR-22 sat in `Ready` through its entire Stage 6 build and was moved by hand on 2026-08-30; and six of six transitions were manual on 2026-08-25, before the automation was understood. A complete figure needs a per-issue history read, which this one did not do — recorded as partial rather than presented as a total | — | — |
| Duplicate edits | **Two, both on canceled issues.** CAR-28 held ~4,000 words duplicating Plan 145's ordering-trial evidence, was canceled 2026-08-27 and was still being edited 2026-08-30. CAR-27 was created and canceled into [Plan 156](plan_156_block_page_detection.md). Both are the shape Stage 3's *Remove* criterion names | — | — |
| Recap effort | **Unchanged.** Maintainer, 2026-08-31: the 2026-08-30 recap was written from git and read Linear not at all, which is the design working — Linear supplies execution facts and does not replace the git-and-plan read. Follow-up identified rather than acted on: there is room to fold Linear history into the recap skill, to be tested against the next recap | — | — |

### What the close changed about Stage 1's budget

**The budget should be re-derived from this read.** Stage 1 currently names
**~30 points**, taken from the mid-cycle read of 2026-08-29 which saw 15 issues
and ~30 points done. The closed cycle completed **44 points across 23 issues** —
the mid-cycle read undercounted by about 47%, because the last two days of the
cycle landed Plans 147, 158, 160 and Plan 134's Stages 0 and 1.

The caveat that travels with the number is unchanged and now has a second
reason to travel: 44 points in 6 days is ~7.3/day, but Cycle 1 was dominated by
Plan 145's compute-bound recovery work. A cycle whose horizon is deploy-gated
slices, observation windows and a wall-clock maintenance window will not convert
at that rate however it is seeded.

One reconciliation detail, kept because it is a live hazard rather than an
arithmetic curiosity: the completed estimates sum to **43**, and Linear reports
**44**. The difference is **CAR-6, which carries no estimate and which Linear
counts as 1 point in scope**. An unestimated issue therefore does not read as
zero — it silently widens the budget by one, which is why `fill-cycle` and
`ticket-now` now report an unestimated issue as unmeasured rather than summing
around it.

## Stage 3 — Keep, change or remove

At the end of Cycle 3, export the Linear data and choose one outcome:

### Keep

Keep the execution layer if all are true:

1. Choosing the next safe slice is materially faster.
2. Soaks and review gates are more visible than in `PLANS.md` alone.
3. No plan state or architectural fact depends on Linear to be understood.
4. Fewer than two material state disagreements occurred across three cycles.
5. Weekly recap effort did not increase.

### Change

Run one corrected cycle if the value is visible but one workflow rule caused
most friction. Change one thing only, name it here, and remeasure.

### Remove

Remove the execution layer if it becomes a second roadmap, requires duplicate
updates, encourages work outside build order, or makes the repository
insufficient on its own. Preserve the final export under a non-`docs/` local
archive or the service export; do not check a second issue history into this
repository.

## GitHub automation boundary

Allowed automation:

- branch or PR links an issue;
- opening a PR moves `In Progress` to `In Review`;
- merging a PR may move the issue to `Soaking` when the issue is explicitly
  marked as requiring production evidence;
- merging a no-soak issue may move it to `Done` only when merge is its stated
  exit condition.

Forbidden automation:

- editing `PLANS.md` or a plan document;
- moving a plan between repository tables;
- marking a production-evidence issue Done merely because its PR merged;
- creating issues for every commit or PR;
- treating a closed Linear project as a completed repository plan.

## Free-tier and cost gate

The current Linear free tier advertises 250 issues, two teams, projects,
cycles, API/webhook access and GitHub integration. That is enough to run this
experiment. Do not upgrade during Stages 0–2.

At Cycle 3, record:

- total issues created;
- projected weeks until the issue limit at the measured creation rate;
- which paid-only capability, if any, would remove an observed constraint.

Paying solely to retain an ever-growing execution archive fails the design.
Completed repository history belongs in git and recaps, not in a subscription
required to preserve a second copy.

## Success criteria

1. Issue 1 bootstraps the system on 2026-08-25.
2. The first cycle is **seeded** to its Stage 1 budget and every issue links a
   canonical plan section. **Restated 2026-08-29** — this read "contains no more
   than eight issues", which Cycle 1 failed on its second day and met exactly as
   a seeding cap. A cycle's *contents* are not a criterion this plan can hold;
   how much arrives unplanned is a measure, not a target.
3. Three weekly cycles complete without Linear becoming necessary to recover
   plan state, build order or historical evidence.
4. PR and soak status can be read at a glance without duplicating design facts.
5. The Cycle 3 decision is recorded here with the measured keep/change/remove
   gate.
6. If kept, the operating rules are short enough to remain in this document;
   no second process manual is created.

## Rollback

Disconnect the GitHub integration and stop using the workspace projection.
There is no repository-state rollback because Linear never owns repository
state. Open PRs, plan documents, `PLANS.md`, the completion archive and weekly
recaps remain complete without it.

## Sources checked when drafted

- [Linear pricing and plan limits](https://linear.app/pricing)
- [Linear cycles](https://linear.app/docs/use-cycles)
- [Linear GitHub integration](https://linear.app/docs/github-integration)
- [Linear projects](https://linear.app/docs/projects)
