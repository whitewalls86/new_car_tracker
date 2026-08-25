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
| PR drafted | In Progress |
| PR opened | In Review |
| PR merged | Soaking |

GitHub Issues Sync remains OFF (the no-two-way-sync rule).

**Deviation from Decision → Workflow.** The plan describes PR-merge behavior
conditional on whether the issue requires production evidence. Linear's PR
automations are per-team, not per-issue, so the conditional has no home. The
chosen tradeoff: merge → Soaking unconditionally; no-soak issues are moved to
Done by hand. A false Done is on the forbidden-automation list; a false
Soaking is one manual click.

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

Seed **no more than eight issues**:

- the currently active top slice;
- the next two unblocked slices;
- any higher-priority slice whose timed gate ends inside the cycle;
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

## Cycle measures

Filled in *after* each cycle closes, from a real read against the board plus
the repository — not before. Rows below track the six measures defined above.

| Measure | Cycle 1 (2026-08-25 → 2026-08-31, 6d) | Cycle 2 (2026-08-31 → 2026-09-07) | Cycle 3 (2026-09-07 → 2026-09-14) |
|---|---|---|---|
| Time to choose next work | — | — | — |
| Issues added after cycle start | — | — | — |
| Issue rollover | — | — | — |
| State corrections | — | — | — |
| Duplicate edits | — | — | — |
| Recap effort | — | — | — |

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
2. The first cycle contains no more than eight issues and all link canonical
   plan sections.
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
