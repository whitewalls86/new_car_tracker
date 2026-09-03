# The Plan Document Contract

**Owner:** [Plan 172](plans/plan_172_plan_authoring_skill.md), Stage A.
**Measured against the repository on 2026-09-02.**

This is the standard for plan documents, not a description of the documents that
happen to exist. The index owns a plan's state; the plan document owns its
content. Implementations cite this contract and encode its mechanics. They do not
keep a second prose copy of its rules.

Three forms carry this contract:

| Form | Where | Owner |
|---|---|---|
| For a person | this document | Plan 172 / CAR-70 |
| For a coding agent | `plan-draft`, `plan-start`, `note-evidence`, `stage-close`, and `close-out` | Plan 172 |
| For the repository | [`tests/test_planning_docs.py`](../tests/test_planning_docs.py) and [`scripts/build_public_roadmap.py`](../scripts/build_public_roadmap.py) | Plan 172 and Plan 138 |

---

## The four questions and the exit

The four live states each ask one question. The index row carries the short
answer needed to scan the roadmap; the document carries the durable answer:

| State | The document answers |
|---|---|
| backlog | **Should I build this?** |
| build order | **What am I building today?** |
| closeout | **What do I need to check, and when?** |
| archive | **What did I build, how, and what did it take?** |

Superseded is an exit from any live state, not a fifth step. A superseded plan
adds `## Superseded` with the date, a link to the plan or plans that replaced it,
and the reason. It does not add `## Record` or `## Public summary`, because it
was replaced rather than delivered.

---

## The section ratchet

A plan gains sections as its answer changes and does not discard an earlier
answer. Its `##` sections have this fixed order:

| Order | Section | Required when | Purpose |
|---:|---|---|---|
| 1 | `## What this plan is for` | drafted | Tell a stranger what the plan is and why it exists |
| 2 | `## The case` | drafted | Make the case for building it |
| 3 | `## Design` | started | State the agreed design |
| 4 | `## Stages` | started | Make the work and its order executable |
| 5 | `## The checks` | closeout | Define the observations that close the plan |
| 6 | `## Public summary` | archive | Tell a stranger what the plan accomplished |
| 7 | `## Record` | after the first stage closes | Preserve the chronological evidence |

Sections that are not yet required are absent, not empty. `## Public summary` is
inserted immediately before `## Record` at archive so the record remains the
last section. A plan following the superseded exit adds `## Superseded` at the
end of the content it already has and stops the ratchet there.

A plan document carries no status marker: no `## Status` section and no
`**Status:**` line. State, priority, effort, and position live only in
[`docs/PLANS.md`](PLANS.md). A plan's origin and any argument about its nature or
position belong in `## The case`. The case is required but free in form; in
particular, it does not require a measurement.

`plan-draft` allocates both the next plan number and the filename
`docs/plans/plan_NNN_<slug>.md`. That filename is permanent because inbound links
outlive later changes to the plan's wording. Drafting writes only the first two
sections and then asks whether the plan belongs in the build order, belongs in
the backlog behind a named trigger, or remains in the backlog until a named fact
would make the decision possible. Starting a plan is always a separate
invocation.

---

## Stages and order

New plans use `Stage` as their only work-unit vocabulary. `Gate`, `Phase`,
`Track`, `Step`, and `Wave` are not stage names in a new plan.

A stage identifier is an opaque letter allocated in discovery order: A, B, C,
through Z, then AA, AB, and so on. Once allocated, it is never renamed or reused,
including when the stage is canceled. A later-discovered stage takes the next
unused letter and may be placed anywhere in the work order.

`## Stages` begins with an order table. `Order` is the only column that holds an
integer and may be renumbered freely; `Stage` holds the permanent letter. Each
table row links to one `### Stage X` section, and that section states an explicit
exit condition. The `State` cell contains exactly one of these values:

| State | Meaning |
|---|---|
| `—` | not started |
| `next` | the one stage currently workable; exactly one stage per plan carries it |
| `blocked` | its order has been reached, but something outside the plan prevents it |
| `done` | its Linear issue is closed and its record entry exists |
| `canceled` | it was rejected, superseded, or made unnecessary; its letter remains spent |

In-progress is not a document state; Linear owns it.

Plans that were already sequenced when this contract landed keep their existing
identifiers. There is no conversion sweep. A plan being rewritten wholesale for
another reason may adopt letters only if it records an old-to-new mapping in its
own document.

---

## The checks

When a plan enters closeout, each item under `## The checks` names what was
deployed and when, what is being watched, when the answer is due, and the
`## Record` entry that will receive the result. The closeout row in
[`docs/PLANS.md`](PLANS.md) keeps the due date and a one-sentence gate so due work
can be found without scanning plan documents.

---

## The record

Evidence appears in one `## Record` section and nowhere else. The section is
last. It contains one `###` entry per closed stage, ordered chronologically with
the oldest first. Each entry names its stage and records what landed, how it was
verified, and what the work took.

Evidence too large for the entry lives at
`docs/evidence/plan_NNN_stage_X_evidence.md`, linked from that entry. Linear's
`Evidence destination` therefore always names `plan_NNN §Record`.

---

## The two public sections

Both public sections address a reader who does not already know the project and
are capped at 320 characters:

| Section | Tense | Answers | Published as |
|---|---|---|---|
| `## What this plan is for` | present | What is this, and why does it exist? | planned work |
| `## Public summary` | past | What did it accomplish? | completed work |

The completed heading remains `## Public summary`; it is not renamed for
symmetry. No plan publishes both sections because the planned and completed
windows read different lifecycle states.

`## What this plan is for` is not edited because work progressed. Landing or
adding a stage, repointing a slice, and filing or closing a ticket do not change
it. It is edited when the plan's purpose changes, and must be edited then so the
published claim continues to describe the plan.

---

## Adoption and enforcement

This contract governs every new plan. Its backward reach is deliberately
bounded:

- Backlog, build-order, and closeout documents are in scope. At the 2026-09-03
  baseline — after Plan 129's same-day closeout moved it out of `Current
  closeout` — that was 44 plans, 43 of which lacked `## What this plan is
  for`. (This bullet postdates this document's own 2026-09-02 measurement
  above; the count moved with Plan 129, and Stage D's waiver list was built
  against the count it actually landed against.)
- Existing live plans acquire the applicable public section, and closeout plans
  acquire `## The checks`. They are not backfilled with `## Design` or
  `## Stages`.
- Archived and superseded documents are historical records and are not rewritten
  to adopt this contract.

Known live violations sit in one named, dated waiver list owned by a plan. The
assertion rejects a new violation, a stale waiver, or a waiver whose owner has
archived. The list only shrinks. A plan whose work does not fit stages receives a
waiver rather than a permanent plan kind; Plans 117 and 163 are the baseline
cases.

The planning-doc assertion covers both public windows: the first four executable
build-order rows and the first four newest completed rows. The completed window
already conformed at the baseline; that fact is asserted rather than used as an
exemption.

| Contract surface | Mechanism that enforces or consumes it |
|---|---|
| Draft sections, number, filename, and absence of status | `plan-draft` |
| Design, lettered stages, exits, and order table | `plan-start` |
| Record entries and stage completion | `note-evidence` and `stage-close` |
| Checks, archive transition, and completed public copy | `close-out` |
| Live-document adoption, waivers, and both published windows | `tests/test_planning_docs.py` |
| The completed projection, and `## Public summary`'s cap | `scripts/build_public_roadmap.py` |
| The planned projection reading `## What this plan is for` | [Plan 138](plans/plan_138_public_surface_refresh.md) Stage 9 — not yet landed; today the build order's published slice cell is what `build_public_roadmap.py` reads for a planned row |

Until a named skill or assertion lands, its row describes specified but
unmechanized behavior; it must not be presented as already enforced.
