---
name: ref-hygiene
description: "Tidy the git refs a finished cycle left behind — audit every local branch, worktree and stash against the remote trunk, propose only the branches whose content has demonstrably landed, and delete those on approval. Use when the user says to clean up branches, tidy worktrees, or run the ref hygiene step of a cycle close. This skill writes git refs and never prose: it edits no plan document, no docs/PLANS.md row and no Linear issue, and it never runs before that week's recap is written, because the recap reads the history this step destroys."
---

# Tidying the refs a cycle left behind

This is the only skill in this repository that **destroys evidence**. A deleted
branch cannot be un-deleted from anything but the reflog and a not-yet-collected
object database, and both of those expire on a timer nobody watches.

So it goes **last**, after the recap, and it deletes nothing that a script did
not first prove had landed.

## The boundary, before anything else

You delete refs. You do not write a word of prose.

| You do | You never do |
|---|---|
| run `scripts/audit_git_refs.py` and read its verdicts | decide a verdict yourself, or override one |
| propose a deletion set and wait | delete anything unapproved |
| delete local branches, one at a time | delete a remote branch, or any tag |
| report stashes and prunable worktrees | drop a stash, ever |
| say a branch holds unlanded work | merge it, rebase it, or cherry-pick it |
| — | edit a plan document, `docs/PLANS.md`, the archive, or Linear |

The fourth kind of state. `plan-week` writes `docs/` prose, `plans` writes
`docs/` state, `fill-cycle` and `ticket-now` write Linear. Git refs were owned
by nobody until [Plan 164](../../../docs/plans/plan_164_cycle_close_ritual.md),
and the reason this is a separate skill rather than a step inside `plan-week` is
in that plan: a tool that both reads history to write prose and deletes refs can
destroy the evidence for the claim it just made, and the prose would still read
as consistent.

## Do not run this before the recap

`plan-week` uses `git branch --contains` as a hint about which plan a commit
belongs to. It is robust to deleted branches and **helped** by live ones — on
the measured week that layer attributed 18 commits over 30 days that nothing
else would have.

So the order is fixed and it is not a preference:

1. `plan-week` writes `docs/recaps/<sunday>.md`.
2. *Then* this skill runs.

[`docs/planning/cycle_close_order.md`](../../../docs/planning/cycle_close_order.md)
holds the whole sequence and the condition on each step. If the recap for the
week just closed is not written, **stop and say so.** Do not write it — that is
`plan-week`'s, and running both in one operation is the merge that plan forbids.

## Phase 1 — Audit

Read, delete nothing:

```bash
python scripts/audit_git_refs.py            # markdown report
python scripts/audit_git_refs.py --json     # if you want to work with the fields
```

The script fetches with `--prune` first and compares against `origin/master`.
Every branch lands in one of five verdicts, and **only `landed` authorises
anything**:

| Verdict | What it means | What you do |
|---|---|---|
| `landed` | every commit is on `origin/master` **by patch identity** | propose it for deletion |
| `protected` | the trunk, the current branch, held by a worktree, or the head of an open PR | leave it, and say why |
| `unpushed` | unlanded commits with no verified remote copy | report it; the fix is a push, and the push is the user's call |
| `owed` | pushed and verified, and still not on the trunk | report it per commit; the decision is the user's |
| `unknown` | the fetch failed, or open PRs could not be listed | **the run is over.** Fix the cause and re-audit |

`unknown` is not a soft result. It appears when the facts a verdict needs could
not be established, and the whole point of the script is that it fails closed
there. Never argue past it.

## Phase 2 — Propose, then stop

Show, in one message:

- **The branches you propose to delete**, each with the script's reason.
- **Every refused branch**, with its verdict and reason — including the
  protected ones, because "I left it alone" is a result the user should see.
- **The stashes**, listed. Say plainly that this skill will not touch them.
- **Any prunable worktree**, named, with the fact that removing it is a separate
  request.
- **Anything the audit could not answer.**

Then **stop.** Do not delete until the set is approved. The user may strike
branches from the list; they may not add to it — a branch the script refused is
refused, and the way to change that is to change the facts (push it, land it)
and re-audit.

## Phase 3 — Delete, one ref at a time

For each approved branch, and no others:

```bash
git branch -D <name>
```

**Why `-D` and not `-d`, and why that is not the reckless choice.** `git branch
-d` checks the branch against its configured **upstream** when it has one and
against **`HEAD`** when it does not — measured on 2026-08-31, three branches in
identical states, two deleted and one refused, the difference being a
`branch.<name>.merge` config value. Worse, `-d` tests ancestry, and a `landed`
verdict is patch identity: a branch whose patches were replayed onto master
under different SHAs will *always* be refused by `-d` however completely its
content landed.

So `-d` cannot answer this question, and its refusal is not information here.
The safety check is the audit, and it already ran. The rule that keeps this
honest:

> **A `-d` refusal is a signal to find out which ref it compared against, never
> a cue to reach for `-D`.** Here the establishing step happens first, in the
> script, and `-D` executes a verdict rather than overriding one.

Delete them individually. Do not pipe the list into `xargs`: a batch that fails
halfway leaves you unsure which half ran, and there is nothing to inspect
afterwards.

**Assume another agent is working in this checkout.** The repository already
carries that rule for staging; it applies with more force to ref deletion, which
has no `git status` to look at first. A branch that appeared since the audit was
not in the approved set and is not deleted — re-audit instead.

## Phase 4 — Report

Say what was deleted, what was refused and why, and what was left for the user
to decide. Name the stashes again if there are any: they survive every run of
this skill and are easy to forget for months.

Write that report into the conversation. **Do not write it into a plan
document** — if it is evidence a plan wants, `note-evidence` or `stage-close`
puts it there, and they are separate invocations.

## What the script encodes, and why you do not second-guess it

Each rule below cost a real mistake or a near miss on 2026-08-31.
`tests/scripts/test_audit_git_refs.py` builds a deliberate case for each one
against a real repository, so a change to the script that loses a rule fails.

1. **The trunk is `origin/master` after a fetch, never local `master`.** Local
   master was 57 commits behind at the time of the cleanup, and every verdict
   taken against it is wrong in the direction that destroys work.
2. **Ancestry is not the test.** Seven commits on one branch were byte-identical
   patches already on master under different SHAs. `--merged` called that branch
   unmerged; `git cherry` gets it right.
3. **An unpushed branch may hold the only copy in existence.** One did — 17
   commits and ~4,700 lines whose remote ref did not exist until the cleanup
   created it. The push is verified by
   `git rev-list --left-right --count <branch>...origin/<branch>` reading `0 0`,
   not by `git push` exiting quietly.
4. **Safe to delete is not the same as content landed**, and the decision is per
   commit: landed, superseded, or owed. Two branches that looked alike produced
   opposite answers — one carried an index true-up superseded within a day whose
   merge would have restored two archived plans to the build order; the other
   carried content that had never landed anywhere, on a base predating a file
   move. Owed content whose base predates a move is *relocated*, not merged.
5. **Stashes are not branches.** Six survive on this machine, four naming
   branches that no longer exist. No `git branch` command can see them.
6. **The current branch, `master`, a branch a worktree holds, and the head of an
   open PR are never deleted**, whatever their contents.

## What this skill never does

- **Delete a remote branch.** GitHub deletes the head branch when a PR merges;
  anything left on the remote is a question for the user, not this skill.
- **Drop, apply or pop a stash.** Report only. Always.
- **Remove a worktree.** Naming a prunable one is useful; removing it is its own
  request, and a worktree can hold uncommitted work that no ref points at.
- **`gc`, `reflog expire`, or any history rewrite.** Out of scope entirely —
  those are what make a mistake here recoverable.
- **Merge, rebase or cherry-pick anything.** An `owed` branch is reported, never
  landed. Landing work is ordinary development, and it happens on its own, with
  a review.
- **Run on a schedule.** Every deletion is proposed and approved by a person.
