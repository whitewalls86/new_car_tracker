---
name: commit-plan-attribution
description: Create Git commits in this repository with explicit plan attribution that the weekly recap can resolve from the commit message alone. Use when the user asks to commit, checkpoint, or record implementation or documentation work. Do not use for rewriting existing history unless the user explicitly requests it.
---

# Commit with plan attribution

Create a reviewable commit whose own subject or body identifies why the work
exists. The weekly recap deliberately ignores branch names and touched files
for attribution, so neither can substitute for the message.

## Attribution contract

Before committing, identify the plan or plans from the work the user authorized
and the current task context. Confirm against `docs/PLANS.md` and the relevant
plan documents when useful, but put the result into the commit message itself.

Use one of these forms:

- Prefer a readable conventional subject and a body such as `Plan 142 Stage 1.`
- A plan-scoped subject such as `docs(plan-152): define one-shot worker lifecycle`
  is also sufficient.
- For work shared by plans, name each one in the body: `Plans 139 and 142.`
- If the work genuinely belongs to no plan, say so explicitly in the body:
  `No plan — <brief reason>.`

Never invent a plan mapping merely to avoid an unattributed commit. If more
than one mapping is plausible and the user's intent does not settle it, ask
before committing.

## Commit workflow

1. Inspect `git status`, the relevant diff, and recent commits. Preserve
   unrelated user changes and do not stage them.
2. Verify the requested slice in proportion to its risk. Do not hide failing
   checks in the commit message.
3. Stage only the files belonging to the slice.
4. Review the staged diff and ensure it contains no secrets, generated debris,
   or unrelated edits.
5. **Write the stamp**, which is what lets the commit through:

   ```bash
   date -u +%Y-%m-%dT%H:%M:%SZ > "$(git rev-parse --git-dir)/commit-attribution-stamp"
   ```

6. Commit with a concise outcome-focused subject and explicit attribution in
   the subject or body.
7. Report the commit SHA, subject, attribution, checks run, and any remaining
   working-tree changes. Push only when the user requested it or the active
   workflow already clearly includes updating an existing remote branch or PR.

## The gate, and why the stamp is per-commit

Two mechanisms enforce this skill rather than trusting that it is remembered,
and they hold different halves of the contract:

| | Enforces | Runs |
|---|---|---|
| [`scripts/commit_attribution_gate.py`](../../../scripts/commit_attribution_gate.py) | that this skill was **invoked** for this commit | `PreToolUse` on `Bash`, before the command |
| [`.githooks/commit-msg`](../../../.githooks/commit-msg) | that the **message** carries attribution | inside `git commit`, however it was invoked |

The stamp exists because a message can be given the right shape by hand while
the mapping in it was never checked against `docs/PLANS.md`. Confirming the
plan is this skill's job, and the stamp is the evidence it happened.

**`.githooks/commit-msg` deletes the stamp when the message passes**, so it is
spent by the commit it was written for. That is deliberate: a stamp that
outlived its commit would make the rule "invoke the skill once per session",
which is exactly the rule that failed -- five commits in one session, one
invocation.

Do not write the stamp except as step 5 of a commit you are actually making
through this skill. Writing it to clear the gate for a commit this skill did
not compose is the one thing that makes the whole mechanism worthless.

## Boundaries

- A request to commit authorizes a new commit, not amend, rebase, squash, or
  force-push.
- Do not rewrite earlier unattributed commits without explicit approval; that
  changes published history and may require coordinated force-pushing.
- Do not attribute from the branch name alone.
- Do not claim that touching a plan document makes the commit belong to that
  plan. Attribution follows the authorized purpose of the work.
- Do not combine separable plan work merely to reduce the number of commits.
