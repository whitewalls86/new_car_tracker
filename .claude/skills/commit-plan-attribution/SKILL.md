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
5. Commit with a concise outcome-focused subject and explicit attribution in
   the subject or body.
6. Report the commit SHA, subject, attribution, checks run, and any remaining
   working-tree changes. Push only when the user requested it or the active
   workflow already clearly includes updating an existing remote branch or PR.

## Boundaries

- A request to commit authorizes a new commit, not amend, rebase, squash, or
  force-push.
- Do not rewrite earlier unattributed commits without explicit approval; that
  changes published history and may require coordinated force-pushing.
- Do not attribute from the branch name alone.
- Do not claim that touching a plan document makes the commit belong to that
  plan. Attribution follows the authorized purpose of the work.
- Do not combine separable plan work merely to reduce the number of commits.
