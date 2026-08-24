---
name: plan-week
description: "Write the weekly recap of what happened in this repo — read the last complete week's commits, map each to the plan it belongs to, open those plan documents for the why, and write docs/recaps/YYYY-MM-DD.md. Use when the user asks for a weekly recap, a summary of the week, or what happened last week. This skill writes prose and never state: it does not move a row, edit any table in docs/PLANS.md or the archive, or change a plan document. It reads plan documents and writes only under docs/recaps/."
---

# Recapping a week

Commits say **what changed**. Plan documents say **what it was for**. A recap
written from either alone is a changelog or a wish list, so this reads both and
writes one file per week to `docs/recaps/`.

## The boundary, before anything else

You compose sentences. You do not move rows.

| You do | You never do |
|---|---|
| read commits, read plan documents, write a recap | move a row between tables, or edit one |
| name a plan a commit belongs to | change a plan's status marker |
| say a plan looks finished, and that nobody has filed it | file it |
| write `docs/recaps/<sunday>.md`, and nothing else | write anywhere else under `docs/` |

Moving rows is the `plans` skill (`.claude/skills/plans/SKILL.md`). The two are
separate on purpose and must not be run as one operation: **a tool that both
summarises work and moves rows can move a row because its own summary said
so**, which is a record that confirms itself. If a recap makes it obvious a row
should move, say so in the recap and stop. The user decides, and the other
skill performs it.

Never emit `*(observed)*`, `*(corroborated)*` or `*(inferred)*`. Those are the
**archive's** labels and they mean something specific about how Stage 1
recovered a completion date from git. Reusing them as generic hedging devalues
25 backfilled rows. Mark your own uncertainty in your own words — "measured",
"the commit says", "not stated anywhere", "I could not tell".

## Which week, and on which clock

A week runs **Monday 00:00:00 to Sunday 23:59:59**, and you recap only the
**last complete week**. Run on Friday 2026-08-21 and the window is Monday
2026-08-10 to Sunday 2026-08-16; the five days since are the next run's
problem. The file is named for the window's **end**: `docs/recaps/2026-08-16.md`.

**One clock: git's local author time.** `git log --since/--until` reads author
dates in the local timezone, and `--date=short` renders them the same way —
which is the clock `scripts/audit_plan_state_history.py` already uses, so
following it keeps every date in this repo's planning documents comparable. A
commit at 23:40 on a Sunday belongs to whichever week that clock puts it in.
This is a choice, not a law; it is written down so nobody has to re-derive it
from a boundary commit.

```bash
python - <<'PY'
from datetime import date, timedelta
today = date.today()
end = today - timedelta(days=today.weekday() + 1)   # the most recent Sunday
print(f"{end - timedelta(days=6)} .. {end}")
PY
```

### Before you read a single commit: is this week already written?

```bash
ls docs/recaps/
```

**Never regenerate an existing recap.** A recap is a dated record of what was
knowable on the day it was written; rewriting it against today's repo replaces
a record with a reconstruction, and the reconstruction always looks better
because it knows how things turned out. If `docs/recaps/<sunday>.md` exists,
stop and say so. If the user wants it corrected, they say what is wrong and you
append a dated correction under a `## Correction, YYYY-MM-DD` heading — the
original paragraphs stay.

**Missing weeks: write the oldest, name the rest, stop.** One file per week
plus last-complete-week-only means a fortnight's silence leaves a hole that
nothing announces. So before writing, list the complete weeks between the
newest recap already in `docs/recaps/` and the last complete week. If more than
one is missing, **recap the oldest missing week** — not the most recent — and
report the ones still owed.

One run writes one recap. That is a cost rule with teeth: a week costs roughly
19k tokens to read, so "catch up on the last quarter" is a quarter of a million
tokens taken without asking. Working oldest-first means the gap always shrinks
from the far end and the filenames show exactly how much is left. If
`docs/recaps/` is empty there is no backlog to catch up on — the target is
simply the last complete week.

**The user may override this**, and often will: "recap the last three weeks" is
a decision about cost that is theirs to make. Measure each window first and say
what it will cost, then do them oldest-first.

### A backfilled recap says that it is one

A recap written weeks after its window is still a dated record — of what was
knowable on **the day it was written**, which is the `**Recapped:**` field, not
the day the window closed. Two things follow, and neither is optional:

- **Say so in the opening**, with the gap in days. A reader comparing a recap
  written on the Monday after its window with one backfilled nineteen days
  later should not have to subtract two dates to spot the difference.
- **`## Deferred to the next recap` needs a second sentence.** The count is
  still "commits after this window's end", which for a backfilled recap sweeps
  up weeks that are now recapped and are not deferred at all. Give the raw
  count and the command, then name which of the following weeks are already
  covered and which are genuinely still uncovered. Left alone the number is
  true and reads as alarming.

**An empty week gets a recap too, and it is worth more than it looks.** The
week of 2026-07-27 held zero commits. Without a file, nobody can tell "nothing
happened" apart from "nobody recapped it" — and a long gap between two bursts
of work reads as an abandoned or blocked effort when it may be neither. Write
every required section, say nothing happened, and name the commits either side
of the silence so its shape is visible.

**Git cannot say why a week was quiet, so ask.** The seventeen-day gap around
that week was a vacation, which is not a thing any command reports. If the user
tells you, write it down and attribute it to them and the date they said it.
Do not guess a reason from the shape of the history — an unexplained gap is
better than a wrong explanation, and "no reason is recorded" is itself a
publishable sentence.

## Reading the history

**Read the commits. Do not fingerprint them.** Subjects alone are ~0.9k tokens
for a week and far too thin to write from; full patches are ~262k for a *quiet*
week, about 106k of which is this repo's own markdown being re-read at enormous
cost. The tier that works:

```bash
# The read. ~19k tokens on the measured week of 2026-08-10.
git log --all --since='<mon> 00:00:00' --until='<sun> 23:59:59' \
        --no-merges --date=short --reverse \
        --pretty='===%h %ad %s%n%b' --stat
```

Subjects, **bodies** and `--stat`: the author's own stated rationale plus the
shape of every change. Check the cost before you read, and say so in your
report if the week is unusually large:

```bash
git log --all --since='<mon> 00:00:00' --until='<sun> 23:59:59' \
        --no-merges --pretty='%h %ad %s%n%b' --date=short --stat | wc -c
```

(Roughly 3.7 characters to the token.)

**Escalate to a full diff only for a commit you genuinely cannot read**, and
treat it as a cost rather than a free clarification: on the measured week the
two commits whose subjects named no plan were ~15.5k tokens of diff between
them, nearly as much as the entire week's read. Usually the `--stat` and the
neighbouring commits already answer it.

## Attribution: it links to the *why*, it does not decide what happened

This is the part that is easy to get backwards.

**A plan number is not a prerequisite for recapping a commit.** `fix(tests):
read counters outside the builtins.open patch` needs no plan number — its
subject and `--stat` say what it did. A plan number buys exactly one thing:
**the right to open `docs/plans/plan_NNN_*.md` and say why the work mattered.**

So attribute from **what the text says**, in this order, and stop at the first
that answers:

1. the **subject** names a plan — `Plan 131 Stage 4: ...`, `docs(plan-146): ...`
2. the **body** names a plan, including a body that says it belongs to *none*

That is the whole rule. On the measured week 40 of 42 non-merge subjects
attribute unaided (95%); the 41st says in its body *"Unrelated to Plan 131"*,
which is an answer; the 42nd has no body at all and stays unattributed.

Three layers you might reach for, and why not:

- **The containing branch is a hint, never a source.** `git branch --contains`
  answers only while the branch still exists, and merging is supposed to delete
  it. On the measured week the branch layer rescued 18 commits over 30 days,
  every one of them also on `master`, found only because 76 refs are still
  lying around. The same window recapped in three months rescues nothing. Use a
  branch name to *confirm* a reading; never let a recap depend on one, and
  never treat a missing branch as a failure.
- **Plan documents touched by the diff contribute nothing.** Measured over 170
  commits in 30 days: **+0** beyond what subjects and bodies already gave. Work
  commits touch code, not plan documents.
- **Another commit naming this one's sha** is real evidence and you may quote
  it in the prose — but it does not attribute the commit. The commit's own text
  is what attributes it. Anything else starts a list of special cases.

**A commit you cannot attribute is recapped anyway**, under what it did, and
*also* listed as unattributed. The missing thing is the link to a *why*, not
the work. Dropping it is the one outcome this skill exists to prevent: an
unattributed commit that goes unnamed is the recap's version of a table row
with no exit condition — invisible, and therefore permanent.

### Merges are counted and named, never recapped

Merges are simultaneously the easiest thing to attribute and the most
misleading thing to count. `Merge pull request #192 from user/plan-131-stage-3`
attributes perfectly by subject — and the work it describes is the commits it
brings in, which are in the window too. Recap both and the week doubles.

**So: name every merge, recap none of them.** The work sections cover the
non-merge commits; a `## Merges` section lists each merge's sha, its PR number
and the plan its branch names, so the denominator still reconciles. Do not
assert that a merge brings in only in-window work unless you checked — on the
measured week twelve of thirteen did, and `788bb33` also carried `8cab72a` from
two days before the window.

```bash
git log --all --since='<mon> 00:00:00' --until='<sun> 23:59:59' \
        --merges --date=short --pretty='%h %ad %s'
```

## What moved between states

Read this from **the index's own history**, not from the index as it stands
today. Today's `docs/PLANS.md` says where a plan ended up, which for a recap
written days after its window is a fact about today. Diff the index across the
window instead, reusing Stage 0's parser:

```bash
.venv/bin/python - <<'PY'
import subprocess, sys
sys.path.insert(0, "scripts")
from audit_plan_state_history import revisions, state_map

START, END = "<mon>", "<sun>"

def at(sha):
    return state_map(subprocess.run(["git", "show", f"{sha}:docs/PLANS.md"],
                                    capture_output=True, text=True).stdout)

revs = revisions()
before = [s for s, d in revs if d < START]
inside = [(s, d) for s, d in revs if START <= d <= END]
print(f"{len(inside)} index revisions in the window")
a = at(before[-1]) if before else {}
b = at(inside[-1][0]) if inside else a
for n in sorted(set(a) | set(b)):
    if a.get(n) != b.get(n):
        print(f"  Plan {n}: {a.get(n, '(absent)')} -> {b.get(n, '(absent)')}")
PY
```

`(absent) -> build` is a plan that entered the index that week. A plan whose
**document** was created in the window but which appears in no state change is
worth a line in *What is still owed* — a plan document in no table is precisely
the defect Plan 146 was written to fix, and a recap is where it becomes
visible.

**The general rule: a fact about the window comes from git inside the window; a
fact about the present carries the date you are writing on.** "Plan 131 is
archived" is a claim about today, not about the week — write it as *"as of
<run date>, Plan 131 is archived"*, or leave it out.

## Opening the plan documents

For every plan the week touched, open `docs/plans/plan_NNN_*.md` and take the
*why* from it — the problem, the constraint, what the stage was gating on.
**If the recap reads like `git log` with better grammar, this step did not
happen** and the skill has produced a changelog.

Two mechanical traps:

- **Link to a plan document where it lives now, not where the commit touched
  it.** Stage 3 moved every plan document into `docs/plans/`, so a commit from
  before that move shows `docs/plan_131_packed_cold_storage.md` in its `--stat`
  and the live path is `docs/plans/plan_131_packed_cold_storage.md`. Recap
  links are relative to `docs/recaps/`, so `../plans/plan_131_....md`. The
  suite's dangling-link check walks every markdown file under `docs/` and will
  fail on a wrong one — which is the check working, not a nuisance.
- **You may not edit what you open.** Plan documents are read-only here, status
  markers included.

## The recap's shape

`docs/recaps/<sunday>.md`, and these headings verbatim — the suite checks for
them, and stable headings are what make the files comparable week to week.

```markdown
# Week of <mon> to <sun>

**Window:** <mon> 00:00:00 to <sun> 23:59:59, local author time
**Recapped:** <the date you are writing>
**Commits in window:** N (M non-merge, K merges)

## What shipped

Grouped by plan, newest work last. Each plan gets its *why* from
[its document](../plans/plan_NNN_....md), then what the week actually did to
it. Commits are cited by short sha.

## What moved between states

From the index's own history across the window. "Nothing moved" is a finding
and belongs here as a sentence.

## What is still owed

What the week left open: a gate not yet met, a plan document with no table row,
a defect recorded rather than fixed.

## Unattributed commits

Present even when empty, and it names shas and subjects rather than a count.
When empty, say so in a sentence.

## Merges

Every merge in the window: sha, PR number, the branch's plan.

## Deferred to the next recap

The commits after this window's end, counted, with the command that counted
them.
```

Every one of those sections is required. **Present even when empty** applies to
all of them — an empty section that says "nothing this week" is a fact; a
missing section is silence you cannot tell apart from an oversight.

### The unattributed section is not optional and not a number

Name the shas and the subjects. A recap that quietly covers 40 of 42 commits is
the old "Plan inventory" section again — it covered 30 of 72 plan documents and
said nothing whatsoever about the other 42, which is how Plan 146 started.

### The deferred count is not optional either

Measured on 2026-08-21: 55 commits inside the window, **117 after it**. The
tail is twice the window, and a recap that ignores it without saying so is
indistinguishable from one that lost it.

```bash
git rev-list --count --all --since='<sun> 23:59:59'
```

## Reconcile before you report

The denominator must come out exactly. Run this against the file you wrote:

```bash
git rev-list --count --all --since='<mon> 00:00:00' --until='<sun> 23:59:59'

# Every commit in the window that does not appear in the recap:
comm -23 \
  <(git log --all --since='<mon> 00:00:00' --until='<sun> 23:59:59' \
            --pretty='%h' | sort -u) \
  <(grep -oE '\b[0-9a-f]{7}\b' docs/recaps/<sun>.md | sort -u)
```

Empty output, or you have dropped a commit. A sha that appears in the recap and
not in the window is also worth a look — usually a sha quoted from a commit
body, which is fine, but check.

## After writing

```bash
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -q
git status --short
```

`git status` must show **only** the new file under `docs/recaps/`. If
`docs/PLANS.md`, `docs/planning/completed_plans.md` or any plan document is
modified, you have crossed the boundary this skill exists to hold — revert it
and report what happened.

Then report, briefly:

- the window, the commit count, and the split between merges and non-merges
- how many commits carry a plan and how many do not, by sha
- how many commits are deferred to the next recap
- how many weeks, if any, are still missing from `docs/recaps/`
- anything you noticed that the user may want the `plans` skill to act on —
  said as an observation, never performed

## What this skill must never do

- **move a row, or edit any of the five tables.** That is the `plans` skill.
- **change a plan document**, status marker included. It reads them and writes
  elsewhere.
- **drop a commit it could not attribute.** Name it.
- **emit `*(observed)*`, `*(corroborated)*` or `*(inferred)*`.**
- **regenerate an existing recap silently.**
- **write anywhere under `docs/` except `docs/recaps/`.** Not `PLANS.md`, and
  not the decision log — the log holds decisions, recaps hold events, and one
  week described in two places is the duplication Plan 146 removed.
- **grow a list of special cases.** If a commit seems to need an exception, the
  rule is wrong. Fix the rule or record the gap in the recap.
