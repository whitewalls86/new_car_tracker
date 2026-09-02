#!/usr/bin/env python3
"""Reconstruct every plan's state timeline from ``docs/PLANS.md``'s own history.

Plan 146 Stage 1. The index has always recorded which section a plan sits in;
it has never recorded *when the plan moved*. Git has, for 134 revisions, and
walking them turns "no completion date was written down" into an observed fact
for most plans and an explicit guess for the rest.

The output is deliberately tiered, because the failure mode this stage can
introduce is worse than the gap it closes -- an unmarked inferred date is
manufactured history:

* ``observed``     -- a dated transition seen in ``PLANS.md``'s own history.
* ``corroborated`` -- the plan document claims a date and git agrees.
* ``inferred``     -- no transition was ever recorded; the plan document's
                      last-touched commit date stands in. **This is a guess.**

Known limits, stated so the sweep is not mistaken for completeness:

* Conventional commit prefixes (``docs(plan-146)``) appear in 11 commits and
  are recent, so commit *messages* are not a usable signal. File history is.
* A plan number can exist with **no document at all**. Seven do: 24, 52, 55,
  56, 57, 59 and 65 were named in commits or in ``PLANS.md`` headings, never
  got a file, and are in no archive row. ``--coverage`` reports them. This is
  strictly worse than a stale table row, because there is nothing left to be
  stale -- Plan 24 shipped at ``0377460`` and appears nowhere else in the repo.
* A plan document that was **deleted** leaves no trace in ``ls docs/``, so the
  sweep also walks ``--diff-filter=D`` over ``docs/`` and reports plan numbers
  whose document is gone. This is how Plan 65 was found: it shipped at
  ``eb96c41``, its document was deleted the next day, and it appears in no
  table and no archive.
* Extraction is table-driven: a section contributes plan numbers only from
  markdown tables that have a ``Plan`` column. Early-March revisions wrote
  plans as prose under ``## Plan N:`` headings, so those revisions yield
  nothing. The archive's oldest entries (Plans 0-7) predate plan documents
  entirely and are out of scope by construction.

Usage::

    python scripts/audit_plan_state_history.py            # markdown report
    python scripts/audit_plan_state_history.py --json     # machine-readable
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
INDEX = "docs/PLANS.md"
# Plan 146 Stage 3 moved the plan documents into ``docs/plans/``. The index and
# the archive keep their own homes: ``PLANS.md`` at the ``docs/`` root, the
# archive under ``docs/planning/``.
PLANS_DIR = "docs/plans"
ARCHIVE = "docs/planning/completed_plans.md"

# Section heading -> canonical state. Headings were renamed repeatedly over the
# history; the canonical set is Plan 146's four states plus ``superseded``,
# which is a terminal state that is not "finished".
STATE_BY_HEADING = {
    "active": "build",
    "in-progress / nearly complete": "build",
    "remaining priority order": "build",
    "default build order": "build",
    # Both closeout headings are live: the long form ran until Plan 146 Stage 2
    # shortened it at ``0c08382`` on 2026-08-21, and this sweep reads all 189
    # revisions, so dropping either loses a real state from part of the history.
    # The short form was missing for nine days, during which every closeout plan
    # read as ``absent`` -- Plan 136 as having left the index, Plans 149 and 160
    # as never arriving. ``TestTheStateParserClassifiesEveryLiveHeading`` in
    # ``tests/test_planning_docs.py`` is what stops the next rename doing that.
    "current closeout": "closeout",
    "current closeout -- finish before opening another large build": "closeout",
    "operational monitoring and completed implementation awaiting closeout": "closeout",
    "operational watch list and completed implementation awaiting closeout": "closeout",
    "operational watch list": "closeout",
    "backlog": "backlog",
    "future ideas (unprioritized)": "backlog",
    "paused or blocked": "backlog",
    "completed": "archive",
    "superseded": "superseded",
    "superseded plans": "superseded",
}

# Headings that assert no state. ``Plan inventory`` is here on purpose: it is a
# parallel status surface, and treating it as a state is what let a plan sit in
# two places at once.
IGNORED_HEADINGS_EXACT = {"plan inventory"}
IGNORED_HEADING_PREFIXES = (
    "current state",
    "coordinating roadmap",
    "how priority",
    "sequencing rationale",
    "notes",
    "read me first",
    "workability audit",
    "plan ",  # per-plan detail sections in early revisions
)

PLAN_LINK = re.compile(
    # ``plans/`` is optional: revisions of the index older than Stage 3 link to
    # the flat path, and this sweep reads every revision.
    r"\[(\d+)\]\((?:plans/)?(?:implementation_)?plan_\d+[^)]*\.md\)")
BARE_PLAN = re.compile(r"^\*{0,2}(\d{1,3})\*{0,2}$")


def _git(*args: str) -> str:
    # ``encoding`` is explicit because ``text=True`` alone decodes with the
    # locale codec. The index has carried non-ASCII since it was written (em
    # dashes in every gate cell), so on a cp1252 host this raises
    # ``UnicodeDecodeError`` part-way through the history walk and the sweep
    # cannot run at all. Same defect, same fix, as ``b80f387`` applied to the
    # test suite's ``Path.read_text()`` calls on 2026-08-25.
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout


def revisions() -> list[tuple[str, str]]:
    """(sha, YYYY-MM-DD) for every revision of the index, oldest first."""
    out = _git("log", "--follow", "--reverse", "--date=short",
               "--format=%H %ad", "--", INDEX)
    return [tuple(line.split(" ", 1)) for line in out.splitlines() if line.strip()]


def _cells(row: str) -> list[str]:
    return [c.strip() for c in row.strip().strip("|").split("|")]


def _plan_number(cell: str) -> int | None:
    match = PLAN_LINK.search(cell)
    if match:
        return int(match.group(1))
    match = BARE_PLAN.match(cell)
    if match:
        return int(match.group(1))
    return None


def plans_in_section(body: str) -> set[int]:
    """Plan numbers keyed off each table's ``Plan`` column.

    Keying on the column rather than "first number in the row" matters: the
    backlog table leads with a priority score, and reading that as a plan
    number invents plans 55, 52, 40, 38, 30 and 25 out of nothing.
    """
    found: set[int] = set()
    plan_column: int | None = None
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            plan_column = None
            continue
        cells = _cells(line)
        lowered = [c.lower() for c in cells]
        if "plan" in lowered:
            plan_column = lowered.index("plan")
            continue
        if set("".join(cells)) <= set("-: "):  # separator row
            continue
        if plan_column is None or plan_column >= len(cells):
            continue
        number = _plan_number(cells[plan_column])
        if number is not None:
            found.add(number)
    return found


def state_map(text: str) -> dict[int, str]:
    """plan number -> canonical state, for one revision of the index."""
    states: dict[int, str] = {}
    sections = re.split(r"^## +", text, flags=re.M)
    for section in sections[1:]:
        heading, _, body = section.partition("\n")
        key = heading.strip().replace("—", "--").rstrip("#").strip().lower()
        if key in IGNORED_HEADINGS_EXACT or key.startswith(IGNORED_HEADING_PREFIXES):
            continue
        state = STATE_BY_HEADING.get(key)
        if state is None:
            continue
        for number in plans_in_section(body):
            # ``archive`` and ``superseded`` are terminal: if a revision lists a
            # plan in both a live section and a terminal one, the terminal claim
            # is the one worth recording as a transition.
            if states.get(number) in ("archive", "superseded"):
                continue
            states[number] = state
    return states


def build_timelines() -> dict[int, list[dict]]:
    timelines: dict[int, list[dict]] = defaultdict(list)
    previous: dict[int, str] = {}
    for sha, date in revisions():
        try:
            text = _git("show", f"{sha}:{INDEX}")
        except subprocess.CalledProcessError:
            continue
        current = state_map(text)
        for number, state in current.items():
            if previous.get(number) != state:
                timelines[number].append({"date": date, "sha": sha[:7], "state": state})
        for number, state in previous.items():
            if number not in current:
                timelines[number].append({"date": date, "sha": sha[:7], "state": "absent"})
        previous = current
    return dict(timelines)


def plan_files() -> dict[int, list[str]]:
    files: dict[int, list[str]] = defaultdict(list)
    for path in sorted((REPO_ROOT / PLANS_DIR).glob("*plan_*.md")):
        match = re.search(r"plan_(\d+)_", path.name)
        if match:
            files[int(match.group(1))].append(path.name)
    return dict(files)


def file_dates(name: str) -> tuple[str | None, str | None]:
    """(created, last touched) for a plan document, ignoring pure renames.

    Stage 3 moved all 79 documents in one commit. Counting that commit as a
    touch would date every plan to the day of the move and quietly destroy the
    only signal ``inferred`` guesses have. ``--numstat`` reports ``0 0`` for a
    rename that changed no content, which is exactly the commit to skip.
    """
    out = _git("log", "--follow", "--date=short", "--format=%x00%ad",
               "--numstat", "--", f"{PLANS_DIR}/{name}")
    dates: list[str] = []
    pending: str | None = None
    for line in out.splitlines():
        if line.startswith("\x00"):
            pending = line[1:].strip()
        elif line.strip() and pending is not None:
            added, deleted, _ = line.split("\t", 2)
            if added != "0" or deleted != "0":
                dates.append(pending)
            pending = None
    if not dates:
        return None, None
    return dates[-1], dates[0]  # created, last touched


CLAIMED_DATE = re.compile(
    r"(?:complete|completed|superseded)\b\D{0,60}?(\d{4}-\d{2}-\d{2})", re.I)


def claimed_completion(name: str) -> str | None:
    """A completion date the plan document itself asserts, if any."""
    text = (REPO_ROOT / PLANS_DIR / name).read_text(encoding="utf-8")
    match = CLAIMED_DATE.search(text)
    return match.group(1) if match else None


DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def recorded_dates() -> dict[int, tuple[str, str]]:
    """plan number -> (date, where) for dates already written in a table.

    Two tables record a completion date today: ``PLANS.md``'s ``Completed``
    section and the archive. A date already written down is not something this
    sweep gets to overwrite -- it is the thing git corroborates.
    """
    found: dict[int, tuple[str, str]] = {}
    for relative in (ARCHIVE, INDEX):
        path = REPO_ROOT / relative
        if not path.exists():
            continue
        in_completed = relative.endswith("completed_plans.md")
        plan_column = date_column = None
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("## "):
                in_completed = stripped.lower().startswith("## completed")
                plan_column = date_column = None
                continue
            if not in_completed or not stripped.startswith("|"):
                continue
            cells = _cells(stripped)
            lowered = [c.lower() for c in cells]
            if "plan" in lowered:
                plan_column = lowered.index("plan")
                date_column = next(
                    (i for i, c in enumerate(lowered) if c in ("date", "completed")), None)
                continue
            if plan_column is None or date_column is None:
                continue
            if max(plan_column, date_column) >= len(cells):
                continue
            number = _plan_number(cells[plan_column])
            match = DATE.search(cells[date_column])
            if number is not None and match and number not in found:
                found[number] = (match.group(0), relative)
    return found


MENTION = re.compile(r"plan[\s_#-]*(\d{1,3})", re.I)


def mentioned_numbers() -> dict[int, list[dict]]:
    """Plan numbers named in a commit *subject* on any ref, oldest first.

    Subjects only, because this feeds the evidence column and a number buried
    in a commit body is rarely that commit's subject matter. ``coverage()``
    searches bodies too, but only to decide whether a number was ever real.
    """
    found: dict[int, list[dict]] = defaultdict(list)
    raw = _git("log", "--all", "--date=short", "--format=%x00%h %ad %s")
    for entry in raw.split("\x00"):
        entry = entry.strip()
        if not entry or entry.count(" ") < 2:
            continue
        sha, date, subject = entry.split(" ", 2)
        for number in {int(m.group(1)) for m in MENTION.finditer(subject)}:
            found[number].append({"sha": sha, "date": date, "subject": subject})
    for rows in found.values():
        rows.sort(key=lambda row: row["date"])
    return dict(found)


def index_numbers() -> set[int]:
    """Every plan number that ever appeared in any revision of the index."""
    numbers: set[int] = set()
    for sha, _ in revisions():
        try:
            text = _git("show", f"{sha}:{INDEX}")
        except subprocess.CalledProcessError:
            continue
        numbers |= {int(m.group(1)) for m in MENTION.finditer(text)}
    return numbers


def archived_numbers() -> set[int]:
    """Plan numbers claimed by an archive row.

    The ``Plan`` cell is not a number. It is one of: a plan number (``93``), a
    sub-item (``14.5``, which archives part of Plan 14 and says nothing about
    Plan 5), several plans at once (``54+58``, ``62 + 63``), a plan with a
    scope note (``71 (steps 8-9, 13+tests+dashboard)``), or a label with no
    number at all (``Silver flush``, ``V029``).

    Reading every digit in the cell is what made Plans 5 and 6 look archived --
    they are the fractional halves of rows ``14.5`` and ``14.6`` -- and hid two
    real unrecorded plans behind a parsing artifact. So: drop parentheticals,
    split on the combining separators, and take the *leading* integer of each
    segment only.
    """
    numbers: set[int] = set()
    for line in (REPO_ROOT / ARCHIVE).read_text(
    encoding="utf-8",
            ).splitlines():
        if not line.startswith("|"):
            continue
        cells = _cells(line)
        if not cells:
            continue
        cell = re.sub(r"\([^)]*\)", " ", cells[0])
        for segment in re.split(r"[+,&]|\band\b", cell):
            match = re.match(r"\s*\**(\d{1,3})", segment)
            if match:
                numbers.add(int(match.group(1)))
    return numbers


def ever_mentioned() -> set[int]:
    """Every plan number this repo has ever named, anywhere.

    Four sources, because each one alone misses plans the others catch: commit
    subjects *and bodies* on all refs, every revision of the index, and the
    current contents of ``docs/``. Plan 24 exists only in a commit subject;
    Plans 5, 52, 55, 56, 57 and 59 exist only in the index's history.
    """
    numbers: set[int] = set()
    log = _git("log", "--all", "--format=%s%n%b")
    numbers |= {int(m.group(1)) for m in MENTION.finditer(log)}
    numbers |= index_numbers()
    for path in (REPO_ROOT / "docs").rglob("*.md"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        numbers |= {int(m.group(1)) for m in MENTION.finditer(text)}
    return numbers


def coverage() -> dict:
    """Plan numbers the repo has mentioned that no surface currently records.

    Also reports the numbers that were never used at all. Both halves matter:
    without the second, an unrecorded plan and a skipped number look the same,
    which is the "silence is indistinguishable from non-existence" defect this
    plan was written about, one level up.
    """
    mentions = mentioned_numbers()
    universe = ever_mentioned()
    on_disk = set(plan_files())
    archived = archived_numbers()
    # ``PLANS.md`` places some document-less numbers (87 superseded, 88
    # backlog), so read the live index rather than its history when asking
    # "is this recorded today".
    live = state_map((REPO_ROOT / INDEX).read_text(encoding="utf-8"))
    in_index_history = index_numbers()

    # A bare number in the archive's ``Plan`` column is not the word "plan"
    # followed by a number, so ``universe`` alone reports plans 38-49 as never
    # used. Recorded-anywhere is the real denominator.
    known = universe | on_disk | archived | set(live)
    highest = max(known)
    unrecorded = []
    for number in sorted(n for n in universe if n <= highest):
        if number in on_disk or number in archived or number in live:
            continue
        unrecorded.append({
            "plan": number,
            "commits": mentions.get(number, []),
            "in_index_history": number in in_index_history,
        })
    return {
        "unrecorded": unrecorded,
        "never_used": [n for n in range(highest + 1) if n not in known],
        "highest": highest,
    }


DELETED_DOC = re.compile(
    r"^docs/(?:plans/)?(?:implementation_)?plan_(\d+)[^/]*\.md$")


def deleted_documents() -> list[dict]:
    """Plan documents that existed on this branch and no longer do.

    ``ls docs/`` is complete for what is *there*; it says nothing about what
    was removed. A plan whose document was deleted before anyone archived it
    disappears from every surface at once, which is the least recoverable
    version of this plan's defect.
    """
    # Deletions recorded before Stage 3 name the flat path, deletions after it
    # name ``docs/plans/``; a document that still exists must match neither.
    present = {
        prefix + p.name
        for p in (REPO_ROOT / PLANS_DIR).glob("*plan_*.md")
        for prefix in ("docs/", f"{PLANS_DIR}/")
    }
    out = _git("log", "--diff-filter=D", "--date=short",
               "--format=%x00%H %ad %s", "--name-only", "--", "docs/")
    gone: dict[str, dict] = {}
    header = None
    for line in out.splitlines():
        if line.startswith("\x00"):
            sha, date, subject = line[1:].split(" ", 2)
            header = {"sha": sha[:7], "date": date, "subject": subject}
            continue
        line = line.strip()
        if not line or header is None or line in present:
            continue
        match = DELETED_DOC.match(line)
        if match and line not in gone:
            gone[line] = {"path": line, "plan": int(match.group(1)), **header}
    return sorted(gone.values(), key=lambda row: (row["plan"], row["date"]))


def resolve() -> list[dict]:
    timelines = build_timelines()
    files = plan_files()
    recorded = recorded_dates()
    rows = []
    for number in sorted(set(files) | set(timelines) | set(recorded)):
        names = files.get(number, [])
        timeline = timelines.get(number, [])
        terminal = next(
            (t for t in timeline if t["state"] in ("archive", "superseded")), None)
        created = touched = claimed = None
        if names:
            created, touched = file_dates(names[0])
            claimed = claimed_completion(names[0])

        written, source = recorded.get(number, (None, None))
        stated = written or claimed
        stated_source = source or (names[0] if claimed else None)

        if stated and terminal:
            tier, date = "corroborated", stated
            evidence = (f"{stated_source} records {stated}; the index moved it to "
                        f"{terminal['state']} at {terminal['sha']} ({terminal['date']})")
        elif terminal:
            tier, date = "observed", terminal["date"]
            evidence = (f"the index moved it to {terminal['state']} at "
                        f"{terminal['sha']} ({terminal['date']})")
        elif stated:
            tier, date = "corroborated", stated
            evidence = f"{stated_source} records {stated}; no index transition recorded"
        elif touched:
            tier, date = "inferred", touched
            evidence = f"GUESS -- no transition ever recorded; last touched {touched}"
        else:
            tier, date, evidence = "inferred", None, "GUESS -- no evidence at all"

        rows.append({
            "plan": number,
            "files": names,
            "created": created,
            "last_touched": touched,
            "claimed": claimed,
            "recorded": written,
            "tier": tier,
            "date": date,
            "evidence": evidence,
            "timeline": timeline,
            "current_state": timeline[-1]["state"] if timeline else None,
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON, not markdown")
    parser.add_argument("--plan", type=int, action="append",
                        help="restrict output to these plan numbers")
    parser.add_argument("--deleted", action="store_true",
                        help="only report plan documents that were deleted")
    parser.add_argument("--coverage", action="store_true",
                        help="only report plan numbers no surface records")
    args = parser.parse_args()

    if args.coverage:
        result = coverage()
        if args.json:
            print(json.dumps(result, indent=2))
            return
        print("| Plan | Ever in the index | Commits naming it |")
        print("|---|---|---|")
        for row in result["unrecorded"]:
            commits = " ; ".join(
                f"`{c['sha']}` {c['date']} {c['subject']}" for c in row["commits"]) or "--"
            print(f"| {row['plan']} | {'yes' if row['in_index_history'] else 'no'} "
                  f"| {commits} |")
        print(f"\n{len(result['unrecorded'])} unrecorded of 0-{result['highest']}.")
        print(f"Never used at all: {result['never_used']} -- gaps in the number "
              f"line, not lost plans.")
        return

    if args.deleted:
        gone = deleted_documents()
        if args.json:
            print(json.dumps(gone, indent=2))
            return
        print("| Plan | Deleted document | Deleted at | Commit |")
        print("|---|---|---|---|")
        for row in gone:
            print(f"| {row['plan']} | `{row['path']}` | {row['date']} | "
                  f"`{row['sha']}` {row['subject']} |")
        return

    rows = resolve()
    if args.plan:
        rows = [r for r in rows if r["plan"] in set(args.plan)]

    if args.json:
        print(json.dumps(rows, indent=2))
        return

    print("| Plan | Current | Date | Tier | Evidence |")
    print("|---|---|---|---|---|")
    for row in rows:
        print(f"| {row['plan']} | {row['current_state'] or '--'} | "
              f"{row['date'] or '--'} | {row['tier']} | {row['evidence']} |")
    print()
    for row in rows:
        if not row["timeline"]:
            continue
        moves = " -> ".join(f"{t['state']}@{t['date']}" for t in row["timeline"])
        print(f"{row['plan']:>4}: {moves}")


if __name__ == "__main__":
    main()
