# Plan 138 Stage 9 — the slice-cell churn, re-measured

**Taken 2026-09-03**, against `origin/master` at `b578d97`, for
[Plan 138](../plans/plan_138_public_surface_refresh.md) Stage 9 exit 5. The
stage was raised on a measurement of how often the published planned list was
rewritten because an internal pointer moved; a stage about churn that never
re-measures its own churn has demonstrated nothing, so this is the re-run.

## What was measured

For every commit on `master`'s first-parent line in the 60 days to the cutoff
that touched `docs/PLANS.md`, the published planned window — the first four
build-order rows and the six fields the artifact carries for each — is projected
from that file **before and after the commit**, and the two are compared. A
commit "changed published copy" when the projections differ. It is
**slice-only** when they differ, the same four plans are in the window in the
same order, and `summary` is the only field that moved.

Three rules are projected over the same commits:

| Rule | `summary` comes from |
|---|---|
| **old** | the build order's **Next executable slice** cell — what the generator published before this stage |
| **new-as-of** | the commit's own plan documents' `## What this plan is for`, falling back to the slice cell |
| **new-today** | the plan documents **as they stand at `HEAD`**, falling back to the slice cell |

**new-as-of is the literal replay and new-today is the counterfactual, and the
counterfactual is the one that answers the question.** `## What this plan is for`
did not exist until Plan 172's Stage A landed it on 2026-09-02, so under the
literal replay almost every commit in the window falls back to the cell and the
new rule scores the same as the old one. That is a fact about when the sections
were written, not about the rule.

## The result — 60 days to 2026-09-03

| | commits |
|---|---:|
| touched `docs/PLANS.md` on the first-parent line | 104 |
| comparable — a `Default build order` table on both sides | **87** |
| not comparable — the index had no build order yet | 17 |

| Rule | changed published copy | of which slice-only |
|---|---:|---:|
| old | 66 (76%) | **37** |
| new-as-of | 65 | 36 |
| new-today | 45 | **16** |

**The 16 that survive are the fallback working, not the rule failing.** Every one
of them has at least one plan in its window with no `## What this plan is for` at
`HEAD` — plans 136, 139, 140, 141, 142, 144, 145, 147 and 161, which are either
archived since or on `tests/test_planning_docs.py`'s named waiver list. **None of
them is in today's published window.**

So the measurement is repeated over the commits where the question is actually
live: those whose window contains only plans that carry the section today, which
is the state the published-window assertion now holds every future window to.

| | commits |
|---|---:|
| comparable commits whose window is entirely conformant | 29 |
| of those, slice-only public edits under the **old** rule | **20** |
| of those, slice-only public edits under the **new** rule | **0** |

**That is the stage's result: 20 → 0.** A slice-cell repoint on a conforming row
no longer reaches the landing page at all.

## Reproducing the original figure

Stage 9 was raised on a measurement over the 60 days to 2026-09-02 recording
**79 comparable commits, 59 changed (75%), 35 slice-only**. That measurement's
recipe was never written down, so the script below was re-derived and checked
against it by replaying the same window:

| | recorded 2026-09-02 | this recipe |
|---|---:|---:|
| comparable | 79 | 80 |
| changed published copy | 59 | 60 |
| slice-only | 35 | 33 |

Close enough to treat as the same measurement — the one-commit difference in the
first two rows is a window boundary, and the two-commit difference in the third
is a classification edge. **The comparison that carries weight is old against new
under one recipe, not this recipe against the earlier number.** Over that same
window this recipe gives 33 slice-only under the old rule and 16 under new-today,
with the conformant-window figure again 16 → 0.

**Two decisions inside the recipe are worth naming**, because both were made to
raise comparability rather than to flatter the result:

- **Columns are read by name.** The build order carried seven columns until
  `Workable?` and `Blocked by` were added; a positional reader drops that whole
  era as unparseable. Reading by name recovers 17 commits, and it is what closed
  most of the gap to the recorded 79.
- **Only unescaped pipes split a row.** One live row carried `` `\|= "403"` ``
  inside a code span. Splitting on every `|` gives the row one cell too many and
  discards it.

The 17 commits still counted not-comparable are the pre-Plan-146 index, which had
`## Active` and `## Backlog` and no build order at all. There was no published
planned window then, so there is nothing to compare.

## The recipe

Run from a checkout with `origin/master` fetched:

```
python measure_slice_churn.py . 2026-09-03 60
```

```python
"""Re-run Plan 138 Stage 9's churn measurement, under the old rule and the new one.

Replays master's first-parent line over a 60-day window. For every commit that
touched docs/PLANS.md, projects the published planned list from that file before
and after the commit, and asks whether the public copy changed.

Two rules:

  old  -- summary is the build order's Next executable slice cell (what the
          generator published before Stage 9).
  new  -- summary is the plan document's '## What this plan is for' when the
          document carries one, falling back to the slice cell (Stage 9).

The new rule is evaluated twice, because the honest answer differs:

  new-asof  -- plan documents as they were at that commit. Nearly every commit
               in the window predates the sections, so this shows what would
               literally have happened.
  new-today -- plan documents as they are at HEAD. The counterfactual, and the
               number that describes how the rule now behaves.

Usage: python measure_slice_churn.py <repo> <cutoff-YYYY-MM-DD> [days]
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

MAX_ITEMS = 4
INDEX = "docs/PLANS.md"
BUILD_ORDER_HEADING = "Default build order"
_HEADING_RE = re.compile(r"^##\s+(.*?)\s*$", re.M)
_LINK_RE = re.compile(r"\[([^\]]*)\]\(([^)]*)\)")
_CODE_RE = re.compile(r"`([^`]*)`")
_PURPOSE_RE = re.compile(r"^##\s+What this plan is for\s*$(.*?)(?=^#|\Z)", re.M | re.S)


def flatten(text: str) -> str:
    text = _LINK_RE.sub(r"\1", text)
    text = _CODE_RE.sub(r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


class Unparseable(Exception):
    pass


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, encoding="utf-8"
    )
    if result.returncode != 0:
        raise Unparseable(result.stderr.strip()[:200])
    return result.stdout


def build_order_rows(text: str) -> tuple[list[str], list[list[str]]]:
    """The Default build order table's header and data rows, or raise.

    Columns are read by name, not by position: the table carried seven columns
    until Workable?/Blocked by were added, and a positional reader would drop
    that whole era as unparseable rather than measuring it.
    """
    for match in _HEADING_RE.finditer(text):
        if match.group(1) != BUILD_ORDER_HEADING:
            continue
        section = text[match.end():]
        end = _HEADING_RE.search(section)
        section = section[: end.start()] if end else section
        lines = [line for line in section.splitlines() if line.strip().startswith("|")]
        if len(lines) < 3:
            raise Unparseable("no table under the heading")
        parsed = []
        for line in lines:
            # Unescaped pipes only: a `\|` inside a code span is cell content,
            # which the live table has carried more than once.
            cells = re.split(r"(?<!\\)\|", line.strip())
            if len(cells) < 3 or cells[0].strip() or cells[-1].strip():
                raise Unparseable("not a table row")
            parsed.append([cell.strip() for cell in cells[1:-1]])
        header, rows = parsed[0], parsed[2:]
        if not rows:
            raise Unparseable("table has no rows")
        return header, rows
    raise Unparseable("no build order heading")


def project(text: str, purpose: dict[str, str | None] | None) -> list[dict]:
    """The published planned window, under one rule.

    ``purpose`` maps plan number to its '## What this plan is for', or is None
    for the old rule. A number missing from the map falls back to the cell.
    """
    header, rows = build_order_rows(text)
    wanted = ("Order", "Plan", "Title", "Next executable slice", "Priority", "Effort")
    try:
        at = {name: header.index(name) for name in wanted}
    except ValueError as exc:
        raise Unparseable(f"missing column: {exc}") from exc

    items = []
    for row in rows[:MAX_ITEMS]:
        if len(row) != len(header):
            raise Unparseable(f"row has {len(row)} cells, header has {len(header)}")
        link = _LINK_RE.search(row[at["Plan"]])
        if not link:
            raise Unparseable(f"plan cell carries no link: {row[at['Plan']][:40]!r}")
        number = link.group(1).strip()
        summary = None
        if purpose is not None:
            summary = purpose.get(number)
        if summary is None:
            summary = flatten(row[at["Next executable slice"]])
        effort = flatten(row[at["Effort"]])
        items.append(
            {
                "plan": number,
                "order": row[at["Order"]],
                "title": flatten(row[at["Title"]]),
                "priority": flatten(row[at["Priority"]]),
                "effort": effort.split()[0] if effort else "",
                "summary": summary,
            }
        )
    return items


def purpose_map_at(repo: Path, ref: str, numbers: set[str]) -> dict[str, str | None]:
    """Each plan's '## What this plan is for' at ``ref``, or None if absent."""
    out: dict[str, str | None] = {}
    try:
        listing = git(repo, "ls-tree", "-r", "--name-only", ref, "docs/plans/").splitlines()
    except Unparseable:
        return out
    for number in numbers:
        candidates = [
            path
            for path in listing
            if re.match(rf"docs/plans/plan_{re.escape(number)}_.*\.md$", path)
        ]
        text = None
        for path in candidates:
            try:
                body = git(repo, "show", f"{ref}:{path}")
            except Unparseable:
                continue
            if len(candidates) > 1 and not re.match(
                rf"^#\s+Plan\s+{re.escape(number)}\s*:", body.splitlines()[0] if body else ""
            ):
                continue
            text = body
            break
        if text is None:
            out[number] = None
            continue
        match = _PURPOSE_RE.search(text)
        out[number] = flatten(match.group(1)) if match else None
    return out


def main() -> int:
    repo = Path(sys.argv[1])
    cutoff = date.fromisoformat(sys.argv[2])
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 60
    since = cutoff - timedelta(days=days)

    commits = git(
        repo,
        "log",
        "--first-parent",
        "origin/master",
        f"--since={since.isoformat()}",
        f"--until={cutoff.isoformat()}",
        "--format=%H %ad",
        "--date=short",
        "--",
        INDEX,
    ).split("\n")
    commits = [line for line in commits if line.strip()]

    tallies = {
        "touching": len(commits),
        "comparable": 0,
        "unparseable": 0,
        "old_changed": 0,
        "old_slice_only": 0,
        "new_asof_changed": 0,
        "new_asof_slice_only": 0,
        "new_today_changed": 0,
        "new_today_slice_only": 0,
        "conformant_window": 0,
        "conformant_old_slice_only": 0,
        "conformant_new_asof_slice_only": 0,
        "conformant_new_today_slice_only": 0,
    }
    detail = []

    for line in commits:
        sha, when = line.split()[0], line.split()[1]
        try:
            after = git(repo, "show", f"{sha}:{INDEX}")
            before = git(repo, "show", f"{sha}^:{INDEX}")
        except Unparseable:
            tallies["unparseable"] += 1
            continue

        try:
            pairs = {}
            for rule in ("old", "new_asof", "new_today"):
                if rule == "old":
                    b, a = project(before, None), project(after, None)
                else:
                    numbers = {
                        item["plan"]
                        for item in project(before, None) + project(after, None)
                    }
                    if rule == "new_asof":
                        b = project(before, purpose_map_at(repo, f"{sha}^", numbers))
                        a = project(after, purpose_map_at(repo, sha, numbers))
                    else:
                        today = purpose_map_at(repo, "HEAD", numbers)
                        b = project(before, today)
                        a = project(after, today)
                pairs[rule] = (b, a)
        except Unparseable as exc:
            tallies["unparseable"] += 1
            detail.append({"sha": sha[:9], "date": when, "skipped": str(exc)})
            continue

        tallies["comparable"] += 1
        row = {"sha": sha[:9], "date": when}

        # A window every one of whose plans carries the section today -- the
        # state tests/test_planning_docs.py now holds every future window to.
        window = {item["plan"] for pair in pairs.values() for item in pair[0] + pair[1]}
        conformant = all(
            section is not None for section in purpose_map_at(repo, "HEAD", window).values()
        )
        tallies["conformant_window"] += conformant
        for rule, (b, a) in pairs.items():
            changed = b != a
            same_membership = [i["plan"] for i in b] == [i["plan"] for i in a]
            only_summary = changed and same_membership and all(
                {k: v for k, v in x.items() if k != "summary"}
                == {k: v for k, v in y.items() if k != "summary"}
                for x, y in zip(b, a)
            )
            key = "old" if rule == "old" else rule
            tallies[f"{key}_changed"] += changed
            tallies[f"{key}_slice_only"] += only_summary
            if conformant:
                tallies[f"conformant_{key}_slice_only"] += only_summary
            row[rule] = "slice-only" if only_summary else ("changed" if changed else "-")
            if rule == "new_today" and only_summary:
                today = purpose_map_at(
                    repo, "HEAD", {item["plan"] for item in b + a}
                )
                row["no_section_today"] = sorted(
                    number for number, section in today.items() if section is None
                )
        detail.append(row)

    print(json.dumps({"window": [since.isoformat(), cutoff.isoformat()], **tallies}, indent=2))
    print()
    for row in detail:
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```
