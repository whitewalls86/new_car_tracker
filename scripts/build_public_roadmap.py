"""Plan 138 Stage 1d: project the roadmap onto the public page, at build time.

Emits ``ops/static_ops/generated/project-updates.json`` from **two explicitly named
tables in two different files** -- the **Default build order** in
``docs/PLANS.md``, and the archive table in ``docs/planning/completed_plans.md``.
The landing page renders that file; nothing parses Markdown or calls GitHub on a
public request.

**Why two files, named rather than discovered.** There is no Completed table in
``docs/PLANS.md`` any more: Plan 146 replaced it with a pointer to the archive.
A generator that crawls ``docs/`` for a table under a "Completed" heading finds
that pointer, matches no rows, and publishes an empty list -- the silent
failure, and the reason this file names its two inputs and fails when either
does not look the way it expects. ``PLANS.md``'s third table, **Current
closeout**, is deliberately not an input: its rows are deployed work whose
evidence is still pending, which is neither planned next nor recently
completed. Such a plan appears in *neither* list until its completion row lands
in the archive, which is the operational form of the truth contract's §4.

**Why ``as_of`` is read and not generated.** It comes from ``PLANS.md``'s
**Current State** heading, so unchanged input produces byte-identical output and
``--check`` means something. A wall-clock timestamp would make every run a diff
and turn the drift check into noise.

**The completed side is assembled differently, because the archive is a
different table.** It is ``| Plan | Description | Date |``: no title column, no
priority, no effort, and a bare number rather than a link. So the title is the
**bolded lead** of the Description cell, the summary is the sentence after it,
priority and effort are not emitted at all, and the link has to be synthesised
from the plan number.

**That synthesis was ambiguous, and is not allowed to guess even now that it is
not.** ``plan_145_*.md`` used to match nine files -- the main document plus
eight stage handoffs -- and 123, 125, 110, 120 and 142 each held a second
document too. On 2026-09-01 the handoffs and implementation prompts moved to
``docs/prompts/`` and the reports to ``docs/reference/``, both directories that
already existed for those kinds, leaving ``docs/plans/`` one-to-one: 94
documents, 94 plan numbers.

The disambiguation rule stays anyway. Main documents title themselves
``# Plan <n>: <title>`` and handoffs do not carry the colon, so a document
refiled into ``docs/plans/`` by mistake still resolves rather than corrupting a
public link; anything the rule cannot settle is a build failure naming the
candidates, never a silent pick. The one-to-one property is asserted separately
in the tests, so the guard is the second line of defence rather than the first.

**Effort is read as its leading token.** The build order's effort column carries
qualifiers that plan sizing needs and the public schema does not -- ``M + first
observed window``, ``S + 7d observation``, ``XL, research-gated``, ``XS each``.
Five of the build order's rows fail a strict ``XS|S|M|L|XL`` match, so a whole
cell vocabulary check fails on the day it is written. The qualifier stays in
``PLANS.md``, where it is doing real work.

**A plan says how it wants to be described in public, and extraction is the
fallback.** The archive's Description cells run to several hundred words of
incident narrative naming migrations, services, columns and object paths --
exactly what §4 bars from the feed -- and they are written for a reader who
already knows the system. So a plan document may carry a ``## Public summary``
section, which the close-out skill writes at the moment the archive row is
written, and this script prefers it. Where there is none the archive cell is
cut at its first sentence as before, and the script names every plan it had to
do that to: that list is Gate 1d's worklist, and it empties itself as sections
get written rather than needing the same four rows re-read every build.

Nothing here was solved by adding a column to ``completed_plans.md``. That file
is Plan 146's, and a public-copy column in it would be a change to someone
else's table; a plan's own document is the plan's to write in.

Usage::

    python scripts/build_public_roadmap.py            # write the artifact
    python scripts/build_public_roadmap.py --check    # fail on drift, write nothing
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# One repo-root-relative constant block, matching tests/test_planning_docs.py
# and scripts/audit_plan_state_history.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
INDEX = "docs/PLANS.md"
ARCHIVE = "docs/planning/completed_plans.md"
PLANS_DIR = "docs/plans"
OUTPUT = "ops/static_ops/generated/project-updates.json"

# Links resolve to GitHub rather than to a first-party page. Plan 138's
# non-goals bar publishing plan documents themselves, and the repository is
# already public, so a blob URL is the honest destination.
BLOB_BASE = "https://github.com/whitewalls86/new_car_tracker/blob/master/"

SCHEMA_VERSION = 1

# §4: "Publish only the first four" on each side. The cap is the editorial
# budget as much as a layout one -- four rows is what Gate 1d asks a human to
# read before every deploy of this artifact.
MAX_ITEMS = 4

BUILD_ORDER_HEADING = "Default build order"
BUILD_ORDER_COLUMNS = (
    "Order",
    "Plan",
    "Title",
    "Next executable slice",
    "Workable?",
    "Blocked by",
    "Priority",
    "Effort",
    "Depends on / safe stopping point",
)
ARCHIVE_COLUMNS = ("Plan", "Description", "Date")

# The section a plan document may carry to say how it wants to be described in
# public. Preferred over extracting the archive cell; see authored_summary.
PUBLIC_SUMMARY_HEADING = "Public summary"

# A published summary is one or two sentences. The cap is not a style rule --
# it is what makes an unauthored archive cell fail loudly instead of pushing a
# paragraph of incident narrative onto the landing page, and the fix it points
# at is writing the plan's own Public summary section.
MAX_SUMMARY_CHARS = 320

EFFORT_TOKENS = ("XS", "XL", "S", "M", "L")

_AS_OF_RE = re.compile(r"^##\s+Current State\s+\(as of (\d{4}-\d{2}-\d{2})\)\s*$", re.M)
_HEADING_RE = re.compile(r"^##\s+(.*?)\s*$", re.M)
_TABLE_DIVIDER_RE = re.compile(r"^\|[\s:|-]+\|$")
_LINK_RE = re.compile(r"\[([^\]]*)\]\(([^)]*)\)")
_CODE_RE = re.compile(r"`([^`]*)`")
_BOLD_LEAD_RE = re.compile(r"^\*\*(.+?)\*\*")
_LEAD_SEPARATOR_RE = re.compile(r"^\s*(?:[—–:-]+|--)\s*")
# A sentence ends at .!? only when whitespace or the end of the cell follows.
# What that buys is the dots this corpus is actually full of: `redeploy.sh`,
# `docs/ARCHITECTURE.md:179`, `0.128` -- every one of them a dot followed
# immediately by another character, so none of them ends a sentence. Requiring
# a *capital* next would be the tempting extra guard and is wrong here: these
# cells routinely open a sentence with an identifier, and doing so ran three of
# the four published summaries together into paragraphs. There are no
# abbreviations in the archive today ("e.g.", "i.e.", "vs.": zero occurrences
# across 119 rows), and the three ". lowercase" boundaries in the file are all
# genuine sentence ends followed by an identifier.
_SENTENCE_END_RE = re.compile(r"[.!?](?=\s|$)")
_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\b")
_PLAN_NUMBER_RE = re.compile(r"^([A-Za-z0-9.+]+)$")


class RoadmapBuildError(Exception):
    """A source table did not look the way this script requires.

    Raised rather than worked around: every condition that reaches here is one
    where the alternative is publishing something wrong to a public page.
    """


# ---------------------------------------------------------------------------
# Markdown, reduced to the little that the two tables actually use
# ---------------------------------------------------------------------------

def flatten_markdown(text: str) -> str:
    """Reduce a table cell to the plain text the page will render.

    The landing page writes every string with ``textContent``, so markup left in
    a summary would be shown literally rather than rendered. Links keep their
    text and lose their target -- the item already carries one link, and a
    second one inside prose has nowhere to go in a one-line summary.
    """
    text = _LINK_RE.sub(r"\1", text)
    text = _CODE_RE.sub(r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def first_sentence(text: str) -> str:
    """The first sentence, or the whole string when it holds only one."""
    match = _SENTENCE_END_RE.search(text)
    return text[: match.end()].strip() if match else text.strip()


def bolded_lead(cell: str) -> tuple[str, str]:
    """Split an archive Description cell into its bolded lead and the rest.

    ``**Scrape state ownership -- separating fetch from enrichment** -- ...``
    gives a title and a remainder. The lead is required: a cell without one is
    an archive row this script has no title for, and inventing one from the
    first few words would publish a sentence fragment as a heading.
    """
    match = _BOLD_LEAD_RE.match(cell.strip())
    if not match:
        raise RoadmapBuildError(
            f"{ARCHIVE}: a published row's Description cell has no bolded lead, "
            f"so it has no title: {cell.strip()[:120]!r}"
        )
    remainder = cell.strip()[match.end():]
    return flatten_markdown(match.group(1)), _LEAD_SEPARATOR_RE.sub("", remainder)


def leading_effort_token(cell: str) -> str:
    """The effort token, read off the front and validated there.

    ``M + first observed window`` is an M. The qualifier is planning
    information, not public information, and it stays in ``PLANS.md``.
    """
    cell = flatten_markdown(cell)
    for token in EFFORT_TOKENS:
        if cell.startswith(token) and not cell[len(token):len(token) + 1].isalpha():
            return token
    raise RoadmapBuildError(
        f"{INDEX}: effort cell does not begin with one of "
        f"{'/'.join(EFFORT_TOKENS)}: {cell[:60]!r}"
    )


# ---------------------------------------------------------------------------
# Table location, by name and by column signature
# ---------------------------------------------------------------------------

def _split_row(line: str) -> list[str]:
    cells = line.strip().split("|")
    if len(cells) < 3 or cells[0].strip() or cells[-1].strip():
        raise RoadmapBuildError(f"not a Markdown table row: {line[:120]!r}")
    return [cell.strip() for cell in cells[1:-1]]


def _parse_table(lines: list[str], columns: tuple[str, ...], where: str) -> list[list[str]]:
    """Read a table, holding it to the columns the caller expects.

    The header check is the point. It is what makes this a named table rather
    than the first table that happens to be there, so a column inserted upstream
    fails the build instead of shifting every published field by one.
    """
    header = _split_row(lines[0])
    if tuple(header) != columns:
        raise RoadmapBuildError(
            f"{where}: expected columns {columns}, found {tuple(header)}"
        )
    if len(lines) < 2 or not _TABLE_DIVIDER_RE.match(lines[1].strip()):
        raise RoadmapBuildError(f"{where}: table header is not followed by a divider")

    rows = []
    for line in lines[2:]:
        cells = _split_row(line)
        if len(cells) != len(columns):
            raise RoadmapBuildError(
                f"{where}: row has {len(cells)} cells, expected {len(columns)}: "
                f"{line[:120]!r}"
            )
        rows.append(cells)
    if not rows:
        raise RoadmapBuildError(f"{where}: table has no rows")
    return rows


def _table_under_heading(
    text: str, heading: str, columns: tuple[str, ...], where: str
) -> list[list[str]]:
    """The first table after a named ``##`` heading, bounded by the next one."""
    for match in _HEADING_RE.finditer(text):
        if match.group(1) != heading:
            continue
        section = text[match.end():]
        end = _HEADING_RE.search(section)
        return _parse_table(
            _table_lines(section[: end.start()] if end else section, where),
            columns,
            where,
        )
    raise RoadmapBuildError(f"{where}: no '## {heading}' heading")


def _table_lines(section: str, where: str) -> list[str]:
    lines = [line for line in section.splitlines() if line.strip().startswith("|")]
    if not lines:
        raise RoadmapBuildError(f"{where}: no Markdown table found")
    return lines


def _sole_table(text: str, columns: tuple[str, ...], where: str) -> list[list[str]]:
    """The archive's one table, identified by its column signature.

    ``completed_plans.md`` carries no ``##`` heading over its table, so the
    header row is the name. Requiring it to be the file's only table is what
    stops a future explanatory table in the prose above from being read as the
    archive.
    """
    lines = _table_lines(text, where)
    headers = [i for i, line in enumerate(lines) if tuple(_split_row(line)) == columns]
    if len(headers) != 1:
        raise RoadmapBuildError(
            f"{where}: expected exactly one table with columns {columns}, "
            f"found {len(headers)}"
        )
    return _parse_table(lines[headers[0]:], columns, where)


# ---------------------------------------------------------------------------
# Plan links
# ---------------------------------------------------------------------------

def _blob_url(path: Path) -> str:
    return BLOB_BASE + path.relative_to(REPO_ROOT).as_posix()


def _display(path: Path) -> str:
    """A path for an error message, which must never raise on its way out."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def _first_heading(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            return line
    return ""


def plan_document(number: str) -> Path:
    """A plan's main document, found from its number.

    The glob is ambiguous by construction -- stage handoffs and reports share
    the prefix. Main documents title themselves ``# Plan <n>: <title>``, which
    is what separates ``plan_145_april_cutover_reconciliation.md`` from its eight
    handoffs. Ambiguity that survives that is a build failure: a public link to
    a stage handoff is worse than a failed build.
    """
    if not _PLAN_NUMBER_RE.match(number):
        raise RoadmapBuildError(f"{ARCHIVE}: unusable plan identifier {number!r}")

    matches = sorted((REPO_ROOT / PLANS_DIR).glob(f"plan_{number}_*.md"))
    if not matches:
        raise RoadmapBuildError(
            f"{ARCHIVE}: plan {number} has no document matching "
            f"{PLANS_DIR}/plan_{number}_*.md, so its public link cannot be built"
        )
    if len(matches) == 1:
        return matches[0]

    colon_form = re.compile(rf"^#\s+Plan\s+{re.escape(number)}\s*:")
    main = [path for path in matches if colon_form.match(_first_heading(path))]
    if len(main) == 1:
        return main[0]
    raise RoadmapBuildError(
        f"{ARCHIVE}: plan {number} matches {len(matches)} documents and "
        f"{len(main)} of them are titled '# Plan {number}: ...', so the main "
        f"document is ambiguous: {[p.name for p in matches]}"
    )


def authored_summary(path: Path) -> tuple[str, str] | None:
    """A plan's own **Public summary** section, when its author wrote one.

    This is the preferred source and the archive cell is the fallback. The
    reason is the shape of the two texts: an archive Description is written for
    someone who already knows the system and runs to several hundred words of
    incident narrative naming migrations, services, columns and object paths --
    exactly what the truth contract's §4 bars from the feed. Extracting its
    first sentence gets a *true* sentence that was never written to be read
    cold by a stranger.

    The section carries the same ``**Title** -- summary`` shape as an archive
    cell, so one parser reads both and whoever writes it can see the target
    format in the thing it replaces. Unlike the archive cell it is taken whole
    rather than cut at the first sentence: it was written for this, and
    truncating authored public copy would be the same mistake in the other
    direction.

    Returns ``None`` when the section is absent, which is the normal state for
    the 118 plans archived before this existed.
    """
    text = path.read_text(encoding="utf-8")
    match = re.search(
        rf"^##\s+{PUBLIC_SUMMARY_HEADING}\s*$(.*?)(?=^#|\Z)", text, re.M | re.S
    )
    if not match:
        return None

    paragraphs = [block.strip() for block in match.group(1).split("\n\n") if block.strip()]
    if not paragraphs:
        raise RoadmapBuildError(
            f"{_display(path)}: '## {PUBLIC_SUMMARY_HEADING}' is empty"
        )

    title, remainder = bolded_lead(paragraphs[0])
    summary = flatten_markdown(remainder)
    if not summary:
        raise RoadmapBuildError(
            f"{_display(path)}: '## {PUBLIC_SUMMARY_HEADING}' has a "
            f"title and no summary after it"
        )
    return title, summary


def _linked_plan(cell: str, where: str) -> tuple[str, str]:
    """Plan number and href from a build-order Plan cell.

    The cell is a link, sometimes followed by a bold stage marker
    (``[154](...) **Stage 0**``), so the link is matched rather than the cell
    parsed. The target is checked against the tree: a build-order row pointing
    at a document that does not exist would publish a 404.
    """
    match = _LINK_RE.search(cell)
    if not match:
        raise RoadmapBuildError(f"{where}: Plan cell carries no link: {cell[:80]!r}")
    number, target = match.group(1).strip(), match.group(2).strip()
    if not _PLAN_NUMBER_RE.match(number):
        raise RoadmapBuildError(f"{where}: unusable plan identifier {number!r}")

    # Targets in PLANS.md are relative to docs/, the file's own directory.
    path = (REPO_ROOT / "docs" / target).resolve()
    if not path.is_file():
        raise RoadmapBuildError(f"{where}: plan {number} links to a missing file: {target}")
    return number, _blob_url(path)


# ---------------------------------------------------------------------------
# The two projections
# ---------------------------------------------------------------------------

def _as_of(index_text: str) -> str:
    match = _AS_OF_RE.search(index_text)
    if not match:
        raise RoadmapBuildError(
            f"{INDEX}: no '## Current State (as of YYYY-MM-DD)' heading, which is "
            f"where as_of comes from -- wall-clock time would make every run a diff"
        )
    return match.group(1)


def planned_items(index_text: str) -> list[dict]:
    rows = _table_under_heading(index_text, BUILD_ORDER_HEADING, BUILD_ORDER_COLUMNS, INDEX)

    orders = [row[0].strip() for row in rows]
    if len(set(orders)) != len(orders):
        raise RoadmapBuildError(f"{INDEX}: build order has duplicate Order values")

    items = []
    for row in rows[:MAX_ITEMS]:
        order_cell, plan_cell, title, slice_cell = row[0], row[1], row[2], row[3]
        priority_cell, effort_cell = row[6], row[7]

        try:
            order = int(order_cell)
        except ValueError as exc:
            raise RoadmapBuildError(f"{INDEX}: Order is not an integer: {order_cell!r}") from exc
        try:
            priority = int(flatten_markdown(priority_cell))
        except ValueError as exc:
            raise RoadmapBuildError(
                f"{INDEX}: Priority is not an integer: {priority_cell!r}"
            ) from exc
        if not 0 <= priority <= 100:
            raise RoadmapBuildError(f"{INDEX}: Priority {priority} is outside 0-100")

        number, href = _linked_plan(plan_cell, INDEX)
        items.append(
            {
                "plan": number,
                "title": flatten_markdown(title),
                "order": order,
                "priority": priority,
                "effort": leading_effort_token(effort_cell),
                "state": "planned",
                "summary": flatten_markdown(slice_cell),
                "href": href,
            }
        )
    return items


def completed_items(archive_text: str, extracted: list[str] | None = None) -> list[dict]:
    """Assemble the completed side, preferring each plan's own public copy.

    ``extracted`` collects the plans that had no **Public summary** section and
    so were described by cutting a sentence out of their archive row. That list
    is Gate 1d's worklist: it names exactly the rows a human still has to read,
    and it empties itself as close-out writes the sections.
    """
    rows = _sole_table(archive_text, ARCHIVE_COLUMNS, ARCHIVE)

    items = []
    for plan_cell, description, date_cell in rows[:MAX_ITEMS]:
        number = flatten_markdown(plan_cell)
        date_match = _DATE_RE.match(flatten_markdown(date_cell))
        if not date_match:
            raise RoadmapBuildError(
                f"{ARCHIVE}: plan {number} has no YYYY-MM-DD completion date: "
                f"{date_cell!r}"
            )

        document = plan_document(number)
        authored = authored_summary(document)
        if authored:
            title, summary = authored
        else:
            title, remainder = bolded_lead(description)
            summary = first_sentence(flatten_markdown(remainder))
            if extracted is not None:
                extracted.append(number)

        if len(summary) > MAX_SUMMARY_CHARS:
            raise RoadmapBuildError(
                f"plan {number}: the published summary is {len(summary)} characters, "
                f"over the {MAX_SUMMARY_CHARS} cap. Write a "
                f"'## {PUBLIC_SUMMARY_HEADING}' section in "
                f"{_display(document)} rather than widening the cap"
            )

        items.append(
            {
                "plan": number,
                "title": title,
                "date": date_match.group(1),
                "state": "completed",
                "summary": summary,
                "href": _blob_url(document),
            }
        )

    dates = [item["date"] for item in items]
    if dates != sorted(dates, reverse=True):
        raise RoadmapBuildError(
            f"{ARCHIVE}: the published rows are not newest-first: {dates}"
        )
    return items


def build(extracted: list[str] | None = None) -> dict:
    index_text = (REPO_ROOT / INDEX).read_text(encoding="utf-8")
    archive_text = (REPO_ROOT / ARCHIVE).read_text(encoding="utf-8")
    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": _as_of(index_text),
        "planned": planned_items(index_text),
        "completed": completed_items(archive_text, extracted),
    }


def render(snapshot: dict) -> str:
    """One serialisation, so ``--check`` compares bytes and not dictionaries."""
    return json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate in memory and exit non-zero on drift; write nothing",
    )
    args = parser.parse_args(argv)

    extracted: list[str] = []
    try:
        rendered = render(build(extracted))
    except RoadmapBuildError as exc:
        print(f"build_public_roadmap: {exc}", file=sys.stderr)
        return 2

    # Gate 1d, reduced to the rows it still applies to. A plan with its own
    # Public summary has already been read by the person who wrote it; the
    # names printed here are the ones described by machine.
    if extracted:
        print(
            f"build_public_roadmap: Gate 1d -- {len(extracted)} completed "
            f"{'summary was' if len(extracted) == 1 else 'summaries were'} cut from "
            f"the archive rather than authored: plans {', '.join(extracted)}. "
            f"Read what they publish, or add '## {PUBLIC_SUMMARY_HEADING}' to those "
            f"plan documents.",
            file=sys.stderr,
        )

    destination = REPO_ROOT / OUTPUT
    if args.check:
        current = destination.read_text(encoding="utf-8") if destination.is_file() else None
        if current == rendered:
            print(f"build_public_roadmap: {OUTPUT} is up to date")
            return 0
        print(
            f"build_public_roadmap: {OUTPUT} is stale -- "
            f"run `python scripts/build_public_roadmap.py` and commit the result",
            file=sys.stderr,
        )
        return 1

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(rendered, encoding="utf-8")
    print(f"build_public_roadmap: wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
