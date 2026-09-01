"""Plan 146 Stage 4: the planning index's structure, asserted rather than agreed.

Plans 146 Stages 0-3 rebuilt ``docs/`` around one rule -- **every plan sits in
exactly one table**, and every row carries the condition that removes it. Stage
0 measured what happens without a test: 24 plan documents in no table at all,
and 9 plans with no document *and* no archive row, including Plan 65, the auth
stack, which shipped and then vanished from the record for four months.

So this file follows ``TestServiceHealthCoverage`` in
``tests/test_observability_config.py``: **coverage is asserted, not
enumerated.** There is no list of plan numbers here to append to. A test you
can silence by adding a number to a list reproduces the defect it was written
to catch -- the old "Plan inventory" section covered 30 of 72 documents and
said nothing whatsoever about the other 42, because nothing forced it to.

Two structural facts the next reader will otherwise assume wrongly:

* **Plan number and plan document are one-to-one again, but nothing here
  depends on that.** It was not: 79 documents carried 73 distinct numbers, with
  Plan 125 holding three documents and Plans 110, 120 and 123 two each. On
  2026-09-01 the stage handoffs and implementation prompts moved to
  ``docs/prompts/`` and the reports to ``docs/reference/`` -- directories that
  already existed for those kinds -- leaving 94 documents and 94 numbers here.
  Coverage still keys on **the plan number a document declares** rather than on
  the filename, because that is what survives the property being lost again;
  ``plan_v018_schema_migration.md`` still declares no number at all.
* **The converse does not hold and is not asserted.** 61 table rows have no
  document -- nearly all archive rows for plans finished before plan documents
  existed, plus six whose documents were deleted. A row without a document is
  the record working, not a gap.

Directories encode kind, not state (Stage 3's decision, in
``docs/planning/plans_decision_log.md``). Nothing here may key on a path that
would change when a plan completes.
"""
from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# One repo-root-relative constant block, matching
# ``scripts/audit_plan_state_history.py``. Stage 3 moved 98 files; whatever
# Stage 6 moves next should have exactly one place to edit.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = "docs"
INDEX = "docs/PLANS.md"
PLANS_DIR = "docs/plans"
ARCHIVE = "docs/planning/completed_plans.md"


@lru_cache(maxsize=None)
def _read(relative: str) -> str:
    """Every file is read once per session.

    ``tests/test_observability_config.py`` re-parses ``docker-compose.yml`` 81
    times in one run -- 3.28s of a 3.41s suite -- because its helpers read from
    disk on every call. ``PLANS.md``, the archive and 79 plan documents are a
    comparable amount of I/O and the link scan walks every markdown file under
    ``docs/``, so caching is the difference between a fast test and that.
    """
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# The five tables. Stage 2 froze these headers and Stage 4 reads them: renaming
# a column to make parsing easier would let the test edit its own input, which
# is the one thing a structural test must never be able to do.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Table:
    name: str
    path: str
    heading: str | None  # ``None`` -- the file holds exactly one table
    header: tuple[str, ...]


CLOSEOUT = Table(
    "closeout", INDEX, "Current closeout",
    ("Plan", "Lands", "Gate — what removes this row"),
)
BUILD_ORDER = Table(
    "build order", INDEX, "Default build order",
    ("Order", "Plan", "Title", "Next executable slice", "Workable?",
     "Blocked by", "Priority", "Effort", "Depends on / safe stopping point"),
)
BACKLOG = Table(
    "backlog", INDEX, "Backlog",
    ("Plan", "Title", "Priority", "Effort", "Trigger"),
)
SUPERSEDED = Table(
    "superseded", INDEX, "Superseded",
    ("Plan", "Title", "Superseded by"),
)
ARCHIVE_TABLE = Table(
    "archive", ARCHIVE, None,
    ("Plan", "Description", "Date"),
)
TABLES = (CLOSEOUT, BUILD_ORDER, BACKLOG, SUPERSEDED, ARCHIVE_TABLE)


# A cell may contain an escaped pipe: build-order row 4 quotes the LogQL
# fragment ``\|= "403"`` that produced 49 of 51 alert annotations. Splitting on
# every ``|`` shifts that row one column left, which is how a parser reads the
# wrong cell and reports nothing.
_UNESCAPED_PIPE = re.compile(r"(?<!\\)\|")


def _cells(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|") and not stripped.endswith(r"\|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in _UNESCAPED_PIPE.split(stripped)]


def _is_separator(cells: list[str]) -> bool:
    """``|---|---|``. An all-blank row is *not* a separator -- it is a row with
    no content, and it should reach the parsers and fail there."""
    joined = "".join(cells)
    return "-" in joined and set(joined) <= set("-: ")


def _section_body(text: str, heading: str) -> str:
    for section in re.split(r"^## +", text, flags=re.M)[1:]:
        found, _, body = section.partition("\n")
        if found.strip() == heading:
            return body
    raise AssertionError(f"no '## {heading}' section")


@lru_cache(maxsize=None)
def rows(table: Table) -> tuple[dict[str, str], ...]:
    """Every data row of one table, as ``header -> cell``.

    Asserts the header on the way past, so a renamed column fails here with the
    column named rather than downstream with a ``KeyError``.
    """
    text = _read(table.path)
    body = _section_body(text, table.heading) if table.heading else text
    header: tuple[str, ...] | None = None
    parsed: list[dict[str, str]] = []
    for line in body.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = _cells(line)
        if header is None:
            assert tuple(cells) == table.header, (
                f"the {table.name} table's header changed: expected "
                f"{list(table.header)}, found {cells}. Stage 2 froze these "
                f"columns; change the test only after changing the document "
                f"on purpose."
            )
            header = tuple(cells)
            continue
        if _is_separator(cells):
            continue
        assert len(cells) == len(header), (
            f"a {table.name} row has {len(cells)} cells against "
            f"{len(header)} columns: {line.strip()[:120]}"
        )
        parsed.append(dict(zip(header, cells)))
    assert header is not None, f"no table found for {table.name} in {table.path}"
    return tuple(parsed)


# ---------------------------------------------------------------------------
# Plan-number parsing. Both parsers fail loudly on an unrecognised cell:
# silently dropping what it could not read is how the old inventory covered 30
# of 72 documents and reported nothing about the rest.
# ---------------------------------------------------------------------------

# ``[135](plans/plan_135_storage_observability.md)``, ``**88**`` for a plan with
# no document, optionally followed by a bold stage marker -- Plan 139 holds two
# build-order rows, ``**Stage C**`` and ``**Stage D**``, which are distinct
# executable slices with different blockers. That is the design, not a leak.
_INDEX_PLAN = re.compile(
    r"^(?:\[(?P<linked>\d+)\]\([^)]*\)|\*\*(?P<bare>\d+)\*\*)"
    r"(?:\s+\*\*[^*]+\*\*)?$"
)

# The archive predates plan documents and its ``Plan`` column shows it: 22 of
# 108 rows are not a plain integer. Each recognised form is deliberate --
_ARCHIVE_INTEGER = re.compile(r"^\d+$")            # 144
_ARCHIVE_SUB_PLAN = re.compile(r"^(\d+)\.\d+$")     # 14.1 -- a sub-plan of 14
_ARCHIVE_COMBINED = re.compile(r"^\d+(?:\s*\+\s*\d+)+$")  # 62 + 63, 60+75
_ARCHIVE_NON_PLAN = re.compile(r"^[A-Za-z][\w .+-]*$")    # V029, Silver flush


def index_plan_number(table: Table, cell: str) -> int:
    match = _INDEX_PLAN.match(cell)
    assert match, (
        f"unparseable Plan cell in the {table.name} table: {cell!r}. Expected "
        f"a link like [135](plans/plan_135_x.md) or a bold bare number like "
        f"**88** for a plan with no document, optionally followed by a bold "
        f"stage marker."
    )
    return int(match.group("linked") or match.group("bare"))


def archive_plan_numbers(cell: str) -> tuple[int, ...]:
    """Plan numbers named by one archive ``Plan`` cell, possibly none.

    ``62 + 63`` is genuinely both plans and yields both. ``14.1`` is a sub-plan
    and yields its parent, 14. A cell that names no plan at all -- ``V029`` is a
    schema version, ``Silver flush`` a named workstream -- yields nothing, and
    is recognised only because it does not begin with a digit. Anything that
    starts like a number and is not one raises instead: ``62 & 63`` must fail
    here rather than quietly count as zero plans.
    """
    if _ARCHIVE_INTEGER.match(cell):
        return (int(cell),)
    sub_plan = _ARCHIVE_SUB_PLAN.match(cell)
    if sub_plan:
        return (int(sub_plan.group(1)),)
    if _ARCHIVE_COMBINED.match(cell):
        return tuple(int(number) for number in re.findall(r"\d+", cell))
    assert _ARCHIVE_NON_PLAN.match(cell), (
        f"unparseable Plan cell in the archive: {cell!r}. Recognised forms are "
        f"an integer (144), a sub-plan (14.1), plans closed together (62 + 63), "
        f"or a non-plan identifier beginning with a letter (V029)."
    )
    return ()


@lru_cache(maxsize=None)
def plan_numbers(table: Table) -> frozenset[int]:
    """The distinct plan numbers one table claims."""
    if table is ARCHIVE_TABLE:
        return frozenset(
            number
            for row in rows(table)
            for number in archive_plan_numbers(row["Plan"])
        )
    return frozenset(
        index_plan_number(table, row["Plan"]) for row in rows(table)
    )


# ---------------------------------------------------------------------------
# Plan documents. ``docs/plans/`` is flat and stays flat -- a path that changed
# when a plan completed would put the same fact in two places, which is the
# defect Plan 146 exists to remove.
# ---------------------------------------------------------------------------
_DOCUMENT_NAME = re.compile(
    r"^(?:implementation_)?plan_(?P<identifier>\d+|v\d+)_[\w.-]+\.md$"
)


@lru_cache(maxsize=None)
def plan_documents() -> tuple[Path, ...]:
    return tuple(sorted((REPO_ROOT / PLANS_DIR).glob("*.md")))


@lru_cache(maxsize=None)
def documents_by_plan_number() -> dict[int, tuple[str, ...]]:
    """Plan number -> the documents declaring it. Many-to-one on purpose.

    Documents whose identifier is not a number contribute nothing, because
    there is no number for a table row to key on. Today that is exactly
    ``plan_v018_schema_migration.md``, a schema-version migration rather than a
    numbered plan. The exclusion is by *form*, never by name -- there is no
    list here to add a filename to.
    """
    found: dict[int, list[str]] = {}
    for path in plan_documents():
        match = _DOCUMENT_NAME.match(path.name)
        if match is None or not match.group("identifier").isdigit():
            continue
        found.setdefault(int(match.group("identifier")), []).append(path.name)
    return {number: tuple(names) for number, names in found.items()}


@lru_cache(maxsize=None)
def known_plan_numbers() -> frozenset[int]:
    """Every plan number this repo knows about, from any source."""
    return frozenset(documents_by_plan_number()).union(
        *(plan_numbers(table) for table in TABLES)
    )


class TestPlanTableCoverage:
    """The invariant: every plan sits in exactly one table.

    Split across two assertions because the halves fail differently. A plan in
    *no* table is invisible work -- Plan 65 shipped and disappeared for four
    months that way. A plan in *two* is a contradiction the index cannot
    resolve, and whoever reads it picks whichever they saw first.
    """

    def test_every_plan_document_filename_declares_an_identifier(self):
        """The gate the rest of this file stands on. A document whose name the
        parser cannot read would otherwise be excluded from coverage silently,
        which is the failure mode, not the fix."""
        unreadable = [
            path.name for path in plan_documents()
            if not _DOCUMENT_NAME.match(path.name)
        ]
        assert not unreadable, (
            f"{unreadable} in {PLANS_DIR}/ do not match "
            f"plan_<number>_<slug>.md. Coverage keys on the declared number, "
            f"so a document named otherwise is in no table by construction."
        )

    def test_every_plan_document_appears_in_a_table(self):
        """79 documents, 73 distinct numbers, and every one of those numbers is
        claimed by a table. Keyed on the number, so Plan 125's three documents
        are covered by Plan 125's single row."""
        claimed = frozenset().union(*(plan_numbers(table) for table in TABLES))
        missing = {
            number: documents_by_plan_number()[number]
            for number in sorted(set(documents_by_plan_number()) - claimed)
        }
        assert not missing, (
            f"{missing} have plan documents but appear in none of the five "
            f"tables. Add each to the closeout, build order, backlog or "
            f"superseded table in {INDEX}, or to the archive in {ARCHIVE}."
        )

    def test_no_plan_number_appears_in_two_tables(self):
        """*Two tables*, not two rows. Plan 139 holds build-order rows 14 and
        16 -- Stage C and Stage D, separate slices with separate blockers --
        and that is one table claiming one plan."""
        collisions: dict[int, list[str]] = {}
        for number in sorted(known_plan_numbers()):
            claiming = [t.name for t in TABLES if number in plan_numbers(t)]
            if len(claiming) > 1:
                collisions[number] = claiming
        assert not collisions, (
            f"{collisions} -- each plan number belongs to exactly one table. "
            f"A plan in two makes 'is plan N done?' unanswerable from the "
            f"index; delete the stale row."
        )


class TestRowExitConditions:
    """Every row names what removes it. A row with no exit condition is
    invisible and therefore permanent -- Plan 123 sat unrecorded from
    2026-07-10 for six weeks, which is why the closeout table has a ``Lands``
    column at all."""

    @staticmethod
    def _is_substantive(cell: str) -> bool:
        return bool(cell.strip()) and cell.strip() not in {"--", "-", "TBD", "?"}

    def test_every_closeout_row_has_a_parsable_lands_date(self):
        broken = {}
        for row in rows(CLOSEOUT):
            found = re.search(r"\d{4}-\d{2}-\d{2}", row["Lands"])
            if not found:
                broken[row["Plan"]] = row["Lands"]
                continue
            try:
                date.fromisoformat(found.group(0))
            except ValueError:
                broken[row["Plan"]] = row["Lands"]
        assert not broken, (
            f"closeout rows without a YYYY-MM-DD date that parses: {broken}. "
            f"A closeout row is deployed work waiting on evidence, and the "
            f"date is when someone looks."
        )

    def test_every_closeout_row_has_a_gate(self):
        gate_column = CLOSEOUT.header[2]
        missing = [
            row["Plan"] for row in rows(CLOSEOUT)
            if not self._is_substantive(row[gate_column])
        ]
        assert not missing, (
            f"closeout rows with no gate: {missing}. The gate says what "
            f"removes the row; without one the date arrives and nothing "
            f"changes."
        )

    def test_every_backlog_row_has_a_trigger(self):
        missing = [
            row["Plan"] for row in rows(BACKLOG)
            if not self._is_substantive(row["Trigger"])
        ]
        assert not missing, (
            f"backlog rows with no trigger: {missing}. A row with no trigger "
            f"is a wish, and wishes do not leave tables."
        )


class TestBuildOrderBlockers:
    """``Blocked by`` names a real plan or a date, or is ``--``.

    ``--`` is not a violation: 9 of 18 rows are unblocked, which is what makes
    the build order workable. What is a violation is a blocker naming a plan
    number nothing else in the repo has heard of, or a vague wait with no date
    -- either way nobody can tell when the row becomes workable.
    """

    # ``Plan 125``, ``Plans 125, 112, 113``, or a leading bare number as in
    # ``139 Stage A coverage data``. Deliberately not "any number in the cell":
    # "a 7d window" contains a 7, and Plan 7 is a real archived plan.
    _NAMED_PLANS = re.compile(
        r"\bPlans?\s+(\d{1,3}(?:\s*,\s*\d{1,3})*)|^(\d{1,3})\b"
    )
    _DATE = re.compile(r"\d{4}-\d{2}(?:-\d{2})?")

    @classmethod
    def _plans_named(cls, cell: str) -> set[int]:
        found: set[int] = set()
        for match in cls._NAMED_PLANS.finditer(cell):
            group = match.group(1) or match.group(2)
            found.update(int(number) for number in re.findall(r"\d+", group))
        return found

    def test_every_blocker_names_a_known_plan_or_a_date(self):
        unresolvable = {}
        for row in rows(BUILD_ORDER):
            blocker = row["Blocked by"].strip()
            if blocker in {"--", ""}:
                continue
            named = self._plans_named(blocker)
            if named & known_plan_numbers() or self._DATE.search(blocker):
                continue
            unresolvable[row["Order"]] = blocker
        assert not unresolvable, (
            f"build-order rows whose 'Blocked by' names neither a known plan "
            f"nor a date: {unresolvable}. A blocker nobody can resolve is a "
            f"row that never becomes workable."
        )

    def test_no_blocker_names_a_plan_that_does_not_exist(self):
        """The other half: a typo'd plan number reads as a real dependency and
        blocks a row forever."""
        unknown = {}
        for row in rows(BUILD_ORDER):
            blocker = row["Blocked by"].strip()
            named = self._plans_named(blocker)
            missing = named - known_plan_numbers()
            if missing:
                unknown[row["Order"]] = sorted(missing)
        assert not unknown, (
            f"build-order blockers naming plan numbers that exist nowhere -- "
            f"no document, no table row: {unknown}"
        )


class TestIndexLineBudget:
    """``PLANS.md`` states its own budget and this reads it back.

    The budget lives in the document rather than here on purpose: the number is
    an editorial decision that belongs where the editor is looking. Before
    Plan 146 the file was 232 lines of index *and* narrative; Stage 2 cut it to
    169 by moving the narrative to the decision log, and the budget is what
    keeps it there.
    """

    _BUDGET = re.compile(r"Line budget:\s*(\d+)\s*lines")

    def test_the_index_states_a_line_budget(self):
        assert self._BUDGET.search(_read(INDEX)), (
            f"{INDEX} no longer states a line budget. The budget is part of "
            f"the document, not part of this test."
        )

    def test_the_index_is_under_its_stated_budget(self):
        budget = int(self._BUDGET.search(_read(INDEX)).group(1))
        actual = len(_read(INDEX).splitlines())
        assert actual <= budget, (
            f"{INDEX} is {actual} lines against its stated budget of {budget}. "
            f"Something has become narrative; move it to "
            f"docs/planning/plans_decision_log.md rather than raising the "
            f"budget."
        )


# ---------------------------------------------------------------------------
# Link integrity.
# ---------------------------------------------------------------------------
_FENCE = re.compile(r"^\s*(```|~~~)")
_CODE_SPAN = re.compile(r"`[^`]*`")
_MARKDOWN_LINK = re.compile(
    r"(?<!\\)\[[^\]]*\]\(\s*([^)\s]+?)\s*(?:\"[^\"]*\")?\s*\)"
)
_EXTERNAL = ("http://", "https://", "mailto:", "//", "#")


def _prose_only(text: str) -> str:
    """Text with fenced blocks and code spans removed.

    This is the distinction that decides whether the test survives contact with
    the repo. Documents that plans *propose* and nobody has written --
    ``docs/governance_inventory.md``, ``docs/runbook_lakehouse.md`` and three
    others -- appear as backticked filenames in deliverables lists, never as
    markdown links. They describe future work. A check that flags them is a
    check somebody disables inside a week, so this follows link syntax only.

    Fenced blocks go too: ``docs/prompts/`` contains regexes made of link
    punctuation. They are escaped today and match nothing, but a code block is
    not prose and its contents are not links.
    """
    kept: list[str] = []
    fence: str | None = None
    for line in text.splitlines():
        opener = _FENCE.match(line)
        if opener:
            if fence is None:
                fence = opener.group(1)
            elif line.strip().startswith(fence):
                fence = None
            continue
        if fence is not None:
            continue
        kept.append(_CODE_SPAN.sub("", line))
    return "\n".join(kept)


@lru_cache(maxsize=None)
def markdown_files() -> tuple[Path, ...]:
    return tuple(sorted((REPO_ROOT / DOCS_DIR).rglob("*.md")))


class TestDocumentationLinks:
    """No markdown link under ``docs/`` is dangling.

    Worth having permanently and not only for Stage 3's rename: a ``see below``
    dangled for a day on 2026-08-21 after a section moved, which is how Plan 146
    started. Stage 3 moved 98 files and rewrote 163 links inside ``docs/``; this
    is what keeps the next move honest.

    Three references are broken **by decision** and this must not flag them,
    each recorded in ``docs/planning/plans_decision_log.md``: ``.env.example``
    cites a runbook nobody has written and is outside ``docs/`` anyway; Plan
    105's two dead links are now backticked prose precisely so this test does
    not see them; and Stage 3's own prompt keeps flat ``docs/plan_*.md`` paths
    in prose because it describes the layout as it stood before the move.
    """

    def test_no_markdown_link_in_docs_is_dangling(self):
        dangling: list[str] = []
        for path in markdown_files():
            relative = path.relative_to(REPO_ROOT)
            for target in _MARKDOWN_LINK.findall(
                _prose_only(_read(str(relative)))
            ):
                if target.startswith(_EXTERNAL):
                    continue
                without_fragment = target.split("#", 1)[0]
                if not without_fragment:
                    continue  # a pure ``#anchor``
                if not (path.parent / without_fragment).exists():
                    dangling.append(f"{relative} -> {target}")
        assert not dangling, (
            "dangling markdown links under docs/:\n  " + "\n  ".join(dangling)
        )

    def test_the_scan_actually_reads_links(self):
        """A link checker that matches nothing passes forever. ``docs/`` held
        394 resolvable links after Stage 3; this asserts the scan is still
        looking at them rather than silently matching an empty set."""
        found = sum(
            len(_MARKDOWN_LINK.findall(
                _prose_only(_read(str(path.relative_to(REPO_ROOT))))
            ))
            for path in markdown_files()
        )
        assert found > 200, (
            f"only {found} markdown links found under docs/ -- the link "
            f"pattern has stopped matching, so the dangling check proves "
            f"nothing."
        )


class TestParserAgreesWithTheDocuments:
    """Shape assertions on the parsers themselves.

    Not row counts -- those change every week and a test that pins them is a
    test that fails on every legitimate edit. These pin the *forms*: every
    Plan cell in all five tables parses, in a file whose entire argument is that
    a row nobody parsed is a row nobody enforced.
    """

    @pytest.mark.parametrize(
        "table", TABLES, ids=lambda t: t.name.replace(" ", "_")
    )
    def test_every_plan_cell_parses(self, table):
        for row in rows(table):
            if table is ARCHIVE_TABLE:
                archive_plan_numbers(row["Plan"])
            else:
                index_plan_number(table, row["Plan"])

    def test_the_archive_still_holds_the_bulk_of_the_record(self):
        """The archive is the only record of what is finished, and ``PLANS.md``
        keeps no copy. If it ever holds fewer rows than the live tables
        combined, something has been deleted rather than archived."""
        live = sum(len(rows(t)) for t in TABLES if t is not ARCHIVE_TABLE)
        assert len(rows(ARCHIVE_TABLE)) > live, (
            f"the archive holds {len(rows(ARCHIVE_TABLE))} rows against "
            f"{live} live rows -- completed plans are going missing rather "
            f"than being archived."
        )


# ===========================================================================
# Plan 146 Stage 5.
#
# Everything above was written against a structure edited by hand. Stage 5 adds
# a tool that edits it, and a tool makes *systematic* mistakes -- in whichever
# direction the test happens not to look. Seven mutations were applied to the
# working tree before the skill existed and **all eighteen assertions above
# passed on every one of them**:
#
#   A  a superseded row's ``Superseded by`` emptied
#   B  archive rows reordered, breaking newest-first
#   C  ``Order`` values duplicated and jumped to 99
#   D  an archive ``Date`` set to ``sometime in August``
#   E  ``[112](plans/plan_113_...)`` -- link text and target disagree
#   F  the ``**88**`` backlog row deleted outright
#   G  one backlog row duplicated within the backlog
#
# A-F are closed below. **G is not, and deliberately.** A duplicate row inside
# one table is caught by nothing here because ``plan_numbers`` returns a set and
# Plan 139 legitimately holds two build-order rows; the rule that would reject
# G -- one row per plan per table -- is false by design. G's defence is the
# skill's own: it splices a row list rather than appending text, so inserting a
# plan that already has a row is a bug rather than an omission.
# ===========================================================================

RECONCILIATION = "docs/planning/plan_state_reconciliation.md"


class TestSupersededRowExitConditions:
    """``Superseded by`` is the superseded table's exit condition.

    It is terminal, so nothing removes these rows -- but the column is what
    makes the row *readable*, and an empty one leaves a plan that was replaced
    with no record of what replaced it. That is the same defect as a backlog
    row with no trigger, one state further along.
    """

    def test_every_superseded_row_names_what_superseded_it(self):
        missing = [
            row["Plan"] for row in rows(SUPERSEDED)
            if not TestRowExitConditions._is_substantive(row["Superseded by"])
        ]
        assert not missing, (
            f"superseded rows naming nothing that superseded them: {missing}. "
            f"A plan replaced by nothing in particular is a plan nobody can "
            f"tell was replaced rather than abandoned."
        )


# The archive's ``Date`` column: ``2026-08-21``, or ``2026-01`` for Plan 4,
# whose n8n retention workflow predates any day-level record. A trailing
# provenance label -- ``*(observed)*``, ``*(corroborated)*``, ``*(inferred)*``
# -- may follow and carries no digits that could be mistaken for the date.
_ARCHIVE_DATE = re.compile(r"\b(\d{4})-(\d{2})(?:-(\d{2}))?\b")


def archive_date(cell: str) -> date | None:
    """The date one archive ``Date`` cell records, or ``None`` if it has none.

    A month-only cell resolves to its first day. That is a *sort* key, not a
    claim about when the work landed, and it is only ever compared against
    other rows in the same table.
    """
    match = _ARCHIVE_DATE.search(cell)
    if not match:
        return None
    year, month, day = match.group(1), match.group(2), match.group(3)
    try:
        return date(int(year), int(month), int(day or 1))
    except ValueError:
        return None


class TestArchiveOrdering:
    """The archive is prepend-only and newest first, and nothing said so.

    Both halves are what an automated writer gets wrong first. Appending is
    where a naive writer puts a new row -- it is what ``>>`` does -- and the
    archive's own header says the opposite. And a date it cannot parse is a
    date that sorts nowhere, so the order check has to stand on a date check or
    it silently stops comparing.
    """

    def test_every_archive_row_has_a_parsable_date(self):
        broken = {
            row["Plan"]: row["Date"] for row in rows(ARCHIVE_TABLE)
            if archive_date(row["Date"]) is None
        }
        assert not broken, (
            f"archive rows whose Date does not parse: {broken}. The archive is "
            f"ordered by this column; a cell that is not a date sorts nowhere "
            f"and the ordering check below stops seeing it."
        )

    def test_the_archive_is_newest_first(self):
        dated = [
            (row["Plan"], archive_date(row["Date"]))
            for row in rows(ARCHIVE_TABLE)
            if archive_date(row["Date"]) is not None
        ]
        inversions = [
            f"{earlier[0]} ({earlier[1]}) sits above {later[0]} ({later[1]})"
            for earlier, later in zip(dated, dated[1:])
            if earlier[1] < later[1]
        ]
        assert not inversions, (
            "the archive is newest-first and these rows are not:\n  "
            + "\n  ".join(inversions)
            + f"\nA new row is *prepended*, immediately after the header "
            f"separator in {ARCHIVE}. Appending is what a writer does by "
            f"default and it is wrong here."
        )


class TestBuildOrderNumbering:
    """``Order`` is 1..N, once each.

    The one column in the index that *is* a position, and therefore the one
    place Plan 146's "key on numbers, never ordinals" rule does not apply --
    which is exactly why it needs checking. Renumbering after an insert is the
    first thing an automated editor reaches for and the easiest to get wrong:
    a duplicated 3 makes two rows claim one position, and a jump to 99 leaves
    a gap that reads as a deleted row.
    """

    def test_the_build_order_is_numbered_one_to_n_without_gaps(self):
        found = [row["Order"].strip() for row in rows(BUILD_ORDER)]
        expected = [str(position) for position in range(1, len(found) + 1)]
        assert found == expected, (
            f"the build order's Order column reads {found} against the "
            f"{expected} that {len(found)} rows should number. Duplicates make "
            f"two rows claim one position; gaps read as a row somebody deleted."
        )


class TestPlanLinksNameTheirOwnPlan:
    """A ``Plan`` cell's link text and its target agree on the number.

    ``[112](plans/plan_113_production_adaptive_refresh.md)`` is well-formed
    markdown, resolves to a real file, parses as Plan 112, and is wrong. The
    dangling-link check cannot see it because nothing dangles, and coverage
    cannot see it because Plan 112 has a document of its own -- so the row
    reads as Plan 112 and points the reader at Plan 113.
    """

    _LINKED_PLAN = re.compile(r"^\[(?P<shown>\d+)\]\((?P<target>[^)]*)\)")
    _TARGET_PLAN = re.compile(r"(?:^|/)(?:implementation_)?plan_(\d+)_")

    def test_every_linked_plan_cell_points_at_that_plans_document(self):
        disagreements = []
        for table in TABLES:
            if table is ARCHIVE_TABLE:
                continue  # its Plan column is a bare identifier, never a link
            for row in rows(table):
                link = self._LINKED_PLAN.match(row["Plan"])
                if not link:
                    continue  # ``**88**`` -- a plan with no document to link to
                target = self._TARGET_PLAN.search(link.group("target"))
                if target is None or int(target.group(1)) != int(
                    link.group("shown")
                ):
                    disagreements.append(f"{table.name}: {row['Plan']}")
        assert not disagreements, (
            f"Plan cells whose link text and target name different plans: "
            f"{disagreements}. The row is filed under the number it shows and "
            f"sends the reader to the other one."
        )


# ---------------------------------------------------------------------------
# The census, and mutation F.
#
# Six index rows name a plan that has **no document**: 88, 87, 5, 52, 55 and
# 56. Coverage above keys on plan documents, so a documentless row has nothing
# whatsoever asserting it exists -- delete it and every assertion in this file
# still passes. That is not hypothetical: it is precisely how Plans 5, 52, 55
# and 56 were lost, and Stage 0 recovered them from the index's git history.
#
# The obvious fix is to assert the *count* of documentless rows. That is a
# number in a test file, which is a deny-list wearing a different hat: the way
# to silence it is to edit the number, and whoever deletes the row is exactly
# the person who will.
#
# So the check keys on an external census instead.
# ``plan_state_reconciliation.md`` is Stage 0's deliverable -- every plan number
# this repository has ever named, settled against its document, its git history
# and production evidence. It says of itself that it is "a record of one
# reconciliation, not a surface that gets maintained", which is the property
# that matters here: **it does not grow when a plan is added**, so it is not a
# list anybody has a routine reason to touch. Silencing this assertion means
# falsifying a dated evidence record, which is a different act from deleting a
# number from a list.
#
# If that record is ever deleted, this fails loudly rather than passing
# vacuously -- see ``test_the_census_reads_the_reconciliation_record``. Losing
# the only defence the documentless rows have should be a decision somebody
# makes, not a side effect.
# ---------------------------------------------------------------------------

# First cells of the reconciliation's tables, which is where its plan numbers
# live. Three forms, and everything else contributes nothing:
_CENSUS_BOLD = re.compile(r"^\*\*(\d+)\*\*$")             # **65**
_CENSUS_TITLED = re.compile(r"^(\d+)\s+\S")               # 81 data migration
_CENSUS_LIST = re.compile(r"^\d+(?:\s*,\s*\d+)*$")        # 66, 122, 79, 94, 108, 88

# Deliberately *not* "any number in the document". Its summary table reads
# "Numbers never used at all | 3 (44, 85, 104)" -- three numbers that name no
# plan and belong to no table, and a census that swept the prose would demand
# rows for them. Keying on first cells keeps the census to what the tables
# resolve rather than what the narrative mentions.


@lru_cache(maxsize=None)
def census_plan_numbers() -> frozenset[int]:
    """Every plan number Stage 0's reconciliation settled into a state."""
    found: set[int] = set()
    for line in _read(RECONCILIATION).splitlines():
        if not line.strip().startswith("|"):
            continue
        cell = _cells(line)[0]
        bold = _CENSUS_BOLD.match(cell)
        titled = _CENSUS_TITLED.match(cell)
        if bold:
            found.add(int(bold.group(1)))
        elif titled:
            found.add(int(titled.group(1)))
        elif _CENSUS_LIST.match(cell):
            found.update(int(number) for number in re.findall(r"\d+", cell))
    return frozenset(found)


class TestNoRowVanishesSilently:
    """Mutation F: every plan Stage 0 settled is still claimed by a table."""

    def test_the_census_reads_the_reconciliation_record(self):
        """A census that reads nothing passes forever, and this one guards the
        rows with no other guard at all."""
        assert (REPO_ROOT / RECONCILIATION).exists(), (
            f"{RECONCILIATION} is gone. It is the only thing asserting that "
            f"the six index rows with no plan document -- 88, 87, 5, 52, 55, "
            f"56 -- still exist. Deleting it removes their only defence, which "
            f"is a decision to make on purpose rather than in passing."
        )
        found = census_plan_numbers()
        assert len(found) > 50, (
            f"the census resolved only {len(found)} plan numbers out of "
            f"{RECONCILIATION}. Its table shape has changed and the check "
            f"below is no longer looking at anything."
        )

    def test_every_reconciled_plan_is_still_claimed_by_a_table(self):
        claimed = frozenset().union(*(plan_numbers(table) for table in TABLES))
        vanished = sorted(census_plan_numbers() - claimed)
        assert not vanished, (
            f"plans {vanished} were settled into a state by Plan 146 Stage 0 "
            f"and are now in none of the five tables. A row that disappears "
            f"rather than moving is the leak this whole plan exists to close: "
            f"Stage 1's sweep found 33 of them across 16 separate days. If a "
            f"row was removed on purpose, it belongs in the archive or the "
            f"superseded table, not nowhere."
        )


class TestTheIndexCountsTheArchiveCorrectly:
    """``PLANS.md``'s "108 rows, newest first" is the index's only hard-coded
    count, it is maintained by hand, and nothing checked it.

    That matters more once a tool writes the archive than it did while a human
    did: archiving is two files, and the second one is a number in a sentence
    that nothing reads. The count goes stale the moment the skill succeeds, and
    a stale count is the index quietly disagreeing with the record it points at.

    This is not a number in a test. It is one document being held to what it
    says about another, the same shape as the line budget above.
    """

    _STATED = re.compile(r"(\d+) rows, newest first")

    def test_the_index_states_the_archives_row_count(self):
        assert self._STATED.search(_read(INDEX)), (
            f"{INDEX} no longer states how many rows {ARCHIVE} holds. The "
            f"claim is what makes it checkable; deleting it is fine, but do it "
            f"on purpose and delete this assertion with it."
        )

    def test_the_stated_count_matches_the_archive(self):
        stated = int(self._STATED.search(_read(INDEX)).group(1))
        actual = len(rows(ARCHIVE_TABLE))
        assert stated == actual, (
            f"{INDEX} says the archive holds {stated} rows; {ARCHIVE} holds "
            f"{actual}. Archiving a plan is two edits, and this is the second "
            f"one. Fix the index, not this test."
        )


RECAPS_DIR = "docs/recaps"

# Deliberately raw-text, not ``_prose_only``. A recap cites commits as
# ``a80b123`` -- inside code spans, which ``_prose_only`` strips -- so scanning
# prose only would find nothing and pass forever.
_SHORT_SHA = re.compile(r"\b[0-9a-f]{7}\b")
_RECAP_NAME = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.md$")

_REQUIRED_RECAP_SECTIONS = (
    "What shipped",
    "What moved between states",
    "What is still owed",
    "Unattributed commits",
    "Merges",
    "Deferred to the next recap",
)
_REQUIRED_RECAP_FIELDS = (
    "**Window:**",
    "**Recapped:**",
    "**Commits in window:**",
    # Plan 138 Stage 1e. The publication marker is required rather than
    # defaulted: a recap with no marker is a recap nobody decided about, and
    # both defaults are wrong. Defaulting to true publishes an unread week the
    # moment it lands; defaulting to false drops a week off the site silently
    # and nothing says so. Absent is a build failure instead.
    "**Publish:**",
)


@lru_cache(maxsize=None)
def recap_files() -> tuple[Path, ...]:
    directory = REPO_ROOT / RECAPS_DIR
    return tuple(sorted(directory.glob("*.md"))) if directory.is_dir() else ()


def _headings(text: str) -> set[str]:
    return {
        line[3:].strip()
        for line in text.splitlines()
        if line.startswith("## ")
    }


class TestWeeklyRecaps:
    """Plan 146 Stage 6's output, held to its shape rather than its content.

    A recap is a **generated artifact**, which makes this a different kind of
    test from everything above it: the rest of this file asserts properties of
    the repo, and these assert properties of something a skill wrote. Nothing
    here says a recap is accurate or complete -- no test can, because a recap's
    job is judgement. What it can hold is the structure that makes a missing
    judgement visible: an ``Unattributed commits`` section that is present even
    when empty, and a window and a deferred count that are stated rather than
    implied.

    The dangling-link check above already covers ``docs/recaps/`` for free,
    since ``markdown_files()`` walks all of ``docs/``. That is what catches the
    likeliest real defect -- a recap linking to a plan document at the flat
    pre-Stage-3 path a commit's own ``--stat`` shows.

    **The assertion deliberately not written here** is "every commit in a
    recap's window appears in that recap". It is the check worth wanting, and
    it cannot be a permanent test, because the window's commit set is only
    well-defined at the moment the recap is written. Measured on 2026-08-21:
    **30 commits sit on refs that are not on master, 17 of them authored a
    month earlier on an unmerged Plan 125 branch.** Merge that branch and every
    one of those commits enters an already-recapped week, so a recap that was
    exactly right when written would start failing for something its author
    could not have seen. The reconciliation therefore runs in
    ``.claude/skills/plan-week/SKILL.md`` at write time, against the history
    the author actually read, which is the only moment the denominator holds
    still.
    """

    def test_every_recap_is_named_for_the_sunday_that_ends_its_window(self):
        misnamed: list[str] = []
        for path in recap_files():
            match = _RECAP_NAME.match(path.name)
            if match is None:
                misnamed.append(f"{path.name}: not YYYY-MM-DD.md")
                continue
            day = date(*(int(part) for part in match.groups()))
            if day.weekday() != 6:
                misnamed.append(f"{path.name}: a {day.strftime('%A')}, not a Sunday")
        assert not misnamed, (
            "recap filenames must be the Sunday that ends the window:\n  "
            + "\n  ".join(misnamed)
            + "\nA week runs Monday to Sunday and the file is named for the "
            "window's end, so the deferred days show on the filesystem rather "
            "than only in prose."
        )

    def test_every_recap_carries_its_required_sections(self):
        missing: list[str] = []
        for path in recap_files():
            present = _headings(_read(str(path.relative_to(REPO_ROOT))))
            for section in _REQUIRED_RECAP_SECTIONS:
                if section not in present:
                    missing.append(f"{path.name} -> ## {section}")
        assert not missing, (
            "recaps are missing required sections:\n  "
            + "\n  ".join(missing)
            + "\nEvery one of them is present even when empty. A section that "
            "says 'nothing this week' is a fact; a missing section is silence "
            "you cannot tell apart from an oversight -- which is how the old "
            "'Plan inventory' covered 30 of 72 documents and said nothing "
            "about the other 42."
        )

    def test_every_recap_states_its_window_run_date_and_commit_count(self):
        missing: list[str] = []
        for path in recap_files():
            text = _read(str(path.relative_to(REPO_ROOT)))
            for field in _REQUIRED_RECAP_FIELDS:
                if field not in text:
                    missing.append(f"{path.name} -> {field}")
        assert not missing, (
            "recaps are missing their header fields:\n  "
            + "\n  ".join(missing)
            + "\nA recap that does not state its window cannot be checked "
            "against one, and a recap that does not state when it was written "
            "cannot be read as the dated record it is."
        )

    def test_no_recap_borrows_the_archives_provenance_labels(self):
        borrowed: list[str] = []
        for path in recap_files():
            text = _read(str(path.relative_to(REPO_ROOT)))
            for label in ("*(observed)*", "*(corroborated)*", "*(inferred)*"):
                if label in text:
                    borrowed.append(f"{path.name} -> {label}")
        assert not borrowed, (
            "recaps must not use the archive's provenance labels:\n  "
            + "\n  ".join(borrowed)
            + f"\nThose three words mean something specific in {ARCHIVE} -- how "
            "Stage 1 recovered a completion date it was never told. Reusing "
            "them as generic hedging makes 25 backfilled rows look like "
            "hedging too. Mark uncertainty in the recap's own words."
        )

    def test_the_recap_scan_actually_reads_recaps(self):
        """A scan that matches nothing passes forever.

        This also fails loudly if ``docs/recaps/`` is ever emptied, which is
        the same argument the census companion assertion makes: losing the
        record should be a decision somebody takes, not a side effect.
        """
        found = sum(
            len(_SHORT_SHA.findall(_read(str(path.relative_to(REPO_ROOT)))))
            for path in recap_files()
        )
        assert found > 0, (
            f"{RECAPS_DIR} holds {len(recap_files())} file(s) and no commit "
            f"sha was found in any of them. Either the recaps are gone or a "
            f"recap has stopped citing the commits it describes, and the "
            f"assertion below is proving nothing."
        )

    def test_every_sha_a_recap_names_is_a_real_commit(self):
        """Catches a fabricated or mistyped sha, which reads as evidence.

        Unlike the window reconciliation this file does not attempt, this one
        is stable over time: a commit that exists keeps existing. It needs git
        to answer, so it skips on a clone that cannot see the history --
        including CI, where ``actions/checkout@v4`` clones at depth 1. Its
        value is local, in the run the ``plan-week`` skill makes immediately
        after writing a recap.
        """
        try:
            shallow = subprocess.run(
                ["git", "rev-parse", "--is-shallow-repository"],
                cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
            )
        except (OSError, subprocess.SubprocessError) as exc:  # pragma: no cover
            pytest.skip(f"git is unavailable: {exc}")
        if shallow.returncode != 0:
            pytest.skip("not a git checkout")
        if shallow.stdout.strip() == "true":
            pytest.skip("shallow clone: the window's commits are not present")

        cited: dict[str, str] = {}
        for path in recap_files():
            relative = str(path.relative_to(REPO_ROOT))
            for sha in _SHORT_SHA.findall(_read(relative)):
                cited.setdefault(sha, relative)

        # One ``--batch-check`` rather than one process per sha. At 52 recaps a
        # year citing ~55 commits each that is the difference between one
        # subprocess and nearly three thousand.
        resolved = subprocess.run(
            ["git", "cat-file", "--batch-check"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=60,
            input="\n".join(sorted(cited)) + "\n",
        )
        lines = resolved.stdout.splitlines()
        assert len(lines) == len(cited), (
            f"git cat-file answered for {len(lines)} of {len(cited)} shas "
            f"(exit {resolved.returncode}): {resolved.stderr.strip()!r}. A "
            f"short answer would let this assertion pass without checking."
        )
        unknown = [
            f"{cited[sha]} -> {sha}"
            for sha, line in zip(sorted(cited), resolved.stdout.splitlines())
            if " commit " not in line
        ]
        assert not unknown, (
            "recaps name shas that are not commits in this repo:\n  "
            + "\n  ".join(unknown)
            + "\nA sha in a recap is the evidence for the sentence around it. "
            "One that resolves to nothing is a citation to nowhere."
        )


# ===========================================================================
# Plan 146 Stage 1's parser, held to the index it reads.
#
# The index's structure is encoded twice: this file's ``TABLES`` name the
# sections it parses, and ``scripts/audit_plan_state_history.py`` maps section
# headings to plan states. Only one of the two failed loudly when a heading
# changed, so ``## Current closeout -- finish before opening another large
# build`` became ``## Current closeout`` at ``0c08382`` on 2026-08-21 and the
# script silently stopped seeing the closeout table for nine days -- reporting
# every closeout plan as ``absent``. Nothing failed, because the script's
# ``state_map`` skips a heading it does not recognise.
#
# It has to skip. It reads all 189 revisions of the index and old revisions
# carry headings that assert no state. What it must not do is skip one in the
# index as it stands *today*, which is the case a test can decide.
# ===========================================================================


class TestTheStateParserClassifiesEveryLiveHeading:
    """Every ``## `` heading in today's index is mapped or explicitly ignored.

    This is the drift check, not a list to append to: it reads the script's own
    two tables, so the only way to satisfy it is to classify the heading in the
    script, which is the thing that was missing.

    Measured over the whole history at the time of writing, ``current
    closeout`` was the *only* unmapped heading across 189 revisions -- so this
    assertion is cheap to keep true and was cheap to violate unnoticed.
    """

    @staticmethod
    def _classification(heading: str):
        from scripts import audit_plan_state_history as audit

        key = heading.strip().replace("—", "--").rstrip("#").strip().lower()
        if key in audit.IGNORED_HEADINGS_EXACT:
            return "ignored"
        if key.startswith(audit.IGNORED_HEADING_PREFIXES):
            return "ignored"
        return audit.STATE_BY_HEADING.get(key)

    def test_no_live_heading_is_silently_skipped(self):
        headings = [
            section.partition("\n")[0].strip()
            for section in re.split(r"^## +", _read(INDEX), flags=re.M)[1:]
        ]
        assert headings, "no '## ' headings found in the index"
        unclassified = [h for h in headings if self._classification(h) is None]
        assert not unclassified, (
            f"headings in {INDEX} that "
            f"scripts/audit_plan_state_history.py neither maps to a state nor "
            f"ignores: {unclassified}. The script skips what it does not "
            f"recognise, so an unmapped heading does not fail -- it silently "
            f"drops that whole table from every state timeline. Add it to "
            f"STATE_BY_HEADING, or to IGNORED_HEADINGS_EXACT/"
            f"IGNORED_HEADING_PREFIXES if it asserts no state."
        )

    def test_every_live_table_section_is_mapped_to_a_state(self):
        """The four tables this file parses out of the index are states.

        Distinct from the check above: a heading could be *ignored* and pass it
        while the table under it vanished from the timelines. Keying on
        ``TABLES`` means the two encodings of the index's shape have to agree.
        """
        wrong = {
            table.heading: self._classification(table.heading)
            for table in TABLES
            if table.heading is not None
            and self._classification(table.heading)
            not in {"build", "closeout", "backlog", "superseded", "archive"}
        }
        assert not wrong, (
            f"sections this file parses as tables that the state parser does "
            f"not read as states: {wrong}. Both files describe the same index; "
            f"when they disagree the script is the one that fails quietly."
        )
