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

* **Plan number and plan document are not one-to-one.** 79 documents carry 73
  distinct numbers: Plan 125 has three documents, Plans 110, 120 and 123 have
  two each, and ``plan_v018_schema_migration.md`` declares no number at all.
  Coverage therefore keys on **the plan number a document declares**, not on
  the filename, and several documents can legitimately share one table row.
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
