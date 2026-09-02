"""Plan 138 Stage 1d: the public roadmap projection, asserted rather than eyeballed.

Layer 1. Two halves, and the split is deliberate.

The first half pins the extraction rules against synthetic tables, because
every one of them was chosen over a simpler rule that fails on real input --
sentence ends that are not dots, effort tokens that are not the whole cell, a
bolded lead whose own title contains an em dash.

The second half asserts the **committed artifact** against the repository, so a
roadmap edit whose public projection was not regenerated fails here as well as
in CI's ``--check`` step. That is the assertion this file exists for: the
failure mode Stage 1d is guarding against is not a crash, it is a public page
quietly serving last month's roadmap.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

from scripts import build_public_roadmap as brm
from scripts.build_public_roadmap import RoadmapBuildError

# ---------------------------------------------------------------------------
# Extraction rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("[162](plans/plan_162_x.md)", "162"),
        ("`last_detail_scraped_at` was the guard", "last_detail_scraped_at was the guard"),
        ("**bold** and *italic*", "bold and italic"),
        ("*(observed)*", "(observed)"),
        ("a\n  b   c", "a b c"),
    ],
)
def test_flatten_markdown_leaves_plain_text(cell, expected):
    assert brm.flatten_markdown(cell) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # The dots this corpus is full of are not sentence ends: nothing
        # follows them but another character.
        ("docs/ARCHITECTURE.md:179 carried a section. Then more.",
         "docs/ARCHITECTURE.md:179 carried a section."),
        ("redeploy.sh could not deploy. _DeployIntentSensor.poke() returned early.",
         "redeploy.sh could not deploy."),
        ("FastAPI changed between 0.128 and 0.141 without erroring. Repaired later.",
         "FastAPI changed between 0.128 and 0.141 without erroring."),
        # A following sentence that opens with an identifier still ends the one
        # before it -- requiring a capital ran four summaries into paragraphs.
        ("It broke naturally. dbt then rebuilt it.", "It broke naturally."),
        ("Shipped 2026-08-18, PR #213. Stage F shipped later.",
         "Shipped 2026-08-18, PR #213."),
        ("One sentence with no terminator", "One sentence with no terminator"),
    ],
)
def test_first_sentence_stops_where_a_reader_would(text, expected):
    assert brm.first_sentence(text) == expected


@pytest.mark.parametrize(
    ("cell", "title", "remainder"),
    [
        ("**The testing contract** — it carried a section.",
         "The testing contract", "it carried a section."),
        # The archive's own titles contain em dashes, so the bold delimiter has
        # to be what closes the title, not the first dash.
        ("**Scrape state ownership — separating fetch from enrichment** — it was the guard.",
         "Scrape state ownership — separating fetch from enrichment", "it was the guard."),
        ("**April cutover reconciliation** -- Deleted objects.",
         "April cutover reconciliation", "Deleted objects."),
        ("**Title**: a colon separator", "Title", "a colon separator"),
    ],
)
def test_bolded_lead_splits_title_from_body(cell, title, remainder):
    assert brm.bolded_lead(cell) == (title, remainder)


def test_bolded_lead_refuses_a_cell_with_no_title():
    with pytest.raises(RoadmapBuildError, match="no bolded lead"):
        brm.bolded_lead("Deleted 1,172 legacy objects after recovering every capture.")


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("L", "L"),
        ("XS each", "XS"),
        ("XL, research-gated", "XL"),
        ("M + first observed window", "M"),
        ("S + 7d observation", "S"),
    ],
)
def test_effort_is_read_off_the_leading_token(cell, expected):
    assert brm.leading_effort_token(cell) == expected


@pytest.mark.parametrize("cell", ["Large", "Small", "", "medium", "MLarge"])
def test_effort_that_is_not_a_token_fails_the_build(cell):
    with pytest.raises(RoadmapBuildError, match="does not begin with"):
        brm.leading_effort_token(cell)


# ---------------------------------------------------------------------------
# Table location
# ---------------------------------------------------------------------------

_GOOD_TABLE = """## Default build order

| Order | Plan |
|---|---|
| 1 | [1](a.md) |
"""


def test_a_named_table_is_found_under_its_heading():
    rows = brm._table_under_heading(_GOOD_TABLE, "Default build order", ("Order", "Plan"), "x")
    assert rows == [["1", "[1](a.md)"]]


def test_a_missing_heading_fails_rather_than_falling_through():
    with pytest.raises(RoadmapBuildError, match="no '## Completed'"):
        brm._table_under_heading(_GOOD_TABLE, "Completed", ("Order", "Plan"), "x")


def test_an_inserted_column_fails_the_build():
    """The header check is what makes this a named table and not the nearest one."""
    with pytest.raises(RoadmapBuildError, match="expected columns"):
        brm._table_under_heading(_GOOD_TABLE, "Default build order", ("Order", "Title"), "x")


def test_a_heading_with_no_rows_fails_rather_than_publishing_nothing():
    """Plan 146's pointer under 'Completed' is exactly this shape."""
    text = "## Completed\n\nSee the archive.\n\n## Next\n"
    with pytest.raises(RoadmapBuildError, match="no Markdown table"):
        brm._table_under_heading(text, "Completed", ("Plan",), "x")


@pytest.mark.parametrize(
    "text",
    [
        "| Other | Table |\n|---|---|\n| a | b |\n",
        "| Plan | Date |\n|---|---|\n| 1 | x |\n\n| Plan | Date |\n|---|---|\n| 2 | y |\n",
    ],
)
def test_the_archive_table_must_be_the_only_one_of_its_shape(text):
    with pytest.raises(RoadmapBuildError, match="exactly one table"):
        brm._sole_table(text, ("Plan", "Date"), "x")


# ---------------------------------------------------------------------------
# Plan links
# ---------------------------------------------------------------------------


def test_an_unambiguous_plan_number_resolves_to_its_document():
    assert brm.plan_document("161").name == "plan_161_testing_contract.md"


def test_every_plan_number_globs_to_exactly_one_document():
    """``docs/plans/`` holds plans and nothing else, and this is what keeps it so.

    Plan 145 used to match nine files there -- the main document plus eight
    stage handoffs -- which is the ambiguity §1d was written around. The
    handoffs moved to ``docs/prompts/`` and the reports to ``docs/reference/``
    on 2026-09-01, so the glob is now one-to-one for every number. File a
    handoff back into ``docs/plans/`` and this fails, before the disambiguation
    rule below ever has to save the build.
    """
    counts = defaultdict(list)
    for path in (brm.REPO_ROOT / brm.PLANS_DIR).glob("*.md"):
        match = re.match(r"plan_([0-9A-Za-z.]+)_", path.name)
        if match:
            counts[match.group(1)].append(path.name)

    assert counts, "the glob matched nothing, so this asserts nothing"
    ambiguous = {number: names for number, names in counts.items() if len(names) > 1}
    assert not ambiguous, f"plan numbers matching more than one document: {ambiguous}"


def test_the_main_document_wins_over_stage_handoffs(tmp_path, mocker):
    """The guard the restructure made unnecessary, and which still has to work.

    ``docs/plans/`` is one-to-one today, so nothing in the tree exercises this.
    The exit criteria still require an ambiguous glob to resolve to the main
    document or fail, and a rule with no live input is exactly the rule that
    rots -- so it is exercised synthetically.
    """
    plans = tmp_path / brm.PLANS_DIR
    plans.mkdir(parents=True)
    (plans / "plan_145_april_cutover_reconciliation.md").write_text(
        "# Plan 145: Recover April Detail Artifacts\n", encoding="utf-8"
    )
    for stage in (3, 4, 6):
        (plans / f"plan_145_stage_{stage}_handoff.md").write_text(
            f"# Plan 145 Stage {stage} — implementation prompt\n", encoding="utf-8"
        )
    mocker.patch.object(brm, "REPO_ROOT", tmp_path)

    assert brm.plan_document("145").name == "plan_145_april_cutover_reconciliation.md"


def test_a_plan_with_no_document_fails_rather_than_publishing_a_dead_link():
    with pytest.raises(RoadmapBuildError, match="has no document"):
        brm.plan_document("9999")


def test_ambiguity_the_title_rule_cannot_settle_is_a_build_failure(tmp_path, mocker):
    plans = tmp_path / brm.PLANS_DIR
    plans.mkdir(parents=True)
    (plans / "plan_900_one.md").write_text("# Plan 900: One\n", encoding="utf-8")
    (plans / "plan_900_two.md").write_text("# Plan 900: Two\n", encoding="utf-8")
    mocker.patch.object(brm, "REPO_ROOT", tmp_path)

    with pytest.raises(RoadmapBuildError, match="ambiguous"):
        brm.plan_document("900")


# ---------------------------------------------------------------------------
# A plan's own public copy, and the fallback to extraction
# ---------------------------------------------------------------------------


def _plan_doc(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "plan_900_example.md"
    path.write_text("# Plan 900: Example\n\n" + body, encoding="utf-8")
    return path


def test_a_plan_with_no_public_summary_section_returns_none(tmp_path):
    """The normal state for the 118 plans archived before this existed."""
    assert brm.authored_summary(_plan_doc(tmp_path, "## Status\n\nDone.\n")) is None


def test_an_authored_summary_is_read_whole_rather_than_cut(tmp_path):
    """It was written for this. Truncating authored copy is the opposite mistake."""
    path = _plan_doc(
        tmp_path,
        "## Public summary\n\n**Scrape state ownership** — Separated two "
        "timestamps. A stalled processor no longer re-fetches the same listings.\n\n"
        "## Status\n\nDone.\n",
    )
    assert brm.authored_summary(path) == (
        "Scrape state ownership",
        "Separated two timestamps. A stalled processor no longer re-fetches "
        "the same listings.",
    )


def test_an_authored_summary_is_flattened_like_any_other_cell(tmp_path):
    path = _plan_doc(
        tmp_path,
        "## Public summary\n\n**Title** — It moved `a_column` and "
        "[a doc](../x.md).\n",
    )
    assert brm.authored_summary(path) == ("Title", "It moved a_column and a doc.")


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ("## Public summary\n\n## Status\n\nDone.\n", "is empty"),
        ("## Public summary\n\n**Title only**\n", "no summary after it"),
        ("## Public summary\n\nNo bolded lead here.\n", "no bolded lead"),
    ],
)
def test_a_malformed_public_summary_fails_the_build(tmp_path, body, match):
    with pytest.raises(RoadmapBuildError, match=match):
        brm.authored_summary(_plan_doc(tmp_path, body))


def test_an_authored_summary_wins_over_the_archive_cell(tmp_path, mocker):
    mocker.patch.object(
        brm, "authored_summary", return_value=("Authored title", "Authored summary.")
    )
    archive = (
        "| Plan | Description | Date |\n|---|---|---|\n"
        "| 161 | **Extracted title** — extracted summary. And more. | 2026-08-31 |\n"
    )
    (item,) = brm.completed_items(archive)
    assert (item["title"], item["summary"]) == ("Authored title", "Authored summary.")


def test_extraction_is_recorded_so_the_gate_knows_which_rows_to_read(tmp_path, mocker):
    mocker.patch.object(brm, "authored_summary", return_value=None)
    archive = (
        "| Plan | Description | Date |\n|---|---|---|\n"
        "| 161 | **Extracted title** — extracted summary. And more. | 2026-08-31 |\n"
    )
    extracted: list[str] = []
    (item,) = brm.completed_items(archive, extracted)
    assert extracted == ["161"]
    assert item["summary"] == "extracted summary."


def test_a_summary_over_the_cap_fails_and_says_what_to_write(mocker):
    """An archive cell whose first sentence is a paragraph must not go public."""
    mocker.patch.object(brm, "authored_summary", return_value=None)
    archive = (
        "| Plan | Description | Date |\n|---|---|---|\n"
        f"| 161 | **Title** — {'word ' * 100}. | 2026-08-31 |\n"
    )
    with pytest.raises(RoadmapBuildError, match="Public summary"):
        brm.completed_items(archive)


def test_a_build_order_link_to_a_missing_file_fails_the_build():
    with pytest.raises(RoadmapBuildError, match="missing file"):
        brm._linked_plan("[999](plans/plan_999_nonexistent.md)", "x")


def test_a_stage_marker_after_the_link_does_not_confuse_the_plan_cell():
    """Rows such as ``[154](...) **Stage 0**`` are live in the build order."""
    number, href = brm._linked_plan(
        "[154](plans/plan_154_container_log_coverage.md) **Stage 0**", "x"
    )
    assert number == "154"
    assert href.endswith("docs/plans/plan_154_container_log_coverage.md")


# ---------------------------------------------------------------------------
# The committed artifact, against the repository
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads((brm.REPO_ROOT / brm.OUTPUT).read_text(encoding="utf-8"))


def test_the_committed_artifact_is_not_stale():
    """A roadmap edit without a regenerated projection fails here."""
    committed = (brm.REPO_ROOT / brm.OUTPUT).read_text(encoding="utf-8")
    assert committed == brm.render(brm.build())


def test_as_of_comes_from_the_index_and_not_the_clock(snapshot):
    heading = f"## Current State (as of {snapshot['as_of']})"
    assert heading in (brm.REPO_ROOT / brm.INDEX).read_text(encoding="utf-8")


def test_the_schema_version_is_declared(snapshot):
    assert snapshot["schema_version"] == brm.SCHEMA_VERSION


@pytest.mark.parametrize("key", ["planned", "completed"])
def test_each_list_is_populated_and_capped(snapshot, key):
    assert 0 < len(snapshot[key]) <= brm.MAX_ITEMS


def test_planned_items_carry_the_planned_fields(snapshot):
    for item in snapshot["planned"]:
        assert item["state"] == "planned"
        assert 0 <= item["priority"] <= 100
        assert item["effort"] in brm.EFFORT_TOKENS
        assert item["title"] and item["summary"]


def test_the_published_build_order_is_unique_and_ascending(snapshot):
    orders = [item["order"] for item in snapshot["planned"]]
    assert orders == sorted(set(orders))


def test_completed_items_emit_no_priority_or_effort(snapshot):
    """The archive has no such columns, so inventing them would be fabrication."""
    for item in snapshot["completed"]:
        assert item["state"] == "completed"
        assert "priority" not in item
        assert "effort" not in item


def test_completed_items_are_newest_first(snapshot):
    dates = [item["date"] for item in snapshot["completed"]]
    assert dates == sorted(dates, reverse=True)


def test_every_link_resolves_to_a_file_in_this_repository(snapshot):
    for item in snapshot["planned"] + snapshot["completed"]:
        assert item["href"].startswith(brm.BLOB_BASE)
        local = brm.REPO_ROOT / item["href"][len(brm.BLOB_BASE):]
        assert local.is_file(), f"plan {item['plan']} links to a missing {local}"


def test_no_summary_carries_markup_the_page_would_show_literally(snapshot):
    """Every string is rendered with textContent, so markup would be shown raw."""
    for item in snapshot["planned"] + snapshot["completed"]:
        for field in ("title", "summary"):
            assert "`" not in item[field]
            assert "**" not in item[field]
            assert "](" not in item[field]


# ---------------------------------------------------------------------------
# The landing page renders the artifact this script writes
# ---------------------------------------------------------------------------


def test_the_landing_page_fetches_this_artifact_into_both_lists():
    """The generator and its only consumer are asserted together, not by hand."""
    page = (brm.REPO_ROOT / "ops/templates/info.html").read_text(encoding="utf-8")
    assert "/static_ops/generated/project-updates.json" in page
    for element_id in ("work-planned", "work-completed"):
        assert f'id="{element_id}"' in page


def test_the_landing_page_keeps_a_fallback_for_every_failure_mode():
    """Both lists must be meaningful before any JavaScript runs."""
    page = (brm.REPO_ROOT / "ops/templates/info.html").read_text(encoding="utf-8")
    for element_id in ("work-planned", "work-completed"):
        start = page.index(f'id="{element_id}"')
        end = page.index("</ul>", start)
        assert "<li>" in page[start:end]


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/build_public_roadmap.py", *args],
        cwd=brm.REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def test_check_passes_against_the_committed_artifact():
    assert _run("--check").returncode == 0


def test_check_reports_drift_and_writes_nothing(tmp_path, mocker):
    """The CI contract: exit non-zero on drift, and leave the file alone."""
    destination = brm.REPO_ROOT / brm.OUTPUT
    before = destination.read_text(encoding="utf-8")
    mocker.patch.object(brm, "MAX_ITEMS", 1)

    assert brm.main(["--check"]) == 1
    assert destination.read_text(encoding="utf-8") == before


def test_a_source_that_does_not_parse_exits_two_rather_than_writing(tmp_path, mocker):
    mocker.patch.object(brm, "INDEX", "docs/planning/completed_plans.md")
    destination = brm.REPO_ROOT / brm.OUTPUT
    before = destination.read_text(encoding="utf-8")

    assert brm.main([]) == 2
    assert destination.read_text(encoding="utf-8") == before


def test_the_artifact_is_written_where_the_public_route_serves_it():
    assert brm.OUTPUT.startswith("ops/static_ops/generated/")
    assert (Path(brm.REPO_ROOT) / "Caddyfile").read_text(encoding="utf-8").count(
        "handle /static_ops/*"
    ) == 1
