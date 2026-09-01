"""Plan 138 Stage 1e: the weekly recap projection.

Gate 1e is three claims -- the publication policy is committed, the generator
refuses an unclassifiable link rather than emitting it, and ``--check`` fails on
a recap that has not been regenerated. Each has a test here that fails when the
claim stops being true, rather than a run that happened to pass once.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import build_public_recaps as brc
from scripts.build_public_recaps import Recap, RecapBuildError

REPO_ROOT = brc.REPO_ROOT

HEADER = """\
# Week of 2026-08-24 to 2026-08-30

**Window:** 2026-08-24 00:00:00 to 2026-08-30 23:59:59, local author time
**Recapped:** 2026-08-31
**Commits in window:** 276 (216 non-merge, 60 merges)
**Publish:** {publish}

## What shipped

{body}
"""


def _recap_file(tmp_path: Path, publish: str = "true", body: str = "Something.",
                name: str = "2026-08-30.md") -> Path:
    path = tmp_path / name
    path.write_text(HEADER.format(publish=publish, body=body), encoding="utf-8")
    return path


def _live_recap(publish: bool = True) -> Recap:
    """A Recap anchored at a real path, so relative links resolve for real."""
    return Recap(
        REPO_ROOT / brc.RECAPS_DIR / "2026-08-30.md",
        "2026-08-24",
        "2026-08-30",
        publish,
        "",
    )


# ---------------------------------------------------------------------------
# The publication marker -- Gate 1e's first claim
# ---------------------------------------------------------------------------

def test_a_recap_with_no_publish_marker_fails_the_build(tmp_path):
    path = tmp_path / "2026-08-30.md"
    path.write_text(
        HEADER.format(publish="true", body="x").replace("**Publish:** true\n", ""),
        encoding="utf-8",
    )
    with pytest.raises(RecapBuildError, match="editorial decision"):
        brc.read_recap(path)


@pytest.mark.parametrize("value", ["True", "yes", "1", "TRUE", "maybe", "no"])
def test_a_publish_marker_that_is_not_exactly_true_or_false_fails(tmp_path, value):
    with pytest.raises(RecapBuildError, match="exactly 'true' or 'false'"):
        brc.read_recap(_recap_file(tmp_path, publish=value))


def test_two_publish_markers_fail_rather_than_one_winning(tmp_path):
    path = _recap_file(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "**Publish:** true", "**Publish:** true\n**Publish:** false"
        ),
        encoding="utf-8",
    )
    with pytest.raises(RecapBuildError, match="more than one"):
        brc.read_recap(path)


@pytest.mark.parametrize("value,expected", [("true", True), ("false", False)])
def test_the_marker_is_read_as_written(tmp_path, value, expected):
    assert brc.read_recap(_recap_file(tmp_path, publish=value)).publish is expected


def test_the_marker_never_reaches_the_page(tmp_path):
    """The marker is plumbing; the rest of the header block is fact the reader sees.

    Asserting the absence alone would pass against a renderer that emitted
    nothing at all, so this also pins what the page must still carry.
    """
    recap = brc.read_recap(_recap_file(tmp_path, body="Something happened."))
    rendered = brc.render_page(recap, {recap.slug})
    assert "Publish" not in rendered
    assert "Something happened." in rendered
    assert "**Window:**" not in rendered, "the header block should be rendered, not literal"
    assert "2026-08-24 00:00:00" in rendered, "the window is a fact the page keeps"


# ---------------------------------------------------------------------------
# Filename and week agreement
# ---------------------------------------------------------------------------

def test_a_filename_that_disagrees_with_the_heading_fails(tmp_path):
    with pytest.raises(RecapBuildError, match="filename says the week ends"):
        brc.read_recap(_recap_file(tmp_path, name="2026-08-23.md"))


def test_a_filename_that_is_not_a_date_fails(tmp_path):
    with pytest.raises(RecapBuildError, match="must be YYYY-MM-DD"):
        brc.read_recap(_recap_file(tmp_path, name="latest.md"))


def test_a_missing_week_heading_fails_rather_than_rendering_untitled(tmp_path):
    path = tmp_path / "2026-08-30.md"
    path.write_text("# Last week\n\n**Publish:** true\n", encoding="utf-8")
    with pytest.raises(RecapBuildError, match="first line must be"):
        brc.read_recap(path)


# ---------------------------------------------------------------------------
# Link classification -- Gate 1e's second claim
# ---------------------------------------------------------------------------

def test_an_internal_anchor_stays_internal():
    assert brc.rewrite_href("#merges", _live_recap(), set()) == "#merges"


def test_an_absolute_url_is_left_alone():
    url = "https://github.com/whitewalls86/Car-Tracker"
    assert brc.rewrite_href(url, _live_recap(), set()) == url


@pytest.mark.parametrize(
    "target",
    [
        "../plans/plan_138_public_surface_refresh.md",
        "../planning/completed_plans.md",
        "../reference/plan_123_dbt_resource_baseline.md",
    ],
)
def test_every_repository_directory_resolves_to_the_same_blob_url(target):
    """Classification is on the docs/-relative path, not on plans/ alone.

    The corpus holds 128 ``../plans/``, 4 ``../planning/`` and 1
    ``../reference/`` link. A rule that recognised only the first would refuse
    to build against files that are already committed.
    """
    result = brc.rewrite_href(target, _live_recap(), set())
    assert result.startswith(brc.BLOB_BASE + "docs/")
    assert result.endswith(Path(target).name)


def test_a_sibling_recap_resolves_to_the_neighbouring_page_when_published():
    assert brc.rewrite_href(
        "2026-08-16.md", _live_recap(), {"2026-08-16"}
    ) == "2026-08-16.html"


def test_a_sibling_recap_falls_back_to_github_when_that_week_is_not_published():
    """The link must not point at a page the projection deliberately did not build."""
    result = brc.rewrite_href("2026-08-16.md", _live_recap(), set())
    assert result == brc.BLOB_BASE + "docs/recaps/2026-08-16.md"


def test_a_fragment_survives_the_rewrite():
    result = brc.rewrite_href(
        "../plans/plan_138_public_surface_refresh.md#stage-1e", _live_recap(), set()
    )
    assert result.endswith("plan_138_public_surface_refresh.md#stage-1e")


@pytest.mark.parametrize(
    "target",
    [
        "/absolute/path.md",
        "mailto:someone@example.com",
        "tel:+15550100",
        "ftp://example.com/x",
    ],
)
def test_an_unclassifiable_link_is_a_build_failure_not_a_passthrough(target):
    """Gate 1e: the generator refuses rather than emitting it.

    A silent passthrough is how a docs/-relative path becomes a 404 on the
    public site.
    """
    with pytest.raises(RecapBuildError, match="cannot classify|resolves"):
        brc.rewrite_href(target, _live_recap(), set())


def test_a_link_to_a_file_that_does_not_exist_fails_the_build():
    with pytest.raises(RecapBuildError, match="not a file in this tree"):
        brc.rewrite_href("../plans/plan_999_imaginary.md", _live_recap(), set())


def test_a_link_that_escapes_the_repository_fails_the_build():
    with pytest.raises(RecapBuildError, match="outside the repository|not a file"):
        brc.rewrite_href("../../../../etc/passwd", _live_recap(), set())


def test_an_empty_link_target_fails_the_build():
    with pytest.raises(RecapBuildError, match="empty link target"):
        brc.rewrite_href("   ", _live_recap(), set())


# ---------------------------------------------------------------------------
# Rendering properties
# ---------------------------------------------------------------------------

def test_a_link_inside_a_fenced_code_block_is_not_rewritten(tmp_path):
    """Why the rewrite walks tokens instead of the text.

    A regex over Markdown cannot tell this from a real link, and would fail the
    build on a path that is documentation, not a destination. The corpus has 42
    fenced blocks full of shell commands and paths.
    """
    body = "```bash\ngit show ../plans/plan_999_imaginary.md\n```"
    recap = brc.read_recap(_recap_file(tmp_path, body=body))
    rendered = brc.render_page(recap, {recap.slug})
    assert "plan_999_imaginary.md" in rendered
    assert "<code" in rendered


def test_raw_html_in_a_recap_is_escaped_rather_than_passed_through(tmp_path):
    recap = brc.read_recap(_recap_file(tmp_path, body="<script>alert(1)</script>"))
    rendered = brc.render_page(recap, {recap.slug})
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered


def test_every_page_carries_its_week_and_a_point_in_time_note(tmp_path):
    """The truth contract's §5: a recap is correct as of its date only."""
    recap = brc.read_recap(_recap_file(tmp_path))
    rendered = brc.render_page(recap, {recap.slug})
    assert "Week of 2026-08-24 to 2026-08-30" in rendered
    assert "point-in-time record" in rendered
    assert "is not revised" in rendered


def test_repeated_headings_get_distinct_anchors(tmp_path):
    body = "## Merges\n\nfirst\n\n## Merges\n\nsecond"
    recap = brc.read_recap(_recap_file(tmp_path, body=body))
    rendered = brc.render_page(recap, {recap.slug})
    assert '<h2 id="merges">' in rendered
    assert '<h2 id="merges-1">' in rendered


def test_tables_are_wrapped_so_they_scroll_instead_of_widening_the_page(tmp_path):
    body = "| a | b |\n|---|---|\n| 1 | 2 |"
    recap = brc.read_recap(_recap_file(tmp_path, body=body))
    rendered = brc.render_page(recap, {recap.slug})
    assert '<div class="table-scroll"><table>' in rendered
    assert "</table></div>" in rendered


# ---------------------------------------------------------------------------
# The projection over the real corpus
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def files() -> dict[str, str]:
    return brc.build()


def test_the_committed_artifact_is_not_stale(files):
    """Gate 1e's third claim, asserted here as well as by --check in CI."""
    destination = REPO_ROOT / brc.OUTPUT_DIR
    on_disk = {path.name: path.read_text(encoding="utf-8")
               for path in sorted(destination.glob("*.html"))}
    assert on_disk == files, (
        "the committed recap projection is stale -- run "
        "`python scripts/build_public_recaps.py` and commit the result"
    )


def test_no_recap_outside_the_published_set_produced_output(files):
    withheld = [recap.slug for recap in brc.read_all() if not recap.publish]
    assert withheld, "expected at least one withheld recap to make this meaningful"
    leaked = [slug for slug in withheld if f"{slug}.html" in files]
    assert not leaked, f"withheld recaps produced pages: {leaked}"


def test_every_published_recap_produced_exactly_one_page(files):
    published = {recap.slug for recap in brc.read_all() if recap.publish}
    pages = {name[: -len(".html")] for name in files if name != "index.html"}
    assert pages == published


def test_the_index_is_newest_first(files):
    order = [
        line.split('href="')[1].split('.html')[0]
        for line in files["index.html"].splitlines()
        if 'class="index-list"' not in line and 'href="' in line and ".html" in line
    ]
    assert order == sorted(order, reverse=True), f"index is not newest-first: {order}"
    assert len(order) == len(files) - 1


def test_every_published_week_agrees_with_its_filename(files):
    for recap in brc.read_all():
        if recap.publish:
            assert recap.week_end == recap.slug


def test_the_corpus_still_holds_the_link_classes_this_script_was_built_for():
    """The counts in the module docstring, asserted rather than remembered."""
    hrefs = []
    for recap in brc.read_all():
        for line in recap.body.splitlines():
            hrefs.extend(part.split(")")[0] for part in line.split("](")[1:])
    classes = {
        "plans": sum(1 for h in hrefs if h.startswith("../plans/")),
        "planning": sum(1 for h in hrefs if h.startswith("../planning/")),
        "reference": sum(1 for h in hrefs if h.startswith("../reference/")),
    }
    assert classes["plans"] > 100
    assert classes["planning"] > 0, "the ../planning/ class disappeared"
    assert classes["reference"] > 0, "the ../reference/ class disappeared"


def test_no_generated_page_carries_a_relative_markdown_link(files):
    """The failure this whole classifier exists to prevent.

    A blob URL legitimately ends in ``.md``; what must not survive is a
    *relative* one, which is the form that 404s on the public site.
    """
    for name, text in files.items():
        for href in re.findall(r'href="([^"]*)"', text):
            if href.startswith(("http://", "https://", "#")):
                continue
            assert href.endswith(".html"), f"{name} kept a relative link: {href}"


# ---------------------------------------------------------------------------
# --check -- Gate 1e's third claim, end to end
# ---------------------------------------------------------------------------

def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/build_public_recaps.py", *args],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )


def test_check_passes_against_the_committed_artifact():
    result = _run("--check")
    assert result.returncode == 0, result.stderr


def test_check_fails_on_a_new_recap_that_has_not_been_regenerated():
    """Gate 1e: a recap nobody regenerated must not pass CI.

    Driven through the real file and the real subprocess rather than a patched
    ``read_all``, because what CI runs is the subprocess. A reserved far-future
    week keeps this clear of the corpus the other tests read.
    """
    new = REPO_ROOT / brc.RECAPS_DIR / "2099-01-04.md"
    assert not new.exists(), "2099-01-04.md is reserved for this test"
    new.write_text(
        "# Week of 2098-12-29 to 2099-01-04\n\n"
        "**Window:** 2098-12-29 00:00:00 to 2099-01-04 23:59:59, local author time\n"
        "**Recapped:** 2099-01-05\n"
        "**Commits in window:** 1 (1 non-merge, 0 merges)\n"
        "**Publish:** true\n\n"
        "## What shipped\n\nA week that exists only inside this test.\n",
        encoding="utf-8",
    )
    try:
        result = _run("--check")
        assert result.returncode == 1, (
            f"--check passed against an unregenerated recap: {result.stdout}"
        )
        assert "2099-01-04.html" in result.stderr
        assert "not generated" in result.stderr
    finally:
        new.unlink()

    assert _run("--check").returncode == 0, "the corpus was left dirty"


def test_check_fails_on_a_page_whose_recap_stopped_being_published():
    """A recap flipped to false must not keep serving its old page.

    The orphan direction matters as much as the stale one: without it, marking
    a week unpublished would leave it on the site and nothing would say so.
    """
    orphan = REPO_ROOT / brc.OUTPUT_DIR / "2099-01-04.html"
    assert not orphan.exists(), "2099-01-04.html is reserved for this test"
    orphan.write_text("<!doctype html><title>x</title>\n", encoding="utf-8")
    try:
        result = _run("--check")
        assert result.returncode == 1, (
            f"--check passed with an orphaned page: {result.stdout}"
        )
        assert "2099-01-04.html" in result.stderr
        assert "without a recap behind it" in result.stderr
    finally:
        orphan.unlink()

    assert _run("--check").returncode == 0, "the output directory was left dirty"


def test_check_fails_on_an_edited_page():
    """The stale direction: a page edited by hand rather than regenerated."""
    page = REPO_ROOT / brc.OUTPUT_DIR / "index.html"
    original = page.read_text(encoding="utf-8")
    page.write_text(original + "<!-- edited by hand -->\n", encoding="utf-8")
    try:
        result = _run("--check")
        assert result.returncode == 1, "--check passed against an edited page"
        assert "stale" in result.stderr
    finally:
        page.write_text(original, encoding="utf-8")

    assert _run("--check").returncode == 0, "the artifact was left dirty"
