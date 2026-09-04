"""Plan 138 Stage 5: the README's links resolve and its two stacks stay apart.

``README.md`` is a public surface — the repository is public, so a change to it
is published the moment it merges, with no deploy in between. The commit gate
holds it until `public-surface-check` has read the diff, but that gate reads
*changed* lines only. These are the two whole-file properties nothing else
asserts.

**The link check catches rot, not typos.** A relative link in the README points
at a file in this repository, and files move — Plan 146 restructured `docs/`,
Plan 172 added `PLAN_DOCUMENT.md`, Plan 138 Stage 8 moved this plan's contract
sections into `PUBLIC_SURFACE.md`. Every one of those was a chance to leave a
link pointing at nothing, and a reader who follows one gets a 404 on the front
door of a portfolio project.

**The stack check is the drift this plan exists for.** Stage 0 measured a public
surface claiming capabilities the repository did not have. The landing page's
half of this is
``test_iceberg_is_labeled_a_migration_track_and_not_a_capability``; this is the
README's, and it is deliberately structural rather than a phrase list. It
asserts that the experimental names appear under the heading that disclaims
them and nowhere near the one that does not, so a later edit cannot quietly
promote Iceberg into the production list by moving one bullet.
"""
from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_README = _REPO_ROOT / "README.md"

# The two headings that carry the split. Asserted to exist in their own right:
# a test that located its section by string match would pass vacuously the day
# somebody reworded the heading, which is the failure mode this file is for.
_PRODUCTION_HEADING = "**Running in production and serving users:**"
_NOT_PRODUCTION_HEADING = "**Proven but not production-serving.**"

# The migration track. None of these is in the path of anything a user sees, and
# the README says so; this is the list that must not drift across the heading.
_EXPERIMENTAL_NAMES = ("Iceberg", "Lakekeeper", "Spark", "MLflow")

_MARKDOWN_LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def _readme() -> str:
    return _README.read_text(encoding="utf-8")


def _local_link_targets(source: str) -> list[str]:
    """Repository-relative link targets, with fragments and externals dropped."""
    targets = []
    for raw in _MARKDOWN_LINK.findall(source):
        target = raw.strip()
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = target.split("#", 1)[0].strip()
        if path:
            targets.append(path)
    return targets


def _section(source: str, start: str, end: str | None) -> str:
    assert start in source, f"README no longer contains {start!r}"
    body = source.split(start, 1)[1]
    if end is not None:
        assert end in body, f"README no longer contains {end!r} after {start!r}"
        body = body.split(end, 1)[0]
    return body


def test_every_local_readme_link_resolves():
    """A relative link that stopped resolving is a 404 on the front door."""
    source = _readme()

    broken = sorted(
        target
        for target in _local_link_targets(source)
        if not (_REPO_ROOT / target).exists()
    )

    assert not broken, (
        "README.md links to paths that do not exist:\n  " + "\n  ".join(broken)
    )


def test_the_readme_states_both_halves_of_the_production_split():
    """Both headings exist, so the two tests below cannot pass vacuously."""
    source = _readme()

    assert _PRODUCTION_HEADING in source
    assert _NOT_PRODUCTION_HEADING in source
    assert source.index(_PRODUCTION_HEADING) < source.index(_NOT_PRODUCTION_HEADING)


def test_no_experimental_component_is_listed_as_production():
    """The drift Stage 0 measured, in the direction that overstates the system."""
    production = _section(_readme(), _PRODUCTION_HEADING, _NOT_PRODUCTION_HEADING)

    promoted = sorted(
        name for name in _EXPERIMENTAL_NAMES if name.lower() in production.lower()
    )

    assert not promoted, (
        f"{promoted} appear under {_PRODUCTION_HEADING!r}. The Iceberg work is a "
        f"migration track with its own gates, not a shipped capability. If one of "
        f"these genuinely reached production, move the bullet and update the "
        f"landing page in the same change -- the two surfaces owe each other "
        f"agreement on this."
    )


def test_the_experimental_stack_is_still_disclaimed_by_name():
    """The other direction: deleting the names would also pass the test above."""
    disclaimed = _section(_readme(), _NOT_PRODUCTION_HEADING, None)

    missing = sorted(
        name for name in _EXPERIMENTAL_NAMES if name.lower() not in disclaimed.lower()
    )

    assert not missing, (
        f"{missing} are no longer named under {_NOT_PRODUCTION_HEADING!r}. If the "
        f"component left the repository, drop it from this test's list; if it "
        f"reached production, say so in the production section instead."
    )


def test_duckdb_is_named_as_what_actually_serves():
    """DuckDB is the current analytics endpoint, and the split turns on saying so.

    Plan 125 will replace it. Until then, a README that stops naming what serves
    the dashboard leaves a reader to infer it from the migration track, which is
    exactly the inference the split exists to prevent.
    """
    production = _section(_readme(), _PRODUCTION_HEADING, _NOT_PRODUCTION_HEADING)

    assert "duckdb" in production.lower()
