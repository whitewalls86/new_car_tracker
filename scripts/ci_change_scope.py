"""Decide which CI jobs a changeset needs, from a NUL-delimited git path list.

Three zones of the tree can be changed without building or deploying anything:

``docs/``
    Prose. Needs the documentation tests and nothing else.

``ops/static_ops/generated/``
    The published projection of ``docs/``, and the same zone as its source. It
    is written only by ``build_public_roadmap.py`` and ``build_public_recaps.py``
    and it reaches production through ``git pull`` alone -- Plan 138 Stage 7
    mounts the directory read-only from the checkout, so no image carries it and
    no build publishes it. The documentation job runs both generators'
    ``--check``, which is a byte comparison against a regeneration from
    ``docs/``, so a hand-edited artifact fails there. **The generators
    themselves are not in this zone**: they are under ``scripts/``, so changing
    one costs a full run, which is what keeps a bad generator from being waved
    through by its own output.

    Plan 138 Stage 9 is why this zone exists. Before it, an edit to a plan
    document could not move the published page; now ``## What this plan is for``
    is the planned summary, so the ordinary docs change regenerates a file
    outside ``docs/`` and would otherwise have dragged every prose commit into
    the full workflow.

``scripts/oneoff/`` and ``tests/scripts/oneoff/``
    Scripts whose owning plan has archived. Nothing builds them into an image,
    invokes them from a Compose file, a workflow step or an ops route, and they
    are out of the coverage denominator. They still need lint and the unit
    suite -- spent is not untested -- but no Docker build, no dbt build and no
    database.

**The zones compose.** A changeset touching more than one takes the union of
what each needs rather than falling back to the full workflow, which is the
whole point of classifying by zone instead of by a single "is it all docs?"
question.

Everything else is unclassified and takes the full workflow. That is the
fail-open direction and it is deliberate: a path this module has never heard of
must cost a full run, never a skipped one.

**One group is the other way round, and only because it is net-new.**
``snapshot_dbt`` gates the job that builds dbt against a production-derived
Plan 120 snapshot, and it runs on a **named trigger set** rather than on
"anything unclassified": a changeset touching none of those paths does not run
it. Plan 139 Stage E's observation window exists because a false positive in a
*narrowing* selector suppresses evidence that previously existed -- but a job
that has never run suppresses nothing, so the worst case of a missing trigger
here is coverage not gained, never coverage silently lost. That asymmetry does
not hold for ``unit`` or ``heavy`` and this docstring is the whole of why the
two directions coexist in one module.

Fail-open still applies at the level ``ci.yml`` implements it: when there is no
base sha to diff, or this module raises, or its output does not parse, every
group keeps its default -- and ``snapshot_dbt``'s default is true.

Output is one ``name=true|false`` line per job group, which is what
``ci.yml`` reads into ``$GITHUB_OUTPUT``.
"""

from __future__ import annotations

import sys

DOCS_PREFIXES = (b"docs/", b"ops/static_ops/generated/")
ONEOFF_PREFIXES = (b"scripts/oneoff/", b"tests/scripts/oneoff/")

# Paths that can change what `dbt build` does against a pinned snapshot. Wider
# than `dbt/` on purpose, and each entry earns its place:
#
#   dbt/                          the models, the data tests, the profile
#   db/migrations/                the schema the two postgres_scan() sources read
#   .github/ci_lake_snapshot_pin.json   which snapshot is built against
#   .github/workflows/ci.yml      the job, and the dbt version pins it installs
#   scripts/seed_lake_snapshot.py      how the snapshot reaches MinIO and Postgres
#   scripts/download_lake_snapshot.py  how it is fetched and verified
#   scripts/lake_snapshot_common.py    the checksum, extraction and target guards
#   shared/lake_snapshot_postgres.py   the two Postgres sources' round trip
#   shared/sql/                   the statements that round trip is made of
#   scripts/ci_change_scope.py    this file: a gate has to be able to see its
#                                 own edit, or you cannot test a trigger change
SNAPSHOT_DBT_TRIGGERS = (
    b"dbt/",
    b"db/migrations/",
    b".github/ci_lake_snapshot_pin.json",
    b".github/workflows/ci.yml",
    b"scripts/seed_lake_snapshot.py",
    b"scripts/download_lake_snapshot.py",
    b"scripts/lake_snapshot_common.py",
    b"shared/lake_snapshot_postgres.py",
    b"shared/sql/",
    b"scripts/ci_change_scope.py",
)

# What a run needs when the classifier cannot say. Everything but the docs
# suite, which is only ever a substitute for the unit run.
FULL = {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True}


def _paths(data: bytes) -> list[bytes]:
    """Split and validate the NUL-delimited list, or raise.

    Empty input is not an error here -- it is an empty changeset, which
    :func:`classify_from_nul` reports as needing the full workflow.
    """
    if not data:
        return []
    if not data.endswith(b"\0"):
        raise ValueError("changed-path input is not NUL terminated")

    paths = data[:-1].split(b"\0")
    if any(not path for path in paths):
        raise ValueError("changed-path input contains an empty path")
    return paths


def classify_from_nul(data: bytes) -> dict[str, bool]:
    """Map a changeset to the job groups it needs.

    ``docs_tests`` is true only when the unit suite is *not* running. The unit
    job runs ``pytest tests/``, which already includes ``test_planning_docs.py``
    -- so on a mixed docs-and-oneoff change the documentation job would be
    re-running tests the unit job has already run. The union of what the two
    zones need, with that redundancy removed, is lint plus the unit suite.
    """
    paths = _paths(data)
    if not paths:
        return dict(FULL)

    # Computed before the zone check below, because the trigger set is an
    # allowlist and not the complement of one: it answers its own question on
    # every changeset, classified or not.
    snapshot_dbt = any(path.startswith(SNAPSHOT_DBT_TRIGGERS) for path in paths)

    docs = sum(path.startswith(DOCS_PREFIXES) for path in paths)
    oneoff = sum(path.startswith(ONEOFF_PREFIXES) for path in paths)
    if docs + oneoff != len(paths):
        return {**FULL, "snapshot_dbt": snapshot_dbt}

    return {
        "docs_tests": bool(docs) and not oneoff,
        "unit": bool(oneoff),
        "heavy": False,
        "snapshot_dbt": snapshot_dbt,
    }


def main() -> int:
    try:
        scopes = classify_from_nul(sys.stdin.buffer.read())
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    for name, value in scopes.items():
        print(f"{name}={'true' if value else 'false'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
