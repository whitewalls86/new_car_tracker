"""Decide which CI jobs a changeset needs, from a NUL-delimited git path list.

Two zones of the tree can be changed without building or deploying anything:

``docs/``
    Prose. Needs the documentation tests and nothing else.

``scripts/oneoff/`` and ``tests/scripts/oneoff/``
    Scripts whose owning plan has archived. Nothing builds them into an image,
    invokes them from a Compose file, a workflow step or an ops route, and they
    are out of the coverage denominator. They still need lint and the unit
    suite -- spent is not untested -- but no Docker build, no dbt build and no
    database.

**The zones compose.** A changeset touching both takes the union of what each
needs rather than falling back to the full workflow, which is the whole point
of classifying by zone instead of by a single "is it all docs?" question.

Everything else is unclassified and takes the full workflow. That is the
fail-open direction and it is deliberate: a path this module has never heard of
must cost a full run, never a skipped one.

Output is one ``name=true|false`` line per job group, which is what
``ci.yml`` reads into ``$GITHUB_OUTPUT``.
"""

from __future__ import annotations

import sys

DOCS_PREFIX = b"docs/"
ONEOFF_PREFIXES = (b"scripts/oneoff/", b"tests/scripts/oneoff/")

# What a run needs when the classifier cannot say. Everything but the docs
# suite, which is only ever a substitute for the unit run.
FULL = {"docs_tests": False, "unit": True, "heavy": True}


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

    docs = sum(path.startswith(DOCS_PREFIX) for path in paths)
    oneoff = sum(path.startswith(ONEOFF_PREFIXES) for path in paths)
    if docs + oneoff != len(paths):
        return dict(FULL)

    return {
        "docs_tests": bool(docs) and not oneoff,
        "unit": bool(oneoff),
        "heavy": False,
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
