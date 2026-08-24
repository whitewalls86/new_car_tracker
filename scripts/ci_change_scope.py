"""Classify a NUL-delimited git path list for the docs-only CI fast path."""

from __future__ import annotations

import sys

DOCS_PREFIX = b"docs/"


def docs_only_from_nul(data: bytes) -> bool:
    """Return true only for a non-empty, well-formed list wholly under docs/."""
    if not data:
        return False
    if not data.endswith(b"\0"):
        raise ValueError("changed-path input is not NUL terminated")

    paths = data[:-1].split(b"\0")
    if any(not path for path in paths):
        raise ValueError("changed-path input contains an empty path")
    return all(path.startswith(DOCS_PREFIX) for path in paths)


def main() -> int:
    try:
        result = docs_only_from_nul(sys.stdin.buffer.read())
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    print("true" if result else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
