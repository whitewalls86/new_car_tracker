"""Record the shape of the CI snapshot manifest, one file per format version.

Plan 162 Stage AA, gap G32.

**The manifest is written by one service and served by another, and neither
could say what it contains.** ``archiver`` composes it, ``ops`` hands it back
over HTTP, and a downloader reads it months or years later. Before this
registry the only statement of its shape was a pydantic model inside ``ops``,
which is a claim one service makes about another service's output, checked by
nothing.

**A version is not superseded when the next one arrives.** An ML rehearsal
pinned to a v1 snapshot and a CI run pulling v2 are both legitimate, at the
same time, indefinitely. So the registry keeps one record per format version
and never removes one: ``contracts/lake_snapshot_manifest/export3-archive1.json``
stays readable after the writer has moved to export 4, because the archives it
describes are still in the bucket.

**Two versions in the filename because the document is two formats layered.**
``build_export_manifest`` writes one half and ``build_archive_manifest`` copies
it and adds the other, so the shape is the *pair* and either half can move on
its own. Keying on one of them would name a shape that is not determined.

**Generated, not written.** The record for the version the writer currently
stamps is produced by calling the writers -- both are pure functions over their
arguments, so this needs no MinIO, no database and no fixture. That is what
makes the record unable to drift from the code: change the writer without
regenerating and the CI diff goes red, which is exactly the mechanism
``scripts/generate_service_contracts.py`` established in Stage Z.

**Retired versions are frozen, and this script will not touch them.** Once the
writer moves on, the code that produced the old shape is gone and the old
record can no longer be generated from anything. It becomes a committed fact.
``--check`` therefore compares only the current version's record and asserts
that every other record still parses and still declares the pair its filename
claims -- because a retired record edited by hand silently redefines what that
version meant, and the archives it describes cannot be re-read to find out.

**Names and nesting, not types.** The writers take their values from callers,
so recording types here would record this script's placeholder arguments rather
than anything the writer guarantees. Keys are also the whole defect: a reader
short of a key deletes it and a reader long invents it, and neither shows up in
a type. The independent check on the *values* is a real manifest, which is a
different mechanism living in the integration layer.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
# Invoked directly as often as it is imported, and `archiver` is a package
# at the root rather than beside this file. Same reason
# `scripts/download_lake_snapshot.py` does it.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
REGISTRY_DIR = REPO_ROOT / "contracts" / "lake_snapshot_manifest"
FILENAME = re.compile(r"^export(\d+)-archive(\d+)\.json$")

# Sentinels. Every one is a value the writer copies through untouched, so the
# only thing they can influence is which keys appear -- which is the point.
_SENTINEL = "<value>"


def _shape(value: Any) -> Any:
    """The key structure of *value*, with leaf values discarded.

    A list records the shape of its first element, or is empty. Nothing here
    depends on how many elements a real manifest carries.
    """
    if isinstance(value, dict):
        return {key: _shape(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_shape(value[0])] if value else []
    return _SENTINEL


def current_record() -> dict[str, Any]:
    """The record for the version the writers stamp today, from the writers."""
    from archiver.processors.lake_snapshot_archive import build_archive_manifest
    from archiver.processors.lake_snapshot_export_cache import build_export_manifest

    export_manifest = build_export_manifest(
        fingerprint=_SENTINEL,
        planning_fingerprint=_SENTINEL,
        export_fingerprint_payload={},
        snapshot_id=_SENTINEL,
        tier=_SENTINEL,
        source_window={},
        counts={},
        coverage={},
        tables={},
        postgres_tables={},
        data_path=_SENTINEL,
        generation_id=_SENTINEL,
    )
    manifest = build_archive_manifest(
        export_manifest,
        archive_key=_SENTINEL,
        archive_bytes=0,
        archive_sha256=_SENTINEL,
        file_count=0,
    )
    return {
        "export_cache_schema_version": manifest["export_cache_schema_version"],
        "archive_cache_schema_version": manifest["archive_cache_schema_version"],
        "generated_by": "scripts/generate_lake_snapshot_manifest_contract.py",
        "shape": _shape(manifest),
    }


def record_path(export_version: int, archive_version: int) -> Path:
    return REGISTRY_DIR / f"export{export_version}-archive{archive_version}.json"


def _serialise(record: dict[str, Any]) -> str:
    return json.dumps(record, indent=2, sort_keys=True) + "\n"


def load_records() -> dict[tuple[int, int], dict[str, Any]]:
    """Every recorded format version, keyed by its ``(export, archive)`` pair."""
    records: dict[tuple[int, int], dict[str, Any]] = {}
    for path in sorted(REGISTRY_DIR.glob("*.json")):
        match = FILENAME.match(path.name)
        if not match:
            raise ValueError(
                f"{path.name} is not a manifest record. Records are named "
                f"export<N>-archive<N>.json; the pair is in the name so a "
                f"reader can find a version without opening every file."
            )
        record = json.loads(path.read_text(encoding="utf-8"))
        records[(int(match.group(1)), int(match.group(2)))] = record
    return records


def record_inconsistencies() -> list[str]:
    """Records whose filename and contents disagree about which format they are.

    Reported rather than raised, and that is not a style preference. This is
    called from a module-level ``parametrize``, so raising here would make one
    bad record a *collection* error -- every rule in that module would stop
    running, and a suite that cannot collect a rule is indistinguishable from
    one that has no rule. The harness saw exactly that: pytest exited 4, no
    assertion ran, and the mutation went unnoticed.
    """
    problems: list[str] = []
    for pair, record in sorted(load_records().items()):
        declared = (
            record.get("export_cache_schema_version"),
            record.get("archive_cache_schema_version"),
        )
        if declared != pair:
            problems.append(
                f"export{pair[0]}-archive{pair[1]}.json declares {declared} and "
                f"its name says {pair}. One of them is a lie and nothing here "
                f"can tell which: the writer for that format is gone and the "
                f"archives it describes cannot be re-read to settle it."
            )
    return problems


def manifest_fixture(
    pair: tuple[int, int] | None = None, **overrides: Any
) -> dict[str, Any]:
    """A complete manifest of a recorded format, built from that record.

    The one way a test should obtain a manifest. Every fixture in this
    repository that held one used to be a dict somebody typed out from reading
    the writer, which is the drift this plan exists to remove: the writer gains
    a key, the fixture does not, and the test goes on passing while production
    silently loses it.

    Built from the record rather than from the writer so it works for a retired
    format whose writing code is gone -- which is the case a pinned ML
    rehearsal depends on.
    """
    records = load_records()
    if pair is None:
        pair = max(records)
    if pair not in records:
        raise KeyError(
            f"no manifest record for {pair}. Recorded formats: "
            f"{sorted(records)}. A test asking for a format nothing describes "
            f"is asking for a document that was never written."
        )

    def build(shape: Any) -> Any:
        if isinstance(shape, dict):
            return {key: build(value) for key, value in shape.items()}
        if isinstance(shape, list):
            return [build(item) for item in shape]
        return _SENTINEL

    manifest = build(records[pair]["shape"])
    manifest["export_cache_schema_version"] = pair[0]
    manifest["archive_cache_schema_version"] = pair[1]
    manifest.update(overrides)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true",
        help="Verify the current version's record matches the writers, and "
             "that every retired record still parses. Writes nothing.",
    )
    args = parser.parse_args(argv)

    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    record = current_record()
    pair = (
        record["export_cache_schema_version"],
        record["archive_cache_schema_version"],
    )
    path = record_path(*pair)

    if not args.check:
        path.write_text(_serialise(record), encoding="utf-8")
        print(f"wrote {path.relative_to(REPO_ROOT).as_posix()}")
        return 0

    inconsistent = record_inconsistencies()
    if inconsistent:
        for problem in inconsistent:
            print(problem, file=sys.stderr)
        return 1

    records = load_records()
    if pair not in records:
        print(
            f"the writers stamp {pair} and no record describes it. Run this "
            f"script without --check to add "
            f"{path.relative_to(REPO_ROOT).as_posix()}.",
            file=sys.stderr,
        )
        return 1
    if _serialise(records[pair]) != _serialise(record):
        print(
            f"{path.relative_to(REPO_ROOT).as_posix()} no longer matches what "
            f"the writers produce. Regenerate it, and check what moved: a key "
            f"added or removed here is a format change, and every reader of "
            f"this version has to be told about it.",
            file=sys.stderr,
        )
        return 1

    retired = sorted(set(records) - {pair})
    print(f"current {pair} matches the writers; {len(retired)} retired: {retired}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
