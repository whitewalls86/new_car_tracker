"""The writer, the reader and the fixtures all answer to one record.

Plan 162 Stage AA, gap G31.

``contracts/lake_snapshot_manifest/export<N>-archive<N>.json`` is the only
statement of what a CI snapshot manifest contains. ``archiver`` writes the
document, ``ops`` serves it back, downloaders read it years later, and before
this registry the shape existed in three places -- a writer, a pydantic model
in a different service, and a dict typed out in each test file -- with nothing
comparing them. Every one of those could move on its own, and two of them had.

The rules here are what make the record load-bearing rather than decorative:

* the writer's output matches the record for the version it stamps,
* ``ops`` has a model for **every** recorded version, not just the newest,
* and no recorded version is dropped, because the archives it describes are
  still in the bucket and someone is still pulling them.

That middle rule is the one a pinned ML rehearsal depends on. A format bump
that quietly removed v1's model would leave every v1 snapshot answered 409 --
or worse, served through v2's model, which drops whatever v1 had and invents
whatever v2 added.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from ops.api_models import ARCHIVE_MANIFEST_MODELS, readable_manifest_versions
from scripts.generate_lake_snapshot_manifest_contract import (
    REGISTRY_DIR,
    current_record,
    load_records,
    manifest_fixture,
)
from shared.lake_snapshot_schema import (
    ARCHIVE_CACHE_SCHEMA_VERSION,
    EXPORT_CACHE_SCHEMA_VERSION,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _shape_keys(shape: dict, prefix: str = "") -> set[str]:
    """Every key path in a recorded shape, so nesting is compared too."""
    keys: set[str] = set()
    for key, value in shape.items():
        keys.add(f"{prefix}{key}")
        if isinstance(value, dict):
            keys |= _shape_keys(value, f"{prefix}{key}.")
    return keys


def test_the_registry_is_not_empty():
    """A floor, for the reason every derived rule in this repository has one.

    These rules read a directory. If it is renamed or emptied, every assertion
    below loops over nothing and passes, and the shape of the document two
    services exchange would be unchecked while the suite stayed green.
    """
    records = load_records()
    assert records, (
        f"{REGISTRY_DIR.relative_to(REPO_ROOT).as_posix()} holds no manifest "
        f"records. Either the directory moved and these rules are reading the "
        f"wrong place, or the last record was deleted -- and a record is never "
        f"deleted, because the archives it describes outlive the writer."
    )


def test_the_writer_matches_the_record_for_the_version_it_stamps():
    """Generated rather than asserted, so the record cannot drift from the code.

    ``--check`` calls ``archiver``'s two manifest builders and compares the
    result to the committed record. This runs it in-process; CI runs the script
    itself, because a generated artifact nobody regenerates is a document again.
    """
    records = load_records()
    stamped = (EXPORT_CACHE_SCHEMA_VERSION, ARCHIVE_CACHE_SCHEMA_VERSION)

    assert stamped in records, (
        f"archiver stamps {stamped} and no record describes it. Run "
        f"`python scripts/generate_lake_snapshot_manifest_contract.py`."
    )
    assert records[stamped]["shape"] == current_record()["shape"], (
        f"the record for {stamped} no longer matches what archiver's writers "
        f"produce. Regenerate it, and look at what moved: a key added or "
        f"removed is a format change, and every reader of this version has to "
        f"be told."
    )


@pytest.mark.parametrize("pair", sorted(load_records()))
def test_ops_has_a_model_for_every_recorded_version(pair):
    """A recorded format that ``ops`` cannot serve is a snapshot nobody can pull.

    Parametrised over the registry rather than over a list, so a new record
    fails here until its model exists -- which is what stops a format bump
    being done by widening a set of version numbers, the mistake this stage
    made and measured before this rule existed.
    """
    assert pair in readable_manifest_versions(), (
        f"contracts/lake_snapshot_manifest/export{pair[0]}-archive{pair[1]}.json "
        f"describes a format ops cannot serve; it serves "
        f"{sorted(readable_manifest_versions())}. Every snapshot written in "
        f"that format would be refused with 409. Add a model to "
        f"ARCHIVE_MANIFEST_MODELS whose version literals are {pair}."
    )


@pytest.mark.parametrize("pair", sorted(load_records()))
def test_every_ops_model_declares_exactly_the_recorded_fields(pair):
    """The reader is measured against the record, not against the writer.

    Field for field, including nesting: ``archive`` is a block of four and a
    model short of one of them deletes it from the response. The comparison is
    two-directional because the failure is: a model short of the record drops
    keys in production, and a model longer than it invents nulls the writer
    never wrote.
    """
    record = load_records()[pair]
    model = next(
        m for m in ARCHIVE_MANIFEST_MODELS
        if (
            m.model_fields["export_cache_schema_version"].annotation.__args__[0],
            m.model_fields["archive_cache_schema_version"].annotation.__args__[0],
        ) == pair
    )

    recorded = _shape_keys(record["shape"])
    declared: set[str] = set()
    for name, field in model.model_fields.items():
        declared.add(name)
        nested = field.annotation
        for candidate in (getattr(nested, "__args__", None) or (nested,)):
            if hasattr(candidate, "model_fields"):
                declared |= {f"{name}.{sub}" for sub in candidate.model_fields}

    assert declared == recorded, (
        f"{model.__name__} does not match the record for {pair}.\n"
        f"  in the record, not in the model (dropped in production): "
        f"{sorted(recorded - declared)}\n"
        f"  in the model, not in the record (invented as null): "
        f"{sorted(declared - recorded)}"
    )


def test_the_fixture_builder_produces_exactly_the_recorded_shape():
    """Every test manifest in this repository comes from here.

    Asserted because the builder is what replaced the hand-written fixtures,
    and a builder that quietly returned a subset would put the old defect back
    with a derivation's reputation.
    """
    for pair, record in sorted(load_records().items()):
        built = manifest_fixture(pair)
        assert set(built) == set(record["shape"]), (
            f"manifest_fixture({pair}) does not build the recorded shape: "
            f"missing {sorted(set(record['shape']) - set(built))}, "
            f"extra {sorted(set(built) - set(record['shape']))}"
        )
        assert built["export_cache_schema_version"] == pair[0]
        assert built["archive_cache_schema_version"] == pair[1]


def test_the_generator_check_passes_as_a_subprocess():
    """CI runs the script, so the script's own exit code is what must be green.

    In-process assertions above share this interpreter's imports; a script that
    only works when something else has already imported ``archiver`` would pass
    them and fail the job.
    """
    result = subprocess.run(
        [sys.executable, "scripts/generate_lake_snapshot_manifest_contract.py", "--check"],
        cwd=REPO_ROOT, capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 0, (
        f"generate_lake_snapshot_manifest_contract.py --check exited "
        f"{result.returncode}:\n{result.stdout}\n{result.stderr}"
    )


def test_every_record_is_valid_json_and_names_its_own_version():
    """A retired record is a committed fact and cannot be regenerated.

    Once the writer moves on, nothing can reproduce an old shape -- the code
    that made it is gone and the archives cannot be re-read to recover it. So
    the only thing standing between a retired record and a silent rewrite is
    that its filename and its contents have to agree, and ``load_records``
    raises when they do not. This calls it for that reason.
    """
    records = load_records()
    for path in sorted(REGISTRY_DIR.glob("*.json")):
        json.loads(path.read_text(encoding="utf-8"))
    assert len(records) == len(list(REGISTRY_DIR.glob("*.json")))
