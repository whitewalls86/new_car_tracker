"""Unit tests for archiver/processors/pack_bronze_html.py (Plan 131 Stage 2).

An in-memory fake stands in for MinIO so a pack really is written, really is
read back through ranged GETs, and really is verified — the parts worth testing
are the ones a MagicMock would assert away. No MinIO, no DuckDB, no dictionary
registry: packs are built with ``allow_no_dictionary=True`` except in the test
that covers the refusal.
"""
from collections import Counter
from datetime import date, datetime, timezone

import pytest

from archiver.processors import pack_bronze_html as packer
from shared.compression import compress_frame, decompress_frame
from shared.packfile import read_index_parquet

_YEAR, _MONTH = 2026, 5
_TYPE = "detail_page"
_PREFIX = f"html/year={_YEAR}/month={_MONTH}/artifact_type={_TYPE}/"
_PACK_PREFIX = f"html_packs/{_TYPE}/{_YEAR:04d}/{_MONTH:02d}/"


# ---------------------------------------------------------------------------
# Fake object store
# ---------------------------------------------------------------------------

class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _Paginator:
    def __init__(self, store):
        self._store = store

    def paginate(self, Bucket=None, Prefix="", Delimiter=None):  # noqa: N803 - boto3 API
        keys = sorted(k for k in self._store.objects if k.startswith(Prefix))
        if Delimiter is None:
            yield {
                "Contents": [
                    {"Key": k, "Size": len(self._store.objects[k])} for k in keys
                ]
            }
            return
        contents, prefixes = [], set()
        for key in keys:
            tail = key[len(Prefix):]
            if Delimiter in tail:
                prefixes.add(Prefix + tail.split(Delimiter, 1)[0] + Delimiter)
            else:
                contents.append({"Key": key, "Size": len(self._store.objects[key])})
        yield {
            "Contents": contents,
            "CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)],
        }


class FakeS3:
    """Enough of the boto3 S3 client for the packer, including ranged GETs."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.raw: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.calls: Counter = Counter()

    def put_object(self, Bucket=None, Key=None, Body=None, **kwargs):  # noqa: N803
        self.calls["put_object"] += 1
        self.objects[Key] = bytes(Body)
        return {}

    def get_object(self, Bucket=None, Key=None, Range=None, **kwargs):  # noqa: N803
        self.calls["get_object"] += 1
        if Key.endswith(".idx.parquet"):
            self.calls["get_index"] += 1
        data = self.objects[Key]
        if Range:
            start, end = Range.replace("bytes=", "").split("-")
            data = data[int(start): int(end) + 1]
        return {"Body": _Body(data)}

    def delete_object(self, Bucket=None, Key=None, **kwargs):  # noqa: N803
        self.deleted.append(Key)
        return {}

    def get_paginator(self, name):
        return _Paginator(self)


def _html(i: int) -> bytes:
    return (
        f"<html><body><h1>vehicle {i}</h1>"
        + f"<p>{'spec ' * 80}</p>" * 3
        + f"<span>{i}</span></body></html>"
    ).encode("utf-8")


def _seed(store: FakeS3, n: int = 6, *, year: int = _YEAR, month: int = _MONTH) -> list[str]:
    keys = []
    for i in range(n):
        key = (
            f"html/year={year}/month={month}/artifact_type={_TYPE}/"
            f"uuid-{i:03d}.html.zst"
        )
        raw = _html(i)
        store.raw[key] = raw
        store.objects[key] = compress_frame(raw, level=1, dict_id=None)
        keys.append(key)
    return keys


def _metadata(keys, *, skip: set[int] = frozenset()) -> list[tuple]:
    """Silver-side rows, already in (listing_id, fetched_at) order.

    Two captures per listing, so ordering is observable in the sidecar.
    """
    rows = []
    for i, key in enumerate(keys):
        if i in skip:
            continue
        rows.append((
            f"s3://bronze/{key}",
            1000 + i,
            f"L{i // 2:03d}",
            datetime(_YEAR, _MONTH, 2 + i, 12, 0, tzinfo=timezone.utc),
        ))
    return rows


class _FakeDuckDB:
    def __init__(self, rows):
        self._rows = list(rows)
        self._pending: list[tuple] = []

    def execute(self, query, params=None):
        if "SELECT" in query:
            self._pending = list(self._rows)
        return self

    def fetchmany(self, size):
        batch, self._pending = self._pending[:size], self._pending[size:]
        return batch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(mocker):
    fake = FakeS3()
    mocker.patch.object(packer, "get_boto3_client", return_value=fake)
    mocker.patch(
        "shared.minio.read_html",
        side_effect=lambda path: decompress_frame(fake.objects[path.split("bronze/", 1)[1]]),
    )
    return fake


@pytest.fixture(autouse=True)
def _no_ambient_dictionary(monkeypatch):
    """A dictionary id leaking in from the developer's shell would send
    _resolve_dictionary to the real registry — and therefore to Postgres."""
    monkeypatch.delenv("PACK_BRONZE_DICT_ID", raising=False)
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)


@pytest.fixture
def duckdb(mocker):
    def _install(rows):
        mocker.patch(
            "shared.duckdb_s3.get_duckdb_s3_connection", return_value=_FakeDuckDB(rows)
        )
    return _install


def _run(**kwargs):
    params = {
        "apply": True,
        "artifact_type": _TYPE,
        "year": _YEAR,
        "month": _MONTH,
        "allow_no_dictionary": True,
        # The floor is exercised by its own tests; everywhere else it must not
        # depend on how full the machine running the suite happens to be.
        "min_free_bytes": 0,
    }
    params.update(kwargs)
    return packer.pack_bronze_html(**params)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def test_dry_run_writes_and_deletes_nothing(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))
    before = dict(store.objects)

    result = _run(apply=False)

    assert result["mode"] == "dry_run"
    assert result["packs_written"] == 0
    assert store.objects == before
    assert store.deleted == []
    assert result["buckets"][0]["pending"] == 6
    assert result["buckets"][0]["estimated_packs_upper_bound"] >= 1


def test_dry_run_below_the_free_space_floor_still_reports(store, duckdb, mocker):
    keys = _seed(store, 3)
    duckdb(_metadata(keys))
    mocker.patch.object(
        packer, "free_space",
        return_value={"path": "/", "free_bytes": 1024, "total_bytes": 10 * 1024 ** 3,
                      "free_inodes": 10},
    )

    result = _run(apply=False, min_free_bytes=5 * 1024 ** 3)

    assert result["error"] is None
    assert result["free_space"]["ok"] is False
    assert result["buckets"][0]["pending"] == 3


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def test_apply_writes_a_verified_pack_and_deletes_no_sources(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))

    result = _run()

    assert result["error"] is None
    assert result["packs_written"] == 1
    assert result["members_packed"] == 6
    assert result["members_verified"] == 6, "verification is 100% or it is a bug"
    assert store.deleted == []
    for key in keys:
        assert key in store.objects, "Stage 2 deletes no source object"

    pack = result["buckets"][0]["packs"][0]
    assert pack["pack_key"].startswith(_PACK_PREFIX)
    assert pack["index_key"] in store.objects


def test_packed_members_extract_to_the_original_html(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))

    result = _run()
    pack = result["buckets"][0]["packs"][0]

    from shared.packfile import PackReader

    reader = PackReader.from_bytes(store.objects[pack["pack_key"]])
    entries = read_index_parquet(store.objects[pack["index_key"]])
    for entry in entries:
        assert reader.read_member(entry) == store.raw[entry.source_key]


def test_members_are_ordered_by_listing_then_fetched_at(store, duckdb):
    keys = _seed(store, 6)
    rows = _metadata(keys)
    duckdb(rows)

    result = _run()
    entries = read_index_parquet(
        store.objects[result["buckets"][0]["packs"][0]["index_key"]]
    )

    assert [e.source_key for e in entries] == [
        row[0].split("bronze/", 1)[1] for row in rows
    ]
    assert [e.listing_id for e in entries] == sorted(e.listing_id for e in entries)


def test_objects_with_no_silver_row_are_still_packed(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys, skip={1, 4}))

    result = _run()
    entries = read_index_parquet(
        store.objects[result["buckets"][0]["packs"][0]["index_key"]]
    )

    assert len(entries) == 6
    unmatched = [e for e in entries if e.artifact_id is None]
    assert {e.source_key for e in unmatched} == {keys[1], keys[4]}
    # They sort last: an object silver cannot describe still has to be packed,
    # but it must not break the listing ordering of the ones it can.
    assert [e.source_key for e in entries[-2:]] == sorted({keys[1], keys[4]})


def test_a_pack_is_only_indexed_after_the_stored_copy_verifies(store, duckdb, mocker):
    keys = _seed(store, 4)
    duckdb(_metadata(keys))
    mocker.patch.object(
        packer, "_verify_stored_pack", side_effect=RuntimeError("member 3 differs")
    )

    result = _run()

    assert "member 3 differs" in result["error"]
    assert not [k for k in store.objects if k.endswith(".idx.parquet")]
    assert store.deleted == []


def test_a_failure_still_reports_the_packs_that_were_written(store, duckdb, mocker):
    """Packs written before the failure are finalized and will be skipped on
    resume, so a report that omits them describes a state that never existed."""
    keys = _seed(store, 6)
    duckdb(_metadata(keys))
    real = packer._verify_stored_pack
    calls = {"n": 0}

    def fail_on_third(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("stored pack differs")
        return real(*args, **kwargs)

    mocker.patch.object(packer, "_verify_stored_pack", side_effect=fail_on_third)

    result = _run(max_pack_bytes=1, frame_target_bytes=1, max_packs=0)

    assert "stored pack differs" in result["error"]
    assert result["packs_written"] == 2
    assert result["members_verified"] == 2
    assert len([k for k in store.objects if k.endswith(".idx.parquet")]) == 2


def test_read_failure_is_reported_without_ending_the_run(store, duckdb, mocker):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))
    real = decompress_frame

    def flaky(path):
        if path.endswith("uuid-002.html.zst"):
            raise RuntimeError("NoSuchKey")
        return real(store.objects[path.split("bronze/", 1)[1]])

    mocker.patch("shared.minio.read_html", side_effect=flaky)

    result = _run()

    assert result["read_failures"] == 1
    assert result["members_packed"] == 5
    assert keys[2] in store.objects, "an unreadable object stays exactly where it is"


# ---------------------------------------------------------------------------
# Checkpoint / resume
# ---------------------------------------------------------------------------

def test_resume_packs_each_artifact_exactly_once(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))

    first = _run()
    assert first["members_packed"] == 6

    second = _run()
    assert second["packs_written"] == 0
    assert second["members_packed"] == 0
    assert second["buckets"][0]["already_packed"] == 6
    assert second["buckets"][0]["pending"] == 0

    packed = [
        entry.source_key
        for key in store.objects
        if key.endswith(".idx.parquet")
        for entry in read_index_parquet(store.objects[key])
    ]
    assert sorted(packed) == sorted(keys)
    assert len(packed) == len(set(packed))


def test_resume_packs_only_the_new_objects(store, duckdb):
    keys = _seed(store, 4)
    duckdb(_metadata(keys))
    _run()

    more = [
        f"html/year={_YEAR}/month={_MONTH}/artifact_type={_TYPE}/uuid-{i:03d}.html.zst"
        for i in (90, 91)
    ]
    for i, key in enumerate(more):
        raw = _html(90 + i)
        store.raw[key] = raw
        store.objects[key] = compress_frame(raw, level=1, dict_id=None)
    duckdb(_metadata(more))

    second = _run()

    assert second["members_packed"] == 2
    entries = read_index_parquet(
        store.objects[second["buckets"][0]["packs"][0]["index_key"]]
    )
    assert sorted(e.source_key for e in entries) == sorted(more)


def test_checkpoint_state_does_not_grow_per_object(store, duckdb):
    """The sidecars are the checkpoint — there is no per-object state file.

    Plan 129 shipped an O(n^2) checkpoint that re-serialised its whole key set
    per object (commit f98e69b). The shape that cannot do that is one where a
    run writes a fixed number of objects per pack and reads one index per pack.
    """
    keys = _seed(store, 12)
    duckdb(_metadata(keys))

    result = _run(max_pack_bytes=1, frame_target_bytes=1, max_packs=0)
    packs = result["packs_written"]
    assert packs == 12, "one member per pack makes the per-pack cost visible"
    assert len(store.objects) == len(keys) + 2 * packs

    store.calls.clear()
    duckdb([])
    _run()
    assert store.calls["get_index"] == packs
    assert store.calls["put_object"] == 0


# ---------------------------------------------------------------------------
# Caps and floors
# ---------------------------------------------------------------------------

def test_max_packs_caps_the_run_and_the_rest_resumes(store, duckdb):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))

    first = _run(max_pack_bytes=1, frame_target_bytes=1, max_packs=2)
    assert first["packs_written"] == 2
    assert first["buckets"][0]["stopped_at_max_packs"] is True

    duckdb(_metadata(keys))
    second = _run(max_pack_bytes=1, frame_target_bytes=1, max_packs=0)
    assert second["members_packed"] == 4
    assert second["buckets"][0]["pending"] == 4


def test_the_pack_size_cut_is_on_stored_bytes_not_raw_bytes(store, duckdb):
    """max_pack_bytes bounds the transient free space a pack needs, which is a
    compressed quantity. Detail pages are ~158 KB raw against ~7.3 KB stored,
    so cutting on raw bytes would be ~20x conservative."""
    keys = _seed(store, 12)
    duckdb(_metadata(keys))
    raw_total = sum(len(store.raw[k]) for k in keys)

    result = _run(max_pack_bytes=raw_total // 2)

    assert result["packs_written"] == 1
    pack = result["buckets"][0]["packs"][0]
    assert pack["raw_bytes"] > result["buckets"][0]["packs"][0]["pack_bytes"]
    assert pack["pack_bytes"] < raw_total // 2


def test_a_pack_rolls_once_its_stored_bytes_reach_the_target(store, duckdb):
    keys = _seed(store, 12)
    duckdb(_metadata(keys))

    single = _run(max_pack_bytes=1 << 30)
    target = single["buckets"][0]["packs"][0]["pack_bytes"] // 3

    for key in [k for k in store.objects if k.startswith("html_packs/")]:
        del store.objects[key]
    duckdb(_metadata(keys))
    rolled = _run(max_pack_bytes=target, frame_target_bytes=1024, max_packs=0)

    assert rolled["packs_written"] > 1
    assert rolled["members_packed"] == 12


def test_apply_refuses_below_the_free_space_floor(store, duckdb, mocker):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))
    before = dict(store.objects)
    mocker.patch.object(
        packer, "free_space",
        return_value={"path": "/", "free_bytes": 3 * 1024 ** 3,
                      "total_bytes": 200 * 1024 ** 3, "free_inodes": 1000},
    )

    result = _run(min_free_bytes=5 * 1024 ** 3)

    assert "refusing to start" in result["error"]
    assert "3.00 GiB free" in result["error"]
    assert result["packs_written"] == 0
    assert store.objects == before


def test_apply_refuses_without_a_dictionary(store, duckdb, monkeypatch):
    keys = _seed(store, 6)
    duckdb(_metadata(keys))
    monkeypatch.delenv("PACK_BRONZE_DICT_ID", raising=False)
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    before = dict(store.objects)

    result = _run(allow_no_dictionary=False)

    assert "PACK_BRONZE_DICT_ID" in result["error"]
    assert store.objects == before


def test_configured_dictionary_is_resolved_before_any_object_is_read(
    store, duckdb, mocker, monkeypatch
):
    keys = _seed(store, 4)
    duckdb(_metadata(keys))
    monkeypatch.setenv("PACK_BRONZE_DICT_ID", "1367127621")
    resolved = mocker.patch(
        "shared.compression.get_dictionary",
        side_effect=RuntimeError("dictionary 1367127621 is not registered"),
    )

    result = _run(allow_no_dictionary=False)

    assert "not registered" in result["error"]
    assert resolved.call_args[0][0] == 1367127621
    assert store.calls["put_object"] == 0


# ---------------------------------------------------------------------------
# Bucket discovery
# ---------------------------------------------------------------------------

def test_discover_buckets_skips_the_month_still_open(store):
    _seed(store, 2, year=2026, month=4)
    _seed(store, 2, year=2026, month=8)

    eligible = packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=1, today=date(2026, 8, 13)
    )

    assert eligible == [(2026, 4)]


def test_last_month_is_eligible_the_day_after_it_closes(store):
    """Eligibility is month completion, not age. An age threshold would have
    held back ~40% of the corpus for no safety benefit — writing a pack is
    additive, and only Stage 4's delete needs a grace period."""
    _seed(store, 2, year=2026, month=7)

    assert packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=1, today=date(2026, 8, 1)
    ) == [(2026, 7)]


def test_a_month_is_not_eligible_before_its_settle_days_elapse(store):
    _seed(store, 2, year=2026, month=7)

    on_the_last_day = packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=1, today=date(2026, 7, 31)
    )
    with_a_longer_settle = packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=3, today=date(2026, 8, 2)
    )

    assert on_the_last_day == []
    assert with_a_longer_settle == []
    assert packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=3, today=date(2026, 8, 3)
    ) == [(2026, 7)]


def test_discover_buckets_ignores_other_artifact_types(store):
    store.objects["html/year=2026/month=4/artifact_type=results_page/a.html.zst"] = b"x"

    eligible = packer.discover_buckets(
        store, "bronze", _TYPE, settle_days=1, today=date(2026, 8, 13)
    )

    assert eligible == []


def test_no_eligible_bucket_is_not_an_error(store):
    result = packer.pack_bronze_html(
        apply=False, allow_no_dictionary=True, min_free_bytes=0
    )

    assert result["error"] is None
    assert result["buckets_eligible"] == 0
    assert result["packs_written"] == 0


# ---------------------------------------------------------------------------
# Orphan packs
# ---------------------------------------------------------------------------

def test_a_pack_without_a_sidecar_is_reported_and_left_alone(store, duckdb):
    keys = _seed(store, 4)
    duckdb(_metadata(keys))
    orphan = f"{_PACK_PREFIX}pack-00007.zpack"
    store.objects[orphan] = b"interrupted run"

    result = _run()

    assert result["buckets"][0]["orphan_packs"] == [orphan]
    assert store.objects[orphan] == b"interrupted run"
    assert store.deleted == []
    # Sequence numbering steps over it rather than overwriting it.
    assert result["buckets"][0]["packs"][0]["pack_key"].endswith("pack-00008.zpack")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_iter_ordered_keys_consumes_remaining_and_appends_the_leftovers():
    remaining = {"a", "b", "c"}
    metadata = iter([
        ("b", 1, "L1", None),
        ("z", 2, "L2", None),   # in silver, not in MinIO
        ("b", 3, "L3", None),   # duplicate row for an object already emitted
    ])

    out = list(packer.iter_ordered_keys(metadata, remaining))

    assert [key for key, *_ in out] == ["b", "a", "c"]
    assert [entry[1] for entry in out] == [1, None, None]
    assert remaining == set()


def test_listing_reports_progress(store, caplog):
    """A phase that owns most of a run's wall clock must not be silent.

    The first production dry-run listed ~1M objects with no output between
    "dictionary resolved" and the summary, and was indistinguishable from hung.
    """
    _seed(store, 12)

    with caplog.at_level("INFO", logger="archiver"):
        packer._list_objects(store, "bronze", _PREFIX, progress_every=5)

    progress = [r for r in caplog.records if "listing" in r.getMessage()]
    assert progress, "no progress lines emitted"
    assert "keys/s" in progress[0].getMessage()
    assert any("listed" in r.getMessage() for r in caplog.records)


def test_the_per_month_existence_probe_stays_silent(store, caplog):
    """discover_buckets probes every month with limit=1; that must not log."""
    _seed(store, 12)

    with caplog.at_level("INFO", logger="archiver"):
        found = packer._list_objects(
            store, "bronze", _PREFIX, limit=1, progress_every=0
        )

    assert len(found) == 1
    assert [r for r in caplog.records if "listing" in r.getMessage()] == []


def test_free_space_falls_back_when_the_configured_path_is_missing(caplog):
    """The default is container-shaped; a CLI run elsewhere must not die on it."""
    with caplog.at_level("WARNING", logger="archiver"):
        reading = packer.free_space("/no/such/path/for/packing")

    assert reading["path"] == "/"
    assert reading["free_bytes"] > 0
    assert any("does not exist" in r.getMessage() for r in caplog.records)


def test_free_space_status_measures_a_real_filesystem():
    status = packer.free_space_status(0)
    assert status["ok"] is True
    assert status["message"] is None
    assert status["free_bytes"] > 0
    assert status["total_bytes"] >= status["free_bytes"]

    breached = packer.free_space_status(1 << 62)
    assert breached["ok"] is False
    assert "refusing to start" in breached["message"]


# ---------------------------------------------------------------------------
# The ordering query, against a real DuckDB
# ---------------------------------------------------------------------------

def _write_parquet(path, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)


@pytest.fixture
def local_lake(tmp_path, monkeypatch):
    """Point the metadata query at local Parquet instead of s3://.

    The SQL is otherwise only ever exercised against production, which is the
    worst place to find out it does not parse.
    """
    events = tmp_path / "events.parquet"
    silver = tmp_path / "silver.parquet"
    monkeypatch.setattr(packer, "_ARTIFACT_EVENTS_PATH", str(events).replace("\\", "/"))
    monkeypatch.setattr(packer, "_SILVER_PATH", str(silver).replace("\\", "/"))
    return events, silver


def test_fetch_member_metadata_orders_by_listing_then_fetched_at(local_lake):
    import duckdb

    events, silver = local_lake
    keys = [f"{_PREFIX}uuid-{i:03d}.html.zst" for i in range(4)]
    _write_parquet(events, [
        {"artifact_id": 100 + i, "artifact_type": _TYPE,
         "minio_path": f"s3://bronze/{key}"}
        for i, key in enumerate(keys)
    ] + [
        # A different month, and a different artifact type: neither belongs to
        # this bucket and both must be filtered out by the prefix/type predicates.
        {"artifact_id": 900, "artifact_type": _TYPE,
         "minio_path": f"s3://bronze/html/year={_YEAR}/month=4/artifact_type={_TYPE}/x.html.zst"},
        {"artifact_id": 901, "artifact_type": "results_page",
         "minio_path": f"s3://bronze/{_PREFIX}srp.html.zst"},
    ])
    _write_parquet(silver, [
        {"artifact_id": 100, "listing_id": "L2",
         "fetched_at": datetime(_YEAR, _MONTH, 4, 9, tzinfo=timezone.utc)},
        {"artifact_id": 101, "listing_id": "L1",
         "fetched_at": datetime(_YEAR, _MONTH, 6, 9, tzinfo=timezone.utc)},
        {"artifact_id": 102, "listing_id": "L1",
         "fetched_at": datetime(_YEAR, _MONTH, 3, 9, tzinfo=timezone.utc)},
        # artifact 103 has no silver row at all.
    ])

    rows = list(
        packer.fetch_member_metadata(duckdb.connect(), "bronze", _TYPE, _YEAR, _MONTH)
    )

    assert [r[0] for r in rows] == [keys[2], keys[1], keys[0], keys[3]]
    assert [r[2] for r in rows] == ["L1", "L1", "L2", None]
    assert rows[-1][1] == 103, "an artifact silver never saw is still returned"


def test_fetch_member_metadata_deduplicates_repeated_event_rows(local_lake):
    import duckdb

    events, silver = local_lake
    key = f"{_PREFIX}uuid-000.html.zst"
    _write_parquet(events, [
        {"artifact_id": 100, "artifact_type": _TYPE, "minio_path": f"s3://bronze/{key}"}
        for _ in range(3)
    ])
    _write_parquet(silver, [
        {"artifact_id": 100, "listing_id": "L1",
         "fetched_at": datetime(_YEAR, _MONTH, 4, 9, tzinfo=timezone.utc)},
        {"artifact_id": 100, "listing_id": "L1",
         "fetched_at": datetime(_YEAR, _MONTH, 5, 9, tzinfo=timezone.utc)},
    ])

    rows = list(
        packer.fetch_member_metadata(duckdb.connect(), "bronze", _TYPE, _YEAR, _MONTH)
    )

    assert len(rows) == 1
    assert rows[0][0] == key


def test_pack_state_of_an_empty_bucket(store):
    packed, next_seq, orphans = packer._pack_state(store, "bronze", _TYPE, _YEAR, _MONTH)

    assert packed == set()
    assert next_seq == 0
    assert orphans == []
