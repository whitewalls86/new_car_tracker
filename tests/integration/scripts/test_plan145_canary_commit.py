"""
Plan 145 Stage 5 slice 3 Phase B -- the write canary, against real Postgres.

The unit tests show which statements the canary builds from the frozen Phase A
manifest. They cannot show that those statements *commit* -- that the manifest's
own rows survive every ``::uuid`` cast, ``NOT NULL`` and CHECK on the real
columns, that all four writes and the receipt land in one transaction, that a
re-run is a genuine no-op, or that the four protected hot tables do not move.
Those are this file's job.

The flush round trip is exercised here in its true shape: the staging rows are
written to the lake prefixes the two flushers write
(``archiver/processors/flush_silver_observations.py`` and
``flush_staging_events.py``) and then **DELETED from Postgres**, exactly as the
flushers do, and ``canary-flush-verify`` is then required to pass on lake
objects alone with staging empty. What is simulated is the flusher's transport,
not its contract: the rows come out of the real committed tables, the partition
layout is the flushers' own, and the deletion is real.

Every test cleans up by artifact id and batch name: the canary commits by
design, so a rollback fixture would prove nothing about it.
"""
import hashlib
import io
import json
import uuid
from datetime import datetime, timezone

import pytest

import scripts.reconcile_april_detail as mod
from scripts.reconcile_april_detail import (
    RECEIPT_TABLE,
    RECOVERED_STATUS,
    ReconcileError,
    allocate_artifact_ids,
    canary_batch_name,
)

pytestmark = pytest.mark.integration

CAPTURE_AT = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)

PROTECTED_TABLES = (
    "ops.artifacts_queue",
    "ops.price_observations",
    "ops.vin_to_listing",
    "ops.blocked_cooldown",
    "ops.detail_scrape_claims",
)


# -- a MinIO the canary can read, with real Postgres behind it -------------

class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _FakeS3:
    def __init__(self, store):
        self.store = store

    def list_objects_v2(self, **kw):
        prefix = kw.get("Prefix", "")
        keys = sorted(k for k in self.store if k.startswith(prefix))
        if kw.get("Delimiter"):
            commons = set()
            for key in keys:
                rest = key[len(prefix):]
                if kw["Delimiter"] in rest:
                    commons.add(prefix + rest.split(kw["Delimiter"], 1)[0]
                                + kw["Delimiter"])
            return {"CommonPrefixes": [{"Prefix": p} for p in sorted(commons)],
                    "IsTruncated": False}
        return {"Contents": [{"Key": k, "Size": len(self.store[k])} for k in keys],
                "IsTruncated": False}

    def get_object(self, Bucket, Key, Range=None):
        return {"Body": _Body(self.store[Key])}


def _write_parquet(schema, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(
        [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
    ), buf, compression="zstd")
    return buf.getvalue()


def _to_import_row(listing_id, object_key, *, source="detail",
                   listing_state="active", price=25000, vin=None):
    return {
        "listing_id": listing_id, "object_key": object_key, "source": source,
        "listing_state": listing_state, "fetched_at": CAPTURE_AT,
        "price": price, "make": "Honda", "model": "CR-V", "vin": vin,
        "canonical_detail_url": f"https://www.cars.com/vehicledetail/{listing_id}/",
        "reason": None,
    }


@pytest.fixture()
def canary_world(monkeypatch, writer_conn, vc):
    """A compare run, an assignment shard and a canary manifest on a fake MinIO,
    with real sequence-allocated artifact ids. Drops every row on teardown."""
    import pyarrow as pa

    import shared.minio as minio

    run_id = f"cmp-itest{uuid.uuid4().hex[:10]}"
    made = {"artifact_ids": [], "batches": [canary_batch_name(run_id)]}

    detail_key = f"html/2026/04/pack/{uuid.uuid4().hex}.html.zst"
    carousel_only_key = f"html/2026/04/pack/{uuid.uuid4().hex}.html.zst"
    unlisted_key = f"html/2026/04/pack/{uuid.uuid4().hex}.html.zst"
    keys = [detail_key, carousel_only_key, unlisted_key]

    with writer_conn.cursor() as cur:
        ids = allocate_artifact_ids(cur, 3)
    writer_conn.commit()
    artifact_ids = dict(zip(sorted(keys), ids))
    made["artifact_ids"].extend(ids)

    primary = {k: str(uuid.uuid4()) for k in keys}
    hint = str(uuid.uuid4())

    rows = [
        _to_import_row(primary[detail_key], detail_key),
        _to_import_row(hint, detail_key, source="carousel", price=17000),
        _to_import_row(primary[carousel_only_key], carousel_only_key,
                       source="carousel", price=19000),
        _to_import_row(primary[unlisted_key], unlisted_key,
                       listing_state="unlisted", price=None),
    ]
    per_object = {k: [r for r in rows if r["object_key"] == k] for k in keys}

    store = {}
    store[f"recovery/plan145/compared/{run_id}/to_import/unit-a.parquet"] = \
        _write_parquet(mod._compared_schema("to_import"), rows)
    store[f"recovery/plan145/compared/{run_id}/compare_report.json"] = \
        json.dumps({"blocked_excluded": {"rows": 0, "objects": 0}}).encode()
    store[f"recovery/plan145/inventory/{run_id}.json"] = b"{}"
    store[f"recovery/plan145/assigned/{run_id}-b00001.parquet"] = _write_parquet(
        mod._assigned_schema(),
        [{"batch_name": f"{run_id}-b00001", "run_id": run_id, "object_key": k,
          "artifact_id": artifact_ids[k],
          "id_source": "allocated_sequence",
          "listing_id": primary[k], "fetched_at": CAPTURE_AT,
          "input_kind": "unpacked", "source_unit": "unit-a",
          "silver_rows": len(per_object[k]),
          "detail_rows": sum(1 for r in per_object[k] if r["source"] == "detail"),
          "assigned_at": CAPTURE_AT}
         for k in sorted(keys)],
    )
    store[f"recovery/plan145/vin_snapshot/{run_id}.parquet"] = _write_parquet(
        pa.schema([pa.field("listing_id", pa.string()),
                   pa.field("vin", pa.string())]),
        [{"listing_id": hint, "vin": "VIN-SNAPSHOT-HINT"}],
    )

    monkeypatch.setattr(mod, "_s3_client", lambda: _FakeS3(store))
    monkeypatch.setattr(minio, "object_exists",
                        lambda k: k.split("bronze/")[-1] in store or k in store)
    monkeypatch.setattr(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)))
    monkeypatch.setattr(minio, "read_json", lambda p: (
        json.loads(store[p.split("bronze/")[-1]].decode())
        if p.split("bronze/")[-1] in store else None))

    # Phase A's real sampler picks the manifest Phase B commits.
    assert mod.run_canary_sample(mod.parse_args(
        ["canary-sample", "--run-id", run_id, "--apply"])) == 0

    manifest = store[f"recovery/plan145/canary/{run_id}-canary_sample.parquet"]
    yield {
        "run_id": run_id, "store": store, "keys": keys,
        "artifact_ids": artifact_ids, "primary": primary, "hint": hint,
        "batch_name": canary_batch_name(run_id),
        # The --apply pin: a commit names the manifest it was approved against.
        "pin": ["--expect-manifest-sha256", hashlib.sha256(manifest).hexdigest(),
                "--expect-rows", "4"],
    }

    vc.execute("DELETE FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (made["artifact_ids"],))
    vc.execute("DELETE FROM staging.price_observation_events "
               "WHERE artifact_id = ANY(%s)", (made["artifact_ids"],))
    vc.execute("DELETE FROM staging.artifacts_queue_events "
               "WHERE artifact_id = ANY(%s)", (made["artifact_ids"],))
    vc.execute(f"DELETE FROM {RECEIPT_TABLE} WHERE batch_name = ANY(%s)",
               (made["batches"],))


def _snapshot_protected(vc):
    out = {}
    for table in PROTECTED_TABLES:
        vc.execute(f"SELECT md5(t::text) AS h FROM {table} t ORDER BY 1")
        out[table] = [r["h"] for r in vc.fetchall()]
    return out


# -- the commit ------------------------------------------------------------

def test_the_canary_commits_exactly_the_manifests_rows_and_nothing_else(
        canary_world, vc):
    world = canary_world
    ids = sorted(world["artifact_ids"].values())

    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"])) == 0

    vc.execute("SELECT artifact_id, listing_id, source, listing_state, vin, "
               "fetched_at FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s) ORDER BY artifact_id, source", (ids,))
    silver = vc.fetchall()
    assert len(silver) == 4                       # the manifest's four rows
    assert {r["fetched_at"] for r in silver} == {CAPTURE_AT}
    assert sorted({r["artifact_id"] for r in silver}) == ids
    # the frozen VIN snapshot filled the carousel row Stage 4 left NULL
    assert next(r["vin"] for r in silver
                if r["listing_id"] == world["hint"]) == "VIN-SNAPSHOT-HINT"

    vc.execute("SELECT artifact_id, event_type, source, event_at "
               "FROM staging.price_observation_events "
               "WHERE artifact_id = ANY(%s)", (ids,))
    events = vc.fetchall()
    assert len(events) == 2                       # detail rows only
    assert {e["source"] for e in events} == {"detail"}
    assert {e["event_at"] for e in events} == {CAPTURE_AT}
    assert sorted(e["event_type"] for e in events) == ["deleted", "upserted"]

    vc.execute("SELECT artifact_id, status, run_id, fetched_at, event_at, "
               "artifact_type FROM staging.artifacts_queue_events "
               "WHERE artifact_id = ANY(%s)", (ids,))
    queue = vc.fetchall()
    assert len(queue) == 3                        # one per artifact
    assert {q["status"] for q in queue} == {RECOVERED_STATUS}
    assert {q["run_id"] for q in queue} == {world["batch_name"]}
    assert {q["fetched_at"] for q in queue} == {CAPTURE_AT}
    # event_at is the recovery action time, fetched_at the April capture
    assert all(q["event_at"] > CAPTURE_AT for q in queue)


def test_the_canary_leaves_one_receipt_naming_the_manifest_digest(canary_world,
                                                                  vc):
    world = canary_world
    digest = hashlib.sha256(world["store"][
        f"recovery/plan145/canary/{world['run_id']}-canary_sample.parquet"
    ]).hexdigest()

    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))

    vc.execute(f"SELECT * FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    receipts = vc.fetchall()
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["manifest_sha256"] == digest
    assert receipt["artifact_count"] == 3
    assert receipt["silver_count"] == 4
    assert receipt["price_event_count"] == 2
    assert receipt["queue_event_count"] == 3


def test_a_rerun_of_the_canary_writes_zero_rows(canary_world, vc):
    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))

    def _counts():
        out = []
        for table in ("staging.silver_observations",
                      "staging.price_observation_events",
                      "staging.artifacts_queue_events"):
            vc.execute(f"SELECT count(*) AS n FROM {table} "  # noqa: S608
                       "WHERE artifact_id = ANY(%s)", (ids,))
            out.append(vc.fetchone()["n"])
        return out

    before = _counts()
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"])) == 0
    assert _counts() == before
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} "
               "WHERE batch_name = %s", (world["batch_name"],))
    assert vc.fetchone()["n"] == 1


def test_the_migrated_manifest_commits_against_real_postgres(canary_world, vc):
    """The path the maintainer will actually run: the manifest on disk predates
    `write_set_digest`, so it is migrated -- original preserved -- and the
    commit goes against the migrated one."""
    import io

    import pyarrow.parquet as pq

    world = canary_world
    store = world["store"]
    ids = sorted(world["artifact_ids"].values())
    source_key = f"recovery/plan145/canary/{world['run_id']}-canary_sample.parquet"
    target_key = (f"recovery/plan145/canary/{world['run_id']}"
                  f"-canary_sample_digested.parquet")

    # age the manifest back to what is on disk today
    rows = pq.read_table(io.BytesIO(store[source_key])).to_pylist()
    for row in rows:
        for name in ("write_set_digest", "vin_snapshot_sha256", "page_fetched_at"):
            row[name] = None
    store[source_key] = _write_parquet(mod._canary_manifest_schema(), rows)
    v1_bytes = store[source_key]

    # a commit against it is refused, and names the migration
    with pytest.raises(ReconcileError, match="canary-remanifest --apply"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply"]
            + world["pin"]))

    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", world["run_id"], "--apply"])) == 0
    assert store[source_key] == v1_bytes          # the original is untouched

    migrated = pq.read_table(io.BytesIO(store[target_key])).to_pylist()
    assert sorted(r["object_key"] for r in migrated) == sorted(world["keys"])

    digest = hashlib.sha256(store[target_key]).hexdigest()
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply",
         "--expect-manifest-sha256", digest, "--expect-rows", "4"])) == 0

    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 4


def test_a_substituted_sibling_manifest_commits_nothing(canary_world, vc):
    """A digest-bearing sibling is accepted by resolution because it exists.
    The commit has to re-prove it against the frozen original, or an
    independently created one could hand it a different object set."""
    import io

    import pyarrow.parquet as pq

    world = canary_world
    store = world["store"]
    ids = sorted(world["artifact_ids"].values())
    source_key = f"recovery/plan145/canary/{world['run_id']}-canary_sample.parquet"
    target_key = (f"recovery/plan145/canary/{world['run_id']}"
                  f"-canary_sample_digested.parquet")

    rows = pq.read_table(io.BytesIO(store[source_key])).to_pylist()
    for row in rows:
        for name in ("write_set_digest", "vin_snapshot_sha256", "page_fetched_at"):
            row[name] = None
    store[source_key] = _write_parquet(mod._canary_manifest_schema(), rows)
    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", world["run_id"], "--apply"])) == 0

    # drop one artifact from the sibling: still self-consistent, still
    # digest-bearing, but no longer the frozen object set
    migrated = pq.read_table(io.BytesIO(store[target_key])).to_pylist()
    dropped = migrated[0]["object_key"]
    store[target_key] = _write_parquet(
        mod._canary_manifest_schema(),
        [r for r in migrated if r["object_key"] != dropped])

    with pytest.raises(ReconcileError, match="not a promotion of the frozen"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply",
             "--expect-manifest-sha256",
             hashlib.sha256(store[target_key]).hexdigest(),
             "--expect-rows", "3"]))

    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    assert vc.fetchone()["n"] == 0


def test_a_same_object_set_sibling_that_changed_a_field_commits_nothing(
        canary_world, vc):
    """Object-set equality is not enough: a sibling can keep every object key
    and still change what the frozen manifest recorded about them."""
    import io

    import pyarrow.parquet as pq

    world = canary_world
    store = world["store"]
    ids = sorted(world["artifact_ids"].values())
    source_key = f"recovery/plan145/canary/{world['run_id']}-canary_sample.parquet"
    target_key = (f"recovery/plan145/canary/{world['run_id']}"
                  f"-canary_sample_digested.parquet")

    rows = pq.read_table(io.BytesIO(store[source_key])).to_pylist()
    for row in rows:
        for name in ("write_set_digest", "vin_snapshot_sha256", "page_fetched_at"):
            row[name] = None
    store[source_key] = _write_parquet(mod._canary_manifest_schema(), rows)
    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", world["run_id"], "--apply"])) == 0

    # same object keys, one field changed
    migrated = pq.read_table(io.BytesIO(store[target_key])).to_pylist()
    migrated[0]["detail_rows"] = 99
    store[target_key] = _write_parquet(mod._canary_manifest_schema(), migrated)

    with pytest.raises(ReconcileError, match="were changed") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply",
             "--expect-manifest-sha256",
             hashlib.sha256(store[target_key]).hexdigest(),
             "--expect-rows", "4"]))
    assert "selects different artifacts" not in str(exc.value)

    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    assert vc.fetchone()["n"] == 0


def test_a_vin_snapshot_that_moved_after_sampling_commits_nothing(canary_world,
                                                                  vc):
    """The VIN snapshot is an input to the write set, not just to the
    comparison: it fills the carousel VIN that reaches Postgres."""
    import pyarrow as pa

    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    world["store"][f"recovery/plan145/vin_snapshot/{world['run_id']}.parquet"] = \
        _write_parquet(
            pa.schema([pa.field("listing_id", pa.string()),
                       pa.field("vin", pa.string())]),
            [{"listing_id": world["hint"], "vin": "VIN-SOMETHING-ELSE"}])

    with pytest.raises(ReconcileError, match="VIN snapshot"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply"]
            + world["pin"]))
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    assert vc.fetchone()["n"] == 0


def test_the_commit_time_comes_from_the_receipt_not_the_process_clock(
        canary_world, vc):
    """V047 sets `committed_at` with now() inside the writing transaction. The
    writer reads it back with RETURNING rather than stamping its own clock, so
    the recorded time is the one the database wrote."""
    world = canary_world
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))

    vc.execute(f"SELECT committed_at FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    from_db = vc.fetchone()["committed_at"]
    report = json.loads(world["store"][
        f"recovery/plan145/canary/{world['run_id']}-canary_commit.json"])
    assert report["committed_at_source"] == "receipt"
    assert report["committed_at"] == from_db.isoformat()


def test_a_lost_report_is_repaired_from_the_receipt_after_the_clock_moves(
        canary_world, vc, monkeypatch):
    """The failure this closes: the transaction commits, the MinIO report write
    fails, and the rerun invents a fresh commit time. canary-flush-verify uses
    that time as its LastModified bound and to pick the queue-event partition,
    so across a month boundary it would look in the wrong place."""
    import shared.minio as minio

    world = canary_world
    commit_key = (f"recovery/plan145/canary/{world['run_id']}-canary_commit.json")
    good_write = minio.write_bytes

    def _boom(key, data, content_type=None):
        if key == commit_key:
            raise RuntimeError("MinIO write failed after the commit")
        return good_write(key, data, content_type)

    monkeypatch.setattr(minio, "write_bytes", _boom)
    with pytest.raises(RuntimeError, match="after the commit"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply"]
            + world["pin"]))

    # the rows and the receipt are durable; the evidence object is not
    vc.execute(f"SELECT committed_at FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    true_commit = vc.fetchone()["committed_at"]
    assert commit_key not in world["store"]

    # the repair run, later by the wall clock, skips on the receipt and still
    # records when the batch actually landed
    monkeypatch.setattr(minio, "write_bytes", good_write)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"])) == 0
    report = json.loads(world["store"][commit_key])
    assert report["skipped_on_receipt"] is True
    assert report["committed_at_source"] == "receipt"
    assert report["committed_at"] == true_commit.isoformat()
    assert report["receipt_row"]["silver_count"] == 4


def test_the_canary_moves_no_protected_table(canary_world, vc):
    world = canary_world
    before = _snapshot_protected(vc)
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))
    assert _snapshot_protected(vc) == before


def test_the_canary_row_budget_refuses_before_any_row_is_committed(
        canary_world, vc, monkeypatch):
    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    monkeypatch.setattr(mod, "CANARY_ROW_BUDGET", 2)
    with pytest.raises(ReconcileError, match="over the fixed 2-row canary budget"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", world["run_id"], "--apply"]
            + world["pin"]))
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    assert vc.fetchone()["n"] == 0


def test_the_canary_dry_run_commits_nothing(canary_world, vc):
    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"]])) == 0
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0


# -- the flush round trip --------------------------------------------------

def _flush_like_the_flushers(world, vc, *, tables=("silver", "price", "queue")):
    """Write the committed staging rows to the lake prefixes the flushers use,
    then DELETE them from Postgres -- the flushers' own two steps."""
    import pyarrow as pa

    store = world["store"]
    ids = sorted(world["artifact_ids"].values())
    part = uuid.uuid4().hex[:8]

    if "silver" in tables:
        vc.execute("SELECT artifact_id, listing_id, source, fetched_at "
                   "FROM staging.silver_observations "
                   "WHERE artifact_id = ANY(%s)", (ids,))
        rows = [dict(r) for r in vc.fetchall()]
        schema = pa.schema([pa.field("artifact_id", pa.int64()),
                            pa.field("listing_id", pa.string()),
                            pa.field("fetched_at", pa.timestamp("us", tz="UTC"))])
        for source in {r["source"] for r in rows}:
            here = [r for r in rows if r["source"] == source]
            key = (f"silver_normalized/observations/source={source}/"
                   f"obs_year={CAPTURE_AT.year}/obs_month={CAPTURE_AT.month}/"
                   f"part-{part}-0.parquet")
            store[key] = _write_parquet(schema, [
                {**r, "listing_id": str(r["listing_id"])} for r in here])
        vc.execute("DELETE FROM staging.silver_observations "
                   "WHERE artifact_id = ANY(%s)", (ids,))

    if "price" in tables:
        vc.execute("SELECT artifact_id, listing_id, event_type, event_at "
                   "FROM staging.price_observation_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))
        rows = [dict(r) for r in vc.fetchall()]
        if rows:
            schema = pa.schema([
                pa.field("artifact_id", pa.int64()),
                pa.field("listing_id", pa.string()),
                pa.field("event_type", pa.string()),
                pa.field("event_at", pa.timestamp("us", tz="UTC"))])
            key = (f"ops_normalized/price_observation_events/"
                   f"year={CAPTURE_AT.year}/month={CAPTURE_AT.month}/"
                   f"part-{part}-0.parquet")
            store[key] = _write_parquet(schema, [
                {**r, "listing_id": str(r["listing_id"])} for r in rows])
        vc.execute("DELETE FROM staging.price_observation_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))

    if "queue" in tables:
        vc.execute("SELECT artifact_id, status, run_id, fetched_at, event_at "
                   "FROM staging.artifacts_queue_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))
        rows = [dict(r) for r in vc.fetchall()]
        if rows:
            schema = pa.schema([
                pa.field("artifact_id", pa.int64()),
                pa.field("status", pa.string()),
                pa.field("run_id", pa.string()),
                pa.field("fetched_at", pa.timestamp("us", tz="UTC"))])
            at = rows[0]["event_at"]
            key = (f"ops_normalized/artifacts_queue_events/year={at.year}/"
                   f"month={at.month}/part-{part}-0.parquet")
            store[key] = _write_parquet(schema, rows)
        vc.execute("DELETE FROM staging.artifacts_queue_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))


def test_the_round_trip_passes_on_lake_objects_with_staging_emptied(
        canary_world, vc):
    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))

    # Before the flush the rows are only in Postgres, and the check says so.
    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", world["run_id"]])) == 1

    _flush_like_the_flushers(world, vc)

    for table in ("staging.silver_observations",
                  "staging.price_observation_events",
                  "staging.artifacts_queue_events"):
        vc.execute(f"SELECT count(*) AS n FROM {table} "  # noqa: S608
                   "WHERE artifact_id = ANY(%s)", (ids,))
        assert vc.fetchone()["n"] == 0        # staging is gone, as it will be

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", world["run_id"], "--apply"])) == 0
    report = json.loads(world["store"][
        f"recovery/plan145/canary/{world['run_id']}-canary_flush_report.json"])
    assert report["passed"] is True
    assert report["tables"]["staging.silver_observations"]["found_rows"] == 4
    assert report["tables"]["staging.price_observation_events"]["found_rows"] == 2
    assert report["tables"]["staging.artifacts_queue_events"]["found_rows"] == 3
    assert all(t["lake_keys"] for t in report["tables"].values())


def test_the_receipt_outlives_the_flush_that_deletes_the_rows(canary_world, vc):
    """The receipt is the only thing that can answer "did this commit?" once
    the flush has deleted the rows -- and a re-run must still be a no-op."""
    world = canary_world
    ids = sorted(world["artifact_ids"].values())
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))
    _flush_like_the_flushers(world, vc)

    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (world["batch_name"],))
    assert vc.fetchone()["n"] == 1

    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"])) == 0
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = ANY(%s)", (ids,))
    assert vc.fetchone()["n"] == 0        # not rewritten into the emptied table


def test_a_half_flushed_canary_fails_the_round_trip(canary_world, vc):
    world = canary_world
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", world["run_id"], "--apply"]
        + world["pin"]))
    _flush_like_the_flushers(world, vc, tables=("silver", "price"))

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", world["run_id"], "--apply"])) == 1
    report = json.loads(world["store"][
        f"recovery/plan145/canary/{world['run_id']}-canary_flush_report.json"])
    assert report["tables"]["staging.silver_observations"]["passed"] is True
    assert report["tables"]["staging.artifacts_queue_events"]["missing_keys"] == 3
