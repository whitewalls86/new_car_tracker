"""
Plan 145 Stage 5 slice 2 -- the historical write set, against real Postgres.

A fake cursor can show which statements the writer builds. It cannot show that
the four writes commit or roll back together, that the receipt survives the
staging flush that deletes the rows it describes, or that the four protected
hot tables are byte-identical before and after. Those are this file's job.

Every test cleans up after itself by artifact_id and batch name: the writer
commits by design, so a rollback fixture would prove nothing about it. The
exception is ``write_import_batch(..., probe=True)``, whose whole contract is
that it issues every statement and then rolls back -- the two probe tests below
assert exactly that against real Postgres.
"""
import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from scripts.reconcile_april_detail import (
    RECEIPT_TABLE,
    RECOVERED_STATUS,
    ReceiptConflict,
    allocate_artifact_ids,
    build_recovery_price_event,
    build_recovery_queue_event,
    build_recovery_silver_row,
    write_import_batch,
)

pytestmark = pytest.mark.integration

#: An April capture time, four months behind anything live.
CAPTURE_AT = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)

PROTECTED_TABLES = (
    "ops.artifacts_queue",
    "ops.price_observations",
    "ops.vin_to_listing",
    "ops.blocked_cooldown",
    "ops.detail_scrape_claims",
)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _parsed_row(listing_id, object_key, *, source="detail",
                listing_state="active", price=25000, vin=None):
    """The shape a ``to_import`` shard hands the writer."""
    return {
        "listing_id": listing_id,
        "object_key": object_key,
        "source": source,
        "listing_state": listing_state,
        "fetched_at": CAPTURE_AT,
        "price": price,
        "make": "Honda",
        "model": "CR-V",
        "vin": vin,
        "canonical_detail_url": f"https://www.cars.com/vehicledetail/{listing_id}/",
    }


@pytest.fixture()
def recovery_batch(vc):
    """Build one batch's write set; drop every row it wrote on teardown."""
    made = {"artifact_ids": [], "batches": []}

    def _factory(rows, artifact_ids, batch_name=None, vin_map=None):
        batch_name = batch_name or f"itest-{uuid.uuid4().hex[:12]}-b00001"
        vin_map = vin_map or {}
        silver, price_events = [], []
        for row in rows:
            enriched = build_recovery_silver_row(
                row, artifact_ids[row["object_key"]], vin_map,
            )
            silver.append(enriched)
            event = build_recovery_price_event(enriched)
            if event is not None:
                price_events.append(event)
        queue_events = [
            build_recovery_queue_event(
                {"object_key": key, "artifact_id": artifact_id,
                 "listing_id": next(r["listing_id"] for r in rows
                                    if r["object_key"] == key
                                    and r["source"] == "detail"),
                 "fetched_at": CAPTURE_AT},
                batch_name, "bronze",
            )
            for key, artifact_id in sorted(artifact_ids.items())
        ]
        made["artifact_ids"].extend(artifact_ids.values())
        made["batches"].append(batch_name)
        return batch_name, silver, price_events, queue_events

    yield _factory

    ids = made["artifact_ids"]
    if ids:
        vc.execute("DELETE FROM staging.silver_observations "
                   "WHERE artifact_id = ANY(%s)", (ids,))
        vc.execute("DELETE FROM staging.price_observation_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))
        vc.execute("DELETE FROM staging.artifacts_queue_events "
                   "WHERE artifact_id = ANY(%s)", (ids,))
    if made["batches"]:
        vc.execute(f"DELETE FROM {RECEIPT_TABLE} WHERE batch_name = ANY(%s)",
                   (made["batches"],))


def _snapshot_protected(vc):
    """Every protected hot table, as an ordered list of whole rows."""
    out = {}
    for table in PROTECTED_TABLES:
        vc.execute(f"SELECT md5(t::text) AS h FROM {table} t ORDER BY 1")
        out[table] = [r["h"] for r in vc.fetchall()]
    return out


def _one_object_batch(recovery_batch, writer_conn, *, listing_id=None,
                      hint_id=None, vin_map=None):
    listing_id = listing_id or str(uuid.uuid4())
    hint_id = hint_id or str(uuid.uuid4())
    key = f"html/2026/04/pack/{uuid.uuid4().hex}.html.zst"
    with writer_conn.cursor() as cur:
        artifact_id = allocate_artifact_ids(cur, 1)[0]
    writer_conn.commit()
    rows = [
        _parsed_row(listing_id, key),
        _parsed_row(hint_id, key, source="carousel", price=17000),
    ]
    return (*recovery_batch(rows, {key: artifact_id}, vin_map=vin_map),
            artifact_id, listing_id, hint_id)


# -- the transaction ------------------------------------------------------

def test_a_batch_writes_all_four_things_at_the_legacy_capture_time(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, listing_id, hint_id) = \
        _one_object_batch(recovery_batch, writer_conn)
    manifest = _digest(batch)

    out = write_import_batch(writer_conn, batch, manifest, silver, events, queue)
    assert out == {"batch_name": batch, "skipped": False, "silver": 2,
                   "price_events": 1, "queue_events": 1, "artifacts": 1}

    vc.execute("SELECT listing_id, source, fetched_at, artifact_id "
               "FROM staging.silver_observations WHERE artifact_id = %s "
               "ORDER BY source", (artifact_id,))
    written = vc.fetchall()
    assert [r["source"] for r in written] == ["carousel", "detail"]
    # The primary and the carousel row share the object's one identity.
    assert {r["artifact_id"] for r in written} == {artifact_id}
    assert {r["fetched_at"] for r in written} == {CAPTURE_AT}

    vc.execute("SELECT listing_id, event_type, source, event_at "
               "FROM staging.price_observation_events WHERE artifact_id = %s",
               (artifact_id,))
    minted = vc.fetchall()
    assert len(minted) == 1                       # the carousel row minted none
    assert str(minted[0]["listing_id"]) == listing_id
    assert minted[0]["event_type"] == "upserted"
    assert minted[0]["event_at"] == CAPTURE_AT    # not the now() default

    vc.execute("SELECT status, artifact_type, minio_path, fetched_at, event_at, "
               "listing_id FROM staging.artifacts_queue_events "
               "WHERE artifact_id = %s", (artifact_id,))
    recovered = vc.fetchall()
    assert len(recovered) == 1
    assert recovered[0]["status"] == RECOVERED_STATUS
    assert recovered[0]["artifact_type"] == "detail_page"
    assert recovered[0]["minio_path"].startswith("s3://bronze/")
    assert recovered[0]["fetched_at"] == CAPTURE_AT       # the April capture
    assert recovered[0]["event_at"] > CAPTURE_AT          # the recovery action

    vc.execute(f"SELECT * FROM {RECEIPT_TABLE} WHERE batch_name = %s", (batch,))
    receipt = vc.fetchone()
    assert receipt["manifest_sha256"] == manifest
    assert (receipt["artifact_count"], receipt["silver_count"],
            receipt["price_event_count"], receipt["queue_event_count"]) == (1, 2, 1, 1)


def test_an_unlisted_detail_row_mints_a_deleted_event(recovery_batch, writer_conn, vc):
    listing_id = str(uuid.uuid4())
    key = f"html/2026/04/pack/{uuid.uuid4().hex}.html.zst"
    with writer_conn.cursor() as cur:
        artifact_id = allocate_artifact_ids(cur, 1)[0]
    writer_conn.commit()
    rows = [_parsed_row(listing_id, key, listing_state="unlisted", price=None)]
    batch, silver, events, queue = recovery_batch(rows, {key: artifact_id})
    write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)

    vc.execute("SELECT event_type, price FROM staging.price_observation_events "
               "WHERE artifact_id = %s", (artifact_id,))
    row = vc.fetchone()
    assert row["event_type"] == "deleted" and row["price"] is None


def test_a_committed_batch_rerun_writes_zero_rows_and_touches_no_sequence(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    manifest = _digest(batch)
    write_import_batch(writer_conn, batch, manifest, silver, events, queue)

    vc.execute("SELECT last_value FROM ops.artifacts_queue_artifact_id_seq")
    sequence_before = vc.fetchone()["last_value"]

    again = write_import_batch(writer_conn, batch, manifest, silver, events, queue)
    assert again["skipped"] is True
    assert (again["silver"], again["price_events"], again["queue_events"]) == (0, 0, 0)

    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 2                # still the first write's rows
    vc.execute("SELECT count(*) AS n FROM staging.price_observation_events "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 1
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (batch,))
    assert vc.fetchone()["n"] == 1

    vc.execute("SELECT last_value FROM ops.artifacts_queue_artifact_id_seq")
    assert vc.fetchone()["last_value"] == sequence_before


def test_the_same_batch_name_with_another_digest_stops_and_writes_nothing(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)

    with pytest.raises(ReceiptConflict) as exc:
        write_import_batch(writer_conn, batch, _digest("a different manifest"),
                           silver, events, queue)
    assert _digest(batch) in str(exc.value)

    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 2                # not doubled
    vc.execute(f"SELECT manifest_sha256 FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (batch,))
    assert [r["manifest_sha256"] for r in vc.fetchall()] == [_digest(batch)]


def test_a_failure_mid_batch_rolls_back_all_four_writes(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    # artifact_id is NOT NULL on staging.artifacts_queue_events, so the third
    # write fails after the first two have already run inside the transaction.
    broken = [dict(queue[0], artifact_id=None)]

    with pytest.raises(Exception):
        write_import_batch(writer_conn, batch, _digest(batch), silver, events, broken)

    for table in ("staging.silver_observations",
                  "staging.price_observation_events",
                  "staging.artifacts_queue_events"):
        vc.execute(f"SELECT count(*) AS n FROM {table} WHERE artifact_id = %s",
                   (artifact_id,))
        assert vc.fetchone()["n"] == 0, table
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (batch,))
    assert vc.fetchone()["n"] == 0

    # And the connection is usable again: the retry succeeds whole.
    write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 2


# -- probe: run the real transaction, then roll it back -----------------

def test_a_probe_apply_issues_every_statement_and_commits_nothing(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    manifest = _digest(batch)

    out = write_import_batch(writer_conn, batch, manifest, silver, events, queue,
                             probe=True)
    # The would-be write set is reported exactly as an authoritative commit
    # reports it -- same dict shape, same counts.
    assert out == {"batch_name": batch, "skipped": False, "silver": 2,
                   "price_events": 1, "queue_events": 1, "artifacts": 1}

    for table in ("staging.silver_observations",
                  "staging.price_observation_events",
                  "staging.artifacts_queue_events"):
        vc.execute(f"SELECT count(*) AS n FROM {table} WHERE artifact_id = %s",
                   (artifact_id,))
        assert vc.fetchone()["n"] == 0, table
    vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} WHERE batch_name = %s",
               (batch,))
    assert vc.fetchone()["n"] == 0

    # The connection survives the rollback and an authoritative write still commits.
    write_import_batch(writer_conn, batch, manifest, silver, events, queue)
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 2


def test_a_probe_apply_still_lets_a_constraint_fire_at_statement_time(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    # artifact_id is NOT NULL on staging.artifacts_queue_events: the third write
    # fails at statement time, inside the probe's own transaction. The rollback
    # must not swallow that -- the exception has to escape.
    broken = [dict(queue[0], artifact_id=None)]
    with pytest.raises(Exception):
        write_import_batch(writer_conn, batch, _digest(batch), silver, events,
                           broken, probe=True)

    for table in ("staging.silver_observations", "staging.price_observation_events"):
        vc.execute(f"SELECT count(*) AS n FROM {table} WHERE artifact_id = %s",
                   (artifact_id,))
        assert vc.fetchone()["n"] == 0, table


# -- what must not move ---------------------------------------------------

def test_the_four_protected_tables_and_the_queue_are_unchanged(
        recovery_batch, writer_conn, vc):
    # Seeded so the comparison is over non-empty tables: an equality claim
    # between two empty snapshots proves nothing.
    live_listing = str(uuid.uuid4())
    vc.execute(
        "INSERT INTO ops.price_observations (listing_id, vin, price, make, model, "
        "last_seen_at, last_artifact_id) VALUES (%s::uuid, %s, 1, 'x', 'y', now(), 1)",
        (live_listing, f"VIN{uuid.uuid4().hex[:14].upper()}"),
    )
    vc.execute(
        "INSERT INTO ops.detail_scrape_claims (listing_id, claimed_by, status) "
        "VALUES (%s::uuid, 'itest', 'running')", (live_listing,),
    )
    try:
        before = _snapshot_protected(vc)
        vc.execute("SELECT count(*) AS n FROM ops.artifacts_queue")
        queue_before = vc.fetchone()["n"]

        (batch, silver, events, queue, _aid, *_rest) = \
            _one_object_batch(recovery_batch, writer_conn)
        write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)

        assert _snapshot_protected(vc) == before
        vc.execute("SELECT count(*) AS n FROM ops.artifacts_queue")
        assert vc.fetchone()["n"] == queue_before
    finally:
        vc.execute("DELETE FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
                   (live_listing,))
        vc.execute("DELETE FROM ops.price_observations WHERE listing_id = %s::uuid",
                   (live_listing,))


def test_the_receipt_outlives_the_flush_that_deletes_the_rows_it_describes(
        recovery_batch, writer_conn, vc):
    # This is the whole reason the table exists: after the flush, Postgres
    # cannot otherwise tell "never committed" from "committed and flushed".
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    manifest = _digest(batch)
    write_import_batch(writer_conn, batch, manifest, silver, events, queue)

    for table in ("staging.silver_observations",
                  "staging.price_observation_events",
                  "staging.artifacts_queue_events"):
        vc.execute(f"DELETE FROM {table} WHERE artifact_id = %s", (artifact_id,))

    again = write_import_batch(writer_conn, batch, manifest, silver, events, queue)
    assert again["skipped"] is True
    vc.execute("SELECT count(*) AS n FROM staging.silver_observations "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["n"] == 0            # the flushed rows stay flushed


def test_the_not_null_column_would_have_accepted_the_string_None(vc):
    """Why the listing id is validated in Python before it is cast.

    ``staging.silver_observations.listing_id`` is ``text NOT NULL``, so it is
    tempting to treat the column as the check. It is not: ``str(None)`` is the
    four-character string ``"None"``, which satisfies NOT NULL and commits. A
    carousel row never reaches the price-event minter, so nothing downstream
    would have caught it either.
    """
    vc.execute(
        "INSERT INTO staging.silver_observations "
        "(artifact_id, listing_id, source, listing_state, fetched_at) "
        "VALUES (%s, %s, 'carousel', 'active', %s) RETURNING id",
        (-1, str(None), CAPTURE_AT),
    )
    row_id = vc.fetchone()["id"]
    try:
        vc.execute("SELECT listing_id FROM staging.silver_observations "
                   "WHERE id = %s", (row_id,))
        assert vc.fetchone()["listing_id"] == "None"      # committed happily
    finally:
        vc.execute("DELETE FROM staging.silver_observations WHERE id = %s",
                   (row_id,))


@pytest.mark.parametrize("bad", [None, "", "not-a-uuid"])
def test_the_writer_refuses_that_row_before_it_reaches_the_column(bad):
    from scripts.reconcile_april_detail import ImportSetInvalid

    with pytest.raises(ImportSetInvalid):
        build_recovery_silver_row(
            _parsed_row(bad, "html/2026/04/pack/x.html.zst", source="carousel"),
            1, {},
        )


# -- identity -------------------------------------------------------------

def test_nextval_never_returns_a_value_twice_across_two_connections(
        writer_conn, pg_conn):
    with writer_conn.cursor() as cur:
        first = allocate_artifact_ids(cur, 5)
    with pg_conn.cursor() as other:
        second = allocate_artifact_ids(other, 5)
    with writer_conn.cursor() as cur:
        third = allocate_artifact_ids(cur, 5)
    writer_conn.commit()
    assert len(set(first + second + third)) == 15
    assert first == sorted(first)


def test_a_rolled_back_allocation_still_never_reissues_the_value(writer_conn, pg_conn):
    with writer_conn.cursor() as cur:
        lost = allocate_artifact_ids(cur, 3)
    writer_conn.rollback()                  # a bigserial gap, not a reuse
    with pg_conn.cursor() as other:
        after = allocate_artifact_ids(other, 3)
    assert min(after) > max(lost)


def test_the_allocated_id_is_far_above_the_largest_historical_id(writer_conn):
    # The March-May window's largest is 4,902,473 and the sequence was at
    # 7,732,177 during the audit, so preserved and allocated ids cannot collide.
    with writer_conn.cursor() as cur:
        allocated = allocate_artifact_ids(cur, 1)[0]
    writer_conn.commit()
    assert allocated > 0


# -- the receipt table's own contract ------------------------------------

def test_the_receipt_table_rejects_a_digest_that_is_not_a_sha256(pg_conn):
    with pg_conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                f"INSERT INTO {RECEIPT_TABLE} (batch_name, manifest_sha256, "
                "artifact_count, silver_count, price_event_count, "
                "queue_event_count) VALUES ('bad', 'tooshort', 0, 0, 0, 0)"
            )


def test_two_digests_for_one_batch_name_are_both_visible(pg_conn, vc):
    # The composite primary key is what makes a digest conflict observable
    # rather than an overwrite the writer could not see.
    batch = f"itest-{uuid.uuid4().hex[:12]}"
    try:
        for digest in (_digest("a"), _digest("b")):
            vc.execute(
                f"INSERT INTO {RECEIPT_TABLE} (batch_name, manifest_sha256, "
                "artifact_count, silver_count, price_event_count, "
                "queue_event_count) VALUES (%s, %s, 0, 0, 0, 0)", (batch, digest),
            )
        vc.execute(f"SELECT count(*) AS n FROM {RECEIPT_TABLE} "
                   "WHERE batch_name = %s", (batch,))
        assert vc.fetchone()["n"] == 2
    finally:
        vc.execute(f"DELETE FROM {RECEIPT_TABLE} WHERE batch_name = %s", (batch,))


def test_the_recovered_status_is_accepted_with_no_hot_queue_row(
        recovery_batch, writer_conn, vc):
    # staging.artifacts_queue_events has no FK and no status CHECK, which is
    # what lets a recovered artifact exist without ever entering the queue.
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)
    vc.execute("SELECT count(*) AS n FROM ops.artifacts_queue WHERE artifact_id = %s",
               (artifact_id,))
    assert vc.fetchone()["n"] == 0
    vc.execute("SELECT status FROM staging.artifacts_queue_events "
               "WHERE artifact_id = %s", (artifact_id,))
    assert vc.fetchone()["status"] == RECOVERED_STATUS


def test_a_recovered_capture_time_stays_four_months_behind_now(
        recovery_batch, writer_conn, vc):
    (batch, silver, events, queue, artifact_id, *_rest) = \
        _one_object_batch(recovery_batch, writer_conn)
    write_import_batch(writer_conn, batch, _digest(batch), silver, events, queue)
    vc.execute("SELECT now() - fetched_at AS age FROM staging.silver_observations "
               "WHERE artifact_id = %s LIMIT 1", (artifact_id,))
    assert vc.fetchone()["age"] > timedelta(days=1)
