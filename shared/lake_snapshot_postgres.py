"""
The two Postgres dimension tables that travel inside a Plan 120 lake snapshot.

`dbt/models/sources.yml` declares six source tables. Four are Parquet in MinIO
and are what the exporter already writes. The other two -- `public.search_configs`
and `ops.tracked_models` -- resolve through `postgres_scan()`, so they have to be
live rows in a database, not objects in a bucket. Left empty, `stg_search_configs`
reads nothing, `int_active_make_models` inner-joins to nothing, and
`mart_vehicle_snapshot` builds green over an empty world.

This module is the contract between the two halves of that round trip: the
archiver's exporter (`lake_snapshot_export.materialize_postgres_tables`) and
`scripts/seed_lake_snapshot.py`. It owns the table allowlist, the archive path
convention, and the only two places the SQL templates in `shared/queries.py` get
their identifiers -- which is also why an archive member naming a relation this
tuple has never heard of is refused rather than formatted into a statement.

Both tables are exported **whole**, not filtered to the snapshot's cohort. They
are small, they carry neither VIN nor dealer data, and full dimensions against a
cohort fact set drop rows for cohort reasons only -- never because a dimension
row was left behind.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from shared.queries import (
    REPLACE_POSTGRES_SNAPSHOT_TABLE,
    SELECT_POSTGRES_SNAPSHOT_TABLE,
)

# (schema, table), in the order a seed applies them. Adding an entry here is the
# whole of "a third table travels with the snapshot": the exporter iterates it,
# the seeder validates against it, and neither holds its own copy.
POSTGRES_SNAPSHOT_TABLES: Tuple[Tuple[str, str], ...] = (
    ("public", "search_configs"),
    ("ops", "tracked_models"),
)

# Deliberately its own top-level prefix, beside `silver_normalized/` and
# `ops_normalized/`: the seeder has to be able to tell "this goes to MinIO" from
# "this goes to Postgres" by path alone, before it opens anything.
POSTGRES_PREFIX = "postgres/"


class UnknownSnapshotTableError(Exception):
    """Raised when an archive member names a relation outside the allowlist."""


def snapshot_object_name(schema: str, table: str) -> str:
    """The archive-relative path one exported table is written to."""
    return f"{POSTGRES_PREFIX}{schema}.{table}.json"


def parse_snapshot_object_name(name: str) -> Tuple[str, str]:
    """Resolve an archive-relative path back to an allowlisted (schema, table).

    Raises :class:`UnknownSnapshotTableError` on anything else. This is the
    guard that keeps a tampered or simply outdated archive from reaching
    ``.format()`` on a statement -- the relation is an identifier, so it cannot
    be bound as a parameter and the allowlist is what stands in for that.
    """
    for schema, table in POSTGRES_SNAPSHOT_TABLES:
        if name == snapshot_object_name(schema, table):
            return schema, table
    raise UnknownSnapshotTableError(
        f"archive member {name!r} does not name a table this snapshot carries; "
        f"expected one of "
        f"{[snapshot_object_name(s, t) for s, t in POSTGRES_SNAPSHOT_TABLES]}"
    )


def dump_table(cur, schema: str, table: str) -> str:
    """Read one whole table as the JSON array text the archive stores."""
    cur.execute(SELECT_POSTGRES_SNAPSHOT_TABLE.format(schema=schema, table=table))
    return cur.fetchone()[0]


def load_table(cur, schema: str, table: str, rows_json: str) -> int:
    """Replace one table's contents with *rows_json*; returns rows inserted.

    ``cur.rowcount`` after a multi-statement execute reports the last statement,
    which is the INSERT -- the DELETE's count is not interesting and is not what
    a caller checking "did the snapshot land" wants to see.
    """
    cur.execute(
        REPLACE_POSTGRES_SNAPSHOT_TABLE.format(schema=schema, table=table),
        (rows_json,),
    )
    return cur.rowcount


def row_count(rows_json: str) -> int:
    """How many rows a stored JSON array holds, without touching a database."""
    rows: List[Dict[str, Any]] = json.loads(rows_json)
    return len(rows)
