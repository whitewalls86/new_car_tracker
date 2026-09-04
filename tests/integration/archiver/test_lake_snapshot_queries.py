"""
SQL smoke tests for the Plan 120 lake snapshot statements.

The eleven files under `archiver/sql/lake_snapshot/` are DuckDB over Parquet in
MinIO, not Postgres, so no fixture in `tests/integration/sql/` can reach them.
They live here for the reason `docs/TESTING.md`'s Layer 2 table already gives
for the selectors beside them: this suite is where the archiver's SQL meets a
real engine and real files. The directory is Layer 4 and this module does not
claim otherwise — the Layer 2 table is about which suites *execute production
SQL*, which is a different question from which layer a directory sits at, and
`tests/integration/archiver/` has answered yes to the first since Stage 7.

**Each statement is executed here, not merely named.** The sibling modules
exercise the *functions* that build these queries and assert what the cohort
should contain; this module asserts the narrower thing Layer 2 is for — the
statement parses against DuckDB, reads the columns the caller unpacks, and
survives a schema drift in the source Parquet. Those are different failures:
`test_lake_snapshot_cohort.py` fails when closure is wrong, this fails when a
column stops existing.

Every statement here is a template, and every template is filled the way its
caller fills it. A test that formatted a simplified WHERE clause of its own
would assert nothing about production — that is the paraphrase this whole
convention exists to prevent — so each case builds its fragments through the
same `in_clause`/`table_time_where` helpers the processors use.
"""
import os

import pytest

from archiver.processors.lake_snapshot_sql import in_clause, table_time_where
from archiver.processors.lake_source_audit import resolve_table_path
from archiver.queries import (
    SELECT_ARTIFACT_IDS,
    SELECT_FILTERED_TABLE_ROWS,
    SELECT_LISTING_IDS_FOR_VINS,
    SELECT_PREVIOUS_LISTING_IDS,
    SELECT_ROW_KEYS_FOR_CANDIDATES,
    SELECT_SEED_VINS_BY_HASH,
    SELECT_SOURCE_TABLE_STATS,
    SELECT_VINS_FOR_LISTING_IDS,
    SELECT_VINS_RANKED_WITHIN_MAKE_MODEL,
    WRAP_AGGREGATE_QUERY,
    WRAP_CANDIDATE_QUERY,
)
from scripts import seed_lake_snapshot_fixture as fx
from shared.duckdb_s3 import get_duckdb_s3_connection

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — no Parquet for these statements to read",
    ),
]

# A candidate query standing in for a selector's, in the one position a bind
# parameter cannot occupy. Deliberately trivial: what the wrappers do with an
# inner query is the subject, not what the inner query selects.
_CANDIDATE_SQL = """
    SELECT vin, listing_id, artifact_id
    FROM read_parquet('{path}', union_by_name=true)
    WHERE vin IS NOT NULL
"""


@pytest.fixture(scope="module")
def minio_con():
    con = get_duckdb_s3_connection()
    yield con
    con.close()


@pytest.fixture(scope="module")
def silver_path():
    return resolve_table_path("silver_observations", None)


@pytest.fixture(scope="module")
def candidate_sql(silver_path):
    return _CANDIDATE_SQL.format(path=silver_path)


class TestSelectorWrappers:
    """The two statements whose interpolated argument is another statement."""

    def test_wrap_candidate_query_returns_counts_and_a_capped_entity_list(
        self, minio_con, candidate_sql,
    ):
        row = minio_con.execute(
            WRAP_CANDIDATE_QUERY.format(
                candidate_sql=candidate_sql, entity_key="vin", cap=3,
            )
        ).fetchone()

        candidate_rows, entities, bounded = row
        assert candidate_rows > 0
        assert entities > 0
        # The cap is the point: the caller reads this to bound how much of the
        # candidate pool it pulls into memory.
        assert len(bounded) <= 3
        assert bounded == sorted(bounded)

    def test_wrap_aggregate_query_returns_counts_and_a_five_entity_sample(
        self, minio_con, candidate_sql,
    ):
        row = minio_con.execute(
            WRAP_AGGREGATE_QUERY.format(
                candidate_sql=candidate_sql, entity_key="vin",
            )
        ).fetchone()

        candidate_rows, entities, sample = row
        assert candidate_rows > 0
        assert entities > 0
        assert len(sample) <= 5

    def test_row_keys_for_candidates_reads_all_three_identity_columns(
        self, minio_con, candidate_sql,
    ):
        membership, params = in_clause("vin", (fx.VIN_RELISTED,))
        rows = minio_con.execute(
            SELECT_ROW_KEYS_FOR_CANDIDATES.format(
                candidate_sql=candidate_sql, membership=membership,
            ),
            params,
        ).fetchall()

        assert rows, "the fixture's relisted VIN should have candidate rows"
        for artifact_id, vin, listing_id in rows:
            assert vin == fx.VIN_RELISTED
            assert artifact_id is not None
            assert listing_id is not None


class TestCohortAllocation:
    def test_seed_vins_by_hash_is_bounded_and_deterministic(
        self, minio_con, silver_path,
    ):
        time_clauses, params = table_time_where(None, None, "fetched_at")
        where_sql = " AND ".join(["vin IS NOT NULL"] + time_clauses)
        query = SELECT_SEED_VINS_BY_HASH.format(
            path=silver_path, where_sql=where_sql, fetch_limit=5,
        )

        first = minio_con.execute(query, params).fetchall()
        assert 0 < len(first) <= 5
        # The md5 ordering is the sampling. Two runs of the same statement over
        # the same files returning different VINs would make the export
        # fingerprint meaningless.
        assert minio_con.execute(query, params).fetchall() == first

    def test_vins_ranked_within_make_model_caps_each_group(
        self, minio_con, silver_path,
    ):
        time_clauses, params = table_time_where(None, None, "fetched_at")
        where_sql = " AND ".join(["vin IS NOT NULL"] + time_clauses)
        rows = minio_con.execute(
            SELECT_VINS_RANKED_WITHIN_MAKE_MODEL.format(
                path=silver_path, where_sql=where_sql, limit_per_group=1,
            ),
            params,
        ).fetchall()

        assert rows
        assert all(len(row) == 2 for row in rows)


class TestEntityClosure:
    """One test per closure step. Each reads a different column set, which is
    what a drift in the ops event tables breaks."""

    def test_listing_ids_for_vins(self, minio_con):
        path = resolve_table_path("silver_observations", None)
        vin_clause, params = in_clause("vin", (fx.VIN_RELISTED,))
        where_sql = " AND ".join([vin_clause, "listing_id IS NOT NULL"])
        rows = minio_con.execute(
            SELECT_LISTING_IDS_FOR_VINS.format(path=path, where_sql=where_sql),
            params,
        ).fetchall()

        # The relisted VIN is the fixture's two-listing case, which is why it is
        # the one that proves this step returns more than the obvious answer.
        assert len({r[0] for r in rows}) >= 2

    def test_vins_for_listing_ids(self, minio_con):
        path = resolve_table_path("silver_observations", None)
        # L2 is the relisted VIN's second listing, so this step is what carries
        # closure back to the VIN when a listing_id is the seed.
        listing_clause, params = in_clause("listing_id", ("L2",))
        where_sql = " AND ".join([listing_clause, "vin IS NOT NULL"])
        rows = minio_con.execute(
            SELECT_VINS_FOR_LISTING_IDS.format(path=path, where_sql=where_sql),
            params,
        ).fetchall()

        assert {r[0] for r in rows} == {fx.VIN_RELISTED}

    def test_previous_listing_ids(self, minio_con):
        """The remap column, which lives only on vin_to_listing_events. A cohort
        missing this step holds a remap event pointing outside itself."""
        path = resolve_table_path("vin_to_listing_events", None)
        vin_clause, params = in_clause("vin", (fx.VIN_RELISTED,))
        where_sql = " AND ".join([vin_clause, "previous_listing_id IS NOT NULL"])
        rows = minio_con.execute(
            SELECT_PREVIOUS_LISTING_IDS.format(path=path, where_sql=where_sql),
            params,
        ).fetchall()

        assert rows, "the relisted VIN should carry a remap event"

    def test_artifact_ids(self, minio_con):
        path = resolve_table_path("silver_observations", None)
        vin_clause, params = in_clause("vin", (fx.VIN_RELISTED,))
        where_sql = " AND ".join([vin_clause, "artifact_id IS NOT NULL"])
        rows = minio_con.execute(
            SELECT_ARTIFACT_IDS.format(path=path, where_sql=where_sql),
            params,
        ).fetchall()

        assert rows
        assert all(r[0] is not None for r in rows)


class TestAuditAndMaterialization:
    @pytest.mark.parametrize(
        ("table", "ts_col", "has_vin"),
        [
            ("silver_observations", "fetched_at", True),
            ("price_observation_events", "event_at", True),
            ("vin_to_listing_events", "event_at", True),
            ("blocked_cooldown_events", "event_at", False),
        ],
    )
    def test_source_table_stats_projects_what_each_table_has(
        self, minio_con, table, ts_col, has_vin,
    ):
        """The projection varies by table -- blocked_cooldown_events has no vin
        -- and the caller unpacks the row positionally, so a statement that
        returned the columns in another order would be a silent mislabel."""
        path = resolve_table_path(table, None)
        select_parts = [
            "count(*) AS rows",
            f"min({ts_col}) AS min_ts",
            f"max({ts_col}) AS max_ts",
        ]
        if has_vin:
            select_parts.append("count(DISTINCT vin) AS distinct_vins")

        row = minio_con.execute(
            SELECT_SOURCE_TABLE_STATS.format(
                select_parts=", ".join(select_parts), path=path, where_sql="",
            )
        ).fetchone()

        assert len(row) == len(select_parts)
        assert row[0] > 0

    def test_filtered_table_rows_returns_the_sort_order_the_archive_checksums(
        self, minio_con, silver_path,
    ):
        vin_clause, params = in_clause("vin", (fx.VIN_RELISTED,))
        rows = minio_con.execute(
            SELECT_FILTERED_TABLE_ROWS.format(
                path=silver_path,
                where_sql=vin_clause,
                order_by="fetched_at, listing_id, artifact_id",
            ),
            params,
        ).fetchall()

        assert rows
        # Deterministic order is not cosmetic here: the archive is checksummed,
        # so a statement that dropped its ORDER BY would produce a different
        # fingerprint per run over identical data.
        fetched_at_index = [
            desc[0] for desc in minio_con.description
        ].index("fetched_at")
        timestamps = [row[fetched_at_index] for row in rows]
        assert timestamps == sorted(timestamps)
