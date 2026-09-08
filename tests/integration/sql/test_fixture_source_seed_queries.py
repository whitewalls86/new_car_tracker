"""Layer 2 — the two statements that keep dbt's Postgres sources non-empty.

Plan 162 Stage S. Four of dbt's six sources are Parquet in MinIO and two are
Postgres tables read through ``postgres_scan``. The lake-snapshot fixture
seeder wrote only the Parquet half, so ``ops.tracked_models`` was empty on
every fixture build: ``int_active_make_models`` inner-joins it and produced
nothing, ``mart_vehicle_snapshot`` inner-joins that, and five of the 23 models
computed nothing while the build reported success and their ``not_null`` tests
passed vacuously over zero rows.

These two statements are what repairs that, so they are the ones that must
execute against a real Postgres rather than be believed. Both run here inside
the rolled-back transaction the suite provides, against the real Flyway schema
-- so a column rename or a dropped table in either relation fails here rather
than at 3am in a CI job whose only symptom is five empty models.

**No SQL is authored in this module.** The statements under test are imported
from the seeder that owns them, which is also what proves the text executed is
the text on disk; and the assertions read ``rowcount`` and the cursor's own
result rather than a hand-written read-back, so the only statements this test
sends are the production ones.
"""
import pytest

from scripts.seed_lake_snapshot_fixture import (
    CONFIG_SCHEMA,
    INSERT_FIXTURE_TRACKED_MODELS,
    OPS_SCHEMA,
    SELECT_ENABLED_SEARCH_KEYS,
    fixture_make_models,
)

pytestmark = pytest.mark.integration

#: A make/model no fixture seeds, so these tests assert on their own insert
#: rather than on whatever the table already holds. The seeder's real rows are
#: committed by the CI step that runs it, and a test reusing one of those would
#: pass or fail on the order the two happened to run in.
_SENTINEL_MAKE = "ZzTestMake"
_SENTINEL_MODEL = "ZzTestModel"


def test_select_enabled_search_keys_runs_against_the_real_config_table(cur):
    """The keys side. An empty result is a legitimate answer and a fatal one.

    ``int_active_make_models`` filters to ``enabled = true``, so a
    ``tracked_models`` row carrying a key that is disabled or absent contributes
    nothing -- the seed would insert rows, report success, and leave the same
    five models empty. Reading the keys back rather than hardcoding one is what
    makes the seed correct when the migration that populates ``search_configs``
    changes, and this asserts the statement can actually be executed against the
    shape Flyway builds.
    """
    cur.execute(SELECT_ENABLED_SEARCH_KEYS.format(schema=CONFIG_SCHEMA))
    rows = cur.fetchall()

    assert cur.description is not None
    assert [column.name for column in cur.description] == ["search_key"]
    assert all(row["search_key"] for row in rows), (
        f"{CONFIG_SCHEMA}.search_configs returned an enabled row with a null or "
        f"empty search_key, which would join to nothing: {rows}"
    )


def test_insert_fixture_tracked_models_writes_a_row_the_dbt_source_can_read(cur):
    """The insert side, executed rather than named.

    Rolled back with the transaction, so this leaves the table as it found it.
    """
    cur.execute(SELECT_ENABLED_SEARCH_KEYS.format(schema=CONFIG_SCHEMA))
    enabled = [row["search_key"] for row in cur.fetchall()]
    if not enabled:
        pytest.skip(
            f"{CONFIG_SCHEMA}.search_configs has no enabled row in this "
            f"database, so there is no key to attach a tracked model to"
        )

    statement = INSERT_FIXTURE_TRACKED_MODELS.format(schema=OPS_SCHEMA)
    cur.execute(statement, {"search_key": enabled[0],
                            "make": _SENTINEL_MAKE, "model": _SENTINEL_MODEL})
    assert cur.rowcount == 1, (
        "the insert reported no row written, so the fixture seed would leave "
        "ops.tracked_models empty and five models would build over an empty "
        "world again"
    )

    # Presence is all that matters, so a second seed of the same row is a no-op
    # rather than a primary-key violation -- the fixture is seeded more than
    # once across the phases of a CI run.
    cur.execute(statement, {"search_key": enabled[0],
                            "make": _SENTINEL_MAKE, "model": _SENTINEL_MODEL})
    assert cur.rowcount == 0, (
        "the conflict clause did not absorb a repeat seed, so seeding twice "
        "would fail the run rather than no-op"
    )


def test_the_statement_lowercases_what_the_model_joins_on(cur):
    """``int_active_make_models`` joins lowercased values, and this is why.

    Its own comment says *"No slug normalization needed: values are already
    lowercased by the processing service"* -- but the fixture's observations
    carry ``Toyota`` and ``Camry`` capitalised, so a seed that inserted them
    verbatim would join to nothing and the five models would stay empty with
    every row present. The ``LOWER()`` lives in the statement rather than in the
    caller so that this is a property of the SQL, and testable as one.
    """
    cur.execute(SELECT_ENABLED_SEARCH_KEYS.format(schema=CONFIG_SCHEMA))
    enabled = [row["search_key"] for row in cur.fetchall()]
    if not enabled:
        pytest.skip("no enabled search config in this database")

    statement = INSERT_FIXTURE_TRACKED_MODELS.format(schema=OPS_SCHEMA)
    cur.execute(statement, {"search_key": enabled[0],
                            "make": _SENTINEL_MAKE.upper(),
                            "model": _SENTINEL_MODEL.upper()})
    assert cur.rowcount == 1

    # Inserting the lowercase form now conflicts, which is only true if the
    # statement stored it lowercased.
    cur.execute(statement, {"search_key": enabled[0],
                            "make": _SENTINEL_MAKE.lower(),
                            "model": _SENTINEL_MODEL.lower()})
    assert cur.rowcount == 0, (
        "the statement stored the make/model as given rather than lowercased, "
        "so int_active_make_models' join on lowercased values finds nothing"
    )


def test_the_seeded_pairs_come_from_the_fixture_rather_than_a_second_list():
    """The pairs are derived, which is the defect this stage keeps repairing.

    A hand-kept list of makes beside a fixture that supplies them is exactly the
    shape that went stale in ``LAKE_TABLES``, in the selector registry and in
    ``schema.yml``. This asserts the derivation still reaches the observations,
    so a fixture that grows a make cannot silently stop reaching the marts.
    """
    pairs = fixture_make_models()

    assert pairs, "the fixture's observations yielded no make/model pair"
    assert all(make == make.lower() and model == model.lower()
               for make, model in pairs), (
        f"fixture_make_models returned a pair that is not lowercased, which "
        f"int_active_make_models' join would miss: {pairs}"
    )
    assert ("honda", "civic") in pairs, (
        "the dense benchmark group (Honda/Civic) is absent from the derived "
        "pairs, so int_benchmarks would have no group to aggregate"
    )
