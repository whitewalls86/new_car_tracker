"""Every dbt model materializes rows, or the fixture is wrong.

Plan 162 Stage S, exit 3. **This is the whole of what the fixture owes.** It was
carrying an obligation to reach particular branches, and measurement said that
was the wrong job for it -- unit tests reach 148 of 308 branch points against
the fixture's 95, and the 22 the fixture alone reaches are the ones a unit test
structurally cannot express. Released from branch coverage, the fixture answers
the one question nothing else can: does a build over this data produce a world
where every model is actually populated?

**Nothing asked that before, and five of the 23 models were empty.** dbt has six
sources and two are Postgres tables read through ``postgres_scan``, while
``scripts/seed_lake_snapshot_fixture.py`` wrote only the MinIO half, so
``ops.tracked_models`` was empty; ``int_active_make_models`` inner-joined it and
the emptiness cascaded through ``mart_vehicle_snapshot`` to ``mart_deal_scores``
and ``mart_price_freshness_trend``. ``int_benchmarks`` was empty for a second,
independent reason -- every fixture price event named a VIN that either never
appears in the observations or appears short-form, which ``stg_observations``'
17-character guard nulls, so ``int_price_history`` and ``int_latest_observation``
shared no ``vin17``.

The build reported success throughout, and every ``not_null`` on those models
passed: **an assertion over an empty relation is trivially true.** Roughly thirty
declared constraints were asserting against nothing at all.

``--require-non-empty`` names this exact cascade in its own CI comment, and
guards *sources* in the other job. This guards *models*, here.

**There is no waiver list, and that is a decision rather than an oversight.**
The only candidate anyone could construct for a legitimately empty model turned
out to be a live defect: ``mart_vehicle_snapshot.sql``'s ``'active'`` arm gates
on ``last_seen_at >= now() - interval '7 days'`` while the fixture's timestamps
are absolute, so it had been dead for weeks because the calendar moved. A waiver
list is exactly where that gets written down as acceptable instead of repaired.
The cost of refusing one is that this gate will one day fail for a reason no
commit caused; the answer to that is anchoring the fixture's dates relative to
``now()``, not an exemption.

**The list is derived, so a new model joins it by existing.** Nobody adds a
model here, which means nobody can forget to.
"""
import os
from functools import lru_cache
from pathlib import Path

import pytest

from tests.sql_loader import queries

from .real_build import analytics_con

SQL = queries(__file__)

REPO_ROOT = Path(__file__).resolve().parents[3]
MODELS_DIR = REPO_ROOT / "dbt" / "models"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("DUCKDB_PATH"),
        reason="DUCKDB_PATH not set -- no real dbt build to inspect",
    ),
]


def declared_models() -> set[str]:
    """Every model on disk. Derived, never maintained."""
    models = {path.stem for path in MODELS_DIR.rglob("*.sql")}
    assert models, f"no dbt models found under {MODELS_DIR}"
    return models


@lru_cache(maxsize=1)
def _row_counts() -> dict[str, int]:
    """``{model: rows}`` for every model, or -1 where the relation is absent."""
    connection = analytics_con()
    try:
        counts = {}
        for model in sorted(declared_models()):
            try:
                row = connection.execute(
                    SQL("count_model_rows").format(relation=f'main."{model}"')
                ).fetchone()
            except Exception:  # noqa: BLE001 - absence is the finding, not an error
                counts[model] = -1
                continue
            counts[model] = int(row[0]) if row else 0
        return counts
    finally:
        connection.close()


def test_every_declared_model_was_actually_built():
    """A model the build skipped is not a model with no rows -- it is absent.

    Kept separate from the row count below because the two have different
    causes and different repairs: a missing relation means the build did not
    reach it, while an empty one means the fixture did not feed it. Reporting
    them together would send somebody to the wrong file.
    """
    missing = sorted(model for model, count in _row_counts().items() if count < 0)
    assert not missing, (
        "these models are declared under dbt/models/ but no relation exists in "
        "the warehouse the build produced. Either the build skipped them -- a "
        "SKIP is not a pass -- or they materialize somewhere this connection "
        "cannot see:\n  " + "\n  ".join(missing)
    )


def test_no_model_materializes_zero_rows():
    """The gate. An empty model is a defect in the fixture, never a fact.

    Demonstrated rather than asserted: on 2026-09-07 this named five models,
    and closing it took two independent fixture repairs -- seeding
    ``ops.tracked_models``, which nothing in the dbt path writes, and giving the
    benchmark VINs price events so ``int_price_history`` and
    ``int_latest_observation`` finally shared a ``vin17``.
    """
    empty = sorted(model for model, count in _row_counts().items() if count == 0)
    assert not empty, (
        f"{len(empty)} of {len(_row_counts())} dbt models materialized zero "
        f"rows. Every assertion on them passes vacuously -- a not_null over an "
        f"empty relation is trivially true -- so their declared constraints are "
        f"asserting against nothing, and any branch-coverage number that "
        f"includes them is measuring a model that did not run.\n\n"
        f"Feed them from scripts/seed_lake_snapshot_fixture.py. There is no "
        f"waiver for this: an empty model is a defect in the fixture, and the "
        f"one case that looked like an exception was a guard that had gone dead "
        f"because the fixture's dates are absolute and the calendar moved.\n  "
        + "\n  ".join(empty)
    )


def test_the_row_counts_cover_every_declared_model():
    """Both directions, so a model cannot leave the denominator by vanishing.

    The set difference above is only as good as the set it walks. If
    ``declared_models()`` silently narrowed -- a glob that stopped matching, a
    directory renamed -- every assertion here would pass over a smaller world,
    which is the failure this plan has now found in four separate instruments,
    including two of its own.
    """
    assert set(_row_counts()) == declared_models()
    assert len(_row_counts()) >= 23, (
        f"only {len(_row_counts())} models were counted; the project had 23 "
        f"when this gate landed, and a sudden drop means the model list stopped "
        f"finding them rather than that the project shrank"
    )
