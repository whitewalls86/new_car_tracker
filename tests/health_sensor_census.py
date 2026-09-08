"""Plan 162 Stage D (Plan 139 Stage H): the health-sensor census, declared once.

Two tests count the same invariant from opposite ends and neither could see the
other's number. `tests/airflow/test_health_sensor_demotion.py` read DAG source
with `ast` and asserted 13 DAG *files* wire a sensor;
`tests/integration/airflow/test_dag_integrity.py` built a DagBag and asserted 14
sensor *tasks*. Both numbers were right -- `hourly_analytics_refresh` wires two
-- and nothing connected them, so the pair could only be kept honest by whoever
happened to remember the second one existed.

Nobody did. Plan 134's survey deleted `cleanup_parquet.py` and
`cleanup_artifacts.py`, updated the first count from 15 to 13, missed the second,
and shipped (`056cde7`); PR #293 then failed on a count its author had not
touched. The comment added in `33b275e` -- "one DAG wires two sensors" -- is
documentation, not a mechanism, and would have drifted the same way.

Both numbers are now derived from the mapping below, so deleting or adding a
sensor is one edit here and both tests follow.

**This module must not import Airflow, and imports nothing at all.** Its two
readers run in different virtual environments: `tests/airflow/` is the main venv,
where importing `airflow` is a contract violation, and
`tests/integration/airflow/` is the isolated `apache-airflow==3.2.0` venv built
in CI. A declaration only both can read is a declaration with no dependencies.

For the same reason, **both readers load this file by path and neither imports
it**. `from tests.health_sensor_census import ...` resolves in the main venv and
not in the Airflow one, where pytest leaves the repo root off `sys.path`: CI run
33444675959 failed collection there with `ModuleNotFoundError: No module named
'tests'` on an import that had passed locally. Importing nothing is what makes
loading by path safe.

The census is checked against the DAGs from both sides rather than trusted:
`test_the_gate_survives_the_demotion` asserts the file-to-service mapping matches
what the DAG sources actually call, and
`test_health_sensors_skip_rather_than_fail_on_the_real_operators` asserts the
task ids it implies are the ones the DagBag actually built.
"""

# DAG file -> the service names it passes to `http_health_sensor`, in call order.
#
# `http_health_sensor(service_name, health_url)` builds exactly one task, with
# task_id `check_{service_name}_health` (airflow/dags/sensors.py), so this
# mapping fixes both censuses: 13 keys, 14 services.
#
# These sensors are gates, not notifiers (Plan 140 Stage 4): each one stops its
# DAG from starting work against a service that is not answering. A DAG that
# quietly loses its entry here is work running blind, which is why the count is
# asserted at all rather than merely observed.
HEALTH_SENSOR_CENSUS = {
    "cleanup_queue.py": ("archiver",),
    "compact_silver.py": ("archiver",),
    "dbt_build.py": ("dbt_runner",),
    "disk_usage.py": ("pack_worker",),
    "export_ci_lake_snapshot.py": ("archiver",),
    "flush_silver_observations.py": ("archiver",),
    "flush_staging_events.py": ("archiver",),
    # The one DAG wiring two, and the reason the two censuses disagree by one.
    "hourly_analytics_refresh.py": ("archiver", "dbt_runner"),
    "orphan_checker.py": ("ops",),
    "pack_bronze_html.py": ("pack_worker",),
    "results_processing.py": ("processing",),
    "scrape_detail_pages.py": ("scraper",),
    "scrape_listings.py": ("scraper",),
}


def expected_sensor_task_ids():
    """The `check_*_health` task ids the census implies, as a sorted list.

    A list rather than a set: `archiver` appears in six DAGs and the count of
    sensor *tasks* is what the DagBag census compares against, so collapsing
    duplicates here would silently turn 14 into 6.
    """
    return sorted(
        f"check_{service}_health"
        for services in HEALTH_SENSOR_CENSUS.values()
        for service in services
    )
