"""Per-DAG admission surfaces for Plan 142 scoped coordination."""

ADMISSION_SURFACES = {
    "cleanup_artifacts": frozenset({"archive"}),
    "cleanup_parquet": frozenset({"archive"}),
    "cleanup_queue": frozenset({"archive"}),
    "compact_silver": frozenset({"analytics", "archive"}),
    "dbt_build": frozenset({"analytics"}),
    "delete_stale_emails": frozenset({"database"}),
    "disk_usage": frozenset({"archive"}),
    "export_ci_lake_snapshot": frozenset({"analytics", "archive"}),
    "flush_silver_observations": frozenset({"analytics", "archive"}),
    "flush_staging_events": frozenset({"analytics", "archive"}),
    "hourly_analytics_refresh": frozenset({"analytics", "archive", "detail_fetch"}),
    "orphan_checker": frozenset({"detail_fetch", "processing"}),
    "pack_bronze_html": frozenset({"archive"}),
    "prune_task_logs": frozenset({"airflow_control"}),
    "results_processing": frozenset({"processing"}),
    "scrape_detail_pages": frozenset({"detail_fetch"}),
    "scrape_listings": frozenset({"listing_fetch"}),
}


def admission_surfaces(dag_id: str) -> frozenset[str]:
    """Return the reviewed admission set; an undeclared DAG fails closed."""
    try:
        return ADMISSION_SURFACES[dag_id]
    except KeyError as exc:
        raise ValueError(f"DAG has no coordination declaration: {dag_id}") from exc
