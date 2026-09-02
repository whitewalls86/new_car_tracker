"""Per-DAG admission surfaces for Plan 142 scoped coordination."""

ADMISSION_SURFACES = {
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

# Only tasks capable of mutating project state participate in drain evidence.
# Sensors and notification tasks are intentionally excluded.
DRAIN_TASKS = {
    "cleanup_queue": frozenset({"cleanup_queue"}),
    "compact_silver": frozenset({"compact_silver"}),
    "dbt_build": frozenset({"dbt_build"}),
    "delete_stale_emails": frozenset({"delete_stale_emails"}),
    "disk_usage": frozenset({"disk_usage"}),
    "export_ci_lake_snapshot": frozenset({"export_ci_lake_snapshot"}),
    "flush_silver_observations": frozenset({"flush_silver_observations"}),
    "flush_staging_events": frozenset({"flush_staging_events"}),
    "hourly_analytics_refresh": frozenset(
        {
            "flush_silver_observations",
            "flush_staging_events",
            "dbt_build",
            "reconcile_cooldown_cohorts",
        }
    ),
    "orphan_checker": frozenset(
        {
            "expire_orphan_detail_claims",
            "reap_stuck_processing",
            "evict_delisted_cooldowns",
        }
    ),
    "pack_bronze_html": frozenset(
        {"pack_bronze_html", "prune_packed_source_html", "verify_pack_read_path"}
    ),
    "prune_task_logs": frozenset({"prune_task_logs"}),
    "results_processing": frozenset({"process_batch"}),
    "scrape_detail_pages": frozenset({"claim_batch", "scrape_detail", "release_claims"}),
    "scrape_listings": frozenset({"advance_rotation", "run_scrapes"}),
}


def admission_surfaces(dag_id: str) -> frozenset[str]:
    """Return the reviewed admission set; an undeclared DAG fails closed."""
    try:
        return ADMISSION_SURFACES[dag_id]
    except KeyError as exc:
        raise ValueError(f"DAG has no coordination declaration: {dag_id}") from exc
