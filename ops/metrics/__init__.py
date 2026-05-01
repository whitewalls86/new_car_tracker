"""Prometheus metrics for cartracker observability."""
from .duckdb_gauges import update_duckdb_metrics

__all__ = ["update_duckdb_metrics"]
