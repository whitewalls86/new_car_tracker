"""Prometheus metrics for cartracker observability."""
from .analytics_gauges import update_analytics_metrics

__all__ = ["update_analytics_metrics"]
