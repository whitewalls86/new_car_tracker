"""Connection boundary for the modeled analytics serving target."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

DEFAULT_ANALYTICS_BACKEND = "duckdb"
DEFAULT_ANALYTICS_PATH = "/data/analytics/analytics.duckdb"


def get_analytics_connection(
    path: str | Path | None = None,
    *,
    backend: str | None = None,
) -> Any:
    """Open the configured modeled-analytics database in read-only mode.

    Retry, locking, caching, and result-shaping policy deliberately remain with
    each caller.  This factory only centralizes backend and path selection.
    """
    selected_backend = backend or os.environ.get(
        "ANALYTICS_BACKEND", DEFAULT_ANALYTICS_BACKEND
    )
    if selected_backend != "duckdb":
        raise ValueError(f"Unsupported analytics backend: {selected_backend}")

    selected_path = path or os.environ.get("DUCKDB_PATH", DEFAULT_ANALYTICS_PATH)

    import duckdb

    return duckdb.connect(str(selected_path), read_only=True)
