"""The cadence names ``dbt/selectors.yml`` declares.

Plan 162 Stage AA. Its own module rather than a function in ``app.py``, for a
reason CI found: `tests/integration/airflow/` runs in an isolated venv holding
Airflow and nothing else -- the same isolation `dbt_runner`'s own image has --
so a test there asserting the DAG names a real cadence imported `app.py` and
died on `prometheus_client`. The question "is `hourly_core` a selector dbt
knows about" needs `yaml` and a path, not a web framework.

Airflow ships PyYAML, so this module imports cleanly in that venv while
`app.py` does not, and the DAG's cadence name stays checked against the file
dbt actually reads instead of against a second copy of the same four strings.
"""
from __future__ import annotations

import os

import yaml


def declared_selectors() -> set[str]:
    """The selector names ``selectors.yml`` declares, read from the file.

    Read rather than restated: a list of names here would be a second copy of
    the same four strings, free to drift from the file dbt actually reads --
    and the panel offering a selector dbt does not have is exactly the class of
    defect this plan exists for.

    ``selectors.yml`` sits beside this service at runtime because
    ``dbt_runner/Dockerfile`` copies ``dbt/`` to the working directory, which is
    the same reason ``dbt build`` finds it.
    """
    path = os.path.join(os.getcwd(), "selectors.yml")
    try:
        with open(path, encoding="utf-8") as handle:
            declared = yaml.safe_load(handle) or {}
    except FileNotFoundError:
        return set()
    return {
        entry["name"] for entry in (declared.get("selectors") or [])
        if isinstance(entry, dict) and entry.get("name")
    }
