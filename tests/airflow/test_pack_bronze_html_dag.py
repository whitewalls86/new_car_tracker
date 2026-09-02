"""Behavior tests for the Plan 131 monthly pack/prune/verify DAG."""

import importlib.util
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).parents[2]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"


def _load_dag_module():
    dags_dir = str(DAGS_DIR)
    added = dags_dir not in sys.path
    if added:
        sys.path.insert(0, dags_dir)
    try:
        spec = importlib.util.spec_from_file_location(
            "pack_bronze_html", DAGS_DIR / "pack_bronze_html.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if added:
            sys.path.remove(dags_dir)


@pytest.fixture(scope="module")
def dag_module():
    return _load_dag_module()


def _context(*, xcom_result=None, conf=None, params=None):
    ti = MagicMock()
    ti.xcom_pull.return_value = xcom_result
    dag_run = MagicMock()
    dag_run.conf = conf or {}
    return {"ti": ti, "dag_run": dag_run, "params": params or {}}


def _fake_sensors(result):
    post_json = MagicMock(return_value=result)
    error_type = type("JsonPostError", (Exception,), {})
    return SimpleNamespace(post_json=post_json, JsonPostError=error_type), post_json


def _pack_result(**bucket_overrides):
    bucket = {
        "year": 2026,
        "month": 7,
        "orphan_packs": [],
        "stopped_at_max_packs": False,
        "stopped_for_deploy": False,
        "error": None,
    }
    bucket.update(bucket_overrides)
    return {
        "artifact_type": "detail_page",
        "error": None,
        "stopped_for_deploy": False,
        "read_failures": 0,
        "buckets": [bucket],
    }


def test_default_lifecycle_params_are_uncapped_and_apply(dag_module):
    assert dag_module.build_lifecycle_params({}) == dag_module.DEFAULT_PARAMS
    assert dag_module.DEFAULT_PARAMS["max_buckets"] == 1
    assert dag_module.DEFAULT_PARAMS["max_packs"] == 0
    assert dag_module.DEFAULT_PARAMS["prune_max_objects"] == 0
    assert dag_module.DEFAULT_PARAMS["prune_max_packs"] == 0
    assert dag_module.DEFAULT_PARAMS["apply"] is True


def test_lifecycle_param_overrides_are_allow_listed(dag_module):
    params = dag_module.build_lifecycle_params(
        {"apply": False, "max_packs": 2, "unknown_option": True}
    )
    assert params["apply"] is False
    assert params["max_packs"] == 2
    assert "unknown_option" not in params


def test_check_pack_result_returns_the_first_bucket(dag_module):
    result = _pack_result()
    result["buckets"].append({"year": 2026, "month": 8})

    assert dag_module.check_pack_result(result) == {
        "artifact_type": "detail_page",
        "year": 2026,
        "month": 7,
    }


@pytest.mark.parametrize(
    "result",
    [
        {"ok": True, "skipped": True},
        {"error": None, "stopped_for_deploy": False, "buckets": []},
    ],
)
def test_check_pack_result_short_circuits_without_a_target(dag_module, result):
    assert dag_module.check_pack_result(result) is None


@pytest.mark.parametrize(
    "result",
    [
        {"stopped_for_deploy": True},
        _pack_result(stopped_for_deploy=True),
    ],
)
def test_check_pack_result_treats_deploy_stop_as_retryable(dag_module, result):
    with pytest.raises(RuntimeError, match="stopped for deploy"):
        dag_module.check_pack_result(result)


@pytest.mark.parametrize(
    "result,match",
    [
        ({"error": "disk full"}, "disk full"),
        (_pack_result(error="bucket exploded"), "bucket exploded"),
        ({"error": None, "buckets": [{}]}, "missing year/month"),
    ],
)
def test_check_pack_result_rejects_malformed_or_failed_results(dag_module, result, match):
    with pytest.raises(RuntimeError, match=match):
        dag_module.check_pack_result(result)


def test_check_pack_result_warns_for_safe_orphan_cap_and_read_conditions(
    dag_module, caplog
):
    result = _pack_result(
        orphan_packs=["pack-00007.zpack"], stopped_at_max_packs=True
    )
    result["read_failures"] = 2

    with caplog.at_level(logging.WARNING):
        dag_module.check_pack_result(result)

    assert "left unpacked" in caplog.text
    assert "safe orphan" in caplog.text
    assert "pack cap" in caplog.text


def test_check_prune_result_accepts_a_drained_month(dag_module):
    dag_module.check_prune_result(
        {"error": None, "objects_refused": 0, "objects_deleted": 0, "capped": False}
    )


def test_check_prune_result_accepts_a_single_flight_skip(dag_module):
    dag_module.check_prune_result({"ok": True, "skipped": True})


@pytest.mark.parametrize(
    "result,match",
    [
        ({"stopped_for_deploy": True}, "stopped for deploy"),
        ({"error": "listing failed"}, "listing failed"),
        ({"error": None, "objects_refused": 7}, "7 object"),
    ],
)
def test_check_prune_result_raises_on_retry_or_safety_failures(dag_module, result, match):
    with pytest.raises(RuntimeError, match=match):
        dag_module.check_prune_result(result)


def test_check_prune_result_warns_instead_of_failing_for_orphans_and_caps(
    dag_module, caplog
):
    with caplog.at_level(logging.WARNING):
        dag_module.check_prune_result(
            {
                "error": None,
                "objects_refused": 0,
                "orphan_packs": ["pack-00008.zpack"],
                "capped": True,
            }
        )

    assert "safe orphan" in caplog.text
    assert "stopped at a cap" in caplog.text


def test_run_prune_does_not_post_when_pack_was_skipped(dag_module):
    context = _context(xcom_result={"ok": True, "skipped": True})
    result = dag_module._run_prune(**context)

    assert result["skipped"] is True
    assert "pack did not select" in result["reason"]


def test_run_pack_targets_the_worker_and_honors_conf_overrides(dag_module, monkeypatch):
    context = _context(
        conf={"max_packs": 2},
        params=dag_module.DEFAULT_PARAMS,
    )
    sensors, post = _fake_sensors(_pack_result())
    monkeypatch.setitem(sys.modules, "sensors", sensors)

    dag_module._run_pack(**context)

    post.assert_called_once_with(
        "http://pack-worker:8001/pack/bronze/run",
        payload={
            "artifact_type": "detail_page",
            "apply": True,
            "max_buckets": 1,
            "max_packs": 2,
        },
        timeout=43_200,
    )


def test_run_prune_derives_year_and_month_from_pack_result(dag_module, monkeypatch):
    context = _context(xcom_result=_pack_result(), params=dag_module.DEFAULT_PARAMS)
    clean_prune = {
        "artifact_type": "detail_page",
        "year": 2026,
        "month": 7,
        "error": None,
        "objects_refused": 0,
        "capped": False,
    }
    sensors, post = _fake_sensors(clean_prune)
    monkeypatch.setitem(sys.modules, "sensors", sensors)

    dag_module._run_prune(**context)

    post.assert_called_once_with(
        "http://pack-worker:8001/pack/bronze/prune",
        payload={
            "artifact_type": "detail_page",
            "year": 2026,
            "month": 7,
            "apply": True,
            "max_objects": 0,
            "max_packs": 0,
        },
        timeout=43_200,
    )


def test_run_verify_calls_the_read_path_canary_after_prune(dag_module, monkeypatch):
    prune_result = {"artifact_type": "detail_page", "year": 2026, "month": 7}
    context = _context(xcom_result=prune_result, params=dag_module.DEFAULT_PARAMS)
    clean_verify = {"verified": 5, "failed": 0}
    sensors, post = _fake_sensors(clean_verify)
    monkeypatch.setitem(sys.modules, "sensors", sensors)

    dag_module._run_verify(**context)

    post.assert_called_once_with(
        "http://pack-worker:8001/pack/bronze/verify",
        payload={"artifact_type": "detail_page", "year": 2026, "month": 7},
        timeout=3_600,
    )


@pytest.mark.parametrize(
    "result,match",
    [
        ({"verified": 4, "failed": 1}, "1 sampled"),
        ({"verified": 0, "failed": 0}, "verified no sampled"),
    ],
)
def test_check_verify_result_enforces_the_canary_contract(dag_module, result, match):
    with pytest.raises(RuntimeError, match=match):
        dag_module.check_verify_result(result)


def test_check_verify_result_accepts_a_clean_canary(dag_module):
    dag_module.check_verify_result({"verified": 5, "failed": 0})
