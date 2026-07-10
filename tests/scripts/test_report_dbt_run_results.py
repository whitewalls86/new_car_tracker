"""Unit tests for scripts/report_dbt_run_results.py

Groups:
  A - extract_node_timings parsing
  B - filter_and_sort (resource type filter, sort order, limit)
  C - format_table rendering
  D - main() end-to-end against a real run_results.json fixture on disk
"""
from __future__ import annotations

import json

from scripts.report_dbt_run_results import (
    NodeTiming,
    extract_node_timings,
    filter_and_sort,
    format_table,
    main,
)


def _run_results(results):
    return {"metadata": {}, "results": results, "elapsed_time": 12.5, "args": {}}


def _result(unique_id, status="success", execution_time=1.0):
    return {"unique_id": unique_id, "status": status, "execution_time": execution_time}


# ── Group A: extract_node_timings parsing ───────────────────────────────────

class TestExtractNodeTimings:
    def test_splits_resource_type_and_name_from_unique_id(self):
        run_results = _run_results(
            [_result("model.cartracker.int_price_history", execution_time=3.5)]
        )
        [timing] = extract_node_timings(run_results)
        assert timing.resource_type == "model"
        assert timing.name == "int_price_history"
        assert timing.status == "success"
        assert timing.execution_time == 3.5

    def test_missing_execution_time_defaults_to_zero(self):
        run_results = _run_results([{"unique_id": "model.cartracker.foo", "status": "success"}])
        [timing] = extract_node_timings(run_results)
        assert timing.execution_time == 0.0

    def test_empty_results_returns_empty_list(self):
        assert extract_node_timings(_run_results([])) == []

    def test_operation_unique_id_gets_operation_resource_type(self):
        run_results = _run_results(
            [_result("operation.cartracker.cartracker-on-run-start-0")]
        )
        [timing] = extract_node_timings(run_results)
        assert timing.resource_type == "operation"


# ── Group B: filter_and_sort ─────────────────────────────────────────────────

class TestFilterAndSort:
    def _timings(self):
        return [
            NodeTiming("model.cartracker.a", "model", "a", "success", 1.0),
            NodeTiming("model.cartracker.b", "model", "b", "success", 5.0),
            NodeTiming("test.cartracker.c", "test", "c", "success", 3.0),
        ]

    def test_sorts_by_execution_time_descending(self):
        result = filter_and_sort(self._timings(), resource_type=None, limit=None)
        assert [t.name for t in result] == ["b", "c", "a"]

    def test_filters_by_resource_type(self):
        result = filter_and_sort(self._timings(), resource_type="model", limit=None)
        assert [t.name for t in result] == ["b", "a"]

    def test_limit_truncates_after_sort(self):
        result = filter_and_sort(self._timings(), resource_type=None, limit=1)
        assert [t.name for t in result] == ["b"]

    def test_no_resource_type_matches_returns_empty(self):
        result = filter_and_sort(self._timings(), resource_type="seed", limit=None)
        assert result == []


# ── Group C: format_table rendering ──────────────────────────────────────────

class TestFormatTable:
    def test_empty_timings_returns_placeholder(self):
        assert format_table([]) == "(no matching nodes)"

    def test_includes_name_status_and_execution_time(self):
        timings = [NodeTiming("model.cartracker.foo", "model", "foo", "success", 2.5)]
        table = format_table(timings)
        assert "foo" in table
        assert "success" in table
        assert "2.500s" in table

    def test_columns_align_to_widest_value(self):
        timings = [
            NodeTiming("model.cartracker.short", "model", "short", "success", 1.0),
            NodeTiming(
                "model.cartracker.a_much_longer_name",
                "model",
                "a_much_longer_name",
                "error",
                2.0,
            ),
        ]
        lines = format_table(timings).splitlines()
        # header + separator + 2 rows
        assert len(lines) == 4
        assert len(lines[2]) == len(lines[3])


# ── Group D: main() end-to-end ───────────────────────────────────────────────

class TestMainEndToEnd:
    def test_missing_file_returns_error_code(self, tmp_path, capsys):
        missing = tmp_path / "run_results.json"
        rc = main(["--path", str(missing)])
        assert rc == 1
        assert "not found" in capsys.readouterr().err

    def test_prints_sorted_model_table_and_writes_json_out(self, tmp_path, capsys):
        run_results = _run_results(
            [
                _result("model.cartracker.slow_model", execution_time=9.9),
                _result("model.cartracker.fast_model", execution_time=0.1),
                _result("test.cartracker.some_test", execution_time=100.0),
            ]
        )
        path = tmp_path / "run_results.json"
        path.write_text(json.dumps(run_results))
        json_out = tmp_path / "out.json"

        rc = main(["--path", str(path), "--json-out", str(json_out)])
        out = capsys.readouterr().out

        assert rc == 0
        # default resource-type filter is "model", so the slower test node is excluded
        assert "some_test" not in out
        assert out.index("slow_model") < out.index("fast_model")
        assert "total elapsed_time: 12.500s" in out

        written = json.loads(json_out.read_text())
        assert [row["name"] for row in written] == ["slow_model", "fast_model"]

    def test_empty_resource_type_includes_all_nodes(self, tmp_path, capsys):
        run_results = _run_results(
            [
                _result("model.cartracker.m", execution_time=1.0),
                _result("test.cartracker.t", execution_time=2.0),
            ]
        )
        path = tmp_path / "run_results.json"
        path.write_text(json.dumps(run_results))

        rc = main(["--path", str(path), "--resource-type", ""])
        out = capsys.readouterr().out

        assert rc == 0
        assert "m" in out and "t" in out
