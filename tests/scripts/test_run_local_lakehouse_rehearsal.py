"""Unit tests for scripts/run_local_lakehouse_rehearsal.py (Plan 112 Gate A4).

Planning/command-construction only -- no Docker, no network. The execute()
tests inject a recording runner, a fake downloader, and a fake seeded
checker.
"""
from __future__ import annotations

import inspect
import os
import time

import pytest

import scripts.run_local_lakehouse_rehearsal as runner_mod
from scripts.run_local_lakehouse_rehearsal import (
    LOCAL_MINIO_ENV,
    _parse_args,
    build_dbt_run_command,
    build_seed_command,
    execute,
    find_newest_snapshot,
    resolve_token,
)


class RecordingRunner:
    """Records every (cmd, env) pair; returns the configured exit codes."""

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on or ()

    def __call__(self, cmd, env=None):
        self.calls.append((cmd, env))
        return 1 if any(marker in " ".join(cmd) for marker in self.fail_on) else 0

    def commands(self):
        return [" ".join(cmd) for cmd, _ in self.calls]


def _make_snapshot(snapshot_dir, snapshot_id, mtime=None):
    d = snapshot_dir / snapshot_id
    d.mkdir(parents=True)
    archive = d / "snapshot.tar.zst"
    archive.write_bytes(b"zst")
    (d / "manifest.json").write_text("{}")
    if mtime is not None:
        os.utime(archive, (mtime, mtime))
    return archive


def _args(tmp_path, *extra):
    return _parse_args([
        "--snapshot-dir", str(tmp_path / "snapshots"),
        "--analytics-dir", str(tmp_path / "analytics"),
        *extra,
    ])


def _run(args, runner, downloaded=None, seeded=True):
    return execute(
        args,
        runner=runner,
        downloader=lambda a, token: downloaded,
        seeded_checker=lambda endpoint: seeded,
    )


# ── newest-snapshot selection ─────────────────────────────────────────────

class TestFindNewestSnapshot:
    def test_returns_none_for_missing_or_empty_dir(self, tmp_path):
        assert find_newest_snapshot(tmp_path / "nope") is None
        assert find_newest_snapshot(tmp_path) is None

    def test_picks_newest_by_mtime(self, tmp_path):
        now = time.time()
        _make_snapshot(tmp_path, "adaptive-refresh-old", mtime=now - 100)
        newest = _make_snapshot(tmp_path, "adaptive-refresh-new", mtime=now)
        assert find_newest_snapshot(tmp_path) == newest

    def test_ignores_dirs_without_archive(self, tmp_path):
        (tmp_path / "half-downloaded").mkdir()
        (tmp_path / "half-downloaded" / "manifest.json").write_text("{}")
        assert find_newest_snapshot(tmp_path) is None


# ── token/env resolution ──────────────────────────────────────────────────

class TestResolveToken:
    def test_cli_token_wins(self):
        assert resolve_token("cli-tok", {"CARTRACKER_SNAPSHOT_TOKEN": "env-tok"}) == "cli-tok"

    def test_falls_back_to_env(self):
        assert resolve_token(None, {"CARTRACKER_SNAPSHOT_TOKEN": "env-tok"}) == "env-tok"

    def test_none_when_absent(self):
        assert resolve_token(None, {}) is None

    def test_refresh_without_token_fails_with_guidance(self, tmp_path, monkeypatch, capsys):
        monkeypatch.delenv("CARTRACKER_SNAPSHOT_TOKEN", raising=False)
        args = _args(tmp_path, "--refresh-seed-data")
        runner = RecordingRunner()
        assert _run(args, runner) == 1
        assert "CARTRACKER_SNAPSHOT_TOKEN" in capsys.readouterr().err


# ── seed command construction ─────────────────────────────────────────────

class TestSeedCommand:
    def test_clear_prefixes_only_when_requested(self, tmp_path):
        archive = tmp_path / "snapshot.tar.zst"
        with_clear = build_seed_command(archive, "http://localhost:19000", clear_prefixes=True)
        without = build_seed_command(archive, "http://localhost:19000", clear_prefixes=False)
        assert "--clear-prefixes" in with_clear
        assert "--clear-prefixes" not in without
        assert "http://localhost:19000" in with_clear

    def test_execute_passes_local_minio_creds_to_seed_env(self, tmp_path):
        archive = _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path, "--reseed-only")
        runner = RecordingRunner()
        assert _run(args, runner, seeded=True) == 0

        seed_calls = [
            (cmd, env) for cmd, env in runner.calls if "scripts.seed_lake_snapshot" in cmd
        ]
        assert len(seed_calls) == 1
        cmd, env = seed_calls[0]
        assert str(archive) in cmd
        assert "--clear-prefixes" in cmd
        assert env["MINIO_ROOT_USER"] == "cartracker"
        assert env["MINIO_ROOT_PASSWORD"] == "cartracker123"

    def test_seed_skipped_when_already_seeded_and_not_forced(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path)
        runner = RecordingRunner()
        assert _run(args, runner, seeded=True) == 0
        assert not any("seed_lake_snapshot" in c for c in runner.commands())

    def test_seed_runs_without_clear_when_unseeded_and_not_forced(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path)
        runner = RecordingRunner()
        assert _run(args, runner, seeded=False) == 0
        seed = next(cmd for cmd, _ in runner.calls if "scripts.seed_lake_snapshot" in cmd)
        assert "--clear-prefixes" not in seed


# ── dbt docker command construction ───────────────────────────────────────

class TestDbtCommand:
    def test_uses_local_network_and_targeted_select(self, tmp_path):
        cmd = build_dbt_run_command(tmp_path, "local-lakehouse", "cartracker-dbt-local")
        joined = " ".join(cmd)
        assert "--network local-lakehouse_cartracker-net" in joined
        assert "--select +int_listing_volatility_features" in joined
        assert "--target duckdb" in joined
        assert "--full-refresh" in joined
        assert "MINIO_ROOT_USER=cartracker" in joined
        assert "MINIO_ROOT_PASSWORD=cartracker123" in joined
        assert "POSTGRES_URL=postgresql://unused:unused@localhost:5432/unused" in joined
        assert f"{tmp_path.resolve()}:/out" in joined
        assert "cartracker-dbt-local" in cmd

    def test_duckdb_build_skipped_when_cached(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        (analytics / "analytics.duckdb").write_bytes(b"duck")
        args = _args(tmp_path)
        runner = RecordingRunner()
        assert _run(args, runner) == 0
        assert not any("cartracker-dbt-local" in c for c in runner.commands())

    def test_rebuild_duckdb_forces_dbt_build(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        analytics = tmp_path / "analytics"
        analytics.mkdir()
        (analytics / "analytics.duckdb").write_bytes(b"duck")
        args = _args(tmp_path, "--rebuild-duckdb")
        runner = RecordingRunner()
        assert _run(args, runner) == 0
        commands = runner.commands()
        assert any("docker build -f dbt/Dockerfile" in c for c in commands)
        assert any("+int_listing_volatility_features" in c for c in commands)

    def test_no_build_images_skips_image_builds_but_not_dbt_run(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path, "--no-build-images")
        runner = RecordingRunner()
        assert _run(args, runner) == 0
        commands = runner.commands()
        assert not any("docker build -f dbt/Dockerfile" in c for c in commands)
        assert not any("build lakehouse-worker" in c for c in commands)
        assert any("+int_listing_volatility_features" in c for c in commands)


# ── skip flags / step planning ────────────────────────────────────────────

class TestStepPlanning:
    def test_full_run_hits_every_step(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path)
        runner = RecordingRunner()
        assert _run(args, runner, seeded=False) == 0
        commands = runner.commands()
        assert any("up -d minio lakekeeper-postgres lakekeeper" in c for c in commands)
        assert any("build lakehouse-worker" in c for c in commands)
        assert any("scripts.seed_lake_snapshot" in c for c in commands)
        assert any("scripts.register_lakehouse_warehouse" in c for c in commands)
        assert any("scripts.preflight_local_lakehouse_snapshot" in c for c in commands)
        assert any("scripts.spike_iceberg_lakehouse roundtrip" in c for c in commands)
        assert any(
            "scripts.export_volatility_features_to_iceberg rehearsal" in c for c in commands
        )

    def test_skip_a2_and_a3(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path, "--skip-a2", "--skip-a3")
        runner = RecordingRunner()
        assert _run(args, runner) == 0
        commands = runner.commands()
        assert not any("spike_iceberg_lakehouse" in c for c in commands)
        assert not any("export_volatility_features_to_iceberg" in c for c in commands)
        # earlier steps still run
        assert any("scripts.preflight_local_lakehouse_snapshot" in c for c in commands)

    def test_keep_iceberg_table_passes_keep(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path, "--keep-iceberg-table")
        runner = RecordingRunner()
        assert _run(args, runner) == 0
        a3 = next(
            cmd for cmd, _ in runner.calls
            if "scripts.export_volatility_features_to_iceberg" in cmd
        )
        assert a3[-2:] == ["rehearsal", "--keep"]

    def test_refresh_downloads_and_clears(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CARTRACKER_SNAPSHOT_TOKEN", "tok")
        archive = _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-dl")
        args = _args(tmp_path, "--refresh-seed-data")
        runner = RecordingRunner()
        assert _run(args, runner, downloaded=archive) == 0
        seed = next(cmd for cmd, _ in runner.calls if "scripts.seed_lake_snapshot" in cmd)
        assert str(archive) in seed
        assert "--clear-prefixes" in seed
        # refresh also rebuilds the DuckDB even though nothing is missing
        assert any("+int_listing_volatility_features" in c for c in runner.commands())

    def test_failing_step_returns_nonzero_and_stops(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path)
        runner = RecordingRunner(fail_on=("preflight_local_lakehouse_snapshot",))
        assert _run(args, runner) != 0
        assert not any("spike_iceberg_lakehouse" in c for c in runner.commands())

    def test_missing_snapshot_fails_with_guidance(self, tmp_path, capsys):
        args = _args(tmp_path)
        runner = RecordingRunner()
        assert _run(args, runner) == 1
        assert "--refresh-seed-data" in capsys.readouterr().err

    def test_snapshot_path_and_refresh_are_mutually_exclusive(self, tmp_path):
        with pytest.raises(SystemExit):
            _args(tmp_path, "--refresh-seed-data", "--snapshot-path", "x.tar.zst")

    def test_refresh_and_reseed_only_are_mutually_exclusive(self, tmp_path):
        with pytest.raises(SystemExit):
            _args(tmp_path, "--refresh-seed-data", "--reseed-only")

    def test_explicit_snapshot_path_is_used(self, tmp_path):
        archive = _make_snapshot(tmp_path / "elsewhere", "manual")
        args = _args(tmp_path, "--snapshot-path", str(archive))
        runner = RecordingRunner()
        assert _run(args, runner, seeded=False) == 0
        seed = next(cmd for cmd, _ in runner.calls if "scripts.seed_lake_snapshot" in cmd)
        assert str(archive) in seed


# ── safety invariants ─────────────────────────────────────────────────────

class TestSafety:
    def test_no_shell_true_anywhere(self):
        source = inspect.getsource(runner_mod)
        assert "shell=True" not in source

    def test_no_destructive_compose_commands(self, tmp_path):
        _make_snapshot(tmp_path / "snapshots", "adaptive-refresh-x")
        args = _args(tmp_path)
        runner = RecordingRunner()
        _run(args, runner, seeded=False)
        for c in runner.commands():
            assert " down" not in c
            assert " -v " not in c or ":/out" in c  # only the dbt bind mount uses -v

    def test_local_minio_env_matches_local_compose_defaults(self):
        assert LOCAL_MINIO_ENV == {
            "MINIO_ROOT_USER": "cartracker",
            "MINIO_ROOT_PASSWORD": "cartracker123",
        }
