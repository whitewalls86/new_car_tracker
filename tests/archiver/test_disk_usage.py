"""Plan 135 Stage 4: the disk watchlist processor.

The properties worth protecting here are the ones that fail silently in
production: a partially-written .prom, a carried-forward value that looks
fresh, and a walk that measures nothing but still writes a file.
"""
import os

import pytest

from archiver.processors import disk_usage


class TestWalkTiers:
    """The tier criterion, which is the assumption that failed in production.

    ``cartracker_airflow_logs`` was daily because 6.3 GiB reads as small. It
    holds ~1.2M inodes -- twice the whole root filesystem -- and ``du``'s cost
    is O(inodes), so it took 397s of a 456s walk. These tests exist so the next
    person to add a volume cannot classify it on size again without failing.
    """

    def test_airflow_logs_is_in_the_slow_tier(self):
        """The regression pin. Small on disk, enormous in inodes."""
        assert "cartracker_airflow_logs" in disk_usage.HIGH_INODE_VOLUMES
        assert "cartracker_airflow_logs" not in disk_usage.DAILY_VOLUMES

    def test_tier_membership_is_decided_by_inodes(self):
        """A volume may only be promoted to the slow tier on a recorded inode
        measurement. Without this, "it looks big" is enough again."""
        for volume in disk_usage.HIGH_INODE_VOLUMES:
            assert volume in disk_usage.MEASURED_INODES, (
                f"{volume} is walked weekly but no inode count justifies it; "
                "measure it with `find <path> | wc -l` and record the number"
            )
            assert disk_usage.MEASURED_INODES[volume] >= 1_000_000

    def test_the_tiers_are_disjoint(self):
        """A volume in both would be walked twice on the weekly run and
        double-counted in the stacked panel."""
        assert not set(disk_usage.DAILY_VOLUMES) & set(disk_usage.HIGH_INODE_VOLUMES)


class TestMeasurePath:
    def test_reports_physical_bytes(self, tmp_path, monkeypatch):
        calls = {}

        class _Proc:
            returncode = 0
            stdout = "4096\t/measure/root/usr\n"
            stderr = ""

        def fake_run(cmd, **kwargs):
            calls["cmd"] = cmd
            return _Proc()

        monkeypatch.setattr(disk_usage.subprocess, "run", fake_run)
        assert disk_usage.measure_path("/measure/root/usr") == {"bytes": 4096, "error": None}

    def test_uses_block_size_not_apparent_size(self, monkeypatch):
        """--block-size=1 is physical; -b would be apparent and would hide the
        per-object floor this whole plan exists to expose."""
        captured = {}

        class _Proc:
            returncode = 0
            stdout = "1\t/x\n"
            stderr = ""

        monkeypatch.setattr(
            disk_usage.subprocess, "run",
            lambda cmd, **kw: (captured.update(cmd=cmd), _Proc())[1],
        )
        disk_usage.measure_path("/x")
        assert "--block-size=1" in captured["cmd"]
        assert "-b" not in captured["cmd"]
        assert "-x" in captured["cmd"]

    def test_partial_total_is_kept(self, monkeypatch):
        """du exits non-zero when it cannot descend into some subtree but still
        prints a total for the rest. A partial number beats no series."""
        class _Proc:
            returncode = 1
            stdout = "512\t/x\n"
            stderr = "du: cannot read directory '/x/secret': Permission denied"

        monkeypatch.setattr(disk_usage.subprocess, "run", lambda cmd, **kw: _Proc())
        assert disk_usage.measure_path("/x")["bytes"] == 512

    def test_unparseable_output_is_an_error_not_a_zero(self, monkeypatch):
        """Reporting 0 for an unmeasurable path would read as 'this is empty'."""
        class _Proc:
            returncode = 1
            stdout = ""
            stderr = "du: cannot access '/nope': No such file or directory"

        monkeypatch.setattr(disk_usage.subprocess, "run", lambda cmd, **kw: _Proc())
        result = disk_usage.measure_path("/nope")
        assert result["bytes"] is None
        assert "No such file" in result["error"]

    def test_timeout_is_an_error_not_a_hang(self, monkeypatch):
        def boom(cmd, **kwargs):
            raise disk_usage.subprocess.TimeoutExpired(cmd, 5)

        monkeypatch.setattr(disk_usage.subprocess, "run", boom)
        result = disk_usage.measure_path("/slow", timeout=5)
        assert result["bytes"] is None
        assert "5s" in result["error"]


class TestRenderAndParse:
    def _readings(self):
        return [
            {"metric": disk_usage.PATH_METRIC, "target": "/usr",
             "bytes": 1024, "measured_at": 1_700_000_000.0,
             "carried_forward": False, "error": None},
            {"metric": disk_usage.VOLUME_METRIC, "target": "cartracker_parquet_data",
             "bytes": 2048, "measured_at": 1_600_000_000.0,
             "carried_forward": True, "error": None},
        ]

    def test_render_emits_help_and_type_once_per_family(self):
        text = disk_usage.render_prom(self._readings())
        for metric in (disk_usage.PATH_METRIC, disk_usage.VOLUME_METRIC,
                       disk_usage.MEASURED_AT_METRIC):
            assert text.count(f"# HELP {metric} ") == 1
            assert text.count(f"# TYPE {metric} gauge") == 1

    def test_render_round_trips_through_parse(self):
        parsed = disk_usage.parse_previous(disk_usage.render_prom(self._readings()))
        assert parsed[(disk_usage.PATH_METRIC, "/usr")] == 1024
        assert parsed[(disk_usage.VOLUME_METRIC, "cartracker_parquet_data")] == 2048
        assert parsed[(disk_usage.MEASURED_AT_METRIC, "/usr")] == 1_700_000_000

    def test_unmeasured_series_is_omitted_entirely(self):
        """No value is better than a zero -- a zero stacks as a real band."""
        readings = [{"metric": disk_usage.PATH_METRIC, "target": "/usr", "bytes": None,
                     "measured_at": None, "carried_forward": True, "error": "boom"}]
        text = disk_usage.render_prom(readings)
        assert "/usr" not in text

    def test_parse_survives_a_truncated_file(self):
        """A half-written previous file must not stop this run writing a good one."""
        parsed = disk_usage.parse_previous(
            "# HELP x y\ncartracker_path_bytes{disk=\"root\",path=\"/usr\"} 10\n"
            "cartracker_volume_bytes{volume=\"broke"
        )
        assert parsed == {(disk_usage.PATH_METRIC, "/usr"): 10}


class TestWriteTextfile:
    def test_write_is_atomic_and_leaves_no_temp_files(self, tmp_path):
        disk_usage.write_textfile(str(tmp_path), "hello\n")
        names = sorted(p.name for p in tmp_path.iterdir())
        assert names == [disk_usage.TEXTFILE_NAME]

    def test_written_file_is_readable_by_node_exporter(self, tmp_path):
        """NamedTemporaryFile creates 0600; node-exporter runs as nobody."""
        path = disk_usage.write_textfile(str(tmp_path), "hello\n")
        assert os.stat(path).st_mode & 0o044

    def test_overwrites_in_place(self, tmp_path):
        disk_usage.write_textfile(str(tmp_path), "first\n")
        disk_usage.write_textfile(str(tmp_path), "second\n")
        assert (tmp_path / disk_usage.TEXTFILE_NAME).read_text() == "second\n"

    def test_a_failed_write_leaves_no_partial_temp_file(self, tmp_path, monkeypatch):
        def boom(src, dst):
            raise OSError("disk full")

        monkeypatch.setattr(disk_usage.os, "replace", boom)
        with pytest.raises(OSError):
            disk_usage.write_textfile(str(tmp_path), "nope\n")
        assert list(tmp_path.iterdir()) == []


class TestRunDiskUsage:
    @pytest.fixture
    def measured(self, monkeypatch):
        """Every path measures to a stable byte count derived from its name."""
        monkeypatch.setattr(
            disk_usage, "measure_path",
            lambda path, timeout=None: {"bytes": len(path), "error": None},
        )

    def test_refuses_without_a_textfile_directory(self, monkeypatch):
        monkeypatch.delenv("DISK_USAGE_TEXTFILE_DIR", raising=False)
        with pytest.raises(RuntimeError, match="pack-worker"):
            disk_usage.run_disk_usage()

    def test_daily_run_skips_every_high_inode_volume(self, tmp_path, measured):
        result = disk_usage.run_disk_usage(include_slow=False, directory=str(tmp_path))
        published = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )
        for volume in disk_usage.HIGH_INODE_VOLUMES:
            assert (disk_usage.VOLUME_METRIC, volume) not in published
        assert result["measured"] == len(disk_usage.ROOT_PATHS) + len(disk_usage.DAILY_VOLUMES)

    def test_weekly_run_includes_every_high_inode_volume(self, tmp_path, measured):
        result = disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))
        published = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )
        for volume in disk_usage.HIGH_INODE_VOLUMES:
            assert (disk_usage.VOLUME_METRIC, volume) in published
        assert result["carried_forward"] == 0

    def test_slow_tier_values_carry_forward_between_weekly_walks(self, tmp_path, measured):
        """The point of the single-file design. Splitting this across two .prom
        files makes node-exporter reject one of them (node_exporter#1885)."""
        disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))
        weekly = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )

        result = disk_usage.run_disk_usage(include_slow=False, directory=str(tmp_path))
        daily = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )

        for volume in disk_usage.HIGH_INODE_VOLUMES:
            key = (disk_usage.VOLUME_METRIC, volume)
            assert daily[key] == weekly[key]
        assert result["carried_forward"] == len(disk_usage.HIGH_INODE_VOLUMES)

    def test_carried_value_keeps_its_original_timestamp(self, tmp_path, measured):
        """A carried value with a refreshed timestamp is a gauge that has gone
        stale in silence -- the exact failure this metric exists to expose."""
        disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))
        first = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )
        disk_usage.run_disk_usage(include_slow=False, directory=str(tmp_path))
        second = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )

        root = (disk_usage.MEASURED_AT_METRIC, "/usr")
        for volume in disk_usage.HIGH_INODE_VOLUMES:
            carried = (disk_usage.MEASURED_AT_METRIC, volume)
            assert second[carried] == first[carried], f"{volume} timestamp must not move"
        assert second[root] >= first[root], "freshly walked paths do move"

    def test_a_failed_measurement_carries_the_old_value_and_is_reported(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            disk_usage, "measure_path",
            lambda path, timeout=None: {"bytes": 500, "error": None},
        )
        disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))

        monkeypatch.setattr(
            disk_usage, "measure_path",
            lambda path, timeout=None: {"bytes": None, "error": "du blew up"},
        )
        result = disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))

        assert result["failed"] == len(disk_usage.ROOT_PATHS) + len(
            disk_usage.DAILY_VOLUMES
        ) + len(disk_usage.HIGH_INODE_VOLUMES)
        published = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )
        assert published[(disk_usage.PATH_METRIC, "/usr")] == 500

    def test_first_ever_run_publishes_nothing_it_could_not_measure(
        self, tmp_path, monkeypatch
    ):
        """No previous file and a failing du must not emit a zero."""
        monkeypatch.setattr(
            disk_usage, "measure_path",
            lambda path, timeout=None: {"bytes": None, "error": "not mounted"},
        )
        result = disk_usage.run_disk_usage(include_slow=True, directory=str(tmp_path))
        assert result["measured"] == 0
        assert sorted(result["unpublished"]) == sorted(
            list(disk_usage.ROOT_PATHS)
            + list(disk_usage.DAILY_VOLUMES)
            + list(disk_usage.HIGH_INODE_VOLUMES)
        )
        text = (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        assert disk_usage.parse_previous(text) == {}

    def test_root_paths_are_measured_under_the_mount_prefix(self, tmp_path, monkeypatch):
        seen = []
        monkeypatch.setattr(
            disk_usage, "measure_path",
            lambda path, timeout=None: (seen.append(path), {"bytes": 1, "error": None})[1],
        )
        disk_usage.run_disk_usage(
            include_slow=True, directory=str(tmp_path),
            root_prefix="/measure/root", volume_prefix="/measure/volumes",
        )
        assert "/measure/root/usr" in seen
        assert "/measure/volumes/cartracker_parquet_data" in seen
        # The label stays the real host path, not the container's view of it.
        published = disk_usage.parse_previous(
            (tmp_path / disk_usage.TEXTFILE_NAME).read_text()
        )
        assert (disk_usage.PATH_METRIC, "/usr") in published
