"""Static Compose contract for Plan 131 Stage 5's pack-worker service."""
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent


def _compose():
    return yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


class TestPackWorkerComposeConfig:
    @staticmethod
    def _services():
        return _compose()["services"]

    def test_service_exists_and_reuses_the_archiver_image(self):
        services = self._services()
        worker = services["pack-worker"]
        archiver = services["archiver"]
        assert worker["build"] == archiver["build"]
        assert worker["image"] == archiver["image"] == "cartracker-archiver"
        assert worker["container_name"] == "cartracker-pack-worker"

    def test_service_is_long_running_internal_only(self):
        worker = self._services()["pack-worker"]
        assert worker["restart"] == "unless-stopped"
        assert "ports" not in worker
        assert worker["networks"] == ["cartracker-net"]

    def test_dependencies_match_the_archiver(self):
        services = self._services()
        assert services["pack-worker"]["depends_on"] == services["archiver"]["depends_on"]

    def test_worker_has_its_own_log_volume(self):
        doc = _compose()
        worker_mounts = doc["services"]["pack-worker"]["volumes"]
        assert "pack_worker_logs:/usr/app/logs" in worker_mounts
        assert "pack_worker_logs" in doc["volumes"]

    def test_worker_inherits_archiver_environment_plus_worker_overrides(self):
        """Every worker-only variable must be listed here deliberately, so the
        two services cannot drift apart by accident."""
        services = self._services()
        worker_env = dict(services["pack-worker"]["environment"])
        archiver_env = services["archiver"]["environment"]

        # Plan 131 D4: long pack/prune jobs run here, not on the archiver.
        assert worker_env.pop("ARCHIVER_ALLOW_PACK_JOBS") == "true"
        # Plan 135 Stage 4: only this service carries the host mounts.
        assert worker_env.pop("DISK_USAGE_TEXTFILE_DIR") == "/textfile"

        assert worker_env == archiver_env
        assert worker_env["PACK_BRONZE_DICT_ID"] == "${HTML_COMPRESSION_DICT_ID:-}"
        assert "ARCHIVER_ALLOW_PACK_JOBS" not in archiver_env
        assert "DISK_USAGE_TEXTFILE_DIR" not in archiver_env

    def test_promtail_mounts_worker_logs_read_only(self):
        mounts = self._services()["promtail"]["volumes"]
        assert "pack_worker_logs:/logs/pack-worker:ro" in mounts
