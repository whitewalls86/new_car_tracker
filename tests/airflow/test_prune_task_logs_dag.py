import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "airflow" / "dags"))

from prune_task_logs import prune_task_logs  # noqa: E402


def _log(root: Path, dag: str, run: str, age_days: int, now: datetime) -> Path:
    path = root / f"dag_id={dag}" / f"run_id={run}" / "task_id=task" / "attempt=1.log"
    path.parent.mkdir(parents=True)
    path.write_text("log", encoding="utf-8")
    timestamp = (now - timedelta(days=age_days)).timestamp()
    path.touch()
    import os
    os.utime(path, (timestamp, timestamp))
    return path


def test_prunes_old_run_trees_and_keeps_recent_ones(tmp_path):
    now = datetime(2026, 8, 17, tzinfo=timezone.utc)
    old = _log(tmp_path, "old", "scheduled__old", 31, now)
    recent = _log(tmp_path, "recent", "scheduled__recent", 29, now)

    result = prune_task_logs(log_root=tmp_path, now=now)

    assert result == {"examined": 2, "deleted": 1}
    assert not old.parent.parent.exists()
    assert recent.exists()


def test_ignores_non_airflow_layout_and_symlinked_run(tmp_path, mocker):
    now = datetime(2026, 8, 17, tzinfo=timezone.utc)
    unrelated = tmp_path / "scheduler" / "latest.log"
    unrelated.parent.mkdir()
    unrelated.write_text("keep", encoding="utf-8")
    link = tmp_path / "dag_id=linked" / "run_id=linked"
    link.mkdir(parents=True)

    # Creating a real symlink requires elevated privileges on Windows. The
    # behavior owned here is our refusal to traverse a run directory that the
    # filesystem classifies as a symlink, not pathlib's OS integration.
    real_is_symlink = Path.is_symlink

    def _is_symlink(path):
        return path == link or real_is_symlink(path)

    mocker.patch.object(Path, "is_symlink", autospec=True, side_effect=_is_symlink)

    result = prune_task_logs(log_root=tmp_path, now=now)

    assert result == {"examined": 0, "deleted": 0}
    assert unrelated.exists()
    assert link.exists()


def test_rejects_nonpositive_retention(tmp_path):
    with pytest.raises(ValueError, match="positive"):
        prune_task_logs(log_root=tmp_path, retention_days=0)
