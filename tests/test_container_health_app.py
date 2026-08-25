"""Plan 142 read-only one-shot execution evidence endpoint."""

from container_health import app


def test_oneoff_endpoint_reads_existing_docker_authority(mocker):
    inspection = {
        "Id": "container-id",
        "Config": {
            "Labels": {
                "com.docker.compose.project": "cartracker",
                "com.docker.compose.service": "snapshot-worker",
                "com.docker.compose.oneoff": "True",
            }
        },
        "State": {"Status": "running", "StartedAt": "2026-08-25T04:00:00Z"},
    }
    inspect = mocker.patch.object(
        app.DOCKER_API, "inspect_project_containers", return_value=[inspection]
    )

    result = app.active_oneoff_processes()

    assert result["known"] is True
    assert result["active_processes"] == 1
    assert result["processes"][0]["service"] == "snapshot-worker"
    inspect.assert_called_once_with("cartracker")
