"""Layer 1 unit tests for the CI image provenance gate and the recorder it reads.

The gate's Docker half is every CI job's runner. What is tested here is the
decision logic that decides *whether* to fail, because a gate that cannot fail
is the thing this plan exists to stop shipping -- and the record format, which
the recorder writes and the gate reads, so the two cannot drift apart.

Plan 183 Stage D.
"""

from __future__ import annotations

import json

import pytest

from scripts import check_ci_image_provenance as gate
from scripts import record_ci_images as recorder
from scripts.check_ci_image_provenance import (
    load_records,
    missing_records,
    normalise,
    unaccounted,
)
from tests.rules.test_every_external_image_comes_from_a_registry_we_own import (
    UNOWNED_IMAGE_LEDGER,
)

OWNED_DIGEST = "sha256:" + "a" * 64
DEFINED = {
    normalise(reference)
    for reference in (
        "postgres:16",
        "grafana/promtail:3.5.8",
        f"ghcr.io/whitewalls86/minio:RELEASE@{OWNED_DIGEST}",
    )
}


def _image(tags=(), digests=()):
    return {"Id": "sha256:" + "0" * 64, "RepoTags": list(tags), "RepoDigests": list(digests)}


@pytest.mark.parametrize(
    ("written", "expected"),
    [
        ("postgres:16", "docker.io/library/postgres:16"),
        ("docker.io/library/postgres:16", "docker.io/library/postgres:16"),
        ("postgres", "docker.io/library/postgres:latest"),
        ("grafana/promtail:3.5.8", "docker.io/grafana/promtail:3.5.8"),
        ("quay.io/lakekeeper/catalog:v0.13.1", "quay.io/lakekeeper/catalog:v0.13.1"),
        ("localhost:5000/scratch", "localhost:5000/scratch:latest"),
        ("postgres@sha256:ab", "docker.io/library/postgres@sha256:ab"),
        ("ghcr.io/whitewalls86/minio:T@sha256:ab", "ghcr.io/whitewalls86/minio@sha256:ab"),
    ],
)
def test_normalise_spells_a_reference_the_way_its_registry_does(written, expected):
    assert normalise(written) == expected


class TestUnaccounted:
    def test_an_image_with_no_digest_was_built_here(self):
        assert unaccounted([_image(tags=["cartracker-scraper:latest"])], DEFINED) == []

    @pytest.mark.parametrize("tag", ["postgres:16", "docker.io/library/postgres:16"])
    def test_a_defined_tag_passes_in_either_spelling(self, tag):
        image = _image([tag], ["postgres@sha256:" + "b" * 64])
        assert unaccounted([image], DEFINED) == []

    def test_an_owned_image_pulled_by_digest_passes_without_a_tag(self):
        image = _image(digests=[f"ghcr.io/whitewalls86/minio@{OWNED_DIGEST}"])
        assert unaccounted([image], DEFINED) == []

    def test_an_owned_repository_at_another_digest_fails(self):
        image = _image(digests=["ghcr.io/whitewalls86/minio@sha256:" + "c" * 64])
        assert unaccounted([image], DEFINED) == [image]

    def test_an_image_nothing_defines_fails(self):
        image = _image(["alpine:latest"], ["alpine@sha256:" + "d" * 64])
        assert unaccounted([image], DEFINED) == [image]


class TestMissingRecords:
    def test_a_job_that_ran_and_recorded_nothing_is_missing(self):
        needs = {"lint": {"result": "success"}, "docker-build": {"result": "failure"}}
        assert missing_records(needs, {"lint"}) == ["docker-build"]

    def test_a_skipped_job_owes_no_record(self):
        assert missing_records({"docker-build": {"result": "skipped"}}, set()) == []


def test_every_ledgered_image_is_one_the_gate_accepts():
    """The gate and the rule read the same references: an image the rule lets
    through by its ledger is one the gate lets through by name."""
    defined = gate.defined_references()
    assert {normalise(entry) for entry in UNOWNED_IMAGE_LEDGER} <= defined


def test_what_the_recorder_writes_is_what_the_gate_reads(tmp_path, monkeypatch, mocker):
    identity = "sha256:" + "1" * 64
    inspected = [
        {
            "Id": identity,
            "RepoTags": ["postgres:16"],
            "RepoDigests": ["postgres@sha256:" + "b" * 64],
            "Size": 1,
        }
    ]
    calls = []

    def docker(*args):
        calls.append(args)
        # `ls --all` lists an image once per tag, so the recorder must dedupe.
        return f"{identity}\n{identity}\n" if args[:2] == ("image", "ls") else json.dumps(inspected)

    mocker.patch.object(recorder, "_docker", docker)
    monkeypatch.setenv("GITHUB_JOB", "docker-build")
    monkeypatch.setenv("CI_IMAGE_RECORDS", str(tmp_path / "ci-image-records-docker-build"))

    assert recorder.main() == 0
    assert calls[-1] == ("image", "inspect", identity)
    assert load_records(tmp_path) == {
        "docker-build": [
            {
                "Id": identity,
                "RepoTags": ["postgres:16"],
                "RepoDigests": inspected[0]["RepoDigests"],
            }
        ]
    }


class TestMain:
    @pytest.fixture(autouse=True)
    def _defined(self, mocker):
        mocker.patch.object(gate, "defined_references", return_value=DEFINED)

    def _record(self, root, job, images):
        directory = root / f"ci-image-records-{job}"
        directory.mkdir()
        (directory / f"images-{job}.json").write_text(
            json.dumps({"job": job, "images": images}), encoding="utf-8"
        )

    def test_passes_when_every_job_that_ran_recorded_only_accounted_images(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv(
            "NEEDS",
            json.dumps({"docker-build": {"result": "success"}, "lint": {"result": "skipped"}}),
        )
        self._record(tmp_path, "docker-build", [_image(["cartracker-scraper:latest"])])
        assert gate.main(["--records", str(tmp_path)]) == 0

    def test_fails_on_a_pull_nothing_defines(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv(
            "NEEDS", json.dumps({"container-health-contract": {"result": "success"}})
        )
        self._record(
            tmp_path,
            "container-health-contract",
            [_image(["alpine:latest"], ["alpine@sha256:" + "d" * 64])],
        )
        assert gate.main(["--records", str(tmp_path)]) == 1
        assert "alpine:latest" in capsys.readouterr().out

    def test_fails_when_a_job_that_ran_left_no_record(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("NEEDS", json.dumps({"docker-build": {"result": "failure"}}))
        assert gate.main(["--records", str(tmp_path)]) == 1
        assert "docker-build" in capsys.readouterr().out

    def test_fails_without_needs(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NEEDS", raising=False)
        assert gate.main(["--records", str(tmp_path)]) == 1
