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
    added_during_job,
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


def _image(tags=(), digests=(), identity="0"):
    return {
        "Id": "sha256:" + identity * 64,
        "RepoTags": list(tags),
        "RepoDigests": list(digests),
    }


# What GitHub's runner image carried before any job began, in the shape a
# preload arrives: pulled, tagged `latest`, and defined nowhere here.
PRELOADED = _image(
    ["ghcr.io/github/github-mcp-server:latest"],
    ["ghcr.io/github/github-mcp-server@sha256:" + "e" * 64],
    identity="e",
)


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


class TestAddedDuringJob:
    def test_an_image_the_runner_held_before_the_job_is_not_judged(self):
        pulled = _image(["postgres:16"], ["postgres@sha256:" + "b" * 64], identity="b")
        record = {"baseline": [PRELOADED["Id"]], "images": [PRELOADED, pulled]}
        assert added_during_job(record) == [pulled]

    def test_a_preloaded_tag_pulled_again_arrives_under_a_new_id_and_is_judged(self):
        repulled = dict(PRELOADED, Id="sha256:" + "f" * 64)
        record = {"baseline": [PRELOADED["Id"]], "images": [repulled]}
        assert added_during_job(record) == [repulled]


def test_every_ledgered_image_is_one_the_gate_accepts():
    """The gate and the rule read the same references: an image the rule lets
    through by its ledger is one the gate lets through by name."""
    defined = gate.defined_references()
    assert {normalise(entry) for entry in UNOWNED_IMAGE_LEDGER} <= defined


class TestRecorder:
    """What the recorder writes is what the gate reads."""

    @pytest.fixture
    def runner(self, tmp_path, monkeypatch, mocker):
        """A fake daemon whose image list the test sets, one inspect per id."""
        held: list[dict] = []
        calls: list[tuple] = []

        def docker(*args):
            calls.append(args)
            if args[:2] == ("image", "ls"):
                # `ls --all` lists an image once per tag, so the recorder must dedupe.
                return "".join(f"{image['Id']}\n{image['Id']}\n" for image in held)
            return json.dumps([dict(image, Size=1) for image in held])

        mocker.patch.object(recorder, "_docker", docker)
        monkeypatch.setenv("GITHUB_JOB", "docker-build")
        monkeypatch.setenv("CI_IMAGE_RECORDS", str(tmp_path / "ci-image-records-docker-build"))
        return held, calls

    def test_the_end_record_carries_the_baseline_taken_before_the_job(self, tmp_path, runner):
        held, calls = runner
        held.append(PRELOADED)
        assert recorder.main(["--baseline"]) == 0

        pulled = _image(["postgres:16"], ["postgres@sha256:" + "b" * 64], identity="b")
        held.append(pulled)
        assert recorder.main([]) == 0

        assert calls[-1] == ("image", "inspect", *sorted([PRELOADED["Id"], pulled["Id"]]))
        record = load_records(tmp_path)["docker-build"]
        assert record == {
            "job": "docker-build",
            "baseline": [PRELOADED["Id"]],
            "images": [PRELOADED, pulled],
        }
        assert added_during_job(record) == [pulled]

    def test_an_end_record_with_no_baseline_says_so(self, tmp_path, runner):
        held, _ = runner
        held.append(PRELOADED)
        assert recorder.main([]) == 0
        assert load_records(tmp_path)["docker-build"]["baseline"] is None


class TestMain:
    @pytest.fixture(autouse=True)
    def _defined(self, mocker):
        mocker.patch.object(gate, "defined_references", return_value=DEFINED)

    def _record(self, root, job, images, baseline=()):
        directory = root / f"ci-image-records-{job}"
        directory.mkdir()
        record = {
            "job": job,
            "baseline": None if baseline is None else list(baseline),
            "images": images,
        }
        (directory / f"images-{job}.json").write_text(json.dumps(record), encoding="utf-8")

    def test_an_image_preloaded_on_the_runner_is_not_judged(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NEEDS", json.dumps({"lint": {"result": "success"}}))
        self._record(tmp_path, "lint", [PRELOADED], baseline=[PRELOADED["Id"]])
        assert gate.main(["--records", str(tmp_path)]) == 0

    def test_fails_on_a_record_with_no_baseline(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("NEEDS", json.dumps({"lint": {"result": "success"}}))
        self._record(tmp_path, "lint", [], baseline=None)
        assert gate.main(["--records", str(tmp_path)]) == 1
        assert "no baseline" in capsys.readouterr().out

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
