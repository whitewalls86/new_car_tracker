"""Layer 1 unit tests for the service OpenAPI contract generator.

The script's job is to fail when a service stops serving what its committed
contract says it serves. Importing all six apps needs every service's
dependencies installed together, which is the `Service OpenAPI contracts
(generated and diffed)` CI job's business; what is tested here is the decision
logic that decides *whether* to fail, because a gate that cannot fail is the
thing this plan exists to stop shipping.

The sibling `tests/scripts/test_verify_container_health_docker_contract.py`
draws the line in the same place, for the same reason.

Plan 162 Stage Z.
"""
from __future__ import annotations

import json

import pytest

from scripts import generate_service_contracts as generator
from scripts.generate_service_contracts import (
    committed_path,
    generate,
    main,
    service_apps,
)


def test_the_service_set_is_derived_and_holds_every_fastapi_app():
    """A new service arrives with no artifact and fails, rather than silently.

    Derived from the tree rather than listed, so this asserts membership and
    not a count: adding a seventh service should make the gate demand a seventh
    contract on its own, which a hardcoded list here would quietly excuse.
    """
    found = set(service_apps())
    assert {
        "archiver",
        "container_health",
        "dbt_runner",
        "ops",
        "processing",
        "scraper",
    } <= found


def test_dashboard_and_tests_are_not_services_with_contracts():
    """`dashboard` has an `app.py` and no routing table -- Streamlit owns its
    URLs -- and `tests` is the suite rather than a service. Neither serves a
    schema, so neither owes an artifact. This is a finding of the derivation,
    not an exemption granted to it."""
    found = service_apps()
    assert "dashboard" not in found
    assert "tests" not in found


def test_an_empty_service_set_fails_rather_than_passing_vacuously(mocker):
    """G1's failure in miniature: the gate passes because nothing ran.

    A repository whose services all stopped exposing apps is a catastrophe, not
    a clean run, so the derivation refuses to return nothing.
    """
    mocker.patch.object(
        generator, "REPO_ROOT", mocker.Mock(glob=mocker.Mock(return_value=[]))
    )
    with pytest.raises(AssertionError, match="pass by having nothing to check"):
        service_apps()


def test_the_generated_text_is_canonical_json(mocker):
    """`sort_keys` and an indent, and deliberately nothing else.

    Asserted on the text rather than the parsed object, because what the gate
    diffs is the text: an artifact that parsed equal but serialised differently
    would fail every run for no reason anyone could act on.
    """
    schema = {"paths": {"/b": {}, "/a": {}}, "openapi": "3.1.0"}
    mocker.patch.object(
        generator.subprocess,
        "run",
        return_value=mocker.Mock(returncode=0, stdout=json.dumps(schema), stderr=""),
    )
    text = generate("ops")

    assert text.endswith("\n"), "a committed text file ends with a newline"
    assert text == json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    assert text.index('"openapi"') < text.index('"paths"'), "keys are sorted"


def test_the_probe_runs_under_a_fixed_hash_seed(mocker):
    """The bug that made the first `--check` fail against untouched code.

    Six `ops` handlers take `methods=["GET", "HEAD"]`, and FastAPI builds the
    operation ID from the route's method *set*, whose iteration order follows
    the per-process hash seed. Without this the artifact alternates between
    `info_page__get` and `info_page__head` forever.
    """
    run = mocker.patch.object(
        generator.subprocess,
        "run",
        return_value=mocker.Mock(returncode=0, stdout="{}", stderr=""),
    )
    generate("ops")
    assert run.call_args.kwargs["env"]["PYTHONHASHSEED"] == "0"


def test_a_service_that_cannot_be_imported_fails_rather_than_skipping(mocker):
    """An ungenerated contract is indistinguishable from a service that serves
    nothing, so both import recipes failing is an error and never a skip."""
    mocker.patch.object(
        generator.subprocess,
        "run",
        return_value=mocker.Mock(returncode=1, stdout="", stderr="ImportError: boom"),
    )
    with pytest.raises(SystemExit, match="could not be imported"):
        generate("ops")


def test_both_import_recipes_are_tried_before_giving_up(mocker):
    """Production has two: most services import as a package from the repo
    root, and `scraper` runs with its own directory as the root. Trying the
    plain recipe first matters -- putting `ops/` on `sys.path` shadows the
    standard library's `email` with `ops/email.py`."""
    run = mocker.patch.object(
        generator.subprocess,
        "run",
        side_effect=[
            mocker.Mock(returncode=1, stdout="", stderr="no module"),
            mocker.Mock(returncode=0, stdout="{}", stderr=""),
        ],
    )
    assert generate("scraper") == "{}\n"
    assert run.call_count == 2
    assert "--service-dir" not in run.call_args_list[0].args[0]
    assert "--service-dir" in run.call_args_list[1].args[0]


def test_check_passes_when_every_service_serves_its_committed_contract(mocker, capsys):
    mocker.patch.object(generator, "service_apps", return_value=["ops"])
    mocker.patch.object(generator, "generate", return_value='{"openapi": "3.1.0"}\n')
    mocker.patch.object(
        generator.Path, "is_file", return_value=True, autospec=True
    )
    mocker.patch.object(
        generator.Path, "read_text", return_value='{"openapi": "3.1.0"}\n', autospec=True
    )
    mocker.patch("sys.argv", ["generate_service_contracts.py", "--check"])

    assert main() == 0


def test_check_fails_and_prints_the_diff_when_a_service_drifts(mocker, capsys):
    """The diff is the deliverable, not the exit code.

    A gate that says only "something changed" gets cleared by regenerating
    without reading, which is the check-you-must-remember this stage replaces.
    """
    mocker.patch.object(generator, "service_apps", return_value=["ops"])
    mocker.patch.object(generator, "generate", return_value='{"openapi": "3.2.0"}\n')
    mocker.patch.object(generator.Path, "is_file", return_value=True, autospec=True)
    mocker.patch.object(
        generator.Path, "read_text", return_value='{"openapi": "3.1.0"}\n', autospec=True
    )
    mocker.patch("sys.argv", ["generate_service_contracts.py", "--check"])

    assert main() == 1
    stderr = capsys.readouterr().err
    assert "3.1.0" in stderr and "3.2.0" in stderr, "the diff names both sides"
    assert "do not serve what their committed contract says" in stderr


def test_check_fails_when_a_service_has_no_committed_contract_at_all(mocker, capsys):
    """A new service is the case this gate exists for. Missing is a failure,
    not a file to create quietly on the way past."""
    mocker.patch.object(generator, "service_apps", return_value=["newservice"])
    mocker.patch.object(generator, "generate", return_value="{}\n")
    mocker.patch.object(generator.Path, "is_file", return_value=False, autospec=True)
    mocker.patch("sys.argv", ["generate_service_contracts.py", "--check"])

    assert main() == 1
    assert "no committed contract" in capsys.readouterr().err


def test_the_committed_artifacts_on_disk_are_canonical():
    """What is committed is what the generator would write.

    Reads the real files rather than a fixture: an artifact hand-edited into a
    different shape would still diff clean against itself under `--check` only
    until the next regeneration, and this is what notices in between.
    """
    for service in service_apps():
        path = committed_path(service)
        assert path.is_file(), f"{service} has no committed contract"
        text = path.read_text(encoding="utf-8")
        parsed = json.loads(text)
        expected = json.dumps(parsed, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        assert text == expected, (
            f"contracts/{service}.json is not in the generator's canonical form. "
            f"Regenerate it rather than editing it by hand."
        )
        assert parsed["paths"], f"{service}'s contract records no paths at all"
