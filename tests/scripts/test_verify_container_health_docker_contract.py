"""Layer 1 unit tests for the container_health Docker contract verifier.

The script's job is to fail when the live Docker API stops carrying what the
corpus records. Its Docker half needs a daemon and is exercised by the
`container_health Docker contract (real proxy)` CI job; what is tested here is
the decision logic that decides *whether* to fail, because a checker that
cannot fail is the thing this plan exists to stop shipping.

The sibling `tests/scripts/test_verify_promtail_contract.py` draws the line in
the same place, for the same reason.

Plan 162 Stage 6.
"""
from __future__ import annotations

import json

import pytest

from container_health.docker_api import DockerApi
from scripts import verify_container_health_docker_contract as verifier
from scripts.verify_container_health_docker_contract import (
    ContractError,
    _capture,
    _check_shape,
    _dotted,
    _fleet_seen,
    _request_kinds,
    _start_fleet,
    main,
)

SERVICE_LABEL = "com.docker.compose.service"


def _inspect(service: str = "ops", **overrides):
    payload = {
        "Id": "abc123",
        "Config": {"Labels": {SERVICE_LABEL: service}},
        "State": {"Status": "running"},
        "HostConfig": {"Memory": 0},
    }
    payload.update(overrides)
    return {"path": "/containers/abc123/json", "params": None, "response": payload}


class TestDotted:
    def test_reads_a_nested_field(self):
        assert _dotted({"State": {"Status": "running"}}, "State.Status") == "running"

    def test_a_missing_leaf_raises_rather_than_returning_none(self):
        """`None` would be indistinguishable from a field that is legitimately
        null, and the caller turns a raise into a named problem."""
        with pytest.raises(KeyError):
            _dotted({"State": {}}, "State.Status")

    def test_a_non_dict_on_the_way_down_raises(self):
        with pytest.raises(KeyError):
            _dotted({"State": "running"}, "State.Status")


class TestCheckShape:
    def test_a_complete_inspect_has_no_problems(self):
        assert _check_shape([_inspect()]) == []

    def test_a_missing_field_is_reported_with_its_path(self):
        broken = _inspect()
        del broken["response"]["State"]["Status"]

        problems = _check_shape([broken])

        assert len(problems) == 1
        assert "State.Status" in problems[0]

    def test_a_retyped_field_is_reported_even_though_it_is_present(self):
        """The failure that motivates a type check rather than a key check: a
        daemon that starts sending a string where an int was is exactly the
        silent shape change the corpus cannot notice on its own."""
        retyped = _inspect()
        retyped["response"]["HostConfig"]["Memory"] = "268435456"

        problems = _check_shape([retyped])

        assert len(problems) == 1
        assert "HostConfig.Memory" in problems[0]
        assert "str" in problems[0] and "int" in problems[0]

    def test_the_container_list_is_not_checked_as_an_inspect(self):
        """`/containers/json` returns a list of summaries, not an inspect
        payload, so applying the inspect field list to it would fail on every
        run."""
        listing = {"path": "/containers/json", "params": {"all": "true"},
                   "response": [{"Id": "abc123"}]}

        assert _check_shape([listing]) == []

    def test_stats_are_checked_for_their_own_field(self):
        stats = {"path": "/containers/abc123/stats", "params": {"stream": "false"},
                 "response": {}}

        problems = _check_shape([stats])

        assert len(problems) == 1
        assert "memory_stats" in problems[0]


class TestFleetSeen:
    def test_nothing_missing_when_every_service_came_back(self):
        exchanges = [
            _inspect("ops"), _inspect("scraper"),
            _inspect("snapshot-worker"), _inspect("lakekeeper"),
        ]

        assert _fleet_seen(exchanges) == []

    def test_a_service_the_proxy_never_returned_is_named(self):
        """Guards the case that would otherwise pass vacuously: a proxy that
        answers but returns an empty or partial fleet leaves the shape check
        with nothing to disagree with."""
        missing = _fleet_seen([_inspect("ops"), _inspect("scraper")])

        assert missing == ["lakekeeper", "snapshot-worker"]

    def test_no_exchanges_at_all_reports_the_whole_fleet(self):
        assert len(_fleet_seen([])) == 4


class TestRequestKinds:
    def test_container_ids_collapse_to_one_kind(self):
        """Two runs inspect different containers and must not read as drift."""
        first = _request_kinds({"/containers/aaa/json", "/containers/bbb/json"})
        second = _request_kinds({"/containers/ccc/json"})

        assert first == second == {"/containers/<id>/json"}

    def test_the_listing_endpoint_keeps_its_own_identity(self):
        """`/containers/json` must not be folded in with the per-container
        inspects, or losing the fleet listing entirely would look identical to
        a normal run."""
        kinds = _request_kinds({"/containers/json", "/containers/aaa/json"})

        assert kinds == {"/containers/json", "/containers/<id>/json"}

    def test_stats_are_their_own_kind(self):
        kinds = _request_kinds({"/containers/aaa/stats", "/containers/aaa/json"})

        assert kinds == {"/containers/<id>/stats", "/containers/<id>/json"}

    def test_a_new_endpoint_survives_normalisation_so_it_can_be_reported(self):
        """The drift the comparison exists to catch: a client that starts
        calling something the corpus has never recorded."""
        assert _request_kinds({"/containers/aaa/top"}) == {"/containers/aaa/top"}


class TestStartFleet:
    """A wrong flag here produces a fleet that silently exercises nothing.

    Every entry exists for a branch in `collector.py`, so `--memory` going
    missing does not fail the run -- it removes `memory_capped`'s only input,
    the corpus quietly stops carrying a stats exchange, and `_check_shape`
    stops checking `memory_stats` forever after.
    """

    def _calls(self, mocker):
        run = mocker.patch.object(verifier, "_run", return_value="")
        _start_fleet()
        return [call.args for call in run.call_args_list]

    def _run_calls(self, mocker):
        """Only the `docker run` invocations.

        `_teardown` passes every container name to `docker rm -f`, so a plain
        name search matches that call first and asserts nothing about how the
        container is actually started.
        """
        return [c for c in self._calls(mocker) if c[0] == "run"]

    def test_the_previous_fleet_is_removed_before_anything_starts(self, mocker):
        calls = self._calls(mocker)

        assert calls[0][0] == "rm", "a leftover fleet would be inspected as if fresh"

    def test_every_fleet_entry_and_the_proxy_are_started(self, mocker):
        runs = [call for call in self._calls(mocker) if call[0] == "run"]

        assert len(runs) == 5

    def test_the_capped_container_declares_its_limit_and_healthcheck(self, mocker):
        ops = next(c for c in self._run_calls(mocker) if "ch-contract-ops" in c)

        assert "--memory" in ops
        assert ops[ops.index("--memory") + 1] == "268435456"
        assert "--health-cmd" in ops

    def test_the_uncapped_container_declares_neither(self, mocker):
        scraper = next(
            c for c in self._run_calls(mocker) if "ch-contract-scraper" in c
        )

        assert "--memory" not in scraper
        assert "--health-cmd" not in scraper, "no healthcheck is the UNCONFIGURED case"

    def test_only_the_one_off_carries_the_one_off_label(self, mocker):
        labelled = [
            c for c in self._calls(mocker) if "com.docker.compose.oneoff=True" in c
        ]

        assert len(labelled) == 1
        assert "ch-contract-oneoff" in labelled[0]

    def test_the_sibling_project_container_carries_the_other_project(self, mocker):
        lake = next(
            c for c in self._run_calls(mocker) if "ch-contract-lakekeeper" in c
        )

        assert "com.docker.compose.project=cartracker-lakehouse" in lake


def _summary(container_id):
    return {"Id": container_id}


def _payload(service, memory=0):
    return {
        "Id": f"id-{service}",
        "Config": {"Labels": {SERVICE_LABEL: service}},
        "State": {"Status": "running"},
        "HostConfig": {"Memory": memory},
    }


class TestCapture:
    """Which containers get a second request, and why.

    `container_stats` is issued per *capped* container, and that rule is the
    difference between a corpus that carries `memory_stats` and one that does
    not.
    """

    def _fake_get(self, capped_memory=268435456):
        def fake(_self, path, params=None):
            if path == "/containers/json":
                filters = (params or {}).get("filters", "")
                if "cartracker-lakehouse" in filters:
                    return [_summary("id-lakekeeper")]
                return [_summary("id-ops"), _summary("id-scraper")]
            if path.endswith("/stats"):
                return {"memory_stats": {"usage": 1}}
            if "id-ops" in path:
                return _payload("ops", capped_memory)
            if "id-scraper" in path:
                return _payload("scraper")
            return _payload("lakekeeper")

        return fake

    def test_only_the_capped_container_is_asked_for_stats(self, mocker):
        mocker.patch.object(DockerApi, "_get", self._fake_get())

        exchanges = _capture(DockerApi("http://unused"))

        stats = [e for e in exchanges if e["path"].endswith("/stats")]
        assert len(stats) == 1
        assert "id-ops" in stats[0]["path"]

    def test_no_stats_request_when_nothing_declares_a_cap(self, mocker):
        mocker.patch.object(DockerApi, "_get", self._fake_get(capped_memory=0))

        exchanges = _capture(DockerApi("http://unused"))

        assert not [e for e in exchanges if e["path"].endswith("/stats")]

    def test_both_projects_are_inspected(self, mocker):
        mocker.patch.object(DockerApi, "_get", self._fake_get())

        exchanges = _capture(DockerApi("http://unused"))

        listings = [e for e in exchanges if e["path"] == "/containers/json"]
        assert len(listings) == 2, "the sibling project is a separate query"

    def test_the_spy_is_restored_even_when_the_client_raises(self, mocker):
        """`_capture` swaps a class attribute. Leaving the spy installed would
        make every later `DockerApi` in the same interpreter append to a dead
        list -- including, in `--record` mode, the one whose output is written
        to the corpus.
        """
        mocker.patch.object(DockerApi, "_get", side_effect=RuntimeError("boom"))
        patched = DockerApi._get

        with pytest.raises(RuntimeError):
            _capture(DockerApi("http://unused"))

        assert DockerApi._get is patched


class TestRun:
    def test_a_failing_docker_command_raises_with_its_stderr(self, mocker):
        """Silently returning empty output would turn a dead daemon into an
        empty fleet, which `_fleet_seen` would then report as the wrong
        problem."""
        mocker.patch.object(
            verifier.subprocess, "run",
            return_value=mocker.Mock(returncode=1, stdout="", stderr="no daemon"),
        )

        with pytest.raises(ContractError, match="no daemon"):
            verifier._run("ps")

    def test_check_false_tolerates_failure(self, mocker):
        """`_teardown` runs against containers that may not exist yet."""
        mocker.patch.object(
            verifier.subprocess, "run",
            return_value=mocker.Mock(returncode=1, stdout="", stderr="no such"),
        )

        assert verifier._run("rm", "-f", "nope", check=False) == ""


class TestAwaitProxy:
    def test_returns_as_soon_as_the_proxy_answers(self, mocker):
        response = mocker.MagicMock()
        response.status = 200
        response.__enter__ = mocker.Mock(return_value=response)
        response.__exit__ = mocker.Mock(return_value=False)
        mocker.patch.object(verifier.urllib.request, "urlopen", return_value=response)

        verifier._await_proxy(timeout=1.0)

    def test_a_proxy_that_never_answers_raises_rather_than_proceeding(self, mocker):
        """The failure this prevents is quiet: capturing against a proxy that
        is not ready records a short fleet, and a short fleet is indexed as a
        contract violation rather than as a startup race."""
        mocker.patch.object(
            verifier.urllib.request, "urlopen", side_effect=OSError("refused")
        )
        mocker.patch.object(verifier.time, "sleep")

        with pytest.raises(ContractError, match="refused"):
            verifier._await_proxy(timeout=0.05)


class TestAwaitHealthy:
    def test_returns_once_the_capped_container_reports_healthy(self, mocker):
        mocker.patch.object(verifier, "_run", return_value="healthy")

        verifier._await_healthy(timeout=1.0)

    def test_a_container_stuck_starting_raises(self, mocker):
        """`starting` is a real state and not the one the fixture exists to
        provide. Recording it would put -1 where the suite asserts 1, failing
        later and further away as a value mismatch."""
        mocker.patch.object(verifier, "_run", return_value="starting")
        mocker.patch.object(verifier.time, "sleep")

        with pytest.raises(ContractError, match="did not become healthy"):
            verifier._await_healthy(timeout=0.05)


@pytest.fixture
def stubbed(mocker, tmp_path):
    """Everything needing a daemon, replaced. Every decision left intact.

    This is where the cheap coverage is: the exit codes CI keys on are pure
    branching over data, and none of them needs Docker to be wrong.
    """
    mocker.patch.object(verifier, "_start_fleet")
    mocker.patch.object(verifier, "_await_proxy")
    mocker.patch.object(verifier, "_await_healthy")
    mocker.patch.object(verifier, "_daemon_version", return_value="test-daemon")
    mocker.patch.object(verifier, "REPO_ROOT", tmp_path)
    mocker.patch.object(verifier, "CORPUS", tmp_path / "corpus.json")
    return mocker.patch.object(verifier, "_teardown")


def _full_fleet():
    return [
        {"path": "/containers/json", "params": {"all": "true"}, "response": []},
        {"path": "/containers/id-ops/json", "params": None,
         "response": _payload("ops", 1)},
        {"path": "/containers/id-scraper/json", "params": None,
         "response": _payload("scraper")},
        {"path": "/containers/id-oneoff/json", "params": None,
         "response": _payload("snapshot-worker")},
        {"path": "/containers/id-lake/json", "params": None,
         "response": _payload("lakekeeper")},
        {"path": "/containers/id-ops/stats", "params": {"stream": "false"},
         "response": {"memory_stats": {}}},
    ]


class TestMain:
    def test_an_incomplete_fleet_fails_before_anything_else_is_judged(
        self, mocker, stubbed
    ):
        """Ordered first on purpose. A proxy that answers with an empty fleet
        leaves the shape check with nothing to disagree with, so it would pass
        vacuously -- G1's failure in miniature."""
        mocker.patch.object(verifier, "_capture", return_value=[])

        assert main([]) == 1

    def test_a_shape_problem_fails(self, mocker, stubbed):
        broken = _full_fleet()
        del broken[1]["response"]["State"]
        mocker.patch.object(verifier, "_capture", return_value=broken)

        assert main([]) == 1

    def test_a_missing_corpus_fails_rather_than_recording_one_silently(
        self, mocker, stubbed
    ):
        """Verify mode must never write. A run that quietly created the file it
        was meant to check against would pass for ever afterwards, against a
        recording nobody reviewed."""
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())

        assert main([]) == 1
        assert not verifier.CORPUS.exists()

    def test_record_writes_the_corpus_with_its_provenance(self, mocker, stubbed):
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())

        assert main(["--record"]) == 0

        written = json.loads(verifier.CORPUS.read_text(encoding="utf-8"))
        assert written["captured_at"]
        assert "test-daemon" in written["source"]
        assert len(written["exchanges"]) == 6

    def test_a_recorded_corpus_then_verifies_clean(self, mocker, stubbed):
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())
        assert main(["--record"]) == 0

        assert main([]) == 0

    def test_a_new_request_kind_is_drift(self, mocker, stubbed):
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())
        assert main(["--record"]) == 0
        mocker.patch.object(
            verifier, "_capture",
            return_value=_full_fleet() + [
                {"path": "/containers/id-ops/top", "params": None, "response": {}}
            ],
        )

        assert main([]) == 1

    def test_a_request_kind_that_stopped_being_made_is_drift(self, mocker, stubbed):
        """The other direction, without which the corpus rots silently: it
        would go on describing a client that no longer exists."""
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())
        assert main(["--record"]) == 0
        mocker.patch.object(
            verifier, "_capture",
            return_value=[e for e in _full_fleet() if not e["path"].endswith("/stats")],
        )

        assert main([]) == 1

    def test_different_container_ids_are_not_drift(self, mocker, stubbed):
        """Ids change every run. A comparison that called that drift would fail
        on every green run and be switched off within a week."""
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())
        assert main(["--record"]) == 0
        renamed = _full_fleet()
        for exchange in renamed:
            exchange["path"] = exchange["path"].replace("id-", "other-")
        mocker.patch.object(verifier, "_capture", return_value=renamed)

        assert main([]) == 0

    def test_the_fleet_is_torn_down_by_default(self, mocker, stubbed):
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())

        main(["--record"])

        stubbed.assert_called_once()

    def test_keep_leaves_the_fleet_running_for_inspection(self, mocker, stubbed):
        mocker.patch.object(verifier, "_capture", return_value=_full_fleet())

        main(["--record", "--keep"])

        stubbed.assert_not_called()

    def test_the_fleet_is_torn_down_even_when_capture_raises(self, mocker, stubbed):
        """Without the `finally`, one failing run leaves four containers and a
        proxy behind on whatever machine ran it -- including a developer's."""
        mocker.patch.object(
            verifier, "_capture", side_effect=ContractError("proxy died")
        )

        with pytest.raises(ContractError):
            main([])

        stubbed.assert_called_once()
