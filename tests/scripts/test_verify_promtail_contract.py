"""The Promtail contract checker's own verdict logic (Plan 160).

The checker is invoked straight from ``ci.yml`` and had no test of its own,
which is part of why it went four occurrences before anyone could say what it
was doing. The parsing and the verdict are pure, so they can be driven here
without a Docker daemon by handing ``_check_group`` a fake runner that returns
recorded observation shapes.

The distinction under test is the whole point of the plan: an entry the
checker failed to *observe* is not an entry Promtail *dropped*.
"""

from __future__ import annotations

import json

import pytest
import yaml

from scripts import verify_promtail_contract as checker
from scripts.verify_promtail_contract import (
    Observation,
    _check_group,
    _config_document,
    _parse_entries,
    _promtail_image,
    _stages_for,
    main,
)

_BANNER = [
    "Clients configured:",
    "----------------------",
    "url: http://127.0.0.1:3100/loki/api/v1/push",
    "batchwait: 1s",
    "batchsize: 1048576",
]


def _case(name, line, retained, **expected):
    return {
        "name": name,
        "service": "ops",
        "source_type": "application_file",
        "line": line,
        "expected": {"retained": retained, **expected},
    }


def _entry(line, **labels):
    rendered = ", ".join(f'{key}="{value}"' for key, value in labels.items())
    return f"2026-08-30T14:30:21Z {{{rendered}}}\t{line}"


def _runner(*observations):
    """A fake ``_run``: one recorded Observation per attempt, then repeat."""
    calls = []

    def run(lines, expected_count):
        calls.append((lines, expected_count))
        return observations[min(len(calls) - 1, len(observations) - 1)]

    run.calls = calls
    return run


def _observation(*entries, complete=True):
    return Observation({line: labels for line, labels in entries}, complete)


class TestParseEntries:
    def test_banner_lines_are_not_parsed_as_entries(self):
        """Promtail prints 15 lines of client config before any entry."""
        output = [*_BANNER, _entry("hello", service="ops", level="INFO")]
        assert _parse_entries(output, "ops") == {
            "hello": {"service": "ops", "level": "INFO"}
        }

    def test_entries_for_another_service_are_ignored(self):
        output = [_entry("hello", service="scraper", level="INFO")]
        assert _parse_entries(output, "ops") == {}


class TestVerdict:
    def test_complete_batch_passes(self):
        group = [_case("kept", "line-a", True, level="INFO", logger="ops")]
        runner = _runner(
            _observation(("line-a", {"level": "INFO", "logger": "ops"}))
        )

        failures, inconclusive, attempts = _check_group("ops", group, runner)

        assert failures == []
        assert inconclusive == []
        assert attempts == 1

    def test_line_missing_on_every_attempt_fails_and_names_it(self):
        """A real regression is deterministic. That is what makes it a finding."""
        group = [_case("kept", "line-a", True, level="INFO")]
        runner = _runner(_observation(complete=False))

        failures, inconclusive, attempts = _check_group("ops", group, runner)

        assert inconclusive == []
        assert attempts == 3
        assert len(failures) == 1
        assert "kept" in failures[0]
        assert "Promtail dropped it on all 3 attempts" in failures[0]

    def test_line_recovered_by_a_retry_is_inconclusive_not_a_failure(self):
        """The defect this plan exists to fix: absence was overloaded."""
        group = [_case("kept", "line-a", True, level="INFO", logger="ops")]
        runner = _runner(
            _observation(complete=False),
            _observation(("line-a", {"level": "INFO", "logger": "ops"})),
        )

        failures, inconclusive, attempts = _check_group("ops", group, runner)

        assert failures == []
        assert attempts == 2
        assert len(inconclusive) == 1
        assert "kept" in inconclusive[0]
        assert "the checker lost it, the pipeline did not drop it" in inconclusive[0]

    def test_a_recovered_line_still_has_its_labels_checked(self):
        """Inconclusive on delivery is not a free pass on the contract."""
        group = [_case("kept", "line-a", True, level="INFO", logger="ops")]
        runner = _runner(
            _observation(complete=False),
            _observation(("line-a", {"level": "WARNING", "logger": "ops"})),
        )

        failures, inconclusive, _ = _check_group("ops", group, runner)

        assert len(inconclusive) == 1
        assert len(failures) == 1
        assert "level is 'WARNING' in Promtail, 'INFO' in the corpus" in failures[0]

    def test_zero_retained_batch_terminates_without_retrying(self):
        """`processing` expects nothing back, so there is no count to wait for.

        It must not spend three attempts, and it must not hang looking for an
        entry that is never coming.
        """
        group = [
            _case(
                "application_malformed",
                "not-json production-shaped debris",
                False,
                drop_reason="application_file_unclassified",
            )
        ]
        runner = _runner(_observation())

        failures, inconclusive, attempts = _check_group("processing", group, runner)

        assert failures == []
        assert inconclusive == []
        assert attempts == 1
        assert runner.calls == [(["not-json production-shaped debris"], 0)]

    def test_a_line_the_corpus_says_is_dropped_but_promtail_keeps_fails(self):
        """The race can only lose entries, so retention is never ambiguous."""
        group = [_case("dropped", "line-a", False, drop_reason="unclassified")]
        runner = _runner(_observation(("line-a", {"level": "INFO"})))

        failures, inconclusive, _ = _check_group("processing", group, runner)

        assert inconclusive == []
        assert len(failures) == 1
        assert "corpus says dropped (unclassified), Promtail kept" in failures[0]

    def test_expected_count_passed_to_the_runner_is_the_retained_count(self):
        """Stage 1's completion signal is positive, not an inference from silence."""
        group = [
            _case("kept", "line-a", True, level="INFO"),
            _case("also-kept", "line-b", True, level="INFO"),
            _case("dropped", "line-c", False, drop_reason="unclassified"),
        ]
        runner = _runner(
            _observation(
                ("line-a", {"level": "INFO"}), ("line-b", {"level": "INFO"})
            )
        )

        _check_group("ops", group, runner)

        assert runner.calls == [(["line-a", "line-b", "line-c"], 2)]

    def test_a_batch_losing_a_different_line_each_time_is_inconclusive(self):
        """A race varies. Nothing here should read as a contract violation."""
        group = [
            _case("first", "line-a", True, level="INFO"),
            _case("second", "line-b", True, level="INFO"),
        ]
        runner = _runner(
            _observation(("line-b", {"level": "INFO"}), complete=False),
            _observation(("line-a", {"level": "INFO"}), complete=False),
            _observation(("line-b", {"level": "INFO"}), complete=False),
        )

        failures, inconclusive, attempts = _check_group("ops", group, runner)

        assert failures == []
        assert attempts == 3
        assert len(inconclusive) == 2

    def test_one_deterministic_loss_among_recoverable_ones_still_fails(self):
        group = [
            _case("always-lost", "line-a", True, level="INFO"),
            _case("flaky", "line-b", True, level="INFO"),
        ]
        runner = _runner(
            _observation(complete=False),
            _observation(("line-b", {"level": "INFO"}), complete=False),
        )

        failures, inconclusive, _ = _check_group("ops", group, runner)

        assert len(failures) == 1
        assert "always-lost" in failures[0]
        assert len(inconclusive) == 1
        assert "flaky" in inconclusive[0]


@pytest.mark.parametrize("complete", [True, False])
def test_a_complete_observation_stops_retrying(complete):
    group = [_case("kept", "line-a", True, level="INFO")]
    runner = _runner(_observation(("line-a", {"level": "INFO"}), complete=complete))

    _, _, attempts = _check_group("ops", group, runner)

    assert attempts == (1 if complete else 3)


# ---------------------------------------------------------------------------
# Plan 162 Stage H: the rest of the checker, which is also daemon-free
# ---------------------------------------------------------------------------
#
# Everything above tests the verdict. What follows tests how the replay is set
# up, which is where a silent failure lives: a checker pointed at the wrong
# image, or replaying stages production does not use, agrees with itself and
# reports nothing.

_CONFIG = {
    "scrape_configs": [
        {
            "job_name": "ops",
            "pipeline_stages": [{"json": {"expressions": {"level": "level"}}}],
        },
        {
            "job_name": "docker-operations",
            "pipeline_stages": [
                {"docker": {}},
                {"regex": {"expression": "(?P<level>INFO|ERROR)"}},
                {"labels": {"level": None}},
            ],
        },
    ]
}


class TestPromtailImage:
    def test_the_image_is_read_from_the_compose_file(self, tmp_path, mocker):
        """The check is only worth anything against the tag production runs.

        Pinning it here instead would let compose move to a new Promtail while
        CI went on agreeing with the old one -- the exact drift this script was
        written to catch, one level up.
        """
        compose = tmp_path / "docker-compose.yml"
        compose.write_text(
            yaml.safe_dump({"services": {"promtail": {"image": "grafana/promtail:9.9"}}}),
            encoding="utf-8",
        )
        mocker.patch.object(checker, "_COMPOSE", compose)

        assert _promtail_image() == "grafana/promtail:9.9"


class TestStagesFor:
    def test_a_file_source_replays_that_service_s_own_stages(self):
        stages = _stages_for(_CONFIG, "ops", "application_file")

        assert stages == [{"json": {"expressions": {"level": "level"}}}]

    def test_a_docker_source_replays_the_shared_operations_stages(self):
        stages = _stages_for(_CONFIG, "scraper", "docker")

        assert {"regex": {"expression": "(?P<level>INFO|ERROR)"}} in stages

    def test_the_docker_envelope_stage_is_dropped(self):
        """`docker: {}` unwraps a container JSON envelope that `-stdin` never
        has. Left in, every replayed line fails to parse and the whole corpus
        reads as dropped -- a total false failure rather than a subtle one.
        """
        stages = _stages_for(_CONFIG, "scraper", "docker")

        assert not any("docker" in stage for stage in stages)


class TestConfigDocument:
    def test_the_synthetic_job_carries_the_service_and_source_labels(self):
        """`_parse_entries` filters output by the `service` label, so if these
        were not set the checker would discard every entry it just produced and
        call the batch incomplete."""
        document = _config_document(_CONFIG, "ops", "application_file")

        scrape = document["scrape_configs"][0]
        labels = scrape["static_configs"][0]["labels"]
        assert scrape["job_name"] == "stdin"
        assert labels == {"service": "ops", "source": "application_file"}

    def test_the_document_carries_the_real_pipeline_stages(self):
        document = _config_document(_CONFIG, "ops", "application_file")

        assert document["scrape_configs"][0]["pipeline_stages"] == [
            {"json": {"expressions": {"level": "level"}}}
        ]

    def test_it_is_serialisable_as_the_yaml_promtail_will_be_handed(self):
        """The document is written to a file and mounted; an unserialisable
        value would fail inside the container, where the error is a Go parse
        message rather than a Python one."""
        document = _config_document(_CONFIG, "ops", "application_file")

        assert yaml.safe_load(yaml.safe_dump(document)) == document


@pytest.fixture
def wired(tmp_path, mocker):
    """`main` with its three file reads and the daemon replaced."""
    config = tmp_path / "promtail.yml"
    config.write_text(yaml.safe_dump(_CONFIG), encoding="utf-8")
    fixtures = tmp_path / "corpus.json"
    mocker.patch.object(checker, "_PROMTAIL_CONFIG", config)
    mocker.patch.object(checker, "_FIXTURES", fixtures)
    mocker.patch.object(checker, "_promtail_image", return_value="grafana/promtail:test")

    def write_cases(cases):
        fixtures.write_text(json.dumps({"cases": cases}), encoding="utf-8")

    return write_cases


class TestMain:
    def test_a_corpus_that_agrees_exits_zero(self, wired, mocker):
        wired([_case("kept", "line-a", True, level="INFO")])
        mocker.patch.object(
            checker, "_run",
            return_value=_observation(("line-a", {"level": "INFO"})),
        )

        assert main() == 0

    def test_a_mismatch_exits_one(self, wired, mocker):
        wired([_case("kept", "line-a", True, level="INFO")])
        mocker.patch.object(
            checker, "_run",
            return_value=_observation(("line-a", {"level": "ERROR"})),
        )

        assert main() == 1

    def test_a_line_recovered_by_a_retry_is_inconclusive_not_a_failure(
        self, wired, mocker
    ):
        """Plan 160's whole point, asserted at the exit code rather than only
        inside `_check_group`.

        The distinction is *recovery*, not absence: a line missing from every
        attempt is a real drop and does turn CI red. One that comes back on a
        retry was never dropped, only unobserved, and must not.
        """
        wired([_case("kept", "line-a", True, level="INFO")])
        mocker.patch.object(
            checker, "_run",
            side_effect=[
                _observation(complete=False),
                _observation(("line-a", {"level": "INFO"})),
            ],
        )

        assert main() == 0

    def test_a_line_missing_from_every_attempt_does_turn_ci_red(self, wired, mocker):
        """The other side of the same distinction, so neither can be loosened
        without the other failing."""
        wired([_case("kept", "line-a", True, level="INFO")])
        mocker.patch.object(
            checker, "_run", return_value=_observation(complete=False)
        )

        assert main() == 1

    def test_cases_are_grouped_into_one_replay_per_service_and_source(
        self, wired, mocker
    ):
        """Each replay starts a container. Grouping is what keeps the job at a
        handful of runs rather than one per fixture line."""
        cases = [
            _case("a", "line-a", True, level="INFO"),
            _case("b", "line-b", True, level="INFO"),
        ]
        cases[1]["source_type"] = "docker"
        cases[1]["service"] = "scraper"
        wired(cases)
        run = mocker.patch.object(
            checker, "_run",
            side_effect=lambda *args, **kwargs: _observation(
                ("line-a", {"level": "INFO"}), ("line-b", {"level": "INFO"})
            ),
        )

        assert main() == 0
        assert run.call_count == 2
