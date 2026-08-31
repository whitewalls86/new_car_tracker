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

import pytest

from scripts.verify_promtail_contract import (
    Observation,
    _check_group,
    _parse_entries,
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
