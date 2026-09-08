"""Plan 144: the invariants scripts/redeploy.sh depends on.

Every check here exists because a production deploy on 2026-08-20 did something
the operator did not intend, and none of them can be caught by running the
script: they are properties of docker-compose.yml and of the shared exemption
file that the script reads at runtime.

The sharpest piece of evidence for this file is that the Plan 136 Stage 2
deploy did not use ``redeploy.sh`` at all. It was driven by hand, because the
script would have run ``up -d`` without ``--no-deps`` across three services and
then reported "Done." after ``sleep 10``.
"""
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).parent.parent
_SCRIPT = _REPO_ROOT / "scripts" / "redeploy.sh"
_EXEMPTIONS = _REPO_ROOT / "healthcheck-exemptions.txt"
_FOLLOWERS = _REPO_ROOT / "deploy-followers.txt"


def load_health_exemptions(path: Path = _EXEMPTIONS) -> dict:
    """Parse one of the deploy registries into ``{service: reason}``.

    Mirrors the ``while read`` loops in ``scripts/redeploy.sh`` line for line:
    ``#`` in column 0 is a comment, an empty line is a separator, a line
    starting with whitespace continues the previous reason, anything else
    opens a new entry. ``test_the_file_cannot_be_read_two_ways`` below is what
    keeps the two parsers from drifting apart.

    Both ``healthcheck-exemptions.txt`` and ``deploy-followers.txt`` use this
    shape, deliberately, so the script carries one parser rather than two.
    """
    entries: dict[str, str] = {}
    current = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "" or line.startswith("#"):
            continue
        if line[0].isspace():
            entries[current] = f"{entries[current]} {line.strip()}".strip()
            continue
        name, _, reason = line.partition(" ")
        current = name
        entries[name] = reason.strip()
    return entries


def _compose_services() -> dict:
    return yaml.safe_load(
        (_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )["services"]


def _cron_fire_interval(schedule: str) -> int:
    """Seconds between fires of a five-field cron expression.

    Only the resolution the drain bound cares about: anything that does not
    repeat within the minute or hour fields fires at most daily, and a daily
    DAG cannot be the tightest one. Returns 86400 for those rather than
    pretending to a precision this does not need.
    """
    fields = schedule.split()
    if len(fields) != 5:
        return 86400
    minute, hour = fields[0], fields[1]
    if minute == "*":
        return 60
    match = re.fullmatch(r"\*/(\d+)", minute)
    if match:
        return int(match.group(1)) * 60
    if hour == "*":
        return 3600
    match = re.fullmatch(r"\*/(\d+)", hour)
    if match:
        return int(match.group(1)) * 3600
    return 86400


def _gated_dag_schedules() -> dict[str, str]:
    """``{dag file: schedule}`` for every DAG behind the coordination gate.

    Every DAG in ``airflow/dags`` carries ``deploy_intent_sensor`` as its first
    task, which is what makes its runs park during a drain; the check is here
    rather than assumed so a DAG added without the sensor does not silently
    drop out of the floor this file derives.
    """
    schedules = {}
    for path in sorted((_REPO_ROOT / "airflow" / "dags").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if "deploy_intent_sensor(" not in text:
            continue
        match = re.search(r'^\s*schedule="([^"]+)"', text, re.MULTILINE)
        if match:
            schedules[path.name] = match.group(1)
    return schedules


class TestHealthExemptionFile:
    """One file, two consumers, no second hand-maintained copy.

    Before Plan 144 the list lived only in ``TestServiceHealthCoverage``. A
    deploy-time poller needs the same list to tell "exempt by contract" from
    "not healthy", and a second copy of a deny-list is how the first one goes
    stale.
    """

    def test_the_file_exists_where_the_script_looks_for_it(self):
        assert _EXEMPTIONS.exists(), (
            "healthcheck-exemptions.txt is missing; redeploy.sh refuses to deploy "
            "without it rather than guess which services are exempt"
        )

    def test_every_entry_names_a_real_service(self):
        stale = set(load_health_exemptions()) - set(_compose_services())
        assert not stale, (
            f"exemptions name services that do not exist: {sorted(stale)}. A "
            "renamed service leaves an exemption behind that then silently "
            "covers whatever takes its name next."
        )

    def test_every_entry_carries_a_reason(self):
        for name, reason in load_health_exemptions().items():
            assert len(reason) > 40, (
                f"{name}'s exemption reason is missing or too thin to be an "
                "actual justification"
            )

    @pytest.mark.parametrize("path", [_EXEMPTIONS, _FOLLOWERS], ids=lambda p: p.name)
    def test_the_file_cannot_be_read_two_ways(self, path):
        """The bash and Python parsers agree only while the file avoids two
        shapes: an indented ``#`` line, which reads as a comment to anything
        that strips first and as a reason continuation to both parsers as
        written; and a whitespace-only line, which looks like a separator and
        parses as a continuation."""
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line == "":
                continue
            assert line.strip(), (
                f"line {number} is whitespace-only; write a truly empty line, "
                "which is the only separator both parsers agree on"
            )
            assert not (line[0].isspace() and line.lstrip().startswith("#")), (
                f"line {number} is an indented comment; both parsers read it as "
                "part of the previous entry's reason"
            )

    def test_the_first_entry_line_is_service_then_reason(self):
        for number, line in enumerate(_EXEMPTIONS.read_text(encoding="utf-8").splitlines(), 1):
            if line == "" or line.startswith("#") or line[0].isspace():
                continue
            name = line.split(" ")[0]
            assert re.fullmatch(r"[A-Za-z0-9_.-]+", name), (
                f"line {number} starts with {name!r}, which is not a service name"
            )


class TestRedeployScriptContract:
    """Defects 1-3 of Plan 144, asserted against the script text.

    These are text assertions on purpose. Running the script needs Docker, a
    compose project and a production fleet; the properties that actually broke
    are visible without any of that.
    """

    @staticmethod
    def _text() -> str:
        return _SCRIPT.read_text(encoding="utf-8")

    def test_every_up_carries_no_deps(self):
        """Defect 1. Deploying `ops archiver pack-worker processing` re-ran
        `flyway` because Compose walked the dependency graph."""
        ups = [
            line.strip() for line in self._text().splitlines()
            if re.search(r"^\s*docker compose up\b", line)
        ]
        assert ups, "redeploy.sh no longer runs `docker compose up`"
        for line in ups:
            assert "--no-deps" in line, (
                f"{line!r} omits --no-deps, so a deploy recreates dependencies "
                "nobody asked for. ARCHITECTURE.md documents --no-deps as the flow."
            )

    def test_readiness_is_not_a_fixed_sleep(self):
        """Defect 2. `sleep 10` is not a readiness contract, and the TODO that
        excused it pointed at Plan 76, which closed 2026-03-30."""
        text = self._text()
        assert "TODO Plan 76" not in text, "the stale Plan 76 TODO is back"
        assert not re.search(r"^\s*sleep \d+\s*$", text, re.MULTILINE), (
            "a bare `sleep <n>` is back in redeploy.sh; readiness comes from "
            "polling health, not from a fixed wait"
        )
        assert ".State.Health.Status" in text, (
            "redeploy.sh no longer polls Docker's health state"
        )

    def test_exempt_services_are_read_from_the_shared_file(self):
        """Defect 2's second half. The moment a service name is inlined in the
        script's *logic*, this is a second copy of the deny-list. Comments may
        name services freely -- the header explains the defects, and those
        happened to specific services."""
        text = self._text()
        assert "healthcheck-exemptions.txt" in text
        code = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        for name in load_health_exemptions():
            assert not re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", code), (
                f"{name} is named in redeploy.sh's logic; exemptions must come "
                "from healthcheck-exemptions.txt so there is only one list"
            )

    def test_the_health_timeout_covers_the_slowest_healthcheck(self):
        """The number in the script is derived, not guessed.

        Docker can take ``start_period + retries * (interval + timeout)`` to
        call a container unhealthy. Raising a `start_period` past the deploy
        timeout would turn a slow-but-fine service into a failed deploy, so
        that trade lands here rather than in production.
        """
        def _seconds(value, default):
            if value is None:
                return default
            match = re.fullmatch(r"(\d+)([smh])", str(value).strip())
            assert match, f"unparseable duration {value!r} in docker-compose.yml"
            return int(match.group(1)) * {"s": 1, "m": 60, "h": 3600}[match.group(2)]

        worst = 0
        for spec in _compose_services().values():
            healthcheck = spec.get("healthcheck")
            if not isinstance(healthcheck, dict) or not healthcheck.get("test"):
                continue
            window = _seconds(healthcheck.get("start_period"), 0) + healthcheck.get(
                "retries", 3
            ) * (
                _seconds(healthcheck.get("interval"), 30)
                + _seconds(healthcheck.get("timeout"), 30)
            )
            worst = max(worst, window)

        match = re.search(r"DEPLOY_HEALTH_TIMEOUT:-(\d+)", self._text())
        assert match, "redeploy.sh no longer defines a default health timeout"
        timeout = int(match.group(1))
        assert timeout >= worst, (
            f"the deploy health timeout is {timeout}s but a healthcheck in "
            f"docker-compose.yml can take {worst}s to settle. Raise "
            "DEPLOY_HEALTH_TIMEOUT's default, or lower the start_period."
        )

    def test_the_authorize_wait_is_bounded(self):
        """Defect 7. The drain poll was the one wait in this script with no
        timeout and no escape.

        On 2026-08-30 a deploy of `ops processing` sat in `while :` for
        nineteen minutes with every production DAG parked, and would have sat
        there indefinitely -- the sensor could not write the observation the
        drain was waiting for, so the count it polled could only rise. What
        ended it was an operator pressing Ctrl-C. The operator was the timeout.

        The bound has to be consulted on the *retry* branch, before sleeping
        again: a timeout the loop only reaches after authorization succeeds is
        not a timeout.
        """
        text = self._text()
        assert re.search(r"DEPLOY_DRAIN_TIMEOUT:-(\d+)", text), (
            "redeploy.sh no longer defines a default drain timeout, so the "
            "authorize poll is unbounded again (Plan 158 decision 7)"
        )
        body = text[text.index("_prepare_coordination() {"):text.index("_begin_validation() {")]
        assert "409)" in body and 'sleep "$DRAIN_POLL_INTERVAL"' in body, (
            "the authorize poll no longer retries on 409; this test no longer "
            "describes the loop it was written for"
        )
        retry = body[body.index("409)"):body.index('sleep "$DRAIN_POLL_INTERVAL"')]
        assert "DRAIN_TIMEOUT" in retry and "return 1" in retry, (
            "the 409 branch retries without consulting DRAIN_TIMEOUT, so a "
            "drain that never drains still polls forever"
        )
        assert "_dump_drain_evidence" in retry, (
            "expiry no longer prints the drain evidence; a bounded wait that "
            "fails without naming the blocking source only saves the operator "
            "the wait, not the diagnosis"
        )

    def test_the_drain_timeout_survives_one_fire_and_park_cycle(self):
        """The number in the script is derived, not guessed -- the same trade
        `test_the_health_timeout_covers_the_slowest_healthcheck` makes.

        A gated DAG run parked on the coordination gate records the
        observation the drain counts only on its *next* poke, so the floor for
        a healthy drain is one complete cycle: the tightest gated schedule
        (`*/5`), plus the deploy-intent sensor's poke interval. Tightening a
        DAG schedule or slowing the sensor past the timeout would turn an
        ordinary overlap into a failed deploy, so that lands here rather than
        in production.
        """
        schedules = _gated_dag_schedules()
        assert schedules, "no gated DAG declares a schedule; the floor is unverifiable"
        tightest = min(_cron_fire_interval(s) for s in schedules.values())

        sensors = (_REPO_ROOT / "airflow" / "dags" / "sensors.py").read_text(encoding="utf-8")
        gate = sensors[sensors.index("def deploy_intent_sensor("):]
        match = re.search(r"poke_interval=(\d+)", gate)
        assert match, "deploy_intent_sensor no longer declares a poke_interval"
        poke = int(match.group(1))

        match = re.search(r"DEPLOY_DRAIN_TIMEOUT:-(\d+)", self._text())
        assert match, "redeploy.sh no longer defines a default drain timeout"
        timeout = int(match.group(1))
        assert timeout >= tightest + poke, (
            f"the deploy drain timeout is {timeout}s, but a gated DAG fires "
            f"every {tightest}s and a parked run takes up to {poke}s to record "
            "its gate observation. Raise DEPLOY_DRAIN_TIMEOUT's default, or "
            "loosen the schedule."
        )

    def test_drain_expiry_names_the_blocking_sources(self):
        """Expiry must fail *loudly*: the evidence, and the sources holding it.

        The formatter is lifted out of the script and executed, rather than
        matched against, so this exercises the real embedded program on the
        real shape of the document -- the 2026-08-30 drain-status read, where
        `airflow_gate_observations` was the blocker and its count was rising.
        """
        text = self._text()
        assert "_dump_drain_evidence() {" in text, (
            "redeploy.sh no longer prints drain evidence on expiry (Plan 158 "
            "decision 7); a bound that fails without a diagnosis saves the "
            "operator the wait but not the triage"
        )
        section = text[
            text.index("_dump_drain_evidence() {"):text.index("_prepare_coordination() {")
        ]
        assert "python3 -c '" in section, "the evidence formatter is gone"
        program = section.split("python3 -c '", 1)[1].split("\n'", 1)[0]

        document = {
            "phase": "draining",
            "scope": ["analytics", "detail_fetch", "processing"],
            "drained": False,
            "blockers": ["airflow_gate_observations", "container_processes"],
            "sources": [
                {
                    "source": "airflow_gate_observations",
                    "status": "known",
                    "count": 9,
                    "oldest_started_at": "2026-08-30T06:00:00.690000+00:00",
                },
                {
                    "source": "container_processes",
                    "status": "unknown",
                    "count": None,
                    "oldest_started_at": None,
                    "reason": "evidence adapter not implemented",
                },
                {
                    "source": "running_detail_claims",
                    "status": "known",
                    "count": 0,
                    "oldest_started_at": None,
                },
                {
                    "source": "ops_jobs",
                    "status": "not_applicable",
                    "count": None,
                    "oldest_started_at": None,
                },
            ],
        }
        result = subprocess.run(
            [sys.executable, "-c", program],
            input=json.dumps(document),
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode == 0, result.stderr
        out = result.stdout

        assert "Blocking sources: airflow_gate_observations, container_processes" in out
        assert "airflow_gate_observations: status=known count=9" in out, (
            "the count the operator has to watch across two reads is missing"
        )
        assert "2026-08-30T06:00:00.690000+00:00" in out
        assert "reason=evidence adapter not implemented" in out, (
            "an `unknown` source fails the drain closed; expiry must say why "
            "it could not be read, not merely that it blocked"
        )
        assert "running_detail_claims: status=" not in out, (
            "a drained source is listed as though it were a blocker"
        )
        assert json.loads(out[out.index("{"):]) == document, (
            "the full evidence document is no longer printed, so the operator "
            "cannot see the sources that were *not* blocking"
        )

    def test_drain_expiry_leaves_nothing_mutated_so_intent_releases(self):
        """The bound is only safe because of where it sits.

        `_prepare_coordination` runs before every mutation, so an expiry
        returns with `MUTATED` still 0 and decision 3's trap releases intent --
        the fleet resumes by itself, exactly as it did after the Ctrl-C on
        2026-08-30. A bounded wait placed after a recreate would strand a
        half-deployed fleet instead.
        """
        text = self._text()
        code = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        mutations = [m.start() for m in re.finditer(r"^\s*MUTATED=1", code, re.MULTILINE)]
        assert mutations, "redeploy.sh no longer marks the fleet as mutated"
        for position in mutations:
            assert '_prepare_coordination "$@"' in code[:position], (
                "a container is recreated before coordination is authorized, "
                "so a drain timeout would abandon a half-deployed fleet"
            )
        marker = "ERROR: in-scope work did not drain"
        assert marker in code, (
            "the drain wait no longer expires loudly (Plan 158 decision 7)"
        )
        assert "DEPLOY_DRAIN_TIMEOUT" in code[code.index(marker):], (
            "the expiry message no longer tells the operator which knob raises "
            "the bound when the wait was legitimate"
        )

    def test_both_spellings_of_the_restart_mode_are_accepted(self):
        """The mode is restart-and-verify. It shipped named `--config` after
        the one use case that motivated it, and the second use case -- a
        process holding a cached peer address -- is not a config change. Both
        spellings select the same mode so the call site can read honestly."""
        text = self._text()
        assert re.search(r"^\s*--restart\|--config\)", text, re.MULTILINE), (
            "the restart mode no longer accepts both spellings; `--config` is "
            "referenced by docs/ARCHITECTURE.md and the Plan 144 write-up"
        )

    def test_a_recreate_that_recreated_nothing_says_so(self):
        """`up -d` on an unchanged service leaves the container running and
        exits 0. Correct, and indistinguishable from a real deploy in the
        output -- which is the defect this whole plan is about. Proved by
        dry-run against production on 2026-08-20."""
        text = self._text()
        assert "BEFORE_ID" in text, (
            "redeploy.sh no longer samples container ids before `up -d`, so it "
            "cannot tell a real recreate from a no-op"
        )
        code = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        sample = code.index("BEFORE_ID[$svc]=")
        assert sample < code.index("docker compose up -d"), (
            "container ids are sampled after `up -d`, which compares the new "
            "state against itself and can never report a no-op"
        )

    def test_intent_release_distinguishes_build_from_recreation_failure(self):
        """Defect 3. The old EXIT trap released intent on every exit path,
        which is right after a failed build and not obviously right after a
        recreation that failed halfway."""
        text = self._text()
        assert "MUTATED" in text, (
            "redeploy.sh no longer tracks whether a container was changed, so "
            "it cannot tell a failed build from a partial recreation"
        )
        assert re.search(r'\[ "\$MUTATED" -eq 0 \]', text), (
            "the intent-release condition no longer consults MUTATED"
        )

    def test_coordination_is_authorized_before_mutation_and_validated_after_health(self):
        text = self._text()
        assert "/coordination/begin-drain" in text
        assert "/coordination/authorize" in text
        assert "/coordination/begin-validation" in text
        for mutation in ('PHASE="restart"', 'PHASE="recreate"'):
            assert text.rindex("_prepare_coordination", 0, text.index(mutation))
        assert text.index('_begin_validation\n\n    PHASE="done"') > text.index(
            '_wait_for_health "$@"'
        )


class TestCachedPeerAddressRegistry:
    """Defect 5, Plan 136 D6: a recreate changes an address, and a long-lived
    sender that resolved it once keeps using the dead one.

    This is the single-file bind mount's twin. Both are deploy actions with an
    invisible side effect on a service the operator did not name, and both went
    unnoticed because the thing that broke reports success. The exporter stayed
    healthy, ``up{job="airflow"}`` stayed 1, and ``ct-pipeline-failures`` went
    green on ``noDataState: OK`` for two days and four hours.

    The registry is prose rather than a machine-readable follower list, because
    the script prints it to a human and does not act on it. Restarting a peer
    nobody named is the defect ``--no-deps`` exists to stop.
    """

    def test_the_registry_exists_and_every_entry_names_a_real_service(self):
        entries = load_health_exemptions(_FOLLOWERS)
        assert entries, "deploy-followers.txt has no entries"
        stale = set(entries) - set(_compose_services())
        assert not stale, f"deploy-followers.txt names missing services: {sorted(stale)}"

    def test_statsd_exporter_is_registered(self):
        """The one instance the fleet has actually paid for. It is a UDP
        receiver, and UDP is the whole hazard: a TCP peer sees a connection
        error, re-resolves and recovers."""
        entry = load_health_exemptions(_FOLLOWERS).get("statsd-exporter")
        assert entry, (
            "statsd-exporter is not in deploy-followers.txt. Recreating it "
            "orphans Airflow's long-lived StatsD senders silently; see "
            "docs/plans/plan_136_solver_recycle_and_liveness.md section D6."
        )
        assert "docker restart" in entry, (
            "the entry does not tell the operator what to run; a warning "
            "without a command is how this stayed unfixed for two days"
        )

    def test_the_registry_names_the_senders_to_restart(self):
        """Every long-lived Airflow process inherits STATSD_HOST from the
        x-airflow-common anchor, so all four need the restart -- not just the
        scheduler, which is merely the one that was noticed."""
        entry = load_health_exemptions(_FOLLOWERS)["statsd-exporter"]
        compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
        assert compose["x-airflow-common"]["environment"][
            "AIRFLOW__METRICS__STATSD_HOST"
        ] == "statsd-exporter", "Airflow no longer points at statsd-exporter"
        for service in _compose_services():
            long_lived = service.startswith("airflow-") and service != "airflow-init"
            if long_lived:
                assert service in entry, (
                    f"{service} is a long-lived Airflow process holding a cached "
                    "StatsD address but the registry does not tell the operator "
                    "to restart it"
                )

    def test_the_script_warns_but_does_not_restart_the_peers(self):
        text = _SCRIPT.read_text(encoding="utf-8")
        assert "deploy-followers.txt" in text, (
            "redeploy.sh no longer reads the cached-peer registry"
        )
        code = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        assert not re.search(r"^\s*docker (compose )?restart .*FOLLOW", code, re.MULTILINE)
        assert "cartracker-airflow-scheduler" not in code, (
            "redeploy.sh restarts a peer itself; the deploy's blast radius must "
            "stay inside its argument list, so the registry is printed and the "
            "operator acts"
        )

    def test_restart_mode_does_not_warn_because_restart_keeps_the_address(self):
        """`docker compose restart` reuses the container, so its IP is
        unchanged and no peer is orphaned. If restart mode ever starts
        recreating, this warning has to move with it."""
        text = _SCRIPT.read_text(encoding="utf-8")
        block = text.split('if [ "$MODE" = "restart" ]; then', 1)[1].split("else", 1)[0]
        assert "_print_follower_notes" not in block
        assert "docker compose restart" in block, (
            "restart mode no longer restarts; if it recreates, it changes the "
            "container address and owes the follower warning"
        )


class TestSingleFileBindMounts:
    """Defect 4, and the reason it belongs in a test rather than in a runbook.

    A single-file bind mount pins the inode it resolved to at container start.
    ``git pull`` replaces the file instead of editing it, so the container goes
    on reading the old, now-unlinked one -- and a SIGHUP reload logs
    "Completed loading of configuration file" against the stale config. Plan
    140 found this on 2026-08-20; the Plan 136 Stage 2 deploy hit it again the
    same day (host 519823, container 519700) and was caught only because the
    earlier finding said to look.

    A test cannot forbid single-file mounts -- six exist and all six are
    reasonable. What it can do is make adding a seventh a deliberate act, so
    the trap moves out of an operator's memory and into CI.
    """

    # Mount source -> why it is a file rather than a directory. Anything here
    # needs `redeploy.sh --config <service>`; a reload will not do.
    _KNOWN = {
        "./Caddyfile": "Caddy's config is a single file by design.",
        "./oauth2-proxy/oauth2-proxy.cfg":
            "Single config file; the image is distroless, so the inode check "
            "in --config reports UNVERIFIED rather than passing silently.",
        "./grafana/statsd_mapping.yml": "statsd-exporter takes one mapping file.",
        "./prometheus/prometheus.yml":
            "The mount that produced the finding twice on 2026-08-20.",
        "./loki/loki.yml": "Loki takes one config file.",
        "./promtail/promtail.yml": "Promtail takes one config file.",
    }

    @staticmethod
    def _bind_sources(spec: dict):
        for volume in spec.get("volumes") or []:
            if isinstance(volume, str):
                source = volume.split(":")[0]
            elif isinstance(volume, dict) and volume.get("type") == "bind":
                source = volume["source"]
            else:
                continue
            if source.startswith("."):
                yield source

    @classmethod
    def _file_mounts(cls) -> dict:
        found = {}
        for name, spec in _compose_services().items():
            for source in cls._bind_sources(spec):
                if (_REPO_ROOT / source).is_file():
                    found.setdefault(source, set()).add(name)
        return found

    def test_the_set_of_single_file_mounts_is_the_documented_one(self):
        found = set(self._file_mounts())
        added = found - set(self._KNOWN)
        assert not added, (
            f"{sorted(added)} are new single-file bind mounts. git pull replaces "
            "the file on a new inode, so the container keeps reading the old one "
            "and a SIGHUP reload reports success against stale config. Deploy "
            "these with `redeploy.sh --config <service>`, and add them to "
            "_KNOWN with a reason. See docs/plans/plan_144_deploy_script_hardening.md."
        )

    def test_no_documented_mount_has_quietly_become_a_directory(self):
        gone = set(self._KNOWN) - set(self._file_mounts())
        assert not gone, (
            f"{sorted(gone)} are no longer single-file bind mounts. If they were "
            "converted to directory mounts they are immune to the inode trap; "
            "drop them from _KNOWN."
        )

    @pytest.mark.parametrize("source", sorted(_KNOWN))
    def test_every_known_mount_says_why(self, source):
        assert len(self._KNOWN[source]) > 20

    def test_the_script_offers_a_restart_deploy_path(self):
        text = _SCRIPT.read_text(encoding="utf-8")
        assert "--restart" in text, (
            "redeploy.sh has no restart path, so a bind-mounted config change "
            "has no way to reach production except by hand"
        )
        assert "stat -c %i" in text, (
            "the restart path no longer verifies the loaded file by inode; a "
            "restart that silently did not take would report success"
        )


def _prune_command_index(code: str) -> int:
    """Where the prune is *run* in ``code``, not where it is mentioned.

    Decision 10's refusal message prints the command as operator guidance and
    is defined above the mode dispatch, so an unanchored search finds the
    advice rather than the call.
    """
    match = re.search(r"^\s*docker builder prune\b", code, re.MULTILINE)
    assert match is not None, "no line runs `docker builder prune`"
    return match.start()


def _uncommented(text: str) -> str:
    """The script's logic, with whole-line comments dropped.

    Same device as ``test_exempt_services_are_read_from_the_shared_file``:
    ``redeploy.sh``'s header discusses ``docker builder prune`` at length, so
    anything counting or locating the *command* has to read past the prose
    that explains it.
    """
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


class TestBuildCachePrune:
    """Plan 170 Stage B: a build reclaims the cache it just produced.

    Build cache grew 2.04 -> 7.52 GB in 8 days on the production host while
    images stayed flat, so it is ~90% of all storage growth, and it is produced
    only by builds. The reclaim is therefore attached to the producer rather
    than to a clock, which makes it a property of these scripts and checkable
    here.

    The build paths are *derived* from the scripts rather than listed, so a
    third one cannot land without a prune. ``.github/workflows/ci.yml`` also
    builds and is deliberately out of scope: a GitHub runner is ephemeral and
    its cache does not outlive the job.
    """

    #: Per script, the last thing that must have happened before the prune runs.
    #: Stage B's rule is "after health verification", and the two scripts verify
    #: health differently -- redeploy.sh polls Docker's health state, deploy.sh
    #: signals the ops API once services have come up. The keys are asserted
    #: against the derived build set below, so a new build path fails here until
    #: somebody says what its prune must follow.
    _HEALTH_ANCHOR = {
        "deploy.sh": '"$OPS_URL/deploy/complete"',
        "redeploy.sh": '_wait_for_health "$@"',
    }

    @staticmethod
    def _build_scripts() -> dict[str, str]:
        """``{filename: text}`` for every shell script here that builds an image."""
        found = {}
        for path in sorted((_REPO_ROOT / "scripts").glob("*.sh")):
            text = path.read_text(encoding="utf-8")
            if re.search(
                r"^\s*docker compose build\b", _uncommented(text), re.MULTILINE
            ):
                found[path.name] = text
        return found

    @staticmethod
    def _prune_lines(text: str) -> list[str]:
        """Lines that *run* the prune, not lines that mention it.

        Decision 10's refusal message prints `docker builder prune -a -f` as
        the remedy an operator should run by hand, which is worth keeping --
        so the match is anchored to the start of the command rather than
        looking for the string anywhere on the line.
        """
        return [
            line.strip()
            for line in _uncommented(text).splitlines()
            if re.match(r"^\s*docker builder prune\b", line)
        ]

    def test_the_build_paths_are_the_two_this_host_has(self):
        """The premise the rest of this class rests on. If a third script starts
        building, the reclaim rule has to cover it or be re-argued."""
        assert set(self._build_scripts()) == set(self._HEALTH_ANCHOR), (
            "the set of scripts that build on this host has changed. Plan 170 "
            "Stage B attached the cache reclaim to the producer, so a new build "
            "path needs a prune and an entry in _HEALTH_ANCHOR saying what it "
            "must run after."
        )

    def test_every_build_path_prunes_the_cache_it_produced(self):
        for name, text in self._build_scripts().items():
            assert self._prune_lines(text), (
                f"{name} builds images but never prunes the build cache. Nothing "
                "else reclaims it -- the 9 GB that vanished in early September "
                "was a person running `docker builder prune` by hand, twice."
            )

    def test_the_prune_is_total_and_unattended(self):
        for name, text in self._build_scripts().items():
            for line in self._prune_lines(text):
                assert re.search(r"(?<![\w-])-f(?![\w-])|--force", line), (
                    f"{name}'s prune omits -f, so it blocks on a confirmation "
                    "prompt no deploy is watching"
                )
                assert re.search(r"(?<![\w-])-a(?![\w-])|--all", line), (
                    f"{name}'s prune omits -a, so it reaches only dangling "
                    "records. Measured 2026-09-08: without it the sweep skips "
                    "internal, frontend and shared records and stopped with "
                    "5.72 GB still resident."
                )

    def test_no_size_cap_came_back(self):
        """`--keep-storage` was deployed twice on 2026-09-08 and enforced nothing:
        7.52 -> 5.72 GB with the cap never reached, then 0 B reclaimed against
        6.00 GB resident with `-a` set. No model explains both runs, so the
        policy stopped depending on the flag rather than tuning its value. If
        someone reintroduces a cap, it needs a measurement showing it binds --
        not a number that looks reasonable."""
        for name, text in self._build_scripts().items():
            for line in self._prune_lines(text):
                assert "--keep-storage" not in line and "--reserved-space" not in line, (
                    f"{name}'s prune carries a size cap again. Two production "
                    "runs showed --keep-storage reclaiming nothing it was asked "
                    "to; see decision 9 in redeploy.sh before restoring one."
                )

    def test_the_build_paths_run_the_same_prune(self):
        """Two scripts, one policy. Without this they are two copies that drift."""
        commands = {
            line[line.index("docker builder prune"):]
            for text in self._build_scripts().values()
            for line in self._prune_lines(text)
        }
        assert len(commands) == 1, (
            f"the build paths run different prunes: {sorted(commands)}. The "
            "reclaim policy is one decision, not a per-script preference."
        )

    def test_the_prune_can_never_fail_a_deploy(self):
        """Both scripts run under `set -e`, and in redeploy.sh a non-zero exit
        after a container has been recreated means MUTATED=1 and deploy intent
        HELD -- every gated DAG parked because a cleanup step failed after the
        fleet was already healthy."""
        for name, text in self._build_scripts().items():
            for line in self._prune_lines(text):
                assert "||" in line, (
                    f"{name}'s prune is unguarded under `set -e`, so a prune "
                    "failure fails a deploy that has already succeeded"
                )

    def test_the_prune_runs_after_health_verification(self):
        """``LastUsedAt`` refreshes on a cache *hit*, not just on creation, so a
        finished build stamps everything it touched. Pruning before the deploy
        is verified would also let a cleanup step be blamed for a deploy that
        had not yet passed."""
        for name, text in self._build_scripts().items():
            code = _uncommented(text)
            anchor = self._HEALTH_ANCHOR[name]
            assert anchor in code, f"{name} no longer contains {anchor!r}"
            assert _prune_command_index(code) > code.index(anchor), (
                f"{name} prunes before {anchor!r}. Stage B's rule is that the "
                "reclaim runs after the deploy is verified."
            )

    def test_restart_mode_does_not_prune(self):
        """``--restart`` keeps the image and builds nothing, so it has no cache
        to reclaim. A prune there would be a deploy action with an effect nobody
        asked for -- the family of defect decisions 4 and 5 name."""
        text = _SCRIPT.read_text(encoding="utf-8")
        code = _uncommented(text)
        assert len(self._prune_lines(text)) == 1, (
            "redeploy.sh prunes in more than one place; only the build path "
            "produces cache"
        )
        assert _prune_command_index(code) > code.index('PHASE="build"'), (
            "redeploy.sh's prune is reachable from --restart, which builds "
            "nothing and so has nothing to reclaim"
        )


class TestPreflightDiskGuard:
    """Plan 170 Stage B decision 10: a build that cannot fit does not start.

    A rebuild writes new layers while the images the fleet is running are
    still resident, so a change low in a shared base can rewrite every
    dependent layer at once. Filling ``/`` mid-build is worse than refusing
    early: the build dies on ENOSPC leaving partial layers, and on a deploy
    that has already begun.

    The floor is not a number this stage invented. It is Plan 142's reviewed
    ``HOST_DISK_FLOORS["bytes_available"]``, and the first test below is what
    keeps the shell copy honest against it.
    """

    @staticmethod
    def _floors(text: str) -> list[int]:
        return [
            int(m) for m in re.findall(r"^\s*DISK_FLOOR_BYTES=(\d+)", text, re.MULTILINE)
        ]

    def test_the_shell_floor_equals_the_reviewed_python_floor(self):
        """Two languages, one number. A shell constant that drifts from the
        Python one is the second copy every registry in this repo exists to
        avoid -- and it would drift silently, because nothing else reads it."""
        from scripts.host_maintenance import HOST_DISK_FLOORS

        expected = HOST_DISK_FLOORS["bytes_available"]
        for name, text in TestBuildCachePrune._build_scripts().items():
            floors = self._floors(text)
            assert floors, (
                f"{name} builds images but declares no DISK_FLOOR_BYTES, so it "
                "will start a build on a host with no room for it"
            )
            for floor in floors:
                assert floor == expected, (
                    f"{name} holds DISK_FLOOR_BYTES={floor} but "
                    f"HOST_DISK_FLOORS['bytes_available'] is {expected}. The "
                    "deploy floor is Plan 142's reviewed number, not a second "
                    "one; change both or neither."
                )

    def test_the_floor_clears_the_measured_worst_case(self):
        """The floor is evidence-backed, not inherited.

        Measured twice on 2026-09-08, and the pair is the point -- the CI
        figure is only useful if it bounds the host, and it does:

          CI, x86, no images at all      4.48 GiB   (ceiling)
          production, ARM64, bases held  4.22 GiB   (2m13s, 13 services)

        So the runner over-estimates by about 6%: tight enough to be worth
        watching per-PR, conservative in the right direction. Of the host's
        4.22 GiB, 3.65 GiB was build *cache* rather than new image layers --
        most rebuilt layers deduplicated against ones already present -- which
        is why the transient demand is far below the fleet's 21 GB of images.

        This asserts the relationship survives someone lowering the floor,
        which is the change that would quietly break it.
        """
        from scripts.host_maintenance import HOST_DISK_FLOORS

        measured_ceiling = 4.48 * 1024**3
        assert HOST_DISK_FLOORS["bytes_available"] > measured_ceiling, (
            "the disk floor no longer clears the measured cold-build ceiling "
            "of 4.48 GiB, so a deploy can pass the check and still fill /"
        )

    def test_the_check_runs_before_the_build(self):
        """The whole value is where it sits. Before `docker compose build`
        nothing is drained, nothing is recreated, MUTATED is 0 and the trap
        releases cleanly -- so refusing costs an operator one message. After
        the build it would be refusing a deploy that had already paid for
        itself."""
        for name, text in TestBuildCachePrune._build_scripts().items():
            code = _uncommented(text)
            floor_at = code.index("DISK_FLOOR_BYTES")
            build_at = code.index("docker compose build")
            assert floor_at < build_at, (
                f"{name} checks disk headroom after it has already built. The "
                "check exists to stop a build that cannot fit from starting."
            )

    def test_falling_below_the_floor_refuses_rather_than_warns(self):
        """A warning in a deploy log nobody is tailing is not a control.

        Scoped to the refusal block rather than the whole file, and that is
        load-bearing: both scripts contain `exit 1`/`return 1` elsewhere -- the
        drain timeout for one -- so a file-wide search passes even when the
        guard itself has been changed to return success. Mutation testing
        caught exactly that on 2026-09-08.
        """
        for name, text in TestBuildCachePrune._build_scripts().items():
            code = _uncommented(text)
            start = code.find("Refusing to build")
            assert start != -1, (
                f"{name} no longer refuses when the floor is breached"
            )
            # From the refusal message to the end of its enclosing block: the
            # function's closing brace, or the `fi` of the inline form.
            end = min(
                idx for idx in (
                    code.find("\n}", start),
                    code.find("\nfi", start),
                    len(code),
                ) if idx != -1
            )
            block = code[start:end]
            assert re.search(r"\b(exit 1|return 1)\b", block), (
                f"{name} prints a refusal but does not act on it, so the disk "
                "check is advice rather than a gate. The message is not the "
                "control; the non-zero exit is."
            )

    def test_restart_mode_does_not_check_disk(self):
        """`--restart` builds nothing, so it needs no room to build. Gating it
        would block the one deploy mode that exists to fix a running fleet.

        The guard deliberately sits *before* ``PHASE="build"`` -- that is what
        makes it pre-flight -- so position alone cannot express this. The
        restart branch is sliced out and the call asserted absent from it.
        """
        code = _uncommented(_SCRIPT.read_text(encoding="utf-8"))
        branch_open = code.index('if [ "$MODE" = "restart" ]; then')
        call_at = code.index("\n    _require_disk_headroom\n")
        between = code[branch_open:call_at]
        assert 'PHASE="restart"' in between, (
            "the restart branch could not be located; this test is no longer "
            "reading what it thinks it is"
        )
        assert "\nelse\n" in between, (
            "redeploy.sh checks disk headroom on the --restart path, which "
            "builds nothing and needs no headroom"
        )
