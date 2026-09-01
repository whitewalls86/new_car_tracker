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
