"""A variable the environment documents reaches the service that reads it.

Plan 162 Stage V / CAR-88.

**Found deploying this plan's own change.** Stage P added
``SNAPSHOT_DOWNLOAD_TOKENS`` to ``.env.example`` and to
``ops/routers/snapshots.py`` and never added it to ``docker-compose.yml``. A
variable in ``.env`` reaches a container only if the service names it, so the
new one was inert: the router fell back to the legacy single token, the deploy
reported healthy, the route answered 200, and **a working rotation and a failed
one were indistinguishable from outside.** It surfaced only because the
container was asked what it had loaded rather than whether it was up.

**Both directions are asserted here, and the second one is the larger defect.**

*Documented but never delivered* is the shape above: ``.env.example`` is the
file an operator reads before touching production, so a key it documents that
no service consumes is a lie in the one place someone reads first.

*Interpolated but never documented* is the mirror, and it was already known.
``plan_142_planned_host_maintenance.md`` recorded it while building an
``AIRFLOW_JWT_SECRET`` rotation -- "12 of the 42 variables Compose interpolates
are missing in total" -- and deliberately left it unowned, wanting "either a
full pass over the template or a test asserting every interpolated variable is
documented." A fresh provision from that template did not get a short JWT
secret; it got an **empty** one, and Grafana came up with an empty admin
password. This stage did the pass and wrote the test.

**References are read out of parsed YAML values, never out of the file text.**
``docker-compose.yml`` names ``SNAPSHOT_DOWNLOAD_TOKENS`` in a comment two
lines above the reference that actually delivers it, so a grep would have
counted the comment and the original defect would have passed this rule on the
day it was written. A declaration a comment makes and nothing enforces is the
class this whole plan is about.

**``$$`` is Compose's escape and is stripped before matching.** The two Airflow
health checks use ``$${HOSTNAME}``, which Compose passes through uninterpolated
so the container's own shell expands it against its hostname. It is not a
variable an operator can set, and a rule that did not know this would have
opened by demanding the template document one that nothing reads.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent

#: ``$${VAR}`` -- Compose emits a literal ``${VAR}`` and interpolates nothing.
_ESCAPED = re.compile(r"\$\$\{[A-Za-z_][A-Za-z0-9_]*\}")

#: ``${VAR}``, ``${VAR:-default}``, ``${VAR-default}``, ``${VAR:?err}``.
_REFERENCE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)")

#: ``KEY=value`` and ``# KEY=value`` alike. **A commented key is still
#: documentation**: an operator reads the whole file, and letting ``#`` exempt a
#: key would hand anyone a one-character way around this rule. It is how
#: ``SCRAPER_RESULTS_BASE_URL`` sat undelivered without being counted.
_DOCUMENTED = re.compile(r"(?m)^#?\s*([A-Z_][A-Z0-9_]*)=")


@dataclass(frozen=True)
class Undelivered:
    """A key the template documents that no Compose service delivers.

    Modelled on ``Dormant`` in :mod:`tests.test_testing_contract` and not on
    ``Waiver``, for the reason that class already gives: a waiver is debt --
    it names a violation, an owner plan, and dies when that plan archives.
    These are decisions with no repair pending, and held as waivers they would
    fail the moment Plan 162 archived, with the only remedy being to delete the
    record of why the key is not wired.

    **The stage called this tier "script-only" and the census found the
    category is wider than that.** Its one member is read by
    ``scraper/processors/scrape_results.py`` -- a production module inside a
    Compose service, not a script -- which takes a default that production must
    never override. Naming the tier for the script case would have made this
    entry look like a mis-filing rather than the thing the tier is for, so it
    is named for what is actually true of every member: documented, read by
    something, delivered to no container.

    ``consumer`` is the mechanical half of the declaration and ``reason`` the
    human one. ``consumer`` must be a real file that contains the key, so a
    declaration whose reader is deleted or renamed fails rather than sitting
    here pointing at nothing.
    """

    key: str
    consumer: str
    reason: str
    since: date


UNDELIVERED = (
    Undelivered(
        "SCRAPER_RESULTS_BASE_URL",
        consumer="scraper/processors/scrape_results.py",
        reason=(
            "documented commented-out, and the documentation exists to tell an "
            "operator *not* to set it. Production takes the module's default "
            "and unset is the only value that cannot be wrong: setting it also "
            "switches off the human-cadence pacing, so a value pointing at "
            "localhost in a production .env would scrape un-paced and look "
            "perfectly healthy until cars.com noticed. Only "
            "tests/integration/scraper/conftest.py sets it, to a loopback "
            "origin serving a recorded page."
        ),
        since=date(2026, 9, 8),
    ),
)


@dataclass(frozen=True)
class Undocumented:
    """A variable Compose interpolates that the template deliberately omits.

    The mirror of :class:`Undelivered`, and the tier Plan 142 asked for. Every
    member has a working default, so an operator who never sets it still gets a
    correct stack -- which is the only reason omitting it is defensible. **A
    default alone is not enough to earn an entry here**, and
    ``SNAPSHOT_DOWNLOAD_TOKENS: ${SNAPSHOT_DOWNLOAD_TOKENS:-}`` is why: it is
    defaulted to empty and entirely operator-facing. So the reason is written
    per variable rather than derived from the presence of a ``:-``.

    ``compose_file`` is checked against the files that actually interpolate the
    variable, so an entry that survives the reference it was written for fails.
    """

    variable: str
    compose_file: str
    reason: str
    since: date


UNDOCUMENTED = (
    Undocumented(
        "HTML_COMPRESSION_DICT_ID",
        compose_file="docker-compose.yml",
        reason=(
            "Plan 129. Set on the writer (the scraper) after a dictionary is "
            "registered, which is an operational step and not a provisioning "
            "one. Compose defaults it to empty and the readers treat absence "
            "as 'no dictionary', so a template line would invite setting it "
            "before a dictionary exists to name."
        ),
        since=date(2026, 9, 8),
    ),
    Undocumented(
        "MINIO_BUCKET",
        compose_file="docker-compose.mlflow.yml",
        reason=(
            "the standalone MLflow stack's artifact prefix, defaulted to "
            "`bronze`. The production stack does not interpolate it, so a "
            "template entry would document a variable that the deployment an "
            "operator is provisioning never reads."
        ),
        since=date(2026, 9, 8),
    ),
    Undocumented(
        "ICEBERG_CATALOG_URI",
        compose_file="docker-compose.lakehouse.yml",
        reason=(
            "an alias whose default falls through to LAKEKEEPER_CATALOG_URI, "
            "which the template does document. Two documented names for one "
            "endpoint is how they drift apart."
        ),
        since=date(2026, 9, 8),
    ),
    Undocumented(
        "LAKEHOUSE_LOCAL_ANALYTICS_DIR",
        compose_file="docker-compose.lakehouse.local.yml",
        reason=(
            "a developer's local output path, defaulted to ./.cache/analytics "
            "and interpolated only by the local-only lakehouse override. It is "
            "not part of any provisioned deployment."
        ),
        since=date(2026, 9, 8),
    ),
)

#: Ceilings, not counts -- the ``--cov-fail-under`` idiom pointed the other way,
#: borrowed from ``DECLARED_SKIP_CEILING``. They exist so that a new declaration
#: cannot be a quiet tuple append: the number has to move in the same diff, and
#: the number is what review argues about. Lower them when a stage wires or
#: deletes a variable; never raise one to fit an entry that could have been
#: fixed instead.
UNDELIVERED_CEILING = 1
UNDOCUMENTED_CEILING = 4


def _compose_files() -> list[Path]:
    return sorted(_REPO_ROOT.glob("docker-compose*.yml"))


def _interpolations(node) -> list[str]:
    """Every ``${VAR}`` in the *values* of a parsed Compose document."""
    if isinstance(node, dict):
        return [name for value in node.values() for name in _interpolations(value)]
    if isinstance(node, list):
        return [name for item in node for name in _interpolations(item)]
    if isinstance(node, str):
        return _REFERENCE.findall(_ESCAPED.sub("", node))
    return []


def interpolated_variables() -> dict[str, set[str]]:
    """Variable -> the Compose filenames that interpolate it."""
    found: dict[str, set[str]] = {}
    for path in _compose_files():
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for name in _interpolations(document):
            found.setdefault(name, set()).add(path.name)
    return found


def documented_keys() -> set[str]:
    return set(_DOCUMENTED.findall((_REPO_ROOT / ".env.example").read_text(encoding="utf-8")))


def test_every_documented_key_reaches_a_service():
    """The direction Stage P's defect ran in.

    A key in ``.env.example`` is delivered by a Compose service, or it is
    declared with what reads it and why it is not wired.
    """
    undelivered = documented_keys() - set(interpolated_variables())
    declared = {entry.key for entry in UNDELIVERED}
    assert not sorted(undelivered - declared), (
        "these keys are documented in .env.example and no docker-compose*.yml "
        "delivers them, so an operator who sets one gets nothing and no health "
        "check can tell. Wire the key into the service that reads it, delete "
        "it, or add an Undelivered entry naming its consumer:\n  "
        + "\n  ".join(sorted(undelivered - declared))
    )


def test_no_undelivered_key_is_quietly_wired():
    """The other direction, without which the declaration is an unread comment.

    A key that gets a Compose reference later and keeps its entry would leave
    this file asserting a reason that stopped being true -- the exact drift the
    plan exists against.
    """
    interpolated = set(interpolated_variables())
    contradicted = sorted(
        f"{entry.key} (declared undelivered {entry.since}: {entry.reason})"
        for entry in UNDELIVERED
        if entry.key in interpolated
    )
    assert not contradicted, (
        "these keys are declared undelivered and a docker-compose*.yml "
        "interpolates them anyway; delete the UNDELIVERED entry:\n  "
        + "\n  ".join(contradicted)
    )


def test_every_undelivered_declaration_names_a_consumer_that_reads_it():
    """Existence is not enough; the named file has to contain the key.

    Without this the tier is a comment again -- a consumer that is deleted or
    renamed leaves a declaration pointing nowhere, and the key goes back to
    being documented for no reason with nothing able to say so.
    """
    broken = []
    for entry in UNDELIVERED:
        consumer = _REPO_ROOT / entry.consumer
        if not consumer.is_file():
            broken.append(f"{entry.key}: {entry.consumer} does not exist")
        elif entry.key not in consumer.read_text(encoding="utf-8"):
            broken.append(f"{entry.key}: {entry.consumer} does not mention it")
    assert not broken, (
        "these UNDELIVERED entries name a consumer that does not read the "
        "key, so the declaration explains nothing:\n  " + "\n  ".join(broken)
    )


def test_every_interpolated_variable_is_documented():
    """The direction Plan 142 found and left unowned.

    A variable ``docker-compose.yml`` interpolates with no default and no
    template line does not fail a provision -- it provisions **empty**, which
    is how an Airflow Fernet key and a Grafana admin password both came to have
    no value on a fresh clone.
    """
    undocumented = set(interpolated_variables()) - documented_keys()
    declared = {entry.variable for entry in UNDOCUMENTED}
    assert not sorted(undocumented - declared), (
        "docker-compose*.yml interpolates these and .env.example never tells "
        "an operator to set them, so a fresh provision gets an empty value "
        "rather than a failure. Document the variable, or add an Undocumented "
        "entry saying why its default makes it safe to omit:\n  "
        + "\n  ".join(sorted(undocumented - declared))
    )


def test_no_undocumented_declaration_is_quietly_documented():
    """The mirror's second direction, for the same reason as the first's."""
    documented = documented_keys()
    contradicted = sorted(
        f"{entry.variable} (declared undocumented {entry.since}: {entry.reason})"
        for entry in UNDOCUMENTED
        if entry.variable in documented
    )
    assert not contradicted, (
        "these variables are declared absent from .env.example and are "
        "documented there anyway; delete the UNDOCUMENTED entry:\n  "
        + "\n  ".join(contradicted)
    )


def test_every_undocumented_declaration_names_a_file_that_interpolates_it():
    """A declaration that outlived the reference it was written for.

    The analogue of the consumer check above. A variable that stops being
    interpolated needs no entry, and one whose reference moved to another
    Compose file is a new decision wearing an old declaration.
    """
    interpolated = interpolated_variables()
    broken = []
    for entry in UNDOCUMENTED:
        files = interpolated.get(entry.variable, set())
        if not files:
            broken.append(f"{entry.variable}: no docker-compose*.yml interpolates it")
        elif entry.compose_file not in files:
            broken.append(
                f"{entry.variable}: declared for {entry.compose_file}, "
                f"interpolated by {', '.join(sorted(files))}"
            )
    assert not broken, (
        "these UNDOCUMENTED entries name a Compose file that does not "
        "interpolate the variable:\n  " + "\n  ".join(broken)
    )


def test_neither_ledger_grows_without_the_ceiling_moving():
    """A new declaration is not a quiet tuple append.

    Both tiers opened small and examined -- one member and four. The value of
    that is entirely in it staying argued over, and a ceiling is what makes the
    diff say so.
    """
    assert len(UNDELIVERED) <= UNDELIVERED_CEILING, (
        f"{len(UNDELIVERED)} undelivered keys against a ceiling of "
        f"{UNDELIVERED_CEILING}; wire or delete the key, or move the ceiling "
        "in this diff and say why in review"
    )
    assert len(UNDOCUMENTED) <= UNDOCUMENTED_CEILING, (
        f"{len(UNDOCUMENTED)} undocumented variables against a ceiling of "
        f"{UNDOCUMENTED_CEILING}; document the variable, or move the ceiling "
        "in this diff and say why in review"
    )


def test_both_corpora_are_not_empty():
    """The floor guard Stage X learned the hard way.

    A set difference over an empty corpus is empty, so a broken glob or a
    regex that stopped matching reads as 0 of 0 and passes green -- every rule
    in this file would go quiet at once, and nothing else here could notice.
    """
    assert len(_compose_files()) >= 2, "docker-compose*.yml glob found almost nothing"
    assert len(documented_keys()) >= 20, ".env.example parsed to almost no keys"
    assert len(interpolated_variables()) >= 20, "Compose parsed to almost no variables"
