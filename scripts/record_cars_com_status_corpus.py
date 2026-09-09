"""Record which HTTP statuses cars.com has actually returned to this scraper.

Plan 162 Stage AB / CAR-106.

``tests/scraper/`` fabricates cars.com responses at the ``make_cf_session``
seam, and cars.com is not a service this repository builds, so Stage AA's
rule -- a caller's fabricated codes must be codes the callee's committed
contract declares -- has nothing to check them against. There is no
``contracts/cars_com.json`` and there never can be.

**So the corpus is recorded from the one vantage point that can reach the real
thing: production.** A live fetch from CI is not available at any price. The
site is behind Cloudflare, and the 2026-08-14 outage established that even a
bare request from the known-good production IP comes back 1020 -- a TLS
fingerprint artifact, not an IP ban, and not something a GitHub runner is
going to do better at. What production *does* have is 90 days of Loki and 30
days of Prometheus saying what the site actually said.

**This is the half of the corpus/replay split that cannot run in CI, and that
asymmetry is the point rather than a gap.** ``verify_promtail_contract.py``
and ``verify_container_health_docker_contract.py`` both record *and* verify in
CI, because the real thing is an image CI can start. Here the recording is
manual and the verifying is a pytest rule
(``tests/test_external_vocabularies.py``) that reads the committed corpus. One
corpus, two consumers, neither importing the other -- the same shape, with the
recorder moved off the runner because the runner cannot see the subject.

    python scripts/record_cars_com_status_corpus.py --record

Run it from somewhere that can reach Loki and Prometheus. On the production
host their compose names resolve; from a workstation, pass ``--loki`` and
``--prometheus`` explicitly or pipe this file to ``python3 -`` over ssh.

**Loki's default max_query_length is 721h**, so a single ``[90d]`` range
selector is refused by dropping the connection rather than returning an error
body. The queries below walk 13 weekly windows instead, which is also why a
count here is a count of *log lines* rather than of fetches: ``_fetch_url``
logs a 403 and ``scrape_detail_fetch`` logs the same fetch again, so Loki
over-counts relative to Prometheus. **Membership is what this corpus asserts;
the counts travel with the instrument that produced them and are not
reconciled**, because reconciling two instruments that disagree by
construction would be inventing a third number nobody measured.
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CORPUS = REPO_ROOT / "tests" / "fixtures" / "external" / "cars_com_responses.json"

DEFAULT_LOKI = "http://loki:3100"
DEFAULT_PROMETHEUS = "http://prometheus:9090"

WEEK_SECONDS = 7 * 24 * 3600
WEEKLY_WINDOWS = 13  # 91 days, one more than Loki's 90d retention

# The two log shapes that carry a cars.com status. They are different because
# the detail and results scrapers log differently, and a query written for one
# silently returns nothing for the other -- which is how the first pass of this
# recording missed every results-page status.
LOKI_QUERIES = {
    "detail_page": (
        'sum by (http_status) (count_over_time('
        '{service="scraper"} |~ `detail fetch HTTP` '
        '| regexp `detail fetch HTTP (?P<http_status>\\d+)` [7d]))'
    ),
    "results_page": (
        'sum by (http_status) (count_over_time('
        '{service="scraper"} |~ `page \\d+: search_key=` '
        '| regexp `status=(?P<http_status>\\d+)` [7d]))'
    ),
}

# Loki never logs a 200 detail fetch -- the warning fires only on non-200 -- so
# the corpus would claim cars.com has never returned one. Prometheus is what
# says otherwise, and it is a different instrument over a different window.
PROMETHEUS_QUERY = "sum by (outcome) (increase(cartracker_detail_fetch_total[30d]))"


def _get(base: str, path: str, params: dict) -> dict:
    url = base.rstrip("/") + path + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=180) as response:
        return json.load(response)


def _loki_statuses(base: str) -> dict[str, collections.Counter]:
    """Distinct statuses per artifact type, summed over the weekly windows."""
    now = time.time()
    observed: dict[str, collections.Counter] = {}
    for artifact_type, query in LOKI_QUERIES.items():
        counts: collections.Counter = collections.Counter()
        for window in range(WEEKLY_WINDOWS):
            at = int(now - window * WEEK_SECONDS)
            payload = _get(base, "/loki/api/v1/query", {"query": query, "time": str(at)})
            for row in payload["data"]["result"]:
                status = row["metric"].get("http_status")
                if status is None:
                    # A line whose regexp did not match: the results scraper
                    # logs `status=None` when the fetch raised before any
                    # response existed. That is a transport failure, not a
                    # status cars.com returned, so it is not a corpus member.
                    continue
                counts[status] += int(float(row["value"][1]))
        observed[artifact_type] = counts
    return observed


def _prometheus_outcomes(base: str) -> dict[str, int]:
    payload = _get(base, "/api/v1/query", {"query": PROMETHEUS_QUERY})
    return {
        row["metric"]["outcome"]: int(float(row["value"][1]))
        for row in payload["data"]["result"]
    }


def record(loki: str, prometheus: str) -> dict:
    loki_counts = _loki_statuses(loki)
    outcomes = _prometheus_outcomes(prometheus)

    observations = []
    for artifact_type, counts in sorted(loki_counts.items()):
        for status, count in sorted(counts.items(), key=lambda kv: int(kv[0])):
            observations.append({
                "status": int(status),
                "artifact_type": artifact_type,
                "count": count,
                "instrument": "loki",
                "window": "91d",
            })

    # `ok` is the 200 bucket; `403` is its own; `error` is every other status
    # *and* every raised exception, so it cannot name a member and is recorded
    # as a total rather than folded into one.
    if outcomes.get("ok"):
        observations.append({
            "status": 200,
            "artifact_type": "detail_page",
            "count": outcomes["ok"],
            "instrument": "prometheus",
            "window": "30d",
        })

    statuses = sorted({row["status"] for row in observations})
    return {
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": (
            "production Loki (grafana/loki:2.9.8, 90d retention) and Prometheus "
            "(30d retention), read through the compose network"
        ),
        "notes": (
            "Recorded by scripts/record_cars_com_status_corpus.py --record. Not "
            "hand-written: a status added here by hand is a status nobody "
            "observed, which is the fabrication this corpus exists to stop. "
            "Counts are per instrument and are NOT reconciled -- Loki counts log "
            "lines and one fetch logs twice, Prometheus counts fetches. Only "
            "membership is asserted."
        ),
        "unresolved": {
            "prometheus_error_bucket": outcomes.get("error", 0),
            "why": (
                "record_detail_fetch buckets every status that is not 200 or "
                "403 into `error`, together with raised exceptions, so this "
                "number cannot name a member. It is recorded because it is the "
                "size of what Prometheus alone could not have told us -- Loki "
                "is what resolved it into 302, 500, 502, 503 and 504."
            ),
        },
        "statuses_observed": statuses,
        "observations": observations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", action="store_true", help="write the corpus")
    parser.add_argument("--loki", default=DEFAULT_LOKI)
    parser.add_argument("--prometheus", default=DEFAULT_PROMETHEUS)
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="print the corpus instead of writing it, for recording over ssh",
    )
    args = parser.parse_args()

    if not args.record:
        parser.error(
            "this script only records. The corpus is verified by "
            "tests/test_external_vocabularies.py, which runs in CI; the real "
            "thing is not reachable from there. See the module docstring."
        )

    corpus = record(args.loki, args.prometheus)
    rendered = json.dumps(corpus, indent=2, sort_keys=False) + "\n"

    if args.stdout:
        sys.stdout.write(rendered)
        return 0

    CORPUS.parent.mkdir(parents=True, exist_ok=True)
    CORPUS.write_text(rendered, encoding="utf-8")
    print(f"wrote {CORPUS} -- statuses observed: {corpus['statuses_observed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
