"""Plan 162 Stage AB: every word this repository restates that it does not own.

Stage W closed this class for the *database* -- ``shared/db_vocabularies.py``
is derived from ``CHECK`` constraints, and the constraint is the owner. Stages
Z and AA close it for *our own services* -- ``contracts/`` is generated from
each running app, so a caller has an artifact to check itself against. This
module is the residue: vocabularies whose owner is outside this repository
entirely, where there is nothing here to derive them from and a stale word is
therefore silent.

**Ownership is not decided here.** Stage AA's resolver reads
``docker-compose.yml`` and produces three buckets: *ours* (a service with
``build.dockerfile: <pkg>/Dockerfile`` and a ``contracts/<pkg>.json``),
*third-party infra* (a compose service with an ``image:`` and no ``build:``),
and *external* (not a compose host at all). AA keeps the first bucket. The
other two are this census, and every entry below names which one it is so the
split is checkable rather than remembered.

**The verdict vocabulary is two words and the second one carries a reason.**
``REPLAYED`` means a CI step asks the owning system whether the word is still
real. ``OUT_OF_SCOPE`` means it does not, and the entry says why -- which is
the exit clause this stage was written against, taken literally: *either*
replayed *or* declared, never merely listed.

**What replay proves, and the sentence that must travel with it.** Running a
recorded corpus through the real thing proves the *shape* still holds. It does
not prove the meaning has not changed. ``docs/TESTING.md`` already says this
about ``container_health`` and it is true of every entry here: cars.com can
keep returning 403 and start meaning something else by it.

**This module imports nothing**, for the reason
``tests/health_sensor_census.py`` gives about itself. Its readers run in two
different virtual environments -- the main one, where importing ``airflow`` is
a contract violation, and the isolated ``apache-airflow==3.2.0`` venv CI builds
for ``tests/integration/airflow/``, where pytest leaves the repository root off
``sys.path`` and ``from tests.external_vocabulary_census import ...`` raises
``ModuleNotFoundError``. Both readers load this file by path. Importing nothing
is what makes that safe.
"""

# --- verdicts ---------------------------------------------------------------

REPLAYED = "replayed"
OUT_OF_SCOPE = "out_of_scope"

# --- buckets (Stage AA's resolver, consumed rather than re-derived) ---------

INFRA = "third_party_infra"  # a compose service with `image:` and no `build:`
EXTERNAL = "external"  # not a compose host at all

# --- the census -------------------------------------------------------------
#
# Each entry:
#   owner     the system that owns the words
#   bucket    INFRA or EXTERNAL
#   what      the vocabulary, named the way its owner names it
#   sites     where this repository restates or fabricates it
#   members   the restated words themselves, where the vocabulary is a
#             finite set this repository types out. Absent when the
#             corpus is a file instead (cars.com) or when the owning
#             library is imported rather than restated.
#   verdict   REPLAYED or OUT_OF_SCOPE
#   checked   for REPLAYED, the test or script that asks the real thing
#   why       for OUT_OF_SCOPE, the reason -- and it must be a reason, not a cost

CENSUS = (
    {
        "owner": "cars.com",
        "bucket": EXTERNAL,
        "what": "the HTTP status codes a fetch can return",
        "sites": (
            "tests/scraper/conftest.py",
            "tests/scraper/processors/test_scrape_detail.py",
            "tests/scraper/processors/test_scrape_results.py",
        ),
        "verdict": REPLAYED,
        "checked": (
            "tests/test_external_vocabularies.py"
            "::test_no_test_fabricates_a_cars_com_status_production_has_never_seen"
        ),
        "why": (
            "Replayed against production's own record rather than against "
            "cars.com. A live fetch from CI is not available at any price: the "
            "site is behind Cloudflare, and the 2026-08-14 outage established "
            "that even a bare request from the known-good production IP comes "
            "back 1020. What *is* available is 90 days of Loki and 30 days of "
            "Prometheus saying which statuses the site has actually returned "
            "to this scraper, which is a recording of the real thing taken "
            "from the one vantage point that can reach it."
        ),
    },
    {
        "owner": "cars.com (via Cloudflare)",
        "bucket": EXTERNAL,
        "what": "the interstitial <title> markers that mean a challenge page",
        "sites": ("shared/challenge.py",),
        "verdict": REPLAYED,
        "checked": (
            "tests/test_external_vocabularies.py"
            "::test_the_challenge_marker_set_still_classifies_the_recorded_interstitial"
        ),
        "why": (
            "The corpus is a real interstitial captured from production, "
            "tests/fixtures/html/challenge_just_a_moment.html.gz. Plan 128's "
            "outage was eight hours of interstitials counted as successful "
            "scrapes, so this marker set going stale is the failure this "
            "repository has already paid for once."
        ),
    },
    {
        "owner": "Airflow (apache-airflow==3.2.0)",
        "bucket": INFRA,
        "what": "airflow.utils.state.TaskInstanceState",
        "sites": ("airflow/dags/notifications.py",),
        "members": ("failed",),
        "verdict": REPLAYED,
        "checked": (
            "tests/integration/airflow/test_external_vocabularies.py"
            "::test_every_restated_airflow_task_state_is_a_real_member"
        ),
        "why": (
            "The restatement is deliberate and must stay one. notifications.py "
            "imports nothing from Airflow on purpose -- its own docstring "
            "records the commit that put `ti.execution_date` in a task "
            "callback and silenced every pager in the fleet for 268 runs, "
            "because Airflow 3's task SDK RuntimeTaskInstance does not carry "
            "it. So the word stays a literal in the DAG and the CI venv that "
            "has real Airflow is what asks whether it is still real: one "
            "corpus, two consumers, neither importing the other."
        ),
    },
    {
        "owner": "Airflow (apache-airflow==3.2.0)",
        "bucket": INFRA,
        "what": "airflow.utils.trigger_rule.TriggerRule",
        "members": ("one_failed", "all_done"),
        "sites": (
            "airflow/dags/dbt_build.py",
            "airflow/dags/hourly_analytics_refresh.py",
            "airflow/dags/pack_bronze_html.py",
            "airflow/dags/scrape_detail_pages.py",
        ),
        "verdict": REPLAYED,
        "checked": (
            "tests/integration/airflow/test_dag_integrity.py"
            "::test_dag_imports_without_error"
        ),
        "why": (
            "Already replayed, and by an instrument that predates this stage. "
            "BaseOperator validates trigger_rule at construction, so a stale "
            "one raises while the DagBag is built, and CI builds a real "
            "DagBag with real Airflow. Recorded here rather than left out: an "
            "entry that says which existing check covers it is what stops the "
            "next census counting it as a hole."
        ),
    },
    {
        "owner": "curl_cffi",
        "bucket": EXTERNAL,
        "what": "curl_cffi.requests.BrowserType impersonation targets",
        "sites": ("scraper/processors/cf_session.py",),
        "members": (
            "chrome99", "chrome100", "chrome101", "chrome104", "chrome107",
            "chrome110", "chrome116", "chrome119", "chrome120", "chrome123",
            "chrome124", "chrome131", "chrome136", "chrome142", "chrome145",
            "chrome146",
        ),
        "verdict": REPLAYED,
        "checked": (
            "tests/test_external_vocabularies.py"
            "::test_every_restated_curl_cffi_target_is_a_real_browser_type"
        ),
        "why": (
            "Found by measuring rather than inherited from the census, and it "
            "is silent in the direction that matters. cffi_target_for_ua walks this "
            "list to match FlareSolverr's reported Chrome major version; if "
            "curl_cffi adds a newer target and the list does not, the scraper "
            "keeps impersonating an old Chrome while presenting a new "
            "user-agent -- a TLS-fingerprint mismatch, which is the shape of "
            "the 2026-08-14 outage. Nothing raises."
        ),
    },
    {
        "owner": "MinIO / the S3 API (botocore)",
        "bucket": INFRA,
        "what": "the ClientError codes that mean 'no such object or bucket'",
        "sites": ("shared/minio.py",),
        "members": ("404", "NoSuchKey", "NotFound", "NoSuchBucket"),
        "verdict": REPLAYED,
        "checked": (
            "tests/integration/shared/test_external_vocabularies.py"
            "::test_a_missing_object_still_reports_a_code_this_repository_knows"
        ),
        "why": (
            "CI already runs MinIO, and since Stage Q it runs *production's* "
            "-- the definition comes from docker-compose.yml rather than a "
            "services: block, so the store answering is configured the way "
            "the real one is. Asking it for a key that does not exist and "
            "reading the code back off the ClientError costs one request. "
            "This is the cheapest true replay in the census: the corpus is "
            "four strings and the real thing is already running."
        ),
    },
    {
        "owner": "Docker Engine API (via tecnativa/docker-socket-proxy)",
        "bucket": INFRA,
        "what": "the response fields container_health.collector reads",
        "sites": ("container_health/collector.py", "container_health/docker_api.py"),
        "verdict": REPLAYED,
        "checked": "scripts/verify_container_health_docker_contract.py",
        "why": (
            "Already replayed, by the script this stage's pattern is copied "
            "from. Listed so the census is the whole picture rather than the "
            "part this stage happened to build."
        ),
    },
    {
        "owner": "Promtail (grafana/promtail:3.5.8)",
        "bucket": INFRA,
        "what": "the pipeline-stage semantics shared/log_ingestion_policy models",
        "sites": ("shared/log_ingestion_policy.py", "promtail/promtail.yml"),
        "verdict": REPLAYED,
        "checked": "scripts/verify_promtail_contract.py",
        "why": (
            "Already replayed, and the other half of the pattern this stage "
            "generalises. Same reason for listing it as the Docker entry."
        ),
    },
    {
        "owner": "FlareSolverr (ghcr.io/flaresolverr/flaresolverr:v3.4.6)",
        "bucket": INFRA,
        "what": "the /v1 solve response body -- status, solution.cookies, userAgent",
        "sites": ("scraper/processors/cf_session.py",),
        "verdict": REPLAYED,
        "checked": "scripts/verify_flaresolverr_contract.py",
        "why": (
            "**Replayed against the real image, in CI, and the first reading "
            "of this row said otherwise.** It was written claiming a recording "
            "was the best available, on the reasoning that exercising "
            "FlareSolverr means pointing it at cars.com and CI cannot reach "
            "cars.com. That is false: FlareSolverr solves any URL, and a page "
            "with no challenge returns the same envelope as one with. The job "
            "stands up the pinned image and serves it four lines from a thread "
            "in the checker's own process. Four of the six fields the scraper "
            "reads are read with `.get` and a default, so a rename there caches "
            "empty credentials and reports success."
        ),
    },
    {
        "owner": "api.telegram.org",
        "bucket": EXTERNAL,
        "what": "sendMessage status codes and the 4096-character body limit",
        "sites": (
            "airflow/dags/notifications.py",
            "ops/routers/users.py",
        ),
        "verdict": OUT_OF_SCOPE,
        "why": (
            "Replaying it means sending a message. There is no sandbox bot and "
            "no dry-run endpoint: getMe would prove the token works and "
            "nothing about sendMessage, and anything that does exercise "
            "sendMessage pages a human. The restatement is also the *only* "
            "kind this census treats as tolerable -- MAX_MESSAGE_CHARS = 4000 "
            "sits deliberately below Telegram's 4096, so the copy is wrong on "
            "the safe side by construction and drifts toward silence only if "
            "Telegram *lowers* the limit. And a rejected send is already "
            "logged: notifications.py exists because it was not."
        ),
    },
    {
        "owner": "Lakekeeper (quay.io/lakekeeper/catalog:v0.13.1)",
        "bucket": INFRA,
        "what": "the management-API status codes register_lakehouse_warehouse reads",
        "sites": ("scripts/register_lakehouse_warehouse.py",),
        "verdict": OUT_OF_SCOPE,
        "why": (
            "Not because it cannot be replayed -- it is a pinned image and a "
            "compose service, so it can -- but because nothing this "
            "repository ships in production reaches it. The lakehouse profile "
            "is Plan 125 Gate B, run by hand, and standing a catalog and its "
            "Postgres up in CI to check two status codes on a path no "
            "production service takes is cost this stage declines rather than "
            "hides. Revisit when the lakehouse profile is deployed: at that "
            "point this row is a hole and not a decision."
        ),
    },
    {
        "owner": "Prometheus and Loki (prom/prometheus, grafana/loki:2.9.8)",
        "bucket": INFRA,
        "what": "the query-API response envelope -- status, data.result, value[1]",
        "sites": ("ops/coordination_release.py",),
        "verdict": OUT_OF_SCOPE,
        "why": (
            "This entry is a *different defect* from the rest of the census "
            "and the reason is worth keeping. Everywhere else a test "
            "fabricates a response that may be wrong. Here there are no "
            "fabrications at all, because the HTTP seam is never exercised: "
            "tests/ops/test_coordination_release.py patches _prometheus_scalar "
            "(7 sites), _container_health_values (9) and "
            "_loki_has_recent_ingestion (5) -- one level *above* requests.get "
            "-- so payload['data']['result'], result[0]['value'][1] and the "
            "payload.get('status') != 'success' guard are asserted by nothing "
            "at all. A corpus here would buy first coverage, not corrected "
            "coverage, and writing the tests that consume it is a larger job "
            "than this stage. Declared rather than absorbed: 'no fabrications' "
            "must not be allowed to read as 'no gap'."
        ),
    },
    {
        "owner": "git",
        "bucket": EXTERNAL,
        "what": "config keys -- fetch.prune, push.autoSetupRemote",
        "sites": ("scripts/audit_git_refs.py", "scripts/verify_git_ref_hygiene_contract.py"),
        "verdict": OUT_OF_SCOPE,
        "why": (
            "Deliberately excluded, and the plan's reason survives "
            "re-measurement. `git config --get` on a key git has renamed "
            "returns empty, the audit reads that as 'not configured' and "
            "fails. The class this stage exists for is silence; a stale word "
            "here is a loud false failure, which is a different and "
            "self-announcing problem."
        ),
    },
    {
        "owner": "markdown-it-py",
        "bucket": EXTERNAL,
        "what": "token types -- heading_open, inline, link_open",
        "sites": ("scripts/build_public_recaps.py",),
        "verdict": OUT_OF_SCOPE,
        "why": (
            "Excluded, but **the plan's stated reason for excluding it is "
            "wrong and this entry corrects it.** Plan 162 says a stale token "
            "type here 'makes a script error out'. It does not: the walk in "
            "_render simply never matches, no heading gets an id, no link gets "
            "rewritten, and the script exits 0 having produced a subtly broken "
            "page. What actually saves it is an assertion on the rendered "
            "output -- tests/scripts/test_build_public_recaps.py asserts "
            "'<h2 id=\"merges\">' is in the HTML, which goes red the moment "
            "the token type stops matching. The verdict is unchanged; the "
            "reason is not the one that was written down."
        ),
    },
)


def entries_for(verdict):
    """The census rows carrying *verdict*."""
    return tuple(row for row in CENSUS if row["verdict"] == verdict)


def owners():
    """Every distinct owner named in the census, in declaration order."""
    seen = []
    for row in CENSUS:
        if row["owner"] not in seen:
            seen.append(row["owner"])
    return tuple(seen)
