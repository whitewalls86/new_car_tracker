# Plan 162 Stage V — evidence

**Issue:** CAR-88 · **Measured and demonstrated:** 2026-09-08

Everything bulky about Stage V: the census run both ways, the answer
established for each of the keys the stage named, the scope change that made
the reverse direction part of this stage, and the six mutations watched
failing.

---

## 1. The census, and why it is read out of YAML values

**37 active keys in `.env.example`, three delivered by no Compose service** —
`FASTAPI_ADMIN_KEY`, `MLFLOW_TRACKING_URI`, `PROVENANCE_ENV`. That reproduces
the measurement the stage was opened on. `SNAPSHOT_DOWNLOAD_TOKENS`, the fourth
in the original count, was wired by `8acf088` when the defect was found.

**Two derivations, run independently, agreeing.** A text grep over
`docker-compose*.yml`, and a parse that walks only the *values* of each loaded
YAML document. They return the same three keys, which is what makes the census
trustworthy — but the parse is what shipped, because the two are not equally
sound:

- **`docker-compose.yml:169` names `SNAPSHOT_DOWNLOAD_TOKENS` in a comment,
  three lines above the reference at `:172` that actually delivers it.** A grep
  counts the comment. So a grep-based rule would have passed Stage P's original
  defect on the day it was written — the variable would have looked wired
  because a comment said so. A declaration a comment makes and nothing enforces
  is the class this whole plan is about, and it would have been baked into the
  mechanism meant to catch it.
- No compose file uses `env_file:`, and all 38 `environment:` blocks are
  mappings rather than lists. So the value walk is exact here rather than a
  heuristic: every delivery in this repository is `KEY: ${KEY…}`.

**`$$` is Compose's escape and is stripped before matching.**
`docker-compose.yml:840` and `:856` use `$${HOSTNAME}` in the DAG-processor and
triggerer health checks — Compose emits a literal `${HOSTNAME}` and the
container's own shell expands it against its hostname. It is not a variable an
operator can set. Counting it produced a false positive in the first run of the
reverse census, and mutation 6 below is that discovery kept as a regression.

## 2. The three keys, answered

The stage's exit refuses waiving all three to make a new assertion pass. Each
got a different answer.

| Key | Answer | What established it |
|---|---|---|
| `FASTAPI_ADMIN_KEY` | **Deleted** | `git log -S` across all history: it entered `.env.example` in `eb96c41` (Plan 65 auth stack, 2026-04-09) and has never appeared in any file but `.env.example` and plan documents. No Python, YAML, shell or Caddy consumer, under that name or any case variant of `admin_key`. It was never wired because it was never read. |
| `MLFLOW_TRACKING_URI` | **Deleted from the template** | Read only by `scripts/log_lakehouse_experiment_provenance.py:116`, as an argparse default. It configures the provenance *client*; `docker-compose.mlflow.yml` sets the server's own backend store. In-development work whose variables do not belong in the file an operator reads before touching production. |
| `PROVENANCE_ENV` | **Deleted from the template** | Same script, line 108, same shape. |

The MLflow block was removed verbatim rather than rewritten, so it can be
restored unchanged when the work that needs it lands.

**A fourth key the stage did not name.** `SCRAPER_RESULTS_BASE_URL` is
documented in `.env.example` commented-out and is interpolated by no Compose
file — invisible to a census that reads only `^KEY=` lines. It is in the corpus
now, because a key an operator reads is documentation whether or not a `#`
precedes it, and letting `#` exempt a key hands anyone a one-character way
around this rule.

## 3. The tier is wider than "script-only", and is named for what is true

The stage specified declaring a key **script-only** with its consumer named.
The census found that label does not fit its one member.
`SCRAPER_RESULTS_BASE_URL` is read by `scraper/processors/scrape_results.py:34`
— a production module inside a Compose service, not a script — which takes a
default that production must never override. Setting it also switches off the
human-cadence pacing, so a value pointing at localhost in a production `.env`
would scrape un-paced and look perfectly healthy until cars.com noticed.

Naming the tier for the script case would have made its only member look like a
mis-filing. It is named `Undelivered` for what is actually true of every
member: documented, read by something, delivered to no container.

## 4. The reverse direction, and the plan that already found it

**Added to this stage on 2026-09-08 at the user's direction**, after the
forward census was complete. The stage as written enforced one direction and
recorded the other as a non-goal.

**It was already known and already unowned.**
`plan_142_planned_host_maintenance.md:812`, under the heading *"A gap found
while building item 6, and deliberately not fixed here"*, records that
`.env.example` documents **none of the seven Airflow variables**
`docker-compose.yml` requires, counts "12 of the 42 variables Compose
interpolates are missing in total", and declines to fix it — wanting "either a
full pass over the template or a test asserting every interpolated variable is
documented". This stage did both. The census here reproduces Plan 142's 12
exactly when restricted to `docker-compose.yml`; Plan 142 counted `HOSTNAME`,
which §1 above shows is not an interpolation at all.

**14 variables interpolated and undocumented. Eight of them required with no
default, all in production `docker-compose.yml`:**

`AIRFLOW_JWT_SECRET` · `AIRFLOW_FERNET_KEY` · `AIRFLOW_DB_PASSWORD` ·
`AIRFLOW_APP_DB_PASSWORD` · `_AIRFLOW_WWW_USER_PASSWORD` · `GRAFANA_ADMIN_USER`
· `GRAFANA_ADMIN_PASSWORD` · `METRICS_DB_PASSWORD`

A fresh provision from this template did not get a weak Fernet key or a short
JWT secret. It got an **empty** one, and Grafana came up with an empty admin
password. This is the same failure as Stage P's running the other way, and it
is the larger of the two.

**`METRICS_DB_PASSWORD` is the sharpest instance.** `docker-compose.yml:84–91`
substitutes six role passwords into Flyway placeholders, so each role is
*created* with whatever value is set. `.env.example` documented three of them
under "Scoped Postgres role passwords (Plan 65)" and silently omitted the other
three. All six are documented now, in one block, with what each role is for.

**Six more are interpolated with a working default** — `AIRFLOW_UID`,
`_AIRFLOW_WWW_USER_USERNAME`, `HTML_COMPRESSION_DICT_ID`, `MINIO_BUCKET`,
`ICEBERG_CATALOG_URI`, `LAKEHOUSE_LOCAL_ANALYTICS_DIR`. The first two are
documented anyway, as part of the Airflow block an operator needs whole. The
other four are declared, each with its own reason.

**A default is not a mechanical exemption**, and
`SNAPSHOT_DOWNLOAD_TOKENS: ${SNAPSHOT_DOWNLOAD_TOKENS:-}` is why: defaulted to
empty and entirely operator-facing. Deriving the tier from the presence of a
`:-` would have exempted the very variable this stage exists because of. So
each declaration carries a written reason instead.

## 5. Both ledgers carry three directions, not two

`DORMANT_SUITES` established two: an undeclared member fails, and a declared
member that stops being true fails. Both apply here in both ledgers.

The third is what stops a tier from being a comment again:

- an `Undelivered` entry whose named consumer does not contain the key fails,
  so a reader that is deleted or renamed does not leave a declaration pointing
  nowhere;
- an `Undocumented` entry whose named Compose file does not interpolate the
  variable fails, so a declaration cannot outlive the reference it was written
  for.

Both tiers also carry a ceiling, borrowed from `DECLARED_SKIP_CEILING`, so a
new declaration cannot be a quiet tuple append.

## 6. The six mutations, watched failing

Baseline before and after every mutation: **8 passed**. Each mutation was
applied to a restored tree, not stacked.

### 6.1 An undelivered key is added to `.env.example`

```
AssertionError: these keys are documented in .env.example and no
docker-compose*.yml delivers them, so an operator who sets one gets nothing and
no health check can tell. ...
assert not ['STAGE_V_MUTANT_KEY']
FAILED test_every_documented_key_reaches_a_service — 1 failed, 7 passed
```

### 6.2 The same key, named **only** in a `docker-compose.yml` comment

The mutation that justifies the YAML parse. A grep-based rule passes here.

```
assert not ['STAGE_V_MUTANT_KEY']
FAILED test_every_documented_key_reaches_a_service — 1 failed, 7 passed
```

### 6.3 A Compose service interpolates an undocumented variable

```
AssertionError: docker-compose*.yml interpolates these and .env.example never
tells an operator to set them, so a fresh provision gets an empty value rather
than a failure. ...
assert not ['STAGE_V_NEW_SECRET']
FAILED test_every_interpolated_variable_is_documented — 1 failed, 7 passed
```

### 6.4 A declared-undelivered key is wired into Compose after all

```
AssertionError: these keys are declared undelivered and a docker-compose*.yml
interpolates them anyway; delete the UNDELIVERED entry:
assert not ['SCRAPER_RESULTS_BASE_URL (declared undelivered 2026-09-08: ...)']
FAILED test_no_undelivered_key_is_quietly_wired — 1 failed, 7 passed
```

### 6.5 The declaration names a consumer that does not read the key

`consumer` repointed from `scraper/processors/scrape_results.py` to
`scraper/app.py`, a real file that does not mention it.

```
AssertionError: these UNDELIVERED entries name a consumer that does not read
the key, so the declaration explains nothing:
assert not ['SCRAPER_RESULTS_BASE_URL: scraper/app.py does not mention it']
FAILED test_every_undelivered_declaration_names_a_consumer_that_reads_it
1 failed, 7 passed
```

### 6.6 The `$$` escape-strip is removed

The false positive from the first census run, kept as a regression.

```
assert not ['HOSTNAME']
FAILED test_every_interpolated_variable_is_documented — 1 failed, 7 passed
```

## 7. Suite

`pytest -m "not integration"` — **3809 passed, 694 deselected in 52.74s**.
`tests/test_testing_contract.py` passes with the new row, which is
`test_every_asserted_rule_names_a_real_test` confirming all eight named
functions exist. `ruff check` clean on the new module.

## 8. One limit, stated rather than closed

**This rule proves delivery, not arrival.** It asserts that a Compose service
names the variable — which is exactly the link Stage P's defect broke — and it
cannot assert that the running container loaded a value. The original defect
was found by asking the container what it had loaded rather than whether it was
up, and nothing here replaces that. A key wired into Compose and left unset in
the VM's `.env` still reaches the service as empty, and this file will be
green.

**The stage did not turn production-gated.** The issue warned it might, and
noted that would be a change of kind rather than a missed estimate. It did not:
all four documented-but-undelivered keys resolved to delete-or-declare, no
variable needed wiring into `docker-compose.yml`, and `.env.example` is a
template rather than a deployed file. Production's live `.env` already carries
the eight secrets, which is why the VM runs. Nothing here requires a deploy to
prove it arrived.
