# The artifact obligation index: what you owe when you add a thing

Plan 162 working analysis, 2026-09-11. Companion to
[the seam census](seam_rule_census.md) and
[the ideal-state spec](seam_ideal_state_spec.md), and the input
[Plan 180](../plans/plan_180_seam_program.md) Stage C needs before it writes
skills, since skills map to tasks and a task is "add one of these".

**What this is.** The rules in [`docs/TESTING.md`](../TESTING.md) are filed by
the stage that added them, and the seam census re-files them by the boundary
they hold. Neither answers the question a person has when they open an editor:
*I am adding a route, what do I owe?* This document answers that question once
per kind of thing that can be added, and it is built from the rules rather than
from memory: every rule already derives the corpus it reads, and the corpus
names the kind.

**Method and confidence.** An AST walk over the 225 tests in `tests/rules/`
resolved, for each test, the transitive closure of helpers it calls and the
repository paths those helpers read. The corpus functions are named in
[`tests/rules/test_testing_contract.py`](../../tests/rules/test_testing_contract.py)
and `tests/service_contracts.py`, and each kind below is the set of rules that
share one. Where a rule reads no corpus function, its kind was taken from the
`Asserted by` row it belongs to, and the section says so. Rules outside
`tests/rules/` that discharge an obligation, such as the dbt gates under
`tests/integration/dbt/` and the runtime plugins, are named where they apply
but were not walked. The join from corpus to kind is a judgement made here;
[what should replace it](#what-this-index-should-become) is the last section.

Three vocabularies are used throughout and are the ones the contract already
has. **Evidence** is what discharges an obligation: a Layer 0 rule, a Layer 1
unit test, a Layer 2 statement execution, a Layer 3 dbt build, a Layer 4
request through the app, or a runtime gate that runs across jobs. **Seam** is
the census's boundary, where the obligation crosses one. **Skill** is the
`.claude/skills/` entry that walks a person or an agent through the obligation
before CI fails on it.

---

## The kinds at a glance

| Kind | Corpus that defines it | Seam | Skill today |
|---|---|---|---|
| [A production SQL statement](#a-production-sql-statement) | `production_sql_files()`, `production_python_files()` | 1 | `add-sql` |
| [A test statement or fixture](#a-test-statement-or-fixture) | `all_test_modules()`, `postgres_test_statements()`, `production_relations()` | 1, 2 | none |
| [A route](#a-route) | `route_handlers()`, `app_routes()`, `contracts/*.json` | 4 | none |
| [A call to another service](#a-call-to-another-service) | `caller_endpoints()`, `owned_hosts()`, `hand_written_callers()` | 4 | none |
| [A service](#a-service) | `service_packages()`, the compose-derived roots | 4, 6 | none |
| [A DAG module](#a-dag-module) | `airflow/dags/` under `production_python_roots()` | 2, 4 | none |
| [A dbt model](#a-dbt-model) | `dbt/models/`, `schema.yml`, `sources.yml` | 3 | none |
| [A migration or a constrained column](#a-migration-or-a-constrained-column) | `check_constrained_columns()`, `production_relations()` | 2 | none |
| [A Compose service, variable or config artifact](#a-compose-service-variable-or-config-artifact) | `docker-compose*.yml`, `.env.example`, the three allowlists | 6, 7 | none |
| [A CI job or pytest step](#a-ci-job-or-pytest-step) | `workflow_steps()`, `pytest_steps()` | 6 | none |
| [A test module or directory](#a-test-module-or-directory) | `_test_directories()`, `_layer_of()`, `producer_shapes()` | cross-cut | `testing-contract` (review only) |
| [A rule](#a-rule) | `rules_directory_tests()`, the `Asserted by` column, the harness | meta | none |
| [A script](#a-script) | `script_buckets()`, `coverage_omissions()` | 1 | none |
| [A third-party dependency or external vocabulary](#a-third-party-dependency-or-external-vocabulary) | `classified_imports()`, `CENSUS`, `constraints.txt` | 2, tier 2 and 3 | none |
| [A generated record](#a-generated-record) | `contracts/`, `contracts/lake_snapshot_manifest/`, the recorded corpora | meta | none |
| [A plan document, recap or index row](#a-plan-document-recap-or-index-row) | `docs/plans/`, `docs/PLANS.md`, `docs/recaps/` | second contract | the `plans` family |

Fifteen kinds. Two more exist in the contract and have nothing to owe yet
because their seams are at grade 0: **a metric name** and **an object-store
key**. Both are named at the end so they are visible as empty rather than
absent.

---

## A production SQL statement

**The finished kind, and the template.** Every obligation is mechanical, the
ledgers are empty, and one skill carries all of it.

**Lives in** `<service>/sql/`, `shared/sql/` when two services issue it, or
`airflow/sql/` for the DAG tree. Loaded by `shared.query_loader.load_query`
and exposed through the service's `queries.py`; the DAG tree loads through its
own `dag_queries.py`, the one declared exemption from the loader clause.

**Must not** appear as a literal in any production module, in any shape: a
call argument, an assignment, a `return`, a dict value. Must not carry a
placeholder in a comment. Must not exist byte-identically under two roots.

**Evidence that discharges it:** a Layer 2 test that executes the statement
and asserts on its result, and the execution-record gate observing the file's
text reach a database client in some CI job. The second is the strong
reading; the first is the one a person writes.

**If it mutates rows,** the caller observes whether anything changed:
`rowcount` reaching a branch, `RETURNING` read, or a settling read the same
function branches on.

**Rules that catch you:** `test_no_production_module_holds_a_sql_statement`,
`test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement`,
`test_every_production_sql_file_is_touched_by_a_layer_2_test`,
`test_no_layer_2_test_executes_a_statement_without_asserting_on_the_result`,
`test_no_sql_comment_contains_a_parameter_placeholder`,
`test_no_two_production_sql_files_hold_the_same_statement`,
`test_the_sql_corpus_shrinks_only_by_naming_the_model_that_absorbed_it`,
`test_every_sql_corpus_exemption_is_declared`,
`test_every_mutation_observes_whether_it_changed_anything`,
`test_every_job_that_runs_pytest_has_its_record_read_by_the_gate`, and the
gate itself, `scripts/check_sql_execution_coverage.py`.

**Skill:** [`add-sql`](../../.claude/skills/add-sql/SKILL.md).

**Residue, declared:** a Spark fragment or DataFrame call is not text and no
rule sees it.

## A test statement or fixture

**What Stage X built for the test side of the same seam.** Obligations are
mechanical; the skill does not exist.

**Lives in** `tests/sql/`, mirroring the test tree down to the module, loaded
by the same loader. A test module may not hold a SQL literal of any kind, seed
or assertion.

**Must** plan against the Flyway-migrated schema whether or not the test that
uses it runs. A statement that is still a template is ledgered under G19 with
its binding named.

**If it stands up a relation production defines,** its columns are a subset
of what the model's `schema.yml` declares. A scratch table standing for
nothing is legitimate.

**If it builds a row in memory,** nothing today checks that the database would
accept it. That is G26 and [Plan 180](../plans/plan_180_seam_program.md)
Stage J, and the obligation will be a fixture factory rather than a rule.

**Rules that catch you:** `test_no_test_module_holds_a_sql_statement`,
`test_every_test_sql_file_is_named_by_the_module_it_mirrors`,
`test_every_test_statement_plans_against_the_migrated_schema` (engine-bound,
in `tests/integration/sql/`), `test_there_is_something_to_check`,
`test_every_test_statement_that_holds_a_template_is_waived`,
`test_no_test_invents_the_shape_of_a_relation_production_defines`,
`test_the_fixture_relation_corpus_is_not_empty`.

**Skill:** none. `add-sql` is written for production statements and its
loader wiring does not apply here.

## A route

**The largest kind, at grade 2, and the one whose obligations change most
under Plan 180.** Everything below is what a route owes today; the ideal spec
says which of these rules retire when the envelope helper and the route
registry land.

**Lives in** a service's router, reachable through the app's routing table.

**Declares** every status code it can produce, and only those, through the
refusal envelope in `shared/api_envelope.py` rather than a literal. Declares
a response model for a JSON body, or a body kind for anything else. Declares
no `422` that no request can trigger. Every declared code is one the standard
names, and for a code the envelope splits, the declared meaning is the meaning
the handler proves.

**Has a caller,** in production code, a Compose healthcheck, Prometheus's
scrape config or the Caddyfile, or is declared externally reachable.

**Its handler's exits are readable:** it returns its declared model and
raises named refusals, so a static reader can see every code. A `Depends` is
an exit the reader cannot read.

**Does not** swallow a failed write and report success.

**Evidence that discharges it:** a Layer 4 test that reaches the route
through the routing table and asserts every status code the handler can
produce. A code triggered by a database outcome is asserted at Layer 4 or 2,
never with a mocked cursor. The service's committed contract is regenerated
and diffed by CI.

**Any double of it in another service's tests** derives from the contract: the
body from `service_response()`, the code from what the contract declares, and
the response object a real `requests.Response`.

**Rules that catch you:** `test_every_route_is_reached_through_the_apps_routing_table`,
`test_no_route_is_hidden_from_the_schema_this_rule_reads`,
`test_every_route_declares_the_statuses_it_can_return`,
`test_no_route_declares_a_422_no_request_can_trigger`,
`test_every_status_code_a_route_can_produce_is_asserted`,
`test_a_database_triggered_code_is_asserted_against_a_real_engine`,
`test_every_handlers_exits_are_readable`,
`test_the_artifact_declares_no_code_its_handler_cannot_return`,
`test_every_declared_code_is_one_the_standard_names`,
`test_every_response_declares_a_shape_or_a_kind`,
`test_every_declared_meaning_of_an_ambiguous_code_is_proven`,
`test_no_call_site_retypes_a_status_the_envelope_declares`,
`test_every_endpoint_has_a_caller_or_is_declared_externally_reachable`,
`test_no_route_swallows_a_failed_write_and_reports_success`,
`test_no_response_model_is_short_of_its_producer`, and the fidelity plugin
`tests/plugins/response_model_fidelity.py`.

**Skill:** none. Stage AL named it, *serve an endpoint*, and Plan 180 Stage C
owns writing it against the ideal corpus rather than today's.

**Ledgers open against this kind:** unreadable exits, retyped statuses,
unproven meanings, bodyless responses, uncalled endpoints, and the `/admin`
307. All drain in Plan 180 Stage D.

## A call to another service

**The caller's half of seam 4.** Today's obligations are comparators; the
ideal is one client that makes them structural.

**Lives in** a production module of the calling service, or a DAG module,
where `post_json` in the plugins is the established copy because the DAG tree
cannot import `shared/`.

**Names the callee** by its owned host, and today that naming is the ledger:
a module that reaches an owned host by hand is in `test_no_module_calls_a_service_by_hand`'s
list until the client seam exists.

**Reads the status** of every response it asks for. **Keeps the outcome** it
asked for; a function returning an outcome type may not be called as a bare
statement. **Branches on meaning,** not on the bare code, where the envelope
gives a code more than one meaning. **A DAG that branches on a status** accepts
only statuses the service it names actually emits.

**Does not import** the other service's package. A shared declaration lives in
`shared/`, or in a file both sides read where one side cannot import.

**Evidence that discharges it:** a Layer 1 test of the caller whose double is
a real response object built from the callee's contract, and, for the seven
endpoints with no caller anywhere, a decision recorded in the ledger.

**Rules that catch you:** `test_no_module_calls_a_service_by_hand`,
`test_the_caller_signature_and_the_resolver_agree`,
`test_every_response_we_ask_for_has_its_status_read`,
`test_no_caller_discards_an_outcome_it_asked_for`,
`test_every_dag_status_check_accepts_only_statuses_its_service_emits`,
`test_no_service_imports_another_services_package`,
`test_no_mock_invents_a_service_response`,
`test_no_mock_invents_a_code_the_service_cannot_answer`,
`test_no_test_fabricates_a_response_objects_behaviour`.

**Skill:** none. Stage AL named it, *call another service*.

**Not yet asserted:** that the request body satisfies the endpoint. No rule
reads an outbound body, and the reason is that there is nothing to key on
until the client takes a request model.

## A service

**A new top-level package.** Most of what it owes is a set of derived rows it
fails until it has.

**Is derived twice, and the two derivations disagree.** `service_packages()`
reads top-level directories holding an `__init__.py` and drives the "enough"
table, the coverage-source rule, the contract-artifact rule and the route
rules. Stage AL's `test_the_service_root_corpus_is_complete` derives a service
from `docker-compose.yml`, as a directory a Compose service builds or mounts
code out of, which is what lets `airflow/dags` count. [Plan 181](../plans/plan_181_rule_readers_are_code.md)
Stage A will create a top-level package that no Compose service builds, and
that is the day the two readers give different answers. Reconciling them is
that stage's first work and is recorded here as the one obligation this kind
cannot currently state.

**Owes a row** in the "enough" table. **Is measured** by coverage: named in
`[tool.coverage.run] source`. **Has a committed contract** under `contracts/`
if it constructs a `FastAPI()`, regenerated and diffed by CI. **Installs under
the pinned stack** for the four packages that decide the schema. **Imports no
other service's package.** **Declares its refusals** through the shared
envelope, or holds the one permitted copy if it cannot import `shared/`,
asserted equal in both directions.

**Evidence that discharges it:** a `tests/<service>/` directory at Layer 1, a
`tests/integration/<service>/` directory at Layer 4 with a `TestClient`, and
a healthcheck in its Compose block.

**Rules that catch you:** `test_every_service_directory_has_a_row_in_the_enough_table`,
`test_every_service_directory_is_measured_by_coverage`,
`test_every_service_has_a_committed_contract`,
`test_the_service_contract_gate_runs_in_ci`,
`test_every_version_that_decides_the_schema_is_pinned_exactly`,
`test_every_ci_install_runs_under_the_pinned_stack`,
`test_no_service_imports_another_services_package`,
`test_the_service_root_corpus_is_complete`,
`test_the_refusal_envelope_is_one_declaration`.

**Skill:** none.

**What a service owes itself is not here,** and that absence is the finding
this index was built to make visible. The "enough" floor's third clause, every
failure branch another service depends on, is judgement. The coverage ratchet
is a repository-wide average that Stage AJ showed cannot see a module with
its whole parsing layer dead. Nothing says a service's own branches are
exercised. That is the within-party frame the seam census cannot host, and it
is the subject of the plan this index precedes.

## A DAG module

**A production module that cannot import `shared/`,** which is the one fact
that shapes every obligation below.

**Lives in** `airflow/dags/`, mounted into the Airflow image with
`airflow/sql/` and `airflow/plugins/` and nothing else.

**Its SQL** lives in `airflow/sql/` and loads through `dag_queries.py`.
**Its status comparisons** against a service are read against what that
service's module emits, resolved by the `<NAME>_URL` constant and the shared
basename; a DAG that resolves to no counterpart fails. **Its Airflow state
words** are replayed against the installed `apache-airflow` in the isolated
venv. **Its outbound calls** are in the by-hand ledger, thirteen of the
seventeen, until the generated registry copy lands under `airflow/plugins/`.
**Its health sensor** is declared in `tests/health_sensor_census.py`, which
both venvs read by path.

**Evidence that discharges it:** a Layer 1 test in `tests/airflow/` that
imports no Airflow, and a DagBag parse in `tests/integration/airflow/` in the
isolated venv. `test_dag_integrity.py`'s `DAG_SPECS` is compared against the
DagBag both ways, so a new DAG missing from the list fails.

**Rules that catch you:** `test_no_production_module_holds_a_sql_statement`,
`test_every_dag_status_check_accepts_only_statuses_its_service_emits`,
`test_every_airflow_state_the_dags_compare_against_is_declared`,
`test_no_module_calls_a_service_by_hand`,
`test_every_service_directory_is_measured_by_coverage`, and the health-sensor
census pair in `tests/airflow/` and `tests/integration/airflow/`.

**Skill:** none.

**Residue:** eleven of the fourteen production parsers no test executes are
in this tree, per Stage AJ's measurement, and whether they are dead or merely
invisible to the unit job's coverage is the re-measurement Plan 180 Stage L
opens with.

## A dbt model

**Seam 3, ideal by delegation.** The obligations are held by dbt itself and by
three gates in `tests/integration/dbt/` rather than by rules in
`tests/rules/`.

**Declares an enforced contract** in `schema.yml`: every column its final
`SELECT` emits, each with a `data_type`, under `contract: {enforced: true}`,
with `on_schema_change: fail` if incremental. **Carries a cadence tag.**
**Names a source** that `sources.yml` declares and the snapshot seeder can
fill.

**Every branch is exercised in both arms by a dbt unit test,** with the branch
list derived from the compiled SQL by `sqlglot`. **Materialises at least one
row** against the fixture, cold and warm, with no waiver list. **Every
declared constraint is shown load-bearing** by mutating the guard that
produces it, or is recorded as decorative with the upstream guard named.

**If it absorbs a `.sql` file,** the change names the model in
`SQL_ABSORBED_BY_DBT`, so the production corpus cannot shrink for free.

**Evidence that discharges it:** the `dbt-models` job's branch-coverage,
non-vacuity and constraint-mutation gates; the `snapshot-dbt` job's build over
production-shaped data with `--require-non-empty`.

**Rules that catch you:** `test_every_dbt_model_declares_an_enforced_contract`,
`test_the_non_empty_gate_reconciles_with_the_dbt_source_list`,
`test_the_snapshot_writer_and_the_source_auditor_include_the_same_tables`,
`test_the_sql_corpus_shrinks_only_by_naming_the_model_that_absorbed_it`,
`test_no_test_invents_the_shape_of_a_relation_production_defines`, the
cadence rule in `tests/dbt/`, and the three Stage S gates.

**Skill:** none.

## A migration or a constrained column

**Seam 2's owner artifact.** A migration is not production SQL and owes no
Layer 2 test; what it owes is that every vocabulary it constrains exists once
in Python.

**Lives in** `db/migrations/`, applied by Flyway under production's own
command in CI.

**Every `CHECK (<column> IN (...))`** has exactly one `StrEnum` in
`shared/db_vocabularies.py`, equal in both directions. **No production module
compares against a bare literal** that is a member of a constrained
vocabulary. **Every `CREATE TABLE`** joins `production_relations()`, so a test
fixture standing for it is held to its columns.

**Evidence that discharges it:** the migration applied in every heavy CI job,
and Layer 2 seeds rejected with `CheckViolation` when they go stale, which is
the constraint policing the integration tests for free.

**Rules that catch you:** `test_every_check_constrained_column_has_one_declared_vocabulary`,
`test_no_module_retypes_a_database_vocabulary_it_could_import`,
`test_the_check_constraint_corpus_is_not_empty`,
`test_no_test_invents_the_shape_of_a_relation_production_defines`.

**Skill:** none.

**Coming:** Plan 180 Stage H converts the constrained columns to enums so a
stale read filter in a `.sql` file or a dbt model raises rather than returning
nothing, and Stage W's corpus reader learns `CREATE TYPE … AS ENUM`. That
stage must update the reader, and `test_the_check_constraint_corpus_is_not_empty`
is what makes forgetting loud.

## A Compose service, variable or config artifact

**Seams 6 and 7, the "one file, two readers" pattern.** The volume of rules
here is intrinsic to configuration having many files and is not a smell.

**A Compose service** has a healthcheck over HTTP if it is owned, a `restart:`
policy or an entry in `maintenance-running-set.txt` saying why not, a
`profiles:` classification if gated, and every image it builds attributed to
a project in the keep-set derivation. CI brings it up from the same Compose
definitions as production, and the difference between the two resolved
configs equals the declared divergence set.

**A variable** that `.env.example` documents is interpolated by some Compose
file, or declared undelivered with the consumer that reads it named. A
variable a Compose file interpolates is documented. References are read from
parsed YAML values, never file text, and `$$` is stripped first.

**A config allowlist entry** in `healthcheck-exemptions.txt`,
`maintenance-running-set.txt` or `deploy-followers.txt` names a real service,
declares a known class, and carries a reason. Plan 180 Stage M tightens the
reason from a length to named parts.

**A config file** the production image must accept, such as `prometheus.yml`,
`loki.yml`, `promtail.yml`, the Grafana dashboards, the `Caddyfile` and
`docker-compose.lakehouse.yml`, has a Layer 0 test at the top level of
`tests/` that parses it and names the one artifact it is about.

**Evidence that discharges it:** a Layer 0 config test naming the artifact,
and the resolved-config diff for anything in a Compose file.

**Rules that catch you:** the eight in `test_env_example_wiring.py`, the ten
in `test_maintenance_running_set.py`, the eleven in `test_image_keep_set.py`,
the four in `test_ci_compose_parity.py`, and the 266 top-level definitions
named in the contract's Layer 0 table.

**Skill:** none.

**Coming:** Plan 180 Stage K moves every inline environment default out of
Python, so a variable is either delivered by Compose or a constant.

## A CI job or pytest step

**Seam 6, and the kind most often added without knowing it owes anything.**

**A job that runs pytest** sets `PYTHONPATH`, runs under the declared-skips
gate through `addopts`, installs under `PIP_CONSTRAINT`, uploads its SQL
execution record, and is named in the coverage gate's `needs`. **A heavy job**
declares no `services:` of its own and starts the Compose definitions.
**A step that invokes `tests/integration/<dir>`** is what makes that directory
not orphaned; a step that `--ignore`s a path is matched by another step in the
same job that invokes it. **A step named `Layer N`** matches the contract's
number for that directory.

**A job that passes `--cov`** also passes `--cov-fail-under`. **The contract
generation job** must survive; deleting it leaves six accurate files going
stale.

**Evidence that discharges it:** the rules below, all Layer 0, all reading
`ci.yml` as YAML rather than text.

**Rules that catch you:** `test_every_integration_suite_is_invoked_by_a_ci_step`,
`test_every_ignored_path_is_invoked_by_another_step_in_the_same_job`,
`test_no_dormant_suite_is_quietly_running`,
`test_every_pytest_invocation_in_ci_sets_pythonpath`,
`test_every_pytest_step_runs_under_the_declared_skip_gate`,
`test_every_job_that_runs_pytest_has_its_record_read_by_the_gate`,
`test_the_coverage_number_the_unit_job_produces_is_consumed`,
`test_every_ci_install_runs_under_the_pinned_stack`,
`test_no_heavy_job_declares_its_own_services`,
`test_every_heavy_job_starts_the_compose_services`,
`test_the_service_contract_gate_runs_in_ci`,
`test_every_layer_number_in_the_code_matches_the_contract`.

**Skill:** none.

## A test module or directory

**The cross-cut.** These are the obligations every test owes regardless of
what it tests, and they are the ones an agent most often meets by failing CI.

**A new directory** has a layer, either by matching a `**Lives in:**` pattern
or by a row in *Where the newer suites sit*. Under `tests/integration/` it is
invoked by a named CI step or declared dormant.

**Patching is `mocker`,** everywhere, with `monkeypatch` reserved for process
state. **Every `read_text` and `write_text` names its encoding.** **No SQL
literal** appears in the module. **A skip** is declared in the registry with
its reason and condition, and a Layer 2 test may not skip at all.

**A mock** hands no literal to a seam whose shape production defines: a
producer's dict comes from `produced_by()`, a service body from
`service_response()`, a relation from the migrated schema, a cars.com status
from the recorded corpus. **A response object** is a real `requests.Response`,
never a bare `Mock` with `.json.return_value` configured.

**A Layer 2 test** asserts something about its result. **A Layer 4 test** of a
route asserts the status code.

**Evidence that discharges it:** the rules below, plus the runtime
declared-skips hook in every job.

**Rules that catch you:** `test_every_test_directory_is_assigned_a_layer`,
`test_patching_is_mocker_everywhere`,
`test_every_text_read_and_write_states_its_encoding`,
`test_no_test_module_holds_a_sql_statement`,
`test_every_declared_skip_names_a_test_that_exists`,
`test_no_declared_skip_sits_at_a_layer_that_admits_none`,
`test_the_declared_skip_registry_only_ratchets_down`,
`test_no_mock_invents_a_shape_production_defines`,
`test_no_mock_invents_a_service_response`,
`test_no_mock_invents_a_code_the_service_cannot_answer`,
`test_no_test_fabricates_a_response_objects_behaviour`,
`test_no_test_fabricates_a_cars_com_status_production_has_never_seen`,
`test_no_test_invents_the_shape_of_a_relation_production_defines`,
`test_no_layer_2_test_executes_a_statement_without_asserting_on_the_result`,
and the seven hook tests in `test_declared_skips.py`.

**Skill:** [`testing-contract`](../../.claude/skills/testing-contract/SKILL.md),
which reviews after the fact and is the only skill that reaches this kind. It
runs the assertions and judges the three rules no test can check; it does not
teach the conventions before the code is written.

**Coming:** Plan 181 Stage A adds the boundary rule, that a module-level
function in a test module is either a test or a fixture, with everything else
in a package coverage measures.

## A rule

**The meta-class, grade 4, and the reason the restructure is safe.** A rule is
a test in `tests/rules/` and owes more than any other kind, because the
contract's whole argument is that a rule nobody has watched fail is a rule
nobody knows anything about.

**Lives in** `tests/rules/`, unless its assertion needs an engine, in which
case it is named in the harness's `ENGINE_BOUND` and lives in the Layer 2
suite that can reach one.

**Is named** in the `Asserted by` column of `docs/TESTING.md`, and the name
resolves to a real test. **Is proved by a mutation** in
`scripts/verify_testing_contract_mutations.py`, whose anchor matches its file
exactly once. **Carries a floor** that asserts non-emptiness or a derived
equality, never a chosen number. **If it grandfathers a violation,** the
waiver names a gap entry that exists and an owner plan that is not archived,
and appears once. **If a stage claims a gap** for it, the gap entry exists.

**Evidence that discharges it:** the row, the mutation watched failing, and
the floor.

**Rules that catch you:** `test_every_test_in_the_rules_directory_is_named_in_the_contract`,
`test_every_asserted_rule_lives_in_the_rules_directory`,
`test_every_asserted_rule_names_a_real_test`,
`test_every_asserted_rule_is_proved_by_a_mutation`,
`test_every_mutation_anchor_still_matches_its_file`,
`test_no_rule_guards_itself_with_a_guessed_number`,
`test_no_waiver_outlives_the_plan_that_owns_it`,
`test_no_gap_entry_outlives_the_plan_that_owns_it`,
`test_every_waiver_names_a_gap_entry_that_exists`,
`test_no_waiver_is_listed_twice`,
`test_every_gap_a_stage_claims_exists`.

**Skill:** none, and this is the kind where the gap costs most: every rule
Stage Y landed shipped with a bug that made the repository look healthier, and
each was caught by writing the mutation rather than by anything a skill
teaches.

**Coming, and this kind changes most:** a skill reference per row (Plan 180
Stage C), an expiry ledger per scaffolding rule with a meta-rule that fails
one whose ledger has emptied (Plan 180 Stage B), the reader in a measured
package with its own unit tests (Plan 181), and retirement of reader and
assertion together (Plan 181 Stage B). After those, a rule is five artifacts:
reader, assertion, mutation, floor, skill, with a ledger and an expiry if it
is scaffolding.

## A script

**Production tooling, classified by directory.**

**Lives in** `scripts/` if CI, an image, a Compose file, an ops route or a
harness hook invokes it, or if production imports it; in `scripts/oneoff/` if
its owning plan has archived and nothing binding names it. Its SQL lives in
`scripts/sql/`.

**Declares its owning plan** in the docstring, which is how the bucket is
derived. An entry in `oneoff/` cites the archived plan.

**Is production Python:** holds no SQL statement, reads no environment
variable with an inline default once Plan 180 Stage K lands, names its
encodings, and classifies its third-party imports.

**Evidence that discharges it:** a Layer 1 test in `tests/scripts/` or
`tests/scripts/oneoff/`, which runs in the unit job either way; only the
coverage denominator differs.

**Rules that catch you:** `test_every_script_directory_is_classified`,
`test_every_unmeasured_script_bucket_is_omitted_from_coverage`,
`test_no_production_module_holds_a_sql_statement`,
`test_every_text_read_and_write_states_its_encoding`,
`test_every_production_import_is_classified`.

**Skill:** none.

## A third-party dependency or external vocabulary

**Tier 2 and tier 3 owners.** What is owed depends on whether the thing can
be asked.

**A new import** across production Python is classified in *How production
reaches an engine*, in both directions, and if it reaches an engine the
execution recorder wraps it. **A version that decides a schema** is pinned
exactly in `constraints.txt`. **`curl_cffi`** is pinned exactly in the
scraper's requirements, because two rules pointing opposite ways require the
impersonation list to equal the installed library.

**A word this repository does not own,** an Airflow state, a curl_cffi
target, an S3 error code, a cars.com status, is registered in
`tests/external_vocabulary_census.py` as `REPLAYED` against the real thing in
a CI job that installs it, or `OUT_OF_SCOPE` with the reason. **cars.com** is
the one tier-3 owner: its corpus is recorded from production by
`scripts/record_cars_com_status_corpus.py`, a test may not fabricate a status
production has never returned, and the code may not leave an observed status
in the `UNKNOWN` catchall.

**Evidence that discharges it:** the replay in the job that installs the
dependency, or the recorded corpus for cars.com.

**Rules that catch you:** `test_every_production_import_is_classified`,
`test_the_recorder_instruments_every_client_production_reaches`,
`test_every_version_that_decides_the_schema_is_pinned_exactly`, and the
sixteen in `test_external_vocabularies.py`.

**Skill:** none.

**Residue, declared:** the census is hand-assembled, so a vocabulary nobody
thought of is invisible to it. The ideal spec proposes generating the member
lists from the interrogation scripts.

## A generated record

**The meta-class's generation gates.** A record is generated from running code
and committed, and the diff is the whole mechanism.

**A service contract** under `contracts/<service>.json` is regenerated by
`scripts/generate_service_contracts.py --check` in CI, exists for every
service that constructs a `FastAPI()` and for no other, and declares no code
its handler cannot return.

**A lake-snapshot manifest record** under `contracts/lake_snapshot_manifest/`
matches the writer for the version it stamps, has an `ops` model declaring
exactly its fields, and is never regenerated once retired.

**A recorded corpus** names which instrument saw each entry and is not empty.

**Evidence that discharges it:** the generation gate in CI, and the registry
rules.

**Rules that catch you:** `test_every_service_has_a_committed_contract`,
`test_the_service_contract_gate_runs_in_ci`,
`test_the_artifact_declares_no_code_its_handler_cannot_return`, the seven in
`test_lake_snapshot_manifest_registry.py`,
`test_the_corpus_records_which_instrument_saw_each_status`.

**Skill:** none.

**Coming:** the route registry and the envelope copies join this kind as
generated artifacts under Plan 180 Stage A, and the ideal spec proposes the
external-vocabulary member lists follow.

## A plan document, recap or index row

**The second contract, filed here because the rules directory is one
directory.** Forty-six of the 225 tests in `tests/rules/` enforce
`docs/PLANS.md`, `docs/plans/`, `docs/recaps/` and the archive, and five more
enforce `README.md`. None of them is about testing. The seam census asked
whether the restructured contract should own them or point at them; this
index points, because they already have what no other kind has.

**Skills:** `plan-draft`, `plan-start`, `plans`, `stage-close`, `close-out`,
`note-evidence`, `plan-week`, `public-surface-check`. This is the only kind
where a person is walked through every obligation before a rule can fail, and
it is the model for what the other fourteen are missing.

**Rules that catch you:** the forty-six in `test_planning_docs.py` and the
five in `test_readme_contract.py`.

---

## Two kinds with nothing to owe yet

**A metric name.** A service emits it, a dashboard or alert rule queries it,
and nothing compares the two. Seam 8, grade 0, and it has already cost eight
silent hours. Plan 180 Stage E gives it `shared/metric_names.py` and the Stage
W pair pointed at observability.

**An object-store key.** `silver_normalized/observations` is typed out in six
modules with no declared constant. Seam 5's channel, grade 1, Plan 180 Stage F.

Both are listed so that a person adding one finds an empty entry rather than
no entry, which is the difference between a known gap and an unknown one.

---

## What the index shows

**Fourteen of fifteen kinds have no skill.** The one that does, SQL, is the
finished seam, and the one family that has several, plan documents, is the
second contract. Every other kind is met by failing CI, which is G31 stated
per kind instead of per rule. Plan 180 Stage C's exit is *every rule names a
skill*; this index suggests the cheaper and more useful exit is *every kind
names a skill*, since a kind is a task and Stage AL's three named skills, serve
an endpoint, call another service, test across the seam, are already kinds
here.

**The service kind cannot say what a service owes itself.** Every obligation
in that section crosses a boundary. The coverage ratchet is the only
within-party instrument and it is a global average. The parsers-nobody-ran
population, the outcome rules, the fixture-fabrication gap and Plan 181's
readers are all within-party obligations filed under other kinds because no
kind holds them. That is the frame the next plan writes, and this index is
where its obligations will be filed once it exists: a per-module branch
obligation under *a service*, *a DAG module* and *a script*, and a reader
obligation under *a rule*.

**Two derivations of "service" disagree,** and Plan 181 Stage A is the day it
matters. Recorded under [A service](#a-service).

**The mechanical map disagrees with the documents in two places worth
knowing.** Thirty-nine tests resolve to no corpus function and no repository
path in the walk. About a dozen are canaries or tests of a hook against stub
reports, and belong to *a rule* as tests of the instrument. The rest are real
rules whose corpus is read at module import time or through a helper the walk
does not classify: the guessed-number rule, the response-object rule, the
producer-shape rule, most of the external-vocabulary census. So the walk
measures less than it claims in one direction, and in the other it credits
`docker-compose.yml` to forty-six planning-docs tests because a shared
constant in the module they import from mentions it. Both are a reader with
a dead branch nobody can see, which is Plan 181's case made by the instrument
that built this index.

## What this index should become

**A rule, not a document.** Every kind above is the set of rules sharing a
corpus function, and every corpus function will be a named, importable member
of Plan 181's package. At that point *which kind does this rule belong to* is
a join, not a judgement: a rule's kind is the kind its corpus declares, a rule
whose corpus declares no kind fails, and a kind with no skill fails. That is
the `Asserted by` column's third duty, after naming a real test and owing a
mutation, and it is what makes this index unable to drift from the rules it
describes.

**A pre-flight, not a review.** The `testing-contract` skill runs the
assertions after the code is written. The same corpus functions, pointed at a
diff, classify the changed files into kinds and print the obligations before
anything fails. Today discovery is serial, one red rule at a time. This is the
one-list version, and it is the mechanical form of "when I add something new I
know what's owed".
