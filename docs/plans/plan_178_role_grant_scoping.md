# Plan 178: Role Grant Scoping

## What this plan is for

Scopes the database's table grants to what each service actually uses. Roles
created when the database held only the scrape pipeline's own tables still
receive blanket privileges on every table added since, so tables belonging to
later plans carry access nobody chose to give them, and nothing checks it.

## The case

**Raised 2026-09-08**, while landing [Plan 173](plan_173_machine_credential_lifecycle.md)
Stage A. That stage created `ops.machine_tokens` and had to revoke four roles
from it in the same migration, which raised the obvious question about every
table that never did.

### The mechanism

Seven `ALTER DEFAULT PRIVILEGES` rules stand in the schema, set by `cartracker`
and `dbt_user` across `public`, `ops` and `analytics`. Read out of
`pg_default_acl` on a Flyway-migrated database, the `public` one is the
load-bearing one:

```
public | tables | viewer=r, scraper_user=arw, dbt_user=r, airflow_app_user=arw
```

So **every table created in `public` is readable by four roles and writable by
two, automatically, and protecting one takes a deliberate `REVOKE`.** The safe
failure direction is inverted: exposure is the default and restriction is the
thing somebody has to remember. `ops` carries the same shape with
`airflow_app_user` added.

This is not a claim about the migrations; it is a reading of the applied
schema. Production runs Flyway on every deploy and has not drifted from
`db/migrations/`, so **the measurements below are production's state**, not a
model of it.

### `V009`'s own comment has been false since the day it landed

[`V009__authorized_users.sql`](../../db/migrations/V009__authorized_users.sql)
says, twice:

> The cartracker superuser owns these tables; scraper_user and dbt_user get no
> access.
>
> Only cartracker (ops service) accesses these tables. scraper_user and
> dbt_user explicitly excluded (no GRANT).

Measured against the migrated schema:

| Table | scraper_user | airflow_app_user | dbt_user | viewer |
|---|---|---|---|---|
| `authorized_users` | INSERT, SELECT, UPDATE | INSERT, SELECT, UPDATE | SELECT | SELECT |
| `access_requests` | INSERT, SELECT, UPDATE | INSERT, SELECT, UPDATE | SELECT | SELECT |

V009 stated its intent as an **absence of GRANTs**, which is how you say it in
a schema with no default ACL and does nothing at all in one that has four. The
comment describes a decision that was made and never took effect, which is
worse than no comment: it is the sentence a later reader trusts instead of
querying the database.

### The exposure was demonstrated, not inferred

Run against a throwaway Postgres 16 with all 51 migrations applied, connecting
as `scraper_user`:

1. ops writes a pending row into `access_requests` for an attacker-controlled
   address — ordinary public access-request-form behaviour.
2. `scraper_user` selects `email_hash` out of that row.
3. `scraper_user` inserts that hash into `authorized_users` with `role='admin'`.
   **The insert succeeds.**

`AUTH_EMAIL_SALT` never has to leak for this to work, which is the part worth
stating plainly: ops computes the hash itself and stores it in a table the
attacker can already read. `airflow_app_user` reaches ops admin by the same
path. `viewer` — the dashboard's role — reads both tables freely.

The same probe found `scraper_user` able to set `deploy_intent.intent` and to
write a valid `coordination_state` row, so the deploy-safety state machine
[Plan 142](plan_142_planned_host_maintenance.md) built is writable by the
scrape role.

**On severity, stated rather than implied:** every one of these requires
already holding a database credential, and those live in the production `.env`.
None of it is remotely exploitable and nothing here is evidence that anything
has happened. What it costs is precisely the isolation that having separate
roles is *for* — today, compromising the scraper container hands over the
authorization table and the deploy state machine along with it.

### Why it happened, and why the answer is not just a migration

The origin is a schema outgrowing the grants written for it, and it is the
ordinary way this goes wrong rather than anybody's mistake. `V003` — Plan 65,
*"scoped Postgres roles"*, whose stated goal was least privilege — and `V004`
were written against the 18 tables `V001` put in `public`, every one of them
the scrape pipeline's own: artifacts, observations, scrape jobs, dealers, the
dbt locks and `deploy_intent`. Blanket grants across a schema where every table
belonged to one pipeline were a reasonable reading of it. Then
[Plan 82](plan_82_user_management.md) put authorization tables in `public`
(`V009`), Plan 142 put the coordination state machine there (`V043`–`V046`),
and both silently inherited privileges written for a schema that no longer
exists in that form. Nothing announced it. Nothing could have.

Which is why the repair has a third part that is not DDL at all. A migration
fixes the tables that exist today; **the eleventh table added to `public` next
year inherits the same grants**, and the plan reads as done. What closes the
class is an assertion that the grant matrix a migration declares equals the one
the database has, in both directions — the shape
[`scripts/public_surface_gate.py`](../../scripts/public_surface_gate.py)
already runs for two files, and whose own argument applies unchanged here: *a
check you must remember is weaker than one you cannot forget*. This plan's own
origin is a demonstration of that, since `machine_tokens` was protected only
because somebody happened to look.

### What makes this affordable now and was not before

Replacing blanket grants with explicit ones requires knowing which tables each
service actually touches, and until recently that was a guess.
[Plan 162](plan_162_testing_census_and_restructure.md) Stage L moved every
production statement into a `.sql` file reachable from its service's
`queries.py`, so each role's real table set is now derivable from the
repository rather than reconstructed by reading code. The audit that would have
been the expensive half of this plan is mostly a query over files that already
exist.

The one cross-role use in the tree is
[`airflow/sql/delete_stale_emails.sql`](../../airflow/sql/delete_stale_emails.sql),
a single `UPDATE` on `access_requests`. `scraper_user`, `dbt_user` and `viewer`
touch neither table anywhere.

### `V015` writes the right grant and the wrong one into the same file

The clearest evidence that the intent exists and the mechanism defeats it is
[`V015__airflow_app_user.sql`](../../db/migrations/V015__airflow_app_user.sql),
which does both within sixteen lines:

```sql
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_app_user;  -- line 22
...
GRANT SELECT, UPDATE ON public.access_requests TO airflow_app_user;               -- line 36
GRANT SELECT ON public.authorized_users TO airflow_app_user;                      -- line 37
```

Lines 36 and 37 are the decision — precisely the access
`delete_stale_emails.sql` needs, named per table, with `INSERT` deliberately
absent. Line 22 had already granted a superset of it fourteen lines earlier,
including the `INSERT` on `authorized_users` that makes the escalation above
work. The narrow grants are not wrong; they are unreachable, because nothing
revokes what the blanket one gave.

So this plan is not introducing least privilege to a codebase that never wanted
it. Plan 65 named it as the goal, V009 stated it in prose, V015 wrote it as
SQL — and all three were overwritten by a rule set in `V003` that none of them
mentions. The repair is to make the intent that is already written down take
effect, and to add the check that would have caught any of the three.

### Not to be confused with Plan 176

[Plan 176](plan_176_role_connection_limits.md) is also about Postgres roles and
is a different subject: it right-sizes `metrics_user`'s `CONNECTION LIMIT`,
which is a resource cap. This plan is about what a role may read and write.
Neither depends on the other.

## Design

The repair is three things and only one of them is DDL. A migration fixes the
tables that exist today; what closes the class is that the intent stops being
prose and becomes something a test holds the database to.

### The declared matrix is a manifest, asserted in both directions

A checked-in matrix of role, table and privilege, compared against a live
Flyway-migrated database: **an undeclared grant fails, and a declared grant the
database does not have fails too.** One direction alone is half a check — the
first catches the blanket grant this plan is about, the second catches a revoke
that went further than intended and quietly broke a service.

This is a shape the repository already runs three times — `PRODUCTION_SQL_MANIFEST`,
[`shared/db_vocabularies.py`](../../shared/db_vocabularies.py) and Plan 142's
`maintenance-running-set.txt` — and the relationship is the same one
`db_vocabularies` has with the `CHECK` constraints it copies: **the migration is
the owner and the manifest is the copy**, kept honest by a test that fails when
they disagree rather than by anyone remembering.

**Rejected: deriving the matrix from the `.sql` corpus.** Tempting, because
Plan 162 Stage L means the corpus exists and is complete. But telling `INSERT`
from `SELECT` per table means parsing SQL, and dbt composes its statements at
run time, so the derivation would be incomplete in exactly the place — dbt_user
— where it currently has the widest grants. A declared list a test enforces
beats a derivation that is silently partial.

**Rejected: a denylist of sensitive tables.** It protects what somebody
remembered to list. That is the failure mode this plan exists to end, and
`ops.machine_tokens` is the proof: it is protected today only because Plan 173
happened to look.

### The default ACLs are dropped, not worked around

Leaving them in place and revoking per table *is* the current state — exposure
by default, restriction as the thing someone has to remember. Dropping them is
what inverts the safe-failure direction, so that a table added to `public` in
two years is private until a migration says otherwise.

One implementation constraint, recorded now because it is easy to miss and the
result of missing it is a revoke that appears to work: **default ACLs belong to
the role that granted them.** Six of the seven were set by `cartracker` and
`V008`'s were set `FOR ROLE dbt_user`, so the revokes have to name each granting
role separately. A single `ALTER DEFAULT PRIVILEGES ... REVOKE` as `cartracker`
silently leaves `dbt_user`'s in place.

**Rejected: moving the authorization and coordination tables into their own
schema.** It is the intuitive fix and it is the wrong size. It solves a problem
that dropping the default ACLs already solves, and it costs every unqualified
reference to those tables across the services, the DAGs and the dbt project —
`search_path` breakage being the exact failure
[Plan 162](plan_162_testing_census_and_restructure.md) recorded as hanging the
drain in production. Once `public` is no longer dangerous by default, the schema
boundary buys nothing that the grant already gives.

### Layer 2 becomes the safety net, by connecting as the role it is testing

This is what makes the migration safe to write, and it is the reason the harness
change is sequenced *before* the DDL rather than after it.

A `REVOKE` that goes too far fails at run time and fails quietly: a `SELECT`
returns nothing where it used to return rows, an `INSERT` raises inside a
handler. Nothing in the test suite would catch it, because Layer 2 connects as
`cartracker` — a role that has every privilege, and therefore the one role that
can never demonstrate a missing one.

Since Plan 162 Stage L every production statement is a `.sql` file that Layer 2
already executes against a real engine. Pointing each module at **the role its
service actually connects as in production** — `scraper_user` for the scraper's
statements, `dbt_user` for dbt's, `viewer` for the dashboard's — turns a missing
privilege into a failing test. The corpus, the engine and the per-service split
all already exist; what is missing is one connection parameter, and it converts
the whole existing suite into the instrument this plan needs.

## Stages

| Order | Stage | What it delivers | Estimate | State | Issue |
|---:|:---:|---|---:|---|---|
| 1 | [**A**](#stage-a--the-measured-needs-matrix) | The measured needs matrix, per role | 2 | `next` | CAR-110 |
| 2 | [**B**](#stage-b--layer-2-connects-as-the-role-it-is-testing) | Layer 2 connects as the role it is testing | 3 | -- | CAR-110 |
| 3 | [**D**](#stage-d--the-grant-matrix-assertion) | The grant-matrix assertion, both directions | 2 | -- | CAR-110 |
| 4 | [**C**](#stage-c--drop-the-default-privileges-and-grant-explicitly) | Drop the default privileges and grant explicitly | 2 | -- | CAR-111 |

**Letters run A, B, D, C because letters record when a stage was thought of and
numbers record when it is worked.** D moved ahead of C during the interview, for
a reason worth keeping: the assertion mechanism is identical whether the
manifest describes the broken state or the fixed one, so building it first makes
Stage C's diff *the manifest change*, with the test proving the database moved
exactly with it and no further. The cost, accepted deliberately, is that D ships
a test blessing the current grants for as long as C takes to land.

### Stage A — The measured needs matrix

Derive, per role, the tables and privileges its service's statements actually
require, from the `.sql` corpus and each service's `queries.py`. No DDL, no
harness change — this stage produces the document every later stage is checked
against.

**Exit:** the manifest exists, and the delta against the grants the database has
today is recorded in `## Record` with every difference classified as either
needed-and-missing or granted-and-unneeded. A difference nobody can classify is
the finding, not an omission.

### Stage B — Layer 2 connects as the role it is testing

Point each Layer 2 module at the role its service connects as in production,
so that a privilege a statement needs and its role lacks fails a test rather
than a request.

**Exit:** the suite passes with per-role connections, **and revoking one
privilege makes a named test fail** — demonstrated by actually revoking it, not
asserted. A net nobody has seen catch anything is not yet a net.

### Stage D — The grant-matrix assertion

Compare the manifest against a live migrated database, both directions, in CI.

**Exit:** the test fails on an undeclared grant and on a declared grant the
database does not have — both demonstrated — and a table added to `public` with
no manifest entry fails. That last case is the one that makes this stage worth
building: it is the failure the next Plan 82 would otherwise inherit in silence.

### Stage C — Drop the default privileges and grant explicitly

The migration. Revoke the seven default ACLs, naming each granting role
separately; revoke the blanket `GRANT ... ON ALL TABLES` statements; grant per
Stage A's matrix.

**Exit:** on production, the grant matrix equals the manifest, Stage B's suite
passes for every role, and no service reports an error across a full scrape and
dbt cycle. The escalation demonstrated in [the case](#the-exposure-was-demonstrated-not-inferred)
is re-run against production and refused.
