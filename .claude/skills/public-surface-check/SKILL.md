---
name: public-surface-check
description: Check a staged change to this repository's two public surfaces — README.md and ops/templates/info.html — against the repository it describes and against each other, then stamp the staged content so the commit gate clears. Use when either file is staged for commit, or when the public-surface hook blocks a commit. This skill reads only the staged diff: it does not audit the whole file, re-derive the architecture, or edit a plan document.
---

# Checking the public surfaces

Plan 138 owns exactly two public surfaces:

| File | What it is |
|---|---|
| `README.md` | The repository front door. Public the moment it merges |
| `ops/templates/info.html` | The landing page at `https://cartracker.info/`. Public when the ops container is **deployed**, which lags `master` |

Nothing else is in scope. Not `docs/`, not the overviews, not the published
articles — those are Stage 1f's problem and they have their own reckoning.

## Read the diff, not the files

**This is the rule that keeps the skill cheap enough to run on every commit.**

```bash
git diff --cached -- README.md ops/templates/info.html
```

That output is your entire input. Both files are long — the landing page alone
is over 55,000 bytes — and a check that re-reads them both, re-derives the
production architecture, and re-litigates every claim would cost more than it
saves and would be turned off within a week.

So: **only claims that appear in the diff get checked.** A paragraph the diff
does not touch is a paragraph somebody already reviewed. You may open the
surrounding lines for context when a hunk is ambiguous on its own; you may not
walk the file.

**Most invocations should end in under a minute.** A diff that changes wording,
formatting, a heading, an anchor or a link target and asserts nothing new about
the system is a pass — say so in one line, stamp it, and stop. Reach for the
repository only when the diff makes or changes a *claim*.

## What counts as a claim

A sentence that would be false if the repository changed. Three kinds, and the
third is the one that has actually bitten:

1. **A mechanism** — what a service does, what writes where, what owns a
   schedule.
2. **A name** — a container, DAG, view, table, or environment variable.
3. **A quantity** — any count, ratio, duration, or size.

## The two questions

### 1. Is the claim true of the repository right now?

This is where every defect this plan has caught actually lived. Gate 0 found
"36 Flyway migrations" against 49, "266 integration tests" against 468, "971
tests" against 3,661, "eleven Docker containers" against more than two dozen.
**Not one of those was a disagreement between the two surfaces** — each was a
surface that had drifted from the code.

Where the truth lives, for the claims that recur:

| Claim about | Check |
|---|---|
| Airflow DAGs, and what is scheduled | `airflow/dags/`, and the `schedule=` argument in the source |
| dbt models | `dbt/models/` |
| Flyway migrations | `db/migrations/` |
| Long-running services | `container_health.expected.EXPECTED_SERVICES`, which `tests/test_observability_config.py` derives |
| Alert rules | `grafana/provisioning/alerting/rules.yml` |
| The live solver | Compose — the live container is `trawl`; `flaresolverr` is retained and vestigial |
| Scrape backoff | The `ops.ops_detail_scrape_queue` view |
| Mechanism, generally | `docs/ARCHITECTURAL_OVERVIEW.md`, reconciled by Gate 0b |

**Two rules on quantities**, both of them lessons this plan paid for:

- **Round it.** The truth contract's §3 bars exact repository counts on a public
  surface: "more than a dozen DAGs", "20+ dbt models". An exact count is a
  promise to update it, and nobody keeps that promise.
- **Name the set.** A number is wrong if it counts the wrong thing, however
  correct the arithmetic. Gate 0 published "28 services without a profile gate"
  — true, and the wrong set, because it excluded `trawl` and `redis-trawl`, the
  live scrape path. Stage 1f said inodes "fell by roughly two thirds" from a
  whole-volume reading of a mechanism that removed 99.99% of the inodes it was
  pointed at, **and the first correction repeated the mistake.** If you cannot
  say what a number is a fraction *of*, that is the finding.

### 2. Does the other surface still agree?

Only for claims the diff touches. Open the other file and look for the same
subject; if it says something different about ownership, production status or
route access, that is a finding.

The two surfaces **do not owe each other identical copy** — the README is a
technical document and the landing page is a portfolio piece, and they are
allowed to say things differently, at different lengths, to different readers.
They owe each other agreement on substance. Reporting a wording difference as a
defect is the failure mode here; it trains the reader to ignore you.

A reader of both should be able to answer Gate 1's four questions without
reconciling anything:

- What runs in production?
- What is experimental?
- Where does history live?
- What requires authentication?

## Report, then stamp

Say plainly which of the two questions produced each finding, and separate what
you verified from what you judged. If the diff asserted nothing, say that — a
fast pass is the common case and should read like a pass, not like an audit
that found nothing.

Then clear the gate for exactly the content you read:

```bash
git diff --cached -- README.md ops/templates/info.html \
  | shasum -a 256 | cut -d' ' -f1 \
  > "$(git rev-parse --git-dir)/public-surface-stamp"
```

**Stamp only after reporting**, and only when you have actually read the diff.
Re-staging changes the digest and closes the gate again, which is deliberate: a
pass buys passage for the content that was checked and for nothing else.

If you found something, say so and **do not stamp**. The author fixes it and
runs this again. You are not the one who decides that a wrong claim is
acceptable.

## What this skill must never do

- **Audit the whole file.** The diff is the input. A request to review an entire
  surface is a different, larger job, and it is not this gate.
- **Edit either surface.** Report; the author writes.
- **Touch a plan document, `docs/PLANS.md`, a status marker or the archive.**
  Those belong to the `plans` and `close-out` skills.
- **Stamp a diff it did not read**, or stamp around a finding to unblock a
  commit. The gate is worth exactly as much as that rule.
- **Judge the deployed page.** These files are the template and the source; the
  live page lags `master` until the ops container is redeployed. Whether
  `https://cartracker.info/` matches is Stage 6's gate, not this one.
