# Run Sheet: Plan 145 Stage 6 — Repack, Retire, Prune, Delete

The last stage. It ends with 1,172 legacy Parquet objects gone and ~13.66 GiB
reclaimed, which is the thing the whole plan exists to do.

Nothing here has been run. The machinery is built and unit-tested; see
[the handoff](../plans/plan_145_stage_6_handoff.md) for why each piece is
shaped the way it is.

**Steps 5, 6 and 7 are irreversible.** Step 7 is recoverable only from MinIO
versioning or backup, which is why it takes a name.

---

## 0. Preflight — the blocker is cleared, confirm it anyway

Stage 6 depends on Stage 5, and not merely by convention. The packer reads
`silver_normalized/observations` and `ops_normalized/artifacts_queue_events`
for member identity. Repacking before the recovered rows are **flushed out of
staging** mints ~426k replacement sidecars with NULL identity, and only another
full repack could repair them.

**Both conditions were met on 2026-08-29**, 14:40–15:30 UTC: the full apply
committed 341,903 artifacts and 701,375 silver rows across 70 receipts, and
staging drained to 0. See *Evidence — slice 3 Phase B and the full apply* in
the plan. Stage 6 is unblocked; confirm rather than assume, because a repack is
a night's work to redo:

| | how to know |
|---|---|
| the full `apply` committed every batch | `public.plan145_recovery_batch_receipts` holds **70** rows — 69 batches plus the canary |
| the flush has run | `staging.silver_observations` is drained and the rows are in `silver_normalized/observations/` |

```bash
ssh -i ssh-key-2026-04-08.key ubuntu@147.224.199.86 \
  'docker exec cartracker-processing psql "$DATABASE_URL" -Atc "
    SELECT (SELECT count(*) FROM public.plan145_recovery_batch_receipts) AS receipts,
           (SELECT count(*) FROM staging.silver_observations)            AS unflushed,
           (SELECT count(*) FROM staging.artifacts_queue_events
             WHERE status = %s)                                          AS recovered
  " -v ON_ERROR_STOP=1' 2>/dev/null
```

Expect **70 receipts**, **0 unflushed** and **0 recovered**. Anything else:
stop, and finish Stage 5.

One thing to know before step 3: the 341,903 recovered queue events landed in
`ops_normalized/artifacts_queue_events/year=2026/month=8/`, not April, because
`build_recovery_queue_event` leaves `event_at` to `now()` by design. The
packer's `paths` CTE globs every partition and filters on `minio_path`, not on
a date, so it picks them up — but do not "fix" that glob to an April window.

### Where to run these

Same rules as Stage 5 — see that run sheet's §0. In short: `compose run`, not
`docker exec`, so you are not running stale code; `april-processor` for the
pyarrow modes; a tmux session, because step 3 outlives your SSH connection.

---

## 1. The order, and why it is this order

```
  2. pack-trial            → which ordering carries the full pass
  3. pack_bronze_html      → the replacement packs (long)
  4. repack-verify         → the gate.  Everything below is checked against it
  5. retire-packs          → 64 keys                        IRREVERSIBLE
  6. delete_packed_source  → ~983k loose objects            IRREVERSIBLE
  7. delete-legacy         → the 1,172                      IRREVERSIBLE
```

Retirement precedes the prune so the prune sees only the replacement packs.
Both precede the legacy deletion, which is last because it is the only step
with no copy left anywhere.

---

## 2. The ordering trial

Dry run first — it reads no object and tells you the sample composition:

```bash
python -m scripts.reconcile_april_detail pack-trial
```

Then the real thing. **~200,000 GETs and ~31 GiB of level-9 compression:
budget 1–1.5 hours**, not the "minutes" the plan says. Run it under tmux.

```bash
python -m scripts.reconcile_april_detail pack-trial --apply
```

It writes one report to `recovery/plan145/pack_trial/<run>/trial_report.json`
and **no pack object anywhere** — the trial packs are built in memory and
thrown away.

### The rule is fixed before the run

True ordering carries the full pass **only if it is smaller on both samples**.
A split verdict means the incumbent carries and the question stays open. Do not
re-read the rule after seeing the numbers; that is what fixing it in advance is
for.

If `true` wins, the size of the win is what justifies — or does not — a separate
plan to reorder May, June and July: 6.86 GiB and ~3M members this plan does not
touch.

### Carrying the verdict into step 3

If `current` wins, step 3 needs no change: it is what the packer already does.

If `true` wins, the packer's `ORDER BY` and frame sealing must move from
`cluster_key` to `listing_id` before step 3 — a one-line change in
`fetch_member_metadata`, which Stage 5b deliberately left separable. **That is a
code change with a review, not a flag**, and it is out of this sheet's scope
until the trial reports.

---

## 3. The repack

```bash
python -m archiver.processors.pack_bronze_html \
  --year 2026 --month 4 --repack-bucket --max-packs 0 --apply
```

Dry-run it first without `--apply`: it reports the pending count and an upper
bound on pack count without reading a body.

**Budget a night, not an hour.** ~983k GETs and ~155 GiB of level-9 dictionary
compression, against Stage 3b's 2h03m for reading 557k pack members and writing
them back. The listing phase alone took ~25 minutes on this VM at ~700 keys/s
and looks hung; it is not, and it reports progress.

Watch for:

- `--repack-bucket` logs a warning naming how many objects an existing sidecar
  already names. Expect **557,065**. A much smaller number means the frozen
  baseline is not what you think it is — stop.
- `next_seq` should be **32**. Lower means a pack was retired early; higher
  means a previous repack ran.
- free space. The replacement set is ~3.5 GiB on top of the originals' 1.99 GiB,
  and the loose population is not pruned until step 6.

Both pack sets coexist and both are readable throughout. The originals stay
authoritative until step 4 says otherwise.

---

## 4. The gate

```bash
python -m scripts.reconcile_april_detail repack-verify
```

Read-only. It writes `recovery/plan145/repack/<run>/verify_report.json` and
exits non-zero if anything below fails. **Nothing after this runs without a
passing report**, and steps 5 and 7 re-check that the store has not moved since.

Once, on the authoritative run, add `--list-population`: it enumerates the
April prefix instead of deriving the population from the manifests. Half an
hour, ~1,000 LIST requests, and it is the only way to learn that nothing
unexpected appeared.

### Read these four numbers

| | expect |
|---|---|
| `old member not replaced` / `old member bytes changed` | **0** and **0** |
| `live object not packed` / `member in two packs` | **0** and **0** |
| `read back mismatched` | **0** of 2,000, extracted from the *replacement* packs specifically — not through `read_packed_html`, which the old sidecars would answer |
| `changed share` | well above 50% — April's old sidecar was correct for 31.4% of members, so most `listing_id` values *should* move |

A high `changed share` is the proof the Stage 5b fix reached the sidecars. Near
zero means the run wrote the historical scrambled column again — stop, and do
not retire anything.

### The identity decomposition — a number to bring back

The plan's gate line says NULL identity should fall from 99,981 to 42,276.
That was a property of the old 557,065-member population; the replacement packs
hold 983,043. Derived from the recorded assign and apply censuses:

| origin | members | with an event | **no event** |
|---|---:|---:|---:|
| old pack member | 557,065 | 551,009 | **6,056** |
| materialized | 425,978 | 292,430 | **133,548** |
| **total** | **983,043** | **843,439** | **139,604** |

- 42,276 pack members had no event at pack time; `assign` attributed 36,220 of
  them, leaving 6,056.
- Of 341,903 import-bearing artifacts, 13,253 preserved an existing queue event
  and 328,650 were allocated one. Preserved ones can only be pack members
  (materialized keys are content-derived and production never saw them), so
  49,473 import-bearing artifacts are pack members and **292,430 are
  materialized**. The other 133,548 materialized objects — `already_represented`,
  `blocked_excluded`, or emitting no rows — got no event and are NULL by
  construction.

**NULL `listing_id` will be higher than 139,604**, because an artifact with a
queue event but only `source='carousel'` silver rows has an id and no subject
listing. That is Stage 5b behaving correctly, not a defect.

Record what the report actually says. If `no event` lands far from 139,604, the
arithmetic above is wrong somewhere and that is worth understanding **before**
step 5, not after.

---

## 5. Retire the superseded packs — IRREVERSIBLE

```bash
python -m scripts.reconcile_april_detail retire-packs             # dry run
python -m scripts.reconcile_april_detail retire-packs --apply
```

64 objects: 32 packs and 32 sidecars. It refuses unless `repack-verify` passed
**and** both the old pack set and the replacement sidecar set still match what
that report verified. If it complains that either changed, re-verify — do not
reach for a flag.

Manifest and receipts land under `recovery/plan145/retire/<run>/`. The manifest
is written before the first delete, so an interrupted run still leaves a
complete record of what it intended to remove.

---

## 6. The prune — IRREVERSIBLE

The existing processor, unchanged. This is the step that actually reclaims the
inodes: ~983k loose objects at 2.24 inodes each.

```bash
python -m archiver.processors.delete_packed_source_html \
  --year 2026 --month 4 --apply
```

Dry-run it first. Its per-artifact verification is what guards it — it re-reads
each source object and checks it against the pack before deleting. Processing
status is **report-only** in this processor, which is why the ~426k materialized
objects with no queue event prune correctly rather than being held back as
"unknown".

Run it only after step 5, so it sees the replacement packs and not the retired
ones.

---

## 7. Delete the legacy Parquet — IRREVERSIBLE, and the end of the plan

Dry run. This proves coverage for all 1,172 objects and deletes nothing:

```bash
python -m scripts.reconcile_april_detail delete-legacy \
  --census-dir <the Stage 1 output directory>
```

If that directory is gone, `--census-from-manifests` derives the key set from
the 1,172 Stage 2 shards in MinIO instead. Weaker, and say so in the record.

Read the dry run's `refused` count. **It must be 0.** Each refusal is a legacy
body that is in no replacement pack — which is to say, a body that would exist
nowhere else the moment you delete it. `--allow-partial` exists and needs a
reason written down.

Then, with a name:

```bash
python -m scripts.reconcile_april_detail delete-legacy \
  --census-dir <dir> --apply --maintainer-approval "<your name>"
```

The name lands in the manifest and in every receipt.

### What the run must report

| | required |
|---|---|
| `deleted + absent + error` | exactly **1,172** |
| `error:` receipts | **0** |
| legacy detail Parquet remaining | **0** |
| results_page objects | unchanged from the pre-run count |

Manifest, receipts and `delete_report.json` under
`recovery/plan145/legacy_delete/<run>/`. A non-zero exit means one of the four
above failed; the report names which.

---

## Appendix A — blast radius at a glance

| § | step | writes | deletes | reversible |
|---|---|---|---|---|
| 2 | `pack-trial` | none | none | — |
| 2 | `pack-trial --apply` | 1 report | none | yes |
| 3 | `pack_bronze_html --repack-bucket --apply` | ~56 packs + sidecars, ~3.5 GiB | **none, ever** | yes — delete the new packs |
| 4 | `repack-verify` | 1 report | none | yes |
| 5 | `retire-packs --apply` | manifest + receipts | **64 objects** | **no** |
| 6 | `delete_packed_source_html --apply` | none | **~983k objects** | **no** |
| 7 | `delete-legacy --apply` | manifest + receipts + report | **1,172 objects, ~13.66 GiB** | **no** |

## Appendix B — flags that are gates

`--repack-bucket`, `--allow-drift`, `--allow-partial`,
`--include-null-identity`, `--min-identity-change`, `--maintainer-approval`.

Each exists so a human can overrule one specific measured refusal after looking
at it. Reaching for one to make a run finish is how a plan built on measurement
starts shipping on assumption. `--allow-partial` in particular deletes objects
whose content is provably nowhere else.

## Appendix C — things that will cost you a debugging pass

- **Stage 6 cannot start before Stage 5's flush.** §0. This is the one that
  costs a whole repack.
- **`--repack-bucket` requires `--year` and `--month`** and refuses without
  them. Aimed at a discovered bucket it would duplicate whatever was eligible.
- **The trial writes no pack.** If you find one under `html_packs/`, something
  is wrong — a pack object there makes `_pack_state` treat its members as
  already packed.
- **The old pack set comes from `recovery/plan145/unpacked/`**, not from
  sequence numbers. Never derive it by pattern-matching `pack-000NN`.
- **Never use the sidecar `listing_id` as a join key**, including in the new
  sidecars. They are now *correct*, which is not the same as usable: they are
  NULL wherever silver has no `source='detail'` row.
- **`repack-verify` is read-only** and cheap enough to re-run. Re-run it rather
  than reasoning about whether the store moved.
- **The plan's Stage 6 gate line about 42,276 is stale.** §4. Do not treat the
  verifier disagreeing with it as a failure.
