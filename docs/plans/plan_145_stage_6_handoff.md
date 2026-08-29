# Plan 145 Stage 6 — implementation handoff (CAR-22)

The machinery is **built and unit-tested**. Nothing has been run. This page is
what the machinery is, why each piece is shaped the way it is, and what the
plan document says that the code found to be wrong.

The run sheet is [`runbook_plan_145_stage_6.md`](../runbooks/runbook_plan_145_stage_6.md).

---

## What Stage 6 actually needed building

Almost nothing, which is the point. Stage 6 writes to no database and mutates
no live state; it moves bytes between object prefixes. Every heavy component
already existed:

| need | already exists as |
|---|---|
| repack, and verify every member from the *stored* object | `pack_bronze_html._verify_stored_pack` |
| identity separated from placement | `shared/packfile.py` `PackMember.cluster_key`, Stage 5b (`dd6aa26`) |
| prune the loose sources | `archiver/processors/delete_packed_source_html.py` |
| extracting and proving one member from one pack | `shared/packfile.py` `PackReader` over ranged GETs |
| capped deletes with a receipt per key | `delete_objects_in_batches` |
| the old pack set, frozen before Stage 6 existed | the 32 Stage 3b manifests |

So this stage is one flag on the packer, four modes on
`scripts/reconcile_april_detail.py`, and the existing prune invoked unchanged.
No new pack format, generation selector, reader contract or prune algorithm —
as the plan requires.

---

## The one production change: `--repack-bucket`

`_process_bucket` subtracts the keys named by existing sidecars from the set it
is about to pack. That subtraction **is** the packer's checkpoint: it is what
makes an interrupted run resumable, and it is correct in every ordinary case.

It is wrong for exactly one run. April's 32 packs name 557,065 of the flattened
population's 983,043 objects, so Stage 6 without this flag would pack only the
~426k materialized remainder and leave the original sidecars — and their
scrambled `listing_id` — in place. That is the opposite of the gate.

`--repack-bucket` lifts the subtraction. It:

- **requires an explicit `--year`/`--month`.** Aimed at a discovered bucket it
  would silently duplicate whatever happened to be eligible that day, so the
  guard is a refusal, not a warning;
- **writes at the next free sequence numbers**, so both pack sets coexist and
  the plan's *"keep the original April packs until the replacements verify"*
  holds literally;
- **deletes nothing**, like every other path through the packer;
- **logs a warning naming the count it is re-packing**, because a run producing
  a second pack set over members an existing sidecar already names should say
  so in its own log rather than in someone's postmortem.

Reading is unaffected while both sets exist: `_find_index_entry` resolves a
`source_key` from whichever sidecar names it, both hold identical bytes, and
`read_packed_html` verifies `raw_sha256` on every read regardless.

---

## `pack-trial` — the ordering question, answered

The plan's one open measurement. The same member set is packed twice, with only
the order changed:

| arm | order | what frames are cut on |
|---|---|---|
| `current` | `(cluster_key, fetched_at)` | `cluster_key` — the historical unfiltered reduction |
| `true` | `(listing_id, fetched_at)` | `listing_id` — the corrected subject listing |

Stage 5b is what makes this askable at all: before it, one column was both
sort key and recorded identity, so the question could not be posed without
also changing what the sidecar records.

**The trial packs are built in memory and never stored.** `pack.size` is the
entire measurement, and not writing them is a stronger form of "discard both
trial packs" than deleting them afterwards. It also removes the one real hazard
in the trial: a trial pack landing under the real pack prefix would make a later
`_pack_state` treat its members as already packed.

### Two samples, not one — a deliberate deviation

The plan says *"the first ~50,000 members of the flattened population"*. Taken
in one order, that sample holds that arm's clusters whole and truncates the
other's, which biases the result toward whichever order drew it — the bias the
plan's own *Caveats for the result* section describes.

A random sample cannot fix it: 50,000 drawn from 983,043 would hold about half
a capture per listing and would destroy the clustering *both* arms depend on,
measuring nothing. Contiguity is required, and contiguity is what carries the
bias.

So the trial draws **one contiguous sample per arm** and packs each both ways —
four passes. `--sample current` runs the plan's literal single-sample reading.

**The rule, fixed before the run:** true ordering carries the full pass only if
it is smaller on **both** samples. A split verdict means the incumbent carries
and the question stays open, which is the honest reading of a disagreement
between two samples biased in opposite directions, and far cheaper than a wrong
month-wide reorder of May, June and July.

### What it costs, correcting the plan

The plan says *"cost is minutes"*. Four passes over 50,000 members is ~200,000
GETs and ~31 GiB of level-9 dictionary compression. **Budget 1–1.5 hours**;
`--sample current` is roughly half.

### Members with no subject listing are left out

A member whose `listing_id` is NULL cannot inform a question about ordering by
subject listing, and in the `true` arm they would collapse into one enormous
false cluster under NULL. They are excluded by default and their count is
reported. `--include-null-identity` keeps them.

---

## `repack-verify` — the gate

Read-only, and every step after it is irreversible. It refuses unless:

1. every member of the frozen Stage 3b baseline is in a replacement pack;
2. none of their `raw_sha256` values moved;
3. every object in the flattened population is in exactly one replacement pack;
4. a stratified sample extracts from **its own replacement pack** and matches
   (below);
5. the replacement `listing_id` actually *changed* (below).

### Where the old pack set comes from

Not from sequence numbers. `recovery/plan145/unpacked/` holds 32 shards naming
every one of the 557,065 members and its `pack_key`, written while the original
packs were the whole population. It cannot be contaminated by anything Stage 6
does, which is what a baseline has to be. Deriving "which packs are old" from
sequence numbers after the fact would be a guess about a store that has already
been written to.

### The population is derived, not listed

Materialized, minus what Stage 3a deleted, union unpacked — the same
derivation Stage 4 uses for its parse units, and for the reason the Stage 4
handoff gives: listing a million keys costs ~1,000 LIST requests and answers a
weaker question than the manifests already do. The count is held to
`EXPECTED_FLATTENED_INPUTS` (983,043), so a population that is not the one
Stage 4 parsed and Stage 5 classified is a stop.

`--list-population` enumerates the prefix instead, for the one run where *"and
nothing unexpected appeared"* is worth half an hour.

### Why the read-back is not `read_packed_html`

`read_packed_html` resolves a `source_key` through whichever sidecar names it.
While both pack sets exist, the **old** sidecars name every one of the 557,065
replaced members — so a read-back through it could serve the entire sample from
the packs this stage is about to delete and report success.

So the verifier reads each sampled member from the pack its replacement sidecar
names, through a `PackReader` over ranged GETs. That is the same machinery
`read_packed_html` uses once it has resolved the pack; the only difference is
that the pack is pinned rather than searched for, which is the whole point.

### Proving the Stage 5b fix reached the sidecars

April's old sidecar `listing_id` was correct for 31.4% of members, so a
replacement that **agrees** with it nearly everywhere means the run wrote the
historical scrambled column again — a silent reproduction of the defect this
plan spent three revisions proving. The verifier compares old sidecar to new
and refuses below `--min-identity-change` (default 0.50), reporting the
`same` / `differs` / `null_now` split.

Checked against the old sidecars rather than against silver because it needs no
lake read and answers exactly the question asked: *did the column change.*

---

## A finding: Stage 6's NULL-identity gate line is stale

> *"Sidecar NULL-identity members drop from 99,981 to the 42,276 that have no
> `artifacts_queue_events` row."*

Both numbers are properties of the **557,065-member** pack population. The
replacement packs hold **983,043** members, and ~426k of them are materialized
objects whose content-derived keys no `artifacts_queue_events` row has ever
named.

Stage 5's full apply (2026-08-29) attributes part of that: it wrote a
`recovered` queue event carrying `minio_path`, plus silver rows, for each of
the 341,903 import-bearing artifacts — which is exactly what the corrected
`obs` CTE reduces.

The arithmetic falls out of the recorded assign and apply censuses:

| origin | members | with an event | **no event** |
|---|---:|---:|---:|
| old pack member | 557,065 | 551,009 | **6,056** |
| materialized | 425,978 | 292,430 | **133,548** |
| **total** | **983,043** | **843,439** | **139,604** |

42,276 pack members had no event at pack time and `assign` attributed 36,220,
leaving 6,056. Of the 341,903 import-bearing artifacts, 13,253 preserved an
existing queue event and 328,650 were allocated one; preserved ones can only be
pack members, since materialized keys are content-derived and production never
saw them. So 49,473 are pack members and **292,430 are materialized**, leaving
133,548 materialized objects — `already_represented`, `blocked_excluded`, or
emitting no rows — with no event at all. They are NULL by construction, the same
way a carousel-only pack member is: the plan's own *"an object nobody can
describe."*

NULL `listing_id` will exceed 139,604, because an artifact with a queue event
but only `source='carousel'` silver rows has an id and no subject listing.

**`repack-verify` reports the decomposition** — members by origin ×
attributed / NULL — so the run measures this rather than restating it. The plan
document's gate line has been updated to the derivation above, marked as
derived rather than measured.

---

## `retire-packs` and `delete-legacy`

Both delete by exact key from a manifest written *before* the first delete, with
one receipt per key, through the shared `delete_objects_in_batches`. Its guard
was lifted to a caller-supplied predicate so each stage checks its own thing;
Stage 3a's "content is in a verified pack" remains the default and its callers
are unchanged.

A guard that returns a reason **stops the whole run** rather than skipping that
key. The manifest and the guard share a source, so a disagreement means the
manifest was edited by hand, and the only safe response to that is to delete
nothing.

### `retire-packs`

64 keys: 32 packs and 32 sidecars, from the frozen Stage 3b set. It refuses
unless a `repack-verify` report passed, **and** the old pack set and the
replacement sidecar set both still match what that report verified — otherwise
the store moved under the proof and the answer is to re-verify, not to retire.

### `delete-legacy`

The end of the plan.

- The key set is regenerated from the frozen Stage 1 census
  (`--census-dir`, fingerprint-checked against `stage1_report.json`). If that
  local output no longer exists, `--census-from-manifests` derives it from the
  1,172 Stage 2 shards in MinIO — a weaker attestation, not an absent one: the
  shards still have to name every live key, and the count is still held to
  `BASELINE_OBJECTS`.
- **Coverage, per object:** every body Stage 2 derived from that Parquet must
  be in a replacement sidecar. That is the plan's *"no key is deleted whose
  content is not provably in a verified pack"*, applied to the Parquet rather
  than to a single object. The 43,014 empty and 101,010 non-success rows
  produced no body and can never need covering.
- An uncovered body is a **refusal, not a skip**. A partially recoverable
  legacy object is exactly the case where deleting loses something.
  `--allow-partial` exists and needs a reason.
- `--maintainer-approval "<name>"` is required by `--apply` and is recorded in
  the manifest and every receipt.
- `results_page` is refused **by key**, as a predicate rather than a filter,
  even though the enumeration cannot produce one.
- Afterwards it re-enumerates both prefixes: the legacy detail prefix must hold
  zero Parquet objects, and the results-page population must be unchanged.

---

## What is unproven

Everything. Not one of these modes has been run against production, or against
anything but fixtures. In particular:

- the ordering trial has never met real April data, so **there is no verdict**;
- no repack has been attempted, so the runtime in the run sheet is an estimate;
- the NULL-identity decomposition above is **derived from Stage 5's recorded
  counts, not measured**. `repack-verify` is what measures it.

Stage 5 is no longer a blocker: the full apply committed on 2026-08-29 and
staging drained to 0, so Stage 6 can run as soon as someone chooses to start
it. Confirm the preflight anyway — a repack is a night's work to redo.

Do not merge a branch, do not open a Linear issue, and do not declare the gate
closed.
