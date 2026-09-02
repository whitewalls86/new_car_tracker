# Plan 125 Gate C — archived measurement evidence

Raw evidence bundles behind Findings 1–6 in
[`docs/plans/plan_125_duckdb_to_iceberg_migration.md`](../../plans/plan_125_duckdb_to_iceberg_migration.md).

## Why this directory exists

The harnesses write to `.cache/lakehouse_scale_harness/<run-id>/`, and `.cache/`
is gitignored (`.gitignore:30`). These 41 files therefore existed on exactly one
developer machine from 2026-07-21 until they were archived here on 2026-09-02.
The plan document transcribes the conclusions — tables, stack traces, heap
readings — but nothing except these bundles makes those conclusions
re-checkable, and a `git clean -xdf` would have ended them.

The harnesses still write to `.cache/`. That is correct and unchanged: this is
an archive of the completed Gate C runs, not a new output location. The repro
commands in the plan document, including the Docker volume mount, continue to
name `.cache/lakehouse_scale_harness`.

## Provenance

- `scripts/lakehouse_scale_harness.py` — the hex `<run-id>/` directories
- `scripts/gate_c_shadow_replay.py` — `vm_replay/`
- File mtimes are preserved and all read 2026-07-21 in `America/Chicago`; the
  late-evening `vm_replay/` runs are the 2026-07-22 of Finding 6's heading.

## How to read a bundle

Each JSON carries `command`, `run_id`, `platform`, `container_limits`,
`spark_conf`, `steps` and `ok`. The run-ids are content-addressed and opaque;
the bundle *filename* is what names the experiment, which is why the inventory
below lists both.

**`ok: False` on the four `probe_oom_cascade` bundles is the result, not a
broken run.** Those probes reproduce a driver OOM on purpose — Finding 1 records
that the cascade is intermittent, reproduced 2 of 6 runs.

## Inventory

| file                                      | harness command      | ok    | bytes   |
|-------------------------------------------|----------------------|-------|---------|
| 0356bbb8/generate.json                    | generate             | True  | 2,332   |
| 135e9574/shape_check.json                 | describe-dataset     | True  | 4,054   |
| 1c855c5a/sweep_snapshot_1g.json           | run-model            | True  | 3,818   |
| 27880466/synthetic_snapshot_profile.json  | describe-dataset     | True  | 8,793   |
| 2a4fecbb/generate.json                    | generate             | True  | 2,307   |
| 2f28fb8f/dupes_shape.json                 | describe-dataset     | True  | 8,944   |
| 3f6d74a4/real_snapshot_profile.json       | describe-dataset     | True  | 9,076   |
| 409ac57a/chain_38m_driver1g.json          | run-model            | True  | 3,580   |
| 425f0f21/sweep_snapshot_4g.json           | run-model            | True  | 3,819   |
| 42fd2c96/chain_38m_wide_fanout_1g.json    | run-model            | True  | 8,158   |
| 4cde7ebc/generate.json                    | generate             | True  | 2,305   |
| 56933fea/probe_oom_cascade.json           | probe-oom-cascade    | False | 17,854  |
| 572ad371/generate.json                    | generate             | True  | 2,307   |
| 5ccc9e7c/probe_oom_cascade.json           | probe-oom-cascade    | False | 9,952   |
| 69a2475c/generate.json                    | generate             | True  | 2,352   |
| 6fd26f40/fp_5m_driver1g.json              | run-model            | True  | 2,949   |
| 724e74df/skew_selftest.json               | describe-dataset     | True  | 13,344  |
| 754f1d76/probe_oom_cascade.json           | probe-oom-cascade    | False | 9,952   |
| 7868a353/probe_oom_cascade.json           | probe-oom-cascade    | False | 9,951   |
| 8ba4c91c/baseline_shape.json              | describe-dataset     | True  | 8,919   |
| 8c44bf72/fp_38m_driver1g.json             | run-model            | True  | 2,950   |
| 8ffaaf75/generate.json                    | generate             | True  | 2,301   |
| 95523562/shape_38m_wide.json              | describe-dataset     | True  | 4,088   |
| ac3693ad/generate.json                    | generate             | True  | 2,307   |
| be60f7f6/chain_38m_36kfiles_driver1g.json | run-model            | True  | 3,580   |
| c7d4e3b8/probe_oom_cascade.json           | probe-oom-cascade    | False | 9,951   |
| d46cb1cd/sweep_dupes_1g.json              | run-model            | True  | 3,824   |
| de95150b/calib_shape.json                 | describe-dataset     | True  | 13,958  |
| e2a24a34/sweep_snapshot_2g.json           | run-model            | True  | 3,820   |
| f608bb0c/generate.json                    | generate             | True  | 2,325   |
| f9e0f2f2/synthetic_treatment_profile.json | describe-dataset     | True  | 8,917   |
| faf256ea/generate.json                    | generate             | True  | 2,303   |
| fdc120f7/generate.json                    | generate             | True  | 2,305   |
| vm_production/vm_production_shape.json    | describe-dataset     | True  | 14,181  |
| vm_replay/evidence/dbt_logs/dbt.log       | --                   | --    | 180,726 |
| vm_replay/evidence/run/heap_samples.jsonl | --                   | --    | 42,710  |
| vm_replay/evidence/run/replay.json        | gate-c-shadow-replay | --    | 19,792  |
| vm_replay/evidence/run/spark_conf.json    | --                   | --    | 1,158   |
| vm_replay/evidence_driver.log             | --                   | --    | 98,644  |
| vm_replay/replay_bundle.tar.gz            | --                   | --    | 46,133  |
| vm_replay/run_replay.sh                   | --                   | --    | 906     |

