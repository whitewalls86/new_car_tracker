# Plan 81: Data Migration — Local → Cloud

**Status:** Complete — executed 2026-04-14
**Priority:** High — pipeline is live on Oracle but has no historical data

## Overview

Migrate three data stores from the local home server to the Oracle cloud VM:

| Store | Size | Notes |
|---|---|---|
| PostgreSQL (`public` schema only) | ~15 GB | `analytics` schema re-derived by dbt after restore |
| Raw artifacts (`cartracker_raw` volume) | ~15.7 GB | HTML files awaiting archival |
| MinIO parquet (`parquet_data` volume) | ~27.1 GB | Archived parquet files in `bronze` bucket |

**Total transfer:** ~58 GB at ~350 Mbps ≈ ~20 min network time  
**Cloud storage:** 186 GB free on `/mnt/data` (200 GB block volume, symlinked to Docker volume root)

The cloud schema is already in place (Flyway applied all migrations on first `docker compose up`), so the Postgres migration is **data-only, public schema only**.

---

## Step-by-Step Execution

### Step 1 — Freeze local pipeline

Put local ops into deploy-hold so no new data is written during migration.

```bash
# On LOCAL machine
curl -X POST http://localhost:8060/deploy/start
# Confirm intent is "pending"
curl http://localhost:8060/deploy/status
```

Wait for any in-flight runs to complete (`number_running` → 0). You can monitor via `/admin/deploy` on the local admin panel.

---

### Step 2 — pg_dump from local (public schema only)

`analytics` schema is dbt-derived and will be rebuilt on cloud — no need to migrate it.

```bash
# On LOCAL machine — dump public schema data only
docker exec cartracker-postgres pg_dump \
  -U cartracker \
  -d cartracker \
  --data-only \
  --schema=public \
  --no-owner \
  --no-acl \
  -Fc \
  -f /tmp/cartracker_data.dump

# Copy dump out of container to local filesystem
docker cp cartracker-postgres:/tmp/cartracker_data.dump ./cartracker_data.dump

# Confirm size (expect ~15 GB)
ls -lh ./cartracker_data.dump
```

---

### Step 3 — Tar raw artifacts volume

```bash
# On LOCAL machine
docker run --rm \
  -v cartracker_raw:/data/raw:ro \
  -v $(pwd):/backup \
  alpine tar czf /backup/cartracker_raw.tar.gz -C /data/raw .

ls -lh ./cartracker_raw.tar.gz
```

---

### Step 4 — Tar MinIO parquet volume

```bash
# On LOCAL machine
docker run --rm \
  -v parquet_data:/data:ro \
  -v $(pwd):/backup \
  alpine tar czf /backup/parquet_data.tar.gz -C /data .

ls -lh ./parquet_data.tar.gz
```

---

### Step 5 — Transfer all three to Oracle VM

Run from the local machine. Files go directly to `/mnt/data` on the cloud VM (the 200 GB block volume).

```bash
# On LOCAL machine — transfer in parallel or sequentially
scp cartracker_data.dump cartracker:/mnt/data/cartracker_data.dump
scp cartracker_raw.tar.gz cartracker:/mnt/data/cartracker_raw.tar.gz
scp parquet_data.tar.gz cartracker:/mnt/data/parquet_data.tar.gz
```

Verify on cloud:
```bash
# On CLOUD (ssh cartracker)
ls -lh /mnt/data/*.dump /mnt/data/*.tar.gz
df -h /mnt/data   # confirm space remaining
```

---

### Step 6 — pg_restore on cloud

```bash
# On CLOUD
docker cp /mnt/data/cartracker_data.dump cartracker-postgres:/tmp/cartracker_data.dump

docker exec cartracker-postgres pg_restore \
  -U cartracker \
  -d cartracker \
  --data-only \
  --no-owner \
  --single-transaction \
  /tmp/cartracker_data.dump
```

If you get FK constraint violations, add `--disable-triggers` and rerun:
```bash
docker exec cartracker-postgres pg_restore \
  -U cartracker \
  -d cartracker \
  --data-only \
  --no-owner \
  --single-transaction \
  --disable-triggers \
  /tmp/cartracker_data.dump
```

**Verify row counts match local:**
```sql
docker exec -it cartracker-postgres psql -U cartracker -d cartracker -c "
  SELECT 'raw_artifacts' as t, COUNT(*) FROM raw_artifacts
  UNION ALL SELECT 'detail_observations', COUNT(*) FROM detail_observations
  UNION ALL SELECT 'srp_observations', COUNT(*) FROM srp_observations
  UNION ALL SELECT 'search_configs', COUNT(*) FROM search_configs
  UNION ALL SELECT 'runs', COUNT(*) FROM runs;
"
```

---

### Step 7 — Restore raw artifacts volume

```bash
# On CLOUD
docker run --rm \
  -v cartracker_raw:/data/raw \
  -v /mnt/data:/backup \
  alpine tar xzf /backup/cartracker_raw.tar.gz -C /data/raw

# Verify
docker run --rm -v cartracker_raw:/data/raw:ro alpine du -sh /data/raw
```

---

### Step 8 — Restore MinIO parquet volume

Stop MinIO before touching its volume.

```bash
# On CLOUD
docker compose stop minio

docker run --rm \
  -v parquet_data:/data \
  -v /mnt/data:/backup \
  alpine tar xzf /backup/parquet_data.tar.gz -C /data

docker compose start minio

# Verify object count
docker exec cartracker-minio mc ls --recursive /data/bronze | wc -l
# Or check via https://cartracker.info/minio console
```

---

### Step 9 — Run dbt build on cloud

Rebuilds the `analytics` schema from the newly restored `public` data.

```bash
# On CLOUD — trigger via admin panel or directly
curl -X POST http://localhost:8060/admin/dbt/build \
  -H "Content-Type: application/json" \
  -d '{"intent": "both"}'

# Or watch the dbt_runner logs
docker compose logs -f dbt_runner
```

---

### Step 10 — Validate and release

```bash
# Row counts (compare to Step 6 output)
docker exec -it cartracker-postgres psql -U cartracker -d cartracker -c "
  SELECT 'raw_artifacts' as t, COUNT(*) FROM raw_artifacts
  UNION ALL SELECT 'detail_observations', COUNT(*) FROM detail_observations
  UNION ALL SELECT 'srp_observations', COUNT(*) FROM srp_observations
  UNION ALL SELECT 'search_configs', COUNT(*) FROM search_configs;
"

# Check dashboard loads with data
curl -s https://cartracker.info | head -20

# Check archiver logs (should find raw artifacts to process)
docker compose logs archiver --tail=50

# Release the local deploy hold
curl -X POST http://localhost:8060/deploy/complete

# Point n8n schedules to cloud (disable local, enable cloud workflows)
```

---

## Caveats

- **Schema-only restore risk:** If pg_restore hits FK constraint violations, use `--disable-triggers`. Re-enable triggers is automatic with `--single-transaction`.
- **Flyway history:** `flyway_schema_history` already exists on cloud; `--data-only` skips it cleanly.
- **Sequences:** pg_restore in `-Fc` format includes sequence state; auto-increment columns resume correctly.
- **Parquet is re-derivable:** If MinIO migration is too complex or fails, skip it — the archiver will process new artifacts going forward. Historical Parquet is nice-to-have.
- **Raw artifacts are expendable:** If the archiver has already processed most artifacts locally, Phase 7 can be skipped.
- **Analytics schema:** Do NOT restore analytics — it's entirely dbt-derived and will be rebuilt in Step 9.

---

## Cleanup (after validation)

```bash
# On CLOUD — remove transfer files to free space
rm /mnt/data/cartracker_data.dump
rm /mnt/data/cartracker_raw.tar.gz
rm /mnt/data/parquet_data.tar.gz

# On LOCAL — remove local copies
rm ./cartracker_data.dump
rm ./cartracker_raw.tar.gz
rm ./parquet_data.tar.gz
```
