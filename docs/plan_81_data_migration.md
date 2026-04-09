# Plan 81: Data Migration — Local → Cloud

**Status:** Planned
**Priority:** High — pipeline is live on Oracle but has no historical data

Migrate three data stores from the local home server to the Oracle cloud VM:
1. **PostgreSQL** — all scraped data, configs, run history
2. **Raw artifacts volume** (`cartracker_raw`) — HTML files awaiting archival
3. **MinIO parquet data** — archived parquet files in the `bronze` bucket

The cloud schema is already in place (Flyway applied V001 + V002 on first `docker compose up`), so Postgres migration is data-only.

---

## Phase 1: PostgreSQL

### 1.1 Dump from local

```bash
# On local machine — dump data only (schema already applied by Flyway on cloud)
docker exec cartracker-postgres pg_dump \
  -U cartracker \
  -d cartracker \
  --data-only \
  --no-owner \
  --no-acl \
  -Fc \
  -f /tmp/cartracker_data.dump

# Copy dump out of container
docker cp cartracker-postgres:/tmp/cartracker_data.dump ./cartracker_data.dump
```

### 1.2 Transfer to Oracle VM

```bash
# From local machine
scp cartracker_data.dump cartracker:/tmp/cartracker_data.dump
```

### 1.3 Restore on cloud

```bash
# SSH into Oracle VM
ssh cartracker

# Restore into the running postgres container
docker cp /tmp/cartracker_data.dump cartracker-postgres:/tmp/cartracker_data.dump

docker exec cartracker-postgres pg_restore \
  -U cartracker \
  -d cartracker \
  --data-only \
  --no-owner \
  --single-transaction \
  /tmp/cartracker_data.dump

# Verify row counts match local
docker exec -it cartracker-postgres psql -U cartracker -d cartracker -c "
  SELECT 'raw_artifacts' as t, COUNT(*) FROM raw_artifacts
  UNION ALL SELECT 'detail_observations', COUNT(*) FROM detail_observations
  UNION ALL SELECT 'srp_observations', COUNT(*) FROM srp_observations
  UNION ALL SELECT 'search_configs', COUNT(*) FROM search_configs
  UNION ALL SELECT 'runs', COUNT(*) FROM runs;
"
```

### 1.4 Caveats

- **Flyway history table** — `flyway_schema_history` will already exist on the cloud; pg_restore with `--data-only` skips it cleanly
- **Sequences** — pg_restore in `-Fc` format includes sequence state; verify auto-increment columns resume correctly after restore
- **Large tables** — `raw_artifacts` and `detail_observations` may be large; expect transfer + restore to take 10-30 min depending on size

---

## Phase 2: Raw artifacts volume

Raw HTML artifacts live in the `cartracker_raw` Docker volume, mounted at `/data/raw` in the scraper container. These are transient (archiver converts them to Parquet then deletes them), so only migrate what's pending archival.

### 2.1 Check what's pending

```bash
# On local — how much data is pending archival?
docker exec cartracker-scraper du -sh /data/raw
```

If the volume is small (< a few GB) or already mostly archived, it may be simpler to skip this phase and let the archiver process new data on the cloud going forward.

### 2.2 Transfer if needed

```bash
# On local — tar the raw volume contents
docker run --rm \
  -v cartracker_raw:/data/raw:ro \
  -v $(pwd):/backup \
  alpine tar czf /backup/cartracker_raw.tar.gz -C /data/raw .

# SCP to Oracle VM
scp cartracker_raw.tar.gz cartracker:/tmp/

# On Oracle VM — extract into the volume
docker run --rm \
  -v cartracker_raw:/data/raw \
  -v /tmp:/backup \
  alpine tar xzf /backup/cartracker_raw.tar.gz -C /data/raw
```

---

## Phase 3: MinIO parquet data

MinIO stores hive-partitioned Parquet files in the `bronze` bucket inside the `parquet_data` Docker volume.

### 3.1 Option A — mc mirror (recommended)

Use the MinIO client to mirror the bucket directly between local and cloud MinIO instances. Requires the cloud MinIO port to be temporarily exposed, or run via SSH tunnel.

```bash
# On local — install mc if not present
docker run --rm -it --entrypoint /bin/sh minio/mc

# Add both instances
mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc alias set cloud http://<oracle-public-ip>:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# Mirror bronze bucket
mc mirror local/bronze cloud/bronze

# Verify object counts match
mc ls --recursive local/bronze | wc -l
mc ls --recursive cloud/bronze | wc -l
```

> **Note:** MinIO port 9000 is not in the Oracle security list by default. Either temporarily open it, or use an SSH tunnel:
> ```bash
> ssh -L 9001:localhost:9000 cartracker
> # Then use http://localhost:9001 as the cloud alias
> ```

### 3.2 Option B — volume tar (simpler, no network exposure)

```bash
# On local — tar the parquet volume
docker run --rm \
  -v parquet_data:/data:ro \
  -v $(pwd):/backup \
  alpine tar czf /backup/parquet_data.tar.gz -C /data .

# SCP to Oracle VM
scp parquet_data.tar.gz cartracker:/tmp/

# On Oracle VM — stop MinIO, restore, restart
docker compose stop minio
docker run --rm \
  -v parquet_data:/data \
  -v /tmp:/backup \
  alpine tar xzf /backup/parquet_data.tar.gz -C /data
docker compose start minio
```

---

## Phase 4: Post-migration validation

```bash
# On Oracle VM

# 1. Trigger a dbt build to confirm models compile against migrated data
curl -X POST http://localhost:8080/dbt/build \
  -H "Content-Type: application/json" \
  -d '{"intent": "both"}'

# 2. Check dashboard loads with data
curl -s https://cartracker-scraper.duckdns.org | head -20

# 3. Verify archiver can see raw artifacts (if migrated)
docker compose logs archiver --tail=50

# 4. Verify MinIO bucket contents via console
# https://cartracker-scraper.duckdns.org/minio  (once Caddy route is added)
```

---

## Order of operations

1. Put local pipeline in deploy-hold (set deploy intent via `/admin/deploy`) to freeze data during migration
2. Complete Phase 1 (Postgres)
3. Complete Phase 2 (raw artifacts) — or skip if volume is small
4. Complete Phase 3 (MinIO) — or skip if starting fresh is acceptable
5. Run Phase 4 validation
6. Release deploy hold, point n8n schedules to run on cloud

---

## Notes

- **Schema-only restore risk:** If `--data-only` restore hits a constraint violation (e.g. FK ordering), add `--disable-triggers` flag to pg_restore
- **Parquet is re-derivable:** If MinIO migration is too complex, it can be skipped — the archiver will re-archive new artifacts going forward; historical Parquet is a nice-to-have, not required for the pipeline to function
- **Raw artifacts are expendable:** If archiver has already processed most artifacts locally, Phase 2 can be skipped entirely
