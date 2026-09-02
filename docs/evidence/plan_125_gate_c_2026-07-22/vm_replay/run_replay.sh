#!/bin/sh
set -u
EV=/home/ubuntu/gate_c_replay/evidence
mkdir -p "$EV"
MU=$(grep -E "^MINIO_ROOT_USER=" /opt/cartracker/.env | cut -d= -f2-)
MP=$(grep -E "^MINIO_ROOT_PASSWORD=" /opt/cartracker/.env | cut -d= -f2-)
[ -n "$MU" ] && [ -n "$MP" ] || { echo MISSING_CREDS; exit 1; }
docker run --rm --network cartracker-net --memory 6g \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER="$MU" -e MINIO_ROOT_PASSWORD="$MP" \
  -e ICEBERG_CATALOG_URI=http://lakekeeper:8181/catalog \
  -e ICEBERG_WAREHOUSE_NAME=cartracker_experiments \
  -e IMAGE_DIGEST=sha256:01d9a5c0e0fffe4dc8cde4dbea7aecc15917916938b4b397e53dbddc1ae791ed \
  -v "$EV":/evidence \
  cartracker-lakehouse-replay:diag \
  sh -c "python -m scripts.gate_c_shadow_replay --evidence-dir /evidence/run; echo REPLAY_EXIT=\$?; mkdir -p /evidence/dbt_logs; cp -r /app/dbt/logs/. /evidence/dbt_logs/ 2>/dev/null; true"
echo "OUTER_EXIT=$?"
