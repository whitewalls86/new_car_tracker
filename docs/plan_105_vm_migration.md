# Plan 105: VM Migration — A2 → A1 (Oracle Cloud)

**Status:** Planned
**Priority:** Medium — cost reduction (A1 is free tier; A2 is paid)

---

## Overview

Migrate the full production stack from the current paid Oracle Cloud Ampere A2 VM to a newly-provisioned A1.Flex (4 OCPU / 24 GB) free-tier instance in the same tenancy.

Both VMs are ARM64 (aarch64) Ampere, on Ubuntu 22.04, in the same VCN. No image rebuilds or architecture changes are required.

The persistent state is contained entirely on a single 200 GB OCI block volume (`/dev/sdb`, mounted at `/mnt/data`) — `/var/lib/docker/volumes` is a symlink to `/mnt/data/docker-volumes`, so every Docker volume (Postgres, MinIO bronze, raw artifacts, Grafana, etc.) lives on it. The migration strategy is to **detach the block volume from the old VM and reattach to the new one**, avoiding any dump-and-restore.

**Estimated downtime:** ~10-15 minutes (Phase 2 stop → Phase 6 healthy).

---

## Pre-conditions

- New A1.Flex VM provisioned via [scripts/provision_oracle_vm.py](../scripts/provision_oracle_vm.py)
- New VM in the same VCN/subnet as the old VM (`vcn-20260408-1353` / `subnet-20260408-1353`)
- Block volume `/dev/sdb` on old VM is healthy and currently mounted at `/mnt/data`
- `/var/lib/docker/volumes` on old VM is a symlink to `/mnt/data/docker-volumes` (verified)
- `.env` file available for secure transfer (gpg-encrypted or 1Password — never plaintext scp)

---

## Phase 0 — Pre-flight (no downtime)

### 0.1 Free space on the block volume

The block volume is 94% full (174G / 196G). Most of the slack is consumed by leftover Plan 81 transfer files. Delete them:

```bash
# On OLD VM
sudo rm /mnt/data/cartracker_data.dump
sudo rm /mnt/data/cartracker_raw.tar.gz
sudo rm /mnt/data/parquet_data.tar.gz
df -h /mnt/data   # confirm drop to ~85% used
```

### 0.2 Reserve the current public IP

OCI Console → **Networking → Reserved Public IPs**. Find the old VM's primary VNIC public IP and select **"Convert ephemeral to reserved"**. This lets you reassign it to the new VM in seconds — no DNS change required.

If skipped: you'll have to update the `cartracker.info` A record at the DNS provider and wait for TTL.

### 0.3 Bootstrap the new VM

```bash
# On NEW VM
sudo apt update
sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker ubuntu
# Re-login to pick up docker group

# Stop Docker until block volume is mounted (Phase 4)
sudo systemctl stop docker
sudo systemctl disable docker  # prevent auto-start on reboot

# Repo
sudo mkdir -p /opt/cartracker
sudo chown ubuntu:ubuntu /opt/cartracker
git clone <repo-url> /opt/cartracker
cd /opt/cartracker && git checkout master
```

Pre-create the volumes symlink target (the daemon must see this when it starts):

```bash
# On NEW VM — preempt Docker creating /var/lib/docker/volumes as a real dir
sudo rm -rf /var/lib/docker/volumes
sudo ln -s /mnt/data/docker-volumes /var/lib/docker/volumes
```

### 0.4 Transfer `.env` securely

```bash
# On LOCAL workstation
gpg --encrypt --recipient <key> /opt/cartracker/.env
# Send encrypted file via secure channel; decrypt on new VM into /opt/cartracker/.env
```

### 0.5 Pre-create the external Docker network

```bash
# On NEW VM (Docker daemon must be running for this — start it briefly)
sudo systemctl start docker
docker network create cartracker-net
sudo systemctl stop docker
```

### 0.6 OCI security list & host firewall

- OCI Console → **Networking → Virtual Cloud Networks → vcn-20260408-1353 → Security Lists** — confirm ingress on 22, 80, 443 covers new VM's subnet
- On new VM: `sudo ufw allow 22,80,443/tcp && sudo ufw enable`

---

## Phase 1 — Freeze old VM

```bash
# On OLD VM
curl -X POST http://localhost:8060/deploy/start
curl http://localhost:8060/deploy/status
# Watch /admin/deploy until number_running = 0
```

This pauses pipeline writes. In-flight DAG runs complete; new submissions block on the deploy_intent flag.

---

## Phase 2 — Stop old stack and unmount

```bash
# On OLD VM
cd /opt/cartracker
docker compose down
sudo umount /mnt/data
sudo systemctl stop docker  # so it doesn't recreate /var/lib/docker/volumes
```

---

## Phase 3 — Detach + reattach block volume

In OCI Console:

1. **Compute → Block Volumes → [data-volume] → Attached Instances → Detach**
2. Wait for state = `AVAILABLE` (~1-2 min)
3. **Attach to new instance** → select new VM, paravirtualized attachment
4. Wait for state = `ATTACHED`

---

## Phase 4 — Mount on new VM

```bash
# On NEW VM
lsblk                                          # confirm /dev/sdb appeared
sudo mount /dev/sdb /mnt/data
ls /mnt/data/docker-volumes/                   # sanity check: should list volumes
```

Add fstab entry so it remounts on reboot:

```bash
sudo blkid /dev/sdb                            # capture UUID
echo "UUID=<uuid> /mnt/data ext4 defaults,_netdev,nofail 0 2" | sudo tee -a /etc/fstab
sudo mount -a                                  # validate fstab syntax
```

Verify the symlink resolves:

```bash
readlink /var/lib/docker/volumes
# → /mnt/data/docker-volumes
ls /var/lib/docker/volumes/
# → cartracker_pgdata, parquet_data, etc.
```

---

## Phase 5 — Move public IP

OCI Console → **Networking → Reserved Public IPs**:

1. Detach reserved IP from old VM's primary VNIC
2. Attach to new VM's primary VNIC

Verify externally:

```bash
# From workstation
dig +short cartracker.info        # should resolve to the reserved IP (unchanged)
```

If you skipped Phase 0.2: update the `cartracker.info` A record at your DNS provider now and wait for TTL.

---

## Phase 6 — Bring up new stack

```bash
# On NEW VM
sudo systemctl enable docker
sudo systemctl start docker
cd /opt/cartracker
docker compose build
docker compose up -d
sleep 30
docker compose ps
```

Caddy will request a fresh Let's Encrypt cert as soon as the first HTTPS request hits `cartracker.info` resolving to the new IP.

---

## Phase 7 — Validate

- [ ] All Compose services healthy (`docker compose ps`)
- [ ] `https://cartracker.info` loads, dashboard renders with real data
- [ ] `/info` (public route) responds
- [ ] OAuth flow works end-to-end (`/dashboard`, `/admin`)
- [ ] `/grafana` shows fresh metrics, panels populated
- [ ] `/airflow` accessible, scheduler healthy, no DAG failures
- [ ] `/pgadmin` and `/minio` accessible (admin)
- [ ] Trigger one scraper search end-to-end, verify parquet lands in MinIO and archiver picks it up
- [ ] Telegram alert fires on synthetic test (kill one container)
- [ ] Release the freeze: `curl -X POST http://localhost:8060/deploy/complete`
- [ ] Watch one scheduled DAG run to completion

Hold validation at least 30 min before declaring success.

---

## Phase 8 — Cleanup (after 24-48h stable)

- Terminate old VM (preserve its boot volume snapshot for 1 week as rollback insurance)
- Update [reference_server_ssh.md](../../../.claude/projects/c--Users-mille-PycharmProjects-cartracker-scraper/memory/reference_server_ssh.md) memory with new IP if not using a reserved IP
- Mark Plan 105 complete in MEMORY.md

---

## Rollback

At any point through Phase 6:

1. Reverse Phase 5: move reserved IP back to old VM's VNIC (instant)
2. Reverse Phase 3: detach block volume from new VM, reattach to old (~2 min)
3. On old VM: `sudo mount /dev/sdb /mnt/data && sudo systemctl start docker && cd /opt/cartracker && docker compose up -d`
4. `curl -X POST http://localhost:8060/deploy/complete` on old VM

After Phase 7 validation passes and writes resume on the new VM, rollback gets riskier — any new writes would be lost. Treat the post-Phase-7 window as the point-of-no-return; commit to forward-only after that.

---

## Caveats

- **Block volume is single-attach.** The volume cannot be mounted on both VMs simultaneously — Phase 3 enforces a strict cutover window.
- **Symlink ordering matters.** Docker daemon must NOT be running on the new VM before the symlink is in place (Phase 0.3) AND `/mnt/data` is mounted (Phase 4). If the daemon starts first, it will create `/var/lib/docker/volumes` as a real directory and ignore the symlink.
- **Boot volume on new VM is 50 GB** (matches old). Tight but adequate; expand later if needed.
- **Reserved IP requires the VNIC to be in a public subnet** — already true for both VMs.
- **TLS cert reissue** is handled automatically by Caddy via Let's Encrypt. No manual cert work.
- **`flyway` service has `platform: linux/amd64` pinned** in [docker-compose.yml](../docker-compose.yml) — runs under qemu emulation. Slow but functional. Already proven on the existing A2 VM, so no behavior change.
- **Scraper runs may fail mid-flight** during Phase 1 freeze. The deploy_intent mechanism stops new submissions but doesn't kill in-flight runs. Wait for `number_running = 0` before Phase 2.

---

## Open items before execution

1. Confirm the public IP is convertible to a reserved IP (Phase 0.2)
2. Confirm the new VM's primary VNIC is in the same subnet as the old VM (it should be, per provisioning script)
3. Decide whether to expand the new VM's boot volume from 50 GB → 100 GB before Phase 6 (optional; can also be done later)
