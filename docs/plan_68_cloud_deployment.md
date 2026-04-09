# Plan 68: Cloud Deployment — Oracle Free Tier

**Status:** ✅ Complete
**Priority:** 1 — Live URL is the #1 portfolio artifact
**Live URL:** https://cartracker-scraper.duckdns.org

Deploy the full cartracker stack to Oracle Cloud's Always Free tier. Oracle's free tier is the most generous available — 4 Ampere ARM cores, 24GB RAM, 200GB storage, no time limit. The full stack (scraper, dbt_runner, ops, dashboard, dbt, Postgres, n8n, MinIO, archiver, pgAdmin, FlareSolverr) runs comfortably with no cost.

---

## Why Oracle Free Tier

| Reason | Why it matters |
|--------|---|
| **Genuinely free forever** | Not a trial — no credit card required; no surprise billing after 12 months |
| **Sufficient resources** | 4 ARM cores + 24GB RAM + 200GB storage runs all 8 containers without throttle |
| **Real infrastructure** | VPCs, security lists (firewalls), DNS, compute shapes, block storage — gives portfolio evidence |
| **No credit card** | Stand-out for career/portfolio — most cloud projects require paid tier |

---

## High-level architecture

```
┌──────────────────────────────────────────┐
│     Oracle Cloud Always Free VM          │
│     (4 ARM cores, 24GB RAM, 200GB)       │
├──────────────────────────────────────────┤
│  Docker Compose (8 services):            │
│  ├─ scraper       (Python FastAPI)       │
│  ├─ dbt_runner    (Python dbt)           │
│  ├─ ops          (Python FastAPI + UI)   │
│  ├─ dashboard    (Streamlit)             │
│  ├─ dbt          (dbt Cloud CLI)         │
│  ├─ postgres     (PostgreSQL 16)         │
│  ├─ n8n          (Workflow orchestrator) │
│  ├─ minio        (S3-compatible storage) │
│  ├─ archiver     (Parquet pipeline)      │
│  ├─ pgadmin      (SQL IDE)               │
│  └─ flareSolverr (Cloudflare bypass)     │
├──────────────────────────────────────────┤
│  Ports (via security list):              │
│  ├─ 80/443  → nginx reverse proxy        │
│  ├─ 22      → SSH (for deployments)      │
│  └─ Others  → closed except internal     │
└──────────────────────────────────────────┘
```

---

## Phase 1: Oracle infrastructure (manual)

### 1.1 Oracle account & VM provisioning

1. **Create Oracle Cloud Account**
   - Go to oracle.com/cloud/free
   - Sign up (no credit card)
   - Verify email and phone
   - Land in Oracle Cloud Console

2. **Provision Compute Instance**
   - Compute → Instances → Create Instance
   - Name: `cartracker-prod` (or similar)
   - Image: **Ubuntu 24.04 (Canonical)** — ensure ARM64 compatible
   - Shape: **Ampere (ARM)** — always-free eligible
   - VCN: Create new or use existing VCN in home region
   - Subnet: Public subnet (we'll add public IP)
   - SSH Key: Download and save `.key` file locally (this is critical)
   - Public IP: Assign (required for access)
   - Boot volume: 200GB (max free tier)
   - Create

3. **Note the instance details**
   - Public IP address (e.g., `130.61.x.x`)
   - Private IP (internal, e.g., `10.0.1.x`)
   - Instance OCID (useful for Terraform later)

### 1.2 Security list (firewall rules)

1. **VCN → Security Lists → VCN's default security list**
   - Ingress rules to add:
     ```
     Protocol: TCP
     Source CIDR: 0.0.0.0/0
     Destination Port: 22 (SSH)
     
     Protocol: TCP
     Source CIDR: 0.0.0.0/0
     Destination Port: 80 (HTTP)
     
     Protocol: TCP
     Source CIDR: 0.0.0.0/0
     Destination Port: 443 (HTTPS)
     ```
   - Egress: Default allows all (keep as-is for simplicity now)
   - Note: n8n, Postgres, MinIO ports stay internal-only; no public access

### 1.3 SSH connectivity verification

```bash
# Test SSH access (from local machine)
ssh -i /path/to/oracle_key.key ubuntu@<public_ip>

# You should land in a Ubuntu shell as `ubuntu` user
# Verify Docker is not yet installed (we'll do that next)
docker --version  # Should fail
```

---

## Phase 2: VM setup (deploy from local)

### 2.1 Install runtime dependencies

```bash
# SSH into the VM
ssh -i /path/to/oracle_key.key ubuntu@<public_ip>

# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker + Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu
newgrp docker

# Install Docker Compose (included in Docker 20.10+, verify)
docker compose version  # Should print version

# Install Git
sudo apt install -y git

# Create app directory
mkdir -p /opt/cartracker
cd /opt/cartracker
```

### 2.2 Clone repo and checkout master

```bash
cd /opt/cartracker
git clone https://github.com/<your-org>/cartracker-scraper.git .
git checkout master
```

### 2.3 ARM architecture check

```bash
# Verify arch
uname -m  # Should print: aarch64 (ARM64)

# Most images are multi-arch (postgres, redis, etc.) but verify in docker-compose.yml:
# - postgres:16  ✅ (has ARM64 build)
# - ubuntu:latest  ✅ (has ARM64)
# - node:20  ✅ (has ARM64)
# - python:3.13  ✅ (has ARM64)
# - minio  ✅ (has ARM64)

# For any custom images, ensure Dockerfile is ARM-compatible:
# - No x86 binaries (gcc, etc. are fine; avoid pre-compiled x86 wheels)
# - Alpine is safer than Debian for cross-arch; verify in scraper/Dockerfile, dbt/Dockerfile
```

### 2.4 Secrets and environment setup

```bash
cd /opt/cartracker

# Create .env from .env.example
cp .env.example .env

# Edit .env with production values
nano .env

# Required changes for cloud (sample values):
# POSTGRES_PASSWORD=<strong-random-pwd>
# FASTAPI_ADMIN_KEY=<strong-random-key>
# N8N_BASIC_AUTH_ACTIVE=true
# N8N_USER_MANAGEMENT_JWT_SECRET=<random>
# MINIO_ROOT_PASSWORD=<strong-random-pwd>
# FLARESOLVERR_API_URL=http://flareSolverr:8191  (keep internal)
# Telegram token / webhook (if using alerts)

# Restrict .env permissions
chmod 600 .env

# Do NOT commit .env to git (verify .gitignore)
```

### 2.5 Build and launch

```bash
cd /opt/cartracker

# Build all images for ARM (first time only, ~5-10 min)
docker compose build

# Start all services
docker compose up -d

# Verify all containers are running
docker compose ps
# Should show: scraper, dbt_runner, ops, dashboard, postgres, n8n, minio, archiver, pgadmin, flareSolverr

# Check logs for startup errors
docker compose logs -f postgres  # Wait for "ready to accept connections"
docker compose logs -f n8n       # Wait for "Editor running on ..."

# Test internal connectivity
docker compose exec postgres psql -U cartracker -c "SELECT 1"  # Should return 1
```

---

## Phase 3: DNS and reverse proxy (free DuckDNS path)

Since the project is fully free, we'll use **DuckDNS** for a free permanent subdomain and **Caddy** for automatic HTTPS.

### 3.1 Get a free DuckDNS subdomain

DuckDNS provides free DNS subdomains that point to your cloud IP.

1. **Sign up at duckdns.org**
   - Go to https://www.duckdns.org/
   - Sign in with GitHub, Google, or other OAuth (no credit card required)
   - You'll land on a dashboard

2. **Create a subdomain**
   - Click the `+` or "Add Domain" button
   - Choose a name (e.g., `cartracker`)
   - You now own: `cartracker.duckdns.org` (permanently free)

3. **Point it to your Oracle public IP**
   - In DuckDNS dashboard, enter your Oracle public IP (e.g., `130.61.45.123`)
   - Click "Update IP" / "Save"
   - DuckDNS resolves `cartracker.duckdns.org` → `130.61.45.123`

4. **Verify DNS resolution**
   ```bash
   # From local machine (or VM)
   ping cartracker.duckdns.org
   # Should resolve to your Oracle public IP
   
   # Or use dig
   dig cartracker.duckdns.org
   # Should show your Oracle IP in the answer section
   ```

### 3.2 Add Caddy reverse proxy

Caddy is a modern web server that automatically handles HTTPS via Let's Encrypt. No manual cert management needed.

Add to `docker-compose.yml`:

```yaml
caddy:
  image: caddy:latest
  container_name: caddy
  ports:
    - "80:80"
    - "443:443"
  volumes:
    - ./Caddyfile:/etc/caddy/Caddyfile:ro
    - caddy_data:/data
    - caddy_config:/config
  depends_on:
    - dashboard
    - ops
    - n8n
  networks:
    - cartracker

volumes:
  caddy_data:
  caddy_config:
```

### 3.3 Create Caddyfile

Create a file named `Caddyfile` in your repo root:

```
cartracker.duckdns.org {
  reverse_proxy / dashboard:8501
  reverse_proxy /admin* ops:5005
  reverse_proxy /api* ops:5005
  reverse_proxy /n8n* n8n:5678
  reverse_proxy /pgadmin* pgadmin:80
}
```

This tells Caddy:
- Listen on ports 80/443
- Automatically get an HTTPS cert from Let's Encrypt
- Route `/` to the dashboard
- Route `/admin*` and `/api*` to ops
- Route `/n8n*` to n8n
- Route `/pgadmin*` to pgAdmin

### 3.4 Launch Caddy

On your VM:

```bash
cd /opt/cartracker

# Rebuild (includes Caddy now)
docker compose build

# Start all services
docker compose up -d

# Verify Caddy is running
docker compose ps | grep caddy

# Watch Caddy logs (especially first startup for cert generation)
docker compose logs -f caddy
# You should see: "autohttpshttps: ...obtaining certificate..."
# After ~30 seconds: "...certificate obtained successfully"
```

### 3.5 Test public access

From your local machine:

```bash
# Test via DuckDNS domain (should redirect to HTTPS)
curl https://cartracker.duckdns.org/

# Check the dashboard is accessible
open https://cartracker.duckdns.org
# Should show Streamlit dashboard with a green HTTPS lock

# Test admin UI
curl https://cartracker.duckdns.org/admin/

# Test n8n
curl https://cartracker.duckdns.org/n8n/
```

If you get SSL warnings on first access, wait 30 seconds—Let's Encrypt cert is being generated. Refresh after a minute.

### 3.6 Update security list (if needed)

Verify your Oracle security list allows:
- **Ingress TCP 80** (HTTP → redirects to HTTPS)
- **Ingress TCP 443** (HTTPS)
- Egress is open (for outbound cert validation)

(You already configured this in Phase 1, but double-check if you get connection refused.)

---

## Phase 4: Deployment automation

### 4.1 Deploy script (`deploy.sh`)

Create at repo root:

```bash
#!/bin/bash
set -e

# Simple deploy script for cloud VM
# Run this after git push from local machine

REPO_DIR="/opt/cartracker"

# Ensure we're on master
cd $REPO_DIR
git fetch origin
git checkout master
git pull origin master

# Rebuild images (captures new code)
docker compose build

# Restart services (minimal downtime)
docker compose up -d

# Verify health
echo "Waiting for services to be healthy..."
sleep 10
docker compose ps

echo "Deploy complete. Check logs with: docker compose logs -f"
```

Make it executable:
```bash
chmod +x /opt/cartracker/deploy.sh
```

### 4.2 Remote deploy from local machine

```bash
# From your local machine
ssh -i /path/to/oracle_key.key ubuntu@<public_ip> /opt/cartracker/deploy.sh
```

Or add a convenience alias to `~/.ssh/config`:
```
Host cartracker
  HostName <oracle-public-ip>
  User ubuntu
  IdentityFile /path/to/oracle_key.key
```

Then:
```bash
ssh cartracker /opt/cartracker/deploy.sh
```

---

## Phase 5: Monitoring and validation

### 5.1 Service health checks

```bash
# SSH to VM
ssh cartracker

# Check all services
docker compose ps

# Check logs for errors
docker compose logs --tail=50 scraper
docker compose logs --tail=50 dbt_runner
docker compose logs --tail=50 ops
docker compose logs --tail=50 dashboard

# Verify Postgres is healthy
docker compose exec postgres psql -U cartracker -c "SELECT version();"

# Verify n8n is running
curl http://localhost:5678/api/v1/health

# Verify MinIO is reachable
curl http://localhost:9000/minio/bootstrap.html  # Should return 200 (redirects)
```

### 5.2 Resource monitoring

```bash
# Check VM resource usage
free -h           # Memory usage
df -h             # Disk usage
top -b -n 1       # CPU + processes (press q to exit)

# Docker-specific
docker stats       # Container resource usage (CPU, memory)

# For persistent monitoring, consider adding:
# - Prometheus container (scrape metrics)
# - Grafana container (visualize)
# But for MVP, manual checks suffice
```

### 5.3 External connectivity test

From local machine:
```bash
# If using domain
curl https://cartracker.example.com/

# If using IP directly (before HTTPS)
curl http://<oracle-public-ip>:8501  # Streamlit dashboard

# Test n8n via reverse proxy
curl http://<oracle-public-ip>/n8n/

# Test ops admin UI
curl http://<oracle-public-ip>/admin/

# Test API (should require auth via Plan 65)
curl http://<oracle-public-ip>/api/health
```

---

## Phase 6: Handoff to Plan 65 (auth)

Once all services are running and accessible via public IP/domain:

1. **Document the live URL** — this is your portfolio artifact
2. **Do NOT expose publicly yet** — wait for Plan 65 (Authelia + Google OAuth)
3. **Plan 65 will add authentication layer** — then you can safely share the link

---

## Status (2026-04-08)

**Completed through Phase 2:**
- ✅ Oracle account created
- ✅ Compute instance provisioned (4 ARM cores, 24GB RAM, 200GB storage)
- ✅ Security list configured (ports 22, 80, 443 inbound)
- ✅ SSH access verified
- ✅ Docker + Compose installed on VM
- ✅ Repo cloned and master checked out
- ✅ `.env` created with production secrets
- ✅ `docker compose build` successful (after ARM64 dbt fix)
- ✅ `docker compose up -d` successful (after creating external networks/volumes)
- ✅ 7 of 11 containers running and healthy:
  - postgres (healthy)
  - dashboard
  - archiver
  - minio
  - flaresolverr
  - n8n
  - pgadmin

**BLOCKER — Application import errors (not infrastructure):**
- ❌ scraper: `ModuleNotFoundError: No module named 'scraper'` — app.py line 14
- ❌ ops: `ImportError: attempted relative import with no known parent package` — app.py line 11
- ❌ dbt_runner: No logs (unknown cause)

These are pre-existing code structure issues unrelated to cloud deployment. Must be fixed before proceeding to Phase 3.

**Still needed for Phase 3+:**
- [ ] DuckDNS account created and subdomain configured
- [ ] DuckDNS pointing to Oracle public IP
- [ ] DNS resolution verified (`ping cartracker.duckdns.org`)
- [ ] Caddyfile created with service routes
- [ ] Caddy container deployed and logs showing cert generation
- [ ] HTTPS certs auto-generated by Let's Encrypt
- [ ] `deploy.sh` created and tested
- [ ] All services accessible via `https://cartracker.duckdns.org`
- [ ] HTTPS lock visible in browser (green ✅)
- [ ] Ready for Plan 65 (auth + firewall)

---

## Notes

- **Total cost:** $0
  - Oracle Always Free VM: free forever
  - DuckDNS subdomain: free forever
  - Let's Encrypt HTTPS certs: free forever (auto-renewed)
  - No credit card required at any step

- **External networks/volumes:** docker-compose.yml declares `cartracker-net`, `cartracker_pgdata`, `cartracker_raw`, `n8n_data` as external. Must create before `docker compose up`:
  ```bash
  docker network create cartracker-net
  docker volume create cartracker_pgdata cartracker_raw n8n_data
  ```
  Add this to `setup.sh` for future deployments.

- **ARM compatibility fix:** dbt-postgres:1.8.2 image lacks ARM64 build. Solution: use Python base image + pip install dbt-postgres. Applied to dbt_runner/Dockerfile.

- **Import errors:** scraper, ops, dbt_runner have application-level import/module errors. Not infrastructure issues. Must fix code structure before these services can run.

- **VM reboot:** Containers will restart automatically if you reboot (via `restart: always` in compose). If not, you can manually `docker compose up -d` or add a cron job.

- **Backup strategy:** MinIO is already archiving Parquet to S3 within its own container. For database backup, consider a Plan 6x backup strategy later.

- **Downtime for deploys:** ~30s (containers stop, rebuild, start). For zero-downtime, consider blue-green deployment in a future plan.

- **DuckDNS IP updates:** If your Oracle public IP ever changes (rare, but possible), simply update it in the DuckDNS dashboard. DNS will propagate immediately.

- **Caddy cert renewal:** Caddy automatically renews Let's Encrypt certs 30 days before expiry. No manual intervention needed.

