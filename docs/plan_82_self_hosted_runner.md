# Plan 82: Self-Hosted GitHub Actions Runner (Oracle ARM64)

**Status:** Planned
**Priority:** Unprioritized

Register the Oracle VM as a GitHub Actions self-hosted runner so CI runs natively on ARM64, matching the production environment exactly. Eliminates platform divergence (e.g. Flyway AMD64 warning) and is free.

---

## Setup

### 1. Register the runner

In the GitHub repo: **Settings → Actions → Runners → New self-hosted runner**

Select: Linux / ARM64. GitHub will generate a registration token and a set of commands to run on the VM.

```bash
# SSH into Oracle VM
ssh cartracker

# Create a runner directory
mkdir -p /opt/actions-runner && cd /opt/actions-runner

# Download the ARM64 runner (check GitHub UI for latest version)
curl -o actions-runner-linux-arm64.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.x.x/actions-runner-linux-arm64-2.x.x.tar.gz

tar xzf actions-runner-linux-arm64.tar.gz

# Configure (token comes from GitHub UI)
./config.sh --url https://github.com/<org>/cartracker-scraper --token <TOKEN>

# Install as a systemd service so it survives reboots
sudo ./svc.sh install
sudo ./svc.sh start
```

### 2. Update the workflow

Change `runs-on` in `.github/workflows/ci.yml` to target the self-hosted runner:

```yaml
jobs:
  lint:
    runs-on: self-hosted  # was: ubuntu-latest
  unit-tests:
    runs-on: self-hosted
  docker-build:
    runs-on: self-hosted
  dbt:
    runs-on: self-hosted
```

Or use a label (set during `./config.sh`) to be more explicit:

```yaml
runs-on: [self-hosted, linux, arm64]
```

---

## Considerations

- **Security** — self-hosted runners execute arbitrary code from PRs. Fine for a private repo; for public repos, restrict to trusted contributors only or use ephemeral runners
- **Postgres service container** — the `dbt` job uses a GitHub-managed Postgres service container. On self-hosted runners, service containers require Docker to be installed (it already is on the VM). Verify this works after switching
- **Concurrency** — the Oracle VM runs prod containers alongside CI. Heavy CI builds (docker compose build) may compete for CPU/RAM. Monitor resource usage after enabling
- **Runner token expiry** — the registration token expires after 1 hour but the runner itself runs indefinitely once registered

---

## Notes

- GitHub-hosted runners will still be used as fallback if the self-hosted runner is offline
- To keep GitHub-hosted runners as a fallback, use a matrix or conditional `runs-on`
- Runner logs: `sudo journalctl -u actions.runner.* -f`
