# CarTracker — First-Time Setup Script (Windows PowerShell)
# Run from the project root directory.

$ErrorActionPreference = "Stop"

Write-Host "=== CarTracker Setup ===" -ForegroundColor Cyan

# --- Check prerequisites ---
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Docker is not installed or not in PATH." -ForegroundColor Red
    Write-Host "Install Docker Desktop for Windows: https://docs.docker.com/desktop/install/windows-install/"
    exit 1
}

# --- Check .env file ---
if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Copy-Item ".env.example" ".env"
        Write-Host "Created .env from .env.example — edit it to set a strong POSTGRES_PASSWORD." -ForegroundColor Yellow
        Write-Host "Then re-run this script."
        exit 1
    } else {
        Write-Host "ERROR: No .env file found. Create one with: POSTGRES_PASSWORD=your_password" -ForegroundColor Red
        exit 1
    }
}

# --- Create external Docker resources ---
Write-Host "`n[1/7] Creating Docker network and volumes..." -ForegroundColor Green
docker network create cartracker-net 2>$null
docker volume create cartracker_pgdata 2>$null
docker volume create cartracker_raw 2>$null
docker volume create n8n_data 2>$null

# --- Start services ---
Write-Host "`n[2/7] Starting services..." -ForegroundColor Green
docker compose up -d

# --- Wait for Postgres to be ready ---
Write-Host "`n[3/7] Waiting for Postgres to be ready..." -ForegroundColor Green
$retries = 0
do {
    Start-Sleep -Seconds 2
    $retries++
    $ready = docker exec cartracker-postgres pg_isready -U cartracker 2>$null
} while ($LASTEXITCODE -ne 0 -and $retries -lt 15)

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Postgres did not become ready in time." -ForegroundColor Red
    exit 1
}
Write-Host "Postgres is ready."

# --- Initialize database schema ---
Write-Host "`n[4/7] Initializing database schema..." -ForegroundColor Green
Get-Content "db\schema\schema_new.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker

# --- Load seed data ---
Write-Host "`n[5/7] Loading seed data (search config, dbt lock, scrape claims)..." -ForegroundColor Green
Get-Content "db\seed\example_search_config.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker
Get-Content "db\seed\dbt_lock.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker
Get-Content "db\seed\detail_scrape_claims.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker

# --- Run dbt ---
Write-Host "`n[6/7] Installing dbt packages and running initial build..." -ForegroundColor Green
docker compose run --rm dbt deps
docker compose run --rm dbt build

# --- Done ---
Write-Host "`n[7/7] All services started. Workflows auto-imported by n8n entrypoint." -ForegroundColor Green

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Service URLs:" -ForegroundColor White
Write-Host "  Dashboard:        http://localhost:8501"
Write-Host "  n8n UI:           http://localhost:5678"
Write-Host "  Scraper API:      http://localhost:8000"
Write-Host "  Scraper Admin:    http://localhost:8000/admin"
Write-Host "  dbt Runner:       http://localhost:8081"
Write-Host "  pgAdmin:          http://localhost:5050"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open n8n at http://localhost:5678"
Write-Host "  2. Create a Postgres credential (host: postgres, user: cartracker, password: from .env, db: cartracker)"
Write-Host "  3. Wire the Postgres credential into each workflow's Postgres nodes"
Write-Host "  4. Verify all 7 workflows are active (auto-activated on startup)"
Write-Host "  5. Add more search configs at http://localhost:8000/admin"
