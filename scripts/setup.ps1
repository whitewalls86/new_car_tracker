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
Write-Host "`n[1/6] Creating Docker network and volumes..." -ForegroundColor Green
docker network create cartracker-net 2>$null
docker volume create cartracker_pgdata 2>$null
docker volume create cartracker_raw 2>$null
docker volume create n8n_data 2>$null

# --- Start services ---
Write-Host "`n[2/6] Starting services..." -ForegroundColor Green
docker compose up -d

# --- Wait for Postgres to be ready ---
Write-Host "`n[3/6] Waiting for Postgres to be ready..." -ForegroundColor Green
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
Write-Host "`n[4/6] Initializing database schema..." -ForegroundColor Green
Get-Content "db\schema\schema_new.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker

# --- Load example search config ---
Write-Host "`n[5/6] Loading example search config (Honda CR-V Hybrid)..." -ForegroundColor Green
Get-Content "db\seed\example_search_config.sql" | docker exec -i cartracker-postgres psql -U cartracker -d cartracker

# --- Run dbt ---
Write-Host "`n[6/6] Installing dbt packages and running initial build..." -ForegroundColor Green
docker compose run --rm dbt deps
docker compose run --rm dbt build

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Service URLs:" -ForegroundColor White
Write-Host "  Dashboard:        http://localhost:8501"
Write-Host "  n8n UI:           http://localhost:5678"
Write-Host "  Scraper API:      http://localhost:8000"
Write-Host "  Scraper Admin:    http://localhost:8000/admin"
Write-Host "  dbt Runner:       http://localhost:8081"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open n8n at http://localhost:5678"
Write-Host "  2. Create a Postgres credential (host: postgres, user: cartracker, password: from .env, db: cartracker)"
Write-Host "  3. Import the 7 workflow JSON files from n8n\workflows\"
Write-Host "  4. Wire the Postgres credential into each workflow"
Write-Host "  5. Activate the workflows"
Write-Host "  6. Add more search configs at http://localhost:8000/admin"
