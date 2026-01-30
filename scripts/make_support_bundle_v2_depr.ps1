<#
CarTracker Support Bundle Generator (Windows / PowerShell)

Creates a zip containing:
- Repo files (selectively)
- n8n workflow JSONs
- DB schema file(s)
- Docker compose config (redacted)
- Docker status + container logs (tail)
- Optional: schema-only pg_dump + small sample queries (redacted)

Usage:
  powershell -ExecutionPolicy Bypass -File .\scripts\make_support_bundle.ps1
  powershell -ExecutionPolicy Bypass -File .\scripts\make_support_bundle.ps1 -IncludeDbDumps
  powershell -ExecutionPolicy Bypass -File .\scripts\make_support_bundle.ps1 -IncludeDbDumps -LogTail 800

Notes:
- Does NOT include .env, .git, .venv, volumes, or raw database data directories.
- Redacts obvious secrets in generated text outputs.
#>

param(
  [int]$LogTail = 300,
  [switch]$IncludeDbDumps = $false,
  [switch]$IncludeSampleQueries = $false
)

$ErrorActionPreference = "Stop"

# ---------- Helpers ----------
function Ensure-Dir($path) {
  if (-not (Test-Path $path)) { New-Item -ItemType Directory -Path $path | Out-Null }
}

function Write-TextFile($path, $content) {
  $dir = Split-Path $path -Parent
  Ensure-Dir $dir
  $content | Out-File -FilePath $path -Encoding UTF8
}

function Copy-IfExists($src, $dst) {
  if (Test-Path $src) {
    Ensure-Dir (Split-Path $dst -Parent)
    Copy-Item -Path $src -Destination $dst -Recurse -Force
  }
}

function Copy-DirFiltered($srcDir, $dstDir, $excludePatterns) {
  if (-not (Test-Path $srcDir)) { return }
  Ensure-Dir $dstDir

  $files = Get-ChildItem -Path $srcDir -Recurse -File
  foreach ($f in $files) {
    $rel = $f.FullName.Substring($srcDir.Length).TrimStart([char]'\',[char]'/')
    $skip = $false
    foreach ($pat in $excludePatterns) {
      if ($rel -like $pat) { $skip = $true; break }
    }
    if ($skip) { continue }

    $destPath = Join-Path $dstDir $rel
    Ensure-Dir (Split-Path $destPath -Parent)
    Copy-Item -Path $f.FullName -Destination $destPath -Force
  }
}


function Redact-Secrets($text) {
  if ($null -eq $text) { return $text }

  $redacted = $text

  # Redact common env keys in yaml/json/log-ish output
  $patterns = @(
    '(POSTGRES_PASSWORD:\s*)(.+)',
    '(POSTGRES_PASSWORD=)(.+)',
    '(N8N_ENCRYPTION_KEY:\s*)(.+)',
    '(N8N_ENCRYPTION_KEY=)(.+)',
    '(DB_PASSWORD:\s*)(.+)',
    '(DB_PASSWORD=)(.+)',
    '(PASSWORD:\s*)(.+)',
    '(PASSWORD=)(.+)',
    '(TOKEN:\s*)(.+)',
    '(TOKEN=)(.+)',
    '(API_KEY:\s*)(.+)',
    '(API_KEY=)(.+)',
    '(AUTHORIZATION:\s*)(.+)'
  )

  foreach ($p in $patterns) {
    $redacted = [regex]::Replace($redacted, $p, '$1<REDACTED>', 'IgnoreCase')
  }

  # Redact things that look like long secrets (rough heuristic)
  $redacted = [regex]::Replace($redacted, '([A-Za-z0-9_\-]{24,})', '<REDACTED_LONG_TOKEN>')

  return $redacted
}

function Run-Cmd($cmd, $outPath, $redact = $true) {
  try {
    $output = Invoke-Expression $cmd 2>&1 | Out-String
  } catch {
    $output = $_ | Out-String
  }

  if ($redact) { $output = Redact-Secrets $output }
  Write-TextFile $outPath $output
}

# ---------- Paths ----------
$repoRoot = (Get-Location).Path
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$bundleRoot = Join-Path $repoRoot ("support_bundle_" + $timestamp)
Ensure-Dir $bundleRoot

$manifestPath = Join-Path $bundleRoot "MANIFEST.txt"

# ---------- Manifest header ----------
$manifest = @()
$manifest += "CarTracker Support Bundle"
$manifest += "Created: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")"
$manifest += "RepoRoot: $repoRoot"
$manifest += ""

# Git metadata
Run-Cmd 'git rev-parse HEAD' (Join-Path $bundleRoot "git_head.txt") $false
Run-Cmd 'git status --porcelain' (Join-Path $bundleRoot "git_status_porcelain.txt") $false
Run-Cmd 'git remote -v' (Join-Path $bundleRoot "git_remotes.txt") $false

# File tree (fast, useful)
Run-Cmd 'cmd /c "tree /F"' (Join-Path $bundleRoot "tree_full.txt") $false

# ---------- Copy key repo files ----------
Copy-IfExists (Join-Path $repoRoot "docker-compose.yml") (Join-Path $bundleRoot "repo\docker-compose.yml")
Copy-IfExists (Join-Path $repoRoot "Dockerfile") (Join-Path $bundleRoot "repo\Dockerfile")
Copy-IfExists (Join-Path $repoRoot ".env.example") (Join-Path $bundleRoot "repo\.env.example")
Copy-IfExists (Join-Path $repoRoot ".gitignore") (Join-Path $bundleRoot "repo\.gitignore")

# Python entrypoints / core files
Copy-IfExists (Join-Path $repoRoot "app.py") (Join-Path $bundleRoot "repo\app.py")
Copy-IfExists (Join-Path $repoRoot "scrape_results.py") (Join-Path $bundleRoot "repo\scrape_results.py")
Copy-IfExists (Join-Path $repoRoot "scrape_detail.py") (Join-Path $bundleRoot "repo\scrape_detail.py")
Copy-IfExists (Join-Path $repoRoot "parse_detail_page.py") (Join-Path $bundleRoot "repo\parse_detail_page.py")
Copy-IfExists (Join-Path $repoRoot "results_page_cards.py") (Join-Path $bundleRoot "repo\results_page_cards.py")

# Copy folders (selective, avoids .venv/.git)
Copy-IfExists (Join-Path $repoRoot "processors") (Join-Path $bundleRoot "repo\processors")
Copy-IfExists (Join-Path $repoRoot "parsers") (Join-Path $bundleRoot "repo\parsers")
Copy-IfExists (Join-Path $repoRoot "reporting") (Join-Path $bundleRoot "repo\reporting")
Copy-IfExists (Join-Path $repoRoot "n8n") (Join-Path $bundleRoot "repo\n8n")
Copy-IfExists (Join-Path $repoRoot "db") (Join-Path $bundleRoot "repo\db")

# dbt project (include source-of-truth only; exclude generated/local artifacts)
Copy-DirFiltered (Join-Path $repoRoot "dbt") (Join-Path $bundleRoot "repo\dbt") @(
  "target\*",
  "logs\*",
  "dbt_packages\*",
  ".user.yml",
  "package-lock.yml"
)

# Helper folders (optional, but useful context)
Copy-IfExists (Join-Path $repoRoot "scripts") (Join-Path $bundleRoot "repo\scripts")
Copy-IfExists (Join-Path $repoRoot "docs") (Join-Path $bundleRoot "repo\docs")


# ---------- Docker runtime state ----------
Run-Cmd 'docker ps -a --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"' (Join-Path $bundleRoot "docker_ps_a.txt") $false
Run-Cmd 'docker volume ls' (Join-Path $bundleRoot "docker_volume_ls.txt") $false
Run-Cmd 'docker network ls' (Join-Path $bundleRoot "docker_network_ls.txt") $false

# Compose expanded config (IMPORTANT: redacted)
Run-Cmd 'docker compose config' (Join-Path $bundleRoot "docker_compose_config_redacted.yml") $true

# Compose logs (tail)
Run-Cmd ("docker compose logs --tail " + $LogTail) (Join-Path $bundleRoot ("docker_compose_logs_tail_" + $LogTail + ".txt")) $true
Run-Cmd ("docker compose logs --tail " + $LogTail + " scraper") (Join-Path $bundleRoot ("logs_scraper_tail_" + $LogTail + ".txt")) $true
Run-Cmd ("docker compose logs --tail " + $LogTail + " postgres") (Join-Path $bundleRoot ("logs_postgres_tail_" + $LogTail + ".txt")) $true
Run-Cmd ("docker compose logs --tail " + $LogTail + " n8n") (Join-Path $bundleRoot ("logs_n8n_tail_" + $LogTail + ".txt")) $true

# ---------- Optional: DB schema-only dump ----------
if ($IncludeDbDumps) {
  # Schema-only dump (safe-ish, no data). Uses container-local pg_dump.
  # Writes into bundle/db/
  $dumpPath = Join-Path $bundleRoot "db\schema_only.sql"
  Ensure-Dir (Split-Path $dumpPath -Parent)

  # Note: uses POSTGRES_USER/DB from your setup; adjust here if you ever rename.
  # We avoid embedding password: pg_dump inside container can connect via local trust.
  Run-Cmd 'docker exec -i cartracker-postgres pg_dump -U cartracker -d cartracker --schema-only' $dumpPath $true

  if ($IncludeSampleQueries) {
    # Small, last-N snapshots from key tables (redacted). Avoids big exports.
    $qPath = Join-Path $bundleRoot "db\sample_queries.txt"
    $q = @()
    $q += "\dt"
    $q += "SELECT now() as collected_at;"
    $q += "SELECT count(*) AS srp_obs FROM srp_observations;"
    $q += "SELECT count(*) AS detail_obs FROM detail_observations;"
    $q += "SELECT count(*) AS carousel_hints FROM detail_carousel_hints;"
    $q += "SELECT count(*) AS vehicles FROM vehicles;"
    $q += ""
    $q += "-- recent observations"
    $q += "SELECT * FROM srp_observations ORDER BY fetched_at DESC NULLS LAST LIMIT 25;"
    $q += "SELECT * FROM detail_observations ORDER BY fetched_at DESC NULLS LAST LIMIT 25;"
    $q += "SELECT * FROM detail_carousel_hints ORDER BY fetched_at DESC NULLS LAST LIMIT 25;"
    $q += ""
    $q += "-- recent artifacts processing (if exists)"
    $q += "SELECT * FROM artifact_processing ORDER BY started_at DESC NULLS LAST LIMIT 25;"

    Write-TextFile $qPath ($q -join "`r`n")

    Run-Cmd ("docker exec -i cartracker-postgres psql -U cartracker -d cartracker -v ON_ERROR_STOP=1 -f /dev/stdin < `"" + $qPath + "`"") (Join-Path $bundleRoot "db\sample_outputs.txt") $true
  }
}

# ---------- Final manifest ----------
$manifest += "Included:"
$manifest += "  - repo/: compose files, Dockerfile, db/, n8n/, reporting/, parsers/, processors/, dbt/, scripts/, docs/, key .py files"
$manifest += "  - docker state: docker ps/volume ls/network ls"
$manifest += "  - docker compose config (redacted)"
$manifest += "  - docker compose logs (tail: $LogTail) (redacted)"
if ($IncludeDbDumps) {
  $manifest += "  - db/schema_only.sql (schema-only pg_dump)"
  if ($IncludeSampleQueries) {
    $manifest += "  - db/sample_queries.txt + db/sample_outputs.txt (redacted)"
  }
}
$manifest += ""
$manifest += "Redaction:"
$manifest += "  - Common password/token env keys replaced with <REDACTED>"
$manifest += "  - Long token-like strings replaced with <REDACTED_LONG_TOKEN>"
$manifest += ""
$manifest += "How to use:"
$manifest += "  - Upload the resulting zip when asking for debugging help."
$manifest += "  - Mention the symptom + when it started + any recent changes."
$manifest += ""

Write-TextFile $manifestPath ($manifest -join "`r`n")

# ---------- Zip it ----------
$zipPath = Join-Path $repoRoot ("support_bundle_" + $timestamp + ".zip")
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

Compress-Archive -Path (Join-Path $bundleRoot "*") -DestinationPath $zipPath -Force

Write-Host ""
Write-Host "✅ Support bundle created:"
Write-Host "   $zipPath"
Write-Host ""
Write-Host "Tip: upload this zip with your debugging question."
