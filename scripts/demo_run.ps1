# FleetStream — Script Démo Jury (one-shot)
# Usage: .\scripts\demo_run.ps1
# Windows PowerShell 5+ ou pwsh 7+

param(
    [string]$ApiBase   = "http://localhost:8001",
    [int]$WarmupSec    = 30,
    [switch]$SkipBuild = $false
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root      = Split-Path -Parent $ScriptDir

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  FleetStream — Demo Jury One-Shot                    " -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# ── 1. Démarrage stack ────────────────────────────────────────────────────────
Write-Host "[1/5] Démarrage du stack Docker..." -ForegroundColor Yellow
Set-Location $Root
if ($SkipBuild) {
    docker compose up -d
} else {
    docker compose up --build -d
}
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up a échoué"; exit 1 }

# ── 2. Warmup / attente readiness ─────────────────────────────────────────────
Write-Host "[2/5] Attente readiness API ($WarmupSec s)..." -ForegroundColor Yellow
$deadline = (Get-Date).AddSeconds($WarmupSec + 60)
$ready = $false
while ((Get-Date) -lt $deadline) {
    try {
        $resp = Invoke-RestMethod -Uri "$ApiBase/health" -TimeoutSec 3 -ErrorAction Stop
        if ($resp.status -eq "ok") { $ready = $true; break }
    } catch { }
    Start-Sleep -Seconds 3
    Write-Host "  ...attente API" -ForegroundColor DarkGray
}
if (-not $ready) { Write-Error "API non disponible après $(WarmupSec+60) s"; exit 1 }
Write-Host "  API prête !" -ForegroundColor Green

# ── 3. Checks critiques ───────────────────────────────────────────────────────
Write-Host "[3/5] Vérification des checks critiques..." -ForegroundColor Yellow

$checks = @{}

try {
    $perf = Invoke-RestMethod -Uri "$ApiBase/health/performance?samples=50" -TimeoutSec 10
    $checks["hot_path_p99_ms"]    = [math]::Round($perf.p99_ms, 2)
    $checks["hot_path_ok"]        = ($perf.p99_ms -lt 10)
} catch { $checks["hot_path_ok"] = $false }

try {
    $score = Invoke-RestMethod -Uri "$ApiBase/copilot/score-offer" -Method Post -ContentType "application/json" `
        -Body '{"courier_id":"drv_demo_001","offer_id":"demo_offer_001","estimated_fare_eur":12.5,"estimated_distance_km":4.2,"estimated_duration_min":18,"zone_id":"Z01","demand_index":0.75,"supply_index":0.4}' `
        -TimeoutSec 10
    $checks["score_ok"]    = ($null -ne $score.score)
    $checks["score_value"] = [math]::Round($score.score, 3)
} catch { $checks["score_ok"] = $false }

try {
    $health = Invoke-RestMethod -Uri "$ApiBase/copilot/health" -TimeoutSec 10
    $checks["copilot_health_ok"] = $true
} catch { $checks["copilot_health_ok"] = $false }

Write-Host ""
Write-Host "  Checks:"
foreach ($k in $checks.Keys) {
    $v = $checks[$k]
    $color = if ($v -is [bool] -and -not $v) { "Red" } else { "Green" }
    Write-Host ("    {0,-30} {1}" -f $k, $v) -ForegroundColor $color
}

# ── 4. Backtest scoreboard ────────────────────────────────────────────────────
Write-Host ""
Write-Host "[4/5] Génération du scoreboard Copilot vs baselines..." -ForegroundColor Yellow
try {
    python3 "$Root\ml\backtest_copilot.py" --out "$Root\data\reports"
} catch {
    python "$Root\ml\backtest_copilot.py" --out "$Root\data\reports"
}

# ── 5. Snapshot KPI final ──────────────────────────────────────────────────────
Write-Host ""
Write-Host "[5/5] Snapshot KPI final..." -ForegroundColor Yellow
$kpiPath = "$Root\data\reports\demo_kpi_snapshot.json"
$snapshot = @{
    timestamp      = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
    hot_path_p99_ms = $checks["hot_path_p99_ms"]
    score_value    = $checks["score_value"]
    checks         = $checks
    backtest_csv   = "$Root\data\reports\backtest_summary.csv"
}
$snapshot | ConvertTo-Json -Depth 4 | Set-Content $kpiPath -Encoding UTF8
Write-Host "  Snapshot saved: $kpiPath" -ForegroundColor Green

# ── Récap URLs ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  DEMO PRETE — URLs"                                    -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  API docs        -> $ApiBase/docs"
Write-Host "  Copilot PWA     -> $ApiBase/copilot"
Write-Host "  Live map        -> $ApiBase/map"
Write-Host "  Redpanda UI     -> http://localhost:8080"
Write-Host "  Grafana         -> http://localhost:3000  (admin/fleetstream)"
Write-Host "  Prometheus      -> http://localhost:9090"
Write-Host ""
Write-Host "  Backtest CSV    -> $Root\data\reports\backtest_summary.csv"
Write-Host "  KPI snapshot    -> $kpiPath"
Write-Host ""
Write-Host "Narration 5 min -> docs/demo-runbook.md"   -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Cyan
