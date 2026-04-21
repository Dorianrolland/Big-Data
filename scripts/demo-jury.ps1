# FleetStream — Script Jury Demo One-Click (COP-030)
# Lance la démo flotte complète et génère les preuves KPI automatiquement.
#
# Usage:
#   .\scripts\demo-jury.ps1                        # mode standard
#   .\scripts\demo-jury.ps1 -Fleet                 # mode flotte multi-chauffeurs
#   .\scripts\demo-jury.ps1 -SkipBuild -MinDrivers 20
#
# Critères d'acceptation:
#   - Exécutable en une commande
#   - KPI de fin générés automatiquement
#   - Flow de présentation clair et reproductible

param(
    [string]$ApiBase     = "http://localhost:8001",
    [int]$WarmupSec      = 45,
    [int]$MinDrivers     = 1,
    [switch]$Fleet       = $false,
    [switch]$SkipBuild   = $false,
    [switch]$KpisOnly    = $false
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root      = Split-Path -Parent $ScriptDir
$StartedAt = Get-Date

function Write-Step([int]$n, [int]$total, [string]$msg) {
    Write-Host ""
    Write-Host "[$n/$total] $msg" -ForegroundColor Yellow
}

function Write-Ok([string]$msg)  { Write-Host "  [OK] $msg" -ForegroundColor Green }
function Write-Fail([string]$msg){ Write-Host "  [!!] $msg" -ForegroundColor Red }
function Write-Info([string]$msg){ Write-Host "  ... $msg" -ForegroundColor DarkGray }

function Invoke-ApiSafe([string]$url) {
    try { return Invoke-RestMethod -Uri $url -TimeoutSec 8 -ErrorAction Stop }
    catch { return $null }
}

function Invoke-ApiPostSafe([string]$url, [string]$body) {
    try {
        return Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" -Body $body -TimeoutSec 10 -ErrorAction Stop
    } catch { return $null }
}

# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  FleetStream — Demo Jury CY Tech ING3 Big Data        ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host "  Mode    : $( if ($Fleet) { 'FLEET (multi-chauffeurs)' } else { 'Single Driver' } )"
Write-Host "  API     : $ApiBase"
Write-Host "  Started : $($StartedAt.ToString('HH:mm:ss'))"

if ($KpisOnly) {
    Write-Host ""
    Write-Host "── Mode KPI uniquement ──" -ForegroundColor Yellow
    try { python3 "$Root/scripts/query-copilot-kpis.py" } catch { python "$Root/scripts/query-copilot-kpis.py" }
    exit 0
}

$TotalSteps = 6

# ── STEP 1 : Stack Docker ─────────────────────────────────────────────────────
Write-Step 1 $TotalSteps "Démarrage du stack Docker"
Set-Location $Root
$envArgs = if ($Fleet) { @("--env-file", "env/fleet_demo.env") } else { @() }
if ($SkipBuild) {
    docker compose @envArgs up -d
} else {
    docker compose @envArgs up --build -d
}
if ($LASTEXITCODE -ne 0) { Write-Fail "docker compose up a échoué"; exit 1 }
Write-Ok "Stack démarré"

# ── STEP 2 : Warmup API ───────────────────────────────────────────────────────
Write-Step 2 $TotalSteps "Attente readiness API (${WarmupSec}s + 60s max)"
$deadline = (Get-Date).AddSeconds($WarmupSec + 60)
$ready = $false
while ((Get-Date) -lt $deadline) {
    $h = Invoke-ApiSafe "$ApiBase/health"
    if ($h -and $h.status -eq "ok") { $ready = $true; break }
    Start-Sleep -Seconds 4
    Write-Info "en attente..."
}
if (-not $ready) { Write-Fail "API non disponible"; exit 1 }
Write-Ok "API prête"

# ── STEP 3 : Vérification chauffeurs actifs ───────────────────────────────────
Write-Step 3 $TotalSteps "Vérification chauffeurs actifs (min $MinDrivers)"
$deadline2 = (Get-Date).AddSeconds(90)
$driversOk = $false
while ((Get-Date) -lt $deadline2) {
    $stats = Invoke-ApiSafe "$ApiBase/stats"
    $active = if ($stats -and $stats.active_couriers) { [int]$stats.active_couriers } else { 0 }
    if ($active -ge $MinDrivers) { $driversOk = $true; break }
    Write-Info "$active/$MinDrivers chauffeurs actifs"
    Start-Sleep -Seconds 5
}
if ($driversOk) { Write-Ok "$active chauffeurs actifs" } else { Write-Fail "Moins de $MinDrivers chauffeurs — démo dégradée" }

# ── STEP 4 : Checks critiques ─────────────────────────────────────────────────
Write-Step 4 $TotalSteps "Checks critiques"
$checks = [ordered]@{}

$perf = Invoke-ApiSafe "$ApiBase/health/performance?samples=50"
if ($perf) {
    $checks["hot_path_p99_ms"] = [math]::Round($perf.p99_ms, 2)
    $checks["hot_path_ok"]     = ($perf.p99_ms -lt 10)
    if ($checks["hot_path_ok"]) { Write-Ok "Hot path p99 = $($checks['hot_path_p99_ms']) ms" }
    else { Write-Fail "Hot path p99 = $($checks['hot_path_p99_ms']) ms (cible < 10)" }
} else { $checks["hot_path_ok"] = $false; Write-Fail "Performance endpoint indisponible" }

$scoreBody = '{"courier_id":"drv_demo_001","offer_id":"jury_offer_001","estimated_fare_eur":14.5,"estimated_distance_km":4.8,"estimated_duration_min":19,"zone_id":"Z01","demand_index":0.8,"supply_index":0.3}'
$scoreResp = Invoke-ApiPostSafe "$ApiBase/copilot/score-offer" $scoreBody
if ($scoreResp -and $null -ne $scoreResp.score) {
    $checks["score_value"] = [math]::Round($scoreResp.score, 3)
    $checks["score_ok"]    = $true
    Write-Ok "Score Copilot = $($checks['score_value']) — $($scoreResp.decision)"
} else { $checks["score_ok"] = $false; Write-Fail "Score endpoint indisponible" }

$health = Invoke-ApiSafe "$ApiBase/copilot/health"
$checks["health_ok"] = ($null -ne $health)
if ($health) { Write-Ok "Copilot health OK" } else { Write-Fail "Health endpoint indisponible" }

$fleet = Invoke-ApiSafe "$ApiBase/copilot/fleet/overview?limit=10"
$checks["fleet_overview_ok"]     = ($null -ne $fleet)
$checks["fleet_active_drivers"]  = if ($fleet) { [int]$fleet.active_drivers } else { 0 }
if ($fleet) { Write-Ok "Fleet overview: $($fleet.active_drivers) chauffeurs, pression $($fleet.avg_pressure_ratio)" }
else { Write-Fail "Fleet overview indisponible" }

# ── STEP 5 : KPI analytiques + Backtest ──────────────────────────────────────
Write-Step 5 $TotalSteps "Génération KPIs analytiques + backtest"

Write-Info "Build du Data Mart..."
try { python3 "$Root/scripts/build-copilot-mart.py" } catch { python "$Root/scripts/build-copilot-mart.py" }

Write-Info "Requêtes KPI..."
try { python3 "$Root/scripts/query-copilot-kpis.py" } catch { python "$Root/scripts/query-copilot-kpis.py" }

Write-Info "Backtest Copilot vs baselines..."
try { python3 "$Root/ml/backtest_copilot.py" --out "$Root/data/reports" } catch { python "$Root/ml/backtest_copilot.py" --out "$Root/data/reports" }

Write-Ok "KPIs et backtest générés"

# ── STEP 6 : Snapshot KPI final ───────────────────────────────────────────────
Write-Step 6 $TotalSteps "Snapshot KPI final"
$elapsed = [math]::Round(((Get-Date) - $StartedAt).TotalSeconds)
$snapshot = @{
    generated_at        = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
    startup_elapsed_s   = $elapsed
    fleet_mode          = $Fleet.IsPresent
    checks              = $checks
    backtest_csv        = "$Root\data\reports\backtest_summary.csv"
    mart_dir            = "$Root\data\marts\copilot"
}
$snapPath = "$Root\data\reports\demo_jury_kpi_snapshot.json"
$snapshot | ConvertTo-Json -Depth 5 | Set-Content $snapPath -Encoding UTF8
Write-Ok "Snapshot: $snapPath"

# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  DEMO PRETE — Démarrage en ${elapsed}s                       ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "  URLs:" -ForegroundColor White
Write-Host "    Copilot PWA        -> $ApiBase/copilot"
Write-Host "    Wallboard Flotte   -> $ApiBase/copilot  (section Wallboard Flotte)"
Write-Host "    API docs           -> $ApiBase/docs"
Write-Host "    Redpanda UI        -> http://localhost:8080"
Write-Host "    Grafana            -> http://localhost:3000  (admin/fleetstream)"
Write-Host ""
Write-Host "  Fichiers:" -ForegroundColor White
Write-Host "    Backtest CSV       -> $Root\data\reports\backtest_summary.csv"
Write-Host "    KPI Snapshot       -> $snapPath"
Write-Host "    Data Mart          -> $Root\data\marts\copilot\"
Write-Host ""
Write-Host "  Narration (5 min)   -> docs/demo-runbook.md" -ForegroundColor Yellow
Write-Host ""

# Résumé checks
$allOk = ($checks.Values | Where-Object { $_ -is [bool] } | ForEach-Object { $_ }) -notcontains $false
if ($allOk) {
    Write-Host "  Tous les checks VERTS — demo optimale !" -ForegroundColor Green
} else {
    Write-Host "  Certains checks ROUGE — voir details ci-dessus" -ForegroundColor Red
}
Write-Host ""
