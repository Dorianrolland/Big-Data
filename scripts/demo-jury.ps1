# FleetStream - Script Jury Demo One-Click (COP-032)
# Usage:
#   .\scripts\demo-jury.ps1
#   .\scripts\demo-jury.ps1 -Fleet
#   .\scripts\demo-jury.ps1 -KpisOnly

param(
    [string]$ApiBase     = "http://localhost:8001",
    [int]$WarmupSec      = 45,
    [int]$MinDrivers     = 1,
    [int]$BacktestMaxOffers = 5000,
    [switch]$Fleet       = $false,
    [switch]$SkipBuild   = $false,
    [switch]$KpisOnly    = $false
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $ScriptDir
$StartedAt = Get-Date

function Write-Step([int]$n, [int]$total, [string]$msg) {
    Write-Host ""
    Write-Host "[$n/$total] $msg" -ForegroundColor Yellow
}

function Write-Ok([string]$msg) { Write-Host "  [OK] $msg" -ForegroundColor Green }
function Write-Fail([string]$msg) { Write-Host "  [FAIL] $msg" -ForegroundColor Red }
function Write-Info([string]$msg) { Write-Host "  ... $msg" -ForegroundColor DarkGray }

function Resolve-PythonExe {
    foreach ($candidate in @("python", "python3")) {
        if (Get-Command $candidate -ErrorAction SilentlyContinue) {
            try {
                & $candidate --version *> $null
                if ($LASTEXITCODE -eq 0) {
                    return $candidate
                }
            } catch {
                continue
            }
        }
    }
    throw "Python executable not found (python3/python)."
}

function Invoke-Python([string[]]$CommandArgs) {
    & $script:PythonExe @CommandArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed: $script:PythonExe $($CommandArgs -join ' ')"
    }
}

function Invoke-ApiSafe([string]$Url) {
    try {
        return Invoke-RestMethod -Uri $Url -TimeoutSec 8 -ErrorAction Stop
    } catch {
        return $null
    }
}

function Invoke-ApiPostSafe([string]$Url, [string]$Body) {
    try {
        return Invoke-RestMethod -Uri $Url -Method Post -ContentType "application/json" -Body $Body -TimeoutSec 10 -ErrorAction Stop
    } catch {
        return $null
    }
}

$PythonExe = Resolve-PythonExe

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  FleetStream - Demo Jury CY Tech ING3 Big Data" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ("  Mode    : {0}" -f ($(if ($Fleet) { "FLEET (multi-drivers)" } else { "Single Driver" })))
Write-Host "  API     : $ApiBase"
Write-Host "  Started : $($StartedAt.ToString('HH:mm:ss'))"

if ($KpisOnly) {
    Write-Host ""
    Write-Host "-- KPI only mode --" -ForegroundColor Yellow
    Invoke-Python @("$Root\scripts\query-copilot-kpis.py")
    exit 0
}

$TotalSteps = 6

Write-Step 1 $TotalSteps "Start Docker stack"
Set-Location $Root
$envArgs = if ($Fleet) { @("--env-file", "env/fleet_demo.env") } else { @() }
if ($SkipBuild) {
    docker compose @envArgs up -d
} else {
    docker compose @envArgs up --build -d
}
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker compose up failed"
    exit 1
}
Write-Ok "Docker stack started"

Write-Step 2 $TotalSteps "Wait API readiness ($WarmupSec s + 60 s max)"
$deadline = (Get-Date).AddSeconds($WarmupSec + 60)
$ready = $false
while ((Get-Date) -lt $deadline) {
    $health = Invoke-ApiSafe "$ApiBase/health"
    if ($health -and $health.status -eq "ok") {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 4
    Write-Info "waiting for /health ..."
}
if (-not $ready) {
    Write-Fail "API unavailable"
    exit 1
}
Write-Ok "API ready"

Write-Step 3 $TotalSteps "Check active drivers (min $MinDrivers)"
$deadline2 = (Get-Date).AddSeconds(90)
$driversOk = $false
$active = 0
while ((Get-Date) -lt $deadline2) {
    $stats = Invoke-ApiSafe "$ApiBase/stats"
    $active = if ($stats -and $stats.active_couriers) { [int]$stats.active_couriers } else { 0 }
    if ($active -ge $MinDrivers) {
        $driversOk = $true
        break
    }
    Write-Info "$active/$MinDrivers active drivers"
    Start-Sleep -Seconds 5
}
if ($driversOk) {
    Write-Ok "$active active drivers"
} else {
    Write-Fail "Less than $MinDrivers active drivers - degraded demo"
}

Write-Step 4 $TotalSteps "Critical checks"
$checks = [ordered]@{}

$perf = Invoke-ApiSafe "$ApiBase/health/performance?samples=50"
if ($perf) {
    $checks["hot_path_p99_ms"] = [math]::Round([double]$perf.p99_ms, 2)
    $checks["hot_path_ok"] = ([double]$perf.p99_ms -lt 10.0)
    if ($checks["hot_path_ok"]) {
        Write-Ok "Hot path p99 = $($checks['hot_path_p99_ms']) ms"
    } else {
        Write-Fail "Hot path p99 = $($checks['hot_path_p99_ms']) ms (target < 10)"
    }
} else {
    $checks["hot_path_ok"] = $false
    Write-Fail "Performance endpoint unavailable"
}

$scoreBody = '{"courier_id":"drv_demo_001","offer_id":"jury_offer_001","estimated_fare_eur":14.5,"estimated_distance_km":4.8,"estimated_duration_min":19,"zone_id":"Z01","demand_index":0.8,"supply_index":0.3}'
$scoreResp = Invoke-ApiPostSafe "$ApiBase/copilot/score-offer" $scoreBody
if ($scoreResp -and $null -ne $scoreResp.score) {
    $checks["score_value"] = [math]::Round([double]$scoreResp.score, 3)
    $checks["score_ok"] = $true
    Write-Ok "Score Copilot = $($checks['score_value']) / decision = $($scoreResp.decision)"
} else {
    $checks["score_ok"] = $false
    Write-Fail "Score endpoint unavailable"
}

$copilotHealth = Invoke-ApiSafe "$ApiBase/copilot/health"
$checks["health_ok"] = ($null -ne $copilotHealth)
if ($copilotHealth) {
    Write-Ok "Copilot health OK"
} else {
    Write-Fail "Copilot health unavailable"
}

$fleet = Invoke-ApiSafe "$ApiBase/copilot/fleet/overview?limit=10"
$checks["fleet_overview_ok"] = ($null -ne $fleet)
$checks["fleet_active_drivers"] = if ($fleet) { [int]$fleet.active_drivers } else { 0 }
if ($fleet) {
    Write-Ok "fleet/overview OK: active_drivers=$($fleet.active_drivers)"
} else {
    Write-Fail "fleet/overview unavailable"
}

Write-Step 5 $TotalSteps "Generate KPI analytics + backtest"
Write-Info "Build data mart ..."
Invoke-Python @("$Root\scripts\build-copilot-mart.py")

Write-Info "Run KPI queries ..."
Invoke-Python @("$Root\scripts\query-copilot-kpis.py")

Write-Info "Run backtest (max offers: $BacktestMaxOffers) ..."
Invoke-Python @(
    "$Root\ml\backtest_copilot.py",
    "--out", "$Root\data\reports",
    "--max-offers", "$BacktestMaxOffers"
)
Write-Ok "KPI and backtest generated"

Write-Step 6 $TotalSteps "Final KPI snapshot"
$elapsed = [math]::Round(((Get-Date) - $StartedAt).TotalSeconds)
$snapshot = @{
    generated_at = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
    startup_elapsed_s = $elapsed
    fleet_mode = $Fleet.IsPresent
    checks = $checks
    backtest_csv = "$Root\data\reports\backtest_summary.csv"
    mart_dir = "$Root\data\marts\copilot"
}
$snapPath = "$Root\data\reports\demo_jury_kpi_snapshot.json"
$snapshot | ConvertTo-Json -Depth 5 | Set-Content $snapPath -Encoding UTF8
Write-Ok "Snapshot: $snapPath"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  DEMO READY - startup in ${elapsed}s" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  URLs:"
Write-Host "    Copilot PWA      -> $ApiBase/copilot"
Write-Host "    Wallboard Fleet  -> $ApiBase/copilot"
Write-Host "    API docs         -> $ApiBase/docs"
Write-Host "    Redpanda UI      -> http://localhost:8080"
Write-Host "    Grafana          -> http://localhost:3000 (admin/fleetstream)"
Write-Host ""
Write-Host "  Files:"
Write-Host "    Backtest CSV     -> $Root\data\reports\backtest_summary.csv"
Write-Host "    KPI Snapshot     -> $snapPath"
Write-Host "    Data Mart        -> $Root\data\marts\copilot\"
Write-Host ""
Write-Host "  Narration (5 min)  -> docs/demo-runbook.md" -ForegroundColor Yellow
Write-Host ""

$allOk = (($checks.Values | Where-Object { $_ -is [bool] }) -notcontains $false)
if ($allOk) {
    Write-Host "  All critical checks are GREEN." -ForegroundColor Green
} else {
    Write-Host "  Some critical checks are RED. See details above." -ForegroundColor Red
}
Write-Host ""
