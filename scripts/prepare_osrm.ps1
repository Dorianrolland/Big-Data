# FleetStream - Prepare OSRM data for New York routing
# Downloads the New York State OSM extract and pre-processes it for OSRM.
# Run this ONCE before starting docker compose with the OSRM service.
#
# Usage:  .\scripts\prepare_osrm.ps1

[CmdletBinding()]
param(
    [string]$RegionUrl = "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf",
    [string]$Profile   = "car",
    [string]$Image     = "ghcr.io/project-osrm/osrm-backend:v5.27.1"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root      = Split-Path -Parent $ScriptDir
$OsrmDir   = Join-Path $Root "data\osrm"
$PbfFile   = Join-Path $OsrmDir "new-york-latest.osm.pbf"

if (-not (Test-Path $OsrmDir)) {
    New-Item -ItemType Directory -Path $OsrmDir | Out-Null
}

Write-Host "==> Downloading New York State OSM extract..." -ForegroundColor Cyan
if (-not (Test-Path $PbfFile)) {
    Invoke-WebRequest -Uri $RegionUrl -OutFile $PbfFile -UseBasicParsing
    Write-Host "    Downloaded to $PbfFile"
} else {
    Write-Host "    Already exists: $PbfFile"
}

function Invoke-Osrm([string[]]$OsrmArgs) {
    $dockerArgs = @("run", "--rm", "-v", "${OsrmDir}:/data", $Image) + $OsrmArgs
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($OsrmArgs -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Write-Host "==> Extracting OSRM data (profile: $Profile)..." -ForegroundColor Cyan
Invoke-Osrm @("osrm-extract", "-p", "/opt/$Profile.lua", "/data/new-york-latest.osm.pbf")

Write-Host "==> Partitioning..." -ForegroundColor Cyan
Invoke-Osrm @("osrm-partition", "/data/new-york-latest.osrm")

Write-Host "==> Customizing..." -ForegroundColor Cyan
Invoke-Osrm @("osrm-customize", "/data/new-york-latest.osrm")

Write-Host ""
Write-Host "==> OSRM data ready in $OsrmDir" -ForegroundColor Green
Write-Host "    Start the routing service with: docker compose --profile routing up osrm"
