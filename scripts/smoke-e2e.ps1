param(
    [string]$Url = "http://localhost:8001",
    [string]$Driver = "L001",
    [int]$Timeout = 180,
    [int]$ScoreRequests = 240,
    [int]$ScoreConcurrency = 30,
    [switch]$Up,
    [switch]$Build,
    [switch]$Down
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scriptPath = Join-Path $projectRoot "scripts\smoke-e2e.py"

if (-not (Test-Path $scriptPath)) {
    throw "Smoke script introuvable: $scriptPath"
}

$args = @(
    $scriptPath,
    "--url", $Url,
    "--driver", $Driver,
    "--timeout", "$Timeout",
    "--score-requests", "$ScoreRequests",
    "--score-concurrency", "$ScoreConcurrency"
)

if ($Up) { $args += "--up" }
if ($Build) { $args += "--build" }
if ($Down) { $args += "--down" }

python @args
