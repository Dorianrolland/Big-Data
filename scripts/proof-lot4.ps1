param(
    [string]$Url = "http://localhost:8001"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

python (Join-Path $projectRoot "scripts\smoke-e2e.py") --url $Url --timeout 180 --score-requests 120 --score-concurrency 12
python (Join-Path $projectRoot "scripts\perf-lot4.py") --url $Url --ingest-window 70 --score-requests 80 --score-concurrency 8
