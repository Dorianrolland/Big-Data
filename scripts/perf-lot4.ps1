param(
    [string]$Url = "http://localhost:8001",
    [int]$IngestWindow = 20,
    [int]$ScoreRequests = 300,
    [int]$ScoreConcurrency = 30,
    [double]$ScoreTimeout = 10.0
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scriptPath = Join-Path $projectRoot "scripts\perf-lot4.py"

if (-not (Test-Path $scriptPath)) {
    throw "Script introuvable: $scriptPath"
}

python $scriptPath --url $Url --ingest-window $IngestWindow --score-requests $ScoreRequests --score-concurrency $ScoreConcurrency --score-timeout $ScoreTimeout
