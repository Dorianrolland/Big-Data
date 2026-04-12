param(
    [ValidateSet("tables", "schema", "sample", "count", "events-sample", "events-count", "shell", "ui")]
    [string]$Action = "tables",
    [int]$SampleRows = 10
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dataDir = Join-Path $projectRoot "data"
$dbPath = Join-Path $dataDir "fleetstream.duckdb"
$parquetRoot = Join-Path $dataDir "parquet"
$parquetGlob = ((Join-Path $parquetRoot "**\*.parquet") -replace "\\", "/")
$eventsRoot = Join-Path $dataDir "parquet_events"
$eventsGlob = ((Join-Path $eventsRoot "**\*.parquet") -replace "\\", "/")

$winGetDuckDb = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages\DuckDB.cli_Microsoft.Winget.Source_8wekyb3d8bbwe\duckdb.exe"
$duckdbCmd = $null
if (Test-Path $winGetDuckDb) {
    $duckdbCmd = $winGetDuckDb
}
else {
    $duckdbFromPath = Get-Command duckdb -ErrorAction SilentlyContinue
    if ($duckdbFromPath) {
        $duckdbCmd = $duckdbFromPath.Source
    }
}

if (-not $duckdbCmd) {
    throw "DuckDB CLI introuvable. Installe-le avec: winget install --id DuckDB.cli --source winget --accept-source-agreements --accept-package-agreements"
}

if (-not (Test-Path $parquetRoot)) {
    throw "Dossier Parquet introuvable: $parquetRoot. Demarre d'abord la stack Docker pour generer des donnees."
}

$parquetCount = (Get-ChildItem $parquetRoot -Recurse -Filter *.parquet -File -ErrorAction SilentlyContinue | Measure-Object).Count
if ($parquetCount -eq 0) {
    Write-Warning "Aucun fichier Parquet trouve dans $parquetRoot. La vue sera creee, mais vide pour le moment."
}

if (-not (Test-Path $dataDir)) {
    New-Item -ItemType Directory -Path $dataDir | Out-Null
}

$bootstrapSql = @"
CREATE OR REPLACE VIEW gps_events AS
SELECT *
FROM read_parquet('$parquetGlob', hive_partitioning = true);

CREATE OR REPLACE VIEW copilot_events AS
SELECT *
FROM read_parquet('$eventsGlob', hive_partitioning = true);
"@

& $duckdbCmd $dbPath -c $bootstrapSql | Out-Null

switch ($Action) {
    "tables" {
        & $duckdbCmd $dbPath -c "SHOW TABLES;"
    }
    "schema" {
        & $duckdbCmd $dbPath -c "DESCRIBE gps_events;"
    }
    "sample" {
        & $duckdbCmd $dbPath -c "SELECT livreur_id, ts, lat, lon, speed_kmh, status FROM gps_events ORDER BY ts DESC LIMIT $SampleRows;"
    }
    "count" {
        & $duckdbCmd $dbPath -c "SELECT COUNT(*) AS nb_lignes FROM gps_events;"
    }
    "events-sample" {
        & $duckdbCmd $dbPath -c "SELECT topic, event_type, courier_id, offer_id, status, ts FROM copilot_events ORDER BY ts DESC LIMIT $SampleRows;"
    }
    "events-count" {
        & $duckdbCmd $dbPath -c "SELECT COUNT(*) AS nb_evenements FROM copilot_events;"
    }
    "shell" {
        Write-Host ""
        Write-Host "DuckDB ouvert sur: $dbPath"
        Write-Host "Vues disponibles: gps_events, copilot_events"
        Write-Host "Exemples:"
        Write-Host "  SHOW TABLES;"
        Write-Host "  DESCRIBE gps_events;"
        Write-Host "  SELECT * FROM copilot_events LIMIT 10;"
        Write-Host ""
        & $duckdbCmd $dbPath
    }
    "ui" {
        Write-Host ""
        Write-Host "Ouverture de l'UI DuckDB locale (localhost) sur: $dbPath"
        Write-Host "Vues disponibles: gps_events, copilot_events"
        Write-Host ""
        & $duckdbCmd $dbPath -ui
    }
}
