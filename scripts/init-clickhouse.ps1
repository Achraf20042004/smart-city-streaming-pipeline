# Apply ClickHouse schema (Kafka engine tables + MergeTree + materialized views).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$sqlDir = Join-Path $root "clickhouse\init"

Get-ChildItem $sqlDir -Filter *.sql | Sort-Object Name | ForEach-Object {
    Write-Host "Applying $($_.Name)..."
    Get-Content $_.FullName -Raw | docker exec -i clickhouse clickhouse-client --multiquery
}
Write-Host "ClickHouse schema applied."
