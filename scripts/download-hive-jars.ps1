# Download the AWS SDK bundle required by Hive Metastore (too large for GitHub).
$ErrorActionPreference = "Stop"

$version = "1.12.367"
$jarName = "aws-java-sdk-bundle-$version.jar"
$jarsDir = Join-Path (Split-Path -Parent $PSScriptRoot) "hive-jars"
$dest = Join-Path $jarsDir $jarName
$url = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$version/$jarName"

if (-not (Test-Path $jarsDir)) {
    New-Item -ItemType Directory -Path $jarsDir | Out-Null
}

if (Test-Path $dest) {
    Write-Host "Hive jar already present: $jarName"
    exit 0
}

Write-Host "Downloading $jarName (~280 MB)..."
Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
Write-Host "Saved to $dest"
