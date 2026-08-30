@echo off
cd /d "%~dp0.."
powershell -ExecutionPolicy Bypass -File scripts\stop-spark.ps1
powershell -ExecutionPolicy Bypass -File scripts\reset-checkpoints.ps1
echo.
echo Ready. Start Spark with: jobs\run-spark.bat
