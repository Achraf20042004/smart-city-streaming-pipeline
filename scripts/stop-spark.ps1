# Stop all active Spark applications on the cluster.
$apps = docker exec project-spark-master-1 curl -s http://localhost:8080/json/ 2>$null | python -c "import sys,json; d=json.load(sys.stdin); print(' '.join(a['id'] for a in d.get('activeapps',[])))"

if ($apps) {
    foreach ($id in $apps.Split(' ')) {
        if ($id) {
            Write-Host "Killing Spark app $id..."
            docker exec project-spark-master-1 curl -s -X POST "http://localhost:8080/app/kill/?id=$id" | Out-Null
        }
    }
    Start-Sleep -Seconds 3
    Write-Host "Spark apps stopped."
} else {
    Write-Host "No active Spark apps found."
}
