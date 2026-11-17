# Cold-start benchmark for StreamMart Docker Compose cluster
Write-Host "=========================================================="
Write-Host "⏱️ StreamMart: Measuring Docker Compose Cold-Start Time"
Write-Host "=========================================================="

Write-Host "`n[Step 1] Bringing down cluster and removing network..."
docker compose down

Write-Host "`n[Step 2] Executing: docker compose up -d --wait (timed)..."
$elapsed = Measure-Command {
    docker compose up -d --wait
}

Write-Host "`n=========================================================="
Write-Host "✅ Cold-Start Results:"
Write-Host "   Total Seconds:       $([math]::Round($elapsed.TotalSeconds, 2)) s"
Write-Host "   Total Milliseconds:  $($elapsed.TotalMilliseconds) ms"
Write-Host "=========================================================="
