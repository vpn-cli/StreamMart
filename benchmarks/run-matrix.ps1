# Automated benchmark runner for StreamMart Phase 1 matrix
# Configurations:
#   1: baseline        (pool=10, keep-alive=false)
#   2: pool_only       (pool=30, keep-alive=false)
#   3: keepalive_only  (pool=10, keep-alive=true)
#   4: pool_keepalive  (pool=30, keep-alive=true)

param(
    [string]$TargetConfig = "all" # "baseline", "pool_only", "keepalive_only", "pool_keepalive", or "all"
)

$k6Path = "C:\Program Files\k6\k6.exe"
$rawDir = "benchmarks/raw"
if (-not (Test-Path $rawDir)) { New-Item -ItemType Directory -Force -Path $rawDir }

function Set-Configuration($poolMax, $keepAlive) {
    Write-Host "`n>>> [CONFIG] Applying: DB_POOL_MAX=$poolMax, PROXY_KEEP_ALIVE=$keepAlive..." -ForegroundColor Cyan
    $env:DB_POOL_MAX = "$poolMax"
    $env:PROXY_KEEP_ALIVE = "$keepAlive"
    
    # Recreate containers with target environment
    docker compose up -d --build api-gateway payment-service
    
    # Wait until both containers are healthy
    Write-Host ">>> Waiting for api-gateway and payment-service healthchecks..."
    for ($i = 0; $i -lt 30; $i++) {
        $gwHealth = docker inspect streammart-gateway --format '{{.State.Health.Status}}' 2>$null
        $pmHealth = docker inspect streammart-payment --format '{{.State.Health.Status}}' 2>$null
        if ($gwHealth -eq "healthy" -and $pmHealth -eq "healthy") {
            Write-Host ">>> Containers healthy!" -ForegroundColor Green
            break
        }
        Start-Sleep -Seconds 2
    }
}

function Drain-Outbox() {
    # Check backlog count
    $count = (docker exec streammart-postgres psql -U postgres -d streammart -t -c "SELECT count(*) FROM outbox_events WHERE published = false AND topic = 'payment-successful';" 2>$null).Trim()
    $backlog = [int]($count -replace '\D','')
    
    if ($backlog -eq 0) {
        return @{ Backlog = 0; DrainSeconds = 0.0 }
    }
    
    Write-Host ">>> Outbox backlog: $backlog events. Waiting for relay to drain..." -ForegroundColor Yellow
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $remaining = $backlog
    while ($remaining -gt 0) {
        Start-Sleep -Milliseconds 250
        $c = (docker exec streammart-postgres psql -U postgres -d streammart -t -c "SELECT count(*) FROM outbox_events WHERE published = false AND topic = 'payment-successful';" 2>$null).Trim()
        $remaining = [int]($c -replace '\D','')
        if ($sw.Elapsed.TotalSeconds -gt 60) {
            Write-Host ">>> Warning: Drain timed out after 60s" -ForegroundColor Red
            break
        }
    }
    $sw.Stop()
    $drainSec = [math]::Round($sw.Elapsed.TotalSeconds, 2)
    Write-Host ">>> Drained $backlog events in $drainSec seconds." -ForegroundColor Green
    return @{ Backlog = $backlog; DrainSeconds = $drainSec }
}

function Run-K6($vus, $duration, $outputFile) {
    & $k6Path run --vus $vus --duration $duration benchmarks/gateway-load.js | Tee-Object -FilePath $outputFile
    $content = Get-Content $outputFile -Raw
    
    # Extract metrics via regex
    $rps = 0.0
    if ($content -match 'http_reqs\.*:\s+([0-9.]+)\s+([0-9.]+)/s') {
        $rps = [double]$matches[2]
    }
    
    $p50 = 0.0
    $p95 = 0.0
    if ($content -match 'http_req_duration\.*:\s+avg=[0-9.]+(?:ms|s)\s+min=[0-9.]+(?:ms|s)\s+med=([0-9.]+)(ms|s)\s+max=[0-9.]+(?:ms|s)\s+p\(90\)=[0-9.]+(?:ms|s)\s+p\(95\)=([0-9.]+)(ms|s)') {
        $p50 = [double]$matches[1] * (if ($matches[2] -eq 's') { 1000 } else { 1 })
        $p95 = [double]$matches[3] * (if ($matches[4] -eq 's') { 1000 } else { 1 })
    }
    
    $errorRate = 0.0
    if ($content -match 'http_req_failed\.*:\s+([0-9.]+)%') {
        $errorRate = [double]$matches[1]
    }
    
    return @{
        Rps = $rps
        p50 = $p50
        p95 = $p95
        ErrorRate = $errorRate
    }
}

function Benchmark-Config($configName, $poolMax, $keepAlive) {
    Set-Configuration $poolMax $keepAlive
    
    $results = @{}
    
    foreach ($vus in @(20, 50, 100)) {
        Write-Host "`n=======================================================" -ForegroundColor Magenta
        Write-Host " Benchmark: $configName | VUs: $vus" -ForegroundColor Magenta
        Write-Host "=======================================================" -ForegroundColor Magenta
        
        # 1. Warm-up run (discarded)
        Write-Host "--- Discarded Warm-up Run (15s) ---" -ForegroundColor Gray
        $warmupFile = "$rawDir/${configName}-${vus}vu-warmup.txt"
        $null = Run-K6 $vus "15s" $warmupFile
        $null = Drain-Outbox
        Start-Sleep -Seconds 2
        
        # 2. Three measured runs
        $runs = @()
        for ($r = 1; $r -le 3; $r++) {
            Write-Host "`n--- Measured Run $r / 3 ($vus VUs, 30s) ---" -ForegroundColor Cyan
            $outFile = "$rawDir/${configName}-${vus}vu-run${r}.txt"
            $k6Res = Run-K6 $vus "30s" $outFile
            $drainRes = Drain-Outbox
            
            $runData = [PSCustomObject]@{
                Run = $r
                Rps = $k6Res.Rps
                p50 = $k6Res.p50
                p95 = $k6Res.p95
                ErrorRate = $k6Res.ErrorRate
                Backlog = $drainRes.Backlog
                DrainSec = $drainRes.DrainSeconds
            }
            $runs += $runData
            Start-Sleep -Seconds 2
        }
        $results[$vus] = $runs
    }
    
    return $results
}

# Export functions for modular invocation
Write-Host "Loaded run-matrix.ps1 helper."
