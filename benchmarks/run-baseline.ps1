. ./benchmarks/run-matrix.ps1
$res = Benchmark-Config "baseline" 10 "false"
$res | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 "benchmarks/raw/results-baseline.json"
Write-Host "`n>>> Baseline Finished! Saved to benchmarks/raw/results-baseline.json"
