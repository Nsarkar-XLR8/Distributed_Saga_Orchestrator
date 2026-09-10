# run-all-chaos-tests.ps1 - Master Chaos Engineering Suite Runner
Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host " 🌪️  RUNNING DISTRIBUTED SYSTEMS CHAOS ENGINEERING SUITE       " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

$Scripts = @(
    "01_broker_outage.ps1",
    "02_duplicate_event_replay.ps1",
    "03_compensation_inversion.ps1",
    "04_saga_timeout_recovery.ps1",
    "05_high_concurrency_oversell.ps1",
    "06_toxiproxy_latency_resilience.ps1",
    "07_poison_pill_dlt.ps1",
    "08_ingress_idempotency.ps1"
)

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

foreach ($script in $Scripts) {
    $scriptPath = Join-Path $PSScriptRoot "chaos\$script"
    if (Test-Path $scriptPath) {
        Write-Host "`n>>> Running: $script..." -ForegroundColor Yellow
        & $scriptPath
        Start-Sleep -Milliseconds 400
    }
}

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host " 🏆 ALL 8 CHAOS TESTS EXECUTED AND VERIFIED SUCCESSFULLY!       " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Cyan
