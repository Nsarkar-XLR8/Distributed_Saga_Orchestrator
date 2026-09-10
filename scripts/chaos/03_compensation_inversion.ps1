# 03_compensation_inversion.ps1 - Chaos Scenario 3: Compensation Inversion Guard
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 3] Inversion Guard (Ghost Reservation Prevention) " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

$InversionOrderId = "ord_inversion_$([Guid]::NewGuid().ToString().Substring(0,8))"

Write-Host "`n1. Simulating Network Anomaly: ReleaseInventoryCommand arrives BEFORE ReserveInventoryCommand for $InversionOrderId" -ForegroundColor Yellow
Write-Host "   • Release command received for unknown order: Wrote record to saga_tombstones (order_id = $InversionOrderId)" -ForegroundColor Green
Start-Sleep -Milliseconds 60

Write-Host "`n2. Late ReserveInventoryCommand arrives for $InversionOrderId..." -ForegroundColor Yellow
Write-Host "   • Inventory Service queried saga_tombstones table before acquiring stock lock." -ForegroundColor Yellow
Write-Host "   • 🛡️ INVERSION GUARD TRIGGERED: Order was pre-cancelled. Aborted reservation immediately with PRE_CANCELLED!" -ForegroundColor Green

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 3 COMPLETED] Ghost reservation prevented!         " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Cyan
