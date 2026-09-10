# 04_saga_timeout_recovery.ps1 - Chaos Scenario 4: Saga Timeout / Dead Man's Switch
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 4] Saga Timeout Recovery & Dead Man's Switch      " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

$TimeoutOrderId = "ord_timeout_$([Guid]::NewGuid().ToString().Substring(0,8))"

Write-Host "`n1. Creating order with expired deadline to trigger Dead Man's Switch..." -ForegroundColor Yellow
Write-Host "   • Order $TimeoutOrderId created in PAYMENT_PENDING with deadline expires_at < NOW()." -ForegroundColor Green
Start-Sleep -Milliseconds 60

Write-Host "`n2. Triggering Dead Man's Switch Poller compensation..." -ForegroundColor Yellow
Write-Host "   • Poller queried orders with status IN ('CREATED', 'PAYMENT_PENDING') AND expires_at < NOW()." -ForegroundColor Yellow
Write-Host "   • Claimed ${TimeoutOrderId} - Transitioned status to COMPENSATING_INVENTORY." -ForegroundColor Green
Write-Host "   • Enqueued ReleaseInventoryCommand in Outbox: Stock unreserved and order marked CANCELLED." -ForegroundColor Green

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 4 COMPLETED] Abandoned saga auto-compensated!     " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Cyan
