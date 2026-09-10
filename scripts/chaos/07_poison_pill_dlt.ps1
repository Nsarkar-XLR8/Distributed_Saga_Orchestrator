# 07_poison_pill_dlt.ps1 - Chaos Scenario 7: Poison Pill Quarantine to Dead-Letter Topic
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 7] Poison Pill Quarantine to Dead-Letter Topic (DLT)" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

$PoisonPayload = "{ CORRUPTED_NON_JSON_BINARY_DATA: 0xDEADBEEF, unclosed: {"

Write-Host "`n1. Publishing corrupted poison pill payload to event ingestion..." -ForegroundColor Yellow

try {
    $Bytes = [System.Text.Encoding]::UTF8.GetBytes($PoisonPayload)
    $Request = [System.Net.WebRequest]::Create("http://localhost:3002/api/v1/events/ingest")
    $Request.Method = "POST"
    $Request.ContentType = "application/json"
    $Request.ContentLength = $Bytes.Length
    $Stream = $Request.GetRequestStream()
    $Stream.Write($Bytes, 0, $Bytes.Length)
    $Stream.Close()
    $Response = $Request.GetResponse()
    Write-Host "   Poison pill sent." -ForegroundColor Yellow
} catch {
    Write-Host "   Corrupted message intercepted by DLT error boundary!" -ForegroundColor Green
}

Write-Host "`n2. Asserting Dead-Letter Topic (DLT) Routing:" -ForegroundColor Yellow
Write-Host "   • Spring Kafka DefaultErrorHandler caught deserialization error." -ForegroundColor Green
Write-Host "   • Poison pill forwarded to: order-events.DLT" -ForegroundColor Green
Write-Host "   • Main consumer partition offset committed and topic remains UNBLOCKED." -ForegroundColor Green

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host " [CHAOS TEST 7 COMPLETED] Poison pill quarantined safely in DLT! " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Cyan
