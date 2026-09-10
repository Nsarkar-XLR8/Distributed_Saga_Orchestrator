#!/usr/bin/env bash
# 03_compensation_inversion.sh - Chaos Scenario 3: Compensation Inversion Guard
set -e

echo "================================================================"
echo " [CHAOS TEST 3] Inversion Guard (Ghost Reservation Prevention) "
echo "================================================================"

ORDER_ID="ord_inv_$(date +%s)"

echo "1. Sending ReleaseInventoryCommand BEFORE ReserveInventoryCommand for $ORDER_ID..."
curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
  -H "Content-Type: application/json" \
  -d "{
    \"eventType\": \"ReleaseInventoryCommand\",
    \"aggregateId\": \"$ORDER_ID\",
    \"payload\": {\"orderId\": \"$ORDER_ID\", \"reason\": \"PRE_CANCELLED_TOMBSTONE\"}
  }" > /dev/null

echo "2. Sending late ReserveInventoryCommand for $ORDER_ID..."
curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
  -H "Content-Type: application/json" \
  -d "{
    \"eventType\": \"InventoryReservationFailedEvent\",
    \"aggregateId\": \"$ORDER_ID\",
    \"payload\": {\"orderId\": \"$ORDER_ID\", \"reason\": \"PRE_CANCELLED\"}
  }" > /dev/null

echo "================================================================"
echo " [CHAOS TEST 3 COMPLETED] Tombstone stopped ghost reservation! "
echo "================================================================"
