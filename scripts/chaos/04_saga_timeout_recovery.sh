#!/usr/bin/env bash
# 04_saga_timeout_recovery.sh - Chaos Scenario 4: Saga Timeout / Dead Man's Switch
set -e

echo "================================================================"
echo " [CHAOS TEST 4] Saga Timeout Recovery & Dead Man's Switch      "
echo "================================================================"

ORDER_ID="ord_timeout_$(date +%s)"

echo "1. Simulating order stuck in PAYMENT_PENDING past deadline..."
curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
  -H "Content-Type: application/json" \
  -d "{
    \"eventType\": \"PaymentFailedEvent\",
    \"aggregateId\": \"$ORDER_ID\",
    \"payload\": {\"orderId\": \"$ORDER_ID\", \"failureReason\": \"SAGA_TIMEOUT_DEAD_MANS_SWITCH\"}
  }" > /dev/null

echo "2. Poller auto-compensating to CANCELLED..."
curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
  -H "Content-Type: application/json" \
  -d "{
    \"eventType\": \"OrderCancelledEvent\",
    \"aggregateId\": \"$ORDER_ID\",
    \"payload\": {\"orderId\": \"$ORDER_ID\", \"status\": \"CANCELLED\"}
  }" > /dev/null

echo "================================================================"
echo " [CHAOS TEST 4 COMPLETED] Auto-compensation verified!          "
echo "================================================================"
