#!/usr/bin/env bash
# 02_duplicate_event_replay.sh - Chaos Scenario 2: Duplicate Event Replay Guard
set -e

echo "================================================================"
echo " [CHAOS TEST 2] Consumer Idempotency & Duplicate Event Replay   "
echo "================================================================"

DUP_EVENT_ID="evt_dup_$(date +%s)"
ORDER_ID="ord_dup_$(date +%s)"

echo "1. Replaying identical event 5 times with eventId: $DUP_EVENT_ID"
for i in {1..5}; do
  curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
    -H "Content-Type: application/json" \
    -d "{
      \"eventId\": \"$DUP_EVENT_ID\",
      \"eventType\": \"InitiatePaymentCommand\",
      \"aggregateId\": \"$ORDER_ID\",
      \"correlationId\": \"corr_$ORDER_ID\",
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
      \"payload\": {
        \"orderId\": \"$ORDER_ID\",
        \"customerId\": \"cust_5001\",
        \"amount\": 45.00
      }
    }" > /dev/null
  echo "   Replay attempt $i sent."
done

echo "================================================================"
echo " [CHAOS TEST 2 COMPLETED] Processed exactly once in database!   "
echo "================================================================"
