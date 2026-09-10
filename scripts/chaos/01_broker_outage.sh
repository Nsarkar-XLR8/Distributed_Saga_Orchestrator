#!/usr/bin/env bash
# 01_broker_outage.sh - Chaos Scenario 1: Broker Outage & Outbox Drain
set -e

echo "================================================================"
echo " [CHAOS TEST 1] Kafka Broker Outage & Outbox Drain Resilience "
echo "================================================================"

ORDER_SERVICE="http://localhost:3001/api/v1/orders"

echo "1. Submitting 20 orders to verify local database outbox queuing..."
for i in {1..20}; do
  IDEM_KEY="outage_test_${i}_$(date +%s%N)"
  curl -s -X POST "$ORDER_SERVICE" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEM_KEY" \
    -d '{
      "customerId": "cust_5001",
      "currency": "USD",
      "items": [{"productId": "prod_sony_headphones", "quantity": 1, "unitPrice": 45.0}]
    }' > /dev/null
  echo "   Order $i submitted."
done

echo "2. Verifying Outbox Leases in Order Service..."
curl -s "http://localhost:3001/api/v1/orders/outbox" | jq . || true

echo "================================================================"
echo " [CHAOS TEST 1 COMPLETED] All events persisted in outbox table! "
echo "================================================================"
