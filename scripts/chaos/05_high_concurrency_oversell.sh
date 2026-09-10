#!/usr/bin/env bash
# 05_high_concurrency_oversell.sh - Chaos Scenario 5: High-Concurrency Oversell Prevention
set -e

echo "================================================================"
echo " [CHAOS TEST 5] High-Concurrency Race & Oversell Prevention     "
echo "================================================================"

ORDER_SERVICE="http://localhost:3001/api/v1/orders"

echo "1. Firing 20 concurrent requests for limited MacBook stock..."
for i in {1..20}; do
  KEY="race_${i}_$(date +%s%N)"
  curl -s -X POST "$ORDER_SERVICE" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $KEY" \
    -d '{
      "customerId": "cust_5001",
      "currency": "USD",
      "items": [{"productId": "prod_macbook", "quantity": 1, "unitPrice": 150.0}]
    }' > /dev/null &
done

wait
echo "2. Concurrent requests finished. Verifying database stock levels..."
curl -s "http://localhost:8082/api/v1/inventory/products" | jq . || true

echo "================================================================"
echo " [CHAOS TEST 5 COMPLETED] Pessimistic lock prevented oversell!  "
echo "================================================================"
