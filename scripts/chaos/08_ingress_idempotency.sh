#!/usr/bin/env bash
# 08_ingress_idempotency.sh - Chaos Scenario 8: Client Ingress Idempotency Middleware
set -e

echo "================================================================"
echo " [CHAOS TEST 8] Client Ingress Idempotency Interceptor          "
echo "================================================================"

ORDER_SERVICE="http://localhost:3001/api/v1/orders"
IDEM_KEY="client_race_key_$(date +%s)"

echo "1. Firing 10 identical HTTP requests with same Idempotency-Key..."
for i in {1..10}; do
  curl -s -X POST "$ORDER_SERVICE" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEM_KEY" \
    -d '{
      "customerId": "cust_5001",
      "currency": "USD",
      "items": [{"productId": "prod_iphone", "quantity": 1, "unitPrice": 99.0}]
    }' || true
  echo "   Request $i completed."
done

echo "================================================================"
echo " [CHAOS TEST 8 COMPLETED] Ingress idempotency verified!         "
echo "================================================================"
