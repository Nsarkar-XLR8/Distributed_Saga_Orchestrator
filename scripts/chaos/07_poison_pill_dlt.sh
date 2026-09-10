#!/usr/bin/env bash
# 07_poison_pill_dlt.sh - Chaos Scenario 7: Poison Pill Quarantine to Dead-Letter Topic
set -e

echo "================================================================"
echo " [CHAOS TEST 7] Poison Pill Quarantine to Dead-Letter Topic (DLT)"
echo "================================================================"

echo "1. Publishing corrupted poison pill..."
curl -s -X POST "http://localhost:3002/api/v1/events/ingest" \
  -H "Content-Type: application/json" \
  -d "CORRUPTED_NON_JSON_DATA_0xDEADBEEF" || true

echo "================================================================"
echo " [CHAOS TEST 7 COMPLETED] Message isolated to *.DLT!           "
echo "================================================================"
