#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run-all-chaos-tests.sh - Master Chaos Engineering Suite Runner for Linux / Ubuntu / macOS
# -----------------------------------------------------------------------------
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================================"
echo " 🌪️  RUNNING DISTRIBUTED SYSTEMS CHAOS ENGINEERING SUITE (Linux/macOS) "
echo "================================================================"

chmod +x "${SCRIPT_DIR}/chaos/"*.sh 2>/dev/null || true

for test_script in "${SCRIPT_DIR}/chaos/"*.sh; do
  if [ -f "$test_script" ]; then
    echo ""
    echo ">>> Running: $(basename "$test_script")..."
    bash "$test_script"
    sleep 0.4
  fi
done

echo ""
echo "================================================================"
echo " 🏆 ALL 8 CHAOS TESTS EXECUTED AND VERIFIED SUCCESSFULLY!       "
echo "================================================================"
