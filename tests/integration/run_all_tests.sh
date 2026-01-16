#!/bin/bash
# Run all integration tests for DataCore
# This script runs both Kafka CLI tests and Schema Registry tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           DataCore Integration Test Suite                 ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Bootstrap Server:  ${YELLOW}$BOOTSTRAP_SERVER${NC}"
echo -e "Schema Registry:   ${YELLOW}$SCHEMA_REGISTRY_URL${NC}"
echo ""

# Check if broker is running
echo -e "${YELLOW}[CHECK]${NC} Checking DataCore broker connection..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo -e "${RED}[ERROR]${NC} Cannot connect to broker at $BOOTSTRAP_SERVER"
    echo ""
    echo "Please start DataCore broker first:"
    echo "  cd src && v run . broker start"
    echo ""
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Broker is reachable"

# Track overall results
TOTAL_SUITES=0
PASSED_SUITES=0
FAILED_SUITES=0

run_test_suite() {
    local name="$1"
    local script="$2"
    
    ((TOTAL_SUITES++))
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Running: $name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    if [[ -x "$script" ]]; then
        if "$script"; then
            ((PASSED_SUITES++))
            echo -e "${GREEN}[SUITE PASSED]${NC} $name"
        else
            ((FAILED_SUITES++))
            echo -e "${RED}[SUITE FAILED]${NC} $name"
        fi
    else
        echo -e "${YELLOW}[SKIP]${NC} $name - Script not executable or not found"
    fi
}

# Make scripts executable
chmod +x "$SCRIPT_DIR/test_kafka_cli.sh" 2>/dev/null || true
chmod +x "$SCRIPT_DIR/test_schema_registry.sh" 2>/dev/null || true

# Run test suites
run_test_suite "Kafka CLI Integration Tests" "$SCRIPT_DIR/test_kafka_cli.sh"
run_test_suite "Schema Registry REST API Tests" "$SCRIPT_DIR/test_schema_registry.sh"

# Final Summary
echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                 Final Test Summary                        ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Total Test Suites: $TOTAL_SUITES"
echo -e "Passed:            ${GREEN}$PASSED_SUITES${NC}"
echo -e "Failed:            ${RED}$FAILED_SUITES${NC}"
echo ""

if [[ $FAILED_SUITES -gt 0 ]]; then
    echo -e "${RED}Some test suites failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All test suites passed!${NC}"
    exit 0
fi
