#!/bin/bash
# DataCore Test Runner
# Usage: ./scripts/test.sh [unit|integration|all]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  unit          Run unit tests (V test files)"
    echo "  integration   Run integration tests (Kafka CLI)"
    echo "  all           Run all tests"
    echo "  quick         Run quick unit tests only"
    echo ""
    echo "Examples:"
    echo "  $0 unit"
    echo "  $0 integration"
    echo "  $0 all"
}

run_unit_tests() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Running Unit Tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    cd "$PROJECT_ROOT/src"
    v test .
    
    echo -e "${GREEN}✅ Unit tests completed${NC}"
}

run_integration_tests() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Running Integration Tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Check if broker is running
    if ! nc -z localhost 9092 2>/dev/null; then
        echo -e "${YELLOW}[WARN]${NC} DataCore broker not running on localhost:9092"
        echo -e "${YELLOW}       Please start the broker first:${NC}"
        echo -e "${YELLOW}         cd src && v run . broker start${NC}"
        echo ""
        echo -e "${YELLOW}Skipping integration tests...${NC}"
        return 0
    fi
    
    cd "$PROJECT_ROOT/tests/integration"
    
    # Run tests
    if [[ -f "run_all_tests.sh" ]]; then
        bash run_all_tests.sh
    else
        echo -e "${YELLOW}Integration test scripts not found${NC}"
    fi
}

case "${1:-unit}" in
    unit)
        run_unit_tests
        ;;
    integration)
        run_integration_tests
        ;;
    all)
        run_unit_tests
        echo ""
        run_integration_tests
        ;;
    quick)
        echo -e "${BLUE}Running quick tests...${NC}"
        cd "$PROJECT_ROOT/src"
        v test . 2>&1 | head -20
        ;;
    -h|--help|help)
        print_usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        print_usage
        exit 1
        ;;
esac
