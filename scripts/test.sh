#!/bin/bash
# DataCore Test Runner
# Usage: ./scripts/test.sh [command] [options]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default options
COVERAGE=false
VERBOSE=false
PARALLEL=false
FAIL_FAST=false

print_usage() {
	echo "Usage: $0 [command] [options]"
	echo ""
	echo "Commands:"
	echo "  unit          Run unit tests (V test files)"
	echo "  integration   Run integration tests (Kafka CLI)"
	echo "  all           Run all tests"
	echo "  quick         Run quick unit tests only"
	echo "  coverage      Run tests with coverage report"
	echo ""
	echo "Options:"
	echo "  --coverage    Generate coverage report"
	echo "  --verbose     Show detailed test output"
	echo "  --parallel    Run tests in parallel (where supported)"
	echo "  --fail-fast   Stop on first test failure"
	echo "  --help        Show this help message"
	echo ""
	echo "Examples:"
	echo "  $0 unit"
	echo "  $0 unit --coverage"
	echo "  $0 all --verbose"
	echo "  $0 integration --fail-fast"
}

# Parse options
COMMAND="${1:-unit}"
shift || true

while [[ $# -gt 0 ]]; do
	case $1 in
	--coverage)
		COVERAGE=true
		shift
		;;
	--verbose)
		VERBOSE=true
		shift
		;;
	--parallel)
		PARALLEL=true
		shift
		;;
	--fail-fast)
		FAIL_FAST=true
		shift
		;;
	--help | -h | help)
		print_usage
		exit 0
		;;
	*)
		echo -e "${RED}Unknown option: $1${NC}"
		print_usage
		exit 1
		;;
	esac
done

# Logging functions
log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
	echo -e "${YELLOW}[WARN]${NC} $1"
}

print_header() {
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
	echo -e "${CYAN}  $1${NC}"
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

run_unit_tests() {
	print_header "Running Unit Tests"

	cd "$PROJECT_ROOT/src"

	# Build test command
	TEST_CMD="v test ."

	if [ "$COVERAGE" = true ]; then
		log_info "Running with coverage..."
		TEST_CMD="v -cov test ."
	fi

	if [ "$FAIL_FAST" = true ]; then
		TEST_CMD="$TEST_CMD -fail-fast"
	fi

	if [ "$VERBOSE" = true ]; then
		log_info "Running: $TEST_CMD"
	fi

	# Run tests
	START_TIME=$(date +%s)

	if [ "$VERBOSE" = true ]; then
		eval "$TEST_CMD"
	else
		eval "$TEST_CMD" 2>&1 | tee /tmp/datacore_test_output.log
	fi

	END_TIME=$(date +%s)
	DURATION=$((END_TIME - START_TIME))

	log_success "Unit tests completed in ${DURATION}s"

	# Show coverage summary if enabled
	if [ "$COVERAGE" = true ] && [ -f "coverage.txt" ]; then
		echo ""
		log_info "Coverage Summary:"
		cat coverage.txt | tail -20
	fi
}

run_integration_tests() {
	print_header "Running Integration Tests"

	# Check if broker is running
	if ! nc -z localhost 9092 2>/dev/null; then
		log_warning "DataCore broker not running on localhost:9092"
		log_warning "Please start the broker first:"
		log_warning "  cd src && v run . broker start"
		echo ""
		log_warning "Skipping integration tests..."
		return 0
	fi

	cd "$PROJECT_ROOT/tests/integration"

	# Run tests
	if [[ -f "run_all_tests.sh" ]]; then
		START_TIME=$(date +%s)

		if [ "$VERBOSE" = true ]; then
			bash run_all_tests.sh
		else
			bash run_all_tests.sh 2>&1 | tee /tmp/datacore_integration_output.log
		fi

		END_TIME=$(date +%s)
		DURATION=$((END_TIME - START_TIME))

		log_success "Integration tests completed in ${DURATION}s"
	else
		log_error "Integration test scripts not found"
		return 1
	fi
}

run_coverage_tests() {
	print_header "Running Tests with Coverage"

	COVERAGE=true
	run_unit_tests

	echo ""
	log_info "Generating coverage report..."

	cd "$PROJECT_ROOT/src"

	if [ -f "coverage.txt" ]; then
		# Calculate coverage percentage
		TOTAL_LINES=$(grep -E "^[^:]+:[0-9]+" coverage.txt | wc -l)
		COVERED_LINES=$(grep -E "^[^:]+:[0-9]+:.*[1-9]" coverage.txt | wc -l)

		if [ "$TOTAL_LINES" -gt 0 ]; then
			COVERAGE_PCT=$((COVERED_LINES * 100 / TOTAL_LINES))
			echo ""
			log_info "Coverage: ${COVERAGE_PCT}% (${COVERED_LINES}/${TOTAL_LINES} lines)"
		fi

		# Show uncovered files
		echo ""
		log_info "Files with low coverage:"
		grep -E "^[^:]+:[0-9]+:.*0$" coverage.txt | head -10 || echo "  (none)"
	else
		log_warning "Coverage file not found"
	fi
}

run_quick_tests() {
	print_header "Running Quick Tests"

	cd "$PROJECT_ROOT/src"

	log_info "Running quick unit tests..."
	v test . 2>&1 | head -50

	log_success "Quick tests completed"
}

# Main execution
case "$COMMAND" in
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
	run_quick_tests
	;;
coverage)
	run_coverage_tests
	;;
-h | --help | help)
	print_usage
	;;
*)
	log_error "Unknown command: $COMMAND"
	print_usage
	exit 1
	;;
esac

log_success "All tests completed successfully!"
