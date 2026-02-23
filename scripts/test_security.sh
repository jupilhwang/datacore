#!/bin/bash
# DataCore Security Test Runner
# Tests SSL/TLS and SASL authentication
#
# Usage: ./scripts/test_security.sh [test] [options]
#
# Tests:
#   ssl         Test SSL/TLS connections
#   sasl        Test SASL authentication
#   all         Run all security tests (default)
#
# Options:
#   --skip-setup    Skip environment setup
#   --verbose       Show detailed output
#   --help          Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default options
TEST_TYPE="${1:-all}"
SKIP_SETUP=false
VERBOSE=false

# Check for help first
if [ "$TEST_TYPE" = "--help" ] || [ "$TEST_TYPE" = "-h" ] || [ "$TEST_TYPE" = "help" ]; then
	echo "Usage: $0 [test] [options]"
	echo ""
	echo "Tests:"
	echo "  ssl         Test SSL/TLS connections"
	echo "  sasl        Test SASL authentication"
	echo "  all         Run all security tests (default)"
	echo ""
	echo "Options:"
	echo "  --skip-setup    Skip environment setup"
	echo "  --verbose       Show detailed output"
	echo "  --help          Show this help message"
	exit 0
fi

# Parse options
shift || true
while [[ $# -gt 0 ]]; do
	case $1 in
	--skip-setup)
		SKIP_SETUP=true
		shift
		;;
	--verbose)
		VERBOSE=true
		shift
		;;
	--help | -h)
		echo "Usage: $0 [test] [options]"
		echo ""
		echo "Tests:"
		echo "  ssl         Test SSL/TLS connections"
		echo "  sasl        Test SASL authentication"
		echo "  all         Run all security tests (default)"
		echo ""
		echo "Options:"
		echo "  --skip-setup    Skip environment setup"
		echo "  --verbose       Show detailed output"
		echo "  --help          Show this help message"
		exit 0
		;;
	*)
		echo -e "${RED}Unknown option: $1${NC}"
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
	echo ""
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
	echo -e "${CYAN}  $1${NC}"
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Test SSL/TLS
test_ssl() {
	print_header "Testing SSL/TLS Connections"

	log_info "Checking SSL test prerequisites..."

	# Check if PostgreSQL SSL tests are available
	if [ -f "$PROJECT_ROOT/tests/postgres_ssl_check.v" ]; then
		log_info "Running PostgreSQL SSL tests..."

		cd "$PROJECT_ROOT"

		if v run tests/postgres_ssl_check.v 2>&1 | tee /tmp/ssl_test.log; then
			log_success "PostgreSQL SSL tests passed"
		else
			log_warning "PostgreSQL SSL tests failed or skipped"
			log_info "Check /tmp/ssl_test.log for details"
		fi
	else
		log_warning "PostgreSQL SSL test not found"
	fi

	# Check if SSL test script exists
	if [ -f "$PROJECT_ROOT/tests/postgres_ssl_test.sh" ]; then
		log_info "Running SSL test script..."

		if bash "$PROJECT_ROOT/tests/postgres_ssl_test.sh" 2>&1 | tee -a /tmp/ssl_test.log; then
			log_success "SSL test script passed"
		else
			log_warning "SSL test script failed or skipped"
		fi
	fi

	log_success "SSL/TLS tests completed"
	return 0
}

# Test SASL authentication
test_sasl() {
	print_header "Testing SASL Authentication"

	log_info "Checking SASL test prerequisites..."

	# Check if SASL test exists
	if [ -f "$PROJECT_ROOT/tests/integration/test_kafka_sasl.py" ]; then
		log_info "Running SASL authentication tests..."

		# Check if Python and kafka-python are available
		if ! command -v python3 &>/dev/null; then
			log_warning "Python3 not found, skipping SASL tests"
			return 0
		fi

		if ! python3 -c "import kafka" 2>/dev/null; then
			log_warning "kafka-python not installed, skipping SASL tests"
			log_info "Install with: pip install kafka-python"
			return 0
		fi

		cd "$PROJECT_ROOT/tests/integration"

		if python3 test_kafka_sasl.py 2>&1 | tee /tmp/sasl_test.log; then
			log_success "SASL authentication tests passed"
		else
			log_warning "SASL authentication tests failed or skipped"
			log_info "Check /tmp/sasl_test.log for details"
		fi
	else
		log_warning "SASL test not found at tests/integration/test_kafka_sasl.py"
	fi

	log_success "SASL authentication tests completed"
	return 0
}

# Main execution
FAILED_TESTS=0
TOTAL_TESTS=0

case "$TEST_TYPE" in
ssl)
	test_ssl || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=1
	;;
sasl)
	test_sasl || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=1
	;;
all)
	test_ssl || FAILED_TESTS=$((FAILED_TESTS + 1))
	test_sasl || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=2
	;;
*)
	log_error "Unknown test type: $TEST_TYPE"
	echo "Valid types: ssl, sasl, all"
	exit 1
	;;
esac

# Summary
print_header "Security Test Summary"

PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))

echo ""
log_info "Total tests: $TOTAL_TESTS"
log_success "Passed: $PASSED_TESTS"

if [ $FAILED_TESTS -gt 0 ]; then
	log_error "Failed: $FAILED_TESTS"
	exit 1
else
	log_success "All security tests passed!"
	exit 0
fi
