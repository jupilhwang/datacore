#!/bin/bash
# DataCore Storage Engine Test Runner
# Tests all storage engines: Memory, PostgreSQL, S3
#
# Usage: ./scripts/test_storage.sh [engine] [options]
#
# Engines:
#   memory      Test memory storage engine
#   postgres    Test PostgreSQL storage engine
#   s3          Test S3 storage engine
#   all         Test all storage engines (default)
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
ENGINE="${1:-all}"
SKIP_SETUP=false
VERBOSE=false

# Check for help first
if [ "$ENGINE" = "--help" ] || [ "$ENGINE" = "-h" ] || [ "$ENGINE" = "help" ]; then
	echo "Usage: $0 [engine] [options]"
	echo ""
	echo "Engines:"
	echo "  memory      Test memory storage engine"
	echo "  postgres    Test PostgreSQL storage engine"
	echo "  s3          Test S3 storage engine"
	echo "  all         Test all storage engines (default)"
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
		echo "Usage: $0 [engine] [options]"
		echo ""
		echo "Engines:"
		echo "  memory      Test memory storage engine"
		echo "  postgres    Test PostgreSQL storage engine"
		echo "  s3          Test S3 storage engine"
		echo "  all         Test all storage engines (default)"
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

# Test memory storage
test_memory_storage() {
	print_header "Testing Memory Storage Engine"

	log_info "Running memory storage tests..."

	cd "$PROJECT_ROOT/src"

	if v -enable-globals test infra/storage/plugins/memory/ 2>&1 | tee /tmp/memory_test.log; then
		log_success "Memory storage tests passed"
		return 0
	else
		log_error "Memory storage tests failed"
		return 1
	fi
}

# Test PostgreSQL storage
test_postgres_storage() {
	print_header "Testing PostgreSQL Storage Engine"

	# Check if PostgreSQL is available
	if [ "$SKIP_SETUP" = false ]; then
		log_info "Checking PostgreSQL availability..."

		if ! command -v psql &>/dev/null; then
			log_warning "PostgreSQL client not found, skipping PostgreSQL tests"
			return 0
		fi

		# Check if test database is accessible
		if ! psql -h localhost -U postgres -d postgres -c "SELECT 1" &>/dev/null; then
			log_warning "Cannot connect to PostgreSQL, skipping tests"
			log_warning "To run PostgreSQL tests, ensure PostgreSQL is running:"
			log_warning "  docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres"
			return 0
		fi
	fi

	log_info "Running PostgreSQL storage tests..."

	cd "$PROJECT_ROOT/src"

	if v -enable-globals test infra/storage/plugins/postgres/ 2>&1 | tee /tmp/postgres_test.log; then
		log_success "PostgreSQL storage tests passed"
		return 0
	else
		log_error "PostgreSQL storage tests failed"
		return 1
	fi
}

# Test S3 storage
test_s3_storage() {
	print_header "Testing S3 Storage Engine"

	# Check if S3 credentials are available
	if [ "$SKIP_SETUP" = false ]; then
		log_info "Checking S3 configuration..."

		if [ -z "$AWS_ACCESS_KEY_ID" ] && [ -z "$DATACORE_S3_ACCESS_KEY" ]; then
			log_warning "S3 credentials not found, skipping S3 tests"
			log_warning "To run S3 tests, set environment variables:"
			log_warning "  export AWS_ACCESS_KEY_ID=your_key"
			log_warning "  export AWS_SECRET_ACCESS_KEY=your_secret"
			return 0
		fi
	fi

	log_info "Running S3 storage tests..."

	cd "$PROJECT_ROOT/src"

	if v -enable-globals test infra/storage/plugins/s3/ 2>&1 | tee /tmp/s3_test.log; then
		log_success "S3 storage tests passed"
		return 0
	else
		log_error "S3 storage tests failed"
		return 1
	fi
}

# Main execution
FAILED_TESTS=0
TOTAL_TESTS=0

case "$ENGINE" in
memory)
	test_memory_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=1
	;;
postgres)
	test_postgres_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=1
	;;
s3)
	test_s3_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=1
	;;
all)
	test_memory_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	test_postgres_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	test_s3_storage || FAILED_TESTS=$((FAILED_TESTS + 1))
	TOTAL_TESTS=3
	;;
*)
	log_error "Unknown engine: $ENGINE"
	echo "Valid engines: memory, postgres, s3, all"
	exit 1
	;;
esac

# Summary
print_header "Storage Test Summary"

PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))

echo ""
log_info "Total tests: $TOTAL_TESTS"
log_success "Passed: $PASSED_TESTS"

if [ $FAILED_TESTS -gt 0 ]; then
	log_error "Failed: $FAILED_TESTS"
	exit 1
else
	log_success "All storage tests passed!"
	exit 0
fi
