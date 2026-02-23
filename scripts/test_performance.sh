#!/bin/bash
# DataCore Performance Regression Test Runner
# Runs performance benchmarks and compares against baseline
#
# Usage: ./scripts/test_performance.sh [options]
#
# Options:
#   --baseline FILE     Use custom baseline file
#   --save-baseline     Save current results as new baseline
#   --threshold PCT     Performance regression threshold (default: 10%)
#   --verbose           Show detailed output
#   --help              Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BASELINE_FILE="$PROJECT_ROOT/.perf_baseline.json"
RESULTS_FILE="/tmp/datacore_perf_results.json"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default options
SAVE_BASELINE=false
THRESHOLD=10
VERBOSE=false

# Parse options
while [[ $# -gt 0 ]]; do
	case $1 in
	--baseline)
		BASELINE_FILE="$2"
		shift 2
		;;
	--save-baseline)
		SAVE_BASELINE=true
		shift
		;;
	--threshold)
		THRESHOLD="$2"
		shift 2
		;;
	--verbose)
		VERBOSE=true
		shift
		;;
	--help | -h)
		echo "Usage: $0 [options]"
		echo ""
		echo "Options:"
		echo "  --baseline FILE     Use custom baseline file"
		echo "  --save-baseline     Save current results as new baseline"
		echo "  --threshold PCT     Performance regression threshold (default: 10%)"
		echo "  --verbose           Show detailed output"
		echo "  --help              Show this help message"
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

print_header "DataCore Performance Test Runner"

# Build with optimizations
log_info "Building with optimizations..."
cd "$PROJECT_ROOT"

if make build >>/tmp/perf_build.log 2>&1; then
	log_success "Build completed"
else
	log_error "Build failed. Check /tmp/perf_build.log"
	exit 1
fi

# Run performance tests
print_header "Running Performance Tests"

log_info "Running benchmark tests..."

cd "$PROJECT_ROOT/src"

# Run V performance tests
if v -enable-globals test infra/performance/benchmarks/ 2>&1 | tee /tmp/perf_test.log; then
	log_success "Performance tests completed"
else
	log_error "Performance tests failed"
	exit 1
fi

# Extract performance metrics
print_header "Performance Metrics"

# Parse test output for timing information
if [ -f "/tmp/perf_test.log" ]; then
	echo ""
	log_info "Test execution times:"
	grep -E "OK.*ms" /tmp/perf_test.log || echo "  No timing data found"

	# Calculate average
	TOTAL_TIME=$(grep -oE "[0-9]+\.[0-9]+ ms" /tmp/perf_test.log | awk '{sum+=$1} END {print sum}')
	TEST_COUNT=$(grep -c "OK.*ms" /tmp/perf_test.log || echo "0")

	if [ "$TEST_COUNT" -gt 0 ] && [ -n "$TOTAL_TIME" ]; then
		AVG_TIME=$(echo "scale=2; $TOTAL_TIME / $TEST_COUNT" | bc)
		echo ""
		log_info "Average test time: ${AVG_TIME}ms"
		log_info "Total tests: $TEST_COUNT"
		log_info "Total time: ${TOTAL_TIME}ms"
	fi
fi

# Compare with baseline if exists
if [ -f "$BASELINE_FILE" ] && [ "$SAVE_BASELINE" = false ]; then
	print_header "Comparing with Baseline"

	log_info "Baseline file: $BASELINE_FILE"

	# Simple comparison (in real implementation, parse JSON and compare metrics)
	log_warning "Baseline comparison not yet implemented"
	log_info "To save current results as baseline, use: --save-baseline"
fi

# Save baseline if requested
if [ "$SAVE_BASELINE" = true ]; then
	print_header "Saving Baseline"

	log_info "Saving performance results to: $BASELINE_FILE"

	# Create simple JSON baseline (in real implementation, extract actual metrics)
	cat >"$BASELINE_FILE" <<EOF
{
  "version": "0.37.0",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "metrics": {
    "total_time_ms": ${TOTAL_TIME:-0},
    "test_count": ${TEST_COUNT:-0},
    "avg_time_ms": ${AVG_TIME:-0}
  }
}
EOF

	log_success "Baseline saved"
fi

# Summary
print_header "Performance Test Summary"

echo ""
log_success "All performance tests passed!"
log_info "Performance metrics collected successfully"

if [ "$SAVE_BASELINE" = true ]; then
	log_info "Baseline saved to: $BASELINE_FILE"
fi

exit 0
