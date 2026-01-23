#!/bin/bash
set -e

# DataCore Integration Test Runner
# Builds the project, starts the broker, runs compatibility tests, and cleans up.
#
# Usage: ./scripts/run_compat_test.sh [OPTIONS]
#
# Options:
#   --skip-build        Skip building the project
#   --timeout SECONDS   Broker startup timeout (default: 30)
#   --verbose           Enable verbose logging
#   --log-file FILE     Custom log file path (default: broker_test.log)
#   --help              Show this help message

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
CONFIG_FILE="$ROOT_DIR/config.toml"
LOG_FILE="$ROOT_DIR/broker_test.log"

# Default options
SKIP_BUILD=false
BROKER_TIMEOUT=30
VERBOSE=false

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse command line arguments
while [[ $# -gt 0 ]]; do
	case $1 in
	--skip-build)
		SKIP_BUILD=true
		shift
		;;
	--timeout)
		BROKER_TIMEOUT="$2"
		shift 2
		;;
	--verbose)
		VERBOSE=true
		shift
		;;
	--log-file)
		LOG_FILE="$2"
		shift 2
		;;
	--help)
		echo "Usage: $0 [OPTIONS]"
		echo ""
		echo "Options:"
		echo "  --skip-build        Skip building the project"
		echo "  --timeout SECONDS   Broker startup timeout (default: 30)"
		echo "  --verbose           Enable verbose logging"
		echo "  --log-file FILE     Custom log file path (default: broker_test.log)"
		echo "  --help              Show this help message"
		exit 0
		;;
	*)
		echo -e "${RED}Unknown option: $1${NC}"
		echo "Use --help for usage information"
		exit 1
		;;
	esac
done

# Logging function
log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
	if [ "$VERBOSE" = true ]; then
		echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" >>"$LOG_FILE"
	fi
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1" >&2
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >>"$LOG_FILE"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $1"
	if [ "$VERBOSE" = true ]; then
		echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1" >>"$LOG_FILE"
	fi
}

log_warning() {
	echo -e "${YELLOW}[WARN]${NC} $1"
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $1" >>"$LOG_FILE"
}

# Build project
if [ "$SKIP_BUILD" = false ]; then
	log_info "Building DataCore..."
	if cd "$ROOT_DIR" && make build >>"$LOG_FILE" 2>&1; then
		log_success "Build completed"
	else
		log_error "Build failed. Check $LOG_FILE for details"
		exit 1
	fi
else
	log_warning "Skipping build (--skip-build flag)"
	if [ ! -f "$BIN_DIR/datacore" ]; then
		log_error "Binary not found at $BIN_DIR/datacore"
		exit 1
	fi
fi

log_info "Starting DataCore Broker..."

# Start broker in background
"$BIN_DIR/datacore" broker start --config "$CONFIG_FILE" >"$LOG_FILE" 2>&1 &
BROKER_PID=$!

log_info "Broker PID: $BROKER_PID"

# Cleanup function
cleanup() {
	log_info "Stopping broker (PID: $BROKER_PID)..."
	if ps -p $BROKER_PID >/dev/null 2>&1; then
		kill $BROKER_PID 2>/dev/null || kill -9 $BROKER_PID 2>/dev/null
		wait $BROKER_PID 2>/dev/null || true
		log_success "Broker stopped"
	else
		log_warning "Broker process not found (may have already exited)"
	fi

	# Show last 20 lines of log if test failed
	if [ "${TEST_EXIT_CODE:-1}" -ne 0 ]; then
		log_error "Test failed. Last 20 lines of broker log:"
		tail -20 "$LOG_FILE"
	fi
}

# Trap exit to ensure cleanup
trap cleanup EXIT INT TERM

# Wait for broker to be ready
log_info "Waiting for broker to start (timeout: ${BROKER_TIMEOUT}s)..."
for ((i = 1; i <= BROKER_TIMEOUT; i++)); do
	if nc -z localhost 9092 2>/dev/null; then
		log_success "Broker is ready! (took ${i}s)"
		break
	fi

	# Check if broker process is still running
	if ! ps -p $BROKER_PID >/dev/null 2>&1; then
		log_error "Broker process died prematurely"
		log_error "Last 30 lines of broker log:"
		tail -30 "$LOG_FILE"
		exit 1
	fi

	if [ "$VERBOSE" = true ] && [ $((i % 5)) -eq 0 ]; then
		log_info "Still waiting... (${i}/${BROKER_TIMEOUT}s)"
	fi

	sleep 1

	if [ $i -eq $BROKER_TIMEOUT ]; then
		log_error "Timeout waiting for broker to start (${BROKER_TIMEOUT}s)"
		log_error "Last 30 lines of broker log:"
		tail -30 "$LOG_FILE"
		exit 1
	fi
done

log_info "Running Compatibility Tests..."
# Use BOOTSTRAP_SERVER env if not set
export BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-127.0.0.1:9092}"

# Run tests with timeout
if [ "$VERBOSE" = true ]; then
	bash "$ROOT_DIR/tests/integration/test_kafka_compat.sh"
	TEST_EXIT_CODE=$?
else
	bash "$ROOT_DIR/tests/integration/test_kafka_compat.sh" 2>&1 | tee -a "$LOG_FILE"
	TEST_EXIT_CODE=${PIPESTATUS[0]}
fi

if [ $TEST_EXIT_CODE -eq 0 ]; then
	log_success "All compatibility tests passed!"
else
	log_error "Some compatibility tests failed (exit code: $TEST_EXIT_CODE)"
fi

exit $TEST_EXIT_CODE
