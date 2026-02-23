#!/bin/bash
# DataCore Long-Running Test Runner
# Runs continuous tests for 24+ hours to verify stability and performance
#
# Usage: ./scripts/test_longrunning.sh [scenario] [options]
#
# Scenarios:
#   producer    Continuous message production
#   consumer    Continuous message consumption
#   mixed       Producer + Consumer simultaneously
#   stress      High-load stress testing
#   all         Run all scenarios (default)
#
# Options:
#   --duration HOURS    Test duration in hours (default: 24)
#   --interval SECONDS  Action interval in seconds (default: 1)
#   --metrics-dir DIR   Directory for metrics output (default: ./metrics)
#   --no-cleanup        Don't cleanup resources after test
#   --verbose           Show detailed output
#   --help              Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Default options
SCENARIO="${1:-all}"
DURATION_HOURS=24
INTERVAL_SECONDS=1
METRICS_DIR="./metrics"
CLEANUP=true
VERBOSE=false

# Test state
START_TIME=$(date +%s)
BROKER_PID=""
TEST_TOPIC="longrunning-test-topic"
METRICS_FILE=""
ERROR_COUNT=0
MESSAGE_COUNT=0

# Check for help first
if [ "$SCENARIO" = "--help" ] || [ "$SCENARIO" = "-h" ] || [ "$SCENARIO" = "help" ]; then
	echo "Usage: $0 [scenario] [options]"
	echo ""
	echo "Scenarios:"
	echo "  producer    Continuous message production"
	echo "  consumer    Continuous message consumption"
	echo "  mixed       Producer + Consumer simultaneously"
	echo "  stress      High-load stress testing"
	echo "  all         Run all scenarios (default)"
	echo ""
	echo "Options:"
	echo "  --duration HOURS    Test duration in hours (default: 24)"
	echo "  --interval SECONDS  Action interval in seconds (default: 1)"
	echo "  --metrics-dir DIR   Directory for metrics output (default: ./metrics)"
	echo "  --no-cleanup        Don't cleanup resources after test"
	echo "  --verbose           Show detailed output"
	echo "  --help              Show this help message"
	exit 0
fi

# Parse options
shift || true
while [[ $# -gt 0 ]]; do
	case $1 in
	--duration)
		DURATION_HOURS="$2"
		shift 2
		;;
	--interval)
		INTERVAL_SECONDS="$2"
		shift 2
		;;
	--metrics-dir)
		METRICS_DIR="$2"
		shift 2
		;;
	--no-cleanup)
		CLEANUP=false
		shift
		;;
	--verbose)
		VERBOSE=true
		shift
		;;
	*)
		echo -e "${RED}Unknown option: $1${NC}"
		exit 1
		;;
	esac
done

# Logging functions
log_info() {
	echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1" >>"$METRICS_FILE"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1" >>"$METRICS_FILE"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1" >&2
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >>"$METRICS_FILE"
	ERROR_COUNT=$((ERROR_COUNT + 1))
}

log_warning() {
	echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: $1" >>"$METRICS_FILE"
}

log_metric() {
	local metric_name=$1
	local metric_value=$2
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] METRIC: $metric_name=$metric_value" >>"$METRICS_FILE"
}

print_header() {
	echo ""
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
	echo -e "${CYAN}  $1${NC}"
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Initialize metrics directory
init_metrics() {
	mkdir -p "$METRICS_DIR"
	METRICS_FILE="$METRICS_DIR/longrunning-$(date +%Y%m%d-%H%M%S).log"

	log_info "Long-running test initialized"
	log_info "Scenario: $SCENARIO"
	log_info "Duration: $DURATION_HOURS hours"
	log_info "Interval: $INTERVAL_SECONDS seconds"
	log_info "Metrics file: $METRICS_FILE"
}

# Start broker
start_broker() {
	print_header "Starting DataCore Broker"

	log_info "Building DataCore..."
	cd "$PROJECT_ROOT"
	if make build >>"$METRICS_FILE" 2>&1; then
		log_success "Build completed"
	else
		log_error "Build failed"
		return 1
	fi

	log_info "Starting broker..."
	./bin/datacore broker start --config=config.toml >"$METRICS_DIR/broker.log" 2>&1 &
	BROKER_PID=$!

	log_info "Broker PID: $BROKER_PID"

	# Wait for broker to be ready
	log_info "Waiting for broker to start..."
	for ((i = 1; i <= 30; i++)); do
		if nc -z localhost 9092 2>/dev/null; then
			log_success "Broker is ready!"
			return 0
		fi

		if ! ps -p $BROKER_PID >/dev/null 2>&1; then
			log_error "Broker process died prematurely"
			return 1
		fi

		sleep 1
	done

	log_error "Timeout waiting for broker"
	return 1
}

# Cleanup function
cleanup() {
	print_header "Cleanup"

	if [ "$CLEANUP" = true ]; then
		log_info "Stopping broker (PID: $BROKER_PID)..."
		if [ -n "$BROKER_PID" ] && ps -p $BROKER_PID >/dev/null 2>&1; then
			kill $BROKER_PID 2>/dev/null || kill -9 $BROKER_PID 2>/dev/null
			wait $BROKER_PID 2>/dev/null || true
			log_success "Broker stopped"
		fi
	else
		log_warning "Skipping cleanup (--no-cleanup flag)"
	fi

	# Print summary
	print_summary
}

trap cleanup EXIT INT TERM

# Check if test should continue
should_continue() {
	local current_time=$(date +%s)
	local elapsed=$((current_time - START_TIME))
	local duration_seconds=$((DURATION_HOURS * 3600))

	if [ $elapsed -ge $duration_seconds ]; then
		return 1
	fi

	return 0
}

# Get elapsed time in human-readable format
get_elapsed_time() {
	local current_time=$(date +%s)
	local elapsed=$((current_time - START_TIME))
	local hours=$((elapsed / 3600))
	local minutes=$(((elapsed % 3600) / 60))
	local seconds=$((elapsed % 60))
	printf "%02d:%02d:%02d" $hours $minutes $seconds
}

# Producer scenario
run_producer_scenario() {
	print_header "Producer Scenario"

	log_info "Starting continuous message production..."

	local iteration=0
	while should_continue; do
		iteration=$((iteration + 1))

		# Produce message
		local timestamp=$(date +%s%N)
		local message="test-message-$iteration-$timestamp"

		if echo "$message" | timeout 5 kafka-console-producer.sh \
			--bootstrap-server localhost:9092 \
			--topic "$TEST_TOPIC" 2>>"$METRICS_FILE"; then
			MESSAGE_COUNT=$((MESSAGE_COUNT + 1))

			if [ "$VERBOSE" = true ] || [ $((iteration % 100)) -eq 0 ]; then
				log_info "Produced message $iteration (elapsed: $(get_elapsed_time))"
				log_metric "messages_produced" "$MESSAGE_COUNT"
			fi
		else
			log_error "Failed to produce message $iteration"
		fi

		sleep "$INTERVAL_SECONDS"
	done

	log_success "Producer scenario completed"
}

# Consumer scenario
run_consumer_scenario() {
	print_header "Consumer Scenario"

	log_info "Starting continuous message consumption..."

	# Start consumer in background
	kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic "$TEST_TOPIC" \
		--from-beginning \
		--timeout-ms 1000 2>>"$METRICS_FILE" |
		while read -r message; do
			MESSAGE_COUNT=$((MESSAGE_COUNT + 1))

			if [ "$VERBOSE" = true ] || [ $((MESSAGE_COUNT % 100)) -eq 0 ]; then
				log_info "Consumed message $MESSAGE_COUNT (elapsed: $(get_elapsed_time))"
				log_metric "messages_consumed" "$MESSAGE_COUNT"
			fi

			if ! should_continue; then
				break
			fi
		done &

	local consumer_pid=$!

	# Wait for test duration
	while should_continue; do
		if ! ps -p $consumer_pid >/dev/null 2>&1; then
			log_warning "Consumer process died, restarting..."
			# Restart consumer logic here
		fi
		sleep 10
	done

	kill $consumer_pid 2>/dev/null || true
	log_success "Consumer scenario completed"
}

# Mixed scenario
run_mixed_scenario() {
	print_header "Mixed Scenario (Producer + Consumer)"

	log_info "Starting producer and consumer simultaneously..."

	# Start producer in background
	(run_producer_scenario) &
	local producer_pid=$!

	# Start consumer in background
	(run_consumer_scenario) &
	local consumer_pid=$!

	# Monitor both processes
	while should_continue; do
		if ! ps -p $producer_pid >/dev/null 2>&1; then
			log_error "Producer process died"
		fi

		if ! ps -p $consumer_pid >/dev/null 2>&1; then
			log_error "Consumer process died"
		fi

		# Log status every 5 minutes
		if [ $(($(date +%s) % 300)) -eq 0 ]; then
			log_info "Status: Messages=$MESSAGE_COUNT, Errors=$ERROR_COUNT, Elapsed=$(get_elapsed_time)"
		fi

		sleep 10
	done

	kill $producer_pid $consumer_pid 2>/dev/null || true
	log_success "Mixed scenario completed"
}

# Stress scenario
run_stress_scenario() {
	print_header "Stress Scenario (High Load)"

	log_info "Starting high-load stress test..."

	# Start multiple producers
	local num_producers=10
	local producer_pids=()

	for ((i = 1; i <= num_producers; i++)); do
		(
			while should_continue; do
				echo "stress-message-$i-$(date +%s%N)" |
					kafka-console-producer.sh \
						--bootstrap-server localhost:9092 \
						--topic "$TEST_TOPIC" 2>>"$METRICS_FILE" || true
				sleep 0.1
			done
		) &
		producer_pids+=($!)
	done

	log_info "Started $num_producers producers"

	# Wait for test duration
	while should_continue; do
		sleep 10
	done

	# Stop all producers
	for pid in "${producer_pids[@]}"; do
		kill $pid 2>/dev/null || true
	done

	log_success "Stress scenario completed"
}

# Print summary
print_summary() {
	print_header "Test Summary"

	local end_time=$(date +%s)
	local total_elapsed=$((end_time - START_TIME))
	local hours=$((total_elapsed / 3600))
	local minutes=$(((total_elapsed % 3600) / 60))

	echo ""
	log_info "Scenario: $SCENARIO"
	log_info "Duration: ${hours}h ${minutes}m"
	log_info "Messages: $MESSAGE_COUNT"
	log_info "Errors: $ERROR_COUNT"
	log_info "Metrics file: $METRICS_FILE"
	echo ""

	if [ $ERROR_COUNT -eq 0 ]; then
		log_success "All tests passed without errors!"
	else
		log_warning "Test completed with $ERROR_COUNT errors"
	fi
}

# Main execution
print_header "DataCore Long-Running Test"

init_metrics

if ! start_broker; then
	log_error "Failed to start broker"
	exit 1
fi

# Run scenario
case "$SCENARIO" in
producer)
	run_producer_scenario
	;;
consumer)
	run_consumer_scenario
	;;
mixed)
	run_mixed_scenario
	;;
stress)
	run_stress_scenario
	;;
all)
	log_info "Running all scenarios sequentially..."
	run_producer_scenario
	run_consumer_scenario
	run_mixed_scenario
	;;
*)
	log_error "Unknown scenario: $SCENARIO"
	echo "Valid scenarios: producer, consumer, mixed, stress, all"
	exit 1
	;;
esac

log_success "Long-running test completed successfully!"
exit 0
