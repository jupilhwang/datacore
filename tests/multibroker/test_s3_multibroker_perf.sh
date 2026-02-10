#!/bin/bash
# Multi-Broker S3 Performance Benchmark
# Tests throughput, latency, and concurrency across 3 broker cluster with S3 storage
#
# Usage: ./tests/multibroker/test_s3_multibroker_perf.sh [options]
#
# Options:
#   --bench <name>    Run a specific benchmark (throughput|large|latency|concurrent|crossbroker)
#   --skip-cleanup    Skip topic cleanup at end
#   --help            Show this help message
#
# Prerequisites:
#   - 3 DataCore brokers running (ports 9092, 9093, 9094)
#   - kafka-console-producer, kafka-console-consumer in PATH
#   - kcat (optional, for latency benchmark)
#   - gdate (optional, for nanosecond precision on macOS)
#   - bc (for floating-point calculations)

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094}"
BROKER1="127.0.0.1:9092"
BROKER2="127.0.0.1:9093"
BROKER3="127.0.0.1:9094"
TEST_PREFIX="mb-s3-perf"
CONSUMER_TIMEOUT_MS=60000
SKIP_CLEANUP=false
RUN_BENCH=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Results storage (associative arrays not portable; use indexed arrays)
RESULT_LABELS=()
RESULT_VALUES=()

# =============================================================================
# macOS Compatibility
# =============================================================================

# timeout / gtimeout
if ! command -v timeout &>/dev/null; then
	if command -v gtimeout &>/dev/null; then
		timeout() { gtimeout "$@"; }
	else
		timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }
	fi
fi

# gdate for nanosecond precision
if [[ "$(uname)" == "Darwin" ]]; then
	DATE_CMD="gdate"
	if ! command -v gdate &>/dev/null; then
		echo -e "${YELLOW}[WARN]${NC} gdate not found, falling back to second-level precision"
		DATE_CMD="date"
	fi
else
	DATE_CMD="date"
fi

# =============================================================================
# Timestamp Helpers
# =============================================================================

get_timestamp_ns() {
	if [[ "$DATE_CMD" == "gdate" ]]; then
		gdate +%s%N
	else
		echo $(($(date +%s) * 1000000000))
	fi
}

get_timestamp_ms() {
	if [[ "$DATE_CMD" == "gdate" ]]; then
		echo $(($(gdate +%s%N) / 1000000))
	else
		echo $(($(date +%s) * 1000))
	fi
}

# =============================================================================
# Logging
# =============================================================================

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# =============================================================================
# Option Parsing
# =============================================================================

while [[ $# -gt 0 ]]; do
	case $1 in
	--bench)
		RUN_BENCH="$2"
		shift 2
		;;
	--skip-cleanup)
		SKIP_CLEANUP=true
		shift
		;;
	--help | -h)
		head -n 17 "$0" | tail -n +2 | sed 's/^# \?//'
		exit 0
		;;
	*)
		log_error "Unknown option: $1"
		exit 1
		;;
	esac
done

# =============================================================================
# Tool Detection
# =============================================================================

detect_tools() {
	if command -v kafka-topics &>/dev/null; then
		KAFKA_TOPICS="kafka-topics"
		KAFKA_PRODUCER="kafka-console-producer"
		KAFKA_CONSUMER="kafka-console-consumer"
	elif command -v kafka-topics.sh &>/dev/null; then
		KAFKA_TOPICS="kafka-topics.sh"
		KAFKA_PRODUCER="kafka-console-producer.sh"
		KAFKA_CONSUMER="kafka-console-consumer.sh"
	else
		log_error "Kafka CLI tools not found in PATH"
		exit 1
	fi

	HAS_KCAT=false
	if command -v kcat &>/dev/null; then
		HAS_KCAT=true
		KCAT_CMD="kcat"
	elif command -v /opt/homebrew/bin/kcat &>/dev/null; then
		HAS_KCAT=true
		KCAT_CMD="/opt/homebrew/bin/kcat"
	elif command -v kafkacat &>/dev/null; then
		HAS_KCAT=true
		KCAT_CMD="kafkacat"
	fi

	if ! command -v bc &>/dev/null; then
		log_error "bc command not found (required for calculations)"
		exit 1
	fi
}

# =============================================================================
# Broker Connectivity Check
# =============================================================================

check_brokers() {
	log_info "Checking broker connectivity..."
	local all_ok=true

	for broker in "$BROKER1" "$BROKER2" "$BROKER3"; do
		local host="${broker%%:*}"
		local port="${broker##*:}"
		if nc -z -w2 "$host" "$port" 2>/dev/null; then
			log_info "  $broker ... reachable"
		else
			log_error "  $broker ... unreachable"
			all_ok=false
		fi
	done

	if [[ "$all_ok" != "true" ]]; then
		log_error "Not all brokers are reachable. Aborting."
		exit 1
	fi
	log_success "All 3 brokers reachable"
}

# =============================================================================
# Topic Helpers
# =============================================================================

create_topic() {
	local topic="$1"
	local partitions="${2:-3}"
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--create --topic "$topic" \
		--partitions "$partitions" \
		--replication-factor 1 2>/dev/null || true
	sleep 0.5
}

# =============================================================================
# Result Storage
# =============================================================================

store_result() {
	local label="$1"
	local value="$2"
	RESULT_LABELS+=("$label")
	RESULT_VALUES+=("$value")
}

# Safe division: returns 0 if divisor is 0
safe_div_int() {
	local dividend="$1"
	local divisor="$2"
	if [[ "$divisor" -le 0 ]]; then
		echo 0
	else
		echo $((dividend * 1000 / divisor))
	fi
}

# Floating-point MB/sec calculation
calc_mbps() {
	local msg_count="$1"
	local msg_size="$2"
	local elapsed_ms="$3"
	if [[ "$elapsed_ms" -le 0 ]]; then
		echo "0.00"
	else
		echo "scale=2; $msg_count * $msg_size / 1048576 * 1000 / $elapsed_ms" | bc
	fi
}

# =============================================================================
# Benchmark 1: Throughput (Standard Messages)
# =============================================================================

bench_throughput() {
	local TOPIC="${TEST_PREFIX}-throughput"
	local MSG_COUNT=10000
	local MSG_SIZE=100

	echo ""
	echo -e "${CYAN}--- Benchmark 1: Throughput (${MSG_COUNT} x ${MSG_SIZE}B) ---${NC}"

	create_topic "$TOPIC" 3

	# Generate a fixed-size message payload
	local msg
	msg=$(head -c "$MSG_SIZE" /dev/zero | tr '\0' 'A')

	# -- Produce --
	log_info "Producing $MSG_COUNT messages (${MSG_SIZE}B each)..."
	local START
	START=$(get_timestamp_ms)

	seq 1 "$MSG_COUNT" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$TOPIC" 2>/dev/null

	local END
	END=$(get_timestamp_ms)
	local PRODUCE_MS=$((END - START))
	local PRODUCE_MPS
	PRODUCE_MPS=$(safe_div_int "$MSG_COUNT" "$PRODUCE_MS")
	local PRODUCE_MBPS
	PRODUCE_MBPS=$(calc_mbps "$MSG_COUNT" "$MSG_SIZE" "$PRODUCE_MS")

	log_info "Produce: ${PRODUCE_MS}ms elapsed"

	# -- Consume --
	log_info "Consuming $MSG_COUNT messages..."
	START=$(get_timestamp_ms)

	$KAFKA_CONSUMER --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$TOPIC" --from-beginning \
		--max-messages "$MSG_COUNT" \
		--timeout-ms "$CONSUMER_TIMEOUT_MS" >/dev/null 2>&1 || true

	END=$(get_timestamp_ms)
	local CONSUME_MS=$((END - START))
	local CONSUME_MPS
	CONSUME_MPS=$(safe_div_int "$MSG_COUNT" "$CONSUME_MS")
	local CONSUME_MBPS
	CONSUME_MBPS=$(calc_mbps "$MSG_COUNT" "$MSG_SIZE" "$CONSUME_MS")

	log_info "Consume: ${CONSUME_MS}ms elapsed"

	# -- Report --
	echo ""
	echo "  [Benchmark 1] Throughput (${MSG_COUNT} x ${MSG_SIZE}B)"
	echo "    Produce: ${PRODUCE_MPS} msg/sec  (${PRODUCE_MBPS} MB/sec)  [${PRODUCE_MS}ms]"
	echo "    Consume: ${CONSUME_MPS} msg/sec  (${CONSUME_MBPS} MB/sec)  [${CONSUME_MS}ms]"

	store_result "Throughput Produce (msg/sec)" "$PRODUCE_MPS"
	store_result "Throughput Produce (MB/sec)" "$PRODUCE_MBPS"
	store_result "Throughput Consume (msg/sec)" "$CONSUME_MPS"
	store_result "Throughput Consume (MB/sec)" "$CONSUME_MBPS"

	log_success "Benchmark 1 completed"
}

# =============================================================================
# Benchmark 2: Large Messages (10KB)
# =============================================================================

bench_large_messages() {
	local TOPIC="${TEST_PREFIX}-large"
	local MSG_COUNT=1000
	local MSG_SIZE=10240 # 10KB

	echo ""
	echo -e "${CYAN}--- Benchmark 2: Large Messages (${MSG_COUNT} x ${MSG_SIZE}B / 10KB) ---${NC}"

	create_topic "$TOPIC" 3

	# Generate 10KB payload
	local msg
	msg=$(head -c "$MSG_SIZE" /dev/zero | tr '\0' 'B')

	# -- Produce --
	log_info "Producing $MSG_COUNT large messages (10KB each)..."
	local START
	START=$(get_timestamp_ms)

	seq 1 "$MSG_COUNT" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$TOPIC" 2>/dev/null

	local END
	END=$(get_timestamp_ms)
	local PRODUCE_MS=$((END - START))
	local PRODUCE_MPS
	PRODUCE_MPS=$(safe_div_int "$MSG_COUNT" "$PRODUCE_MS")
	local PRODUCE_MBPS
	PRODUCE_MBPS=$(calc_mbps "$MSG_COUNT" "$MSG_SIZE" "$PRODUCE_MS")

	log_info "Produce: ${PRODUCE_MS}ms elapsed"

	# -- Consume --
	log_info "Consuming $MSG_COUNT large messages..."
	START=$(get_timestamp_ms)

	$KAFKA_CONSUMER --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$TOPIC" --from-beginning \
		--max-messages "$MSG_COUNT" \
		--timeout-ms "$CONSUMER_TIMEOUT_MS" >/dev/null 2>&1 || true

	END=$(get_timestamp_ms)
	local CONSUME_MS=$((END - START))
	local CONSUME_MPS
	CONSUME_MPS=$(safe_div_int "$MSG_COUNT" "$CONSUME_MS")
	local CONSUME_MBPS
	CONSUME_MBPS=$(calc_mbps "$MSG_COUNT" "$MSG_SIZE" "$CONSUME_MS")

	log_info "Consume: ${CONSUME_MS}ms elapsed"

	# -- Report --
	echo ""
	echo "  [Benchmark 2] Large Messages (${MSG_COUNT} x 10KB)"
	echo "    Produce: ${PRODUCE_MPS} msg/sec  (${PRODUCE_MBPS} MB/sec)  [${PRODUCE_MS}ms]"
	echo "    Consume: ${CONSUME_MPS} msg/sec  (${CONSUME_MBPS} MB/sec)  [${CONSUME_MS}ms]"

	store_result "Large Produce (msg/sec)" "$PRODUCE_MPS"
	store_result "Large Produce (MB/sec)" "$PRODUCE_MBPS"
	store_result "Large Consume (msg/sec)" "$CONSUME_MPS"
	store_result "Large Consume (MB/sec)" "$CONSUME_MBPS"

	log_success "Benchmark 2 completed"
}

# =============================================================================
# Benchmark 3: End-to-End Latency
# =============================================================================

bench_latency() {
	local TOPIC="${TEST_PREFIX}-latency"
	local MSG_COUNT=100
	local LATENCY_TMP="/tmp/mb_s3_latency_$$"

	echo ""
	echo -e "${CYAN}--- Benchmark 3: End-to-End Latency (${MSG_COUNT} messages) ---${NC}"

	create_topic "$TOPIC" 1

	if [[ "$HAS_KCAT" != "true" ]]; then
		log_warn "kcat not available; using simplified latency measurement"

		# Simplified: measure round-trip for small batches
		local total_latency_ms=0
		local measured=0

		for batch in $(seq 1 10); do
			local send_ts
			send_ts=$(get_timestamp_ms)

			echo "latency-msg-${batch}" |
				$KAFKA_PRODUCER --bootstrap-server "$BROKER1" \
					--topic "$TOPIC" 2>/dev/null

			$KAFKA_CONSUMER --bootstrap-server "$BROKER2" \
				--topic "$TOPIC" --from-beginning \
				--max-messages 1 \
				--timeout-ms 30000 >/dev/null 2>&1 || true

			local recv_ts
			recv_ts=$(get_timestamp_ms)
			local diff=$((recv_ts - send_ts))
			echo "$diff" >>"$LATENCY_TMP"
			total_latency_ms=$((total_latency_ms + diff))
			measured=$((measured + 1))
		done

		if [[ $measured -gt 0 ]]; then
			# Sort and compute percentiles
			sort -n "$LATENCY_TMP" >"${LATENCY_TMP}.sorted"
			local count
			count=$(wc -l <"${LATENCY_TMP}.sorted" | tr -d ' ')
			local p50_idx=$(((count * 50 + 99) / 100))
			local p95_idx=$(((count * 95 + 99) / 100))
			local p99_idx=$(((count * 99 + 99) / 100))
			# Clamp indices
			[[ $p50_idx -lt 1 ]] && p50_idx=1
			[[ $p95_idx -lt 1 ]] && p95_idx=1
			[[ $p99_idx -lt 1 ]] && p99_idx=1
			[[ $p50_idx -gt $count ]] && p50_idx=$count
			[[ $p95_idx -gt $count ]] && p95_idx=$count
			[[ $p99_idx -gt $count ]] && p99_idx=$count

			local P50
			P50=$(sed -n "${p50_idx}p" "${LATENCY_TMP}.sorted")
			local P95
			P95=$(sed -n "${p95_idx}p" "${LATENCY_TMP}.sorted")
			local P99
			P99=$(sed -n "${p99_idx}p" "${LATENCY_TMP}.sorted")
			local AVG=$((total_latency_ms / measured))

			echo ""
			echo "  [Benchmark 3] End-to-End Latency (simplified, ${measured} samples)"
			echo "    Average:  ${AVG} ms"
			echo "    P50:      ${P50} ms"
			echo "    P95:      ${P95} ms"
			echo "    P99:      ${P99} ms"

			store_result "Latency Avg (ms)" "$AVG"
			store_result "Latency P50 (ms)" "$P50"
			store_result "Latency P95 (ms)" "$P95"
			store_result "Latency P99 (ms)" "$P99"
		fi

		rm -f "$LATENCY_TMP" "${LATENCY_TMP}.sorted"
		log_success "Benchmark 3 completed (simplified)"
		return
	fi

	# Full kcat-based latency measurement
	log_info "Producing $MSG_COUNT timestamped messages via kcat..."
	rm -f "$LATENCY_TMP"

	for i in $(seq 1 "$MSG_COUNT"); do
		local send_ts
		send_ts=$(get_timestamp_ms)
		echo "${send_ts}:latency-payload-${i}" |
			"$KCAT_CMD" -P -b "$BROKER1" -t "$TOPIC" 2>/dev/null
	done

	sleep 2

	log_info "Consuming and computing latencies..."
	local consumed
	consumed=$("$KCAT_CMD" -C -b "$BROKER2" -t "$TOPIC" -e -q -f '%s\n' 2>/dev/null || true)

	local total_latency_ms=0
	local measured=0

	while IFS= read -r line; do
		local send_ts="${line%%:*}"
		if [[ "$send_ts" =~ ^[0-9]+$ ]]; then
			local recv_ts
			recv_ts=$(get_timestamp_ms)
			local diff=$((recv_ts - send_ts))
			echo "$diff" >>"$LATENCY_TMP"
			total_latency_ms=$((total_latency_ms + diff))
			measured=$((measured + 1))
		fi
	done <<<"$consumed"

	if [[ $measured -gt 0 ]]; then
		sort -n "$LATENCY_TMP" >"${LATENCY_TMP}.sorted"
		local count
		count=$(wc -l <"${LATENCY_TMP}.sorted" | tr -d ' ')
		local p50_idx=$(((count * 50 + 99) / 100))
		local p95_idx=$(((count * 95 + 99) / 100))
		local p99_idx=$(((count * 99 + 99) / 100))
		[[ $p50_idx -lt 1 ]] && p50_idx=1
		[[ $p95_idx -lt 1 ]] && p95_idx=1
		[[ $p99_idx -lt 1 ]] && p99_idx=1
		[[ $p50_idx -gt $count ]] && p50_idx=$count
		[[ $p95_idx -gt $count ]] && p95_idx=$count
		[[ $p99_idx -gt $count ]] && p99_idx=$count

		local P50
		P50=$(sed -n "${p50_idx}p" "${LATENCY_TMP}.sorted")
		local P95
		P95=$(sed -n "${p95_idx}p" "${LATENCY_TMP}.sorted")
		local P99
		P99=$(sed -n "${p99_idx}p" "${LATENCY_TMP}.sorted")
		local AVG=$((total_latency_ms / measured))

		echo ""
		echo "  [Benchmark 3] End-to-End Latency (kcat, ${measured} samples)"
		echo "    Average:  ${AVG} ms"
		echo "    P50:      ${P50} ms"
		echo "    P95:      ${P95} ms"
		echo "    P99:      ${P99} ms"

		store_result "Latency Avg (ms)" "$AVG"
		store_result "Latency P50 (ms)" "$P50"
		store_result "Latency P95 (ms)" "$P95"
		store_result "Latency P99 (ms)" "$P99"
	else
		log_warn "No latency samples collected"
	fi

	rm -f "$LATENCY_TMP" "${LATENCY_TMP}.sorted"
	log_success "Benchmark 3 completed"
}

# =============================================================================
# Benchmark 4: Concurrent Producers (3 producers, 3 brokers)
# =============================================================================

bench_concurrent_producers() {
	local TOPIC="${TEST_PREFIX}-concurrent"
	local MSG_PER_PRODUCER=5000
	local PRODUCER_COUNT=3
	local TOTAL_MSGS=$((MSG_PER_PRODUCER * PRODUCER_COUNT))
	local MSG_SIZE=100

	echo ""
	echo -e "${CYAN}--- Benchmark 4: Concurrent Producers (${PRODUCER_COUNT} x ${MSG_PER_PRODUCER} msgs) ---${NC}"

	create_topic "$TOPIC" 3

	local msg
	msg=$(head -c "$MSG_SIZE" /dev/zero | tr '\0' 'C')

	log_info "Starting $PRODUCER_COUNT parallel producers (one per broker)..."

	local START
	START=$(get_timestamp_ms)

	# Producer 1 -> BROKER1
	seq 1 "$MSG_PER_PRODUCER" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BROKER1" --topic "$TOPIC" 2>/dev/null &
	local PID1=$!

	# Producer 2 -> BROKER2
	seq 1 "$MSG_PER_PRODUCER" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BROKER2" --topic "$TOPIC" 2>/dev/null &
	local PID2=$!

	# Producer 3 -> BROKER3
	seq 1 "$MSG_PER_PRODUCER" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BROKER3" --topic "$TOPIC" 2>/dev/null &
	local PID3=$!

	# Wait for all producers
	local failed=0
	wait "$PID1" 2>/dev/null || failed=$((failed + 1))
	wait "$PID2" 2>/dev/null || failed=$((failed + 1))
	wait "$PID3" 2>/dev/null || failed=$((failed + 1))

	local END
	END=$(get_timestamp_ms)
	local ELAPSED_MS=$((END - START))
	local TOTAL_MPS
	TOTAL_MPS=$(safe_div_int "$TOTAL_MSGS" "$ELAPSED_MS")
	local TOTAL_MBPS
	TOTAL_MBPS=$(calc_mbps "$TOTAL_MSGS" "$MSG_SIZE" "$ELAPSED_MS")

	if [[ $failed -gt 0 ]]; then
		log_warn "$failed producer(s) exited with errors"
	fi

	# Verify total message count via consumer
	log_info "Verifying produced messages..."
	local CONSUMED_COUNT
	CONSUMED_COUNT=$($KAFKA_CONSUMER --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$TOPIC" --from-beginning \
		--timeout-ms "$CONSUMER_TIMEOUT_MS" 2>/dev/null | wc -l | tr -d ' ')

	echo ""
	echo "  [Benchmark 4] Concurrent Producers ($PRODUCER_COUNT x $MSG_PER_PRODUCER)"
	echo "    Total Time:     ${ELAPSED_MS} ms"
	echo "    Total Messages: ${TOTAL_MSGS} (verified: ${CONSUMED_COUNT})"
	echo "    Throughput:     ${TOTAL_MPS} msg/sec  (${TOTAL_MBPS} MB/sec)"
	echo "    Per-producer:   $((TOTAL_MPS / PRODUCER_COUNT)) msg/sec"

	store_result "Concurrent Total (msg/sec)" "$TOTAL_MPS"
	store_result "Concurrent Total (MB/sec)" "$TOTAL_MBPS"
	store_result "Concurrent Verified Messages" "$CONSUMED_COUNT"

	log_success "Benchmark 4 completed"
}

# =============================================================================
# Benchmark 5: Cross-Broker Consumer Performance
# =============================================================================

bench_cross_broker_consume() {
	local TOPIC="${TEST_PREFIX}-crossbroker"
	local MSG_COUNT=10000
	local MSG_SIZE=100

	echo ""
	echo -e "${CYAN}--- Benchmark 5: Cross-Broker Consumer Performance ---${NC}"

	create_topic "$TOPIC" 3

	local msg
	msg=$(head -c "$MSG_SIZE" /dev/zero | tr '\0' 'D')

	# Produce all messages via BROKER1
	log_info "Producing $MSG_COUNT messages via broker1 ($BROKER1)..."
	local PRODUCE_START
	PRODUCE_START=$(get_timestamp_ms)

	seq 1 "$MSG_COUNT" | while read -r _i; do echo "$msg"; done |
		$KAFKA_PRODUCER --bootstrap-server "$BROKER1" --topic "$TOPIC" 2>/dev/null

	local PRODUCE_END
	PRODUCE_END=$(get_timestamp_ms)
	local PRODUCE_MS=$((PRODUCE_END - PRODUCE_START))
	log_info "Produce completed: ${PRODUCE_MS}ms"

	sleep 1

	# Consume from each broker and measure time
	local BROKERS=("$BROKER1" "$BROKER2" "$BROKER3")
	local BROKER_NAMES=("broker1" "broker2" "broker3")

	echo ""
	echo "  [Benchmark 5] Cross-Broker Consumer Performance"
	echo "    Producer: broker1 ($BROKER1) -> ${MSG_COUNT} msgs in ${PRODUCE_MS}ms"
	echo ""

	for idx in 0 1 2; do
		local broker="${BROKERS[$idx]}"
		local name="${BROKER_NAMES[$idx]}"

		log_info "Consuming from $name ($broker)..."
		local C_START
		C_START=$(get_timestamp_ms)

		local received
		received=$($KAFKA_CONSUMER --bootstrap-server "$broker" \
			--topic "$TOPIC" --from-beginning \
			--max-messages "$MSG_COUNT" \
			--timeout-ms "$CONSUMER_TIMEOUT_MS" 2>/dev/null | wc -l | tr -d ' ')

		local C_END
		C_END=$(get_timestamp_ms)
		local C_MS=$((C_END - C_START))
		local C_MPS
		C_MPS=$(safe_div_int "$MSG_COUNT" "$C_MS")
		local C_MBPS
		C_MBPS=$(calc_mbps "$MSG_COUNT" "$MSG_SIZE" "$C_MS")

		echo "    Consumer via ${name}: ${C_MPS} msg/sec  (${C_MBPS} MB/sec)  [${C_MS}ms]  received=${received}"

		store_result "CrossBroker ${name} (msg/sec)" "$C_MPS"
		store_result "CrossBroker ${name} (MB/sec)" "$C_MBPS"
	done

	log_success "Benchmark 5 completed"
}

# =============================================================================
# Result Report
# =============================================================================

print_report() {
	echo ""
	echo "============================================================"
	echo "  Multi-Broker S3 Performance Report"
	echo "============================================================"
	echo "  Cluster:    3 brokers ($BROKER1, $BROKER2, $BROKER3)"
	echo "  Bootstrap:  $BOOTSTRAP_SERVERS"
	echo "  Storage:    S3"
	echo "  Date:       $(date '+%Y-%m-%d %H:%M:%S')"
	echo "  OS:         $(uname -s) $(uname -m)"
	echo "  Precision:  $DATE_CMD"
	echo "============================================================"
	echo ""

	if [[ ${#RESULT_LABELS[@]} -eq 0 ]]; then
		echo "  (no results collected)"
		echo ""
		return
	fi

	# Print results table
	printf "  %-40s  %s\n" "Metric" "Value"
	printf "  %-40s  %s\n" "----------------------------------------" "----------"

	for i in "${!RESULT_LABELS[@]}"; do
		printf "  %-40s  %s\n" "${RESULT_LABELS[$i]}" "${RESULT_VALUES[$i]}"
	done

	echo ""
	echo "============================================================"
	echo ""
}

# =============================================================================
# Cleanup
# =============================================================================

cleanup() {
	if [[ "$SKIP_CLEANUP" == "true" ]]; then
		log_info "Skipping cleanup (--skip-cleanup)"
		return
	fi

	log_info "Cleaning up performance test topics..."
	local topics
	topics=$($KAFKA_TOPICS --list --bootstrap-server "$BOOTSTRAP_SERVERS" 2>/dev/null | grep "^${TEST_PREFIX}" || true)

	if [[ -n "$topics" ]]; then
		while IFS= read -r topic; do
			$KAFKA_TOPICS --delete --bootstrap-server "$BOOTSTRAP_SERVERS" \
				--topic "$topic" 2>/dev/null || true
		done <<<"$topics"
		log_info "Cleanup complete"
	else
		log_info "No test topics to clean"
	fi
}

# =============================================================================
# Main
# =============================================================================

main() {
	echo ""
	echo "============================================================"
	echo "  Multi-Broker S3 Performance Benchmarks"
	echo "============================================================"
	echo ""

	detect_tools
	check_brokers

	echo ""
	log_info "Bootstrap Servers: $BOOTSTRAP_SERVERS"
	log_info "Kafka CLI: $KAFKA_TOPICS"
	log_info "kcat: $(if [[ "$HAS_KCAT" == "true" ]]; then echo "$KCAT_CMD"; else echo "not available"; fi)"
	log_info "Date command: $DATE_CMD"
	echo ""

	if [[ -n "$RUN_BENCH" ]]; then
		case "$RUN_BENCH" in
		throughput) bench_throughput ;;
		large) bench_large_messages ;;
		latency) bench_latency ;;
		concurrent) bench_concurrent_producers ;;
		crossbroker) bench_cross_broker_consume ;;
		*)
			log_error "Unknown benchmark: $RUN_BENCH"
			log_info "Available: throughput, large, latency, concurrent, crossbroker"
			exit 1
			;;
		esac
	else
		bench_throughput
		bench_large_messages
		bench_latency
		bench_concurrent_producers
		bench_cross_broker_consume
	fi

	print_report
	cleanup

	log_success "All benchmarks completed"
}

main "$@"
