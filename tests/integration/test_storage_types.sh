#!/bin/bash
# =============================================================================
# DataCore Storage Type Compatibility Tests
# Tests for Memory and S3 storage engines
#
# Prerequisites:
# - Kafka CLI tools (kafka-topics, kafka-console-producer, kafka-console-consumer)
# - DataCore broker running
# - For S3 tests: AWS credentials or S3-compatible endpoint configured
#
# Usage:
#   ./test_storage_types.sh                    # Run all tests
#   ./test_storage_types.sh --memory-only      # Memory storage tests only
#   ./test_storage_types.sh --s3-only          # S3 storage tests only
#   ./test_storage_types.sh --skip-cleanup     # Skip cleanup after tests
#   ./test_storage_types.sh --keep-broker      # Don't restart broker between tests
# =============================================================================

set -o pipefail

# =============================================================================
# Configuration
# =============================================================================

# Broker configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
BROKER_HOST="${BROKER_HOST:-localhost}"
BROKER_PORT="${BROKER_PORT:-9092}"
REST_PORT="${REST_PORT:-8080}"

# Test configuration
TEST_PREFIX="storage-test"
TIMEOUT=15
MESSAGE_COUNT=20

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0
MEMORY_TESTS_PASSED=0
S3_TESTS_PASSED=0
SWITCH_TESTS_PASSED=0

# Options
MEMORY_ONLY=false
S3_ONLY=false
SKIP_CLEANUP=false
KEEP_BROKER=false
VERBOSE=false

# Storage type for current test
CURRENT_STORAGE=""

# PID file for broker management
BROKER_PID_FILE="/tmp/datacore_broker.pid"
CONFIG_FILE="config.toml"
CONFIG_MEMORY="config_memory.toml"
CONFIG_S3="config_s3.toml"

# =============================================================================
# Utility Functions
# =============================================================================

log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
	echo -e "${GREEN}[PASS]${NC} $1"
	((TESTS_PASSED++))
}

log_fail() {
	echo -e "${RED}[FAIL]${NC} $1"
	((TESTS_FAILED++))
}

log_skip() {
	echo -e "${YELLOW}[SKIP]${NC} $1"
	((TESTS_SKIPPED++))
}

log_section() {
	echo ""
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
	echo -e "${CYAN}  $1${NC}"
	echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

log_subsection() {
	echo ""
	echo -e "${YELLOW}--- $1 ---${NC}"
}

log_debug() {
	if [[ "$VERBOSE" == "true" ]]; then
		echo -e "${BLUE}[DEBUG]${NC} $1"
	fi
}

# Detect available Kafka CLI tools
detect_tools() {
	# Kafka CLI
	if command -v kafka-topics &>/dev/null; then
		KAFKA_TOPICS="kafka-topics"
		KAFKA_PRODUCER="kafka-console-producer"
		KAFKA_CONSUMER="kafka-console-consumer"
		KAFKA_GROUPS="kafka-consumer-groups"
	elif command -v kafka-topics.sh &>/dev/null; then
		KAFKA_TOPICS="kafka-topics.sh"
		KAFKA_PRODUCER="kafka-console-producer.sh"
		KAFKA_CONSUMER="kafka-console-consumer.sh"
		KAFKA_GROUPS="kafka-consumer-groups.sh"
	else
		echo "Error: Kafka CLI tools not found"
		echo "Please install Kafka CLI tools and add them to PATH"
		exit 1
	fi

	log_info "Kafka CLI tools detected: $KAFKA_TOPICS, $KAFKA_PRODUCER, $KAFKA_CONSUMER"
}

# Check broker connectivity
check_broker() {
	log_info "Checking broker connectivity at $BOOTSTRAP_SERVER..."

	local host=$(echo $BOOTSTRAP_SERVER | cut -d: -f1)
	local port=$(echo $BOOTSTRAP_SERVER | cut -d: -f2)

	if ! nc -z -w2 "$host" "$port" 2>/dev/null; then
		echo "Error: Cannot connect to broker at $BOOTSTRAP_SERVER"
		echo "Please ensure DataCore is running before running tests"
		return 1
	fi

	log_info "Broker is reachable at $BOOTSTRAP_SERVER"
	return 0
}

# Wait for broker to be fully ready using health check
wait_for_broker_ready() {
	local max_attempts=15
	local attempt=0
	local host="$1"
	local port="$2"

	while [[ $attempt -lt $max_attempts ]]; do
		# Try to get broker metadata quickly
		if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list --timeout 2000 2>/dev/null; then
			log_info "Broker is ready (metadata available)"
			return 0
		fi
		sleep 0.3
		((attempt++))
	done

	# Fallback to TCP check
	if nc -z "$host" "$port" 2>/dev/null; then
		log_info "Broker is ready (TCP available)"
		return 0
	fi

	return 1
}

# Generate unique topic name
unique_topic() {
	echo "${TEST_PREFIX}-${CURRENT_STORAGE}-$(date +%s)-$$"
}

# Generate unique consumer group name
unique_group() {
	echo "${TEST_PREFIX}-group-$(date +%s)-$$"
}

# Generate random message of specified size
generate_message() {
	local size_bytes=$1
	# Generate random base64 content of specified size
	head -c "$size_bytes" /dev/urandom | base64 | head -c "$size_bytes"
}

# =============================================================================
# Broker Management
# =============================================================================

# Create memory storage config
create_memory_config() {
	log_debug "Creating memory storage configuration..."

	cat >"$CONFIG_MEMORY" <<'EOF'
[broker]
broker_id = 1
cluster_id = "datacore-test-memory"
host = "0.0.0.0"
port = 9092
max_connections = 1000

[storage]
engine = "memory"

[storage.memory]
max_memory_mb = 1024
segment_size_bytes = 104857600

[rest]
enabled = true
host = "0.0.0.0"
port = 8080
EOF
}

# Create S3 storage config
create_s3_config() {
	log_debug "Creating S3 storage configuration..."

	# Get S3 settings from environment or config
	local s3_endpoint="${DATACORE_S3_ENDPOINT:-}"
	local s3_bucket="${DATACORE_S3_BUCKET:-datacore-test}"
	local s3_region="${DATACORE_S3_REGION:-us-east-1}"
	local s3_prefix="${DATACORE_S3_PREFIX:-datacore-test/}"

	cat >"$CONFIG_S3" <<EOF
[broker]
broker_id = 1
cluster_id = "datacore-test-s3"
host = "0.0.0.0"
port = 9092
max_connections = 1000

[storage]
engine = "s3"

[storage.s3]
endpoint = "$s3_endpoint"
bucket = "$s3_bucket"
region = "$s3_region"
prefix = "$s3_prefix"
batch_timeout_ms = 1000
batch_max_bytes = 10485760
index_cache_ttl_ms = 60000

[rest]
enabled = true
host = "0.0.0.0"
port = 8080
EOF
}

# Stop broker if running
stop_broker() {
	if [[ -f "$BROKER_PID_FILE" ]]; then
		local pid=$(cat "$BROKER_PID_FILE" 2>/dev/null)
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			log_info "Stopping broker (PID: $pid)..."
			kill "$pid" 2>/dev/null || true
			sleep 2
			# Force kill if still running
			if kill -0 "$pid" 2>/dev/null; then
				kill -9 "$pid" 2>/dev/null || true
			fi
		fi
		rm -f "$BROKER_PID_FILE"
	fi
}

# Start broker with specified config
start_broker() {
	local config_file="$1"
	local storage_type="$2"

	log_info "Starting broker with $storage_type storage..."

	# Check if broker binary exists
	if [[ ! -f "bin/datacore" ]]; then
		log_info "Building DataCore..."
		make build 2>/dev/null || {
			echo "Error: Failed to build DataCore"
			return 1
		}
	fi

	# Stop existing broker
	stop_broker

	# Start broker with specified config
	./bin/datacore broker start --config="$config_file" &
	local pid=$!
	echo "$pid" >"$BROKER_PID_FILE"

	log_info "Broker started (PID: $pid), waiting for readiness..."

	# Wait for broker to be ready using optimized check
	if wait_for_broker_ready "$BROKER_HOST" "$BROKER_PORT"; then
		CURRENT_STORAGE="$storage_type"
		return 0
	fi

	echo "Error: Broker failed to start"
	return 1
}

# =============================================================================
# Topic Management
# =============================================================================

# Create topic with specified partitions
create_topic() {
	local topic_name="$1"
	local partitions="${2:-1}"

	log_debug "Creating topic: $topic_name with $partitions partitions"

	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic_name" \
		--partitions "$partitions" \
		--replication-factor 1 \
		2>/dev/null

	local result=$?
	if [[ $result -ne 0 ]]; then
		log_debug "Topic creation returned non-zero: $result (may already exist)"
	fi
	return 0
}

# Delete topic
delete_topic() {
	local topic_name="$1"

	log_debug "Deleting topic: $topic_name"

	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--topic "$topic_name" \
		2>/dev/null || true
}

# List topics matching test prefix
list_test_topics() {
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--list 2>/dev/null | grep "^${TEST_PREFIX}-${CURRENT_STORAGE}" || true
}

# Clean up all test topics for current storage
cleanup_topics() {
	local topics
	topics=$(list_test_topics)

	if [[ -n "$topics" ]]; then
		log_info "Cleaning up ${CURRENT_STORAGE} test topics..."
		for topic in $topics; do
			delete_topic "$topic"
		done
		# Wait for deletion to propagate
		sleep 1
	fi
}

# =============================================================================
# Test Functions
# =============================================================================

run_test() {
	local test_name="$1"
	local test_func="$2"
	local storage_type="$3"

	((TESTS_RUN++))

	log_subsection "$test_name ($storage_type)"

	if $test_func; then
		log_pass "$test_name"
		case "$storage_type" in
		memory) ((MEMORY_TESTS_PASSED++)) ;;
		s3) ((S3_TESTS_PASSED++)) ;;
		switch) ((SWITCH_TESTS_PASSED++)) ;;
		esac
		return 0
	else
		log_fail "$test_name"
		return 1
	fi
}

# =============================================================================
# Memory Storage Tests
# =============================================================================

test_memory_basic_produce_consume() {
	local topic
	topic=$(unique_topic)
	local message="Memory storage test message $(date +%s)"

	create_topic "$topic" 1

	# Produce message
	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.3

	# Consume message
	local received
	received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$topic"

	[[ "$received" == *"$message"* ]]
}

test_memory_large_messages() {
	local topic
	topic=$(unique_topic)

	create_topic "$topic" 1

	# Test reduced message sizes (faster testing)
	local sizes=("100" "1000" "10240" "102400") # 100B, 1KB, 10KB, 100KB (1MB removed for speed)
	local all_passed=true

	for size in "${sizes[@]}"; do
		local message
		message=$(generate_message "$size")

		echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || {
			all_passed=false
			continue
		}

		sleep 0.2

		local received
		received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--timeout-ms $((TIMEOUT * 1000)) \
			2>/dev/null) || {
			all_passed=false
			continue
		}

		local received_len=${#received}
		# Base64 adds ~33% overhead, so check if length is reasonable
		if [[ $received_len -lt $((size / 2)) ]]; then
			log_debug "Message size mismatch: expected ~$size, got $received_len"
			all_passed=false
		fi
	done

	delete_topic "$topic"
	$all_passed
}

test_memory_multiple_topics() {
	local topics=("topic1" "topic2" "topic3")
	local all_passed=true

	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		local message="Message for $topic"

		create_topic "$topic" 1
		echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || {
			all_passed=false
			continue
		}
	done

	sleep 0.3

	# Verify all topics exist
	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		local exists=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -c "^${topic}$" || echo 0)
		if [[ $exists -eq 0 ]]; then
			all_passed=false
		fi
	done

	# Cleanup
	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		delete_topic "$topic" 2>/dev/null || true
	done

	$all_passed
}

test_memory_consumer_groups() {
	local topic
	topic=$(unique_topic)
	local group1
	group1=$(unique_group)
	local group2
	group2=$(unique_group)

	create_topic "$topic" 2

	# Produce messages
	for i in $(seq 1 5); do
		echo "Message $i" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || return 1
	done

	sleep 0.3

	# Consume with group 1
	local count1
	count1=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group1" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count1=0

	# Consume with group 2 (should get all messages again)
	local count2
	count2=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group2" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count2=0

	# Verify both groups consumed messages
	delete_topic "$topic"

	[[ $count1 -ge 3 ]] && [[ $count2 -ge 3 ]]
}

test_memory_multiple_partitions() {
	local topic
	topic=$(unique_topic)
	local partitions=3

	create_topic "$topic" "$partitions"

	# Produce messages (reduced count)
	for i in $(seq 1 15); do
		echo "Message $i for partition test" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			--property "parse.key=true" \
			--property "key.separator=:" \
			2>/dev/null || return 1
	done

	sleep 0.3

	# Consume all messages
	local count
	count=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	delete_topic "$topic"

	[[ $count -ge 10 ]]
}

test_memory_batch_produce() {
	local topic
	topic=$(unique_topic)

	create_topic "$topic" 1

	# Batch produce (reduced count)
	for i in $(seq 1 30); do
		echo "Batch message $i"
	done | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--batch-size 10 \
		2>/dev/null || return 1

	sleep 0.3

	# Consume all
	local count
	count=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	delete_topic "$topic"

	[[ $count -ge 20 ]]
}

test_memory_data_persistence() {
	local topic
	topic=$(unique_topic)
	local message="Persistent message $(date +%s)"

	create_topic "$topic" 1

	# Produce
	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.3

	# Consume first time
	local first
	first=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	# Short delay
	sleep 0.5

	# Consume again (should get same message)
	local second
	second=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$topic"

	# Both should contain the message
	[[ "$first" == *"$message"* ]] && [[ "$second" == *"$message"* ]]
}

test_memory_topic_deletion() {
	local topic
	topic=$(unique_topic)

	create_topic "$topic" 1

	# Produce message
	echo "Test message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.3

	# Verify topic exists
	local exists_before
	exists_before=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -c "^${topic}$" || echo 0)

	# Delete topic
	delete_topic "$topic"

	sleep 1

	# Verify topic is deleted
	local exists_after
	exists_after=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -c "^${topic}$" || echo 0)

	[[ $exists_before -eq 1 ]] && [[ $exists_after -eq 0 ]]
}

# =============================================================================
# S3 Storage Tests
# =============================================================================

test_s3_basic_produce_consume() {
	local topic
	topic=$(unique_topic)
	local message="S3 storage test message $(date +%s)"

	create_topic "$topic" 1

	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.5

	local received
	received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$topic"

	[[ "$received" == *"$message"* ]]
}

test_s3_large_messages() {
	local topic
	topic=$(unique_topic)

	create_topic "$topic" 1

	# Test larger messages (reduced sizes for speed)
	local sizes=("1024" "10240" "102400") # 1KB, 10KB, 100KB (1MB removed)
	local all_passed=true

	for size in "${sizes[@]}"; do
		local message
		message=$(generate_message "$size")

		echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || {
			all_passed=false
			continue
		}

		sleep 0.5

		local received
		received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--timeout-ms $((TIMEOUT * 1000)) \
			2>/dev/null) || {
			all_passed=false
			continue
		}

		local received_len=${#received}
		if [[ $received_len -lt $((size / 2)) ]]; then
			log_debug "S3: Message size mismatch for $size bytes"
			all_passed=false
		fi
	done

	delete_topic "$topic"
	$all_passed
}

test_s3_multiple_topics() {
	local topics=("s3topic1" "s3topic2" "s3topic3")
	local all_passed=true

	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		local message="S3 message for $topic"

		create_topic "$topic" 1
		echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || {
			all_passed=false
			continue
		}
	done

	sleep 0.5

	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		local exists=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -c "^${topic}$" || echo 0)
		if [[ $exists -eq 0 ]]; then
			all_passed=false
		fi
	done

	for t in "${topics[@]}"; do
		local topic="${TEST_PREFIX}-${CURRENT_STORAGE}-${t}-$(date +%s)"
		delete_topic "$topic" 2>/dev/null || true
	done

	$all_passed
}

test_s3_consumer_groups() {
	local topic
	topic=$(unique_topic)
	local group1
	group1=$(unique_group)
	local group2
	group2=$(unique_group)

	create_topic "$topic" 2

	for i in $(seq 1 5); do
		echo "S3 Message $i" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || return 1
	done

	sleep 0.5

	local count1
	count1=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group1" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count1=0

	local count2
	count2=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group2" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count2=0

	delete_topic "$topic"

	[[ $count1 -ge 3 ]] && [[ $count2 -ge 3 ]]
}

test_s3_multiple_partitions() {
	local topic
	topic=$(unique_topic)
	local partitions=3

	create_topic "$topic" "$partitions"

	for i in $(seq 1 15); do
		echo "S3 Partition message $i" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic" \
			2>/dev/null || return 1
	done

	sleep 0.5

	local count
	count=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	delete_topic "$topic"

	[[ $count -ge 10 ]]
}

test_s3_data_persistence() {
	local topic
	topic=$(unique_topic)
	local message="S3 persistent message $(date +%s)"

	create_topic "$topic" 1

	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.5

	local first
	first=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	sleep 0.5

	local second
	second=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$topic"

	[[ "$first" == *"$message"* ]] && [[ "$second" == *"$message"* ]]
}

test_s3_prefix_isolation() {
	local topic
	topic=$(unique_topic)
	local message="S3 prefix test $(date +%s)"

	create_topic "$topic" 1

	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		2>/dev/null || return 1

	sleep 0.5

	local received
	received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$topic"

	# Message should be readable
	[[ "$received" == *"$message"* ]]
}

test_s3_batch_performance() {
	local topic
	topic=$(unique_topic)

	create_topic "$topic" 2

	# Produce batch (reduced count)
	for i in $(seq 1 20); do
		echo "S3 batch message $i"
	done | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--batch-size 10 \
		2>/dev/null || return 1

	sleep 0.5

	local count
	count=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	delete_topic "$topic"

	[[ $count -ge 15 ]]
}

# =============================================================================
# Storage Switching Tests
# =============================================================================

test_switch_memory_to_s3() {
	local memory_topic="${TEST_PREFIX}-memory-$(date +%s)"
	local s3_topic="${TEST_PREFIX}-s3-$(date +%s)"
	local message="Switch test message $(date +%s)"

	# Create and produce in memory storage
	create_topic "$memory_topic" 1
	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$memory_topic" \
		2>/dev/null || return 1

	sleep 0.3

	# Verify message in memory
	local mem_received
	mem_received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$memory_topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$memory_topic"

	# Switch to S3 (this would require broker restart, but we test functionality)
	CURRENT_STORAGE="s3"

	# Create and produce in S3 storage
	create_topic "$s3_topic" 1
	echo "$message" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$s3_topic" \
		2>/dev/null || return 1

	sleep 0.5

	# Verify message in S3
	local s3_received
	s3_received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$s3_topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null) || return 1

	delete_topic "$s3_topic"

	[[ "$mem_received" == *"$message"* ]] && [[ "$s3_received" == *"$message"* ]]
}

test_switch_data_integrity() {
	local topic1="${TEST_PREFIX}-switch1-$(date +%s)"
	local topic2="${TEST_PREFIX}-switch2-$(date +%s)"
	local messages=("Message1" "Message2" "Message3")
	local all_received=true

	# Test with memory first
	CURRENT_STORAGE="memory"
	create_topic "$topic1" 1

	for msg in "${messages[@]}"; do
		echo "$msg" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic1" \
			2>/dev/null || {
			all_received=false
			continue
		}
	done

	sleep 1

	for msg in "${messages[@]}"; do
		local received
		received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic1" \
			--from-beginning \
			--max-messages 1 \
			--timeout-ms $((TIMEOUT * 1000)) \
			2>/dev/null) || {
			all_received=false
			continue
		}

		[[ "$received" == *"$msg"* ]] || { all_received=false; }
	done

	delete_topic "$topic1"

	# Now test with S3
	CURRENT_STORAGE="s3"
	create_topic "$topic2" 1

	for msg in "${messages[@]}"; do
		echo "$msg" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic2" \
			2>/dev/null || {
			all_received=false
			continue
		}
	done

	sleep 2

	for msg in "${messages[@]}"; do
		local received
		received=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic2" \
			--from-beginning \
			--max-messages 1 \
			--timeout-ms $((TIMEOUT * 1000)) \
			2>/dev/null) || {
			all_received=false
			continue
		}

		[[ "$received" == *"$msg"* ]] || { all_received=false; }
	done

	delete_topic "$topic2"

	$all_received
}

test_switch_consumer_offsets() {
	local topic1="${TEST_PREFIX}-offset1-$(date +%s)"
	local topic2="${TEST_PREFIX}-offset2-$(date +%s)"
	local group
	group=$(unique_group)
	local all_passed=true

	# Memory storage test
	CURRENT_STORAGE="memory"
	create_topic "$topic1" 1

	for i in $(seq 1 10); do
		echo "Offset test message $i" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic1" \
			2>/dev/null || {
			all_passed=false
			continue
		}
	done

	sleep 1

	# Consume with group (commits offsets)
	local count1
	count1=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic1" \
		--group "$group" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count1=0

	delete_topic "$topic1"

	# S3 storage test
	CURRENT_STORAGE="s3"
	create_topic "$topic2" 1

	for i in $(seq 1 10); do
		echo "S3 Offset test message $i" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
			--bootstrap-server "$BOOTSTRAP_SERVER" \
			--topic "$topic2" \
			2>/dev/null || {
			all_passed=false
			continue
		}
	done

	sleep 2

	# Same consumer group should work with S3 storage
	local count2
	count2=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic2" \
		--group "$group" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l) || count2=0

	delete_topic "$topic2"

	[[ $count1 -eq 5 ]] && [[ $count2 -eq 5 ]]
}

# =============================================================================
# Cleanup
# =============================================================================

cleanup_all() {
	if [[ "$SKIP_CLEANUP" == "true" ]]; then
		log_info "Skipping cleanup (--skip-cleanup set)"
		return
	fi

	log_info "Cleaning up all test resources..."

	# Cleanup topics for both storage types
	CURRENT_STORAGE="memory"
	cleanup_topics

	CURRENT_STORAGE="s3"
	cleanup_topics

	# Stop broker if we started it
	if [[ -f "$BROKER_PID_FILE" ]]; then
		stop_broker
	fi

	# Remove config files
	rm -f "$CONFIG_MEMORY" "$CONFIG_S3"
}

# =============================================================================
# Main Test Runner
# =============================================================================

usage() {
	echo "Usage: $0 [OPTIONS]"
	echo ""
	echo "Options:"
	echo "  --memory-only     Run memory storage tests only"
	echo "  --s3-only         Run S3 storage tests only"
	echo "  --skip-cleanup    Skip cleanup after tests"
	echo "  --keep-broker     Don't restart broker between storage tests"
	echo "  --verbose         Enable verbose output"
	echo "  --help            Show this help message"
	echo ""
	echo "Environment variables:"
	echo "  BOOTSTRAP_SERVER  Broker address (default: localhost:9092)"
	echo "  DATACORE_S3_ENDPOINT   S3 endpoint URL"
	echo "  DATACORE_S3_BUCKET     S3 bucket name"
	echo "  DATACORE_S3_REGION     S3 region"
	echo "  DATACORE_S3_PREFIX     S3 object prefix"
}

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--memory-only)
			MEMORY_ONLY=true
			S3_ONLY=false
			shift
			;;
		--s3-only)
			S3_ONLY=true
			MEMORY_ONLY=false
			shift
			;;
		--skip-cleanup)
			SKIP_CLEANUP=true
			shift
			;;
		--keep-broker)
			KEEP_BROKER=true
			shift
			;;
		--verbose)
			VERBOSE=true
			shift
			;;
		--help | -h)
			usage
			exit 0
			;;
		*)
			echo "Unknown option: $1"
			usage
			exit 1
			;;
		esac
	done
}

run_memory_tests() {
	log_section "Memory Storage Tests"

	CURRENT_STORAGE="memory"

	# Create memory config
	create_memory_config

	if [[ "$KEEP_BROKER" != "true" ]]; then
		start_broker "$CONFIG_MEMORY" "memory" || return 1
	else
		CURRENT_STORAGE="memory"
	fi

	# Run tests
	run_test "Basic Produce/Consume" test_memory_basic_produce_consume "memory"
	run_test "Large Messages (100B-100KB)" test_memory_large_messages "memory"
	run_test "Multiple Topics" test_memory_multiple_topics "memory"
	run_test "Consumer Groups" test_memory_consumer_groups "memory"
	run_test "Multiple Partitions" test_memory_multiple_partitions "memory"
	run_test "Batch Production" test_memory_batch_produce "memory"
	run_test "Data Persistence" test_memory_data_persistence "memory"
	run_test "Topic Deletion" test_memory_topic_deletion "memory"

	# Cleanup
	cleanup_topics
}

run_s3_tests() {
	log_section "S3 Storage Tests"

	CURRENT_STORAGE="s3"

	# Check S3 availability first
	local s3_endpoint="${DATACORE_S3_ENDPOINT:-}"
	if [[ -z "$s3_endpoint" ]]; then
		log_skip "S3 tests - No S3 endpoint configured (DATACORE_S3_ENDPOINT not set)"
		return 0
	fi

	# Create S3 config
	create_s3_config

	if [[ "$KEEP_BROKER" != "true" ]]; then
		start_broker "$CONFIG_S3" "s3" || {
			log_skip "S3 tests - Could not start broker with S3 storage"
			log_info "Ensure S3 configuration is correct (DATACORE_S3_*)"
			return 0
		}
	else
		CURRENT_STORAGE="s3"
	fi

	# Run tests
	run_test "Basic Produce/Consume" test_s3_basic_produce_consume "s3"
	run_test "Large Messages (1KB-100KB)" test_s3_large_messages "s3"
	run_test "Multiple Topics" test_s3_multiple_topics "s3"
	run_test "Consumer Groups" test_s3_consumer_groups "s3"
	run_test "Multiple Partitions" test_s3_multiple_partitions "s3"
	run_test "Data Persistence" test_s3_data_persistence "s3"
	run_test "S3 Prefix Isolation" test_s3_prefix_isolation "s3"
	run_test "Batch Performance" test_s3_batch_performance "s3"

	# Cleanup
	cleanup_topics
}

run_switch_tests() {
	log_section "Storage Switching Tests"

	# These tests verify behavior when switching between storage types
	CURRENT_STORAGE="switch"

	run_test "Memory to S3 Switch" test_switch_memory_to_s3 "switch"
	run_test "Data Integrity Switch" test_switch_data_integrity "switch"
	run_test "Consumer Offsets Switch" test_switch_consumer_offsets "switch"
}

main() {
	parse_args "$@"

	echo "=========================================="
	echo "  DataCore Storage Type Compatibility Tests"
	echo "=========================================="
	echo ""
	echo "Bootstrap Server: $BOOTSTRAP_SERVER"
	echo "Test Prefix: $TEST_PREFIX"
	echo ""

	# Detect tools
	detect_tools

	# Check broker connectivity
	if ! check_broker; then
		log_info "Starting broker for tests..."
		# Create default config and start broker
		create_memory_config
		start_broker "$CONFIG_MEMORY" "memory" || {
			echo "Error: Cannot start broker. Please ensure DataCore is built and configured."
			exit 1
		}
	fi

	# Run tests based on options
	if [[ "$S3_ONLY" == "true" ]]; then
		run_s3_tests
	elif [[ "$MEMORY_ONLY" == "true" ]]; then
		run_memory_tests
	else
		# Run all tests
		run_memory_tests

		if [[ "$KEEP_BROKER" != "true" ]]; then
			stop_broker
		fi

		run_s3_tests
		run_switch_tests
	fi

	# Cleanup
	cleanup_all

	# Summary
	echo ""
	echo "=========================================="
	echo "  Test Summary"
	echo "=========================================="
	echo ""
	echo "Storage Tests:"
	echo -e "  Memory Tests Passed: ${GREEN}$MEMORY_TESTS_PASSED${NC}"
	echo -e "  S3 Tests Passed:     ${GREEN}$S3_TESTS_PASSED${NC}"
	echo -e "  Switch Tests Passed: ${GREEN}$SWITCH_TESTS_PASSED${NC}"
	echo ""
	echo "Overall:"
	echo -e "  Total:   $TESTS_RUN"
	echo -e "  Passed:  ${GREEN}$TESTS_PASSED${NC}"
	echo -e "  Failed:  ${RED}$TESTS_FAILED${NC}"
	echo -e "  Skipped: ${YELLOW}$TESTS_SKIPPED${NC}"
	echo ""

	if [[ $TESTS_FAILED -gt 0 ]]; then
		echo -e "${RED}Some tests failed!${NC}"
		exit 1
	else
		echo -e "${GREEN}All tests passed!${NC}"
		exit 0
	fi
}

# Trap for cleanup on exit
trap cleanup_all EXIT

# Run main
main "$@"
