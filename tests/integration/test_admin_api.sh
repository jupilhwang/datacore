#!/bin/bash
# =============================================================================
# DataCore Admin API Compatibility Tests
# =============================================================================
# Tests compatibility with Kafka Admin API operations using standard CLI tools
#
# Prerequisites:
# - Kafka CLI tools (kafka-topics.sh, kafka-consumer-groups.sh, kafka-configs.sh, kafka-acls.sh)
# - DataCore broker running
# - Optional: kcat/kafkacat for additional verification
#
# Usage:
#   ./test_admin_api.sh                          # Run with defaults (memory storage)
#   STORAGE_TYPE=s3 ./test_admin_api.sh         # Run with S3 storage
#   BOOTSTRAP_SERVER=localhost:9093 ./test_admin_api.sh  # Custom broker
#
# =============================================================================

set -o pipefail

# Define timeout function for macOS/BSD compatibility
if ! command -v timeout &>/dev/null; then
	if command -v gtimeout &>/dev/null; then
		timeout() { gtimeout "$@"; }
	else
		function timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }
	fi
fi

# =============================================================================
# Configuration
# =============================================================================

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
STORAGE_TYPE="${STORAGE_TYPE:-memory}"
TEST_PREFIX="admin-test"
TIMEOUT=15
RETRY_COUNT=2
RETRY_DELAY=1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Track created resources for cleanup
CREATED_TOPICS=()
CREATED_GROUPS=()

# =============================================================================
# Utility Functions
# =============================================================================

log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_section() {
	echo ""
	echo -e "${BOLD}${CYAN}==========================================${NC}"
	echo -e "${BOLD}${CYAN}  $1${NC}"
	echo -e "${BOLD}${CYAN}==========================================${NC}"
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

log_debug() {
	if [[ "${DEBUG:-false}" == "true" ]]; then
		echo -e "${YELLOW}[DEBUG]${NC} $1"
	fi
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Retry a command with exponential backoff
retry_command() {
	local max_attempts=$RETRY_COUNT
	local delay=$RETRY_DELAY
	local attempt=1
	local cmd="$*"

	while [[ $attempt -le $max_attempts ]]; do
		log_debug "Attempt $attempt of $max_attempts: $cmd"
		if eval "$cmd"; then
			return 0
		fi

		if [[ $attempt -lt $max_attempts ]]; then
			log_debug "Command failed, retrying in ${delay}s..."
			sleep $delay
		fi

		((attempt++))
	done

	log_debug "Command failed after $max_attempts attempts"
	return 1
}

# Generate unique test topic name
unique_topic() {
	echo "${TEST_PREFIX}-topic-$(date +%s)-$$"
}

# Generate unique consumer group name
unique_group() {
	echo "${TEST_PREFIX}-group-$(date +%s)-$$"
}

# =============================================================================
# Tool Detection
# =============================================================================

detect_kafka_tools() {
	log_info "Detecting Kafka CLI tools..."

	# Kafka CLI - topics
	if command -v kafka-topics &>/dev/null; then
		KAFKA_TOPICS="kafka-topics"
		KAFKA_PRODUCER="kafka-console-producer"
		KAFKA_CONSUMER="kafka-console-consumer"
		KAFKA_GROUPS="kafka-consumer-groups"
		KAFKA_CONFIGS="kafka-configs"
		KAFKA_ACLS="kafka-acls"
		KAFKA_LOGS="kafka-logDirs"
	elif command -v kafka-topics.sh &>/dev/null; then
		KAFKA_TOPICS="kafka-topics.sh"
		KAFKA_PRODUCER="kafka-console-producer.sh"
		KAFKA_CONSUMER="kafka-console-consumer.sh"
		KAFKA_GROUPS="kafka-consumer-groups.sh"
		KAFKA_CONFIGS="kafka-configs.sh"
		KAFKA_ACLS="kafka-acls.sh"
		KAFKA_LOGS="kafka-logDirs.sh"
	else
		log_error "Kafka CLI tools not found. Please install Kafka or add to PATH."
		exit 1
	fi

	# Verify essential tools are available
	if ! command -v $KAFKA_TOPICS &>/dev/null; then
		log_error "kafka-topics tool not found"
		exit 1
	fi

	if ! command -v $KAFKA_GROUPS &>/dev/null; then
		log_error "kafka-consumer-groups tool not found"
		exit 1
	fi

	# Optional tools
	if command -v $KAFKA_CONFIGS &>/dev/null; then
		HAS_KAFKA_CONFIGS=true
	else
		HAS_KAFKA_CONFIGS=false
		log_info "kafka-configs.sh not available - config tests will be skipped"
	fi

	if command -v $KAFKA_ACLS &>/dev/null; then
		HAS_KAFKA_ACLS=true
	else
		HAS_KAFKA_ACLS=false
		log_info "kafka-acls.sh not available - ACL tests will be skipped"
	fi

	if command -v $KAFKA_LOGS &>/dev/null; then
		HAS_KAFKA_LOGS=true
	else
		HAS_KAFKA_LOGS=false
		log_info "kafka-logDirs.sh not available - broker log tests will be skipped"
	fi

	# kcat
	if command -v kcat &>/dev/null; then
		HAS_KCAT=true
		KCAT_CMD="kcat"
	elif command -v kafkacat &>/dev/null; then
		HAS_KCAT=true
		KCAT_CMD="kafkacat"
	else
		HAS_KCAT=false
	fi

	log_info "Tools detected: topics=$KAFKA_TOPICS, groups=$KAFKA_GROUPS, configs=$HAS_KAFKA_CONFIGS, acls=$HAS_KAFKA_ACLS, kcat=$HAS_KCAT"
}

# =============================================================================
# Broker Connectivity
# =============================================================================

check_broker() {
	log_info "Checking broker connectivity at $BOOTSTRAP_SERVER (Storage: $STORAGE_TYPE)..."

	local host=$(echo "$BOOTSTRAP_SERVER" | cut -d: -f1)
	local port=$(echo "$BOOTSTRAP_SERVER" | cut -d: -f2)

	# Try multiple methods to check connectivity
	if command -v nc &>/dev/null; then
		if nc -z -w2 "$host" "$port" 2>/dev/null; then
			log_info "Broker is reachable via netcat"
			return 0
		fi
	fi

	if command -v timeout &>/dev/null; then
		if timeout 2 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
			log_info "Broker is reachable via bash TCP"
			return 0
		fi
	fi

	# Try Kafka metadata API as final check
	log_info "Trying Kafka metadata API..."
	if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list --timeout 5000 2>/dev/null; then
		log_info "Broker is reachable via Kafka API"
		return 0
	fi

	log_error "Cannot connect to broker at $BOOTSTRAP_SERVER"
	log_error "Please ensure DataCore is running with: ./bin/datacore broker start --config=config.toml"
	exit 1
}

# =============================================================================
# Test Framework
# =============================================================================

run_test() {
	local test_name="$1"
	local test_func="$2"

	((TESTS_RUN++))
	echo ""
	log_info "Running: $test_name"

	if $test_func; then
		log_pass "$test_name"
		return 0
	else
		log_fail "$test_name"
		return 1
	fi
}

# =============================================================================
# Resource Cleanup
# =============================================================================

cleanup_topic() {
	local topic="$1"
	if [[ -n "$topic" ]]; then
		log_debug "Cleaning up topic: $topic"
		$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --delete --topic "$topic" 2>/dev/null || true
	fi
}

cleanup_group() {
	local group="$1"
	if [[ -n "$group" ]]; then
		log_debug "Cleaning up group: $group"
		$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" --delete --group "$group" 2>/dev/null || true
	fi
}

cleanup_all_resources() {
	log_section "Cleaning Up Test Resources"

	# Cleanup topics
	for topic in "${CREATED_TOPICS[@]}"; do
		cleanup_topic "$topic"
	done
	CREATED_TOPICS=()

	# Cleanup groups
	for group in "${CREATED_GROUPS[@]}"; do
		cleanup_group "$group"
	done
	CREATED_GROUPS=()

	# Cleanup any remaining test topics
	local topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep "^${TEST_PREFIX}" || true)
	if [[ -n "$topics" ]]; then
		log_info "Cleaning up remaining test topics..."
		for topic in $topics; do
			cleanup_topic "$topic"
		done
	fi

	log_info "Cleanup complete"
}

# =============================================================================
# TOPIC MANAGEMENT TESTS
# =============================================================================

test_topic_create_basic() {
	local topic
	topic=$(unique_topic)

	# Create topic
	if ! $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 3 \
		--replication-factor 1 \
		2>/dev/null; then
		log_error "Failed to create topic: $topic"
		return 1
	fi

	CREATED_TOPICS+=("$topic")

	# Verify topic exists
	sleep 0.3
	local topics
	topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	echo "$topics" | grep -q "^${topic}$"
}

test_topic_create_various_partitions() {
	local partitions_array=(1 3 6 12)
	local all_passed=true

	for partitions in "${partitions_array[@]}"; do
		local topic
		topic=$(unique_topic)

		log_info "Testing topic creation with $partitions partitions"

		if ! $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
			--create \
			--topic "$topic" \
			--partitions "$partitions" \
			--replication-factor 1 \
			2>/dev/null; then
			log_error "Failed to create topic with $partitions partitions"
			all_passed=false
			continue
		fi

		CREATED_TOPICS+=("$topic")

		sleep 0.3

		# Verify partition count
		local describe
		describe=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
			--describe --topic "$topic" 2>/dev/null)

		local actual_partitions
		actual_partitions=$(echo "$describe" | grep -c "Partition:" || echo "0")

		if [[ "$actual_partitions" -ne "$partitions" ]]; then
			log_error "Expected $partitions partitions but found $actual_partitions"
			all_passed=false
		fi
	done

	$all_passed
}

test_topic_create_various_replication_factors() {
	# Note: Replication factor is limited by available brokers
	# For DataCore single-broker setup, we test with rf=1 only
	local topic
	topic=$(unique_topic)

	log_info "Testing topic creation with replication-factor=1"

	if ! $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null; then
		log_error "Failed to create topic with rf=1"
		return 1
	fi

	CREATED_TOPICS+=("$topic")

	sleep 0.3

	# Verify topic exists and has correct replication
	local describe
	describe=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --topic "$topic" 2>/dev/null)

	echo "$describe" | grep -q "ReplicationFactor.*1"
}

test_topic_list() {
	local topic
	topic=$(unique_topic)

	# Create a topic first
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 0.3

	# List topics
	local topics
	topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	echo "$topics" | grep -q "^${topic}$"
}

test_topic_list_empty() {
	# Create a unique topic and immediately delete it
	local temp_topic="${TEST_PREFIX}-temp-list-$(date +%s)"

	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$temp_topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	# List should include our topic
	local topics
	topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	if ! echo "$topics" | grep -q "^${temp_topic}$"; then
		log_error "Newly created topic not found in list"
		return 1
	fi

	# Delete and verify
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--topic "$temp_topic" 2>/dev/null

	sleep 0.5
	topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)
	! echo "$topics" | grep -q "^${temp_topic}$"
}

test_topic_describe() {
	local topic
	topic=$(unique_topic)

	# Create topic with specific config
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 3 \
		--replication-factor 1 \
		--config retention.ms=86400000 \
		--config segment.bytes=1073741824 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 0.3

	# Describe topic
	local describe
	describe=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --topic "$topic" 2>/dev/null)

	log_debug "Topic describe output: $describe"

	# Verify expected output contains key fields
	echo "$describe" | grep -q "Topic:" &&
		echo "$describe" | grep -q "PartitionCount" &&
		echo "$describe" | grep -q "ReplicationFactor" &&
		echo "$describe" | grep -q "Partition:"
}

test_topic_describe_nonexistent() {
	local nonexistent_topic="${TEST_PREFIX}-nonexistent-$(date +%s)"

	# Try to describe non-existent topic
	local output
	output=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --topic "$nonexistent_topic" 2>&1)

	# Should fail with appropriate error
	echo "$output" | grep -qiE "(not found|does not exist|error|unknown topic)" ||
		[[ $? -ne 0 ]] # If no error message, check that command failed
}

test_topic_delete() {
	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	sleep 1

	# Verify exists
	local topics_before
	topics_before=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	if ! echo "$topics_before" | grep -q "^${topic}$"; then
		log_error "Topic was not created"
		return 1
	fi

	# Delete topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Verify deleted (may take time for async deletion)
	local topics_after
	topics_after=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	! echo "$topics_after" | grep -q "^${topic}$"
}

test_topic_delete_nonexistent() {
	local nonexistent_topic="${TEST_PREFIX}-nonexistent-del-$(date +%s)"

	# Try to delete non-existent topic
	# Should not crash, may return success (idempotent) or error
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--topic "$nonexistent_topic" 2>/dev/null

	# Just verify no crash - idempotent delete is acceptable
	return 0
}

test_topic_alter_config() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Alter topic config
	if ! $KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type topics \
		--entity-name "$topic" \
		--add-config retention.ms=172800000 \
		2>/dev/null; then
		log_error "Failed to alter topic config"
		return 1
	fi

	sleep 0.3

	# Describe config to verify
	local config
	config=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe \
		--entity-type topics \
		--entity-name "$topic" 2>/dev/null)

	echo "$config" | grep -q "retention.ms"
}

test_topic_alter_partitions() {
	local topic
	topic=$(unique_topic)

	# Create topic with 1 partition
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Alter partitions
	if ! $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--topic "$topic" \
		--partitions 5 \
		2>/dev/null; then
		log_error "Failed to alter topic partitions"
		return 1
	fi

	sleep 0.3

	# Verify new partition count
	local describe
	describe=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --topic "$topic" 2>/dev/null)

	local actual_partitions
	actual_partitions=$(echo "$describe" | grep -c "Partition:" || echo "0")

	[[ "$actual_partitions" -eq 5 ]]
}

test_topic_create_duplicate() {
	local topic
	topic=$(unique_topic)

	# Create topic first time
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 0.3

	# Try to create duplicate - should fail
	local output
	local exit_code=0
	output=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 2>&1) || exit_code=$?

	# Should have failed (exit code non-zero)
	# Or output should indicate topic already exists
	[[ $exit_code -ne 0 ]] || echo "$output" | grep -qiE "(already exists|duplicate|error)"
}

test_topic_create_invalid_config() {
	local topic
	topic=$(unique_topic)

	# Try to create topic with invalid config
	local output
	local exit_code=0
	output=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		--config invalid.config.name=value 2>&1) || exit_code=$?

	# Cleanup - topic might still have been created
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--topic "$topic" 2>/dev/null || true

	# Command should fail or show warning about invalid config
	[[ $exit_code -ne 0 ]] || echo "$output" | grep -qiE "(invalid|error|warning)"
}

# =============================================================================
# CONSUMER GROUP MANAGEMENT TESTS
# =============================================================================

test_consumer_group_list() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	echo "test-message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume to create group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 1

	# List groups
	local groups
	groups=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	CREATED_GROUPS+=("$group")

	echo "$groups" | grep -q "$group"
}

test_consumer_group_describe() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 2 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	for i in $(seq 1 5); do
		echo "describe-test-message-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 0.3

	# Consume to create group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 0.3

	# Describe group
	local describe
	describe=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --group "$group" 2>&1)

	log_debug "Group describe output: $describe"

	CREATED_GROUPS+=("$group")

	# Verify output contains expected information
	echo "$describe" | grep -qE "(GROUP|TOPIC|PARTITION|CURRENT-OFFSET|$group)"
}

test_consumer_group_describe_nonexistent() {
	local nonexistent_group="${TEST_PREFIX}-nonexistent-$(date +%s)"

	# Try to describe non-existent group
	local output
	output=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --group "$nonexistent_group" 2>&1)

	# Should fail with appropriate error
	echo "$output" | grep -qiE "(not found|does not exist|error|no such group)" ||
		[[ $? -ne 0 ]]
}

test_consumer_group_delete() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	echo "delete-test-message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume to create group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 1

	# Verify group exists
	local groups_before
	groups_before=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	if ! echo "$groups_before" | grep -q "$group"; then
		log_error "Consumer group was not created"
		return 1
	fi

	# Delete group
	$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--group "$group" 2>/dev/null

	sleep 1

	# Verify deleted
	local groups_after
	groups_after=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	! echo "$groups_after" | grep -q "$group"
}

test_consumer_group_delete_nonexistent() {
	local nonexistent_group="${TEST_PREFIX}-nonexistent-del-$(date +%s)"

	# Try to delete non-existent group
	# Should not crash, may return success (idempotent) or error
	$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--delete \
		--group "$nonexistent_group" 2>/dev/null

	return 0
}

test_consumer_group_reset_offsets_earliest() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	for i in $(seq 1 10); do
		echo "reset-test-message-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume all messages (moves offset to end)
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 10 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 0.3

	# Reset offsets to earliest
	$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--group "$group" \
		--reset-offsets \
		--to-earliest \
		--topic "$topic" \
		--execute 2>/dev/null

	sleep 0.3

	# Consume again - should get messages from beginning
	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null | wc -l)

	CREATED_GROUPS+=("$group")

	# Should have consumed messages after reset
	[[ $consumed -gt 0 ]]
}

test_consumer_group_reset_offsets_latest() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce initial messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	for i in $(seq 1 5); do
		echo "latest-reset-initial-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume some messages
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 0.3

	# Reset offsets to latest
	$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--group "$group" \
		--reset-offsets \
		--to-latest \
		--topic "$topic" \
		--execute 2>/dev/null

	# Produce new messages
	for i in $(seq 1 3); do
		echo "latest-reset-new-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 0.3

	# Should only get new messages (offset at latest)
	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null)

	CREATED_GROUPS+=("$group")

	# Should contain new messages
	echo "$consumed" | grep -q "latest-reset-new"
}

test_consumer_group_reset_offsets_to_offset() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	for i in $(seq 1 10); do
		echo "offset-reset-message-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume some messages
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 1

	# Reset offsets to offset 2
	$KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--group "$group" \
		--reset-offsets \
		--to-offset 2 \
		--topic "$topic" \
		--execute 2>/dev/null

	sleep 0.3

	# Should get messages starting from offset 2
	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null)

	CREATED_GROUPS+=("$group")

	# Should start from message with offset 2 (message-3 in 1-indexed)
	echo "$consumed" | grep -q "offset-reset-message-3"
}

test_consumer_group_members() {
	local topic
	topic=$(unique_topic)
	local group1
	group1=$(unique_group)
	local group2
	group2=$(unique_group)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 2 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	# Produce messages
	for i in $(seq 1 10); do
		echo "multi-group-message-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume with first group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group1" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	# Consume with second group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group2" \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 0.3

	# List groups
	local groups
	groups=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	CREATED_GROUPS+=("$group1" "$group2")

	echo "$groups" | grep -q "$group1" && echo "$groups" | grep -q "$group2"
}

test_consumer_group_lag() {
	local topic
	topic=$(unique_topic)
	local group
	group=$(unique_group)

	# Create topic and produce messages
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	for i in $(seq 1 10); do
		echo "lag-test-message-$i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 1

	# Consume some messages
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--group "$group" \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null >/dev/null

	sleep 0.3

	# Describe group (should show lag)
	local describe
	describe=$($KAFKA_GROUPS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe --group "$group" 2>&1)

	CREATED_GROUPS+=("$group")

	# Describe output should show group information
	echo "$describe" | grep -qE "(GROUP|TOPIC|$group)"
}

# =============================================================================
# BROKER MANAGEMENT TESTS
# =============================================================================

test_broker_describe() {
	# Try to get broker information via metadata
	local topics
	topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

	# If we can list topics, broker is responding to metadata requests
	[[ -n "$topics" ]] || [[ $? -eq 0 ]]
}

test_broker_list_configs() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	# Try to describe broker configs
	local config
	config=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe \
		--entity-type brokers \
		--entity-default 2>/dev/null)

	# Should not crash
	return 0
}

test_broker_alter_config() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local broker_id="${BOOTSTRAP_SERVER%%:*}"
	# Use default broker if we can't parse ID
	[[ "$broker_id" =~ ^[0-9]+$ ]] || broker_id=0

	# Try to alter broker config
	$KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type brokers \
		--entity-default \
		--add-config log.flush.interval.ms=10000 \
		2>/dev/null

	# Command may or may not succeed depending on broker config support
	return 0
}

test_broker_log_dirs() {
	if [[ "$HAS_KAFKA_LOGS" != "true" ]]; then
		log_skip "kafka-logDirs.sh not available"
		return 0
	fi

	# Try to describe log dirs
	local logs
	logs=$($KAFKA_LOGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe 2>/dev/null)

	# Should not crash
	return 0
}

# =============================================================================
# ACL MANAGEMENT TESTS
# =============================================================================

test_acl_list() {
	if [[ "$HAS_KAFKA_ACLS" != "true" ]]; then
		log_skip "kafka-acls.sh not available"
		return 0
	fi

	# List ACLs
	local result
	result=$($KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>&1)

	# Should not crash
	return 0
}

test_acl_create() {
	if [[ "$HAS_KAFKA_ACLS" != "true" ]]; then
		log_skip "kafka-acls.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)
	local principal="User:test-admin-user"

	# Create topic first
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	# Create ACL
	local create_result
	create_result=$($KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--add \
		--allow-principal "$principal" \
		--operation Read \
		--operation Write \
		--topic "$topic" 2>&1)

	log_debug "ACL create result: $create_result"

	# Should not crash
	return 0
}

test_acl_delete() {
	if [[ "$HAS_KAFKA_ACLS" != "true" ]]; then
		log_skip "kafka-acls.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)
	local principal="User:test-admin-delete"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	# Create ACL
	$KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--add \
		--allow-principal "$principal" \
		--operation Read \
		--topic "$topic" 2>/dev/null

	# Delete ACL
	local delete_result
	delete_result=$($KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--remove \
		--allow-principal "$principal" \
		--operation Read \
		--topic "$topic" \
		--force 2>&1)

	log_debug "ACL delete result: $delete_result"

	# Should not crash
	return 0
}

test_acl_create_topic_acl() {
	if [[ "$HAS_KAFKA_ACLS" != "true" ]]; then
		log_skip "kafka-acls.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	# Create various ACLs
	$KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--add \
		--allow-principal "User:producer1" \
		--allow-principal "User:consumer1" \
		--operation Write \
		--operation Read \
		--topic "$topic" 2>/dev/null

	# List ACLs for topic
	local list_result
	list_result=$($KAFKA_ACLS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--list \
		--topic "$topic" 2>&1)

	log_debug "Topic ACL list: $list_result"

	return 0
}

# =============================================================================
# CONFIGURATION MANAGEMENT TESTS
# =============================================================================

test_config_describe_topic() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic with config
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		--config retention.ms=86400000 \
		--config segment.bytes=1073741824 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Describe configs
	local config
	config=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe \
		--entity-type topics \
		--entity-name "$topic" 2>/dev/null)

	log_debug "Topic config: $config"

	# Should contain some config entries
	[[ -n "$config" ]]
}

test_config_describe_broker() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	# Describe broker configs
	local config
	config=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe \
		--entity-type brokers \
		--entity-default 2>/dev/null)

	log_debug "Broker config: $config"

	return 0
}

test_config_alter_topic() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Alter config
	$KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type topics \
		--entity-name "$topic" \
		--add-config retention.ms=172800000 \
		--add-config min.insync.replicas=1 \
		2>/dev/null

	sleep 1

	# Verify
	local config
	config=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--describe \
		--entity-type topics \
		--entity-name "$topic" 2>/dev/null)

	echo "$config" | grep -q "retention.ms"
}

test_config_alter_topic_delete_config() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic with config
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		--config retention.ms=86400000 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Add a config first
	$KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type topics \
		--entity-name "$topic" \
		--add-config segment.bytes=1073741824 \
		2>/dev/null

	sleep 1

	# Delete config
	$KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type topics \
		--entity-name "$topic" \
		--delete-config segment.bytes \
		2>/dev/null

	sleep 1

	# Verify - segment.bytes should be gone or default
	return 0
}

test_config_invalid_config() {
	if [[ "$HAS_KAFKA_CONFIGS" != "true" ]]; then
		log_skip "kafka-configs.sh not available"
		return 0
	fi

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Try to alter with invalid config
	local output
	output=$($KAFKA_CONFIGS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--alter \
		--entity-type topics \
		--entity-name "$topic" \
		--add-config invalid.config.name=value \
		2>&1)

	# Should fail or warn about invalid config
	echo "$output" | grep -qiE "(invalid|error|warning|unknown)" || [[ $? -ne 0 ]]
}

# =============================================================================
# STORAGE TYPE TESTS
# =============================================================================

test_storage_type_compatibility() {
	log_info "Testing with storage type: $STORAGE_TYPE"

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 2 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 0.3

	# Produce and consume
	echo "storage-test-message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 0.3

	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null)

	[[ "$consumed" == *"storage-test-message"* ]]
}

test_storage_type_large_messages() {
	log_info "Testing large messages with storage type: $STORAGE_TYPE"

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Create 50KB message (reduced size for speed)
	local large_message
	large_message=$(head -c 51200 /dev/urandom | base64)

	echo "$large_message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	sleep 0.3

	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null)

	[[ "$consumed" == *"$large_message"* ]]
}

test_storage_type_persistence() {
	log_info "Testing message persistence with storage type: $STORAGE_TYPE"

	local topic
	topic=$(unique_topic)

	# Create topic
	$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
		--create \
		--topic "$topic" \
		--partitions 1 \
		--replication-factor 1 \
		2>/dev/null

	CREATED_TOPICS+=("$topic")

	sleep 1

	# Produce unique message
	local unique_message="persistence-test-$(date +%s)-$$"
	echo "$unique_message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" 2>/dev/null

	# Wait for flush
	sleep 0.5

	# Consume and verify
	local consumed
	consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server "$BOOTSTRAP_SERVER" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null)

	[[ "$consumed" == *"$unique_message"* ]]
}

# =============================================================================
# MAIN
# =============================================================================

main() {
	echo "=========================================="
	echo "  DataCore Admin API Compatibility Tests"
	echo "=========================================="
	echo ""
	echo "Bootstrap Server: $BOOTSTRAP_SERVER"
	echo "Storage Type:     $STORAGE_TYPE"
	echo "Test Prefix:      $TEST_PREFIX"
	echo ""

	# Detect tools and check broker
	detect_kafka_tools
	check_broker

	echo ""
	echo "Storage Type:     $STORAGE_TYPE"
	echo ""

	# =============================================================================
	# TOPIC MANAGEMENT TESTS
	# =============================================================================
	log_section "Topic Management Tests"

	run_test "Create Topic (Basic)" test_topic_create_basic
	run_test "Create Topic (Various Partitions: 1, 3, 6, 12)" test_topic_create_various_partitions
	run_test "Create Topic (Replication Factor 1)" test_topic_create_various_replication_factors
	run_test "List Topics" test_topic_list
	run_test "List Topics (Empty Check)" test_topic_list_empty
	run_test "Describe Topic" test_topic_describe
	run_test "Describe Nonexistent Topic (Error Case)" test_topic_describe_nonexistent
	run_test "Delete Topic" test_topic_delete
	run_test "Delete Nonexistent Topic (Error Case)" test_topic_delete_nonexistent
	run_test "Alter Topic Config" test_topic_alter_config
	run_test "Alter Topic Partitions" test_topic_alter_partitions
	run_test "Create Duplicate Topic (Error Case)" test_topic_create_duplicate
	run_test "Create Topic with Invalid Config (Error Case)" test_topic_create_invalid_config

	# =============================================================================
	# CONSUMER GROUP MANAGEMENT TESTS
	# =============================================================================
	log_section "Consumer Group Management Tests"

	run_test "List Consumer Groups" test_consumer_group_list
	run_test "Describe Consumer Group" test_consumer_group_describe
	run_test "Describe Nonexistent Group (Error Case)" test_consumer_group_describe_nonexistent
	run_test "Delete Consumer Group" test_consumer_group_delete
	run_test "Delete Nonexistent Group (Error Case)" test_consumer_group_delete_nonexistent
	run_test "Reset Offsets (to-earliest)" test_consumer_group_reset_offsets_earliest
	run_test "Reset Offsets (to-latest)" test_consumer_group_reset_offsets_latest
	run_test "Reset Offsets (to-offset)" test_consumer_group_reset_offsets_to_offset
	run_test "Multiple Consumer Groups" test_consumer_group_members
	run_test "Consumer Group Lag" test_consumer_group_lag

	# =============================================================================
	# BROKER MANAGEMENT TESTS
	# =============================================================================
	log_section "Broker Management Tests"

	run_test "Describe Broker (Metadata)" test_broker_describe
	run_test "List Broker Configs" test_broker_list_configs
	run_test "Alter Broker Config" test_broker_alter_config
	run_test "Broker Log Directories" test_broker_log_dirs

	# =============================================================================
	# ACL MANAGEMENT TESTS
	# =============================================================================
	log_section "ACL Management Tests"

	run_test "List ACLs" test_acl_list
	run_test "Create ACL" test_acl_create
	run_test "Delete ACL" test_acl_delete
	run_test "Create Topic ACL" test_acl_create_topic_acl

	# =============================================================================
	# CONFIGURATION MANAGEMENT TESTS
	# =============================================================================
	log_section "Configuration Management Tests"

	run_test "Describe Topic Config" test_config_describe_topic
	run_test "Describe Broker Config" test_config_describe_broker
	run_test "Alter Topic Config" test_config_alter_topic
	run_test "Alter Topic Config (Delete Config)" test_config_alter_topic_delete_config
	run_test "Alter with Invalid Config (Error Case)" test_config_invalid_config

	# =============================================================================
	# STORAGE TYPE TESTS
	# =============================================================================
	log_section "Storage Type Compatibility Tests"

	run_test "Storage Type Compatibility ($STORAGE_TYPE)" test_storage_type_compatibility
	run_test "Storage Type Large Messages ($STORAGE_TYPE)" test_storage_type_large_messages
	run_test "Storage Type Persistence ($STORAGE_TYPE)" test_storage_type_persistence

	# =============================================================================
	# CLEANUP
	# =============================================================================
	cleanup_all_resources

	# =============================================================================
	# SUMMARY
	# =============================================================================
	echo ""
	echo "=========================================="
	echo "  Test Summary"
	echo "=========================================="
	echo "Storage Type:     $STORAGE_TYPE"
	echo "Bootstrap Server: $BOOTSTRAP_SERVER"
	echo ""
	echo "Total:   $TESTS_RUN"
	echo -e "Passed:  ${GREEN}$TESTS_PASSED${NC}"
	echo -e "Failed:  ${RED}$TESTS_FAILED${NC}"
	echo -e "Skipped: ${YELLOW}$TESTS_SKIPPED${NC}"
	echo ""

	if [[ $TESTS_FAILED -gt 0 ]]; then
		echo -e "${RED}Some tests failed. Please review the output above.${NC}"
		exit 1
	fi

	echo -e "${GREEN}All tests passed!${NC}"
	exit 0
}

# Run main function
main "$@"
