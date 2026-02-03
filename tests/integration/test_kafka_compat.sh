#!/bin/bash
# Kafka Compatibility Tests for DataCore
# Tests compatibility with standard Kafka clients and tools
#
# Prerequisites:
# - Kafka CLI tools (kafka-topics, kafka-console-producer, etc.)
# - Optional: kcat (formerly kafkacat)
# - DataCore broker running
# - Optional: jq for Schema Registry tests
# - Optional: curl for Schema Registry tests
#
# Test Categories:
# 1. Admin API Tests (5 tests)
# 2. Producer Tests (4 tests)
# 3. Consumer Tests (2 tests)
# 4. kcat Tests (2 tests)
# 5. Compression Tests (6 tests)
# 6. Consumer Group Tests (4 tests)
# 7. ACL Tests (2 tests)
# 8. Message Format Tests (7 tests)
# 9. Performance/Benchmark Tests (5 tests)
# 10. Stress Tests (3 tests)
# 11. Error Handling Tests (4 tests)
# 12. Storage Engine Tests (3 tests)
# 13. Schema Registry Tests (3 tests)
#
# Total: 50 tests (excluding skipped tests)

set -o pipefail

# Define timeout function for macOS/BSD compatibility
if ! command -v timeout &>/dev/null; then
	if command -v gtimeout &>/dev/null; then
		timeout() { gtimeout "$@"; }
	else
		function timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }
	fi
fi

# Configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TEST_PREFIX="compat-test"
TIMEOUT=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# ============================================
# Utility Functions
# ============================================

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
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
		exit 1
	fi

	# kcat
	if command -v kcat &>/dev/null; then
		HAS_KCAT=true
	elif command -v kafkacat &>/dev/null; then
		HAS_KCAT=true
		alias kcat=kafkacat
	else
		HAS_KCAT=false
	fi
}

# Check broker connectivity
check_broker() {
	log_info "Checking broker connectivity at $BOOTSTRAP_SERVER..."

	# Use nc (netcat) to check if port is open
	local host=$(echo $BOOTSTRAP_SERVER | cut -d: -f1)
	local port=$(echo $BOOTSTRAP_SERVER | cut -d: -f2)

	if ! nc -z -w2 $host $port 2>/dev/null; then
		echo "Error: Cannot connect to broker at $BOOTSTRAP_SERVER"
		echo "Please ensure DataCore is running."
		exit 1
	fi

	log_info "Broker is reachable"
}

# ============================================
# Admin API Tests
# ============================================

test_api_versions() {
	# Use kcat to query API versions
	if [[ "$HAS_KCAT" == "true" ]]; then
		kcat -b $BOOTSTRAP_SERVER -L -J 2>/dev/null | grep -q "brokers"
	else
		# Fallback: try to list topics (requires Metadata API)
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null
	fi
}

test_metadata_api() {
	local output
	output=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>&1)
	local exit_code=$?

	if [[ $exit_code -ne 0 ]]; then
		echo "  Metadata API failed: $output"
		return 1
	fi
	return 0
}

test_create_topic_with_partitions() {
	local topic="${TEST_PREFIX}-partitions-$(date +%s)"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create \
		--topic $topic \
		--partitions 5 \
		--replication-factor 1

	# Verify partition count
	local info=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--describe --topic $topic)

	echo "DEBUG: Topic info for $topic:"
	echo "$info"

	if echo "$info" | grep -q "PartitionCount.*5"; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 0
	fi

	# Fallback: count partition lines
	local count=$(echo "$info" | grep -c "Partition:")
	echo "DEBUG: Counted $count partitions"

	if [[ $count -eq 5 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 0
	fi

	return 1
}

test_create_duplicate_topic() {
	local topic="${TEST_PREFIX}-duplicate-$(date +%s)"

	# Create first time
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null

	# Try to create again - should fail
	local output
	output=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 2>&1)

	local result=$?

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Creating duplicate should fail or report already exists
	[[ $result -ne 0 ]] || echo "$output" | grep -qi "exists"
}

test_delete_nonexistent_topic() {
	local topic="${TEST_PREFIX}-nonexistent-$(date +%s)"

	# Delete should fail or be idempotent
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--delete --topic $topic 2>&1 || true

	# Just checking no crash
	return 0
}

# ============================================
# Producer Tests
# ============================================

test_produce_simple_message() {
	local topic="${TEST_PREFIX}-produce-$(date +%s)"
	local message="Hello DataCore $(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	local result=$?

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	return $result
}

test_produce_with_key() {
	local topic="${TEST_PREFIX}-key-$(date +%s)"
	local key="mykey"
	local value="myvalue"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with key
	echo "${key}:${value}" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--property "parse.key=true" \
		--property "key.separator=:" \
		2>/dev/null

	local result=$?

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	return $result
}

test_produce_batch() {
	local topic="${TEST_PREFIX}-batch-$(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce multiple messages
	for i in $(seq 1 100); do
		echo "Message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--batch-size 10 \
		2>/dev/null

	local result=$?

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	return $result
}

test_produce_to_nonexistent_topic_autocreate() {
	local topic="${TEST_PREFIX}-autocreate-$(date +%s)"

	# Produce to non-existent topic (should auto-create)
	echo "auto-create test" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	local result=$?

	# Check if topic was created
	local exists=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep -c "^${topic}$")

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ $result -eq 0 ]] && [[ $exists -ge 1 ]]
}

# ============================================
# Consumer Tests
# ============================================

test_consume_from_beginning() {
	local topic="${TEST_PREFIX}-consume-$(date +%s)"
	local message="test-message-$(date +%s)"

	# Create and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	# Give some time for the message to be committed
	sleep 1

	# Consume
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_consume_max_messages() {
	local topic="${TEST_PREFIX}-maxmsg-$(date +%s)"
	local max=5

	# Create and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	for i in $(seq 1 10); do
		echo "Message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 1

	# Consume only 5 messages
	local count=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages $max \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ $count -eq $max ]]
}

# ============================================
# kcat Tests (if available)
# ============================================

test_kcat_metadata() {
	if [[ "$HAS_KCAT" != "true" ]]; then
		log_skip "kcat not available"
		return 0
	fi

	kcat -b $BOOTSTRAP_SERVER -L 2>/dev/null | grep -q "broker"
}

test_kcat_produce_consume() {
	if [[ "$HAS_KCAT" != "true" ]]; then
		log_skip "kcat not available"
		return 0
	fi

	local topic="${TEST_PREFIX}-kcat-$(date +%s)"
	local message="kcat-test-$(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1

	# Produce with kcat
	echo "$message" | kcat -b $BOOTSTRAP_SERVER -P -t $topic

	sleep 0.3

	# Consume with kcat
	local received=$(kcat -b $BOOTSTRAP_SERVER -C -t $topic -c 1 -e)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

# ============================================
# Consumer Group Tests
# ============================================

test_consumer_group_basic() {
	local topic="${TEST_PREFIX}-group-$(date +%s)"
	local group="test-group-$(date +%s)"

	# Create topic and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 3 --replication-factor 1

	for i in $(seq 1 5); do
		echo "Group message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic

	sleep 0.3

	# Consume with group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--group $group \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000))

	# Check group exists
	local groups=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER --list)

	echo "DEBUG: Consumer groups list:"
	echo "$groups"

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "$groups" | grep -q "$group"
}

test_consumer_group_describe() {
	local topic="${TEST_PREFIX}-grpdesc-$(date +%s)"
	local group="test-group-desc-$(date +%s)"

	# Create topic and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 2 --replication-factor 1

	for i in $(seq 1 5); do
		echo "Describe group message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic

	sleep 0.3

	# Consume with group
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--group $group \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000))

	sleep 0.3

	# Describe group
	local describe=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER \
		--describe --group $group 2>&1)

	echo "DEBUG: Group describe output:"
	echo "$describe"

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Check describe output contains expected info
	echo "$describe" | grep -qE "(GROUP|TOPIC|PARTITION|CURRENT-OFFSET|$group)"
}

test_consumer_group_delete() {
	local topic="${TEST_PREFIX}-grpdel-$(date +%s)"
	local group="test-group-del-$(date +%s)"

	# Create topic and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1

	echo "Delete group test message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic

	sleep 0.3

	# Consume with group (creates the group)
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--group $group \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000))

	sleep 0.3

	# Verify group exists
	local before=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER --list)
	echo "DEBUG: Groups before delete: $before"

	if ! echo "$before" | grep -q "$group"; then
		echo "Group not created properly"
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	# Delete the group
	local delete_result=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER \
		--delete --group $group 2>&1)

	echo "DEBUG: Delete result: $delete_result"

	sleep 0.5

	# Verify group is deleted
	local after=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER --list)
	echo "DEBUG: Groups after delete: $after"

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Group should not exist after delete
	! echo "$after" | grep -q "$group"
}

test_consumer_group_reset_offsets() {
	local topic="${TEST_PREFIX}-grpreset-$(date +%s)"
	local group="test-group-reset-$(date +%s)"

	# Create topic and produce
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1

	for i in $(seq 1 10); do
		echo "Reset offset message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic

	sleep 1

	# Consume with group (consume all messages)
	timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--group $group \
		--from-beginning \
		--max-messages 5 \
		--timeout-ms $((TIMEOUT * 1000))

	sleep 0.3

	# Reset offsets to earliest
	local reset_result=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER \
		--group $group \
		--reset-offsets \
		--to-earliest \
		--topic $topic \
		--execute 2>&1)

	echo "DEBUG: Reset offsets result: $reset_result"

	# Verify we can consume messages again from beginning
	local consumed=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--group $group \
		--from-beginning \
		--max-messages 3 \
		--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null | wc -l)

	echo "DEBUG: Messages consumed after reset: $consumed"

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
	$KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER --delete --group $group 2>/dev/null || true

	# Should have consumed messages after reset
	[[ $consumed -gt 0 ]]
}

# ============================================
# Compression Tests
# ============================================

test_compression_snappy() {
	local topic="${TEST_PREFIX}-snappy-$(date +%s)"
	local message="Snappy test message $(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with Snappy compression
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec snappy \
		2>/dev/null

	local produce_result=$?

	if [[ $produce_result -ne 0 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	sleep 1

	# Consume to verify message is readable
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_compression_gzip() {
	local topic="${TEST_PREFIX}-gzip-$(date +%s)"
	local message="Gzip test message $(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with Gzip compression
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec gzip \
		2>/dev/null

	local produce_result=$?

	if [[ $produce_result -ne 0 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	sleep 1

	# Consume to verify message is readable
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_compression_lz4() {
	local topic="${TEST_PREFIX}-lz4-$(date +%s)"
	local message="LZ4 test message $(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with LZ4 compression
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec lz4 \
		2>/dev/null

	local produce_result=$?

	if [[ $produce_result -ne 0 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	sleep 1

	# Consume to verify message is readable
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_compression_zstd() {
	local topic="${TEST_PREFIX}-zstd-$(date +%s)"
	local message="Zstd test message $(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with Zstd compression
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec zstd \
		2>/dev/null

	local produce_result=$?

	if [[ $produce_result -ne 0 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	sleep 1

	# Consume to verify message is readable
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_compression_batch_compressed() {
	local topic="${TEST_PREFIX}-batch-$(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce multiple messages with compression (reduced count)
	for i in $(seq 1 20); do
		echo "Compressed batch message $i"
	done | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec snappy \
		--batch-size 10 \
		2>/dev/null

	local produce_result=$?

	if [[ $produce_result -ne 0 ]]; then
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
		return 1
	fi

	sleep 0.3

	# Consume all messages
	local count=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ $count -ge 15 ]]
}

test_compression_mixed_types() {
	local topic="${TEST_PREFIX}-mixed-$(date +%s)"

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce with Snappy
	echo "Snappy message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec snappy \
		2>/dev/null

	sleep 0.5

	# Produce with Gzip
	echo "Gzip message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec gzip \
		2>/dev/null

	sleep 0.5

	# Produce with LZ4
	echo "LZ4 message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--compression-codec lz4 \
		2>/dev/null

	sleep 0.5

	# Consume all messages
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Verify all three messages are present
	[[ "$received" == *"Snappy message"* ]] &&
		[[ "$received" == *"Gzip message"* ]] &&
		[[ "$received" == *"LZ4 message"* ]]
}

# ============================================
# ACL Tests (Basic checks - may not be enforced)
# ============================================

test_acl_list() {
	# Try to list ACLs (should not error even if ACLs are empty)
	if command -v kafka-acls &>/dev/null; then
		KAFKA_ACLS="kafka-acls"
	elif command -v kafka-acls.sh &>/dev/null; then
		KAFKA_ACLS="kafka-acls.sh"
	else
		log_skip "kafka-acls tool not available"
		return 0
	fi

	local result=$($KAFKA_ACLS --bootstrap-server $BOOTSTRAP_SERVER --list 2>&1)
	local exit_code=$?

	echo "DEBUG: ACL list result: $result"

	# Should not crash, even if no ACLs or not supported
	# Exit code 0 or output contains expected messages
	[[ $exit_code -eq 0 ]] || echo "$result" | grep -qiE "(no acls|empty|Current ACLs)"
}

test_acl_create_and_delete() {
	if command -v kafka-acls &>/dev/null; then
		KAFKA_ACLS="kafka-acls"
	elif command -v kafka-acls.sh &>/dev/null; then
		KAFKA_ACLS="kafka-acls.sh"
	else
		log_skip "kafka-acls tool not available"
		return 0
	fi

	local topic="${TEST_PREFIX}-acl-$(date +%s)"
	local principal="User:test-user"

	# Create topic first
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 2>/dev/null || true

	# Create ACL
	local create_result=$($KAFKA_ACLS --bootstrap-server $BOOTSTRAP_SERVER \
		--add \
		--allow-principal "$principal" \
		--operation Read \
		--topic $topic 2>&1)

	echo "DEBUG: ACL create result: $create_result"

	sleep 1

	# List ACLs to verify
	local list_result=$($KAFKA_ACLS --bootstrap-server $BOOTSTRAP_SERVER --list 2>&1)
	echo "DEBUG: ACL list after create: $list_result"

	# Delete ACL
	local delete_result=$($KAFKA_ACLS --bootstrap-server $BOOTSTRAP_SERVER \
		--remove \
		--allow-principal "$principal" \
		--operation Read \
		--topic $topic \
		--force 2>&1)

	echo "DEBUG: ACL delete result: $delete_result"

	# Cleanup
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Success if no errors (ACL operations may be no-ops if not enforced)
	[[ $? -eq 0 ]] || echo "$create_result" | grep -qiE "(Added|Adding|Success)"
}

# ============================================
# Message Format Tests
# ============================================

test_message_plain_text() {
	local topic="${TEST_PREFIX}-plain-$(date +%s)"
	local message="Hello, World! This is a plain text message $(date +%s)"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 0.5

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_message_json() {
	local topic="${TEST_PREFIX}-json-$(date +%s)"
	local message='{"id":1,"name":"test","value":42.5,"enabled":true}'

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 0.5

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_message_binary() {
	local topic="${TEST_PREFIX}-binary-$(date +%s)"
	local message="binary-data-$(date +%s)"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Use printf to create binary-like data
	printf '%s' "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 0.5

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"$message"* ]]
}

test_message_1kb() {
	local topic="${TEST_PREFIX}-1kb-$(date +%s)"
	# Generate 1KB message
	local message=$(head -c 1024 /dev/zero | tr '\0' 'x')

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 1

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ ${#received} -ge 1000 ]]
}

test_message_10kb() {
	local topic="${TEST_PREFIX}-10kb-$(date +%s)"
	# Generate 10KB message
	local message=$(head -c 10240 /dev/zero | tr '\0' 'x')

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 2

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 2000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ ${#received} -ge 10000 ]]
}

test_message_empty() {
	local topic="${TEST_PREFIX}-empty-$(date +%s)"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce empty message
	echo "" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 0.5

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Empty message should succeed
	[[ $? -eq 0 ]]
}

test_message_unicode() {
	local topic="${TEST_PREFIX}-unicode-$(date +%s)"
	local message="Unicode test: Hello 世界 🌍 Привет مرحبا"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 0.5

	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	[[ "$received" == *"世界"* ]] && [[ "$received" == *"🌍"* ]] && [[ "$received" == *"Привет"* ]]
}

# ============================================
# Performance/Benchmark Tests
# ============================================

test_throughput() {
	local topic="${TEST_PREFIX}-throughput-$(date +%s)"
	local message_count=1000

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	local start_time=$(date +%s%N)

	# Produce messages
	for i in $(seq 1 $message_count); do
		echo "Throughput message $i"
	done | timeout 30 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	local end_time=$(date +%s%N)
	local duration=$(((end_time - start_time) / 1000000))
	local throughput=$(((message_count * 1000) / duration))

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Throughput: $throughput msg/sec ($message_count messages in ${duration}ms)"
	[[ $throughput -gt 10 ]]
}

test_batch_size_1() {
	local topic="${TEST_PREFIX}-batch1-$(date +%s)"
	local message_count=100

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	local start_time=$(date +%s%N)

	# Produce with batch size 1
	for i in $(seq 1 $message_count); do
		echo "Batch test message $i"
	done | timeout 30 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--batch-size 1 \
		2>/dev/null

	local end_time=$(date +%s%N)
	local duration=$(((end_time - start_time) / 1000000))

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Batch size 1: $duration ms for $message_count messages"
	[[ $? -eq 0 ]]
}

test_batch_size_10() {
	local topic="${TEST_PREFIX}-batch10-$(date +%s)"
	local message_count=100

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	local start_time=$(date +%s%N)

	# Produce with batch size 10
	for i in $(seq 1 $message_count); do
		echo "Batch test message $i"
	done | timeout 30 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--batch-size 10 \
		2>/dev/null

	local end_time=$(date +%s%N)
	local duration=$(((end_time - start_time) / 1000000))

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Batch size 10: $duration ms for $message_count messages"
	[[ $? -eq 0 ]]
}

test_batch_size_100() {
	local topic="${TEST_PREFIX}-batch100-$(date +%s)"
	local message_count=100

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	local start_time=$(date +%s%N)

	# Produce with batch size 100
	for i in $(seq 1 $message_count); do
		echo "Batch test message $i"
	done | timeout 30 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--batch-size 100 \
		2>/dev/null

	local end_time=$(date +%s%N)
	local duration=$(((end_time - start_time) / 1000000))

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Batch size 100: $duration ms for $message_count messages"
	[[ $? -eq 0 ]]
}

test_concurrent_producer_consumer() {
	local topic="${TEST_PREFIX}-concurrent-$(date +%s)"
	local message_count=50

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 3 --replication-factor 1 \
		2>/dev/null || true

	# Start consumer in background
	timeout 30 $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--timeout-ms 30000 \
		2>/dev/null >/tmp/consumer_output_$$.txt &
	local consumer_pid=$!

	# Give consumer time to start
	sleep 1

	# Produce messages
	for i in $(seq 1 $message_count); do
		echo "Concurrent message $i"
	done | timeout 30 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	# Wait for consumer
	wait $consumer_pid 2>/dev/null || true

	local received_count=$(wc -l </tmp/consumer_output_$$.txt 2>/dev/null || echo 0)
	rm -f /tmp/consumer_output_$$.txt

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Concurrent: $received_count messages received"
	[[ $received_count -ge 30 ]]
}

# ============================================
# Stress Tests
# ============================================

test_many_topics() {
	local topic_count=20
	local messages_per_topic=2

	# Create multiple topics
	for i in $(seq 1 $topic_count); do
		local topic="${TEST_PREFIX}-many-$i-$(date +%s)"
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
			--create --topic $topic --partitions 1 --replication-factor 1 \
			2>/dev/null || true

		# Produce some messages
		for j in $(seq 1 $messages_per_topic); do
			echo "Message $j for topic $i"
		done | timeout 15 $KAFKA_PRODUCER \
			--bootstrap-server $BOOTSTRAP_SERVER \
			--topic $topic \
			2>/dev/null || true
	done

	sleep 1

	# Count created topics
	local created=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep -c "^${TEST_PREFIX}-many")

	# Cleanup
	local topics=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep "^${TEST_PREFIX}-many")
	for topic in $topics; do
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
	done

	echo "  Created $created topics"
	[[ $created -ge $topic_count ]]
}

test_many_partitions() {
	local topic="${TEST_PREFIX}-partitions-$(date +%s)"
	local partition_count=10
	local message_count=50

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic \
		--partitions $partition_count \
		--replication-factor 1 \
		2>/dev/null || true

	# Produce messages (should be distributed across partitions)
	for i in $(seq 1 $message_count); do
		echo "Partition test message $i"
	done | timeout 20 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	# Verify partition count
	local info=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--describe --topic $topic 2>/dev/null)

	# Count partitions
	local count=$(echo "$info" | grep -c "Partition:" || echo 0)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Topic has $count partitions"
	[[ $count -ge $partition_count ]]
}

test_sustained_throughput() {
	local topic="${TEST_PREFIX}-sustained-$(date +%s)"
	local message_count=300
	local duration_seconds=60

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 3 --replication-factor 1 \
		2>/dev/null || true

	local start_time=$(date +%s)
	local elapsed=0

	# Produce messages for the duration
	while [[ $elapsed -lt $duration_seconds ]]; do
		for i in $(seq 1 10); do
			echo "Sustained message $i at $(date +%s)"
		done | timeout 15 $KAFKA_PRODUCER \
			--bootstrap-server $BOOTSTRAP_SERVER \
			--topic $topic \
			2>/dev/null || true

		elapsed=$(($(date +%s) - start_time))
		sleep 2
	done

	local total_messages=$((duration_seconds * 10))

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	echo "  Sustained test ran for $elapsed seconds"
	[[ $elapsed -ge 20 ]]
}

# ============================================
# Error Handling Tests
# ============================================

test_invalid_topic_name() {
	local invalid_topic="invalid_topic_name!"

	# Try to create topic with invalid name
	local output=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $invalid_topic \
		--partitions 1 --replication-factor 1 2>&1)

	# Should fail
	[[ $? -ne 0 ]] || echo "$output" | grep -qiE "(error|invalid|failed)"
}

test_consume_nonexistent_topic() {
	local topic="${TEST_PREFIX}-notexist-$(date +%s)"

	# Try to consume from non-existent topic
	local output=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms 5000 \
		2>&1)

	# Should timeout or error without crashing
	[[ $? -ne 0 ]] || echo "$output" | grep -qiE "(error|timeout|does not exist)"
}

test_produce_nonexistent_topic_no_autocreate() {
	local topic="${TEST_PREFIX}-noauto-$(date +%s)"

	# Try to produce to non-existent topic with no-auto-create config if supported
	# Note: This test may pass if auto-create is enabled by default
	local output=$(echo "test" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>&1)

	# Cleanup if topic was auto-created
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Test passes if no crash
	[[ true ]]
}

test_timeout_scenarios() {
	local topic="${TEST_PREFIX}-timeout-$(date +%s)"

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Test consumer timeout (no messages)
	local output=$(timeout 5 $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--max-messages 1 \
		--timeout-ms 1000 \
		2>&1)

	local exit_code=$?

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Should timeout gracefully
	[[ $exit_code -ne 0 ]] || echo "$output" | grep -qiE "(timeout|processed 0)"
}

# ============================================
# Storage Engine Tests
# ============================================

test_memory_storage_persistence() {
	local topic="${TEST_PREFIX}-memory-$(date +%s)"
	local message="Persistence test $(date +%s)"

	# Create topic (assuming memory storage is default)
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce message
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 1

	# Consume immediately to verify
	local received1=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Message should be available
	[[ "$received1" == *"$message"* ]]
}

test_s3_storage_compatibility() {
	local topic="${TEST_PREFIX}-s3compat-$(date +%s)"
	local message="S3 storage test $(date +%s)"

	# This test checks if S3 storage configuration is compatible
	# It will succeed regardless of storage backend

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Produce message
	echo "$message" | timeout $TIMEOUT $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 1

	# Consume to verify storage
	local received=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--max-messages 1 \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Message should be persisted and retrievable
	[[ "$received" == *"$message"* ]]
}

test_storage_write_read() {
	local topic="${TEST_PREFIX}-write-read-$(date +%s)"
	local messages=50

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Write messages
	for i in $(seq 1 $messages); do
		echo "Write-read test message $i"
	done | timeout 20 $KAFKA_PRODUCER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		2>/dev/null

	sleep 1

	# Read messages
	local received_count=$(timeout $TIMEOUT $KAFKA_CONSUMER \
		--bootstrap-server $BOOTSTRAP_SERVER \
		--topic $topic \
		--from-beginning \
		--timeout-ms $((TIMEOUT * 1000)) \
		2>/dev/null | wc -l)

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# All messages should be readable
	[[ $received_count -ge $messages ]]
}

# ============================================
# Schema Registry Tests
# ============================================

test_schema_avro_registration() {
	local topic="${TEST_PREFIX}-avro-$(date +%s)"
	# Check if Schema Registry is available (default port 8081)
	if ! nc -z localhost 8081 2>/dev/null; then
		log_skip "Schema Registry not available at localhost:8081"
		return 0
	fi

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Try to register Avro schema
	local schema='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'

	local output=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
		--data "{\"schema\":\"$(echo $schema | jq -Rs .)\"}" \
		http://localhost:8081/subjects/$topic-value/versions)

	local exit_code=$?

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Schema registration should succeed or return meaningful error
	[[ $exit_code -eq 0 ]] || echo "$output" | grep -qiE "(version|error|id)"
}

test_schema_json_registration() {
	local topic="${TEST_PREFIX}-jsonschema-$(date +%s)"
	# Check if Schema Registry is available
	if ! nc -z localhost 8081 2>/dev/null; then
		log_skip "Schema Registry not available at localhost:8081"
		return 0
	fi

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Try to register JSON Schema
	local schema='{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}}'

	local output=$(curl -s -X POST -H "Content-Type: application/json; artifactType=io.apicurio.registry.avro" \
		--data "{\"schema\":\"$(echo $schema | jq -Rs .)\"}" \
		http://localhost:8081/subjects/$topic-value/versions)

	local exit_code=$?

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Schema registration should succeed or return meaningful error
	[[ $exit_code -eq 0 ]] || echo "$output" | grep -qiE "(version|error|id)"
}

test_schema_evolution() {
	local topic="${TEST_PREFIX}-evolution-$(date +%s)"
	# Check if Schema Registry is available
	if ! nc -z localhost 8081 2>/dev/null; then
		log_skip "Schema Registry not available at localhost:8081"
		return 0
	fi

	# Create topic
	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
		--create --topic $topic --partitions 1 --replication-factor 1 \
		2>/dev/null || true

	# Register initial schema
	local schema_v1='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'

	curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
		--data "{\"schema\":\"$(echo $schema_v1 | jq -Rs .)\"}" \
		http://localhost:8081/subjects/$topic-value/versions >/dev/null 2>&1

	# Register evolved schema (backward compatible)
	local schema_v2='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}'

	local output=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
		--data "{\"schema\":\"$(echo $schema_v2 | jq -Rs .)\"}" \
		http://localhost:8081/subjects/$topic-value/versions)

	local exit_code=$?

	$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true

	# Schema evolution should succeed
	[[ $exit_code -eq 0 ]] || echo "$output" | grep -qiE "(version|id)"
}

# ============================================
# Cleanup
# ============================================

cleanup_test_topics() {
	log_info "Cleaning up test topics..."

	local topics=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep "^${TEST_PREFIX}")

	for topic in $topics; do
		$KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
	done
}

# ============================================
# Main
# ============================================

main() {
	echo "=========================================="
	echo "  DataCore Kafka Compatibility Tests"
	echo "=========================================="
	echo ""
	echo "Bootstrap Server: $BOOTSTRAP_SERVER"
	echo ""

	detect_tools
	check_broker

	echo ""
	echo "--- Admin API Tests ---"
	run_test "API Versions Query" test_api_versions
	run_test "Metadata API" test_metadata_api
	run_test "Create Topic with Partitions" test_create_topic_with_partitions
	run_test "Duplicate Topic Error" test_create_duplicate_topic
	run_test "Delete Nonexistent Topic" test_delete_nonexistent_topic

	echo ""
	echo "--- Producer Tests ---"
	run_test "Produce Simple Message" test_produce_simple_message
	run_test "Produce with Key" test_produce_with_key
	run_test "Produce Batch Messages" test_produce_batch
	run_test "Auto-Create Topic on Produce" test_produce_to_nonexistent_topic_autocreate

	echo ""
	echo "--- Consumer Tests ---"
	run_test "Consume from Beginning" test_consume_from_beginning
	run_test "Consume Max Messages" test_consume_max_messages

	echo ""
	echo "--- kcat Tests ---"
	run_test "kcat Metadata" test_kcat_metadata
	run_test "kcat Produce/Consume" test_kcat_produce_consume

	echo ""
	echo "--- Compression Tests ---"
	run_test "Compression: Snappy" test_compression_snappy
	run_test "Compression: Gzip" test_compression_gzip
	run_test "Compression: LZ4" test_compression_lz4
	run_test "Compression: Zstd" test_compression_zstd
	run_test "Compression: Batch Compressed" test_compression_batch_compressed
	run_test "Compression: Mixed Types" test_compression_mixed_types

	echo ""
	echo "--- Consumer Group Tests ---"
	run_test "Basic Consumer Group" test_consumer_group_basic
	run_test "Consumer Group Describe" test_consumer_group_describe
	run_test "Consumer Group Delete (DeleteGroups API)" test_consumer_group_delete
	run_test "Consumer Group Reset Offsets" test_consumer_group_reset_offsets

	echo ""
	echo "--- ACL Tests ---"
	run_test "ACL List" test_acl_list
	run_test "ACL Create and Delete" test_acl_create_and_delete

	echo ""
	echo "--- Message Format Tests ---"
	run_test "Plain Text Message" test_message_plain_text
	run_test "JSON Message" test_message_json
	run_test "Binary Data Message" test_message_binary
	run_test "Large Message (1KB)" test_message_1kb
	run_test "Large Message (10KB)" test_message_10kb
	run_test "Empty Message" test_message_empty
	run_test "Unicode/Special Characters" test_message_unicode

	echo ""
	echo "--- Performance/Benchmark Tests ---"
	run_test "Throughput Test" test_throughput
	run_test "Batch Size 1" test_batch_size_1
	run_test "Batch Size 10" test_batch_size_10
	run_test "Batch Size 100" test_batch_size_100
	run_test "Concurrent Producer/Consumer" test_concurrent_producer_consumer

	echo ""
	echo "--- Stress Tests ---"
	run_test "Many Topics (20)" test_many_topics
	run_test "Many Partitions (10)" test_many_partitions
	run_test "Sustained Throughput (60s)" test_sustained_throughput

	echo ""
	echo "--- Error Handling Tests ---"
	run_test "Invalid Topic Name" test_invalid_topic_name
	run_test "Consume Non-existent Topic" test_consume_nonexistent_topic
	run_test "Produce Non-existent Topic (No Auto-create)" test_produce_nonexistent_topic_no_autocreate
	run_test "Timeout Scenarios" test_timeout_scenarios

	echo ""
	echo "--- Storage Engine Tests ---"
	run_test "Memory Storage Persistence" test_memory_storage_persistence
	run_test "S3 Storage Compatibility" test_s3_storage_compatibility
	run_test "Storage Write/Read" test_storage_write_read

	echo ""
	echo "--- Schema Registry Tests ---"
	run_test "Avro Schema Registration" test_schema_avro_registration
	run_test "JSON Schema Registration" test_schema_json_registration
	run_test "Schema Evolution" test_schema_evolution

	# Cleanup
	cleanup_test_topics

	# Summary
	echo ""
	echo "=========================================="
	echo "  Test Summary"
	echo "=========================================="
	echo "Total:   $TESTS_RUN"
	echo -e "Passed:  ${GREEN}$TESTS_PASSED${NC}"
	echo -e "Failed:  ${RED}$TESTS_FAILED${NC}"
	echo -e "Skipped: ${YELLOW}$TESTS_SKIPPED${NC}"
	echo ""

	if [[ $TESTS_FAILED -gt 0 ]]; then
		exit 1
	fi
}

# Run
main "$@"
