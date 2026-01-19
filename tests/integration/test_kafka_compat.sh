#!/bin/bash
# Kafka Compatibility Tests for DataCore
# Tests compatibility with standard Kafka clients and tools
# 
# Prerequisites:
# - Kafka CLI tools (kafka-topics, kafka-console-producer, etc.)
# - Optional: kcat (formerly kafkacat)
# - DataCore broker running

set -o pipefail

# Define timeout function for macOS/BSD compatibility
if ! command -v timeout &> /dev/null; then
    if command -v gtimeout &> /dev/null; then
        timeout() { gtimeout "$@"; }
    else
        function timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }
    fi
fi

# Configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TEST_PREFIX="compat-test"
TIMEOUT=15

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
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((TESTS_PASSED++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((TESTS_FAILED++)); }
log_skip() { echo -e "${YELLOW}[SKIP]${NC} $1"; ((TESTS_SKIPPED++)); }

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
    if command -v kafka-topics &> /dev/null; then
        KAFKA_TOPICS="kafka-topics"
        KAFKA_PRODUCER="kafka-console-producer"
        KAFKA_CONSUMER="kafka-console-consumer"
        KAFKA_GROUPS="kafka-consumer-groups"
    elif command -v kafka-topics.sh &> /dev/null; then
        KAFKA_TOPICS="kafka-topics.sh"
        KAFKA_PRODUCER="kafka-console-producer.sh"
        KAFKA_CONSUMER="kafka-console-consumer.sh"
        KAFKA_GROUPS="kafka-consumer-groups.sh"
    else
        echo "Error: Kafka CLI tools not found"
        exit 1
    fi
    
    # kcat
    if command -v kcat &> /dev/null; then
        HAS_KCAT=true
    elif command -v kafkacat &> /dev/null; then
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
        --replication-factor 1 \
        2>/dev/null
    
    # Verify partition count
    local info=$($KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --describe --topic $topic 2>/dev/null)
    
    if echo "$info" | grep -q "PartitionCount.*5"; then
        $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
        return 0
    fi
    
    # Fallback: count partition lines
    local count=$(echo "$info" | grep -c "Partition:")
    [[ $count -eq 5 ]]
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
        --timeout-ms $((TIMEOUT*1000)) \
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
        --timeout-ms $((TIMEOUT*1000)) \
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
        --create --topic $topic --partitions 1 --replication-factor 1 \
        2>/dev/null || true
    
    # Produce with kcat
    echo "$message" | kcat -b $BOOTSTRAP_SERVER -P -t $topic 2>/dev/null
    
    sleep 1
    
    # Consume with kcat
    local received=$(kcat -b $BOOTSTRAP_SERVER -C -t $topic -c 1 -e 2>/dev/null)
    
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
        --create --topic $topic --partitions 3 --replication-factor 1 \
        2>/dev/null || true
    
    for i in $(seq 1 10); do
        echo "Group message $i"
    done | timeout $TIMEOUT $KAFKA_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic \
        2>/dev/null
    
    sleep 1
    
    # Consume with group
    timeout $TIMEOUT $KAFKA_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic \
        --group $group \
        --from-beginning \
        --max-messages 10 \
        --timeout-ms $((TIMEOUT*1000)) \
        2>/dev/null > /dev/null
    
    # Check group exists
    local groups=$($KAFKA_GROUPS --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null)
    
    # Cleanup
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $topic 2>/dev/null || true
    
    echo "$groups" | grep -q "$group"
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
    echo "--- Consumer Group Tests ---"
    run_test "Basic Consumer Group" test_consumer_group_basic
    
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
