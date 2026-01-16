#!/bin/bash
# Integration tests using Kafka CLI tools
# Requires: Kafka CLI tools installed (kafka-topics.sh, kafka-console-producer.sh, etc.)
# Run DataCore broker before executing this script

set -e

# Configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TEST_TOPIC="datacore-test-$(date +%s)"
AVRO_TOPIC="datacore-avro-test-$(date +%s)"
TIMEOUT=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Logging functions
log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((TESTS_PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((TESTS_FAILED++))
}

run_test() {
    local test_name="$1"
    local test_func="$2"
    
    ((TESTS_RUN++))
    echo ""
    log_info "Running: $test_name"
    
    if $test_func; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
    fi
}

# Check if Kafka CLI tools are available
check_kafka_cli() {
    if ! command -v kafka-topics.sh &> /dev/null && ! command -v kafka-topics &> /dev/null; then
        echo "Kafka CLI tools not found. Please install Kafka or set PATH."
        echo "You can download Kafka from: https://kafka.apache.org/downloads"
        exit 1
    fi
}

# Detect kafka command prefix (kafka-topics.sh or kafka-topics)
detect_kafka_cmd() {
    if command -v kafka-topics.sh &> /dev/null; then
        KAFKA_TOPICS="kafka-topics.sh"
        KAFKA_CONSOLE_PRODUCER="kafka-console-producer.sh"
        KAFKA_CONSOLE_CONSUMER="kafka-console-consumer.sh"
        KAFKA_AVRO_CONSOLE_PRODUCER="kafka-avro-console-producer"
        KAFKA_AVRO_CONSOLE_CONSUMER="kafka-avro-console-consumer"
    else
        KAFKA_TOPICS="kafka-topics"
        KAFKA_CONSOLE_PRODUCER="kafka-console-producer"
        KAFKA_CONSOLE_CONSUMER="kafka-console-consumer"
        KAFKA_AVRO_CONSOLE_PRODUCER="kafka-avro-console-producer"
        KAFKA_AVRO_CONSOLE_CONSUMER="kafka-avro-console-consumer"
    fi
}

# ============================================
# Topic Management Tests
# ============================================

test_create_topic() {
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $TEST_TOPIC \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null
    
    # Verify topic exists
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --list 2>/dev/null | grep -q $TEST_TOPIC
}

test_list_topics() {
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --list 2>/dev/null | grep -q $TEST_TOPIC
}

test_describe_topic() {
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --describe \
        --topic $TEST_TOPIC 2>/dev/null | grep -q "PartitionCount"
}

test_delete_topic() {
    local delete_topic="datacore-delete-test-$(date +%s)"
    
    # Create topic
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $delete_topic \
        --partitions 1 \
        --replication-factor 1 \
        2>/dev/null
    
    # Delete topic
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --delete \
        --topic $delete_topic \
        2>/dev/null
    
    # Verify topic is deleted (should not appear in list)
    sleep 1
    ! $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --list 2>/dev/null | grep -q "^${delete_topic}$"
}

# ============================================
# Producer/Consumer Tests (Plain Text)
# ============================================

test_produce_consume_simple() {
    local test_message="Hello DataCore $(date +%s)"
    
    # Produce message
    echo "$test_message" | $KAFKA_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        2>/dev/null
    
    # Consume and verify
    local received=$($KAFKA_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms ${TIMEOUT}000 \
        2>/dev/null)
    
    [[ "$received" == *"$test_message"* ]]
}

test_produce_multiple_messages() {
    local count=10
    
    # Produce multiple messages
    for i in $(seq 1 $count); do
        echo "Message $i from batch test"
    done | $KAFKA_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        2>/dev/null
    
    # Consume and count
    local received_count=$($KAFKA_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        --from-beginning \
        --max-messages $count \
        --timeout-ms ${TIMEOUT}000 \
        2>/dev/null | wc -l)
    
    [[ $received_count -ge $count ]]
}

test_produce_with_key() {
    local key="test-key-$(date +%s)"
    local value="test-value"
    
    # Produce message with key
    echo "${key}:${value}" | $KAFKA_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        --property "parse.key=true" \
        --property "key.separator=:" \
        2>/dev/null
    
    # Consume and verify
    local received=$($KAFKA_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $TEST_TOPIC \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms ${TIMEOUT}000 \
        --property "print.key=true" \
        2>/dev/null)
    
    [[ "$received" == *"$key"* ]] && [[ "$received" == *"$value"* ]]
}

# ============================================
# Avro Producer/Consumer Tests
# ============================================

test_create_avro_topic() {
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $AVRO_TOPIC \
        --partitions 1 \
        --replication-factor 1 \
        2>/dev/null
}

test_avro_produce_consume() {
    # Check if avro tools are available
    if ! command -v $KAFKA_AVRO_CONSOLE_PRODUCER &> /dev/null; then
        log_info "Skipping Avro test: kafka-avro-console-producer not found"
        return 0
    fi
    
    local schema='{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
    local message='{"id":1,"name":"TestUser"}'
    
    # Produce Avro message
    echo "$message" | $KAFKA_AVRO_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $AVRO_TOPIC \
        --property "schema.registry.url=$SCHEMA_REGISTRY_URL" \
        --property "value.schema=$schema" \
        2>/dev/null
    
    # Consume Avro message
    local received=$($KAFKA_AVRO_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $AVRO_TOPIC \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms ${TIMEOUT}000 \
        --property "schema.registry.url=$SCHEMA_REGISTRY_URL" \
        2>/dev/null)
    
    [[ "$received" == *"TestUser"* ]]
}

test_avro_with_key() {
    if ! command -v $KAFKA_AVRO_CONSOLE_PRODUCER &> /dev/null; then
        log_info "Skipping Avro key test: kafka-avro-console-producer not found"
        return 0
    fi
    
    local key_schema='{"type":"string"}'
    local value_schema='{"type":"record","name":"Order","fields":[{"name":"orderId","type":"int"},{"name":"product","type":"string"}]}'
    local key='"order-001"'
    local value='{"orderId":1,"product":"Widget"}'
    
    # Produce with key
    echo "${key}\t${value}" | $KAFKA_AVRO_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $AVRO_TOPIC \
        --property "schema.registry.url=$SCHEMA_REGISTRY_URL" \
        --property "parse.key=true" \
        --property "key.schema=$key_schema" \
        --property "value.schema=$value_schema" \
        2>/dev/null
    
    # Consume and verify
    local received=$($KAFKA_AVRO_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $AVRO_TOPIC \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms ${TIMEOUT}000 \
        --property "schema.registry.url=$SCHEMA_REGISTRY_URL" \
        --property "print.key=true" \
        2>/dev/null)
    
    [[ "$received" == *"order-001"* ]] && [[ "$received" == *"Widget"* ]]
}

# ============================================
# Consumer Group Tests
# ============================================

test_consumer_group() {
    local group_id="test-group-$(date +%s)"
    local group_topic="group-test-topic-$(date +%s)"
    
    # Create topic
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $group_topic \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null
    
    # Produce messages
    for i in $(seq 1 10); do
        echo "Group message $i"
    done | $KAFKA_CONSOLE_PRODUCER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $group_topic \
        2>/dev/null
    
    # Consume with consumer group
    $KAFKA_CONSOLE_CONSUMER \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $group_topic \
        --group $group_id \
        --from-beginning \
        --max-messages 10 \
        --timeout-ms ${TIMEOUT}000 \
        2>/dev/null > /dev/null
    
    # List consumer groups
    if command -v kafka-consumer-groups.sh &> /dev/null; then
        kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVER \
            --list 2>/dev/null | grep -q $group_id
    elif command -v kafka-consumer-groups &> /dev/null; then
        kafka-consumer-groups --bootstrap-server $BOOTSTRAP_SERVER \
            --list 2>/dev/null | grep -q $group_id
    else
        return 0  # Skip if command not available
    fi
}

# ============================================
# Cleanup
# ============================================

cleanup() {
    log_info "Cleaning up test topics..."
    
    # Delete test topics
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --delete \
        --topic $TEST_TOPIC \
        2>/dev/null || true
    
    $KAFKA_TOPICS --bootstrap-server $BOOTSTRAP_SERVER \
        --delete \
        --topic $AVRO_TOPIC \
        2>/dev/null || true
}

# ============================================
# Main
# ============================================

main() {
    echo "=========================================="
    echo "  DataCore Integration Tests (Kafka CLI)"
    echo "=========================================="
    echo ""
    echo "Bootstrap Server: $BOOTSTRAP_SERVER"
    echo "Schema Registry:  $SCHEMA_REGISTRY_URL"
    echo ""
    
    check_kafka_cli
    detect_kafka_cmd
    
    # Topic Management Tests
    run_test "Create Topic" test_create_topic
    run_test "List Topics" test_list_topics
    run_test "Describe Topic" test_describe_topic
    run_test "Delete Topic" test_delete_topic
    
    # Producer/Consumer Tests
    run_test "Produce/Consume Simple Message" test_produce_consume_simple
    run_test "Produce Multiple Messages" test_produce_multiple_messages
    run_test "Produce/Consume with Key" test_produce_with_key
    
    # Avro Tests
    run_test "Create Avro Topic" test_create_avro_topic
    run_test "Avro Produce/Consume" test_avro_produce_consume
    run_test "Avro with Key" test_avro_with_key
    
    # Consumer Group Tests
    run_test "Consumer Group" test_consumer_group
    
    # Cleanup
    cleanup
    
    # Summary
    echo ""
    echo "=========================================="
    echo "  Test Summary"
    echo "=========================================="
    echo "Total:  $TESTS_RUN"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo ""
    
    if [[ $TESTS_FAILED -gt 0 ]]; then
        exit 1
    fi
}

# Run main
main "$@"
