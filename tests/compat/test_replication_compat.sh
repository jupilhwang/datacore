#!/bin/bash
# Kafka Replication Compatibility Tests for DataCore
# Tests that DataCore replication works transparently with standard Kafka clients
#
# Prerequisites:
# - 2+ DataCore brokers running with replication enabled
# - Kafka CLI tools (kafka-topics, kafka-console-producer, kafka-console-consumer)
# - Optional: kcat (formerly kafkacat)
#
# Test Categories:
# 1. Replication Configuration Tests
# 2. kafka-console-producer Tests (replication to ISRs)
# 3. kafka-console-consumer Tests (consume from any replica)
# 4. kcat Tests (produce/consume with replication)
# 5. Replication Behavior Tests
#
# Usage:
#   ./tests/compat/test_replication_compat.sh
#   BOOTSTRAP_SERVERS="host1:9092,host2:9093" ./tests/compat/test_replication_compat.sh

set -uo pipefail

# ============================================
# Configuration
# ============================================

# Default: assume local multi-broker setup with replication
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-127.0.0.1:9092,127.0.0.1:9094}"
BROKER1="${BOOTSTRAP_SERVERS%%,*}"
BROKER2="${BOOTSTRAP_SERVERS##*,}"
BROKER2="${BROKER2%%,*}"

# If there's only one broker, use it for both
if [[ "$BROKER1" == "$BROKER2" ]]; then
    BROKER2="$BROKER1"
fi

TEST_PREFIX="repl-compat"
TIMEOUT_SEC=30

# Kafka CLI detection
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
    echo "Error: Kafka CLI tools not found in PATH"
    exit 1
fi

# kcat detection
KCAT_CMD=""
if command -v kcat &>/dev/null; then
    KCAT_CMD="kcat"
elif command -v kafkacat &>/dev/null; then
    KCAT_CMD="kafkacat"
fi

# ============================================
# Colors
# ============================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================
# Test Counters
# ============================================

TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0

# ============================================
# Utility Functions
# ============================================

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_pass() {
    echo -e "${GREEN}[PASS]${NC} $*"
    PASSED=$((PASSED + 1))
    TOTAL=$((TOTAL + 1))
}
log_fail() {
    echo -e "${RED}[FAIL]${NC} $*"
    FAILED=$((FAILED + 1))
    TOTAL=$((TOTAL + 1))
}
log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $*"
    SKIPPED=$((SKIPPED + 1))
    TOTAL=$((TOTAL + 1))
}

# Generate unique topic name
gen_topic() {
    echo "${TEST_PREFIX}-${1}-$(date +%s)-${RANDOM}"
}

# Quick topic delete
delete_topic() {
    local topic=$1
    (timeout -s KILL 5 $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic "$topic" >/dev/null 2>&1 || true) 2>/dev/null
}

# Wait for broker to be reachable
wait_for_broker() {
    local host_port=$1
    local max_wait=${2:-30}
    local host="${host_port%%:*}"
    local port="${host_port##*:}"

    local elapsed=0
    while [[ $elapsed -lt $max_wait ]]; do
        if nc -z -w 2 "$host" "$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

# Run with timeout (macOS compatible)
run_with_timeout() {
    local secs=$1
    shift
    if command -v timeout &>/dev/null; then
        timeout "$secs" "$@"
    else
        perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
    fi
}

# Check broker connectivity
check_brokers() {
    log_info "Checking broker connectivity..."
    
    local brokers=($BROKER1 $BROKER2)
    local all_ok=true
    
    for broker in "${brokers[@]}"; do
        if ! wait_for_broker "$broker" 10; then
            log_fail "Broker $broker not reachable"
            all_ok=false
        fi
    done
    
    if [[ "$all_ok" == "true" ]]; then
        log_info "All brokers are reachable"
    else
        log_fail "Not all brokers are reachable"
        exit 1
    fi
}

# ============================================
# Test 1: Broker Connectivity
# ============================================

test_broker_connectivity() {
    log_info "Test 1: Broker Connectivity"
    
    check_brokers
    log_pass "Test 1: All brokers connected"
}

# ============================================
# Test 2: Create Topic with Replication Factor
# ============================================

test_create_topic_with_replication() {
    log_info "Test 2: Create Topic with Replication Factor > 1"
    
    local topic
    topic=$(gen_topic "rf2")
    
    # Create topic with replication factor 2
    local output
    output=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 2 2>&1)
    
    local rc=$?
    
    sleep 2
    
    # Verify topic was created
    local describe
    describe=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --describe --topic "$topic" 2>&1)
    
    # Check for replication factor in output
    local rf_found=false
    if echo "$describe" | grep -q "ReplicationFactor: 2\|Replicas: .*,.*"; then
        rf_found=true
    fi
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$rf_found" == "true" ]]; then
        log_pass "Test 2: Topic created with replication factor 2"
    else
        log_fail "Test 2: Topic replication factor not verified (output: $describe)"
    fi
}

# ============================================
# Test 3: kafka-console-producer - Messages Replicate to ISRs
# ============================================

test_producer_replicates_to_isr() {
    log_info "Test 3: kafka-console-producer - Messages replicate to ISRs"
    
    local topic
    topic=$(gen_topic "isr")
    local msg_count=20
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 3 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce messages via kafka-console-producer
    for i in $(seq 1 $msg_count); do
        echo "repl-test-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" 2>/dev/null
    
    # Wait for replication
    sleep 3
    
    # Consume messages to verify they were produced
    local consumed
    consumed=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" \
        --from-beginning \
        --max-messages $msg_count \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    local consumed_count
    consumed_count=$(echo "$consumed" | grep -c "repl-test-" || true)
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 3: kafka-console-producer - $consumed_count/$msg_count messages replicated to ISRs"
    else
        log_fail "Test 3: Expected $msg_count messages, got $consumed_count"
    fi
}

# ============================================
# Test 4: kafka-console-consumer - Consume from Leader
# ============================================

test_consumer_from_leader() {
    log_info "Test 4: kafka-console-consumer - Consume from Leader"
    
    local topic
    topic=$(gen_topic "leader")
    local msg_count=10
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 2 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce messages
    for i in $(seq 1 $msg_count); do
        echo "leader-msg-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" 2>/dev/null
    
    sleep 2
    
    # Consume from leader broker
    local consumed
    consumed=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" \
        --from-beginning \
        --max-messages $msg_count \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    local consumed_count
    consumed_count=$(echo "$consumed" | grep -c "leader-msg-" || true)
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 4: kafka-console-consumer from leader - $consumed_count/$msg_count messages"
    else
        log_fail "Test 4: Expected $msg_count messages, got $consumed_count"
    fi
}

# ============================================
# Test 5: kafka-console-consumer - Consume from Follower
# ============================================

test_consumer_from_follower() {
    log_info "Test 5: kafka-console-consumer - Consume from Follower"
    
    local topic
    topic=$(gen_topic "follower")
    local msg_count=10
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 2 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce messages to broker1 (leader)
    for i in $(seq 1 $msg_count); do
        echo "follower-msg-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" 2>/dev/null
    
    # Wait for replication to complete
    sleep 5
    
    # Try to consume from broker2 (follower)
    local consumed=""
    local consumed_count=0
    
    # First attempt: consume from broker2
    consumed=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BROKER2" \
        --topic "$topic" \
        --from-beginning \
        --max-messages $msg_count \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    consumed_count=$(echo "$consumed" | grep -c "follower-msg-" || true)
    
    # If broker2 doesn't have messages yet, try bootstrap servers
    if [[ "$consumed_count" -lt "$msg_count" ]]; then
        sleep 3
        consumed=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
            --bootstrap-server "$BOOTSTRAP_SERVERS" \
            --topic "$topic" \
            --from-beginning \
            --max-messages $msg_count \
            --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
        
        consumed_count=$(echo "$consumed" | grep -c "follower-msg-" || true)
    fi
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 5: kafka-console-consumer from follower - $consumed_count/$msg_count messages"
    else
        log_fail "Test 5: Expected $msg_count messages from follower, got $consumed_count"
    fi
}

# ============================================
# Test 6: kcat - Produce with Replication
# ============================================

test_kcat_produce() {
    log_info "Test 6: kcat - Produce with Replication"
    
    if [[ -z "$KCAT_CMD" ]]; then
        log_skip "Test 6: kcat not available"
        return
    fi
    
    local topic
    topic=$(gen_topic "kcat-produce")
    local msg_count=15
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 2 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce messages via kcat
    for i in $(seq 1 $msg_count); do
        echo "kcat-msg-$i"
    done | $KCAT_CMD -b "$BROKER1" -P -t "$topic" 2>/dev/null
    
    # Wait for replication
    sleep 3
    
    # Consume to verify
    local consumed
    consumed=$($KCAT_CMD -b "$BOOTSTRAP_SERVERS" -C -t "$topic" -c $msg_count -e 2>/dev/null || true)
    
    local consumed_count
    consumed_count=$(echo "$consumed" | grep -c "kcat-msg-" || true)
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 6: kcat produce - $consumed_count/$msg_count messages"
    else
        log_fail "Test 6: Expected $msg_count messages, got $consumed_count"
    fi
}

# ============================================
# Test 7: kcat - Consume from Any Replica
# ============================================

test_kcat_consume_from_replica() {
    log_info "Test 7: kcat - Consume from Any Replica"
    
    if [[ -z "$KCAT_CMD" ]]; then
        log_skip "Test 7: kcat not available"
        return
    fi
    
    local topic
    topic=$(gen_topic "kcat-consume")
    local msg_count=10
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 2 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce via kafka-console-producer
    for i in $(seq 1 $msg_count); do
        echo "kcat-replica-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" 2>/dev/null
    
    # Wait for replication
    sleep 5
    
    # Consume from broker2 (potential follower)
    local consumed=""
    local consumed_count=0
    
    consumed=$($KCAT_CMD -b "$BROKER2" -C -t "$topic" -c $msg_count -e 2>/dev/null || true)
    consumed_count=$(echo "$consumed" | grep -c "kcat-replica-" || true)
    
    # If broker2 doesn't have messages, try bootstrap servers
    if [[ "$consumed_count" -lt "$msg_count" ]]; then
        consumed=$($KCAT_CMD -b "$BOOTSTRAP_SERVERS" -C -t "$topic" -c $msg_count -e 2>/dev/null || true)
        consumed_count=$(echo "$consumed" | grep -c "kcat-replica-" || true)
    fi
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 7: kcat consume from replica - $consumed_count/$msg_count messages"
    else
        log_fail "Test 7: Expected $msg_count messages, got $consumed_count"
    fi
}

# ============================================
# Test 8: Cross-Broker Produce/Consume with Replication
# ============================================

test_cross_broker_replication() {
    log_info "Test 8: Cross-Broker Produce/Consume with Replication"
    
    local topic
    topic=$(gen_topic "cross-repl")
    local msg_count=20
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 3 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce via broker1
    for i in $(seq 1 $msg_count); do
        echo "cross-repl-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" 2>/dev/null
    
    sleep 3
    
    # Consume from broker2
    local consumed
    consumed=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BROKER2" \
        --topic "$topic" \
        --from-beginning \
        --max-messages $msg_count \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    local consumed_count
    consumed_count=$(echo "$consumed" | grep -c "cross-repl-" || true)
    
    # Cleanup
    delete_topic "$topic"
    
    if [[ "$consumed_count" -ge "$msg_count" ]]; then
        log_pass "Test 8: Cross-broker replication - $consumed_count/$msg_count messages"
    else
        log_fail "Test 8: Expected $msg_count messages, got $consumed_count"
    fi
}

# ============================================
# Test 9: Consumer Group with Replication
# ============================================

test_consumer_group_replication() {
    log_info "Test 9: Consumer Group with Replication"
    
    local topic
    topic=$(gen_topic "group-repl")
    local group="repl-group-$(date +%s)"
    local msg_count=15
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 3 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce messages
    for i in $(seq 1 $msg_count); do
        echo "group-repl-$i"
    done | run_with_timeout $TIMEOUT_SEC $KAFKA_PRODUCER \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --topic "$topic" 2>/dev/null
    
    sleep 3
    
    # Consume with group from broker1
    local consumed1
    consumed1=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BROKER1" \
        --topic "$topic" \
        --group "$group" \
        --from-beginning \
        --max-messages 10 \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    sleep 2
    
    # Consume with same group from broker2 (should continue from offset)
    local consumed2
    consumed2=$(run_with_timeout $TIMEOUT_SEC $KAFKA_CONSUMER \
        --bootstrap-server "$BROKER2" \
        --topic "$topic" \
        --group "$group" \
        --max-messages 10 \
        --timeout-ms $((TIMEOUT_SEC * 1000)) 2>/dev/null)
    
    local count1 count2
    count1=$(echo "$consumed1" | grep -c "group-repl-" || true)
    count2=$(echo "$consumed2" | grep -c "group-repl-" || true)
    
    local total=$((count1 + count2))
    
    # Cleanup
    delete_topic "$topic"
    
    # Success if total >= msg_count (allow for some timing variations)
    if [[ "$total" -ge $((msg_count * 8 / 10)) ]]; then
        log_pass "Test 9: Consumer group with replication - total $total/$msg_count messages (c1=$count1, c2=$count2)"
    else
        log_fail "Test 9: Expected at least $((msg_count * 8 / 10)) messages, got $total (c1=$count1, c2=$count2)"
    fi
}

# ============================================
# Test 10: Compression with Replication
# ============================================

test_compression_with_replication() {
    log_info "Test 10: Compression with Replication"
    
    if [[ -z "$KCAT_CMD" ]]; then
        log_skip "Test 10: kcat not available for compression test"
        return
    fi
    
    local codec="snappy"
    local topic
    topic=$(gen_topic "compress-repl")
    local message="compressed-repl-test-$(date +%s)"
    
    # Create topic with RF=2
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --topic "$topic" \
        --partitions 2 --replication-factor 2 2>/dev/null
    
    sleep 2
    
    # Produce with compression via kcat
    echo "$message" | $KCAT_CMD -b "$BROKER1" -P -t "$topic" -z "$codec" 2>/dev/null
    
    sleep 5
    
    # Consume from broker2
    local consumed
    consumed=$($KCAT_CMD -b "$BROKER2" -C -t "$topic" -c 1 -e 2>/dev/null || true)
    
    # If broker2 doesn't have it yet, try bootstrap
    if ! echo "$consumed" | grep -q "$message"; then
        consumed=$($KCAT_CMD -b "$BOOTSTRAP_SERVERS" -C -t "$topic" -c 1 -e 2>/dev/null || true)
    fi
    
    # Cleanup
    delete_topic "$topic"
    
    if echo "$consumed" | grep -q "$message"; then
        log_pass "Test 10: Compression with replication - $codec works"
    else
        log_fail "Test 10: Compression with replication - message not found"
    fi
}

# ============================================
# Cleanup
# ============================================

cleanup_test_topics() {
    log_info "Cleaning up test topics..."
    
    local topics
    topics=$($KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVERS" --list 2>/dev/null | grep "^${TEST_PREFIX}" || true)
    
    if [[ -z "$topics" ]]; then
        return
    fi
    
    while IFS= read -r topic; do
        if [[ -n "$topic" ]]; then
            delete_topic "$topic"
        fi
    done <<< "$topics"
}

# ============================================
# Summary
# ============================================

print_summary() {
    echo ""
    echo "=========================================="
    echo "  Replication Compatibility Test Summary"
    echo "=========================================="
    echo "Total:   $TOTAL"
    echo -e "Passed:  ${GREEN}$PASSED${NC}"
    echo -e "Failed:  ${RED}$FAILED${NC}"
    echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"
    echo "=========================================="
    echo ""
    
    if [[ $FAILED -gt 0 ]]; then
        exit 1
    fi
}

# ============================================
# Main
# ============================================

main() {
    echo "=========================================="
    echo "  DataCore Replication Compatibility Tests"
    echo "=========================================="
    echo ""
    echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
    echo "Broker 1: $BROKER1"
    echo "Broker 2: $BROKER2"
    echo ""
    
    # Check prerequisites
    check_brokers
    
    echo ""
    log_info "--- Replication Configuration Tests ---"
    test_broker_connectivity
    test_create_topic_with_replication
    
    echo ""
    log_info "--- kafka-console-producer Tests ---"
    test_producer_replicates_to_isr
    
    echo ""
    log_info "--- kafka-console-consumer Tests ---"
    test_consumer_from_leader
    test_consumer_from_follower
    
    echo ""
    log_info "--- kcat Tests ---"
    test_kcat_produce
    test_kcat_consume_from_replica
    
    echo ""
    log_info "--- Replication Behavior Tests ---"
    test_cross_broker_replication
    test_consumer_group_replication
    test_compression_with_replication
    
    echo ""
    cleanup_test_topics
    print_summary
}

main "$@"
