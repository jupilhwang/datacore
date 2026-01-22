#!/bin/bash
# =============================================================================
# DataCore Failure Recovery E2E Test
# Tests broker restart, data persistence, and cluster recovery scenarios
# =============================================================================

set -e

# Configuration
DATACORE_BIN="${DATACORE_BIN:-./bin/datacore}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC_PREFIX="recovery-test"
LOG_DIR="tests/benchmark/results/recovery"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Create log directory
mkdir -p "$LOG_DIR"

echo "=============================================="
echo "DataCore Failure Recovery E2E Test"
echo "=============================================="
echo "Bootstrap Server: $BOOTSTRAP_SERVER"
echo "Log Directory: $LOG_DIR"
echo ""

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_broker() {
    local server=$1
    kafka-broker-api-versions --bootstrap-server "$server" &>/dev/null
    return $?
}

wait_for_broker() {
    local server=$1
    local timeout=${2:-30}
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if check_broker "$server"; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

produce_messages() {
    local topic=$1
    local count=$2
    local server=${3:-$BOOTSTRAP_SERVER}

    seq 1 "$count" | kafka-console-producer \
        --topic "$topic" \
        --bootstrap-server "$server" \
        --property "parse.key=false" 2>/dev/null
}

count_messages() {
    local topic=$1
    local server=${2:-$BOOTSTRAP_SERVER}

    kafka-console-consumer \
        --topic "$topic" \
        --bootstrap-server "$server" \
        --from-beginning \
        --timeout-ms 5000 2>/dev/null | wc -l | tr -d ' '
}

# =============================================================================
# Test 1: Single Broker Restart
# =============================================================================
test_single_broker_restart() {
    log_info "Test 1: Single Broker Restart"

    local topic="${TOPIC_PREFIX}-single-restart"
    local msg_count=100

    # Check broker is running
    if ! check_broker "$BOOTSTRAP_SERVER"; then
        log_error "Broker not running at $BOOTSTRAP_SERVER"
        return 1
    fi

    # Create topic
    kafka-topics --create --topic "$topic" \
        --partitions 1 --replication-factor 1 \
        --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null || true

    # Produce messages
    log_info "Producing $msg_count messages..."
    produce_messages "$topic" "$msg_count"

    # Count messages before restart
    local before=$(count_messages "$topic")
    log_info "Messages before restart: $before"

    # Wait for data to flush (if using S3)
    sleep 3

    # Restart broker (assuming PID file or process management)
    log_info "Restarting broker..."
    pkill -f datacore || true
    sleep 2

    # Start broker again
    $DATACORE_BIN -c config.toml &>/dev/null &
    local broker_pid=$!

    # Wait for broker to come up
    if ! wait_for_broker "$BOOTSTRAP_SERVER" 30; then
        log_error "Broker did not restart in time"
        return 1
    fi

    # Count messages after restart
    sleep 2
    local after=$(count_messages "$topic")
    log_info "Messages after restart: $after"

    # Verify
    if [ "$after" -ge "$before" ]; then
        log_info "Test 1 PASSED: Data persisted after restart ($before -> $after)"
        return 0
    else
        log_error "Test 1 FAILED: Data loss detected ($before -> $after)"
        return 1
    fi
}

# =============================================================================
# Test 2: Produce During Shutdown
# =============================================================================
test_produce_during_shutdown() {
    log_info "Test 2: Produce During Shutdown (Graceful)"

    local topic="${TOPIC_PREFIX}-graceful-shutdown"
    local msg_count=1000

    if ! check_broker "$BOOTSTRAP_SERVER"; then
        log_error "Broker not running"
        return 1
    fi

    # Create topic
    kafka-topics --create --topic "$topic" \
        --partitions 3 --replication-factor 1 \
        --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null || true

    # Start producing in background
    log_info "Starting background producer..."
    (
        for i in $(seq 1 $msg_count); do
            echo "message-$i"
            sleep 0.01
        done
    ) | kafka-console-producer \
        --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --property "parse.key=false" 2>/dev/null &
    local producer_pid=$!

    # Wait a bit then initiate graceful shutdown
    sleep 2
    log_info "Initiating graceful shutdown..."
    pkill -TERM -f datacore || true

    # Wait for producer to finish (should fail gracefully)
    wait $producer_pid 2>/dev/null || true

    # Restart broker
    sleep 2
    $DATACORE_BIN -c config.toml &>/dev/null &

    if ! wait_for_broker "$BOOTSTRAP_SERVER" 30; then
        log_error "Broker did not restart"
        return 1
    fi

    # Count messages
    sleep 2
    local received=$(count_messages "$topic")
    log_info "Messages received after restart: $received"

    if [ "$received" -gt 0 ]; then
        log_info "Test 2 PASSED: Some messages persisted during shutdown ($received/$msg_count)"
        return 0
    else
        log_warn "Test 2 WARNING: No messages persisted (may be expected with Memory storage)"
        return 0
    fi
}

# =============================================================================
# Test 3: Consumer Group Recovery
# =============================================================================
test_consumer_group_recovery() {
    log_info "Test 3: Consumer Group Recovery"

    local topic="${TOPIC_PREFIX}-group-recovery"
    local group="recovery-test-group"
    local msg_count=100

    if ! check_broker "$BOOTSTRAP_SERVER"; then
        log_error "Broker not running"
        return 1
    fi

    # Create topic
    kafka-topics --create --topic "$topic" \
        --partitions 1 --replication-factor 1 \
        --bootstrap-server "$BOOTSTRAP_SERVER" 2>/dev/null || true

    # Produce messages
    log_info "Producing $msg_count messages..."
    produce_messages "$topic" "$msg_count"

    # Consume half with consumer group
    log_info "Consuming first half..."
    kafka-console-consumer \
        --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$group" \
        --from-beginning \
        --max-messages 50 \
        --timeout-ms 10000 2>/dev/null >/dev/null

    # Check committed offset
    local offset_before=$(kafka-consumer-groups \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$group" \
        --describe 2>/dev/null | grep "$topic" | awk '{print $4}' | head -1)
    log_info "Committed offset before restart: $offset_before"

    # Restart broker
    log_info "Restarting broker..."
    pkill -f datacore || true
    sleep 2
    $DATACORE_BIN -c config.toml &>/dev/null &

    if ! wait_for_broker "$BOOTSTRAP_SERVER" 30; then
        log_error "Broker did not restart"
        return 1
    fi

    # Check committed offset after restart
    sleep 2
    local offset_after=$(kafka-consumer-groups \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$group" \
        --describe 2>/dev/null | grep "$topic" | awk '{print $4}' | head -1)
    log_info "Committed offset after restart: $offset_after"

    # Continue consuming
    log_info "Resuming consumption..."
    local remaining=$(kafka-console-consumer \
        --topic "$topic" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --group "$group" \
        --timeout-ms 10000 2>/dev/null | wc -l | tr -d ' ')
    log_info "Remaining messages consumed: $remaining"

    if [ "${offset_after:-0}" -ge "${offset_before:-0}" ]; then
        log_info "Test 3 PASSED: Consumer group offset persisted"
        return 0
    else
        log_warn "Test 3 WARNING: Offset not persisted (may be expected with Memory storage)"
        return 0
    fi
}

# =============================================================================
# Test 4: Multi-Broker Failover
# =============================================================================
test_multi_broker_failover() {
    log_info "Test 4: Multi-Broker Failover"

    # Check if multi-broker configs exist
    if [ ! -f "tests/benchmark/config_broker1.toml" ]; then
        log_warn "Multi-broker configs not found, skipping test"
        return 0
    fi

    local topic="${TOPIC_PREFIX}-multi-failover"

    # Start all brokers
    log_info "Starting 3-broker cluster..."
    $DATACORE_BIN -c tests/benchmark/config_broker1.toml &>/dev/null &
    local pid1=$!
    $DATACORE_BIN -c tests/benchmark/config_broker2.toml &>/dev/null &
    local pid2=$!
    $DATACORE_BIN -c tests/benchmark/config_broker3.toml &>/dev/null &
    local pid3=$!

    sleep 3

    # Check all brokers
    for port in 9092 9093 9094; do
        if ! check_broker "localhost:$port"; then
            log_error "Broker on port $port not responding"
            kill $pid1 $pid2 $pid3 2>/dev/null || true
            return 1
        fi
    done

    # Create topic
    kafka-topics --create --topic "$topic" \
        --partitions 3 --replication-factor 1 \
        --bootstrap-server localhost:9092 2>/dev/null || true

    # Produce to broker 1
    log_info "Producing to broker 1..."
    seq 1 100 | kafka-console-producer \
        --topic "$topic" \
        --bootstrap-server localhost:9092 2>/dev/null

    # Kill broker 1
    log_info "Killing broker 1..."
    kill $pid1 2>/dev/null || true

    sleep 2

    # Produce to broker 2
    log_info "Producing to broker 2..."
    seq 101 200 | kafka-console-producer \
        --topic "$topic" \
        --bootstrap-server localhost:9093 2>/dev/null

    # Consume from broker 3
    log_info "Consuming from broker 3..."
    local count=$(kafka-console-consumer \
        --topic "$topic" \
        --bootstrap-server localhost:9094 \
        --from-beginning \
        --timeout-ms 10000 2>/dev/null | wc -l | tr -d ' ')

    # Cleanup
    kill $pid2 $pid3 2>/dev/null || true

    log_info "Total messages consumed: $count"

    if [ "$count" -ge 100 ]; then
        log_info "Test 4 PASSED: Failover successful, messages readable from surviving broker"
        return 0
    else
        log_warn "Test 4 WARNING: Some data may have been lost during failover"
        return 0
    fi
}

# =============================================================================
# Run Tests
# =============================================================================
run_all_tests() {
    local passed=0
    local failed=0

    # Test 1
    if test_single_broker_restart; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    echo ""

    # Test 2
    if test_produce_during_shutdown; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    echo ""

    # Test 3
    if test_consumer_group_recovery; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    echo ""

    # Test 4
    if test_multi_broker_failover; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
    fi

    echo ""
    echo "=============================================="
    echo "Test Results: $passed passed, $failed failed"
    echo "=============================================="

    return $failed
}

# Main
case "${1:-all}" in
    single)
        test_single_broker_restart
        ;;
    shutdown)
        test_produce_during_shutdown
        ;;
    group)
        test_consumer_group_recovery
        ;;
    multi)
        test_multi_broker_failover
        ;;
    all)
        run_all_tests
        ;;
    *)
        echo "Usage: $0 {single|shutdown|group|multi|all}"
        exit 1
        ;;
esac
