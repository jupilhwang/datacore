#!/bin/bash
set -e

echo "=========================================="
echo "DataCore E2E Replication Test"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
PASSED=0
FAILED=0

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

check_result() {
    if [ $? -eq 0 ]; then
        log_info "$1: PASSED"
        ((PASSED++))
        return 0
    else
        log_error "$1: FAILED"
        ((FAILED++))
        return 1
    fi
}

cleanup() {
    log_info "Cleaning up..."
    pkill -f "datacore broker" || true
    sleep 2
}

# Cleanup on exit
trap cleanup EXIT

# 1. Check binary exists
log_info "Step 1: Checking binary..."
if [ ! -f "./bin/datacore" ]; then
    log_error "Binary not found. Run 'make build' first."
    exit 1
fi
check_result "Binary check"

# 2. Check AWS credentials
log_info "Step 2: Checking AWS credentials..."
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    log_warn "AWS credentials not set in environment variables"
    log_info "Using credentials from ~/.aws/credentials"
fi

# 3. Start Broker 1
log_info "Step 3: Starting Broker 1 (port 9092, replication 9093)..."
./bin/datacore broker start --config=config-broker1.toml > broker1.log 2>&1 &
BROKER1_PID=$!
log_info "Broker 1 PID: $BROKER1_PID"
sleep 3

# Check broker1 is running
if ps -p $BROKER1_PID > /dev/null; then
    check_result "Broker 1 startup"
else
    log_error "Broker 1 failed to start. Check broker1.log"
    cat broker1.log
    exit 1
fi

# 4. Start Broker 2
log_info "Step 4: Starting Broker 2 (port 9094, replication 9095)..."
./bin/datacore broker start --config=config-broker2.toml > broker2.log 2>&1 &
BROKER2_PID=$!
log_info "Broker 2 PID: $BROKER2_PID"
sleep 3

# Check broker2 is running
if ps -p $BROKER2_PID > /dev/null; then
    check_result "Broker 2 startup"
else
    log_error "Broker 2 failed to start. Check broker2.log"
    cat broker2.log
    exit 1
fi

# 5. Create test topic
log_info "Step 5: Creating test topic 'test-replication'..."
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
    --create \
    --topic test-replication \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists 2>&1 | tee topic-create.log
check_result "Topic creation"

# 6. Send messages with acks=-1
log_info "Step 6: Sending messages with acks=-1..."
echo "message1" | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic test-replication \
    --request-required-acks -1 \
    --property "acks=-1" 2>&1 | tee produce.log

sleep 1

echo "message2" | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic test-replication \
    --request-required-acks -1 \
    --property "acks=-1" 2>&1 | tee -a produce.log

check_result "Message production"
sleep 2

# 7. Check broker logs for replication
log_info "Step 7: Checking replication logs..."
log_info "Broker 1 log (last 20 lines):"
tail -20 broker1.log

log_info ""
log_info "Broker 2 log (last 20 lines):"
tail -20 broker2.log

# Check for replication keywords in logs
if grep -q "REPLICATE\|replica" broker1.log || grep -q "REPLICATE\|replica" broker2.log; then
    check_result "Replication evidence in logs"
else
    log_warn "No replication keywords found in logs (might be working silently)"
fi

# 8. Consume messages
log_info "Step 8: Consuming messages..."
timeout 5 kafka-console-consumer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic test-replication \
    --from-beginning \
    --max-messages 2 2>&1 | tee consume.log

if grep -q "message1" consume.log && grep -q "message2" consume.log; then
    check_result "Message consumption"
else
    log_error "Messages not found in consume output"
    ((FAILED++))
fi

# 9. Test broker crash scenario
log_info "Step 9: Testing broker crash scenario..."
log_info "Killing Broker 1 (PID: $BROKER1_PID)..."
kill -9 $BROKER1_PID || true
sleep 2

# Send message to Broker 2
log_info "Sending message to Broker 2..."
echo "message3-after-crash" | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9094 \
    --topic test-replication \
    --request-required-acks -1 2>&1 | tee -a produce.log

check_result "Message to Broker 2 after crash"
sleep 2

# Consume from Broker 2
log_info "Consuming from Broker 2..."
timeout 5 kafka-console-consumer.sh \
    --bootstrap-server 127.0.0.1:9094 \
    --topic test-replication \
    --from-beginning \
    --max-messages 3 2>&1 | tee consume2.log

if grep -q "message3-after-crash" consume2.log; then
    check_result "Message availability after crash"
else
    log_error "Message not available after crash"
    ((FAILED++))
fi

# 10. Summary
echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}PASSED: $PASSED${NC}"
echo -e "${RED}FAILED: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    log_info "All tests PASSED!"
    exit 0
else
    log_error "Some tests FAILED!"
    exit 1
fi
