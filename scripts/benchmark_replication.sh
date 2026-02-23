#!/bin/bash
set -e

echo "=========================================="
echo "DataCore Replication Benchmark"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

# Configuration
MESSAGE_COUNT=1000
MESSAGE_SIZE=1024  # 1KB
TOPIC="benchmark-replication"

# Results
declare -A RESULTS

# Start brokers
log_info "Starting brokers..."
./bin/datacore broker start --config=config-broker1.toml > broker1-bench.log 2>&1 &
BROKER1_PID=$!
sleep 3

./bin/datacore broker start --config=config-broker2.toml > broker2-bench.log 2>&1 &
BROKER2_PID=$!
sleep 3

cleanup() {
    log_info "Cleaning up..."
    kill $BROKER1_PID $BROKER2_PID 2>/dev/null || true
    sleep 2
}
trap cleanup EXIT

# Create topic
log_info "Creating topic..."
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
    --create --topic $TOPIC --partitions 1 --replication-factor 1 \
    --if-not-exists >/dev/null 2>&1 || true

# Generate test data
log_info "Generating test data..."
TEST_FILE=$(mktemp)
for i in $(seq 1 $MESSAGE_COUNT); do
    # Generate 1KB message
    head -c $MESSAGE_SIZE </dev/urandom | base64 | tr -d '\n' >> $TEST_FILE
    echo "" >> $TEST_FILE
done

# Benchmark 1: Single broker, acks=0
log_info "Benchmark 1: Single broker, acks=0"
START=$(date +%s%3N)
cat $TEST_FILE | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic $TOPIC \
    --request-required-acks 0 >/dev/null 2>&1
END=$(date +%s%3N)
ACKS0_TIME=$((END - START))
RESULTS[acks0]=$ACKS0_TIME
log_info "Time: ${ACKS0_TIME}ms"

sleep 5

# Benchmark 2: Single broker, acks=1
log_info "Benchmark 2: Single broker, acks=1"
START=$(date +%s%3N)
cat $TEST_FILE | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic $TOPIC \
    --request-required-acks 1 >/dev/null 2>&1
END=$(date +%s%3N)
ACKS1_TIME=$((END - START))
RESULTS[acks1]=$ACKS1_TIME
log_info "Time: ${ACKS1_TIME}ms"

sleep 5

# Benchmark 3: Multi broker, acks=-1
log_info "Benchmark 3: Multi broker, acks=-1 (with replication)"
START=$(date +%s%3N)
cat $TEST_FILE | kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:9092 \
    --topic $TOPIC \
    --request-required-acks -1 >/dev/null 2>&1
END=$(date +%s%3N)
ACKS_ALL_TIME=$((END - START))
RESULTS[acks_all]=$ACKS_ALL_TIME
log_info "Time: ${ACKS_ALL_TIME}ms"

# Calculate metrics
log_info ""
log_info "=========================================="
log_info "Benchmark Results"
log_info "=========================================="
log_info "Message count: $MESSAGE_COUNT"
log_info "Message size: ${MESSAGE_SIZE} bytes"
log_info ""

# Throughput
THROUGHPUT_ACKS0=$((MESSAGE_COUNT * 1000 / ACKS0_TIME))
THROUGHPUT_ACKS1=$((MESSAGE_COUNT * 1000 / ACKS1_TIME))
THROUGHPUT_ACKS_ALL=$((MESSAGE_COUNT * 1000 / ACKS_ALL_TIME))

log_info "acks=0  : ${ACKS0_TIME}ms, ${THROUGHPUT_ACKS0} msg/sec"
log_info "acks=1  : ${ACKS1_TIME}ms, ${THROUGHPUT_ACKS1} msg/sec"
log_info "acks=-1 : ${ACKS_ALL_TIME}ms, ${THROUGHPUT_ACKS_ALL} msg/sec"
log_info ""

# Latency per message
LATENCY_ACKS0=$(awk "BEGIN {printf \"%.2f\", $ACKS0_TIME / $MESSAGE_COUNT}")
LATENCY_ACKS1=$(awk "BEGIN {printf \"%.2f\", $ACKS1_TIME / $MESSAGE_COUNT}")
LATENCY_ACKS_ALL=$(awk "BEGIN {printf \"%.2f\", $ACKS_ALL_TIME / $MESSAGE_COUNT}")

log_info "Average latency per message:"
log_info "acks=0  : ${LATENCY_ACKS0}ms"
log_info "acks=1  : ${LATENCY_ACKS1}ms"
log_info "acks=-1 : ${LATENCY_ACKS_ALL}ms"
log_info ""

# Replication overhead
REPL_OVERHEAD=$((ACKS_ALL_TIME - ACKS0_TIME))
REPL_OVERHEAD_PER_MSG=$(awk "BEGIN {printf \"%.2f\", $REPL_OVERHEAD / $MESSAGE_COUNT}")

log_info "Replication overhead:"
log_info "Total: ${REPL_OVERHEAD}ms"
log_info "Per message: ${REPL_OVERHEAD_PER_MSG}ms"
log_info ""

# Check S3 flush time from logs
log_info "Checking S3 flush times from logs..."
if grep -q "flush.*completed" broker1-bench.log; then
    FLUSH_TIMES=$(grep "flush.*completed" broker1-bench.log | grep -oE '[0-9]+ms' | head -5)
    log_info "Sample S3 flush times: $FLUSH_TIMES"
fi

# Save results
RESULT_FILE="benchmark_results/replication_$(date +%Y%m%d_%H%M%S).txt"
mkdir -p benchmark_results
cat > $RESULT_FILE << ENDRESULT
DataCore Replication Benchmark Results
=======================================
Date: $(date)
Message count: $MESSAGE_COUNT
Message size: $MESSAGE_SIZE bytes

Timing Results:
- acks=0  : ${ACKS0_TIME}ms, ${THROUGHPUT_ACKS0} msg/sec, ${LATENCY_ACKS0}ms/msg
- acks=1  : ${ACKS1_TIME}ms, ${THROUGHPUT_ACKS1} msg/sec, ${LATENCY_ACKS1}ms/msg
- acks=-1 : ${ACKS_ALL_TIME}ms, ${THROUGHPUT_ACKS_ALL} msg/sec, ${LATENCY_ACKS_ALL}ms/msg

Replication Overhead:
- Total: ${REPL_OVERHEAD}ms
- Per message: ${REPL_OVERHEAD_PER_MSG}ms

ENDRESULT

log_info "Results saved to: $RESULT_FILE"

# Cleanup
rm -f $TEST_FILE
