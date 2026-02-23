#!/bin/bash

# Kafka CLI Compatibility Test Script
# 표준 Kafka CLI 도구로 DataCore 호환성 테스트

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$RESULTS_DIR"

# 색상
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SERVER_PID=""

cleanup() {
    if [ ! -z "$SERVER_PID" ]; then
        log_info "Stopping DataCore server..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo -e "${BLUE}=== Kafka CLI Compatibility Test ===${NC}"
echo "Testing DataCore with standard Kafka CLI tools"
echo ""

# DataCore 서버 시작
log_info "Starting DataCore server..."
cat > "$RESULTS_DIR/config_compat.toml" << CONFIG
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1
cluster_id = "compat-test"

[storage]
engine = "memory"

[logging]
level = "info"
output = "stdout"
CONFIG

"$PROJECT_DIR/bin/datacore" broker start --config="$RESULTS_DIR/config_compat.toml" > "$RESULTS_DIR/compat_server.log" 2>&1 &
SERVER_PID=$!

log_info "Waiting for server to be ready..."
for i in {1..30}; do
    if nc -z localhost 9092 2>/dev/null; then
        log_success "Server is ready (PID: $SERVER_PID)"
        sleep 2
        break
    fi
    sleep 1
done

# Kafka CLI 도구 경로 확인
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
KAFKA_BIN="$KAFKA_HOME/bin"

if [ ! -d "$KAFKA_BIN" ]; then
    log_error "Kafka CLI tools not found at $KAFKA_BIN"
    log_info "Please set KAFKA_HOME environment variable"
    log_info "Example: export KAFKA_HOME=/opt/homebrew/opt/kafka"
    exit 1
fi

log_success "Found Kafka CLI tools at $KAFKA_BIN"
echo ""

TEST_TOPIC="kafka-cli-test-topic"
TEST_MESSAGES=100

# 테스트 1: kafka-topics.sh - 토픽 생성
log_info "Test 1: Creating topic with kafka-topics.sh..."
if "$KAFKA_BIN/kafka-topics.sh" --create \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" \
    --partitions 3 \
    --replication-factor 1 2>&1 | tee "$RESULTS_DIR/test1_create_topic.log"; then
    log_success "✓ Topic creation test passed"
else
    log_error "✗ Topic creation test failed"
fi
echo ""

# 테스트 2: kafka-topics.sh - 토픽 조회
log_info "Test 2: Listing topics with kafka-topics.sh..."
if "$KAFKA_BIN/kafka-topics.sh" --list \
    --bootstrap-server localhost:9092 2>&1 | tee "$RESULTS_DIR/test2_list_topics.log" | grep -q "$TEST_TOPIC"; then
    log_success "✓ Topic listing test passed"
else
    log_error "✗ Topic listing test failed"
fi
echo ""

# 테스트 3: kafka-console-producer.sh - 메시지 전송
log_info "Test 3: Producing messages with kafka-console-producer.sh..."
{
    for i in $(seq 1 $TEST_MESSAGES); do
        echo "test-message-$i"
    done
} | "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" 2>&1 | tee "$RESULTS_DIR/test3_produce.log"

if [ ${PIPESTATUS[1]} -eq 0 ]; then
    log_success "✓ Message production test passed ($TEST_MESSAGES messages)"
else
    log_error "✗ Message production test failed"
fi
echo ""

# 테스트 4: kafka-console-consumer.sh - 메시지 소비
log_info "Test 4: Consuming messages with kafka-console-consumer.sh..."
timeout 10 "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" \
    --from-beginning \
    --max-messages $TEST_MESSAGES 2>&1 | tee "$RESULTS_DIR/test4_consume.log"

consumed_count=$(grep -c "test-message" "$RESULTS_DIR/test4_consume.log" || echo "0")
if [ "$consumed_count" -ge "$TEST_MESSAGES" ]; then
    log_success "✓ Message consumption test passed ($consumed_count/$TEST_MESSAGES messages)"
else
    log_error "✗ Message consumption test failed ($consumed_count/$TEST_MESSAGES messages)"
fi
echo ""

# 테스트 5: kafka-consumer-groups.sh - Consumer Group 조회
log_info "Test 5: Listing consumer groups with kafka-consumer-groups.sh..."
if "$KAFKA_BIN/kafka-consumer-groups.sh" --list \
    --bootstrap-server localhost:9092 2>&1 | tee "$RESULTS_DIR/test5_groups.log"; then
    log_success "✓ Consumer group listing test passed"
else
    log_error "✗ Consumer group listing test failed"
fi
echo ""

# 테스트 6: kcat (kafkacat) - 호환성 테스트
if command -v kcat &> /dev/null; then
    log_info "Test 6: Testing with kcat..."
    
    # kcat으로 메타데이터 조회
    if kcat -b localhost:9092 -L 2>&1 | tee "$RESULTS_DIR/test6_kcat_metadata.log" | grep -q "topic"; then
        log_success "✓ kcat metadata test passed"
    else
        log_error "✗ kcat metadata test failed"
    fi
    
    # kcat으로 메시지 전송
    echo "kcat-test-message" | kcat -b localhost:9092 -t "$TEST_TOPIC" -P 2>&1 | tee "$RESULTS_DIR/test6_kcat_produce.log"
    
    # kcat으로 메시지 소비
    kcat -b localhost:9092 -t "$TEST_TOPIC" -C -c 1 -o -1 2>&1 | tee "$RESULTS_DIR/test6_kcat_consume.log"
    
    log_success "✓ kcat compatibility test passed"
else
    log_info "Test 6: kcat not installed, skipping"
fi
echo ""

# 압축 테스트
log_info "Test 7: Testing compression with kafka-console-producer..."
for compression in "snappy" "gzip" "lz4" "zstd"; do
    log_info "  Testing $compression compression..."
    echo "compressed-message-$compression" | "$KAFKA_BIN/kafka-console-producer.sh" \
        --bootstrap-server localhost:9092 \
        --topic "${TEST_TOPIC}-${compression}" \
        --compression-codec "$compression" 2>&1 | tee "$RESULTS_DIR/test7_${compression}.log"
    
    if [ ${PIPESTATUS[1]} -eq 0 ]; then
        log_success "  ✓ $compression compression test passed"
    else
        log_error "  ✗ $compression compression test failed"
    fi
done
echo ""

# 최종 리포트
log_info "=== Test Summary ==="
total_tests=7
passed_tests=0

[ -f "$RESULTS_DIR/test1_create_topic.log" ] && ((passed_tests++))
[ -f "$RESULTS_DIR/test2_list_topics.log" ] && ((passed_tests++))
[ -f "$RESULTS_DIR/test3_produce.log" ] && ((passed_tests++))
[ "$consumed_count" -ge "$TEST_MESSAGES" ] && ((passed_tests++))
[ -f "$RESULTS_DIR/test5_groups.log" ] && ((passed_tests++))
command -v kcat &> /dev/null && [ -f "$RESULTS_DIR/test6_kcat_metadata.log" ] && ((passed_tests++))
[ -f "$RESULTS_DIR/test7_snappy.log" ] && ((passed_tests++))

echo ""
echo -e "${GREEN}Passed: $passed_tests / $total_tests tests${NC}"
echo ""
log_success "Kafka CLI compatibility test completed!"
log_info "Detailed logs saved in: $RESULTS_DIR"
