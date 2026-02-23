#!/bin/bash
set -e

echo "=== Quick Performance Test ==="
echo "Testing with 1000 messages (1KB each)"
echo ""

PROJECT_DIR="$(pwd)"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"
mkdir -p "$RESULTS_DIR"

# Memory storage config
cat > "$RESULTS_DIR/quick_config.toml" << CONFIG
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1

[storage]
engine = "memory"

[logging]
level = "warn"
output = "stdout"
CONFIG

# 서버 시작
echo "Starting DataCore..."
"$PROJECT_DIR/bin/datacore" broker start --config="$RESULTS_DIR/quick_config.toml" > "$RESULTS_DIR/quick_server.log" 2>&1 &
SERVER_PID=$!

sleep 3

if ! nc -z localhost 9092 2>/dev/null; then
    echo "Server failed to start"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

echo "Server started (PID: $SERVER_PID)"
echo ""

# Produce 테스트
echo "=== Produce Test ==="
MESSAGE=$(head -c 1024 /dev/urandom | base64 | tr -d '\n')
START=$(date +%s.%N)

for i in {1..1000}; do
    echo "$MESSAGE" | "$PROJECT_DIR/bin/datacore" cli produce \
        --bootstrap-server localhost:9092 \
        --topic test-topic \
        --partition 0 > /dev/null 2>&1 || true
done

END=$(date +%s.%N)
DURATION=$(echo "$END - $START" | bc)
THROUGHPUT=$(echo "scale=2; 1000 / $DURATION" | bc)
MB_PER_SEC=$(echo "scale=2; (1000 * 1024) / (1024 * 1024 * $DURATION)" | bc)

echo "Duration: ${DURATION}s"
echo "Throughput: ${THROUGHPUT} msg/sec"
echo "Throughput: ${MB_PER_SEC} MB/sec"
echo ""

# Consume 테스트
echo "=== Consume Test ==="
START=$(date +%s.%N)

"$PROJECT_DIR/bin/datacore" cli consume \
    --bootstrap-server localhost:9092 \
    --topic test-topic \
    --partition 0 \
    --offset 0 \
    --max-messages 1000 > /dev/null 2>&1 || true

END=$(date +%s.%N)
DURATION=$(echo "$END - $START" | bc)
THROUGHPUT=$(echo "scale=2; 1000 / $DURATION" | bc)
MB_PER_SEC=$(echo "scale=2; (1000 * 1024) / (1024 * 1024 * $DURATION)" | bc)

echo "Duration: ${DURATION}s"
echo "Throughput: ${THROUGHPUT} msg/sec"
echo "Throughput: ${MB_PER_SEC} MB/sec"
echo ""

# 정리
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "Test completed!"
