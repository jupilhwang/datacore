#!/bin/bash
# E2E 테스트 스크립트 (메모리 스토리지)

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=============================================="
echo "DataCore E2E Test (Memory Storage)"
echo "=============================================="
echo ""

# Cleanup 함수
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f 'datacore broker' 2>/dev/null || true
    sleep 2
}

trap cleanup EXIT

# 1. 바이너리 확인
echo -e "${GREEN}Step 1: Checking binary...${NC}"
if [ ! -f "./bin/datacore" ]; then
    echo -e "${RED}ERROR: Binary not found${NC}"
    exit 1
fi
echo "✓ Binary found"
echo ""

# 2. 기존 프로세스 정리
echo -e "${GREEN}Step 2: Cleaning up old processes...${NC}"
cleanup
echo "✓ Cleanup complete"
echo ""

# 3. Broker 1 시작 (메모리 스토리지)
echo -e "${GREEN}Step 3: Starting Broker 1...${NC}"
./bin/datacore broker start --config=config-broker1-memory.toml > broker1-mem.log 2>&1 &
BROKER1_PID=$!
sleep 5

if ! ps -p $BROKER1_PID > /dev/null; then
    echo -e "${RED}ERROR: Broker 1 failed to start${NC}"
    echo "Log:"
    tail -50 broker1-mem.log
    exit 1
fi
echo "✓ Broker 1 started (PID: $BROKER1_PID)"
echo ""

# 4. 로그 확인
echo -e "${GREEN}Step 4: Checking Broker 1 logs...${NC}"
sleep 2
echo "Last 20 lines of broker1-mem.log:"
tail -20 broker1-mem.log
echo ""

# 5. 스토리지 엔진 확인
echo -e "${GREEN}Step 5: Verifying storage engine...${NC}"
if grep -q "Initializing Memory storage" broker1-mem.log; then
    echo "✓ Memory storage initialized"
else
    echo -e "${RED}ERROR: Memory storage not initialized${NC}"
    grep "Initializing.*storage" broker1-mem.log || true
    exit 1
fi
echo ""

# 6. 복제 확인
echo -e "${GREEN}Step 6: Verifying replication...${NC}"
if grep -q "Replication manager started" broker1-mem.log; then
    echo "✓ Replication enabled"
elif grep -q "Replication disabled" broker1-mem.log; then
    echo -e "${YELLOW}WARNING: Replication disabled${NC}"
    echo "Checking configuration..."
    grep -E "(cluster_brokers|replication)" config-broker1-memory.toml | head -10
else
    echo -e "${RED}ERROR: No replication status found${NC}"
fi
echo ""

# 7. Kafka 호환성 테스트
echo -e "${GREEN}Step 7: Testing Kafka compatibility...${NC}"
echo "test-message-1" | kafka-console-producer --broker-list localhost:9092 --topic test-topic 2>&1 || true
sleep 2

kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning --max-messages 1 --timeout-ms 5000 2>&1 || true
echo ""

# 8. 요약
echo "=============================================="
echo "Test Complete"
echo "=============================================="
echo ""
echo "Broker 1 PID: $BROKER1_PID"
echo "Log file: broker1-mem.log"
echo ""
echo -e "${GREEN}✓ Test completed${NC}"
