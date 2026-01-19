#!/bin/bash
set -e

# DataCore Integration Test Runner
# Builds the project, starts the broker, runs compatibility tests, and cleans up.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
CONFIG_FILE="$ROOT_DIR/config.toml"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Building DataCore...${NC}"
cd "$ROOT_DIR" && make build

echo -e "${BLUE}Starting DataCore Broker...${NC}"

# Start broker in background
"$BIN_DIR/datacore" broker start --config "$CONFIG_FILE" > "$ROOT_DIR/broker_test.log" 2>&1 &
BROKER_PID=$!

echo "Broker PID: $BROKER_PID"

# Cleanup function
cleanup() {
    echo -e "${BLUE}Stopping broker (PID: $BROKER_PID)...${NC}"
    if ps -p $BROKER_PID > /dev/null; then
        kill $BROKER_PID
        wait $BROKER_PID 2>/dev/null
    fi
    echo "Broker stopped."
}

# Trap exit to ensure cleanup
trap cleanup EXIT

# Wait for broker to be ready
echo "Waiting for broker to start..."
MAX_RETRIES=30
for ((i=1; i<=MAX_RETRIES; i++)); do
    if nc -z localhost 9092 2>/dev/null; then
        echo -e "${GREEN}Broker is ready!${NC}"
        break
    fi
    if ! ps -p $BROKER_PID > /dev/null; then
        echo -e "${RED}Broker process died prematurely. Check broker_test.log${NC}"
        cat "$ROOT_DIR/broker_test.log"
        exit 1
    fi
    sleep 1
    if [ $i -eq $MAX_RETRIES ]; then
        echo -e "${RED}Timeout waiting for broker to start.${NC}"
        exit 1
    fi
done

echo -e "${BLUE}Running Compatibility Tests...${NC}"
# Use BOOTSTRAP_SERVER env if not set
export BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-127.0.0.1:9092}"
bash "$ROOT_DIR/tests/integration/test_kafka_compat.sh"

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All compatibility tests passed!${NC}"
else
    echo -e "${RED}Some compatibility tests failed.${NC}"
fi

exit $TEST_EXIT_CODE
