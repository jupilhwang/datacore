#!/bin/bash
#
# Debug Compression Test Script for DataCore
# This script tests compression with different methods and compares results
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TEST_PREFIX="debug-compress-$(date +%s)"
TIMEOUT=10

# Kafka CLI paths
KAFKA_HOME="${KAFKA_HOME:-/Users/jhwang/works/confluent/confluent-8.1.1}"
KAFKA_PRODUCER="${KAFKA_HOME}/bin/kafka-console-producer"
KAFKA_CONSUMER="${KAFKA_HOME}/bin/kafka-console-consumer"
KAFKA_TOPICS="${KAFKA_HOME}/bin/kafka-topics"

echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║         DataCore Compression Debug Test                                    ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Bootstrap Server: ${BOOTSTRAP_SERVER}${NC}"
echo -e "${BLUE}Kafka CLI Path:   ${KAFKA_PRODUCER}${NC}"
echo ""

# Test 1: Check if broker is running
echo -e "${YELLOW}[1/7]${NC} Checking if broker is running..."
if lsof -i :9092 2>/dev/null | grep -q LISTEN; then
	echo -e "  ${GREEN}✓${NC} Broker is running on port 9092"
else
	echo -e "  ${RED}✗${NC} Broker is NOT running on port 9092"
	exit 1
fi

# Test 2: Check Kafka CLI tools
echo ""
echo -e "${YELLOW}[2/7]${NC} Checking Kafka CLI tools..."
if [ -x "$KAFKA_PRODUCER" ]; then
	echo -e "  ${GREEN}✓${NC} kafka-console-producer found"
else
	echo -e "  ${RED}✗${NC} kafka-console-producer NOT found"
	exit 1
fi

if [ -x "$KAFKA_CONSUMER" ]; then
	echo -e "  ${GREEN}✓${NC} kafka-console-consumer found"
else
	echo -e "  ${RED}✗${NC} kafka-console-consumer NOT found"
	exit 1
fi

# Test 3: Test plain (uncompressed) message first
echo ""
echo -e "${YELLOW}[3/7]${NC} Testing plain (uncompressed) message..."
PLAIN_TOPIC="${TEST_PREFIX}-plain"
echo "  Creating topic: $PLAIN_TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
	--create --topic "$PLAIN_TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true

PLAIN_MESSAGE="Plain test message $(date +%s)"
echo "  Producing message: $PLAIN_MESSAGE"
echo "$PLAIN_MESSAGE" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$PLAIN_TOPIC" 2>&1 | head -20

sleep 1

echo "  Consuming message..."
RECEIVED=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$PLAIN_TOPIC" \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null || echo "")

if echo "$RECEIVED" | grep -q "$PLAIN_MESSAGE"; then
	echo -e "  ${GREEN}✓${NC} Plain message test PASSED"
else
	echo -e "  ${RED}✗${NC} Plain message test FAILED"
	echo "  Expected: $PLAIN_MESSAGE"
	echo "  Received: $RECEIVED"
fi

# Test 4: Test Snappy compression with Kafka CLI
echo ""
echo -e "${YELLOW}[4/7]${NC} Testing Snappy compression with Kafka CLI..."
SNAPPY_TOPIC="${TEST_PREFIX}-snappy"
echo "  Creating topic: $SNAPPY_TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
	--create --topic "$SNAPPY_TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true

SNAPPY_MESSAGE="Snappy test message $(date +%s)"
echo "  Producing message with --compression-codec snappy"
echo "  Message: $SNAPPY_MESSAGE"

# Capture all output including errors
PRODUCE_OUTPUT=$(echo "$SNAPPY_MESSAGE" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$SNAPPY_TOPIC" \
	--compression-codec snappy 2>&1) || true

echo "  Producer output:"
echo "$PRODUCE_OUTPUT" | head -20

sleep 1

echo "  Consuming message..."
RECEIVED=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$SNAPPY_TOPIC" \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null || echo "")

echo "  Consumer output: $RECEIVED"

if echo "$RECEIVED" | grep -q "$SNAPPY_MESSAGE"; then
	echo -e "  ${GREEN}✓${NC} Snappy compression test PASSED"
else
	echo -e "  ${RED}✗${NC} Snappy compression test FAILED"
	echo "  Expected: $SNAPPY_MESSAGE"
	echo "  Received: $RECEIVED"
fi

# Test 5: Test Gzip compression with Kafka CLI
echo ""
echo -e "${YELLOW}[5/7]${NC} Testing Gzip compression with Kafka CLI..."
GZIP_TOPIC="${TEST_PREFIX}-gzip"
echo "  Creating topic: $GZIP_TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
	--create --topic "$GZIP_TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true

GZIP_MESSAGE="Gzip test message $(date +%s)"
echo "  Producing message with --compression-codec gzip"
echo "  Message: $GZIP_MESSAGE"

PRODUCE_OUTPUT=$(echo "$GZIP_MESSAGE" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$GZIP_TOPIC" \
	--compression-codec gzip 2>&1) || true

echo "  Producer output:"
echo "$PRODUCE_OUTPUT" | head -20

sleep 1

echo "  Consuming message..."
RECEIVED=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$GZIP_TOPIC" \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null || echo "")

echo "  Consumer output: $RECEIVED"

if echo "$RECEIVED" | grep -q "$GZIP_MESSAGE"; then
	echo -e "  ${GREEN}✓${NC} Gzip compression test PASSED"
else
	echo -e "  ${RED}✗${NC} Gzip compression test FAILED"
	echo "  Expected: $GZIP_MESSAGE"
	echo "  Received: $RECEIVED"
fi

# Test 6: Test LZ4 compression with Kafka CLI
echo ""
echo -e "${YELLOW}[6/7]${NC} Testing LZ4 compression with Kafka CLI..."
LZ4_TOPIC="${TEST_PREFIX}-lz4"
echo "  Creating topic: $LZ4_TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
	--create --topic "$LZ4_TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true

LZ4_MESSAGE="LZ4 test message $(date +%s)"
echo "  Producing message with --compression-codec lz4"
echo "  Message: $LZ4_MESSAGE"

PRODUCE_OUTPUT=$(echo "$LZ4_MESSAGE" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$LZ4_TOPIC" \
	--compression-codec lz4 2>&1) || true

echo "  Producer output:"
echo "$PRODUCE_OUTPUT" | head -20

sleep 1

echo "  Consuming message..."
RECEIVED=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$LZ4_TOPIC" \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null || echo "")

echo "  Consumer output: $RECEIVED"

if echo "$RECEIVED" | grep -q "$LZ4_MESSAGE"; then
	echo -e "  ${GREEN}✓${NC} LZ4 compression test PASSED"
else
	echo -e "  ${RED}✗${NC} LZ4 compression test FAILED"
	echo "  Expected: $LZ4_MESSAGE"
	echo "  Received: $RECEIVED"
fi

# Test 7: Test Zstd compression with Kafka CLI
echo ""
echo -e "${YELLOW}[7/7]${NC} Testing Zstd compression with Kafka CLI..."
ZSTD_TOPIC="${TEST_PREFIX}-zstd"
echo "  Creating topic: $ZSTD_TOPIC"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" \
	--create --topic "$ZSTD_TOPIC" --partitions 1 --replication-factor 1 2>/dev/null || true

ZSTD_MESSAGE="Zstd test message $(date +%s)"
echo "  Producing message with --compression-codec zstd"
echo "  Message: $ZSTD_MESSAGE"

PRODUCE_OUTPUT=$(echo "$ZSTD_MESSAGE" | timeout "$TIMEOUT" "$KAFKA_PRODUCER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$ZSTD_TOPIC" \
	--compression-codec zstd 2>&1) || true

echo "  Producer output:"
echo "$PRODUCE_OUTPUT" | head -20

sleep 1

echo "  Consuming message..."
RECEIVED=$(timeout "$TIMEOUT" "$KAFKA_CONSUMER" \
	--bootstrap-server "$BOOTSTRAP_SERVER" \
	--topic "$ZSTD_TOPIC" \
	--from-beginning \
	--max-messages 1 \
	--timeout-ms $((TIMEOUT * 1000)) 2>/dev/null || echo "")

echo "  Consumer output: $RECEIVED"

if echo "$RECEIVED" | grep -q "$ZSTD_MESSAGE"; then
	echo -e "  ${GREEN}✓${NC} Zstd compression test PASSED"
else
	echo -e "  ${RED}✗${NC} Zstd compression test FAILED"
	echo "  Expected: $ZSTD_MESSAGE"
	echo "  Received: $RECEIVED"
fi

# Cleanup
echo ""
echo -e "${YELLOW}[Cleanup]${NC} Removing test topics..."
for topic in "$PLAIN_TOPIC" "$SNAPPY_TOPIC" "$GZIP_TOPIC" "$LZ4_TOPIC" "$ZSTD_TOPIC"; do
	"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --delete --topic "$topic" 2>/dev/null || true
done

echo ""
echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║         Debug Complete                                                      ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}To view broker logs:${NC}"
echo -e "  tail -f /tmp/datacore_broker.log"
echo ""
