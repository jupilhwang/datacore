#!/bin/bash
# =============================================================================
# DataCore Storage Engine E2E Benchmark
# Compares Memory vs S3 storage engine performance
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
KAFKA_BIN="${KAFKA_BIN:-/Users/jhwang/works/confluent/confluent-8.1.1/bin}"
BOOTSTRAP_SERVER="localhost:9092"

# Output
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${SCRIPT_DIR}/results/${TIMESTAMP}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# =============================================================================
# Helper Functions
# =============================================================================

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_header() { echo -e "\n${CYAN}════════════════════════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}\n"; }

wait_for_broker() {
    local max_wait=30
    local count=0
    log_info "Waiting for broker to be ready..."
    while ! nc -z localhost 9092 2>/dev/null; do
        sleep 1
        count=$((count + 1))
        if [ $count -ge $max_wait ]; then
            log_error "Broker did not start within ${max_wait}s"
            return 1
        fi
    done
    sleep 2  # Extra wait for full initialization
    log_success "Broker is ready"
}

stop_broker() {
    log_info "Stopping any running DataCore broker..."
    pkill -f "datacore broker" 2>/dev/null || true
    sleep 2
}

start_broker() {
    local config=$1
    local engine=$2
    log_info "Starting broker with ${engine} engine..."
    cd "$PROJECT_ROOT"
    ./bin/datacore broker start -c "$config" > "${RESULT_DIR}/${engine}_broker.log" 2>&1 &
    local pid=$!
    echo $pid > "${RESULT_DIR}/${engine}_broker.pid"
    wait_for_broker
}

create_topic() {
    local topic=$1
    local partitions=$2
    log_info "Creating topic: ${topic} (partitions=${partitions})"
    "${KAFKA_BIN}/kafka-topics" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor 1 \
        2>/dev/null || true
    sleep 1
}

delete_topic() {
    local topic=$1
    "${KAFKA_BIN}/kafka-topics" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --delete \
        --topic "${topic}" \
        2>/dev/null || true
}

# =============================================================================
# Benchmark Functions
# =============================================================================

run_producer_test() {
    local engine=$1
    local test_name=$2
    local topic=$3
    local num_records=$4
    local record_size=$5
    local partitions=$6
    
    log_info "Producer test: ${test_name} (${num_records} records x ${record_size}B, ${partitions} partitions)"
    
    create_topic "${topic}" "${partitions}"
    
    local output_file="${RESULT_DIR}/${engine}_producer_${test_name}.txt"
    
    "${KAFKA_BIN}/kafka-producer-perf-test" \
        --topic "${topic}" \
        --num-records "${num_records}" \
        --record-size "${record_size}" \
        --throughput -1 \
        --producer-props bootstrap.servers="${BOOTSTRAP_SERVER}" acks=1 \
        2>&1 | tee "${output_file}"
    
    # Extract key metrics
    local result=$(tail -1 "${output_file}")
    echo "${engine},${test_name},producer,${result}" >> "${RESULT_DIR}/raw_results.csv"
}

run_consumer_test() {
    local engine=$1
    local test_name=$2
    local topic=$3
    local num_messages=$4
    
    log_info "Consumer test: ${test_name} (${num_messages} messages)"
    
    local output_file="${RESULT_DIR}/${engine}_consumer_${test_name}.txt"
    
    "${KAFKA_BIN}/kafka-consumer-perf-test" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --topic "${topic}" \
        --messages "${num_messages}" \
        2>&1 | tee "${output_file}"
    
    delete_topic "${topic}"
}

run_benchmark_suite() {
    local engine=$1
    local topic_prefix="bench-${engine}"
    local engine_upper=$(echo "$engine" | tr '[:lower:]' '[:upper:]')
    
    log_header "Running ${engine_upper} Engine Benchmark Suite"
    
    # Test 1: Baseline (single partition, small scale)
    run_producer_test "${engine}" "baseline" "${topic_prefix}-baseline" 10000 1024 1
    run_consumer_test "${engine}" "baseline" "${topic_prefix}-baseline" 10000
    
    # Test 2: Throughput (3 partitions, medium scale)
    run_producer_test "${engine}" "throughput" "${topic_prefix}-throughput" 50000 1024 3
    run_consumer_test "${engine}" "throughput" "${topic_prefix}-throughput" 50000
    
    # Test 3: Large messages (10KB records)
    run_producer_test "${engine}" "large_msg" "${topic_prefix}-large" 10000 10240 3
    run_consumer_test "${engine}" "large_msg" "${topic_prefix}-large" 10000
    
    # Test 4: Multi-partition (8 partitions - tests v0.27.0 parallel fetch)
    run_producer_test "${engine}" "multi_part" "${topic_prefix}-multi" 50000 1024 8
    run_consumer_test "${engine}" "multi_part" "${topic_prefix}-multi" 50000
}

# =============================================================================
# Results Processing
# =============================================================================

generate_report() {
    log_header "Generating Benchmark Report"
    
    local report_file="${RESULT_DIR}/BENCHMARK_RESULTS.md"
    
    cat > "${report_file}" << 'EOF'
# DataCore Storage Engine Benchmark Results

**Date:** TIMESTAMP_PLACEHOLDER
**DataCore Version:** 0.27.0

## Test Environment
- **Host:** macOS
- **Memory Engine:** In-memory storage with partition-level locking
- **S3 Engine:** AWS S3 (ap-northeast-2) with buffered writes

## Test Scenarios

| Test | Records | Size | Partitions | Purpose |
|------|---------|------|------------|---------|
| baseline | 10,000 | 1KB | 1 | Basic latency measurement |
| throughput | 50,000 | 1KB | 3 | Throughput measurement |
| large_msg | 10,000 | 10KB | 3 | Large message handling |
| multi_part | 50,000 | 1KB | 8 | Parallel fetch optimization (v0.27.0) |

---

## Producer Performance

| Test | Engine | Records/sec | MB/sec | Avg Latency (ms) | Max Latency (ms) |
|------|--------|-------------|--------|------------------|------------------|
EOF

    # Parse producer results
    for engine in memory s3; do
        for test in baseline throughput large_msg multi_part; do
            local file="${RESULT_DIR}/${engine}_producer_${test}.txt"
            if [ -f "$file" ]; then
                local line=$(tail -1 "$file" 2>/dev/null)
                # Parse: 50000 records sent, 12345.67 records/sec (12.34 MB/sec), 1.23 ms avg latency, 45.67 ms max latency
                local records_sec=$(echo "$line" | grep -oE '[0-9]+\.[0-9]+ records/sec' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
                local mb_sec=$(echo "$line" | grep -oE '[0-9]+\.[0-9]+ MB/sec' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
                local avg_lat=$(echo "$line" | grep -oE '[0-9]+\.[0-9]+ ms avg' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
                local max_lat=$(echo "$line" | grep -oE '[0-9]+\.[0-9]+ ms max' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
                echo "| ${test} | ${engine} | ${records_sec} | ${mb_sec} | ${avg_lat} | ${max_lat} |" >> "${report_file}"
            fi
        done
    done

    cat >> "${report_file}" << 'EOF'

---

## Consumer Performance

| Test | Engine | Records/sec | MB/sec | Total Time (ms) |
|------|--------|-------------|--------|-----------------|
EOF

    # Parse consumer results
    for engine in memory s3; do
        for test in baseline throughput large_msg multi_part; do
            local file="${RESULT_DIR}/${engine}_consumer_${test}.txt"
            if [ -f "$file" ]; then
                # Consumer output: data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
                local data_line=$(grep -E "^[0-9]" "$file" | tail -1 2>/dev/null)
                if [ -n "$data_line" ]; then
                    local mb_sec=$(echo "$data_line" | awk -F',' '{print $2}' | tr -d ' ')
                    local msg_sec=$(echo "$data_line" | awk -F',' '{print $4}' | tr -d ' ')
                    local fetch_time=$(echo "$data_line" | awk -F',' '{print $6}' | tr -d ' ')
                    echo "| ${test} | ${engine} | ${msg_sec:-N/A} | ${mb_sec:-N/A} | ${fetch_time:-N/A} |" >> "${report_file}"
                fi
            fi
        done
    done

    cat >> "${report_file}" << 'EOF'

---

## Performance Comparison Summary

### Producer Throughput Comparison

| Metric | Memory | S3 | Memory/S3 Ratio |
|--------|--------|-----|-----------------|
EOF

    # Calculate averages and ratios
    local mem_avg_rps=0
    local s3_avg_rps=0
    local mem_count=0
    local s3_count=0
    
    for test in baseline throughput large_msg multi_part; do
        local mem_file="${RESULT_DIR}/memory_producer_${test}.txt"
        local s3_file="${RESULT_DIR}/s3_producer_${test}.txt"
        
        if [ -f "$mem_file" ]; then
            local rps=$(tail -1 "$mem_file" | grep -oE '[0-9]+\.[0-9]+ records/sec' | grep -oE '[0-9]+\.[0-9]+')
            if [ -n "$rps" ]; then
                mem_avg_rps=$(echo "$mem_avg_rps + $rps" | bc)
                mem_count=$((mem_count + 1))
            fi
        fi
        
        if [ -f "$s3_file" ]; then
            local rps=$(tail -1 "$s3_file" | grep -oE '[0-9]+\.[0-9]+ records/sec' | grep -oE '[0-9]+\.[0-9]+')
            if [ -n "$rps" ]; then
                s3_avg_rps=$(echo "$s3_avg_rps + $rps" | bc)
                s3_count=$((s3_count + 1))
            fi
        fi
    done
    
    if [ $mem_count -gt 0 ] && [ $s3_count -gt 0 ]; then
        mem_avg_rps=$(echo "scale=2; $mem_avg_rps / $mem_count" | bc)
        s3_avg_rps=$(echo "scale=2; $s3_avg_rps / $s3_count" | bc)
        local ratio=$(echo "scale=2; $mem_avg_rps / $s3_avg_rps" | bc 2>/dev/null || echo "N/A")
        echo "| Avg Records/sec | ${mem_avg_rps} | ${s3_avg_rps} | ${ratio}x |" >> "${report_file}"
    fi

    cat >> "${report_file}" << 'EOF'

---

## Key Observations

### Memory Engine
- **Strengths:** Ultra-low latency (<5ms), very high throughput
- **Weaknesses:** Non-persistent, limited by available RAM
- **Use Case:** Development, testing, high-speed caching

### S3 Engine  
- **Strengths:** Durable, scalable, cost-effective storage
- **Weaknesses:** Higher latency due to network I/O
- **Use Case:** Production workloads requiring persistence

### v0.27.0 Optimizations Impact
- Multi-partition parallel fetch should show improvement in `multi_part` test
- Memory clone removal reduces latency in all Memory engine tests
- S3 lock optimization reduces contention in concurrent scenarios

---

## Raw Result Files

All detailed results are available in this directory:
- `memory_producer_*.txt` - Memory engine producer results
- `memory_consumer_*.txt` - Memory engine consumer results  
- `s3_producer_*.txt` - S3 engine producer results
- `s3_consumer_*.txt` - S3 engine consumer results
- `*_broker.log` - Broker logs during tests

EOF

    # Replace timestamp
    sed -i '' "s/TIMESTAMP_PLACEHOLDER/$(date)/" "${report_file}"
    
    log_success "Report generated: ${report_file}"
    echo ""
    cat "${report_file}"
}

# =============================================================================
# S3 Cleanup
# =============================================================================

cleanup_s3_prefix() {
    log_info "Cleaning up S3 benchmark prefix..."
    # Using AWS CLI if available
    if command -v aws &> /dev/null; then
        aws s3 rm s3://jhwang-s3/datacore-benchmark/ --recursive 2>/dev/null || true
        log_success "S3 prefix cleaned"
    else
        log_warn "AWS CLI not found, skipping S3 cleanup"
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║     DataCore Storage Engine E2E Benchmark                    ║"
    echo "║     Memory vs S3 Performance Comparison                      ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Setup
    mkdir -p "${RESULT_DIR}"
    echo "engine,test,type,result" > "${RESULT_DIR}/raw_results.csv"
    log_info "Results directory: ${RESULT_DIR}"
    
    # Check prerequisites
    if [ ! -f "${KAFKA_BIN}/kafka-producer-perf-test" ]; then
        log_error "Kafka tools not found at ${KAFKA_BIN}"
        exit 1
    fi
    
    if [ ! -f "${PROJECT_ROOT}/bin/datacore" ]; then
        log_error "DataCore binary not found. Run 'make build' first."
        exit 1
    fi
    
    # Stop any existing broker
    stop_broker
    
    # =========================================================================
    # Memory Engine Benchmark
    # =========================================================================
    start_broker "${SCRIPT_DIR}/config_memory.toml" "memory"
    run_benchmark_suite "memory"
    stop_broker
    
    # =========================================================================
    # S3 Engine Benchmark
    # =========================================================================
    cleanup_s3_prefix
    start_broker "${SCRIPT_DIR}/config_s3.toml" "s3"
    run_benchmark_suite "s3"
    stop_broker
    
    # =========================================================================
    # Generate Report
    # =========================================================================
    generate_report
    
    log_success "Benchmark completed!"
    log_info "Results saved to: ${RESULT_DIR}"
}

# Run with optional single engine test
if [ "$1" == "memory" ]; then
    mkdir -p "${RESULT_DIR}"
    stop_broker
    start_broker "${SCRIPT_DIR}/config_memory.toml" "memory"
    run_benchmark_suite "memory"
    stop_broker
elif [ "$1" == "s3" ]; then
    mkdir -p "${RESULT_DIR}"
    stop_broker
    cleanup_s3_prefix
    start_broker "${SCRIPT_DIR}/config_s3.toml" "s3"
    run_benchmark_suite "s3"
    stop_broker
else
    main
fi
