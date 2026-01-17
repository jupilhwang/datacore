#!/bin/bash
# DataCore Performance Benchmark Suite
# Comprehensive performance testing using Kafka perf tools

set -e

# =============================================================================
# Configuration
# =============================================================================

# Kafka tools path
KAFKA_BIN="${KAFKA_BIN:-/Users/jhwang/works/confluent/confluent-8.1.1/bin}"

# DataCore connection
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

# Test configuration
TOPIC_PREFIX="perf-test"
NUM_RECORDS="${NUM_RECORDS:-100000}"
RECORD_SIZE="${RECORD_SIZE:-1024}"
THROUGHPUT="${THROUGHPUT:--1}"  # -1 = unlimited
NUM_PARTITIONS="${NUM_PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

# Output directory
OUTPUT_DIR="./benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${OUTPUT_DIR}/${TIMESTAMP}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Kafka tools
    if [ ! -f "${KAFKA_BIN}/kafka-producer-perf-test" ]; then
        log_error "Kafka producer perf tool not found at ${KAFKA_BIN}"
        log_info "Set KAFKA_BIN environment variable to your Kafka bin directory"
        exit 1
    fi
    
    if [ ! -f "${KAFKA_BIN}/kafka-consumer-perf-test" ]; then
        log_error "Kafka consumer perf tool not found at ${KAFKA_BIN}"
        exit 1
    fi
    
    # Check broker connectivity
    if ! nc -z $(echo $BOOTSTRAP_SERVER | tr ':' ' ') 2>/dev/null; then
        log_error "Cannot connect to broker at ${BOOTSTRAP_SERVER}"
        log_info "Start DataCore: ./bin/datacore broker start"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

setup_output_dir() {
    mkdir -p "${RESULT_DIR}"
    log_info "Results will be saved to: ${RESULT_DIR}"
}

create_topic() {
    local topic_name=$1
    local partitions=${2:-$NUM_PARTITIONS}
    
    log_info "Creating topic: ${topic_name} (partitions=${partitions})"
    
    "${KAFKA_BIN}/kafka-topics" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --topic "${topic_name}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        2>/dev/null || true
}

delete_topic() {
    local topic_name=$1
    
    log_info "Deleting topic: ${topic_name}"
    
    "${KAFKA_BIN}/kafka-topics" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --delete \
        --topic "${topic_name}" \
        2>/dev/null || true
}

# =============================================================================
# Benchmark Tests
# =============================================================================

run_producer_perf_test() {
    local test_name=$1
    local topic=$2
    local num_records=$3
    local record_size=$4
    local throughput=$5
    local extra_args=${6:-""}
    
    log_info "Running producer test: ${test_name}"
    log_info "  Records: ${num_records}, Size: ${record_size}B, Throughput: ${throughput}"
    
    local output_file="${RESULT_DIR}/producer_${test_name}.txt"
    
    "${KAFKA_BIN}/kafka-producer-perf-test" \
        --topic "${topic}" \
        --num-records "${num_records}" \
        --record-size "${record_size}" \
        --throughput "${throughput}" \
        --producer-props bootstrap.servers="${BOOTSTRAP_SERVER}" acks=1 \
        ${extra_args} \
        2>&1 | tee "${output_file}"
    
    log_success "Producer test completed: ${output_file}"
}

run_consumer_perf_test() {
    local test_name=$1
    local topic=$2
    local num_messages=$3
    local extra_args=${4:-""}
    
    log_info "Running consumer test: ${test_name}"
    log_info "  Messages: ${num_messages}"
    
    local output_file="${RESULT_DIR}/consumer_${test_name}.txt"
    
    "${KAFKA_BIN}/kafka-consumer-perf-test" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --topic "${topic}" \
        --messages "${num_messages}" \
        --show-detailed-stats \
        ${extra_args} \
        2>&1 | tee "${output_file}"
    
    log_success "Consumer test completed: ${output_file}"
}

# =============================================================================
# Test Scenarios
# =============================================================================

test_baseline() {
    log_info "=========================================="
    log_info "Test 1: Baseline Performance"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-baseline"
    create_topic "${topic}" 1
    sleep 2
    
    # Producer test
    run_producer_perf_test "baseline" "${topic}" "${NUM_RECORDS}" "${RECORD_SIZE}" "-1"
    
    # Consumer test
    run_consumer_perf_test "baseline" "${topic}" "${NUM_RECORDS}"
    
    delete_topic "${topic}"
}

test_throughput_scaling() {
    log_info "=========================================="
    log_info "Test 2: Throughput with Different Record Sizes"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-size"
    create_topic "${topic}" 3
    sleep 2
    
    local sizes=(100 512 1024 4096 10240)
    
    for size in "${sizes[@]}"; do
        log_info "Testing record size: ${size} bytes"
        run_producer_perf_test "size_${size}b" "${topic}" 50000 "${size}" "-1"
    done
    
    delete_topic "${topic}"
}

test_partition_scaling() {
    log_info "=========================================="
    log_info "Test 3: Partition Scaling"
    log_info "=========================================="
    
    local partitions=(1 2 4 8)
    
    for p in "${partitions[@]}"; do
        local topic="${TOPIC_PREFIX}-partitions-${p}"
        create_topic "${topic}" "${p}"
        sleep 2
        
        run_producer_perf_test "partitions_${p}" "${topic}" 50000 1024 "-1"
        run_consumer_perf_test "partitions_${p}" "${topic}" 50000
        
        delete_topic "${topic}"
    done
}

test_batching() {
    log_info "=========================================="
    log_info "Test 4: Batching Performance"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-batching"
    create_topic "${topic}" 3
    sleep 2
    
    # Small batch
    run_producer_perf_test "batch_small" "${topic}" 50000 1024 "-1" \
        "--producer-props batch.size=1024 linger.ms=0"
    
    # Medium batch
    run_producer_perf_test "batch_medium" "${topic}" 50000 1024 "-1" \
        "--producer-props batch.size=16384 linger.ms=5"
    
    # Large batch
    run_producer_perf_test "batch_large" "${topic}" 50000 1024 "-1" \
        "--producer-props batch.size=65536 linger.ms=10"
    
    delete_topic "${topic}"
}

test_acks_modes() {
    log_info "=========================================="
    log_info "Test 5: ACK Modes Performance"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-acks"
    create_topic "${topic}" 3
    sleep 2
    
    # acks=0 (fire and forget)
    run_producer_perf_test "acks_0" "${topic}" 50000 1024 "-1" \
        "--producer-props acks=0"
    
    # acks=1 (leader only)
    run_producer_perf_test "acks_1" "${topic}" 50000 1024 "-1" \
        "--producer-props acks=1"
    
    delete_topic "${topic}"
}

test_sustained_throughput() {
    log_info "=========================================="
    log_info "Test 6: Sustained Throughput (Rate Limited)"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-sustained"
    create_topic "${topic}" 3
    sleep 2
    
    # 10K msg/sec
    run_producer_perf_test "sustained_10k" "${topic}" 100000 1024 10000
    
    # 50K msg/sec
    run_producer_perf_test "sustained_50k" "${topic}" 100000 1024 50000
    
    delete_topic "${topic}"
}

test_concurrent_consumers() {
    log_info "=========================================="
    log_info "Test 7: Concurrent Consumers"
    log_info "=========================================="
    
    local topic="${TOPIC_PREFIX}-concurrent"
    create_topic "${topic}" 6
    sleep 2
    
    # Produce data first
    run_producer_perf_test "concurrent_produce" "${topic}" 100000 1024 "-1"
    
    # Single consumer
    run_consumer_perf_test "concurrent_1" "${topic}" 100000
    
    # Note: For multiple concurrent consumers, run multiple instances manually
    log_info "For concurrent consumer testing, run multiple instances:"
    log_info "  ${KAFKA_BIN}/kafka-consumer-perf-test --bootstrap-server ${BOOTSTRAP_SERVER} --topic ${topic} --messages 50000 --group perf-group"
    
    delete_topic "${topic}"
}

# =============================================================================
# Results Summary
# =============================================================================

generate_summary() {
    log_info "=========================================="
    log_info "Generating Summary Report"
    log_info "=========================================="
    
    local summary_file="${RESULT_DIR}/SUMMARY.md"
    
    cat > "${summary_file}" << EOF
# DataCore Performance Benchmark Results

**Date:** $(date)
**Bootstrap Server:** ${BOOTSTRAP_SERVER}
**Default Configuration:**
- Records: ${NUM_RECORDS}
- Record Size: ${RECORD_SIZE} bytes
- Partitions: ${NUM_PARTITIONS}

## Test Results

### Producer Tests

| Test | Records/sec | MB/sec | Avg Latency (ms) | Max Latency (ms) |
|------|-------------|--------|------------------|------------------|
EOF

    # Parse producer results
    for f in "${RESULT_DIR}"/producer_*.txt; do
        if [ -f "$f" ]; then
            local test_name=$(basename "$f" .txt | sed 's/producer_//')
            local result=$(tail -1 "$f" 2>/dev/null || echo "N/A")
            
            # Parse: 100000 records sent, 12345.67 records/sec (12.34 MB/sec), 1.23 ms avg latency, 45.67 ms max latency
            local records_sec=$(echo "$result" | grep -oE '[0-9]+\.[0-9]+ records/sec' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
            local mb_sec=$(echo "$result" | grep -oE '[0-9]+\.[0-9]+ MB/sec' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
            local avg_lat=$(echo "$result" | grep -oE '[0-9]+\.[0-9]+ ms avg' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
            local max_lat=$(echo "$result" | grep -oE '[0-9]+\.[0-9]+ ms max' | grep -oE '[0-9]+\.[0-9]+' || echo "N/A")
            
            echo "| ${test_name} | ${records_sec} | ${mb_sec} | ${avg_lat} | ${max_lat} |" >> "${summary_file}"
        fi
    done

    cat >> "${summary_file}" << EOF

### Consumer Tests

| Test | Records/sec | MB/sec | Fetch Time (ms) |
|------|-------------|--------|-----------------|
EOF

    # Parse consumer results
    for f in "${RESULT_DIR}"/consumer_*.txt; do
        if [ -f "$f" ]; then
            local test_name=$(basename "$f" .txt | sed 's/consumer_//')
            # Consumer output format varies, extract key metrics
            local data_line=$(grep -E "^[0-9]" "$f" | tail -1 2>/dev/null || echo "")
            if [ -n "$data_line" ]; then
                echo "| ${test_name} | (see detailed log) | | |" >> "${summary_file}"
            fi
        fi
    done

    cat >> "${summary_file}" << EOF

## Raw Results

All detailed results are available in the individual test files in this directory.

## Test Descriptions

1. **Baseline**: Single partition, default settings
2. **Record Sizes**: Various message sizes (100B - 10KB)
3. **Partition Scaling**: 1, 2, 4, 8 partitions
4. **Batching**: Different batch sizes and linger times
5. **ACK Modes**: acks=0 vs acks=1
6. **Sustained Throughput**: Rate-limited tests (10K, 50K msg/sec)
7. **Concurrent Consumers**: Multi-consumer testing

## Notes

- All tests use local broker (single node)
- Results may vary based on system resources
- For production benchmarks, run on dedicated hardware
EOF

    log_success "Summary saved to: ${summary_file}"
    cat "${summary_file}"
}

# =============================================================================
# Quick Tests
# =============================================================================

quick_test() {
    log_info "Running quick performance test..."
    
    local topic="${TOPIC_PREFIX}-quick"
    create_topic "${topic}" 1
    sleep 2
    
    log_info "Producer test (10K records, 1KB each):"
    "${KAFKA_BIN}/kafka-producer-perf-test" \
        --topic "${topic}" \
        --num-records 10000 \
        --record-size 1024 \
        --throughput -1 \
        --producer-props bootstrap.servers="${BOOTSTRAP_SERVER}" acks=1
    
    log_info "Consumer test:"
    "${KAFKA_BIN}/kafka-consumer-perf-test" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --topic "${topic}" \
        --messages 10000
    
    delete_topic "${topic}"
}

# =============================================================================
# Main
# =============================================================================

usage() {
    cat << EOF
DataCore Performance Benchmark Suite

Usage: $0 [command] [options]

Commands:
    all         Run all benchmark tests (default)
    quick       Run a quick sanity test
    producer    Run only producer tests
    consumer    Run only consumer tests (requires data)
    baseline    Run baseline test only
    scaling     Run partition scaling test
    batching    Run batching performance test
    summary     Generate summary from existing results

Options:
    --records N         Number of records (default: 100000)
    --size N            Record size in bytes (default: 1024)
    --throughput N      Target throughput, -1 for unlimited (default: -1)
    --partitions N      Number of partitions (default: 3)
    --server HOST:PORT  Bootstrap server (default: localhost:9092)
    --output DIR        Output directory (default: ./benchmark-results)
    --help              Show this help

Examples:
    $0 quick                    # Quick sanity test
    $0 all                      # Full benchmark suite
    $0 --records 500000 all     # Custom record count
    $0 baseline                 # Only baseline test

Environment Variables:
    KAFKA_BIN           Path to Kafka bin directory
    BOOTSTRAP_SERVER    Broker address
    NUM_RECORDS         Number of records
    RECORD_SIZE         Record size in bytes
EOF
}

main() {
    local command="all"
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            all|quick|producer|consumer|baseline|scaling|batching|summary)
                command=$1
                shift
                ;;
            --records)
                NUM_RECORDS=$2
                shift 2
                ;;
            --size)
                RECORD_SIZE=$2
                shift 2
                ;;
            --throughput)
                THROUGHPUT=$2
                shift 2
                ;;
            --partitions)
                NUM_PARTITIONS=$2
                shift 2
                ;;
            --server)
                BOOTSTRAP_SERVER=$2
                shift 2
                ;;
            --output)
                OUTPUT_DIR=$2
                RESULT_DIR="${OUTPUT_DIR}/${TIMESTAMP}"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║         DataCore Performance Benchmark Suite                 ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    
    check_prerequisites
    
    case $command in
        quick)
            quick_test
            ;;
        all)
            setup_output_dir
            test_baseline
            test_throughput_scaling
            test_partition_scaling
            test_batching
            test_acks_modes
            test_sustained_throughput
            generate_summary
            ;;
        baseline)
            setup_output_dir
            test_baseline
            generate_summary
            ;;
        scaling)
            setup_output_dir
            test_partition_scaling
            generate_summary
            ;;
        batching)
            setup_output_dir
            test_batching
            generate_summary
            ;;
        producer)
            setup_output_dir
            test_baseline
            test_throughput_scaling
            test_batching
            test_acks_modes
            generate_summary
            ;;
        consumer)
            setup_output_dir
            test_concurrent_consumers
            generate_summary
            ;;
        summary)
            # Find latest results
            local latest=$(ls -td "${OUTPUT_DIR}"/*/ 2>/dev/null | head -1)
            if [ -n "$latest" ]; then
                RESULT_DIR="${latest%/}"
                generate_summary
            else
                log_error "No benchmark results found in ${OUTPUT_DIR}"
                exit 1
            fi
            ;;
    esac
    
    echo ""
    log_success "Benchmark completed!"
}

main "$@"
