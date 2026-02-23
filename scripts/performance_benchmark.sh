#!/bin/bash

# DataCore Performance Benchmark Script
# Memory vs S3 Storage Engine Performance Comparison
#
# S3 benchmark requires AWS credentials:
#   AWS_ACCESS_KEY_ID     - AWS access key
#   AWS_SECRET_ACCESS_KEY - AWS secret key
#   AWS_DEFAULT_REGION    - AWS region (default: us-west-2)
#   S3_BUCKET_NAME        - S3 bucket name (default: datacore-benchmark)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# 결과 디렉토리 생성
mkdir -p "$RESULTS_DIR"

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== DataCore Performance Benchmark ===${NC}"
echo "Timestamp: $TIMESTAMP"
echo ""

# 테스트 설정
TEST_MESSAGE_COUNT=10000
TEST_MESSAGE_SIZE=1024
TEST_TOPIC="benchmark-topic"
TEST_PARTITION=0

# 유틸리티 함수
log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
	echo -e "${YELLOW}[WARN]${NC} $1"
}

# 서버 PID 저장 변수
SERVER_PID=""

# cleanup 함수
cleanup() {
	if [ ! -z "$SERVER_PID" ]; then
		log_info "Stopping DataCore server (PID: $SERVER_PID)..."
		kill $SERVER_PID 2>/dev/null || true
		wait $SERVER_PID 2>/dev/null || true
	fi
}

trap cleanup EXIT

# 서버 시작 함수
start_server() {
	local config_file=$1
	local storage_type=$2

	log_info "Starting DataCore with $storage_type storage..."

	# 기존 서버 종료
	if [ ! -z "$SERVER_PID" ]; then
		kill $SERVER_PID 2>/dev/null || true
		wait $SERVER_PID 2>/dev/null || true
	fi

	# 서버 시작
	"$PROJECT_DIR/bin/datacore" broker start --config="$config_file" >"$RESULTS_DIR/${storage_type}_server.log" 2>&1 &
	SERVER_PID=$!

	# 서버 준비 대기
	log_info "Waiting for server to be ready..."
	for i in {1..30}; do
		if nc -z localhost 9092 2>/dev/null; then
			log_success "Server is ready (PID: $SERVER_PID)"
			sleep 2 # 추가 안정화 대기
			return 0
		fi
		sleep 1
	done

	log_error "Server failed to start"
	return 1
}

# Produce 성능 테스트
test_produce() {
	local storage_type=$1
	local result_file="$RESULTS_DIR/${storage_type}_produce.json"

	log_info "Running produce test ($storage_type)..."

	# 메시지 생성
	local message=$(head -c $TEST_MESSAGE_SIZE /dev/urandom | base64)

	# 시작 시간
	local start_time=$(date +%s.%N)

	# 메시지 전송
	for i in $(seq 1 $TEST_MESSAGE_COUNT); do
		echo "$message" | "$PROJECT_DIR/bin/datacore" cli produce \
			--bootstrap-server localhost:9092 \
			--topic "$TEST_TOPIC" \
			--partition $TEST_PARTITION \
			>/dev/null 2>&1 || true

		# 진행률 표시 (1000개마다)
		if [ $((i % 1000)) -eq 0 ]; then
			echo -ne "\rProduced: $i / $TEST_MESSAGE_COUNT messages"
		fi
	done
	echo ""

	# 종료 시간
	local end_time=$(date +%s.%N)
	local duration=$(echo "$end_time - $start_time" | bc)
	local throughput=$(echo "scale=2; $TEST_MESSAGE_COUNT / $duration" | bc)
	local mb_per_sec=$(echo "scale=2; ($TEST_MESSAGE_COUNT * $TEST_MESSAGE_SIZE) / (1024 * 1024 * $duration)" | bc)

	# 결과 저장
	cat >"$result_file" <<RESULT
{
    "storage_type": "$storage_type",
    "operation": "produce",
    "message_count": $TEST_MESSAGE_COUNT,
    "message_size": $TEST_MESSAGE_SIZE,
    "duration_sec": $duration,
    "throughput_msg_per_sec": $throughput,
    "throughput_mb_per_sec": $mb_per_sec
}
RESULT

	log_success "Produce test completed: $throughput msg/sec, $mb_per_sec MB/sec"
	echo "  Duration: ${duration}s"
}

# Consume 성능 테스트
test_consume() {
	local storage_type=$1
	local result_file="$RESULTS_DIR/${storage_type}_consume.json"

	log_info "Running consume test ($storage_type)..."

	# 시작 시간
	local start_time=$(date +%s.%N)

	# 메시지 소비
	"$PROJECT_DIR/bin/datacore" cli consume \
		--bootstrap-server localhost:9092 \
		--topic "$TEST_TOPIC" \
		--partition $TEST_PARTITION \
		--offset 0 \
		--max-messages $TEST_MESSAGE_COUNT \
		>/dev/null 2>&1 || true

	# 종료 시간
	local end_time=$(date +%s.%N)
	local duration=$(echo "$end_time - $start_time" | bc)
	local throughput=$(echo "scale=2; $TEST_MESSAGE_COUNT / $duration" | bc)
	local mb_per_sec=$(echo "scale=2; ($TEST_MESSAGE_COUNT * $TEST_MESSAGE_SIZE) / (1024 * 1024 * $duration)" | bc)

	# 결과 저장
	cat >"$result_file" <<RESULT
{
    "storage_type": "$storage_type",
    "operation": "consume",
    "message_count": $TEST_MESSAGE_COUNT,
    "message_size": $TEST_MESSAGE_SIZE,
    "duration_sec": $duration,
    "throughput_msg_per_sec": $throughput,
    "throughput_mb_per_sec": $mb_per_sec
}
RESULT

	log_success "Consume test completed: $throughput msg/sec, $mb_per_sec MB/sec"
	echo "  Duration: ${duration}s"
}

# Memory 스토리지 벤치마크
benchmark_memory() {
	log_info "=== Memory Storage Benchmark ==="

	# Memory config 생성
	cat >"$RESULTS_DIR/config_memory.toml" <<CONFIG
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1
cluster_id = "benchmark-cluster"

[storage]
engine = "memory"

[logging]
level = "warn"
output = "stdout"
CONFIG

	start_server "$RESULTS_DIR/config_memory.toml" "memory" || return 1

	test_produce "memory"
	test_consume "memory"

	# 서버 종료
	kill $SERVER_PID 2>/dev/null || true
	wait $SERVER_PID 2>/dev/null || true
	SERVER_PID=""

	sleep 2
}

# S3 Storage Benchmark (AWS S3)
benchmark_s3() {
	log_info "=== S3 Storage Benchmark (AWS S3) ==="

	# Validate AWS credentials
	if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
		log_warn "SKIP: AWS credentials not configured"
		log_warn "Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET_NAME"
		return 0
	fi

	local s3_bucket="${S3_BUCKET_NAME:-datacore-benchmark}"
	local s3_region="${AWS_DEFAULT_REGION:-us-west-2}"
	local bench_prefix="benchmark/$(date +%Y%m%d-%H%M%S)"

	# Verify S3 bucket access
	if ! aws s3 ls "s3://${s3_bucket}" &>/dev/null; then
		log_warn "SKIP: Cannot access S3 bucket '${s3_bucket}'"
		return 0
	fi

	log_info "Using S3 bucket: ${s3_bucket} (region: ${s3_region})"
	log_info "Benchmark prefix: datacore/${bench_prefix}/"

	# S3 config
	local S3_CONFIG="$RESULTS_DIR/config_s3.toml"
	cat >"$S3_CONFIG" <<CONFIG
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1
cluster_id = "benchmark-cluster"

[storage]
engine = "s3"

[s3]
endpoint = ""
bucket = "${s3_bucket}"
region = "${s3_region}"
access_key = "${AWS_ACCESS_KEY_ID}"
secret_key = "${AWS_SECRET_ACCESS_KEY}"
prefix = "datacore/${bench_prefix}/"
use_path_style = false
batch_timeout_ms = 100
batch_max_bytes = 4096000
compaction_interval_ms = 60000
target_segment_bytes = 104857600
index_cache_ttl_ms = 3600000

[logging]
level = "warn"
output = "stdout"
CONFIG

	start_server "$S3_CONFIG" "s3" || return 1

	test_produce "s3"
	test_consume "s3"

	# Stop server
	kill $SERVER_PID 2>/dev/null || true
	wait $SERVER_PID 2>/dev/null || true
	SERVER_PID=""

	# Clean up benchmark data from S3
	log_info "Cleaning up S3 benchmark data..."
	aws s3 rm "s3://${s3_bucket}/datacore/${bench_prefix}" --recursive &>/dev/null || true

	sleep 2
}

# 결과 리포트 생성
generate_report() {
	log_info "Generating performance report..."

	local report_file="$RESULTS_DIR/performance_report_${TIMESTAMP}.md"

	cat >"$report_file" <<'REPORT'
# DataCore Performance Benchmark Report

## Test Configuration

- **Message Count**: 10,000
- **Message Size**: 1 KB
- **Topic**: benchmark-topic
- **Partition**: 0

## Results

### Memory Storage Engine

**Produce Performance**:
REPORT

	if [ -f "$RESULTS_DIR/memory_produce.json" ]; then
		local mem_prod_tput=$(jq -r '.throughput_msg_per_sec' "$RESULTS_DIR/memory_produce.json")
		local mem_prod_mb=$(jq -r '.throughput_mb_per_sec' "$RESULTS_DIR/memory_produce.json")
		local mem_prod_dur=$(jq -r '.duration_sec' "$RESULTS_DIR/memory_produce.json")

		cat >>"$report_file" <<REPORT
- Throughput: ${mem_prod_tput} msg/sec
- Throughput: ${mem_prod_mb} MB/sec
- Duration: ${mem_prod_dur} seconds

**Consume Performance**:
REPORT
	fi

	if [ -f "$RESULTS_DIR/memory_consume.json" ]; then
		local mem_cons_tput=$(jq -r '.throughput_msg_per_sec' "$RESULTS_DIR/memory_consume.json")
		local mem_cons_mb=$(jq -r '.throughput_mb_per_sec' "$RESULTS_DIR/memory_consume.json")
		local mem_cons_dur=$(jq -r '.duration_sec' "$RESULTS_DIR/memory_consume.json")

		cat >>"$report_file" <<REPORT
- Throughput: ${mem_cons_tput} msg/sec
- Throughput: ${mem_cons_mb} MB/sec
- Duration: ${mem_cons_dur} seconds

### S3 Storage Engine (AWS)

**Produce Performance**:
REPORT
	fi

	if [ -f "$RESULTS_DIR/s3_produce.json" ]; then
		local s3_prod_tput=$(jq -r '.throughput_msg_per_sec' "$RESULTS_DIR/s3_produce.json")
		local s3_prod_mb=$(jq -r '.throughput_mb_per_sec' "$RESULTS_DIR/s3_produce.json")
		local s3_prod_dur=$(jq -r '.duration_sec' "$RESULTS_DIR/s3_produce.json")

		cat >>"$report_file" <<REPORT
- Throughput: ${s3_prod_tput} msg/sec
- Throughput: ${s3_prod_mb} MB/sec
- Duration: ${s3_prod_dur} seconds

**Consume Performance**:
REPORT
	fi

	if [ -f "$RESULTS_DIR/s3_consume.json" ]; then
		local s3_cons_tput=$(jq -r '.throughput_msg_per_sec' "$RESULTS_DIR/s3_consume.json")
		local s3_cons_mb=$(jq -r '.throughput_mb_per_sec' "$RESULTS_DIR/s3_consume.json")
		local s3_cons_dur=$(jq -r '.duration_sec' "$RESULTS_DIR/s3_consume.json")

		cat >>"$report_file" <<REPORT
- Throughput: ${s3_cons_tput} msg/sec
- Throughput: ${s3_cons_mb} MB/sec
- Duration: ${s3_cons_dur} seconds

## Performance Comparison

| Metric | Memory | S3 (AWS) | Ratio |
|--------|--------|------------|-------|
| Produce (msg/sec) | ${mem_prod_tput} | ${s3_prod_tput} | $(echo "scale=2; $mem_prod_tput / $s3_prod_tput" | bc)x |
| Produce (MB/sec) | ${mem_prod_mb} | ${s3_prod_mb} | $(echo "scale=2; $mem_prod_mb / $s3_prod_mb" | bc)x |
| Consume (msg/sec) | ${mem_cons_tput} | ${s3_cons_tput} | $(echo "scale=2; $mem_cons_tput / $s3_cons_tput" | bc)x |
| Consume (MB/sec) | ${mem_cons_mb} | ${s3_cons_mb} | $(echo "scale=2; $mem_cons_mb / $s3_cons_mb" | bc)x |

## Conclusion

Memory storage is **$(echo "scale=1; $mem_prod_tput / $s3_prod_tput" | bc)x faster** for produce operations and **$(echo "scale=1; $mem_cons_tput / $s3_cons_tput" | bc)x faster** for consume operations.

**Recommendations**:
- Use Memory storage for low-latency, high-throughput scenarios
- Use S3 storage for durability and long-term retention
- Consider hybrid approach: Memory for hot data, S3 for cold data
REPORT
	fi

	log_success "Report generated: $report_file"
	cat "$report_file"
}

# 메인 실행
main() {
	log_info "Building DataCore..."
	cd "$PROJECT_DIR"
	make build >/dev/null 2>&1 || {
		log_error "Build failed"
		exit 1
	}

	log_success "Build completed"
	echo ""

	# Memory 벤치마크
	benchmark_memory || log_error "Memory benchmark failed"
	echo ""

	# S3 벤치마크
	benchmark_s3 || log_error "S3 benchmark failed"
	echo ""

	# 리포트 생성
	generate_report

	log_success "Benchmark completed!"
	log_info "Results saved in: $RESULTS_DIR"
}

main "$@"
