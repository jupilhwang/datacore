#!/bin/bash
# Multi-Broker S3 Test Orchestrator
# Manages full lifecycle: start cluster -> run tests -> collect results -> cleanup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
COMPOSE_CMD="docker-compose"

# 명령줄 옵션
SKIP_BUILD=false
TEST_TYPE="all" # all, compat, perf
SKIP_CLEANUP=false
VERBOSE=false

usage() {
	echo "Usage: $0 [options]"
	echo ""
	echo "Options:"
	echo "  --skip-build      Skip Docker image build"
	echo "  --test=TYPE       Test type: all (default), compat, perf"
	echo "  --skip-cleanup    Don't stop cluster after tests"
	echo "  --verbose         Enable verbose output"
	echo "  -h, --help        Show this help"
}

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--skip-build) SKIP_BUILD=true ;;
		--test=*) TEST_TYPE="${1#--test=}" ;;
		--skip-cleanup) SKIP_CLEANUP=true ;;
		--verbose) VERBOSE=true ;;
		-h | --help)
			usage
			exit 0
			;;
		*)
			echo "[ERROR] Unknown option: $1"
			usage
			exit 1
			;;
		esac
		shift
	done
}

log_info() { echo "[INFO] $(date '+%H:%M:%S') $*"; }
log_error() { echo "[ERROR] $(date '+%H:%M:%S') $*"; }

# Docker Compose 명령 감지
detect_compose_cmd() {
	if command -v docker-compose &>/dev/null; then
		COMPOSE_CMD="docker-compose"
	elif docker compose version &>/dev/null 2>&1; then
		COMPOSE_CMD="docker compose"
	else
		log_error "docker-compose not found"
		exit 1
	fi
}

# 사전 조건 확인
check_prerequisites() {
	log_info "Checking prerequisites..."

	# Docker
	if ! command -v docker &>/dev/null; then
		log_error "Docker is not installed"
		exit 1
	fi

	# Docker Compose
	detect_compose_cmd
	log_info "Using compose command: $COMPOSE_CMD"

	# datacore:local 이미지
	if ! docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "datacore:local"; then
		if [[ "$SKIP_BUILD" == "true" ]]; then
			log_error "datacore:local image not found and --skip-build specified"
			exit 1
		fi
		log_info "Building datacore:local image..."
		cd "${SCRIPT_DIR}/../.."
		docker build -t datacore:local .
		cd "$SCRIPT_DIR"
	fi

	# AWS 자격증명
	if [[ ! -f "$HOME/.aws/credentials" ]]; then
		log_error "AWS credentials not found at ~/.aws/credentials"
		exit 1
	fi

	# AWS 접근 확인
	if command -v aws &>/dev/null; then
		if ! aws sts get-caller-identity &>/dev/null 2>&1; then
			log_error "AWS credentials may be expired. Run: aws sts get-caller-identity"
			exit 1
		fi
		log_info "AWS credentials valid"
	fi

	# Kafka CLI 도구
	local tools=("kafka-topics" "kafka-console-producer" "kafka-console-consumer")
	for tool in "${tools[@]}"; do
		if ! command -v "$tool" &>/dev/null; then
			log_error "$tool not found in PATH"
			exit 1
		fi
	done
	log_info "All Kafka CLI tools available"

	# kcat (선택)
	if command -v kcat &>/dev/null; then
		log_info "kcat available"
	else
		log_info "kcat not found (some tests will be skipped)"
	fi

	log_info "All prerequisites met"
}

# 클러스터 시작
start_cluster() {
	log_info "Starting 3-broker cluster with S3 storage..."

	cd "$SCRIPT_DIR"
	$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

	log_info "Waiting for all brokers to be ready..."

	local MAX_WAIT=90
	local WAITED=0
	local ALL_READY=false

	while [[ $WAITED -lt $MAX_WAIT ]]; do
		local READY_COUNT=0

		for port in 9092 9093 9094; do
			if nc -z 127.0.0.1 $port 2>/dev/null; then
				READY_COUNT=$((READY_COUNT + 1))
			fi
		done

		if [[ $READY_COUNT -eq 3 ]]; then
			ALL_READY=true
			break
		fi

		echo -n "."
		sleep 2
		WAITED=$((WAITED + 2))
	done
	echo ""

	if [[ "$ALL_READY" == "true" ]]; then
		log_info "All 3 brokers are ready (${WAITED}s)"
	else
		log_error "Timeout waiting for brokers after ${MAX_WAIT}s"
		log_error "Broker status:"
		for port in 9092 9093 9094; do
			if nc -z 127.0.0.1 $port 2>/dev/null; then
				echo "  Port $port: UP"
			else
				echo "  Port $port: DOWN"
			fi
		done
		# 로그 출력
		$COMPOSE_CMD -f "$COMPOSE_FILE" logs --tail=50
		stop_cluster
		exit 1
	fi

	# 추가 안정화 대기 (S3 연결 초기화)
	log_info "Waiting 5s for S3 connection stabilization..."
	sleep 5
}

# 클러스터 정지
stop_cluster() {
	log_info "Stopping cluster..."
	cd "$SCRIPT_DIR"
	$COMPOSE_CMD -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
}

# 호환성 테스트 실행
run_compat_tests() {
	log_info "=========================================="
	log_info "  Running Compatibility Tests"
	log_info "=========================================="

	if [[ -x "${SCRIPT_DIR}/test_s3_multibroker_compat.sh" ]]; then
		timeout 1200 bash "${SCRIPT_DIR}/test_s3_multibroker_compat.sh"
		return $?
	else
		log_error "test_s3_multibroker_compat.sh not found or not executable"
		return 1
	fi
}

# 성능 테스트 실행
run_perf_tests() {
	log_info "=========================================="
	log_info "  Running Performance Benchmarks"
	log_info "=========================================="

	if [[ -x "${SCRIPT_DIR}/test_s3_multibroker_perf.sh" ]]; then
		timeout 600 bash "${SCRIPT_DIR}/test_s3_multibroker_perf.sh"
		return $?
	else
		log_error "test_s3_multibroker_perf.sh not found or not executable"
		return 1
	fi
}

# 로그 수집
collect_logs() {
	local LOG_DIR="${SCRIPT_DIR}/logs/$(date '+%Y%m%d_%H%M%S')"
	mkdir -p "$LOG_DIR"

	cd "$SCRIPT_DIR"
	for broker in broker1 broker2 broker3; do
		$COMPOSE_CMD -f "$COMPOSE_FILE" logs "$broker" >"${LOG_DIR}/${broker}.log" 2>&1 || true
	done

	log_info "Logs collected in: $LOG_DIR"
}

# 정리 함수 (EXIT trap)
on_exit() {
	local EXIT_CODE=$?

	# 로그 수집
	collect_logs

	# 클러스터 정지
	if [[ "$SKIP_CLEANUP" != "true" ]]; then
		stop_cluster
	else
		log_info "Skipping cleanup (--skip-cleanup). Cluster is still running."
		log_info "To stop: cd $SCRIPT_DIR && $COMPOSE_CMD -f $COMPOSE_FILE down"
	fi

	exit $EXIT_CODE
}

# 메인
main() {
	parse_args "$@"

	echo "============================================"
	echo "  DataCore Multi-Broker S3 Test Suite"
	echo "  3 Brokers | AWS S3 Storage"
	echo "  Date: $(date '+%Y-%m-%d %H:%M:%S')"
	echo "============================================"
	echo ""

	check_prerequisites

	trap on_exit EXIT

	start_cluster

	local COMPAT_RESULT=0
	local PERF_RESULT=0

	case "$TEST_TYPE" in
	all)
		run_compat_tests || COMPAT_RESULT=$?
		echo ""
		run_perf_tests || PERF_RESULT=$?
		;;
	compat)
		run_compat_tests || COMPAT_RESULT=$?
		;;
	perf)
		run_perf_tests || PERF_RESULT=$?
		;;
	*)
		log_error "Unknown test type: $TEST_TYPE"
		exit 1
		;;
	esac

	echo ""
	echo "============================================"
	echo "  Final Summary"
	echo "============================================"
	echo "  Compatibility Tests: $([ $COMPAT_RESULT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
	echo "  Performance Tests:   $([ $PERF_RESULT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
	echo "============================================"

	if [[ $COMPAT_RESULT -ne 0 ]] || [[ $PERF_RESULT -ne 0 ]]; then
		exit 1
	fi
}

main "$@"
