#!/usr/bin/env bash
#
# Manual Compression Test for DataCore
# 이 스크립트는 DataCore의 압축 기능을 Kafka CLI 없이 테스트합니다.
# 테스트 항목: Snappy, Gzip, LZ4, Zstd
#
# 사용법:
#   ./tests/manual_compression_test.sh

set -e

cd "$(dirname "$0")/.."

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║         DataCore Manual Compression Test                                  ║${NC}"
echo -e "${CYAN}║         (No Kafka CLI Required)                                           ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# V 소스 파일에서 테스트 코드 추출하여 임시 파일 생성
cat >/tmp/compression_standalone.v <<'VEND'
module main

import time
import infra.compression

struct TestResult {
	name            string
	passed          bool
	compress_time   time.Duration
	decompress_time time.Duration
	original_size   int
	compressed_size int
	ratio           f64
	error_msg       string
}

fn generate_test_data() []u8 {
	message := '{"id":"12345","timestamp":"2026-02-01T12:00:00Z","data":"This is a test message for compression testing. It contains various patterns and repeated content to test compression efficiency. Repeated: ABCABCABCXYZXYZXYZ123123123","metadata":{"source":"manual_test","version":"1.0","tags":["test","compression","kafka"]}}'
	mut data := []u8{}
	for _ in 0 .. 100 {
		data << message.bytes()
		data << u8(`\n`)
	}
	return data
}

fn run_single_test(ct compression.CompressionType, test_data []u8, mut service compression.CompressionService) TestResult {
	name := ct.str()
	compress_start := time.now()
	compressed := service.compress(test_data, ct) or {
		return TestResult{ name: name, passed: false, error_msg: 'Compression failed: ${err}' }
	}
	compress_time := time.since(compress_start)
	if compressed.len == 0 && test_data.len > 0 {
		return TestResult{ name: name, passed: false, error_msg: 'Compressed data is empty' }
	}
	decompress_start := time.now()
	decompressed := service.decompress(compressed, ct) or {
		return TestResult{ name: name, passed: false, error_msg: 'Decompression failed: ${err}' }
	}
	decompress_time := time.since(decompress_start)
	if decompressed != test_data {
		return TestResult{ name: name, passed: false, error_msg: 'Data mismatch' }
	}
	mut ratio := 0.0
	if compressed.len > 0 {
		ratio = f64(test_data.len) / f64(compressed.len)
	}
	return TestResult{
		name: name, passed: true, compress_time: compress_time, decompress_time: decompress_time,
		original_size: test_data.len, compressed_size: compressed.len, ratio: ratio
	}
}

fn print_result(result TestResult) {
	if result.passed {
		println('[\x1b[32mPASS\x1b[0m] ${result.name:10} | ${result.original_size:8}B → ${result.compressed_size:8}B | ratio: ${result.ratio:5.2f}x | compress: ${result.compress_time:8} | decompress: ${result.decompress_time:8}')
	} else {
		println('[\x1b[31mFAIL\x1b[0m] ${result.name:10} | Error: ${result.error_msg}')
	}
}

fn main() {
	println('\n📦 Generating test data...')
	test_data := generate_test_data()
	println('   Generated ${test_data.len} bytes of test data\n')
	println('🔧 Initializing compression service...')
	mut service := compression.new_default_compression_service() or {
		eprintln('   Failed: ${err}')
		exit(1)
	}
	println('   ✓ Compression service initialized\n')
	compression_types := [ compression.CompressionType.snappy, compression.CompressionType.gzip, compression.CompressionType.lz4, compression.CompressionType.zstd ]
	mut results := []TestResult{}
	println('🧪 Running compression tests...')
	println('═══════════════════════════════════════════════════════════════════════════')
	println('Type       | Original | Compressed | Ratio  | Compress   | Decompress')
	println('-----------|----------|------------|--------|------------|------------')
	for ct in compression_types {
		result := run_single_test(ct, test_data, mut service)
		results << result
		print_result(result)
	}
	println('\n📊 Compression Metrics Summary')
	println('═══════════════════════════════════════════════════════════════════════════')
	mut total_original := 0
	mut total_compressed := 0
	mut passed_count := 0
	for result in results {
		if result.passed {
			passed_count++
			total_original += result.original_size
			total_compressed += result.compressed_size
		}
	}
	if total_compressed > 0 {
		overall_ratio := f64(total_original) / f64(total_compressed)
		println('Overall compression ratio: ${overall_ratio:.2f}x')
		println('Total bytes saved: ${total_original - total_compressed} bytes')
	}
	println('Tests passed: ${passed_count}/${results.len}')
	mut all_passed := true
	for result in results { if !result.passed { all_passed = false break } }
	println('═══════════════════════════════════════════════════════════════════════════')
	if all_passed { println('\x1b[32m✓ All compression tests passed!\x1b[0m') exit(0) }
	else { println('\x1b[31m✗ Some compression tests failed!\x1b[0m') exit(1) }
}
VEND

# 소스 디렉토리로 이동하여 컴파일
cd src

echo -e "${YELLOW}📦 Building compression test...${NC}"
if v -enable-globals -o ../bin/manual_compression_test /tmp/compression_standalone.v 2>&1 | grep -E "^(✓|✗|error:|builder error:)"; then
	echo -e "${RED}✗ Build failed${NC}"
	exit 1
else
	echo -e "${GREEN}✓ Build successful${NC}"
fi

cd ..

echo ""
echo -e "${YELLOW}🚀 Running compression tests...${NC}"
echo ""

# 테스트 실행 - JSON 로그 라인 필터링
./bin/manual_compression_test 2>&1 | grep -v '^[[:space:]]*{'
