/// Storage Engine Performance Benchmark
module main

import time
import infra.storage.plugins.memory
import domain

const test_message_count = 10000
const test_message_size = 1024

fn main() {
	println('=== DataCore Storage Benchmark ===')
	println('Message Count: ${test_message_count}')
	println('Message Size: ${test_message_size} bytes')
	println('')

	// Memory Storage 벤치마크
	benchmark_memory_storage()
}

fn benchmark_memory_storage() {
	println('=== Memory Storage Benchmark ===')

	// Memory adapter 생성
	mut adapter := memory.new_memory_adapter()

	// 토픽 생성
	topic := 'benchmark-topic'
	adapter.create_topic(topic, 1, domain.TopicConfig{}) or {
		println('토픽 생성 실패: ${err}')
		return
	}

	// Produce 벤치마크
	println('\n[Produce Test]')
	produce_start := time.now()

	for i in 0 .. test_message_count {
		message_data := 'test-message-${i}' + 'x'.repeat(test_message_size - 20)
		records := [
			domain.Record{
				key:       'key-${i}'.bytes()
				value:     message_data.bytes()
				timestamp: time.now()
			},
		]

		adapter.append(topic, 0, records, i16(0)) or {
			println('메시지 전송 실패: ${err}')
			return
		}

		// 진행률 표시
		if (i + 1) % 1000 == 0 {
			println('  Produced: ${i + 1} / ${test_message_count}')
		}
	}

	produce_duration := time.since(produce_start)
	produce_seconds := f64(produce_duration.milliseconds()) / 1000.0
	produce_throughput := f64(test_message_count) / produce_seconds
	produce_mb_per_sec := (f64(test_message_count) * f64(test_message_size)) / (1024.0 * 1024.0 * produce_seconds)

	println('  Duration: ${produce_seconds:.2f}s')
	println('  Throughput: ${produce_throughput:.2f} msg/sec')
	println('  Throughput: ${produce_mb_per_sec:.2f} MB/sec')

	// Consume 벤치마크
	println('\n[Consume Test]')
	consume_start := time.now()

	mut fetched_count := 0
	mut current_offset := i64(0)

	for fetched_count < test_message_count {
		result := adapter.fetch(topic, 0, current_offset, 1000) or {
			println('메시지 조회 실패: ${err}')
			break
		}

		if result.records.len == 0 {
			break
		}

		fetched_count += result.records.len
		current_offset += i64(result.records.len)

		if fetched_count % 1000 == 0 {
			println('  Consumed: ${fetched_count} / ${test_message_count}')
		}
	}

	consume_duration := time.since(consume_start)
	consume_seconds := f64(consume_duration.milliseconds()) / 1000.0
	consume_throughput := f64(fetched_count) / consume_seconds
	consume_mb_per_sec := (f64(fetched_count) * f64(test_message_size)) / (1024.0 * 1024.0 * consume_seconds)

	println('  Duration: ${consume_seconds:.2f}s')
	println('  Throughput: ${consume_throughput:.2f} msg/sec')
	println('  Throughput: ${consume_mb_per_sec:.2f} MB/sec')

	// 결과 요약
	println('\n=== Summary ===')
	println('Memory Storage Performance:')
	println('  Produce: ${produce_throughput:.0f} msg/sec (${produce_mb_per_sec:.2f} MB/sec)')
	println('  Consume: ${consume_throughput:.0f} msg/sec (${consume_mb_per_sec:.2f} MB/sec)')
	println('')
	println('✅ Benchmark completed successfully!')
}
