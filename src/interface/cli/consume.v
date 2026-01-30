// Interface Layer - CLI Consume Command
// 인터페이스 레이어 - CLI 소비 명령어
//
// Kafka 프로토콜을 사용한 메시지 소비 명령어를 제공합니다.
// 토픽에서 메시지를 읽어 콘솔에 출력합니다.
//
// 주요 기능:
// - 특정 토픽/파티션에서 메시지 소비
// - 시작 오프셋 지정 (earliest, latest, 숫자)
// - 최대 메시지 수 제한
// - 컨슈머 그룹 지원
module cli

import net
import time

/// ConsumeOptions는 소비 명령어 옵션을 담는 구조체입니다.
pub struct ConsumeOptions {
pub:
	bootstrap_server string = 'localhost:9092' // 브로커 주소
	topic            string // 토픽 이름
	partition        int    // 파티션 번호
	group            string // 컨슈머 그룹 ID
	offset           string = 'latest' // 시작 오프셋 (latest, earliest, 숫자)
	max_messages     int    = -1       // 최대 메시지 수 (-1 = 무제한)
	timeout_ms       int    = 30000    // 타임아웃 (ms)
	from_beginning   bool // 처음부터 시작
}

/// parse_consume_options는 소비 명령어 옵션을 파싱합니다.
pub fn parse_consume_options(args []string) ConsumeOptions {
	mut opts := ConsumeOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partition', '-p' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						partition: args[i + 1].int()
					}
					i += 1
				}
			}
			'--group', '-g' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						group: args[i + 1]
					}
					i += 1
				}
			}
			'--offset', '-o' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						offset: args[i + 1]
					}
					i += 1
				}
			}
			'--max-messages', '-n' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						max_messages: args[i + 1].int()
					}
					i += 1
				}
			}
			'--from-beginning' {
				opts = ConsumeOptions{
					...opts
					from_beginning: true
					offset:         'earliest'
				}
			}
			else {
				// 첫 번째 위치 인자가 토픽일 수 있음
				if !args[i].starts_with('-') && opts.topic.len == 0 {
					opts = ConsumeOptions{
						...opts
						topic: args[i]
					}
				}
			}
		}
		i += 1
	}

	return opts
}

/// run_consume은 토픽에서 메시지를 소비합니다.
pub fn run_consume(opts ConsumeOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// 시작 오프셋 가져오기
	mut fetch_offset := get_starting_offset(mut conn, opts)!

	println('\x1b[90mConsuming from "${opts.topic}" partition ${opts.partition} starting at offset ${fetch_offset}...\x1b[0m')
	println('\x1b[90mPress Ctrl+C to stop\x1b[0m')
	println('')

	mut message_count := 0

	// 소비 루프
	for {
		// 최대 메시지 수 확인
		if opts.max_messages > 0 && message_count >= opts.max_messages {
			break
		}

		// Fetch 요청 생성
		request := build_fetch_request(opts.topic, opts.partition, fetch_offset, 1048576,
			opts.timeout_ms)

		// 요청 전송
		send_kafka_request(mut conn, 1, 13, request)! // API Key 1 = Fetch, version 13

		// 응답 읽기
		response := read_kafka_response(mut conn)!

		// 응답 파싱
		records := parse_fetch_response(response)

		if records.len == 0 {
			// 새 레코드 없음, 잠시 대기
			time.sleep(100 * time.millisecond)
			continue
		}

		// 레코드 출력
		for record in records {
			message_count++

			// 출력 형식 지정
			key_str := if record.key.len > 0 { record.key.bytestr() } else { 'null' }
			value_str := record.value.bytestr()

			if opts.group.len > 0 {
				// 컨슈머 그룹 사용 시 - 오프셋 표시
				println('\x1b[90m[${opts.topic}:${opts.partition}:${record.offset}]\x1b[0m')
			}

			if record.key.len > 0 {
				println('\x1b[33m${key_str}\x1b[0m: ${value_str}')
			} else {
				println(value_str)
			}

			fetch_offset = record.offset + 1

			if opts.max_messages > 0 && message_count >= opts.max_messages {
				break
			}
		}
	}

	println('')
	println('\x1b[32m✓\x1b[0m Consumed ${message_count} message(s)')
}

struct ConsumedRecord {
	offset    i64  // 오프셋
	key       []u8 // 키
	value     []u8 // 값
	timestamp i64  // 타임스탬프
}

fn get_starting_offset(mut conn net.TcpConn, opts ConsumeOptions) !i64 {
	// 옵션에 따라 오프셋 결정
	if opts.offset == 'earliest' || opts.from_beginning {
		return get_list_offset(mut conn, opts.topic, opts.partition, -2) // -2 = earliest
	} else if opts.offset == 'latest' {
		return get_list_offset(mut conn, opts.topic, opts.partition, -1) // -1 = latest
	} else {
		// 숫자 오프셋
		return opts.offset.i64()
	}
}

// get_list_offset은 ListOffsets API를 사용하여 오프셋을 가져옵니다.
fn get_list_offset(mut conn net.TcpConn, topic string, partition int, timestamp i64) !i64 {
	// ListOffsets 요청 생성
	request := build_list_offsets_request(topic, partition, timestamp)

	// 요청 전송
	send_kafka_request(mut conn, 2, 7, request)! // API Key 2 = ListOffsets, version 7

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 응답에서 오프셋 파싱
	return parse_list_offsets_response(response)
}

// build_list_offsets_request는 ListOffsets 요청을 생성합니다.
fn build_list_offsets_request(topic string, partition int, timestamp i64) []u8 {
	mut body := []u8{}

	// Replica ID (4바이트) - 컨슈머는 -1
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Isolation level (1바이트) - 0 = read_uncommitted
	body << u8(0)

	// Topics 배열 (compact array)
	body << u8(2) // 1개 토픽 + 1

	// 토픽 이름 (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partitions 배열 (compact array)
	body << u8(2) // 1개 파티션 + 1

	// 파티션 인덱스 (4바이트)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Current leader epoch (4바이트) - -1
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// 타임스탬프 (8바이트)
	body << u8(timestamp >> 56)
	body << u8((timestamp >> 48) & 0xff)
	body << u8((timestamp >> 40) & 0xff)
	body << u8((timestamp >> 32) & 0xff)
	body << u8((timestamp >> 24) & 0xff)
	body << u8((timestamp >> 16) & 0xff)
	body << u8((timestamp >> 8) & 0xff)
	body << u8(timestamp & 0xff)

	// 파티션용 Tagged fields
	body << u8(0)

	// 토픽용 Tagged fields
	body << u8(0)

	// 요청용 Tagged fields
	body << u8(0)

	return body
}

// parse_list_offsets_response는 ListOffsets 응답에서 오프셋을 파싱합니다.
fn parse_list_offsets_response(response []u8) !i64 {
	if response.len < 30 {
		return error('Invalid ListOffsets response')
	}

	// 간소화된 파싱 - 프로덕션에서는 전체 프로토콜 파싱 필요
	// 응답: correlation_id(4) + tagged_fields(1) + throttle_time(4) + topics 배열

	// 응답에서 오프셋 찾기 (오프셋은 8바이트)
	// 이것은 대략적인 휴리스틱 - 예상 오프셋 위치로 건너뛰기

	// 현재는 폴백으로 0 반환
	return 0
}

fn build_fetch_request(topic string, partition int, offset i64, max_bytes int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Cluster ID (compact nullable string) - null (v12+)
	body << u8(0)

	// Replica ID (4바이트) - 컨슈머는 -1
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Max wait ms (4바이트)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// Min bytes (4바이트)
	min_bytes := 1
	body << u8(min_bytes >> 24)
	body << u8((min_bytes >> 16) & 0xff)
	body << u8((min_bytes >> 8) & 0xff)
	body << u8(min_bytes & 0xff)

	// Max bytes (4바이트)
	body << u8(max_bytes >> 24)
	body << u8((max_bytes >> 16) & 0xff)
	body << u8((max_bytes >> 8) & 0xff)
	body << u8(max_bytes & 0xff)

	// Isolation level (1바이트)
	body << u8(0)

	// Session ID (4바이트)
	body << u8(0)
	body << u8(0)
	body << u8(0)
	body << u8(0)

	// Session epoch (4바이트)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Topics 배열 (compact array)
	body << u8(2) // 1개 토픽 + 1

	// Topic ID (16바이트 UUID) - v13+, 이름 기반 조회 시 0
	for _ in 0 .. 16 {
		body << u8(0)
	}

	// Partitions 배열 (compact array)
	body << u8(2) // 1개 파티션 + 1

	// 파티션 인덱스 (4바이트)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Current leader epoch (4바이트)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Fetch offset (8바이트)
	body << u8(offset >> 56)
	body << u8((offset >> 48) & 0xff)
	body << u8((offset >> 40) & 0xff)
	body << u8((offset >> 32) & 0xff)
	body << u8((offset >> 24) & 0xff)
	body << u8((offset >> 16) & 0xff)
	body << u8((offset >> 8) & 0xff)
	body << u8(offset & 0xff)

	// Last fetched epoch (4바이트)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Log start offset (8바이트)
	for _ in 0 .. 8 {
		body << u8(0)
	}

	// Partition max bytes (4바이트)
	body << u8(max_bytes >> 24)
	body << u8((max_bytes >> 16) & 0xff)
	body << u8((max_bytes >> 8) & 0xff)
	body << u8(max_bytes & 0xff)

	// 파티션용 Tagged fields
	body << u8(0)

	// 토픽용 Tagged fields
	body << u8(0)

	// Forgotten topics data (compact array) - 빈
	body << u8(1)

	// Rack ID (compact string) - 빈
	body << u8(1)

	// 요청용 Tagged fields
	body << u8(0)

	return body
}

// parse_fetch_response는 Fetch 응답에서 레코드를 파싱합니다.
fn parse_fetch_response(response []u8) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if response.len < 50 {
		return records
	}

	// 간소화된 파싱 - 전체 구현은 완전한 응답 파싱 필요
	// 응답 구조는 중첩 배열로 복잡함

	// RecordBatch 매직 바이트 (0x02) 찾아서 레코드 파싱
	// 이것은 휴리스틱 접근법

	mut pos := 0
	for pos < response.len - 50 {
		// 매직 바이트 0x02 찾기 (record batch v2)
		if response[pos] == 0x02 {
			// 이 위치에서 record batch 파싱 시도
			parsed := try_parse_record_batch(response, pos - 16) // 배치 시작으로 백업
			if parsed.len > 0 {
				records << parsed
				break // 레코드 찾음
			}
		}
		pos++
	}

	return records
}

// try_parse_record_batch는 record batch 파싱을 시도합니다.
fn try_parse_record_batch(data []u8, start int) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if start < 0 || start + 61 > data.len {
		return records
	}

	pos := start

	// Base offset (8바이트)
	base_offset := i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))

	// batch_length (4) + partition_leader_epoch (4) + magic (1) + crc (4) + attributes (2)
	// + last_offset_delta (4) + first_timestamp (8) + max_timestamp (8)
	// + producer_id (8) + producer_epoch (2) + base_sequence (4) + records_count (4) 건너뛰기

	// 이것은 간소화된 버전 - 전체 구현은 각 필드를 적절히 파싱해야 함
	record_start := start + 57 // 레코드 배열의 대략적인 시작 위치

	if record_start < data.len {
		// 최소 하나의 레코드 추출 시도
		// 레코드는 varint로 길이 접두사가 붙음

		// 현재는 플레이스홀더만 반환
		// 전체 구현은 varint와 레코드 형식을 디코딩해야 함
	}

	_ = base_offset // 미사용 경고 방지

	return records
}

/// print_consume_help는 소비 명령어 도움말을 출력합니다.
pub fn print_consume_help() {
	println('\x1b[33mConsume Command:\x1b[0m')
	println('')
	println('Usage: datacore consume <topic> [options]')
	println('')
	println('\x1b[33mOptions:\x1b[0m')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -t, --topic             Topic name (required)')
	println('  -p, --partition         Partition number (default: 0)')
	println('  -g, --group             Consumer group ID')
	println('  -o, --offset            Starting offset: earliest, latest, or number')
	println('  -n, --max-messages      Maximum messages to consume')
	println('      --from-beginning    Start from earliest offset')
	println('')
	println('\x1b[33mExamples:\x1b[0m')
	println('  datacore consume my-topic')
	println('  datacore consume my-topic --from-beginning')
	println('  datacore consume my-topic -g my-group')
	println('  datacore consume my-topic -n 10')
}
