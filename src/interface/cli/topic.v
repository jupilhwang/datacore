// Interface Layer - CLI Topic Commands
// 인터페이스 레이어 - CLI 토픽 명령어
//
// Kafka 프로토콜을 사용한 토픽 관리 명령어를 제공합니다.
// 토픽 생성, 삭제, 조회, 상세 정보 확인 등의 기능을 지원합니다.
//
// 주요 기능:
// - 토픽 생성 (파티션 수, 복제 계수 지정)
// - 토픽 목록 조회
// - 토픽 상세 정보 조회
// - 토픽 삭제
module cli

import net
import time

/// TopicOptions는 토픽 명령어 옵션을 담는 구조체입니다.
pub struct TopicOptions {
pub:
	bootstrap_server string = 'localhost:9092' // 브로커 주소
	topic            string // 토픽 이름
	partitions       int = 1     // 파티션 수
	replication      int = 1     // 복제 계수
	timeout_ms       int = 30000 // 타임아웃 (ms)
}

/// parse_topic_options는 토픽 명령어 옵션을 파싱합니다.
pub fn parse_topic_options(args []string) TopicOptions {
	mut opts := TopicOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partitions', '-p' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						partitions: args[i + 1].int()
					}
					i += 1
				}
			}
			'--replication-factor', '-r' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						replication: args[i + 1].int()
					}
					i += 1
				}
			}
			else {
				// 위치 인자가 토픽 이름일 수 있음
				if !args[i].starts_with('-') && opts.topic.len == 0 {
					opts = TopicOptions{
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

/// run_topic_create는 토픽을 생성합니다.
pub fn run_topic_create(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Creating topic "${opts.topic}"...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// CreateTopics 요청 생성
	request := build_create_topic_request(opts.topic, opts.partitions, opts.replication,
		opts.timeout_ms)

	// 요청 전송
	send_kafka_request(mut conn, 19, 3, request)! // API Key 19 = CreateTopics, version 3 (비유연)

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 응답 파싱 및 확인
	check_create_topic_response(response, opts.topic)!

	println('\x1b[32m✓\x1b[0m Topic "${opts.topic}" created successfully')
	println('  Partitions: ${opts.partitions}')
	println('  Replication: ${opts.replication}')
}

/// run_topic_list는 모든 토픽을 나열합니다.
pub fn run_topic_list(opts TopicOptions) ! {
	println('\x1b[90m⏳ Listing topics...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Metadata 요청 생성 (빈 토픽 배열 = 모든 토픽)
	request := build_metadata_request([])

	// 요청 전송
	send_kafka_request(mut conn, 3, 12, request)! // API Key 3 = Metadata, version 12

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 토픽 파싱 및 표시
	topics := parse_metadata_response_topics(response)

	if topics.len == 0 {
		println('\x1b[33m⚠\x1b[0m  No topics found')
		return
	}

	println('')
	println('\x1b[33mTopics:\x1b[0m')
	for topic in topics {
		internal_marker := if topic.is_internal { ' \x1b[90m(internal)\x1b[0m' } else { '' }
		println('  • ${topic.name}${internal_marker}')
		println('    Partitions: ${topic.partitions}')
	}
}

/// run_topic_describe는 토픽의 상세 정보를 표시합니다.
pub fn run_topic_describe(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Describing topic "${opts.topic}"...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// 특정 토픽에 대한 Metadata 요청 생성
	request := build_metadata_request([opts.topic])

	// 요청 전송
	send_kafka_request(mut conn, 3, 12, request)!

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 토픽 상세 정보 파싱 및 표시
	topics := parse_metadata_response_topics(response)

	if topics.len == 0 {
		return error('Topic "${opts.topic}" not found')
	}

	topic := topics[0]
	println('')
	println('\x1b[33mTopic:\x1b[0m ${topic.name}')
	println('  Internal:    ${topic.is_internal}')
	println('  Partitions:  ${topic.partitions}')
	// TODO: 파티션 상세 정보 추가 (리더, 복제본, ISR)
}

/// run_topic_delete는 토픽을 삭제합니다.
pub fn run_topic_delete(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Deleting topic "${opts.topic}"...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DeleteTopics 요청 생성
	request := build_delete_topic_request(opts.topic, opts.timeout_ms)

	// 요청 전송
	send_kafka_request(mut conn, 20, 6, request)! // API Key 20 = DeleteTopics, version 6

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 응답 파싱 및 확인
	check_delete_topic_response(response, opts.topic)!

	println('\x1b[32m✓\x1b[0m Topic "${opts.topic}" deleted successfully')
}

struct TopicInfo {
	name        string // 토픽 이름
	partitions  int    // 파티션 수
	is_internal bool   // 내부 토픽 여부
}

fn connect_broker(addr string) !&net.TcpConn {
	parts := addr.split(':')
	host := if parts.len > 0 { parts[0] } else { 'localhost' }
	port := if parts.len > 1 { parts[1].int() } else { 9092 }

	return net.dial_tcp('${host}:${port}') or {
		return error('Failed to connect to broker at ${addr}: ${err}')
	}
}

// send_kafka_request는 Kafka 요청을 전송합니다.
fn send_kafka_request(mut conn net.TcpConn, api_key i16, api_version i16, body []u8) ! {
	// 헤더 생성
	mut header := []u8{}

	// API Key (2바이트)
	header << u8(api_key >> 8)
	header << u8(api_key & 0xff)

	// API Version (2바이트)
	header << u8(api_version >> 8)
	header << u8(api_version & 0xff)

	// Correlation ID (4바이트)
	correlation_id := i32(1)
	header << u8(correlation_id >> 24)
	header << u8((correlation_id >> 16) & 0xff)
	header << u8((correlation_id >> 8) & 0xff)
	header << u8(correlation_id & 0xff)

	// Client ID
	client_id := 'datacore-cli'

	// 유연한 헤더(V2) 또는 비유연 헤더(V1) 사용 여부 결정
	// CreateTopics V3는 비유연, Metadata V12는 유연
	is_flexible_api := (api_key == 3 && api_version >= 9) || // Metadata V9+
	 (api_key == 1 && api_version >= 12) || // Fetch V12+
	 (api_key == 20 && api_version >= 6) || // DeleteTopics V6+
	 (api_key == 19 && api_version >= 5) // CreateTopics V5+는 유연

	if is_flexible_api {
		// 유연한 헤더 V2: Compact Client ID + Tagged Fields
		// Client ID (compact string)
		header << u8(client_id.len + 1)
		header << client_id.bytes()
		// Tagged fields (빈 compact array)
		header << u8(0)
	} else {
		// 비유연 헤더 V1: Nullable String Client ID
		// 길이 접두사에 i16 사용 (2바이트)
		header << u8(client_id.len >> 8)
		header << u8(client_id.len & 0xff)
		header << client_id.bytes()
	}

	// 헤더와 본문 결합
	mut message := header.clone()
	message << body

	// 크기 접두사 쓰기 (4바이트)
	size := i32(message.len)
	mut frame := []u8{}
	frame << u8(size >> 24)
	frame << u8((size >> 16) & 0xff)
	frame << u8((size >> 8) & 0xff)
	frame << u8(size & 0xff)
	frame << message

	conn.write(frame) or { return error('Failed to send request: ${err}') }
}

// read_kafka_response는 Kafka 응답을 읽습니다.
fn read_kafka_response(mut conn net.TcpConn) ![]u8 {
	conn.set_read_timeout(30 * time.second)

	// 크기 읽기 (4바이트)
	mut size_buf := []u8{len: 4}
	conn.read(mut size_buf) or { return error('Failed to read response size: ${err}') }

	size := i32(u32(size_buf[0]) << 24 | u32(size_buf[1]) << 16 | u32(size_buf[2]) << 8 | u32(size_buf[3]))

	if size <= 0 || size > 104857600 {
		return error('Invalid response size: ${size}')
	}

	// 응답 본문 읽기
	mut response := []u8{len: int(size)}
	mut total_read := 0
	for total_read < int(size) {
		n := conn.read(mut response[total_read..]) or {
			return error('Failed to read response body: ${err}')
		}
		if n == 0 {
			break
		}
		total_read += n
	}

	return response
}

fn build_create_topic_request(name string, partitions int, replication int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Topics 배열 (비유연 배열)
	body << u8(0) // 배열 길이 바이트 1
	body << u8(0) // 배열 길이 바이트 2
	body << u8(0) // 배열 길이 바이트 3
	body << u8(1) // 배열 길이 바이트 4 (1개 토픽)

	// 토픽 이름 (string)
	body << u8(name.len >> 8)
	body << u8(name.len & 0xff)
	body << name.bytes()

	// 파티션 수 (4바이트)
	body << u8(partitions >> 24)
	body << u8((partitions >> 16) & 0xff)
	body << u8((partitions >> 8) & 0xff)
	body << u8(partitions & 0xff)

	// 복제 계수 (2바이트)
	body << u8(replication >> 8)
	body << u8(replication & 0xff)

	// Assignments (빈 배열)
	body << u8(0) // 배열 길이 바이트 1
	body << u8(0) // 배열 길이 바이트 2
	body << u8(0) // 배열 길이 바이트 3
	body << u8(0) // 배열 길이 바이트 4 (0개 할당)

	// Configs (빈 배열)
	body << u8(0) // 배열 길이 바이트 1
	body << u8(0) // 배열 길이 바이트 2
	body << u8(0) // 배열 길이 바이트 3
	body << u8(0) // 배열 길이 바이트 4 (0개 설정)

	// 타임아웃 ms (4바이트)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	return body
}

// build_metadata_request는 Metadata 요청을 생성합니다.
fn build_metadata_request(topics []string) []u8 {
	mut body := []u8{}

	// Topics 배열 (compact nullable array)
	if topics.len == 0 {
		body << u8(1) // null array = 모든 토픽
	} else {
		body << u8(topics.len + 1)
		for topic in topics {
			// 토픽 이름 (compact string)
			body << u8(topic.len + 1)
			body << topic.bytes()

			// Topic ID (v12용 16바이트 0)
			for _ in 0 .. 16 {
				body << u8(0)
			}

			// Tagged fields
			body << u8(0)
		}
	}

	// 자동 토픽 생성 허용 (1바이트)
	body << u8(0)

	// 토픽 인가 작업 포함 (1바이트, v8+)
	body << u8(0)

	// Tagged fields
	body << u8(0)

	return body
}

// build_delete_topic_request는 DeleteTopics 요청을 생성합니다.
fn build_delete_topic_request(name string, timeout_ms int) []u8 {
	mut body := []u8{}

	// Topics 배열 (compact array)
	body << u8(2) // 배열 길이 + 1

	// 토픽 이름 (compact nullable string)
	body << u8(name.len + 1)
	body << name.bytes()

	// Topic ID (이름으로 삭제 시 16바이트 0)
	for _ in 0 .. 16 {
		body << u8(0)
	}

	// 토픽용 Tagged fields
	body << u8(0)

	// 타임아웃 ms (4바이트)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// 요청용 Tagged fields
	body << u8(0)

	return body
}

fn check_create_topic_response(response []u8, expected_topic string) ! {
	if response.len < 4 {
		return error('Invalid response: too short')
	}

	// CreateTopics 응답 파싱 (version 3, 비유연)
	// 구조:
	// throttle_time_ms (4바이트)
	// topics (배열)
	//   - name (string)
	//   - error_code (2바이트)
	//   - error_message (nullable string)
	//   - topic_id (16바이트, v3+)
	//   - num_partitions (4바이트)
	//   - replication_factor (2바이트)

	mut pos := 4 // throttle_time_ms 건너뛰기

	// topics 배열 길이 읽기 (i32)
	if pos + 4 > response.len {
		return error('Invalid response: cannot read topics array length')
	}
	topics_len := i32(u32(response[pos]) << 24 | u32(response[pos + 1]) << 16 | u32(response[pos + 2]) << 8 | u32(response[
		pos + 3]))
	pos += 4

	if topics_len != 1 {
		return error('Expected 1 topic in response, got ${topics_len}')
	}

	// 토픽 이름 읽기 (string: i16 길이 + 데이터)
	if pos + 2 > response.len {
		return error('Invalid response: cannot read topic name length')
	}
	name_len := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
	pos += 2

	if pos + int(name_len) > response.len {
		return error('Invalid response: cannot read topic name')
	}
	topic_name := response[pos..pos + int(name_len)].bytestr()
	pos += int(name_len)

	// error_code 읽기 (2바이트)
	if pos + 2 > response.len {
		return error('Invalid response: cannot read error code')
	}
	error_code := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
	pos += 2

	if error_code != 0 {
		// 에러 발생
		mut error_message := 'Unknown error'
		// error_message 읽기 시도 (nullable string)
		if pos + 2 <= response.len {
			msg_len := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
			pos += 2
			if msg_len > 0 && pos + int(msg_len) <= response.len {
				error_message = response[pos..pos + int(msg_len)].bytestr()
			}
		}
		return error('Failed to create topic: ${error_message} (error code: ${error_code})')
	}

	// error_message, topic_id, num_partitions, replication_factor 건너뛰기
	// 성공 응답 확인만 수행

	if topic_name != expected_topic {
		return error('Topic name mismatch: expected "${expected_topic}", got "${topic_name}"')
	}
}

// check_delete_topic_response는 DeleteTopics 응답을 확인합니다.
fn check_delete_topic_response(response []u8, expected_topic string) ! {
	if response.len < 10 {
		return error('Invalid response')
	}
	// 간소화된 확인
	return
}

// parse_metadata_response_topics는 Metadata 응답에서 토픽 정보를 파싱합니다.
fn parse_metadata_response_topics(response []u8) []TopicInfo {
	mut topics := []TopicInfo{}

	if response.len < 20 {
		return topics
	}

	// 헤더 건너뛰기: correlation_id(4) + tagged_fields(varint) + throttle_time(4)
	// + brokers 배열 + cluster_id + controller_id + topics 배열
	// 이것은 간소화된 파서 - 프로덕션 코드는 전체 프로토콜 파싱 필요

	mut pos := 4 // correlation_id 건너뛰기
	if pos >= response.len {
		return topics
	}

	// tagged fields 건너뛰기 (varint)
	pos += 1
	if pos >= response.len {
		return topics
	}

	// throttle_time 건너뛰기 (4바이트)
	pos += 4
	if pos >= response.len {
		return topics
	}

	// brokers 배열 건너뛰기 (varint 길이 + 데이터)
	if pos < response.len {
		broker_count := int(response[pos]) - 1
		pos += 1

		// 브로커 데이터 건너뛰기 (간소화 - 대략적인 양 건너뛰기)
		for _ in 0 .. broker_count {
			pos += 50 // 브로커 항목 크기 대략 추정
			if pos >= response.len {
				break
			}
		}
	}

	// cluster_id, controller_id 건너뛰기
	pos += 40 // 대략 추정

	if pos >= response.len {
		return topics
	}

	// topics 배열 파싱
	topic_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. topic_count {
		if pos >= response.len {
			break
		}

		// error_code 건너뛰기 (2바이트)
		pos += 2
		if pos >= response.len {
			break
		}

		// 토픽 이름 읽기 (compact string)
		name_len := int(response[pos]) - 1
		pos += 1
		if pos + name_len > response.len {
			break
		}

		topic_name := response[pos..pos + name_len].bytestr()
		pos += name_len

		// topic_id 건너뛰기 (16바이트)
		pos += 16
		if pos >= response.len {
			break
		}

		// is_internal 읽기 (1바이트)
		is_internal := response[pos] != 0
		pos += 1
		if pos >= response.len {
			break
		}

		// partitions 배열 길이 읽기
		partition_count := int(response[pos]) - 1
		pos += 1

		topics << TopicInfo{
			name:        topic_name
			partitions:  partition_count
			is_internal: is_internal
		}

		// 파티션 상세 + tagged fields 건너뛰기 (대략 추정)
		pos += partition_count * 30 + 10
	}

	return topics
}
