// Interface Layer - CLI Group Commands
//
// 컨슈머 그룹 관리 명령어를 제공합니다 (간소화된 구현).
// 그룹 목록 조회 및 상세 정보 확인 기능을 지원합니다.
//
// 주요 기능:
// - 컨슈머 그룹 목록 조회
// - 컨슈머 그룹 상세 정보 조회
module cli

import net as _
import time as _

/// GroupOptions는 그룹 명령어 옵션을 담는 구조체입니다.
pub struct GroupOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	group_id         string
}

/// parse_group_options는 그룹 명령어 옵션을 파싱합니다.
pub fn parse_group_options(args []string) GroupOptions {
	mut opts := GroupOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--group', '-g' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						group_id: args[i + 1]
					}
					i += 1
				}
			}
			else {
				// 위치 인자가 group_id일 수 있음
				if !args[i].starts_with('-') && opts.group_id.len == 0 {
					opts = GroupOptions{
						...opts
						group_id: args[i]
					}
				}
			}
		}
		i += 1
	}

	return opts
}

/// run_group_list는 모든 컨슈머 그룹을 나열합니다.
pub fn run_group_list(opts GroupOptions) ! {
	println('\x1b[90m⏳ Listing consumer groups...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// ListGroups 요청 생성 (API Key 16, Version 4)
	request := build_list_groups_request()

	// 요청 전송
	send_kafka_request(mut conn, 16, 4, request)!

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 간소화된 파싱 - 응답 유효성만 확인
	if response.len < 10 {
		return error('Failed to list groups: Invalid response')
	}

	println('\x1b[33mConsumer Groups:\x1b[0m')
	println('  (Note: Full parsing not yet implemented)')
	println('  Use kafka-consumer-groups.sh for complete information')
}

/// run_group_describe는 컨슈머 그룹의 상세 정보를 표시합니다.
pub fn run_group_describe(opts GroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use --group <group-id>')
	}

	println('\x1b[90m⏳ Describing group "${opts.group_id}"...\x1b[0m')

	// 브로커에 연결
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DescribeGroups 요청 생성 (API Key 15, Version 5)
	request := build_describe_groups_request(opts.group_id)

	// 요청 전송
	send_kafka_request(mut conn, 15, 5, request)!

	// 응답 읽기
	response := read_kafka_response(mut conn)!

	// 간소화된 파싱 - 응답 유효성만 확인
	if response.len < 10 {
		return error('Failed to describe group: Invalid response')
	}

	println('\x1b[33mGroup:\x1b[0m ${opts.group_id}')
	println('  (Note: Full parsing not yet implemented)')
	println('  Use kafka-consumer-groups.sh for complete information')
}

fn build_list_groups_request() []u8 {
	mut body := []u8{}

	// Groups 필터 (빈 배열 = 모든 그룹)
	body << u8(1)

	// Tagged fields (빈)
	body << u8(0)

	return body
}

// build_describe_groups_request는 DescribeGroups 요청을 생성합니다.
fn build_describe_groups_request(group_id string) []u8 {
	mut body := []u8{}

	// Groups 배열 (compact array)
	body << u8(2)

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// 인가 작업 포함 (1바이트)
	body << u8(0)

	// Tagged fields (빈)
	body << u8(0)

	return body
}
