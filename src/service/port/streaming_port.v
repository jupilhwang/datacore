// 실시간 메시지 스트리밍 기능을 추상화합니다.
module port

import domain

// 스트리밍 포트 인터페이스

/// StreamingPort는 메시지 스트리밍 작업을 정의합니다.
/// SSE, WebSocket, gRPC 등 다양한 스트리밍 메커니즘을 추상화합니다.
pub interface StreamingPort {
mut:
	/// 새로운 연결을 등록합니다.
	/// 반환값: 연결 ID
	register_connection(conn domain.SSEConnection) !string

	/// 연결을 해제합니다.
	unregister_connection(conn_id string) !

	/// 연결 정보를 조회합니다.
	get_connection(conn_id string) !domain.SSEConnection

	/// 모든 활성 연결 목록을 반환합니다.
	list_connections() []domain.SSEConnection
	/// 토픽/파티션을 구독합니다.
	subscribe(conn_id string, sub domain.Subscription) !

	/// 구독을 해제합니다.
	unsubscribe(conn_id string, topic string, partition ?i32) !

	/// 연결의 모든 구독 목록을 반환합니다.
	get_subscriptions(conn_id string) []domain.Subscription
	/// 특정 연결에 이벤트를 전송합니다.
	send_event(conn_id string, event domain.SSEEvent) !

	/// 토픽/파티션을 구독 중인 모든 연결에 이벤트를 브로드캐스트합니다.
	broadcast_event(topic string, partition i32, event domain.SSEEvent) !
	/// 스트리밍 통계를 반환합니다.
	get_stats() StreamingStats
}

/// StreamingStats는 스트리밍 통계 정보를 담습니다.
pub struct StreamingStats {
pub:
	active_connections  int
	total_subscriptions int
	messages_sent       i64
	bytes_sent          i64
	connections_created i64
	connections_closed  i64
}

// 메시지 컨슈머 포트 (메시지 조회용)

/// MessageConsumerPort는 메시지 소비 작업을 정의합니다.
/// 스트리밍 서비스에서 스토리지로부터 메시지를 가져올 때 사용합니다.
pub interface MessageConsumerPort {
mut:
	/// 토픽/파티션에서 지정된 오프셋부터 메시지를 가져옵니다.
	consume(topic string, partition i32, offset i64, max_messages int) ![]domain.Record

	/// 가장 이른 사용 가능한 오프셋을 반환합니다.
	get_earliest_offset(topic string, partition i32) !i64

	/// 최신 오프셋을 반환합니다 (다음에 기록될 오프셋).
	get_latest_offset(topic string, partition i32) !i64

	/// 컨슈머 그룹의 오프셋을 커밋합니다.
	commit_offset(group_id string, topic string, partition i32, offset i64) !

	/// 컨슈머 그룹의 커밋된 오프셋을 조회합니다.
	get_committed_offset(group_id string, topic string, partition i32) !i64
}

// SSE 라이터 포트

/// SSEWriterPort는 HTTP 응답에 SSE 이벤트를 쓰기 위한 인터페이스입니다.
pub interface SSEWriterPort {
mut:
	/// SSE 이벤트를 응답에 씁니다.
	write_event(event domain.SSEEvent) !

	/// 응답 버퍼를 플러시합니다.
	flush() !

	/// 연결이 아직 살아있는지 확인합니다.
	is_alive() bool

	/// 연결을 종료합니다.
	close() !
}

// 구독 필터

/// SubscriptionFilter는 메시지 필터링 조건을 정의합니다.
pub struct SubscriptionFilter {
pub:
	key_pattern    ?string
	header_filters map[string]string
	value_contains ?string
}

/// matches는 레코드가 필터 조건에 맞는지 확인합니다.
pub fn (f &SubscriptionFilter) matches(record domain.Record) bool {
	// 키 패턴 매칭
	if pattern := f.key_pattern {
		if record.key.len == 0 {
			return false
		}
		// 간단한 glob 매칭 (TODO: 적절한 glob/정규식 구현)
		if !simple_match(pattern, record.key.bytestr()) {
			return false
		}
	}

	// 헤더 필터 (record.headers는 map[string][]u8 타입)
	for key, expected_value in f.header_filters {
		if header_value := record.headers[key] {
			if header_value.bytestr() != expected_value {
				return false
			}
		} else {
			return false
		}
	}

	// 값 포함 여부
	if contains := f.value_contains {
		if !record.value.bytestr().contains(contains) {
			return false
		}
	}

	return true
}

/// simple_match는 간단한 와일드카드 매칭을 수행합니다.
fn simple_match(pattern string, value string) bool {
	if pattern == '*' {
		return true
	}
	if pattern.starts_with('*') && pattern.ends_with('*') {
		return value.contains(pattern[1..pattern.len - 1])
	}
	if pattern.starts_with('*') {
		return value.ends_with(pattern[1..])
	}
	if pattern.ends_with('*') {
		return value.starts_with(pattern[..pattern.len - 1])
	}
	return pattern == value
}

// 스트리밍 오류

/// StreamingError는 스트리밍 관련 오류를 나타냅니다.
pub enum StreamingError {
	connection_not_found
	subscription_not_found
	max_connections_reached
	max_subscriptions_reached
	topic_not_found
	partition_not_found
	invalid_offset
	connection_closed
	write_failed
}

/// streaming_error_message는 StreamingError에 대한 오류 메시지를 반환합니다.
pub fn streaming_error_message(err StreamingError) string {
	return match err {
		.connection_not_found { 'Connection not found' }
		.subscription_not_found { 'Subscription not found' }
		.max_connections_reached { 'Maximum connections reached' }
		.max_subscriptions_reached { 'Maximum subscriptions reached' }
		.topic_not_found { 'Topic not found' }
		.partition_not_found { 'Partition not found' }
		.invalid_offset { 'Invalid offset' }
		.connection_closed { 'Connection closed' }
		.write_failed { 'Write failed' }
	}
}
