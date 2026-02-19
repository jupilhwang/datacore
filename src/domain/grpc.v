// gRPC 스트리밍 프로토콜 도메인 모델
module domain

import time

// gRPC 요청/응답 타입
// V에서 Protocol Buffer 메시지에 해당하는 구조체들

/// GrpcProduceRequest는 gRPC produce 요청을 나타냅니다.
pub struct GrpcProduceRequest {
pub:
	topic     string       // 대상 토픽
	partition ?i32         // 대상 파티션 (선택사항, -1이면 자동 할당)
	records   []GrpcRecord // 전송할 레코드 목록
}

/// GrpcRecord는 gRPC 형식의 단일 레코드를 나타냅니다.
pub struct GrpcRecord {
pub:
	key       []u8            // 레코드 키 (선택사항)
	value     []u8            // 레코드 값
	headers   map[string][]u8 // 레코드 헤더
	timestamp i64             // 타임스탬프 (0이면 서버 시간 사용)
}

/// GrpcProduceResponse는 gRPC produce 응답을 나타냅니다.
pub struct GrpcProduceResponse {
pub:
	topic        string
	partition    i32
	base_offset  i64 // 생성된 레코드의 기본 오프셋
	record_count int // 생성된 레코드 수
	timestamp    i64 // 로그 추가 시간
	error_code   i32 // 에러 코드 (0 = 성공)
	error_msg    string
}

/// new_grpc_produce_response는 성공적인 produce 응답을 생성합니다.
pub fn new_grpc_produce_response(topic string, partition i32, base_offset i64, count int) GrpcProduceResponse {
	return GrpcProduceResponse{
		topic:        topic
		partition:    partition
		base_offset:  base_offset
		record_count: count
		timestamp:    time.now().unix_milli()
		error_code:   0
		error_msg:    ''
	}
}

/// new_grpc_produce_error는 에러 produce 응답을 생성합니다.
pub fn new_grpc_produce_error(topic string, partition i32, code i32, msg string) GrpcProduceResponse {
	return GrpcProduceResponse{
		topic:       topic
		partition:   partition
		base_offset: -1
		error_code:  code
		error_msg:   msg
		timestamp:   time.now().unix_milli()
	}
}

// Consume 스트리밍

/// GrpcConsumeRequest는 gRPC consume 요청을 나타냅니다.
pub struct GrpcConsumeRequest {
pub:
	topic       string // 소비할 토픽
	partition   i32
	offset      i64     // 시작 오프셋
	max_records int     // 배치당 최대 레코드 수
	max_bytes   int     // 배치당 최대 바이트 수
	group_id    ?string // 컨슈머 그룹 ID (선택사항)
}

/// GrpcConsumeResponse는 gRPC consume 응답(스트림 요소)을 나타냅니다.
pub struct GrpcConsumeResponse {
pub:
	topic          string
	partition      i32
	records        []GrpcRecord // 조회된 레코드 목록
	high_watermark i64          // 하이 워터마크 오프셋
	next_offset    i64          // 다음 조회 오프셋
	error_code     i32          // 에러 코드 (0 = 성공)
	error_msg      string
}

/// new_grpc_consume_response는 성공적인 consume 응답을 생성합니다.
pub fn new_grpc_consume_response(topic string, partition i32, records []GrpcRecord, hwm i64, next i64) GrpcConsumeResponse {
	return GrpcConsumeResponse{
		topic:          topic
		partition:      partition
		records:        records
		high_watermark: hwm
		next_offset:    next
		error_code:     0
		error_msg:      ''
	}
}

/// new_grpc_consume_error는 에러 consume 응답을 생성합니다.
pub fn new_grpc_consume_error(topic string, partition i32, code i32, msg string) GrpcConsumeResponse {
	return GrpcConsumeResponse{
		topic:      topic
		partition:  partition
		records:    []GrpcRecord{}
		error_code: code
		error_msg:  msg
	}
}

// 양방향 스트리밍

/// GrpcStreamRequest는 양방향 스트림 요청을 나타냅니다.
pub struct GrpcStreamRequest {
pub:
	request_type GrpcStreamRequestType // 요청 유형
	produce      ?GrpcProduceRequest   // produce 요청 (유형이 produce인 경우)
	consume      ?GrpcConsumeRequest   // consume 요청 (유형이 subscribe인 경우)
	commit       ?GrpcCommitRequest    // commit 요청 (유형이 commit인 경우)
	ack          ?GrpcAckRequest       // ack 요청 (유형이 ack인 경우)
}

/// GrpcStreamRequestType은 스트림 요청의 유형을 나타냅니다.
pub enum GrpcStreamRequestType {
	produce   // 레코드 전송
	subscribe // 토픽/파티션 구독
	commit    // 오프셋 커밋
	ack       // 메시지 수신 확인
	ping      // Keep-alive ping
}

/// grpc_stream_request_type_from_int는 정수를 GrpcStreamRequestType으로 변환합니다.
pub fn grpc_stream_request_type_from_int(i int) GrpcStreamRequestType {
	return match i {
		0 { .produce }
		1 { .subscribe }
		2 { .commit }
		3 { .ack }
		4 { .ping }
		else { .ping }
	}
}

/// GrpcCommitRequest는 오프셋 커밋 요청을 나타냅니다.
pub struct GrpcCommitRequest {
pub:
	group_id string                // 컨슈머 그룹 ID
	offsets  []GrpcPartitionOffset // 커밋할 오프셋 목록
}

/// GrpcPartitionOffset은 파티션 오프셋을 나타냅니다.
pub struct GrpcPartitionOffset {
pub:
	topic     string
	partition i32
	offset    i64    // 커밋할 오프셋
	metadata  string // 커밋 메타데이터
}

/// GrpcAckRequest는 메시지 수신 확인을 나타냅니다.
pub struct GrpcAckRequest {
pub:
	topic     string
	partition i32
	offset    i64 // 확인된 오프셋
}

/// GrpcStreamResponse는 양방향 스트림 응답을 나타냅니다.
pub struct GrpcStreamResponse {
pub:
	response_type GrpcStreamResponseType // 응답 유형
	produce       ?GrpcProduceResponse   // produce 결과
	message       ?GrpcMessageResponse   // 소비된 메시지
	commit        ?GrpcCommitResponse    // commit 결과
	error         ?GrpcErrorResponse     // 에러 응답
	pong          ?GrpcPongResponse      // pong 응답
}

/// GrpcStreamResponseType은 스트림 응답의 유형을 나타냅니다.
pub enum GrpcStreamResponseType {
	produce_ack // produce 확인
	message     // 소비된 메시지
	commit_ack  // commit 확인
	error       // 에러 응답
	pong        // pong 응답
}

/// GrpcMessageResponse는 소비된 메시지를 나타냅니다.
pub struct GrpcMessageResponse {
pub:
	topic     string
	partition i32
	offset    i64             // 메시지 오프셋
	timestamp i64             // 메시지 타임스탬프
	key       []u8            // 메시지 키
	value     []u8            // 메시지 값
	headers   map[string][]u8 // 메시지 헤더
}

/// GrpcCommitResponse는 커밋 결과를 나타냅니다.
pub struct GrpcCommitResponse {
pub:
	success bool   // 커밋 성공 여부
	message string // 결과 메시지
}

/// GrpcErrorResponse는 에러를 나타냅니다.
pub struct GrpcErrorResponse {
pub:
	code    i32
	message string
}

/// GrpcPongResponse는 pong을 나타냅니다.
pub struct GrpcPongResponse {
pub:
	timestamp i64 // 서버 타임스탬프
}

// gRPC 연결 상태

/// GrpcConnectionState는 gRPC 연결의 상태를 나타냅니다.
pub enum GrpcConnectionState {
	connecting // 초기 연결 중
	ready      // 요청 처리 준비 완료
	streaming  // 활성 스트리밍 중
	closing    // 정상 종료 중
	closed     // 연결 종료됨
}

/// GrpcConnection은 활성 gRPC 연결을 나타냅니다.
pub struct GrpcConnection {
pub:
	id         string // 연결 ID
	client_ip  string // 클라이언트 IP 주소
	created_at i64    // 연결 생성 시간
pub mut:
	state          GrpcConnectionState // 현재 상태
	subscriptions  []Subscription      // 활성 구독 목록 (consume 스트림용)
	requests_recv  i64                 // 총 수신 요청 수
	responses_sent i64                 // 총 송신 응답 수
	bytes_recv     i64                 // 총 수신 바이트
	bytes_sent     i64                 // 총 송신 바이트
	last_activity  i64                 // 마지막 활동 타임스탬프
	stream_type    GrpcStreamType      // 스트림 유형
}

/// GrpcStreamType은 gRPC 스트림의 유형을 나타냅니다.
pub enum GrpcStreamType {
	unary            // 단일 요청/응답
	server_streaming // 서버 스트리밍 (예: consume)
	client_streaming // 클라이언트 스트리밍 (예: 배치 produce)
	bidirectional    // 양방향 스트리밍
}

/// new_grpc_connection은 새로운 gRPC 연결을 생성합니다.
pub fn new_grpc_connection(client_ip string, stream_type GrpcStreamType) GrpcConnection {
	now := time.now().unix_milli()
	return GrpcConnection{
		id:             'grpc-${time.now().unix_nano()}'
		client_ip:      client_ip
		created_at:     now
		state:          .connecting
		subscriptions:  []Subscription{}
		requests_recv:  0
		responses_sent: 0
		bytes_recv:     0
		bytes_sent:     0
		last_activity:  now
		stream_type:    stream_type
	}
}

// gRPC 설정

/// GrpcConfig는 gRPC 서버 설정을 보관합니다.
pub struct GrpcConfig {
pub:
	port                   int  = 9093    // gRPC 서버 포트
	max_connections        int  = 10000   // 최대 동시 연결 수
	max_message_size       int  = 4194304 // 최대 메시지 크기 (4MB)
	max_concurrent_streams int  = 100     // 연결당 최대 동시 스트림 수
	keepalive_time_ms      int  = 30000   // Keepalive 시간 (30초)
	keepalive_timeout_ms   int  = 10000   // Keepalive 타임아웃 (10초)
	connection_timeout_ms  int  = 300000  // 연결 타임아웃 (5분)
	max_batch_size         int  = 1000    // 배치당 최대 레코드 수
	enable_reflection      bool = true    // 디버깅용 gRPC reflection 활성화
}

/// default_grpc_config는 기본 gRPC 설정을 반환합니다.
pub fn default_grpc_config() GrpcConfig {
	return GrpcConfig{}
}

// gRPC 에러 코드 (Kafka 호환)

pub const grpc_error_none = 0
pub const grpc_error_unknown = -1
pub const grpc_error_offset_out_of_range = 1
pub const grpc_error_invalid_message = 2
pub const grpc_error_unknown_topic = 3
pub const grpc_error_invalid_partition = 4
pub const grpc_error_leader_not_available = 5
pub const grpc_error_not_leader_for_partition = 6
pub const grpc_error_request_timed_out = 7
pub const grpc_error_message_too_large = 10
pub const grpc_error_group_coordinator_not_available = 15
pub const grpc_error_not_coordinator = 16
pub const grpc_error_invalid_topic = 17
pub const grpc_error_record_list_too_large = 18
pub const grpc_error_group_auth_failed = 30
pub const grpc_error_invalid_session_timeout = 26

/// grpc_error_message는 사람이 읽을 수 있는 에러 메시지를 반환합니다.
pub fn grpc_error_message(code i32) string {
	return match code {
		grpc_error_none { 'No error' }
		grpc_error_offset_out_of_range { 'Offset out of range' }
		grpc_error_invalid_message { 'Invalid message' }
		grpc_error_unknown_topic { 'Unknown topic' }
		grpc_error_invalid_partition { 'Invalid partition' }
		grpc_error_leader_not_available { 'Leader not available' }
		grpc_error_not_leader_for_partition { 'Not leader for partition' }
		grpc_error_request_timed_out { 'Request timed out' }
		grpc_error_message_too_large { 'Message too large' }
		grpc_error_group_coordinator_not_available { 'Group coordinator not available' }
		grpc_error_not_coordinator { 'Not coordinator' }
		grpc_error_invalid_topic { 'Invalid topic' }
		grpc_error_record_list_too_large { 'Record list too large' }
		grpc_error_group_auth_failed { 'Group authorization failed' }
		grpc_error_invalid_session_timeout { 'Invalid session timeout' }
		else { 'Unknown error' }
	}
}

// 바이너리 인코딩 헬퍼 (gRPC 와이어 포맷용)

/// encode는 GrpcRecord를 바이트로 인코딩합니다.
pub fn (r &GrpcRecord) encode() []u8 {
	mut buf := []u8{cap: 64 + r.key.len + r.value.len}

	// 키 길이 (4바이트) + 키
	key_len := r.key.len
	buf << u8(key_len >> 24)
	buf << u8(key_len >> 16)
	buf << u8(key_len >> 8)
	buf << u8(key_len)
	buf << r.key

	// 값 길이 (4바이트) + 값
	val_len := r.value.len
	buf << u8(val_len >> 24)
	buf << u8(val_len >> 16)
	buf << u8(val_len >> 8)
	buf << u8(val_len)
	buf << r.value

	// 타임스탬프 (8바이트)
	ts := r.timestamp
	buf << u8(ts >> 56)
	buf << u8(ts >> 48)
	buf << u8(ts >> 40)
	buf << u8(ts >> 32)
	buf << u8(ts >> 24)
	buf << u8(ts >> 16)
	buf << u8(ts >> 8)
	buf << u8(ts)

	// 헤더 개수 (4바이트)
	header_count := r.headers.len
	buf << u8(header_count >> 24)
	buf << u8(header_count >> 16)
	buf << u8(header_count >> 8)
	buf << u8(header_count)

	// 헤더들
	for k, v in r.headers {
		// 키 길이 (2바이트) + 키
		k_len := k.len
		buf << u8(k_len >> 8)
		buf << u8(k_len)
		buf << k.bytes()

		// 값 길이 (2바이트) + 값
		v_len := v.len
		buf << u8(v_len >> 8)
		buf << u8(v_len)
		buf << v
	}

	return buf
}

/// decode_grpc_record는 바이트를 GrpcRecord로 디코딩합니다.
pub fn decode_grpc_record(data []u8) !GrpcRecord {
	if data.len < 20 {
		return error('Data too short for GrpcRecord')
	}

	mut pos := 0

	// 키 길이 + 키
	key_len := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4
	if pos + key_len > data.len {
		return error('Invalid key length')
	}
	key := data[pos..pos + key_len].clone()
	pos += key_len

	// 값 길이 + 값
	if pos + 4 > data.len {
		return error('Data too short for value length')
	}
	val_len := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4
	if pos + val_len > data.len {
		return error('Invalid value length')
	}
	value := data[pos..pos + val_len].clone()
	pos += val_len

	// 타임스탬프
	if pos + 8 > data.len {
		return error('Data too short for timestamp')
	}
	timestamp := i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))
	pos += 8

	// 헤더 개수
	if pos + 4 > data.len {
		return error('Data too short for header count')
	}
	header_count := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4

	// 헤더들
	mut headers := map[string][]u8{}
	for _ in 0 .. header_count {
		// 헤더 키
		if pos + 2 > data.len {
			return error('Data too short for header key length')
		}
		hk_len := int(u32(data[pos]) << 8 | u32(data[pos + 1]))
		pos += 2
		if pos + hk_len > data.len {
			return error('Invalid header key length')
		}
		hk := data[pos..pos + hk_len].bytestr()
		pos += hk_len

		// 헤더 값
		if pos + 2 > data.len {
			return error('Data too short for header value length')
		}
		hv_len := int(u32(data[pos]) << 8 | u32(data[pos + 1]))
		pos += 2
		if pos + hv_len > data.len {
			return error('Invalid header value length')
		}
		hv := data[pos..pos + hv_len].clone()
		pos += hv_len

		headers[hk] = hv
	}

	return GrpcRecord{
		key:       key
		value:     value
		timestamp: timestamp
		headers:   headers
	}
}

/// to_domain_record는 GrpcRecord를 domain.Record로 변환합니다.
pub fn (r &GrpcRecord) to_domain_record() Record {
	return Record{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: if r.timestamp > 0 { time.unix(r.timestamp / 1000) } else { time.now() }
	}
}

/// grpc_record_from_domain은 domain.Record로부터 GrpcRecord를 생성합니다.
pub fn grpc_record_from_domain(r &Record) GrpcRecord {
	return GrpcRecord{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: r.timestamp.unix_milli()
	}
}
