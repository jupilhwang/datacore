// 인프라 레이어 - Kafka Share Group 타입 정의 (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// 요청/응답 구조체 정의
//
// 이 모듈은 Kafka 4.0에서 도입된 Share Group 프로토콜의 타입들을 정의합니다.
// Share Group은 큐 기반 메시지 소비 패턴을 지원하며,
// 메시지 확인(acknowledge) 메커니즘을 통해 at-least-once 전달을 보장합니다.
module kafka

// ShareGroupHeartbeat (API Key 76) 타입 - Share Group 하트비트

/// ShareGroupHeartbeat 요청 - Share Group 멤버십 유지를 위한 하트비트
///
/// Share Group 멤버가 주기적으로 전송하여 그룹 멤버십을 유지합니다.
/// 구독 토픽 변경 시에도 이 요청을 통해 알립니다.
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string   // 그룹 ID
	member_id              string   // 멤버 ID
	member_epoch           i32      // 멤버 에포크
	rack_id                string   // 랙 ID
	subscribed_topic_names []string // 구독 토픽 이름 목록
}

/// ShareGroupHeartbeat 응답 - Share Group 하트비트 응답
pub struct ShareGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32                   // 스로틀링 시간 (밀리초)
	error_code            i16                   // 에러 코드
	error_message         string                // 에러 메시지
	member_id             string                // 멤버 ID
	member_epoch          i32                   // 멤버 에포크
	heartbeat_interval_ms i32                   // 하트비트 간격 (밀리초)
	assignment            ?ShareGroupAssignment // 파티션 할당
}

/// ShareGroup 할당 - 하트비트 응답의 파티션 할당
pub struct ShareGroupAssignment {
pub:
	topic_partitions []ShareGroupTopicPartitions // 토픽별 파티션 할당
}

/// ShareGroup 토픽 파티션 - 토픽별 파티션 할당
pub struct ShareGroupTopicPartitions {
pub:
	topic_id   []u8  // 토픽 UUID (16바이트)
	partitions []i32 // 파티션 인덱스 목록
}

// ShareFetch (API Key 78) 타입 - Share Group 메시지 조회

/// ShareFetch 요청 - Share Group에서 메시지를 가져오는 요청
///
/// 일반 Fetch와 달리 메시지를 "획득(acquire)"하며,
/// 획득한 메시지는 확인(acknowledge)될 때까지 다른 컨슈머에게 전달되지 않습니다.
pub struct ShareFetchRequest {
pub:
	group_id            string                // 그룹 ID
	member_id           string                // 멤버 ID
	share_session_epoch i32                   // Share 세션 에포크
	max_wait_ms         i32                   // 최대 대기 시간 (밀리초)
	min_bytes           i32                   // 최소 응답 바이트 수
	max_bytes           i32                   // 최대 응답 바이트 수
	max_records         i32                   // 최대 레코드 수
	batch_size          i32                   // 배치 크기
	topics              []ShareFetchTopic     // 조회할 토픽 목록
	forgotten_topics    []ShareForgottenTopic // 세션에서 제거할 토픽
}

/// ShareFetch 토픽 - Share Fetch 요청의 토픽
pub struct ShareFetchTopic {
pub:
	topic_id   []u8                  // 토픽 UUID
	partitions []ShareFetchPartition // 조회할 파티션 목록
}

/// ShareFetch 파티션 - 조회할 파티션
pub struct ShareFetchPartition {
pub:
	partition_index         i32                         // 파티션 인덱스
	acknowledgement_batches []ShareAcknowledgementBatch // 확인 배치 (피기백)
}

/// Share 확인 배치 - 메시지 확인 배치
///
/// 메시지 범위에 대한 확인 유형을 지정합니다.
/// 0: Gap (갭), 1: Accept (수락), 2: Release (해제), 3: Reject (거부)
pub struct ShareAcknowledgementBatch {
pub:
	first_offset      i64  // 첫 번째 오프셋
	last_offset       i64  // 마지막 오프셋
	acknowledge_types []u8 // 확인 유형 (0:Gap, 1:Accept, 2:Release, 3:Reject)
}

/// Share 잊혀진 토픽 - 세션에서 제거할 토픽
pub struct ShareForgottenTopic {
pub:
	topic_id   []u8  // 토픽 UUID
	partitions []i32 // 제거할 파티션 목록
}

/// ShareFetch 응답 - Share Fetch 요청에 대한 응답
pub struct ShareFetchResponse {
pub:
	throttle_time_ms            i32                       // 스로틀링 시간 (밀리초)
	error_code                  i16                       // 에러 코드
	error_message               string                    // 에러 메시지
	acquisition_lock_timeout_ms i32                       // 획득 잠금 타임아웃 (밀리초)
	responses                   []ShareFetchTopicResponse // 토픽별 응답
	node_endpoints              []ShareNodeEndpoint       // 노드 엔드포인트 목록
}

/// ShareFetch 토픽 응답 - 토픽별 응답
pub struct ShareFetchTopicResponse {
pub:
	topic_id   []u8 // 토픽 UUID
	partitions []ShareFetchPartitionResponse // 파티션별 응답
}

/// ShareFetch 파티션 응답 - 파티션별 응답
pub struct ShareFetchPartitionResponse {
pub:
	partition_index           i32                    // 파티션 인덱스
	error_code                i16                    // 에러 코드
	error_message             string                 // 에러 메시지
	acknowledge_error_code    i16                    // 확인 에러 코드
	acknowledge_error_message string                 // 확인 에러 메시지
	current_leader            ShareLeaderIdAndEpoch  // 현재 리더 정보
	records                   []u8                   // 레코드 배치 바이트
	acquired_records          []ShareAcquiredRecords // 획득된 레코드 범위
}

/// Share 리더 ID와 에포크 - 리더 정보
pub struct ShareLeaderIdAndEpoch {
pub:
	leader_id    i32 // 리더 브로커 ID
	leader_epoch i32 // 리더 에포크
}

/// Share 획득된 레코드 - 획득된 레코드 범위
pub struct ShareAcquiredRecords {
pub:
	first_offset   i64 // 첫 번째 오프셋
	last_offset    i64 // 마지막 오프셋
	delivery_count i16 // 전달 횟수
}

/// Share 노드 엔드포인트 - 브로커 엔드포인트
pub struct ShareNodeEndpoint {
pub:
	node_id i32    // 노드 ID
	host    string // 호스트명
	port    i32    // 포트 번호
	rack    string // 랙 ID
}

// ShareAcknowledge (API Key 79) 타입 - Share Group 메시지 확인

/// ShareAcknowledge 요청 - 획득한 메시지를 확인하는 요청
///
/// 컨슈머가 메시지 처리를 완료한 후 확인을 전송합니다.
/// Accept, Release, Reject 등의 확인 유형을 지원합니다.
pub struct ShareAcknowledgeRequest {
pub:
	group_id            string                  // 그룹 ID
	member_id           string                  // 멤버 ID
	share_session_epoch i32                     // Share 세션 에포크
	topics              []ShareAcknowledgeTopic // 확인할 토픽 목록
}

/// ShareAcknowledge 토픽 - 확인할 토픽
pub struct ShareAcknowledgeTopic {
pub:
	topic_id   []u8                        // 토픽 UUID
	partitions []ShareAcknowledgePartition // 확인할 파티션 목록
}

/// ShareAcknowledge 파티션 - 확인할 파티션
pub struct ShareAcknowledgePartition {
pub:
	partition_index         i32                         // 파티션 인덱스
	acknowledgement_batches []ShareAcknowledgementBatch // 확인 배치
}

/// ShareAcknowledge 응답 - 메시지 확인 결과
pub struct ShareAcknowledgeResponse {
pub:
	throttle_time_ms i32    // 스로틀링 시간 (밀리초)
	error_code       i16    // 에러 코드
	error_message    string // 에러 메시지
	responses        []ShareAcknowledgeTopicResponse // 토픽별 응답
	node_endpoints   []ShareNodeEndpoint             // 노드 엔드포인트 목록
}

/// ShareAcknowledge 토픽 응답 - 토픽별 확인 결과
pub struct ShareAcknowledgeTopicResponse {
pub:
	topic_id   []u8 // 토픽 UUID
	partitions []ShareAcknowledgePartitionResponse // 파티션별 응답
}

/// ShareAcknowledge 파티션 응답 - 파티션별 확인 결과
pub struct ShareAcknowledgePartitionResponse {
pub:
	partition_index i32                   // 파티션 인덱스
	error_code      i16                   // 에러 코드
	error_message   string                // 에러 메시지
	current_leader  ShareLeaderIdAndEpoch // 현재 리더 정보
}
