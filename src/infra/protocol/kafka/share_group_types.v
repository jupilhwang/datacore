// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// 요청/응답 구조체 정의
//
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
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

/// ShareGroupHeartbeat 응답 - Share Group 하트비트 응답
pub struct ShareGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         string
	member_id             string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ShareGroupAssignment
}

/// ShareGroup 할당 - 하트비트 응답의 파티션 할당
pub struct ShareGroupAssignment {
pub:
	topic_partitions []ShareGroupTopicPartitions
}

/// ShareGroup 토픽 파티션 - 토픽별 파티션 할당
pub struct ShareGroupTopicPartitions {
pub:
	topic_id   []u8 // 토픽 UUID (16바이트)
	partitions []i32
}

// ShareFetch (API Key 78) 타입 - Share Group 메시지 조회

/// ShareFetch 요청 - Share Group에서 메시지를 가져오는 요청
///
/// 일반 Fetch와 달리 메시지를 "획득(acquire)"하며,
/// 획득한 메시지는 확인(acknowledge)될 때까지 다른 컨슈머에게 전달되지 않습니다.
pub struct ShareFetchRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	max_wait_ms         i32
	min_bytes           i32
	max_bytes           i32
	max_records         i32
	batch_size          i32
	topics              []ShareFetchTopic
	forgotten_topics    []ShareForgottenTopic
}

/// ShareFetch 토픽 - Share Fetch 요청의 토픽
pub struct ShareFetchTopic {
pub:
	topic_id   []u8
	partitions []ShareFetchPartition
}

/// ShareFetch 파티션 - 조회할 파티션
pub struct ShareFetchPartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

/// Share 확인 배치 - 메시지 확인 배치
///
/// 메시지 범위에 대한 확인 유형을 지정합니다.
/// 0: Gap (갭), 1: Accept (수락), 2: Release (해제), 3: Reject (거부)
pub struct ShareAcknowledgementBatch {
pub:
	first_offset      i64
	last_offset       i64
	acknowledge_types []u8
}

/// Share 잊혀진 토픽 - 세션에서 제거할 토픽
pub struct ShareForgottenTopic {
pub:
	topic_id   []u8
	partitions []i32
}

/// ShareFetch 응답 - Share Fetch 요청에 대한 응답
pub struct ShareFetchResponse {
pub:
	throttle_time_ms            i32
	error_code                  i16
	error_message               string
	acquisition_lock_timeout_ms i32
	responses                   []ShareFetchTopicResponse
	node_endpoints              []ShareNodeEndpoint
}

/// ShareFetch 토픽 응답 - 토픽별 응답
pub struct ShareFetchTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareFetchPartitionResponse
}

/// ShareFetch 파티션 응답 - 파티션별 응답
pub struct ShareFetchPartitionResponse {
pub:
	partition_index           i32
	error_code                i16
	error_message             string
	acknowledge_error_code    i16
	acknowledge_error_message string
	current_leader            ShareLeaderIdAndEpoch
	records                   []u8
	acquired_records          []ShareAcquiredRecords
}

/// Share 리더 ID와 에포크 - 리더 정보
pub struct ShareLeaderIdAndEpoch {
pub:
	leader_id    i32
	leader_epoch i32
}

/// Share 획득된 레코드 - 획득된 레코드 범위
pub struct ShareAcquiredRecords {
pub:
	first_offset   i64
	last_offset    i64
	delivery_count i16
}

/// Share 노드 엔드포인트 - 브로커 엔드포인트
pub struct ShareNodeEndpoint {
pub:
	node_id i32
	host    string
	port    i32
	rack    string
}

// ShareAcknowledge (API Key 79) 타입 - Share Group 메시지 확인

/// ShareAcknowledge 요청 - 획득한 메시지를 확인하는 요청
///
/// 컨슈머가 메시지 처리를 완료한 후 확인을 전송합니다.
/// Accept, Release, Reject 등의 확인 유형을 지원합니다.
pub struct ShareAcknowledgeRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	topics              []ShareAcknowledgeTopic
}

/// ShareAcknowledge 토픽 - 확인할 토픽
pub struct ShareAcknowledgeTopic {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartition
}

/// ShareAcknowledge 파티션 - 확인할 파티션
pub struct ShareAcknowledgePartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

/// ShareAcknowledge 응답 - 메시지 확인 결과
pub struct ShareAcknowledgeResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    string
	responses        []ShareAcknowledgeTopicResponse
	node_endpoints   []ShareNodeEndpoint
}

/// ShareAcknowledge 토픽 응답 - 토픽별 확인 결과
pub struct ShareAcknowledgeTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartitionResponse
}

/// ShareAcknowledge 파티션 응답 - 파티션별 확인 결과
pub struct ShareAcknowledgePartitionResponse {
pub:
	partition_index i32
	error_code      i16
	error_message   string
	current_leader  ShareLeaderIdAndEpoch
}
