// 도메인 레이어 - 에러 정의
// Kafka 호환 에러 코드를 정의합니다.
module domain

/// DomainError는 도메인 수준 에러를 나타냅니다.
/// code: Kafka 에러 코드
/// message: 에러 메시지
pub struct DomainError {
pub:
	code    ErrorCode
	message string
}

/// ErrorCode는 Kafka 호환 에러 코드를 나타냅니다.
/// Kafka 프로토콜 스펙에 정의된 에러 코드와 동일합니다.
pub enum ErrorCode {
	none                             = 0   // 성공
	unknown_server_error             = -1  // 알 수 없는 서버 에러
	offset_out_of_range              = 1   // 오프셋이 범위를 벗어남
	corrupt_message                  = 2   // 손상된 메시지
	unknown_topic_or_partition       = 3   // 알 수 없는 토픽 또는 파티션
	invalid_fetch_size               = 4   // 잘못된 fetch 크기
	leader_not_available             = 5   // 리더를 사용할 수 없음
	not_leader_or_follower           = 6   // 리더 또는 팔로워가 아님
	request_timed_out                = 7   // 요청 타임아웃
	broker_not_available             = 8   // 브로커를 사용할 수 없음
	replica_not_available            = 9   // 레플리카를 사용할 수 없음
	message_too_large                = 10  // 메시지가 너무 큼
	stale_controller_epoch           = 11  // 오래된 컨트롤러 에포크
	offset_metadata_too_large        = 12  // 오프셋 메타데이터가 너무 큼
	network_exception                = 13  // 네트워크 예외
	coordinator_load_in_progress     = 14  // 코디네이터 로드 중
	coordinator_not_available        = 15  // 코디네이터를 사용할 수 없음
	not_coordinator                  = 16  // 코디네이터가 아님
	invalid_topic_exception          = 17  // 잘못된 토픽
	record_list_too_large            = 18  // 레코드 목록이 너무 큼
	not_enough_replicas              = 19  // 레플리카가 부족함
	not_enough_replicas_after_append = 20  // 추가 후 레플리카 부족
	invalid_required_acks            = 21  // 잘못된 acks 요구사항
	illegal_generation               = 22  // 잘못된 세대
	inconsistent_group_protocol      = 23  // 일관성 없는 그룹 프로토콜
	invalid_group_id                 = 24  // 잘못된 그룹 ID
	unknown_member_id                = 25  // 알 수 없는 멤버 ID
	invalid_session_timeout          = 26  // 잘못된 세션 타임아웃
	rebalance_in_progress            = 27  // 리밸런싱 진행 중
	invalid_commit_offset_size       = 28  // 잘못된 커밋 오프셋 크기
	topic_authorization_failed       = 29  // 토픽 권한 없음
	group_authorization_failed       = 30  // 그룹 권한 없음
	cluster_authorization_failed     = 31  // 클러스터 권한 없음
	invalid_timestamp                = 32  // 잘못된 타임스탬프
	unsupported_sasl_mechanism       = 33  // 지원하지 않는 SASL 메커니즘
	illegal_sasl_state               = 34  // 잘못된 SASL 상태
	unsupported_version              = 35  // 지원하지 않는 버전
	topic_already_exists             = 36  // 토픽이 이미 존재함
	invalid_partitions               = 37  // 잘못된 파티션
	invalid_replication_factor       = 38  // 잘못된 복제 팩터
	invalid_replica_assignment       = 39  // 잘못된 레플리카 할당
	invalid_config                   = 40  // 잘못된 설정
	not_controller                   = 41  // 컨트롤러가 아님
	invalid_request                  = 42  // 잘못된 요청
	sasl_authentication_failed       = 58  // SASL 인증 실패
	group_id_not_found               = 69  // 그룹 ID를 찾을 수 없음
	unknown_topic_id                 = 100 // 알 수 없는 토픽 ID
}

/// message는 에러 코드에 해당하는 메시지를 반환합니다.
pub fn (e ErrorCode) message() string {
	return match e {
		.none { 'Success' }
		.unknown_server_error { 'Unknown server error' }
		.offset_out_of_range { 'Offset out of range' }
		.corrupt_message { 'Corrupt message' }
		.unknown_topic_or_partition { 'Unknown topic or partition' }
		.invalid_fetch_size { 'Invalid fetch size' }
		.leader_not_available { 'Leader not available' }
		.not_leader_or_follower { 'Not leader or follower' }
		.request_timed_out { 'Request timed out' }
		.broker_not_available { 'Broker not available' }
		.topic_already_exists { 'Topic already exists' }
		.invalid_partitions { 'Invalid partitions' }
		.invalid_replication_factor { 'Invalid replication factor' }
		.group_id_not_found { 'Group ID not found' }
		.unknown_topic_id { 'Unknown topic ID' }
		else { 'Error code: ${int(e)}' }
	}
}
