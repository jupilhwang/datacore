// 인프라 레이어 - Kafka 프로토콜 핸들러
// 메인 핸들러 구조체 및 요청 라우팅
//
// 이 모듈은 Kafka 프로토콜의 핵심 핸들러를 구현합니다.
// 클라이언트 요청을 파싱하고 적절한 API 핸들러로 라우팅하여
// 응답을 생성합니다.
module kafka

import infra.observability
import rand
import service.cluster
import service.group
import service.port
import service.transaction
import time

/// 프로토콜 핸들러 - Kafka 요청을 처리하고 응답을 생성하는 핵심 구조체
///
/// Handler는 브로커의 모든 Kafka API 요청을 처리합니다.
/// 스토리지, 인증, ACL, 트랜잭션 코디네이터 등 다양한 컴포넌트와 연동됩니다.
pub struct Handler {
	broker_id   i32
	host        string
	broker_port i32
	cluster_id  string
mut:
	storage                 port.StoragePort
	broker_registry         ?&cluster.BrokerRegistry            // Optional: Broker registry for multi-broker mode
	auth_manager            ?port.AuthManager                   // Optional: SASL authentication manager
	acl_manager             ?port.AclManager                    // Optional: ACL manager
	txn_coordinator         ?transaction.TransactionCoordinator // Optional: Transaction coordinator
	share_group_coordinator ?&group.ShareGroupCoordinator       // Optional: Share group coordinator (KIP-932)
	logger                  &observability.Logger
}

/// 스토리지와 함께 새로운 Kafka 프로토콜 핸들러를 생성합니다.
///
/// 기본 핸들러로, 인증이나 ACL 없이 스토리지만 사용합니다.
/// 개발/테스트 환경에 적합합니다.
pub fn new_handler(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort) Handler {
	return Handler{
		broker_id:               broker_id
		host:                    host
		broker_port:             broker_port
		cluster_id:              cluster_id
		storage:                 storage
		broker_registry:         none
		auth_manager:            none
		acl_manager:             none
		txn_coordinator:         none
		share_group_coordinator: none
		logger:                  observability.get_named_logger('kafka.handler')
	}
}

/// 스토리지와 인증 매니저를 포함한 새로운 Kafka 프로토콜 핸들러를 생성합니다.
///
/// SASL 인증이 필요한 환경에서 사용합니다.
pub fn new_handler_with_auth(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager port.AuthManager) Handler {
	return Handler{
		broker_id:               broker_id
		host:                    host
		broker_port:             broker_port
		cluster_id:              cluster_id
		storage:                 storage
		broker_registry:         none
		auth_manager:            auth_manager
		acl_manager:             none
		txn_coordinator:         none
		share_group_coordinator: none
		logger:                  observability.get_named_logger('kafka.handler')
	}
}

/// 모든 컴포넌트를 포함한 완전한 Kafka 프로토콜 핸들러를 생성합니다.
///
/// 프로덕션 환경에서 인증, ACL, 트랜잭션을 모두 지원할 때 사용합니다.
pub fn new_handler_full(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?transaction.TransactionCoordinator) Handler {
	return Handler{
		broker_id:               broker_id
		host:                    host
		broker_port:             broker_port
		cluster_id:              cluster_id
		storage:                 storage
		broker_registry:         none
		auth_manager:            auth_manager
		acl_manager:             acl_manager
		txn_coordinator:         txn_coordinator
		share_group_coordinator: none
		logger:                  observability.get_named_logger('kafka.handler')
	}
}

/// Share Group 지원을 포함한 Kafka 프로토콜 핸들러를 생성합니다 (KIP-932).
///
/// Share Group은 Kafka 4.0에서 도입된 새로운 컨슈머 그룹 유형으로,
/// 큐 기반 메시지 소비 패턴을 지원합니다.
pub fn new_handler_with_share_groups(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?transaction.TransactionCoordinator, share_coordinator &group.ShareGroupCoordinator) Handler {
	return Handler{
		broker_id:               broker_id
		host:                    host
		broker_port:             broker_port
		cluster_id:              cluster_id
		storage:                 storage
		broker_registry:         none
		auth_manager:            auth_manager
		acl_manager:             acl_manager
		txn_coordinator:         txn_coordinator
		share_group_coordinator: share_coordinator
		logger:                  observability.get_named_logger('kafka.handler')
	}
}

/// 멀티 브로커 모드를 위한 브로커 레지스트리를 설정합니다.
///
/// 클러스터 환경에서 브로커 간 조정을 위해 사용됩니다.
pub fn (mut h Handler) set_broker_registry(registry &cluster.BrokerRegistry) {
	h.broker_registry = registry
}

/// 핸들러에 Share Group 코디네이터를 설정합니다.
pub fn (mut h Handler) set_share_group_coordinator(coordinator &group.ShareGroupCoordinator) {
	h.share_group_coordinator = coordinator
}

/// 수신된 요청을 처리하고 응답 바이트를 반환합니다 (레거시 메서드).
///
/// 이 메서드는 원시 바이트 데이터를 받아 파싱하고,
/// 적절한 API 핸들러로 라우팅한 후 응답을 생성합니다.
pub fn (mut h Handler) handle_request(data []u8) ![]u8 {
	start_time := time.now()

	// 요청 파싱
	req := parse_request(data) or {
		h.logger.error('Failed to parse request', observability.field_err_str(err.str()),
			observability.field_bytes('request_size', data.len))
		return err
	}

	api_key := unsafe { ApiKey(req.header.api_key) }
	version := req.header.api_version
	correlation_id := req.header.correlation_id

	h.logger.debug('Processing request', observability.field_string('api', api_key.str()),
		observability.field_int('version', version), observability.field_int('correlation_id',
		correlation_id), observability.field_bytes('request_size', data.len))

	// API 키에 따라 처리
	response_body := match api_key {
		.api_versions {
			h.handle_api_versions(version)
		}
		.metadata {
			h.handle_metadata(req.body, version)!
		}
		.find_coordinator {
			h.handle_find_coordinator(req.body, version)!
		}
		.produce {
			h.handle_produce(req.body, version)!
		}
		.fetch {
			h.handle_fetch(req.body, version)!
		}
		.list_offsets {
			h.handle_list_offsets(req.body, version)!
		}
		.offset_commit {
			h.handle_offset_commit(req.body, version)!
		}
		.offset_fetch {
			h.handle_offset_fetch(req.body, version)!
		}
		.join_group {
			h.handle_join_group(req.body, version)!
		}
		.sync_group {
			h.handle_sync_group(req.body, version)!
		}
		.heartbeat {
			h.handle_heartbeat(req.body, version)!
		}
		.leave_group {
			h.handle_leave_group(req.body, version)!
		}
		.list_groups {
			h.handle_list_groups(req.body, version)!
		}
		.describe_groups {
			h.handle_describe_groups(req.body, version)!
		}
		.create_topics {
			h.handle_create_topics(req.body, version)!
		}
		.delete_topics {
			h.handle_delete_topics(req.body, version)!
		}
		.init_producer_id {
			h.handle_init_producer_id(req.body, version)!
		}
		.add_partitions_to_txn {
			h.handle_add_partitions_to_txn(req.body, version)!
		}
		.add_offsets_to_txn {
			h.handle_add_offsets_to_txn(req.body, version)!
		}
		.end_txn {
			h.handle_end_txn(req.body, version)!
		}
		.write_txn_markers {
			h.handle_write_txn_markers(req.body, version)!
		}
		.txn_offset_commit {
			h.handle_txn_offset_commit(req.body, version)!
		}
		.consumer_group_heartbeat {
			h.handle_consumer_group_heartbeat(req.body, version)!
		}
		.sasl_handshake {
			h.handle_sasl_handshake(req.body, version)!
		}
		.sasl_authenticate {
			h.handle_sasl_authenticate(req.body, version)!
		}
		.describe_cluster {
			h.handle_describe_cluster(req.body, version)!
		}
		.describe_configs {
			h.handle_describe_configs(req.body, version)!
		}
		.describe_acls {
			h.handle_describe_acls(req.body, version)!
		}
		.create_acls {
			h.handle_create_acls(req.body, version)!
		}
		.delete_acls {
			h.handle_delete_acls(req.body, version)!
		}
		.alter_configs {
			h.handle_alter_configs(req.body, version)!
		}
		.create_partitions {
			h.handle_create_partitions(req.body, version)!
		}
		.delete_records {
			h.handle_delete_records(req.body, version)!
		}
		.share_group_heartbeat {
			h.handle_share_group_heartbeat(req.body, version)!
		}
		.share_fetch {
			h.handle_share_fetch(req.body, version)!
		}
		.share_acknowledge {
			h.handle_share_acknowledge(req.body, version)!
		}
		else {
			h.logger.warn('Unsupported API key', observability.field_int('api_key', int(api_key)))
			return error('unsupported API key: ${int(api_key)}')
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Request completed', observability.field_string('api', api_key.str()),
		observability.field_int('correlation_id', correlation_id), observability.field_bytes('response_size',
		response_body.len), observability.field_duration('latency', elapsed))

	// 헤더와 함께 응답 빌드
	// 참고: ApiVersions는 특별함 - v3+에서도 항상 non-flexible 헤더 사용
	// 클라이언트가 아직 서버 기능을 모르기 때문
	if api_key == .api_versions {
		return build_response(correlation_id, response_body)
	}

	is_flexible := is_flexible_version(api_key, version)
	if is_flexible {
		return build_flexible_response(correlation_id, response_body)
	}
	return build_response(correlation_id, response_body)
}

/// 수신된 Frame을 처리하고 응답 Frame을 반환합니다.
///
/// 이것이 요청을 처리하는 권장 방법입니다.
/// Frame 기반 처리는 더 나은 타입 안전성과 구조화된 처리를 제공합니다.
pub fn (mut h Handler) handle_frame(frame Frame) !Frame {
	// 요청 헤더 정보 추출
	req_header := match frame.header {
		FrameRequestHeader { frame.header as FrameRequestHeader }
		else { return error('expected request header') }
	}

	api_key := req_header.api_key
	version := req_header.api_version
	correlation_id := req_header.correlation_id

	// 타입에 따라 본문 처리 후 응답 본문 반환
	response_body := h.process_body(frame.body, api_key, version)!

	// 응답 프레임 빌드
	return frame_response(correlation_id, api_key, version, response_body)
}

// 본문을 처리하고 응답 본문을 반환합니다.
fn (mut h Handler) process_body(body Body, api_key ApiKey, version i16) !Body {
	return match body {
		ApiVersionsRequest {
			Body(new_api_versions_response())
		}
		MetadataRequest {
			Body(h.process_metadata(body, version)!)
		}
		ProduceRequest {
			Body(h.process_produce(body, version)!)
		}
		FetchRequest {
			Body(h.process_fetch(body, version)!)
		}
		FindCoordinatorRequest {
			Body(h.process_find_coordinator(body, version)!)
		}
		JoinGroupRequest {
			Body(h.process_join_group(body, version)!)
		}
		SyncGroupRequest {
			Body(h.process_sync_group(body, version)!)
		}
		HeartbeatRequest {
			Body(h.process_heartbeat(body, version)!)
		}
		LeaveGroupRequest {
			Body(h.process_leave_group(body, version)!)
		}
		OffsetCommitRequest {
			Body(h.process_offset_commit(body, version)!)
		}
		OffsetFetchRequest {
			Body(h.process_offset_fetch(body, version)!)
		}
		ListOffsetsRequest {
			Body(h.process_list_offsets(body, version)!)
		}
		CreateTopicsRequest {
			Body(h.process_create_topics(body, version)!)
		}
		DeleteTopicsRequest {
			Body(h.process_delete_topics(body, version)!)
		}
		ListGroupsRequest {
			Body(h.process_list_groups(body, version)!)
		}
		DescribeGroupsRequest {
			Body(h.process_describe_groups(body, version)!)
		}
		InitProducerIdRequest {
			Body(h.process_init_producer_id(body, version)!)
		}
		ConsumerGroupHeartbeatRequest {
			Body(h.process_consumer_group_heartbeat(body, version)!)
		}
		DescribeClusterRequest {
			Body(h.process_describe_cluster(body, version)!)
		}
		DescribeConfigsRequest {
			Body(h.process_describe_configs(body, version)!)
		}
		else {
			return error('unsupported request type')
		}
	}
}

// 최소한의 (빈) 컨슈머 그룹 할당 페이로드를 빌드합니다.
fn empty_consumer_assignment() []u8 {
	mut writer := new_writer()
	// 컨슈머 프로토콜 버전
	writer.write_i16(0)
	// 할당 배열 길이 (0개 토픽)
	writer.write_i32(0)
	// 사용자 데이터 길이 (0)
	writer.write_i32(0)
	return writer.bytes()
}

// 설정값을 i64로 파싱하는 헬퍼 함수
fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
	val := configs[key] or { return default_val }
	return val.i64()
}

// 설정값을 int로 파싱하는 헬퍼 함수
fn parse_config_int(configs map[string]string, key string, default_val int) int {
	val := configs[key] or { return default_val }
	return val.int()
}

// 랜덤 UUID v4를 생성합니다 (16바이트).
fn generate_uuid() []u8 {
	mut uuid := []u8{len: 16}
	for i in 0 .. 16 {
		uuid[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	// 버전 (4) 및 변형 (RFC 4122) 설정
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // 버전 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // RFC 4122 변형
	return uuid
}
