// 인프라 레이어 - Kafka Metadata API 핸들러 (API Key 3)
// Metadata 요청/응답 타입, 파싱, 인코딩 및 핸들러 구현
//
// 이 모듈은 Kafka Metadata API를 구현합니다.
// 클라이언트가 클러스터의 브로커 정보, 토픽 메타데이터,
// 파티션 리더 정보 등을 조회할 때 사용됩니다.
module kafka

import domain
import infra.observability
import time

// Metadata (API Key 3) - 클러스터 메타데이터 조회 API

/// Metadata 요청 - 클러스터 및 토픽 메타데이터 조회 요청
///
/// topics가 비어있으면 모든 토픽의 메타데이터를 반환합니다.
/// allow_auto_topic_creation이 true이면 존재하지 않는 토픽을 자동 생성합니다.
pub struct MetadataRequest {
pub:
	topics                         []MetadataRequestTopic // 조회할 토픽 목록 (빈 배열: 전체)
	allow_auto_topic_creation      bool                   // 토픽 자동 생성 허용 여부
	include_cluster_authorized_ops bool                   // 클러스터 권한 작업 포함 여부 (v8-10)
	include_topic_authorized_ops   bool                   // 토픽 권한 작업 포함 여부 (v8+)
}

/// Metadata 요청 토픽 - 조회할 토픽 정보
pub struct MetadataRequestTopic {
pub:
	topic_id []u8    // 토픽 UUID (v10+)
	name     ?string // 토픽 이름 (nullable)
}

/// Metadata 응답 - 클러스터 및 토픽 메타데이터
pub struct MetadataResponse {
pub:
	throttle_time_ms       i32                      // 스로틀링 시간 (밀리초)
	brokers                []MetadataResponseBroker // 브로커 목록
	cluster_id             ?string                  // 클러스터 ID
	controller_id          i32                      // 컨트롤러 브로커 ID
	topics                 []MetadataResponseTopic  // 토픽 메타데이터 목록
	cluster_authorized_ops i32                      // 클러스터 권한 작업 비트마스크
}

/// Metadata 응답 브로커 - 브로커 정보
pub struct MetadataResponseBroker {
pub:
	node_id i32     // 브로커 노드 ID
	host    string  // 호스트명
	port    i32     // 포트 번호
	rack    ?string // 랙 ID (nullable)
}

/// Metadata 응답 토픽 - 토픽 메타데이터
pub struct MetadataResponseTopic {
pub:
	error_code           i16                         // 에러 코드
	name                 string                      // 토픽 이름
	topic_id             []u8                        // 토픽 UUID (v10+)
	is_internal          bool                        // 내부 토픽 여부
	partitions           []MetadataResponsePartition // 파티션 목록
	topic_authorized_ops i32                         // 토픽 권한 작업 비트마스크
}

/// Metadata 응답 파티션 - 파티션 메타데이터
pub struct MetadataResponsePartition {
pub:
	error_code       i16   // 에러 코드
	partition_index  i32   // 파티션 인덱스
	leader_id        i32   // 리더 브로커 ID
	leader_epoch     i32   // 리더 에포크 (v7+)
	replica_nodes    []i32 // 복제본 브로커 ID 목록
	isr_nodes        []i32 // ISR(In-Sync Replicas) 브로커 ID 목록
	offline_replicas []i32 // 오프라인 복제본 브로커 ID 목록 (v5+)
}

// Metadata 요청을 파싱합니다.
// 버전에 따라 다른 필드들을 읽어 MetadataRequest 구조체를 생성합니다.
fn parse_metadata_request(mut reader BinaryReader, version i16, is_flexible bool) !MetadataRequest {
	mut topics := []MetadataRequestTopic{}

	// 토픽 배열 파싱
	topic_count := reader.read_flex_array_len(is_flexible)!

	// topic_count >= 0: 특정 토픽 조회, -1: 모든 토픽 조회
	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			// v10+에서 토픽 UUID 지원
			mut topic_id := []u8{}
			if version >= 10 {
				topic_id = reader.read_uuid()!
			}

			mut topic := MetadataRequestTopic{}
			if is_flexible {
				name := reader.read_compact_string()!
				topic = MetadataRequestTopic{
					topic_id: topic_id
					name:     if name.len > 0 { name } else { none }
				}
			} else {
				topic = MetadataRequestTopic{
					name: reader.read_nullable_string()!
				}
			}

			// flexible 버전에서 tagged fields 건너뛰기
			if is_flexible && reader.remaining() > 0 {
				reader.skip_tagged_fields()!
			}

			topics << topic
		}
	}

	// v4+에서 allow_auto_topic_creation 필드 추가
	mut allow_auto_topic_creation := true
	if version >= 4 {
		allow_auto_topic_creation = reader.read_i8()! != 0
	}

	// v8-10에서 include_cluster_authorized_ops, include_topic_authorized_ops 필드
	// v11+에서는 include_topic_authorized_ops만 존재
	mut include_cluster_authorized_ops := false
	mut include_topic_authorized_ops := false
	if version >= 8 && version <= 10 {
		include_cluster_authorized_ops = reader.read_i8()! != 0
		include_topic_authorized_ops = reader.read_i8()! != 0
	} else if version >= 11 {
		include_topic_authorized_ops = reader.read_i8()! != 0
	}

	return MetadataRequest{
		topics:                         topics
		allow_auto_topic_creation:      allow_auto_topic_creation
		include_cluster_authorized_ops: include_cluster_authorized_ops
		include_topic_authorized_ops:   include_topic_authorized_ops
	}
}

/// Metadata 응답을 바이트 배열로 인코딩합니다.
/// 버전에 따라 flexible 또는 non-flexible 형식으로 인코딩합니다.
pub fn (r MetadataResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	// v3+에서 throttle_time_ms 필드 추가
	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	// 브로커 배열 인코딩
	if is_flexible {
		writer.write_compact_array_len(r.brokers.len)
	} else {
		writer.write_array_len(r.brokers.len)
	}

	for b in r.brokers {
		writer.write_i32(b.node_id)
		if is_flexible {
			writer.write_compact_string(b.host)
		} else {
			writer.write_string(b.host)
		}
		writer.write_i32(b.port)
		// v1+에서 rack 필드 추가
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(b.rack)
			} else {
				writer.write_nullable_string(b.rack)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// v2+에서 cluster_id 필드 추가
	if version >= 2 {
		if is_flexible {
			writer.write_compact_nullable_string(r.cluster_id)
		} else {
			writer.write_nullable_string(r.cluster_id)
		}
	}

	// v1+에서 controller_id 필드 추가
	if version >= 1 {
		writer.write_i32(r.controller_id)
	}

	// 토픽 배열 인코딩
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		writer.write_i16(t.error_code)
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// v10+에서 topic_id (UUID) 필드 추가
		if version >= 10 {
			writer.write_uuid(t.topic_id)
		}

		// v1+에서 is_internal 필드 추가
		if version >= 1 {
			writer.write_i8(if t.is_internal { i8(1) } else { i8(0) })
		}

		// 파티션 배열 인코딩
		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i16(p.error_code)
			writer.write_i32(p.partition_index)
			writer.write_i32(p.leader_id)
			// v7+에서 leader_epoch 필드 추가
			if version >= 7 {
				writer.write_i32(p.leader_epoch)
			}

			// 복제본 노드 배열 인코딩
			if is_flexible {
				writer.write_compact_array_len(p.replica_nodes.len)
			} else {
				writer.write_array_len(p.replica_nodes.len)
			}
			for n in p.replica_nodes {
				writer.write_i32(n)
			}

			// ISR 노드 배열 인코딩
			if is_flexible {
				writer.write_compact_array_len(p.isr_nodes.len)
			} else {
				writer.write_array_len(p.isr_nodes.len)
			}
			for n in p.isr_nodes {
				writer.write_i32(n)
			}

			// v5+에서 offline_replicas 배열 추가
			if version >= 5 {
				if is_flexible {
					writer.write_compact_array_len(p.offline_replicas.len)
				} else {
					writer.write_array_len(p.offline_replicas.len)
				}
				for n in p.offline_replicas {
					writer.write_i32(n)
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		// v8+에서 topic_authorized_ops 필드 추가
		if version >= 8 {
			writer.write_i32(t.topic_authorized_ops)
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// v8-10에서 cluster_authorized_ops 필드 추가
	if version >= 8 && version <= 10 {
		writer.write_i32(r.cluster_authorized_ops)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	resp_bytes := writer.bytes()
	return resp_bytes
}

// 레거시 핸들러 - 바이트 배열 기반 요청 처리
fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata,
		version))!
	resp := h.process_metadata(req, version)!
	return resp.encode(version)
}

// Metadata 요청을 처리합니다.
// 클러스터의 브로커 정보와 요청된 토픽들의 메타데이터를 조회하여 응답을 생성합니다.
fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing metadata request', observability.field_int('topics', req.topics.len),
		observability.field_bool('allow_auto_create', req.allow_auto_topic_creation))

	mut resp_topics := []MetadataResponseTopic{}

	// 멀티 브로커 모드에서 활성 브로커 목록 조회
	mut brokers := []MetadataResponseBroker{}
	mut active_broker_ids := []i32{}

	if mut registry := h.broker_registry {
		// 멀티 브로커 모드: 레지스트리에서 모든 활성 브로커 조회
		active_brokers := registry.list_active_brokers() or { []domain.BrokerInfo{} }
		for broker in active_brokers {
			brokers << MetadataResponseBroker{
				node_id: broker.broker_id
				host:    broker.host
				port:    broker.port
				rack:    if broker.rack.len > 0 { broker.rack } else { none }
			}
			active_broker_ids << broker.broker_id
		}
	}

	// 브로커가 없거나 싱글 브로커 모드인 경우 자신을 브로커로 추가
	if brokers.len == 0 {
		if h.broker_registry == none {
			h.logger.debug('Metadata response: broker_registry not available, returning local broker only',
				observability.field_int('broker_id', h.broker_id), observability.field_string('host',
				h.host), observability.field_int('port', h.broker_port))
		} else {
			h.logger.debug('Metadata response: broker_registry returned empty active brokers, returning local broker only',
				observability.field_int('broker_id', h.broker_id))
		}
		brokers << MetadataResponseBroker{
			node_id: h.broker_id
			host:    h.host
			port:    h.broker_port
			rack:    none
		}
		active_broker_ids << h.broker_id
	}

	// 특정 토픽이 요청된 경우
	if req.topics.len > 0 {
		for req_topic in req.topics {
			topic_name := req_topic.name or { '' }
			if topic_name.len == 0 {
				continue
			}

			// 토픽 조회 또는 자동 생성
			topic := h.storage.get_topic(topic_name) or {
				if req.allow_auto_topic_creation {
					// 토픽 자동 생성 시도
					h.storage.create_topic(topic_name, 1, domain.TopicConfig{}) or {
						resp_topics << MetadataResponseTopic{
							error_code:           i16(ErrorCode.unknown_topic_or_partition)
							name:                 topic_name
							topic_id:             []u8{len: 16}
							is_internal:          false
							partitions:           []
							topic_authorized_ops: -2147483648 // 권한 정보 없음
						}
						continue
					}
					h.storage.get_topic(topic_name) or {
						resp_topics << MetadataResponseTopic{
							error_code:           i16(ErrorCode.unknown_topic_or_partition)
							name:                 topic_name
							topic_id:             []u8{len: 16}
							is_internal:          false
							partitions:           []
							topic_authorized_ops: -2147483648
						}
						continue
					}
				} else {
					// 토픽이 존재하지 않고 자동 생성이 비활성화된 경우
					resp_topics << MetadataResponseTopic{
						error_code:           i16(ErrorCode.unknown_topic_or_partition)
						name:                 topic_name
						topic_id:             []u8{len: 16}
						is_internal:          false
						partitions:           []
						topic_authorized_ops: -2147483648
					}
					continue
				}
			}

			// 파티션 메타데이터 생성
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				// 파티션 할당 서비스에서 리더 브로커 조회 (동적 할당)
				mut leader_id := h.broker_id
				if mut assigner := h.partition_assigner {
					// 할당 서비스에서 파티션 리더 조회
					assigned_leader := assigner.get_partition_leader(topic.name, i32(p)) or {
						h.broker_id
					}
					leader_id = assigned_leader
				} else if active_broker_ids.len > 1 {
					// 할당 서비스가 없는 경우 라운드 로빈 사용 (backward compatibility)
					leader_id = active_broker_ids[p % active_broker_ids.len]
				}
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        leader_id
					leader_epoch:     0
					replica_nodes:    active_broker_ids.clone()
					isr_nodes:        active_broker_ids.clone()
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:           0
				name:                 topic.name
				topic_id:             topic.topic_id
				is_internal:          topic.is_internal
				partitions:           partitions
				topic_authorized_ops: -2147483648
			}
		}
	} else {
		// 모든 토픽 조회
		topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }

		for topic in topic_list {
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				// 파티션 할당 서비스에서 리더 브로커 조회 (동적 할당)
				mut leader_id := h.broker_id
				if mut assigner := h.partition_assigner {
					// 할당 서비스에서 파티션 리더 조회
					assigned_leader := assigner.get_partition_leader(topic.name, i32(p)) or {
						h.broker_id
					}
					leader_id = assigned_leader
				} else if active_broker_ids.len > 1 {
					// 할당 서비스가 없는 경우 라운드 로빈 사용 (backward compatibility)
					leader_id = active_broker_ids[p % active_broker_ids.len]
				}
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        leader_id
					leader_epoch:     0
					replica_nodes:    active_broker_ids.clone()
					isr_nodes:        active_broker_ids.clone()
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:           0
				name:                 topic.name
				topic_id:             topic.topic_id
				is_internal:          topic.is_internal
				partitions:           partitions
				topic_authorized_ops: -2147483648
			}
		}
	}

	// 컨트롤러는 멀티 브로커 모드에서 첫 번째 활성 브로커, 싱글 브로커 모드에서는 자신
	controller_id := if active_broker_ids.len > 0 { active_broker_ids[0] } else { h.broker_id }

	elapsed := time.since(start_time)
	h.logger.debug('Metadata request completed', observability.field_int('brokers', brokers.len),
		observability.field_int('topics', resp_topics.len), observability.field_duration('latency',
		elapsed))

	return MetadataResponse{
		throttle_time_ms:       0
		brokers:                brokers
		cluster_id:             h.cluster_id
		controller_id:          controller_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648
	}
}
