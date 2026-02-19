// Kafka 프로토콜 프레임
// 프레임 기반 요청/응답 처리
module kafka

/// Frame은 완전한 Kafka 프로토콜 메시지를 나타냅니다.
/// 형식: [size: 4바이트][header][body]
pub struct Frame {
pub:
	size   i32
	header FrameHeader
	body   Body
}

/// FrameHeader는 요청 헤더 또는 응답 헤더일 수 있습니다.
pub type FrameHeader = FrameRequestHeader | FrameResponseHeader

/// Frame 요청 헤더 (v0 및 v2 형식 모두 지원)
pub struct FrameRequestHeader {
pub:
	api_key        ApiKey
	api_version    i16
	correlation_id i32
	client_id      ?string
}

/// Frame 응답 헤더
pub struct FrameResponseHeader {
pub:
	correlation_id i32
}

/// Body는 모든 요청 또는 응답 타입이 될 수 있습니다.
pub type Body = ApiVersionsRequest
	| ApiVersionsResponse
	| MetadataRequest
	| MetadataResponse
	| ProduceRequest
	| ProduceResponse
	| FetchRequest
	| FetchResponse
	| FindCoordinatorRequest
	| FindCoordinatorResponse
	| JoinGroupRequest
	| JoinGroupResponse
	| SyncGroupRequest
	| SyncGroupResponse
	| HeartbeatRequest
	| HeartbeatResponse
	| LeaveGroupRequest
	| LeaveGroupResponse
	| OffsetCommitRequest
	| OffsetCommitResponse
	| OffsetFetchRequest
	| OffsetFetchResponse
	| ListOffsetsRequest
	| ListOffsetsResponse
	| CreateTopicsRequest
	| CreateTopicsResponse
	| DeleteTopicsRequest
	| DeleteTopicsResponse
	| ListGroupsRequest
	| ListGroupsResponse
	| DescribeGroupsRequest
	| DescribeGroupsResponse
	| InitProducerIdRequest
	| InitProducerIdResponse
	| ConsumerGroupHeartbeatRequest
	| ConsumerGroupHeartbeatResponse
	| DescribeClusterRequest
	| DescribeClusterResponse
	| DescribeConfigsRequest
	| DescribeConfigsResponse

/// ApiKey 트레이트 - 각 요청 타입은 자신의 API 키를 알고 있습니다.
pub interface ApiKeyProvider {
	api_key() ApiKey
}

/// 원시 바이트에서 Frame을 파싱합니다 (요청).
pub fn frame_from_bytes(data []u8) !Frame {
	if data.len < 8 {
		return error('frame too short: need at least 8 bytes, got ${data.len}')
	}

	mut reader := new_reader(data)

	// 헤더 필드 파싱
	api_key_raw := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	api_key := unsafe { ApiKey(api_key_raw) }

	// ApiVersions는 헤더에서 항상 non-flexible입니다 (클라이언트가 서버 버전을 아직 모름)
	is_flexible := api_key != .api_versions && is_flexible_version(api_key, api_version)

	// 요청 헤더 v2 (flexible)에서 client_id는 여전히 NULLABLE_STRING입니다 (compact가 아님!)
	client_id := reader.read_nullable_string()!

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	// API 키에 따라 본문 파싱
	body_data := data[reader.pos..].clone()
	body := parse_body(api_key, api_version, body_data)!

	// 크기 계산 (size 필드 자체를 제외한 모든 것)
	size := i32(data.len)

	return Frame{
		size:   size
		header: FrameRequestHeader{
			api_key:        api_key
			api_version:    api_version
			correlation_id: correlation_id
			client_id:      if client_id.len > 0 { client_id } else { none }
		}
		body:   body
	}
}

/// API 키와 버전에 따라 본문을 파싱합니다.
fn parse_body(api_key ApiKey, version i16, data []u8) !Body {
	mut reader := new_reader(data)
	is_flexible := is_flexible_version(api_key, version)

	return match api_key {
		.api_versions {
			Body(parse_api_versions_request(mut reader, version, is_flexible)!)
		}
		.metadata {
			Body(parse_metadata_request(mut reader, version, is_flexible)!)
		}
		.produce {
			Body(parse_produce_request(mut reader, version, is_flexible)!)
		}
		.fetch {
			Body(parse_fetch_request(mut reader, version, is_flexible)!)
		}
		.find_coordinator {
			Body(parse_find_coordinator_request(mut reader, version, is_flexible)!)
		}
		.join_group {
			Body(parse_join_group_request(mut reader, version, is_flexible)!)
		}
		.sync_group {
			Body(parse_sync_group_request(mut reader, version, is_flexible)!)
		}
		.heartbeat {
			Body(parse_heartbeat_request(mut reader, version, is_flexible)!)
		}
		.leave_group {
			Body(parse_leave_group_request(mut reader, version, is_flexible)!)
		}
		.offset_commit {
			Body(parse_offset_commit_request(mut reader, version, is_flexible)!)
		}
		.offset_fetch {
			Body(parse_offset_fetch_request(mut reader, version, is_flexible)!)
		}
		.list_offsets {
			Body(parse_list_offsets_request(mut reader, version, is_flexible)!)
		}
		.create_topics {
			Body(parse_create_topics_request(mut reader, version, is_flexible)!)
		}
		.delete_topics {
			Body(parse_delete_topics_request(mut reader, version, is_flexible)!)
		}
		.list_groups {
			Body(parse_list_groups_request(mut reader, version, is_flexible)!)
		}
		.describe_groups {
			Body(parse_describe_groups_request(mut reader, version, is_flexible)!)
		}
		.init_producer_id {
			Body(parse_init_producer_id_request(mut reader, version, is_flexible)!)
		}
		.consumer_group_heartbeat {
			Body(parse_consumer_group_heartbeat_request(mut reader, version, is_flexible)!)
		}
		.describe_cluster {
			Body(parse_describe_cluster_request(mut reader, version, is_flexible)!)
		}
		.describe_configs {
			Body(parse_describe_configs_request(mut reader, version, is_flexible)!)
		}
		else {
			return error('unsupported API key: ${int(api_key)}')
		}
	}
}

/// 응답 Frame을 생성합니다.
pub fn frame_response(correlation_id i32, api_key ApiKey, version i16, body Body) Frame {
	return Frame{
		size:   0
		header: FrameResponseHeader{
			correlation_id: correlation_id
		}
		body:   body
	}
}

/// Frame을 바이트로 인코딩합니다 (응답용).
pub fn (f Frame) response_to_bytes(api_key ApiKey, version i16) []u8 {
	// 본문 바이트 획득
	body_bytes := encode_body(f.body, version)

	// ApiVersions는 항상 non-flexible 응답 헤더를 사용합니다.
	is_flexible := api_key != .api_versions && is_flexible_version(api_key, version)

	if is_flexible {
		resp_bytes := build_flexible_response(f.header.correlation_id(), body_bytes)
		return resp_bytes
	}
	return build_response(f.header.correlation_id(), body_bytes)
}

/// 헤더에서 correlation_id를 가져옵니다.
fn (h FrameHeader) correlation_id() i32 {
	return match h {
		FrameRequestHeader { h.correlation_id }
		FrameResponseHeader { h.correlation_id }
	}
}

/// 타입에 따라 본문을 인코딩합니다.
fn encode_body(body Body, version i16) []u8 {
	return match body {
		ApiVersionsResponse { body.encode(version) }
		MetadataResponse { body.encode(version) }
		ProduceResponse { body.encode(version) }
		FetchResponse { body.encode(version) }
		FindCoordinatorResponse { body.encode(version) }
		JoinGroupResponse { body.encode(version) }
		SyncGroupResponse { body.encode(version) }
		HeartbeatResponse { body.encode(version) }
		LeaveGroupResponse { body.encode(version) }
		OffsetCommitResponse { body.encode(version) }
		OffsetFetchResponse { body.encode(version) }
		ListOffsetsResponse { body.encode(version) }
		CreateTopicsResponse { body.encode(version) }
		DeleteTopicsResponse { body.encode(version) }
		ListGroupsResponse { body.encode(version) }
		DescribeGroupsResponse { body.encode(version) }
		InitProducerIdResponse { body.encode(version) }
		ConsumerGroupHeartbeatResponse { body.encode(version) }
		DescribeClusterResponse { body.encode(version) }
		DescribeConfigsResponse { body.encode(version) }
		else { []u8{} }
	}
}
