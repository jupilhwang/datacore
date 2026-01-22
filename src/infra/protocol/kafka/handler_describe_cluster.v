// Kafka 프로토콜 - DescribeCluster (API Key 60)
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
module kafka

import domain

// ============================================================================
// DescribeCluster (API Key 60)
// ============================================================================

pub struct DescribeClusterRequest {
pub:
	include_cluster_authorized_operations bool
}

pub struct DescribeClusterResponse {
pub:
	throttle_time_ms              i32
	error_code                    i16
	error_message                 ?string
	cluster_id                    string
	controller_id                 i32
	brokers                       []DescribeClusterBroker
	cluster_authorized_operations i32
}

pub struct DescribeClusterBroker {
pub:
	broker_id i32
	host      string
	port      i32
	rack      ?string
}

fn parse_describe_cluster_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeClusterRequest {
	include_cluster_authorized_operations := reader.read_i8()! != 0

	reader.skip_flex_tagged_fields(is_flexible)!

	return DescribeClusterRequest{
		include_cluster_authorized_operations: include_cluster_authorized_operations
	}
}

pub fn (r DescribeClusterResponse) encode(version i16) []u8 {
	is_flexible := version >= 0
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
		writer.write_compact_string(r.cluster_id)
	} else {
		writer.write_nullable_string(r.error_message)
		writer.write_string(r.cluster_id)
	}

	writer.write_i32(r.controller_id)

	if is_flexible {
		writer.write_compact_array_len(r.brokers.len)
	} else {
		writer.write_array_len(r.brokers.len)
	}

	for b in r.brokers {
		writer.write_i32(b.broker_id)
		if is_flexible {
			writer.write_compact_string(b.host)
		} else {
			writer.write_string(b.host)
		}
		writer.write_i32(b.port)
		if is_flexible {
			writer.write_compact_nullable_string(b.rack)
			writer.write_tagged_fields()
		} else {
			writer.write_nullable_string(b.rack)
		}
	}

	writer.write_i32(r.cluster_authorized_operations)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

fn (mut h Handler) handle_describe_cluster(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_cluster_request(mut reader, version, is_flexible_version(.describe_cluster,
		version))!
	resp := h.process_describe_cluster(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_describe_cluster(req DescribeClusterRequest, version i16) !DescribeClusterResponse {
	mut brokers := []DescribeClusterBroker{}
	mut controller_id := h.broker_id

	if mut registry := h.broker_registry {
		// Multi-broker mode: get all active brokers from registry
		active_brokers := registry.list_active_brokers() or { []domain.BrokerInfo{} }
		for broker in active_brokers {
			brokers << DescribeClusterBroker{
				broker_id: broker.broker_id
				host:      broker.host
				port:      broker.port
				rack:      if broker.rack.len > 0 { broker.rack } else { none }
			}
		}
		// Controller is the first active broker
		if active_brokers.len > 0 {
			controller_id = active_brokers[0].broker_id
		}
	}

	// Fallback to single broker if no brokers found
	if brokers.len == 0 {
		brokers << DescribeClusterBroker{
			broker_id: h.broker_id
			host:      h.host
			port:      h.broker_port
			rack:      none
		}
	}

	return DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    h.cluster_id
		controller_id:                 controller_id
		brokers:                       brokers
		cluster_authorized_operations: -2147483648
	}
}
