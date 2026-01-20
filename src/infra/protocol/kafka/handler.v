// Infra Layer - Kafka Protocol Handler
// Main handler structure and request routing
module kafka

import log
import rand
import service.port
import service.transaction

// Protocol Handler - processes Kafka requests and generates responses
pub struct Handler {
	broker_id   i32
	host        string
	broker_port i32
	cluster_id  string
mut:
	storage         port.StoragePort
	auth_manager    ?port.AuthManager                   // Optional: SASL authentication manager
	acl_manager     ?port.AclManager                    // Optional: ACL manager
	txn_coordinator ?transaction.TransactionCoordinator // Optional: Transaction coordinator
	logger          log.Log
}

// new_handler creates a new Kafka protocol handler with storage
pub fn new_handler(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort) Handler {
	eprintln('[DEBUG] new_handler: broker_id=${broker_id} host="${host}" (len=${host.len}) broker_port=${broker_port} cluster_id="${cluster_id}"')
	return Handler{
		broker_id:       broker_id
		host:            host
		broker_port:     broker_port
		cluster_id:      cluster_id
		storage:         storage
		auth_manager:    none
		acl_manager:     none
		txn_coordinator: none
		logger:          log.Log{}
	}
}

// new_handler_with_auth creates a new Kafka protocol handler with storage and authentication
pub fn new_handler_with_auth(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager port.AuthManager) Handler {
	return Handler{
		broker_id:       broker_id
		host:            host
		broker_port:     broker_port
		cluster_id:      cluster_id
		storage:         storage
		auth_manager:    auth_manager
		acl_manager:     none
		txn_coordinator: none
		logger:          log.Log{}
	}
}

// new_handler_full creates a new Kafka protocol handler with all components
pub fn new_handler_full(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?transaction.TransactionCoordinator) Handler {
	return Handler{
		broker_id:       broker_id
		host:            host
		broker_port:     broker_port
		cluster_id:      cluster_id
		storage:         storage
		auth_manager:    auth_manager
		acl_manager:     acl_manager
		txn_coordinator: txn_coordinator
		logger:          log.Log{}
	}
}

// Handle incoming request and return response bytes (legacy method)
pub fn (mut h Handler) handle_request(data []u8) ![]u8 {
	// Parse request
	req := parse_request(data)!

	api_key := unsafe { ApiKey(req.header.api_key) }
	version := req.header.api_version
	correlation_id := req.header.correlation_id

	// Handle by API key
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
		else {
			return error('unsupported API key: ${int(api_key)}')
		}
	}

	// Build response with header
	// Note: ApiVersions is special - always uses non-flexible header even for v3+
	// because clients don't know server capabilities yet
	if api_key == .api_versions {
		return build_response(correlation_id, response_body)
	}

	is_flexible := is_flexible_version(api_key, version)
	if is_flexible {
		return build_flexible_response(correlation_id, response_body)
	}
	return build_response(correlation_id, response_body)
}

// Handle incoming Frame and return response Frame
// This is the recommended way to process requests
pub fn (mut h Handler) handle_frame(frame Frame) !Frame {
	// Extract request header info
	req_header := match frame.header {
		FrameRequestHeader { frame.header as FrameRequestHeader }
		else { return error('expected request header') }
	}

	api_key := req_header.api_key
	version := req_header.api_version
	correlation_id := req_header.correlation_id

	// Process body based on type and return response body
	response_body := h.process_body(frame.body, api_key, version)!

	// Build response frame
	return frame_response(correlation_id, api_key, version, response_body)
}

// Process body and return response body
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

// Build a minimal (empty) consumer group assignment payload
fn empty_consumer_assignment() []u8 {
	mut writer := new_writer()
	// Consumer protocol version
	writer.write_i16(0)
	// assignment array length (0 topics)
	writer.write_i32(0)
	// user data length (0)
	writer.write_i32(0)
	return writer.bytes()
}

// Helper function to parse config as i64
fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
	val := configs[key] or { return default_val }
	return val.i64()
}

// Helper function to parse config as int
fn parse_config_int(configs map[string]string, key string, default_val int) int {
	val := configs[key] or { return default_val }
	return val.int()
}

// generate_uuid generates a random UUID v4 (16 bytes)
fn generate_uuid() []u8 {
	mut uuid := []u8{len: 16}
	for i in 0 .. 16 {
		uuid[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	// Set version (4) and variant (RFC 4122)
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant RFC 4122
	return uuid
}
