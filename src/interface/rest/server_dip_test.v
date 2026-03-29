// DIP verification: RestServer depends on abstractions (SSEHandlerPort, WebSocketHandlerPort),
// not on concrete infra types (proto_http.SSEHandler, proto_http.WebSocketHandler).
// This test proves that mock implementations can replace concrete handlers.
module rest

import domain
import infra.protocol.http as proto_http
import net
import service.port

// -- DIP Test Mock: StoragePort --

struct DipMockStorage {}

fn (m DipMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{
		name:            name
		partition_count: partitions
	}
}

fn (m DipMockStorage) delete_topic(name string) ! {}

fn (m DipMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m DipMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m DipMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m DipMockStorage) add_partitions(name string, new_count int) ! {}

fn (m DipMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (m DipMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m DipMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m DipMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m DipMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m DipMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m DipMockStorage) delete_group(group_id string) ! {}

fn (m DipMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m DipMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m DipMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m DipMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &DipMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.StorageCapability{}
}

fn (m &DipMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m DipMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m DipMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m DipMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m DipMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []domain.SharePartitionState{}
}

// -- DIP Test Mock: SSEHandlerPort --

struct MockSSEHandler {
mut:
	stats_called bool
}

fn (mut m MockSSEHandler) handle_sse_request(request proto_http.SSERequest) !(int, map[string]string, bool) {
	return 200, map[string]string{}, true
}

fn (mut m MockSSEHandler) start_streaming(conn_id string, mut writer proto_http.SSEResponseWriter) {}

fn (mut m MockSSEHandler) get_stats() port.StreamingStats {
	m.stats_called = true
	return port.StreamingStats{}
}

fn (mut m MockSSEHandler) unregister_connection(conn_id string) ! {}

// -- DIP Test Mock: WebSocketHandlerPort --

struct MockWSHandler {
mut:
	stats_called bool
}

fn (mut m MockWSHandler) handle_upgrade(mut conn net.TcpConn, headers map[string]string, client_ip string) !string {
	return 'mock-conn-id'
}

fn (mut m MockWSHandler) start_connection(conn_id string, mut conn net.TcpConn) {}

fn (mut m MockWSHandler) get_stats() port.WebSocketServiceStats {
	m.stats_called = true
	return port.WebSocketServiceStats{}
}

// -- DIP Verification Tests --

fn test_rest_server_accepts_mock_handlers() {
	// DIP proof: RestServer depends on SSEHandlerPort and WebSocketHandlerPort,
	// not concrete proto_http.SSEHandler / proto_http.WebSocketHandler.
	config := default_rest_config()
	mock_storage := DipMockStorage{}
	mock_sse := &MockSSEHandler{}
	mock_ws := &MockWSHandler{}

	server := new_rest_server(config, mock_storage, mock_sse, mock_ws)

	assert !server.running
	assert !server.ready
}

fn test_rest_server_no_metrics_dependency() {
	// ISP proof: RestServer no longer requires DataCoreMetrics field.
	// Previously required infra.observability.DataCoreMetrics; now removed.
	config := default_rest_config()
	mock_storage := DipMockStorage{}
	mock_sse := &MockSSEHandler{}
	mock_ws := &MockWSHandler{}

	server := new_rest_server(config, mock_storage, mock_sse, mock_ws)

	// Server is constructed without any metrics dependency
	assert !server.running
}
