// Unit tests for the gateway adapter
module gateway

import domain
import infra.storage.plugins.memory
import service.port

fn new_test_adapter() GatewayAdapter {
	storage := port.StoragePort(memory.new_memory_adapter())
	return new_gateway_adapter(storage)
}

fn setup_topic(mut adapter GatewayAdapter, name string) {
	adapter.storage.create_topic(name, 1, domain.TopicConfig{}) or {}
}

fn test_new_gateway_adapter() {
	adapter := new_test_adapter()
	// adapter is valid when no panic occurs
	assert true
}

fn test_protocol_type_str() {
	assert ProtocolType.grpc.str() == 'grpc'
	assert ProtocolType.sse.str() == 'sse'
	assert ProtocolType.websocket.str() == 'websocket'
}

fn test_gateway_action_str() {
	assert GatewayAction.produce.str() == 'produce'
	assert GatewayAction.consume.str() == 'consume'
	assert GatewayAction.commit.str() == 'commit'
}

fn test_handle_produce_topic_not_found() {
	mut adapter := new_test_adapter()

	req := GatewayRequest{
		protocol: .grpc
		action:   .produce
		topic:    'nonexistent'
		records:  [
			GatewayRecord{
				key:   'key1'.bytes()
				value: 'value1'.bytes()
			},
		]
	}

	resp := adapter.handle_request(req)
	assert resp.error_code != 0
	assert resp.error_msg.contains('not found') || resp.error_msg.contains('nonexistent')
}

fn test_handle_produce_success() {
	mut adapter := new_test_adapter()
	setup_topic(mut adapter, 'test-topic')

	req := GatewayRequest{
		protocol: .grpc
		action:   .produce
		topic:    'test-topic'
		records:  [
			GatewayRecord{
				key:   'k1'.bytes()
				value: 'v1'.bytes()
			},
		]
	}

	resp := adapter.handle_request(req)
	assert resp.error_code == 0
	assert resp.topic == 'test-topic'
	assert resp.base_offset >= 0
}

fn test_handle_consume_success() {
	mut adapter := new_test_adapter()
	setup_topic(mut adapter, 'consume-topic')

	// produce a record first
	produce_req := GatewayRequest{
		protocol: .grpc
		action:   .produce
		topic:    'consume-topic'
		records:  [
			GatewayRecord{
				value: 'hello'.bytes()
			},
		]
	}
	adapter.handle_request(produce_req)

	// consume it
	consume_req := GatewayRequest{
		protocol:  .grpc
		action:    .consume
		topic:     'consume-topic'
		offset:    i64(0)
		max_bytes: 65536
	}

	resp := adapter.handle_request(consume_req)
	assert resp.error_code == 0
	assert resp.records.len == 1
	assert resp.records[0].value == 'hello'.bytes()
}

fn test_handle_consume_topic_not_found() {
	mut adapter := new_test_adapter()

	req := GatewayRequest{
		protocol: .grpc
		action:   .consume
		topic:    'missing-topic'
		offset:   i64(0)
	}

	resp := adapter.handle_request(req)
	assert resp.error_code != 0
}

fn test_handle_commit_missing_group_id() {
	mut adapter := new_test_adapter()
	setup_topic(mut adapter, 'commit-topic')

	req := GatewayRequest{
		protocol: .grpc
		action:   .commit
		topic:    'commit-topic'
		// group_id intentionally omitted
	}

	resp := adapter.handle_request(req)
	assert resp.error_code != 0
	assert resp.error_msg.contains('group_id')
}

fn test_handle_commit_success() {
	mut adapter := new_test_adapter()
	setup_topic(mut adapter, 'commit-topic')

	req := GatewayRequest{
		protocol:  .grpc
		action:    .commit
		topic:     'commit-topic'
		group_id:  'test-group'
		partition: i32(0)
		offset:    i64(10)
	}

	resp := adapter.handle_request(req)
	assert resp.error_code == 0
	assert resp.topic == 'commit-topic'
}

fn test_gateway_record_conversion() {
	record := GatewayRecord{
		key:       'mykey'.bytes()
		value:     'myvalue'.bytes()
		timestamp: 1700000000000
	}

	assert record.key == 'mykey'.bytes()
	assert record.value == 'myvalue'.bytes()
	assert record.timestamp == 1700000000000
}
