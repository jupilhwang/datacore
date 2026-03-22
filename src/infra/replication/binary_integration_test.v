module replication

import domain
import net
import time
import log

// --- Binary protocol integration tests ---
// Verify that manager/client/server use BinaryProtocol for wire communication.

// test_binary_client_server_roundtrip: client sends binary-encoded message, server
// decodes it with BinaryProtocol, processes through handler, returns binary response.
fn test_binary_client_server_roundtrip() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'binary-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			records_data:   msg.records_data
			success:        true
		}
	}
	mut server := Server.new(19220, handler)

	server.start() or {
		assert false, 'server start failed: ${err}'
		return
	}
	time.sleep(time.Duration(100 * time.millisecond))

	mut client := Client.new(3000)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'bin-int-001'
		sender_id:      'binary-client'
		timestamp:      time.now().unix_milli()
		topic:          'binary-topic'
		partition:      2
		offset:         777
		records_data:   'binary-payload-data'.bytes()
		success:        true
	}

	response := client.send('127.0.0.1:19220', msg) or {
		server.stop() or {}
		assert false, 'client send failed: ${err}'
		return
	}

	assert response.msg_type == .replicate_ack
	assert response.correlation_id == 'bin-int-001'
	assert response.sender_id == 'binary-server'
	assert response.topic == 'binary-topic'
	assert response.partition == 2
	assert response.offset == 777
	assert response.records_data == 'binary-payload-data'.bytes()
	assert response.success == true

	server.stop() or {}
}

// test_binary_raw_wire_format_verification: connect via raw TCP, send a binary-encoded
// message, and verify the server responds with binary-encoded data (not JSON).
fn test_binary_raw_wire_format_verification() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'wire-server'
			timestamp:      msg.timestamp
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(19221, handler)

	server.start() or {
		assert false, 'server start failed: ${err}'
		return
	}
	time.sleep(time.Duration(100 * time.millisecond))

	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'wire-001'
		sender_id:      'wire-client'
		timestamp:      time.now().unix_milli()
		success:        true
	}

	// Encode with BinaryProtocol and send raw bytes via TCP
	wire_data := bp.encode(msg)

	mut conn := net.dial_tcp('127.0.0.1:19221') or {
		server.stop() or {}
		assert false, 'TCP connect failed: ${err}'
		return
	}
	conn.set_read_timeout(time.Duration(3000 * time.millisecond))
	conn.set_write_timeout(time.Duration(3000 * time.millisecond))

	conn.write(wire_data) or {
		conn.close() or {}
		server.stop() or {}
		assert false, 'TCP write failed: ${err}'
		return
	}

	// Read response and decode with BinaryProtocol
	response := bp.read_message(mut conn, 3000) or {
		conn.close() or {}
		server.stop() or {}
		assert false, 'binary read_message failed: ${err}'
		return
	}

	assert response.msg_type == .replicate_ack
	assert response.correlation_id == 'wire-001'
	assert response.sender_id == 'wire-server'
	assert response.success == true

	conn.close() or {}
	server.stop() or {}
}

// test_binary_manager_handler_processes_replicate: the manager's create_handler callback
// correctly processes a REPLICATE message arriving via binary protocol, stores the
// replica buffer, and returns a binary-encoded ACK through the server.
fn test_binary_manager_handler_processes_replicate() {
	config := domain.ReplicationConfig{
		enabled:                       true
		replication_port:              19222
		replica_count:                 2
		replica_timeout_ms:            3000
		heartbeat_interval_ms:         5000
		reassignment_interval_ms:      5000
		orphan_cleanup_interval_ms:    5000
		retry_count:                   1
		replica_buffer_ttl_ms:         60000
		max_replica_buffer_size_bytes: 0
	}
	mut m := Manager.new('mgr-binary-test', config, []domain.BrokerRef{})

	// Start only the server (not the full manager to avoid background workers)
	m.server.start() or {
		assert false, 'manager server start failed: ${err}'
		return
	}
	time.sleep(time.Duration(100 * time.millisecond))

	mut client := Client.new(3000)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'mgr-bin-001'
		sender_id:      'remote-broker'
		timestamp:      time.now().unix_milli()
		topic:          'mgr-binary-topic'
		partition:      1
		offset:         500
		records_data:   'manager-binary-data'.bytes()
		success:        true
	}

	response := client.send('127.0.0.1:19222', msg) or {
		m.server.stop() or {}
		assert false, 'client send failed: ${err}'
		return
	}

	// Verify ACK response
	assert response.msg_type == .replicate_ack
	assert response.correlation_id == 'mgr-bin-001'
	assert response.sender_id == 'mgr-binary-test'
	assert response.topic == 'mgr-binary-topic'
	assert response.partition == 1
	assert response.offset == 500
	assert response.success == true

	// Verify the replica buffer was stored by the manager handler
	all_buffers := m.get_all_replica_buffers() or {
		m.server.stop() or {}
		assert false, 'get_all_replica_buffers failed: ${err}'
		return
	}
	assert all_buffers.len == 1
	assert all_buffers[0].topic == 'mgr-binary-topic'
	assert all_buffers[0].partition == 1
	assert all_buffers[0].offset == 500
	assert all_buffers[0].records_data == 'manager-binary-data'.bytes()

	m.server.stop() or {}
}

// test_binary_pooled_client_server_roundtrip: pooled client sends binary-encoded
// messages via connection pool, verifying binary protocol works with connection reuse.
fn test_binary_pooled_client_server_roundtrip() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'pool-binary-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(19223, handler)

	server.start() or {
		assert false, 'server start failed: ${err}'
		return
	}
	time.sleep(time.Duration(100 * time.millisecond))

	mut pool := ConnectionPool.new(5, 30000)
	mut client := Client.new_with_pool(3000, pool)

	// Send multiple messages to verify binary protocol with connection pooling
	for i in 0 .. 3 {
		msg := domain.ReplicationMessage{
			msg_type:       .replicate
			correlation_id: 'pool-bin-${i}'
			sender_id:      'pool-binary-client'
			timestamp:      time.now().unix_milli()
			topic:          'pool-binary-topic'
			partition:      i32(i)
			offset:         i64(i * 100)
			records_data:   'pool-data-${i}'.bytes()
			success:        true
		}

		response := client.send('127.0.0.1:19223', msg) or {
			pool.close_all()
			server.stop() or {}
			assert false, 'pooled send ${i} failed: ${err}'
			return
		}

		assert response.msg_type == .replicate_ack
		assert response.correlation_id == 'pool-bin-${i}'
		assert response.sender_id == 'pool-binary-server'
		assert response.partition == i32(i)
		assert response.offset == i64(i * 100)
		assert response.success == true
	}

	// Verify connection was reused (pool should have exactly 1 connection)
	assert pool.pool_size() == 1

	pool.close_all()
	server.stop() or {}
}
