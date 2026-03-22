module replication

import time
import domain

// --- ConnectionPool tests ---

// Helper: start a simple echo server for pool tests
fn start_pool_test_server(port int) !&Server {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'pool-test-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(port, handler)
	server.start()!
	time.sleep(time.Duration(100 * time.millisecond))
	return server
}

// test_connection_pool_creation: pool creation with correct config values
fn test_connection_pool_creation() {
	pool := ConnectionPool.new(5, 30000)
	assert pool.max_per_host == 5
	assert pool.max_idle_time_ms == 30000
}

// test_pool_initial_size_zero: new pool starts with zero connections
fn test_pool_initial_size_zero() {
	mut pool := ConnectionPool.new(5, 30000)
	assert pool.pool_size() == 0
}

// test_pool_acquire_creates_new_connection: acquire on empty pool creates TCP connection
fn test_pool_acquire_creates_new_connection() {
	mut server := start_pool_test_server(19300) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)

	mut pc := pool.acquire('127.0.0.1:19300') or {
		server.stop() or {}
		assert false, 'acquire failed: ${err}'
		return
	}

	assert pc.in_use == true
	assert pc.addr == '127.0.0.1:19300'
	assert pool.pool_size() == 1

	pool.close_all()
	server.stop() or {}
}

// test_pool_release_returns_connection: release marks connection as idle
fn test_pool_release_returns_connection() {
	mut server := start_pool_test_server(19301) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)

	mut pc := pool.acquire('127.0.0.1:19301') or {
		server.stop() or {}
		assert false, 'acquire failed: ${err}'
		return
	}

	assert pc.in_use == true
	pool.release(mut pc)
	assert pc.in_use == false

	pool.close_all()
	server.stop() or {}
}

// test_pool_acquire_reuses_released_connection: second acquire reuses released connection
fn test_pool_acquire_reuses_released_connection() {
	mut server := start_pool_test_server(19302) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)

	mut pc1 := pool.acquire('127.0.0.1:19302') or {
		server.stop() or {}
		assert false, 'first acquire failed: ${err}'
		return
	}
	first_created_at := pc1.created_at

	pool.release(mut pc1)

	mut pc2 := pool.acquire('127.0.0.1:19302') or {
		server.stop() or {}
		assert false, 'second acquire failed: ${err}'
		return
	}

	// Same connection reused (same created_at timestamp)
	assert pc2.created_at == first_created_at
	assert pool.pool_size() == 1

	pool.close_all()
	server.stop() or {}
}

// test_pool_max_per_host_enforcement: cannot exceed max connections per host
fn test_pool_max_per_host_enforcement() {
	mut server := start_pool_test_server(19303) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(2, 30000)

	pool.acquire('127.0.0.1:19303') or {
		pool.close_all()
		server.stop() or {}
		assert false, 'first acquire failed: ${err}'
		return
	}

	pool.acquire('127.0.0.1:19303') or {
		pool.close_all()
		server.stop() or {}
		assert false, 'second acquire failed: ${err}'
		return
	}

	// Third acquire should fail (both in use, at limit)
	pool.acquire('127.0.0.1:19303') or {
		assert err.msg().contains('max connections')
		pool.close_all()
		server.stop() or {}
		return
	}

	pool.close_all()
	server.stop() or {}
	assert false, 'third acquire should have failed with max connections error'
}

// test_pool_cleanup_removes_idle: cleanup removes connections idle beyond threshold
fn test_pool_cleanup_removes_idle() {
	mut server := start_pool_test_server(19304) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 50) // 50ms idle timeout

	mut pc := pool.acquire('127.0.0.1:19304') or {
		server.stop() or {}
		assert false, 'acquire failed: ${err}'
		return
	}

	pool.release(mut pc)
	assert pool.pool_size() == 1

	// Wait for idle timeout
	time.sleep(time.Duration(100 * time.millisecond))

	removed := pool.cleanup_idle()
	assert removed == 1
	assert pool.pool_size() == 0

	pool.close_all()
	server.stop() or {}
}

// test_pool_close_all_empties_pool: close_all removes all connections
fn test_pool_close_all_empties_pool() {
	mut server := start_pool_test_server(19305) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)

	pool.acquire('127.0.0.1:19305') or {
		server.stop() or {}
		assert false, 'acquire failed: ${err}'
		return
	}

	assert pool.pool_size() == 1
	pool.close_all()
	assert pool.pool_size() == 0

	server.stop() or {}
}

// test_pool_size_tracking: pool_size tracks connections correctly
fn test_pool_size_tracking() {
	mut server := start_pool_test_server(19306) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)

	assert pool.pool_size() == 0

	mut pc1 := pool.acquire('127.0.0.1:19306') or {
		server.stop() or {}
		assert false, 'first acquire failed: ${err}'
		return
	}
	assert pool.pool_size() == 1

	mut pc2 := pool.acquire('127.0.0.1:19306') or {
		pool.close_all()
		server.stop() or {}
		assert false, 'second acquire failed: ${err}'
		return
	}
	assert pool.pool_size() == 2

	pool.release(mut pc1)
	pool.release(mut pc2)
	// Size stays 2 (connections returned to pool, not removed)
	assert pool.pool_size() == 2

	pool.close_all()
	assert pool.pool_size() == 0

	server.stop() or {}
}

// test_pool_multiple_hosts_independent: different hosts tracked separately
fn test_pool_multiple_hosts_independent() {
	mut server1 := start_pool_test_server(19307) or {
		assert false, 'server1 start failed: ${err}'
		return
	}
	mut server2 := start_pool_test_server(19308) or {
		server1.stop() or {}
		assert false, 'server2 start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(2, 30000)

	pool.acquire('127.0.0.1:19307') or {
		pool.close_all()
		server1.stop() or {}
		server2.stop() or {}
		assert false, 'acquire host1 failed: ${err}'
		return
	}

	pool.acquire('127.0.0.1:19308') or {
		pool.close_all()
		server1.stop() or {}
		server2.stop() or {}
		assert false, 'acquire host2 failed: ${err}'
		return
	}

	// Each host has 1 connection, total = 2
	assert pool.pool_size() == 2

	// Can still acquire more on each host (max_per_host = 2)
	pool.acquire('127.0.0.1:19307') or {
		pool.close_all()
		server1.stop() or {}
		server2.stop() or {}
		assert false, 'second acquire host1 failed: ${err}'
		return
	}

	assert pool.pool_size() == 3

	pool.close_all()
	server1.stop() or {}
	server2.stop() or {}
}

// test_pool_acquire_after_close_all_fails: acquire on closed pool returns error
fn test_pool_acquire_after_close_all_fails() {
	mut pool := ConnectionPool.new(5, 30000)
	pool.close_all()

	pool.acquire('127.0.0.1:19309') or {
		assert err.msg().contains('closed')
		return
	}

	assert false, 'acquire on closed pool should fail'
}

// test_pool_cleanup_preserves_in_use: cleanup does not remove in-use connections
fn test_pool_cleanup_preserves_in_use() {
	mut server := start_pool_test_server(19311) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 50) // 50ms idle timeout

	pool.acquire('127.0.0.1:19311') or {
		server.stop() or {}
		assert false, 'acquire failed: ${err}'
		return
	}

	// Connection is in_use, wait beyond idle timeout
	time.sleep(time.Duration(100 * time.millisecond))

	removed := pool.cleanup_idle()
	assert removed == 0
	assert pool.pool_size() == 1

	pool.close_all()
	server.stop() or {}
}

// test_client_with_pool_send: client with pool sends and receives correctly
fn test_client_with_pool_send() {
	mut server := start_pool_test_server(19310) or {
		assert false, 'server start failed: ${err}'
		return
	}

	mut pool := ConnectionPool.new(5, 30000)
	mut client := Client.new_with_pool(3000, pool)

	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'pool-client-001'
		sender_id:      'pool-test-client'
		timestamp:      time.now().unix_milli()
		topic:          'pool-topic'
		partition:      0
		offset:         100
	}

	response := client.send('127.0.0.1:19310', msg) or {
		pool.close_all()
		server.stop() or {}
		assert false, 'send failed: ${err}'
		return
	}

	assert response.msg_type == .replicate_ack
	assert response.correlation_id == 'pool-client-001'
	assert response.success == true
	assert pool.pool_size() == 1

	pool.close_all()
	server.stop() or {}
}
