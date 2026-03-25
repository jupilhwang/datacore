module replication

import sync
import net
import time

/// PooledConnection wraps a TCP connection with pool management metadata.
pub struct PooledConnection {
pub mut:
	conn         net.TcpConn
	addr         string
	created_at   i64
	last_used_at i64
	in_use       bool
}

/// ConnectionPool manages reusable TCP connections grouped by host address.
/// All operations are thread-safe via sync.Mutex.
@[heap]
pub struct ConnectionPool {
mut:
	connections map[string][]&PooledConnection
	mtx         sync.Mutex
	closed      bool
pub:
	max_per_host     int
	max_idle_time_ms i64
}

/// ConnectionPool.new creates a pool with the given per-host limit and idle timeout.
fn ConnectionPool.new(max_per_host int, max_idle_time_ms i64) &ConnectionPool {
	return &ConnectionPool{
		max_per_host:     max_per_host
		max_idle_time_ms: max_idle_time_ms
		closed:           false
	}
}

/// acquire returns an idle connection for the address, or creates a new one.
/// Returns error if the pool is closed or the per-host limit has been reached.
fn (mut p ConnectionPool) acquire(addr string) !&PooledConnection {
	p.mtx.@lock()
	if p.closed {
		p.mtx.unlock()
		return error('connection pool is closed')
	}

	if addr in p.connections {
		mut conns := unsafe { p.connections[addr] }
		for mut pc in conns {
			if !pc.in_use {
				pc.in_use = true
				pc.last_used_at = time.now().unix_milli()
				p.mtx.unlock()
				return pc
			}
		}
		if conns.len >= p.max_per_host {
			p.mtx.unlock()
			return error('max connections (${p.max_per_host}) reached for ${addr}')
		}
	}

	// Release lock before dialing TCP (may block).
	p.mtx.unlock()

	mut conn := net.dial_tcp(addr) or { return error('failed to connect to ${addr}: ${err}') }

	now := time.now().unix_milli()
	mut pc := &PooledConnection{
		conn:         conn
		addr:         addr
		created_at:   now
		last_used_at: now
		in_use:       true
	}

	p.mtx.@lock()
	if p.closed {
		p.mtx.unlock()
		conn.close() or {}
		return error('connection pool is closed')
	}
	// Re-check max_per_host after re-acquiring lock (TOCTOU guard).
	// Another thread may have added connections while we were dialing.
	conns2 := p.connections[addr] or { []&PooledConnection{} }
	if conns2.len >= p.max_per_host {
		p.mtx.unlock()
		conn.close() or {}
		return error('connection limit exceeded for ${addr} (concurrent dial)')
	}
	if addr !in p.connections {
		p.connections[addr] = []&PooledConnection{}
	}
	unsafe {
		p.connections[addr] << pc
	}
	p.mtx.unlock()

	return pc
}

/// remove_connection removes a specific connection from the pool.
/// Used to evict broken connections that should not be reused.
fn (mut p ConnectionPool) remove_connection(pc &PooledConnection) {
	p.mtx.@lock()
	if pc.addr in p.connections {
		mut kept := []&PooledConnection{}
		conns := unsafe { p.connections[pc.addr] }
		for c in conns {
			if c != pc {
				kept << c
			}
		}
		p.connections[pc.addr] = kept
	}
	p.mtx.unlock()
}

/// release marks a pooled connection as idle for reuse.
fn (mut p ConnectionPool) release(mut pc PooledConnection) {
	p.mtx.@lock()
	pc.in_use = false
	pc.last_used_at = time.now().unix_milli()
	p.mtx.unlock()
}

/// close_all shuts down every connection and marks the pool as closed.
/// Collects connections under lock, then closes them outside the lock
/// to avoid blocking on TCP close while holding the mutex.
fn (mut p ConnectionPool) close_all() {
	mut to_close := []&PooledConnection{}

	p.mtx.@lock()
	addrs := p.connections.keys()
	for addr in addrs {
		conns := unsafe { p.connections[addr] }
		for pc in conns {
			to_close << pc
		}
	}
	p.connections = map[string][]&PooledConnection{}
	p.closed = true
	p.mtx.unlock()

	// Close connections outside the lock to avoid blocking on TCP close
	for mut pc in to_close {
		pc.conn.close() or {}
	}
}

/// cleanup_idle removes idle connections that exceeded max_idle_time_ms.
/// Returns the number of connections removed.
fn (mut p ConnectionPool) cleanup_idle() int {
	mut to_close := []&PooledConnection{}

	p.mtx.@lock()
	now := time.now().unix_milli()
	addrs := p.connections.keys()

	for addr in addrs {
		mut kept := []&PooledConnection{}
		mut conns := unsafe { p.connections[addr] }
		for mut pc in conns {
			if !pc.in_use && (now - pc.last_used_at) > p.max_idle_time_ms {
				to_close << pc
			} else {
				kept << pc
			}
		}
		p.connections[addr] = kept
	}
	p.mtx.unlock()

	// Close connections outside the lock to avoid blocking on TCP close
	for mut pc in to_close {
		pc.conn.close() or {}
	}

	return to_close.len
}

/// pool_size returns the total number of connections across all hosts.
fn (mut p ConnectionPool) pool_size() int {
	p.mtx.@lock()
	mut total := 0
	addrs := p.connections.keys()
	for addr in addrs {
		conns := unsafe { p.connections[addr] }
		total += conns.len
	}
	p.mtx.unlock()
	return total
}
