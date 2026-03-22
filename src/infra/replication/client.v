module replication

import net
import domain
import time
import log

// Client sends replication messages to remote brokers
/// Client sends replication messages to remote brokers.
pub struct Client {
mut:
	binary_protocol BinaryProtocol
	timeout_ms      int
	logger          log.Logger
	pool            &ConnectionPool = unsafe { nil }
}

// Client.new creates a new replication Client with the given timeout in milliseconds.
/// Client.
pub fn Client.new(timeout_ms int) &Client {
	return &Client{
		binary_protocol: BinaryProtocol.new()
		timeout_ms:      timeout_ms
		logger:          log.Log{}
	}
}

// Client.new_with_pool creates a Client that reuses connections from the given pool.
/// Client.new_with_pool creates a Client that reuses TCP connections from the given pool.
pub fn Client.new_with_pool(timeout_ms int, pool &ConnectionPool) &Client {
	return &Client{
		binary_protocol: BinaryProtocol.new()
		timeout_ms:      timeout_ms
		logger:          log.Log{}
		pool:            pool
	}
}

// send sends a replication message to a remote broker and waits for response.
// Uses connection pooling when a pool is configured, otherwise creates a new connection.
//
// Parameters:
// - broker_address: target broker address in "host:port" format (e.g., "localhost:9093")
// - msg: ReplicationMessage to send
//
// Returns: response ReplicationMessage from the remote broker
//
// Errors:
// - Connection failure if the broker is unreachable
// - Timeout if no response within Client.timeout_ms
// - Protocol error if message serialization/deserialization fails
/// send sends a replication message to a remote broker and waits for response.
pub fn (mut c Client) send(broker_address string, msg domain.ReplicationMessage) !domain.ReplicationMessage {
	if !isnil(c.pool) {
		return c.send_pooled(broker_address, msg)
	}
	return c.send_direct(broker_address, msg)
}

// send_direct creates a new TCP connection for a single request-response cycle.
fn (mut c Client) send_direct(broker_address string, msg domain.ReplicationMessage) !domain.ReplicationMessage {
	mut conn := net.dial_tcp(broker_address) or {
		return error('failed to connect to ${broker_address}: ${err}')
	}

	defer {
		conn.close() or { c.logger.error('Failed to close connection: ${err}') }
	}

	conn.set_write_timeout(time.Duration(c.timeout_ms * time.millisecond))

	c.binary_protocol.write_message(mut conn, msg) or {
		return error('failed to write message: ${err}')
	}

	c.logger.debug('Sent ${msg.msg_type} to ${broker_address}')

	response := c.binary_protocol.read_message(mut conn, i64(c.timeout_ms)) or {
		return error('failed to read response: ${err}')
	}

	c.logger.debug('Received ${response.msg_type} from ${broker_address}')

	return response
}

// send_pooled acquires a pooled connection for the request-response cycle.
// On success the connection is released back to the pool for reuse.
// On failure the underlying TCP socket is closed (not returned to pool).
fn (mut c Client) send_pooled(broker_address string, msg domain.ReplicationMessage) !domain.ReplicationMessage {
	mut pool := c.pool
	mut pc := pool.acquire(broker_address)!

	pc.conn.set_write_timeout(time.Duration(c.timeout_ms * time.millisecond))

	c.binary_protocol.write_message(mut pc.conn, msg) or {
		pc.conn.close() or {}
		pool.remove_connection(pc)
		return error('failed to write message: ${err}')
	}

	c.logger.debug('Sent ${msg.msg_type} to ${broker_address}')

	response := c.binary_protocol.read_message(mut pc.conn, i64(c.timeout_ms)) or {
		pc.conn.close() or {}
		pool.remove_connection(pc)
		return error('failed to read response: ${err}')
	}

	c.logger.debug('Received ${response.msg_type} from ${broker_address}')
	pool.release(mut pc)

	return response
}

// send_async sends a replication message without waiting for response (fire-and-forget).
// Spawns a background coroutine via send_fire_forget.
// Errors from the underlying send are logged but not propagated.
//
// Parameters:
// - broker_address: target broker address in "host:port" format
// - msg: ReplicationMessage to send
/// send_async sends a replication message without waiting for response.
pub fn (mut c Client) send_async(broker_address string, msg domain.ReplicationMessage) {
	spawn c.send_fire_forget(broker_address, msg)
}

// send_fire_forget is the internal implementation for async send.
// Calls send() and logs errors without propagating them.
//
// Parameters:
// - broker_address: target broker address in "host:port" format
// - msg: ReplicationMessage to send
fn (mut c Client) send_fire_forget(broker_address string, msg domain.ReplicationMessage) {
	_ := c.send(broker_address, msg) or {
		c.logger.error('Async send to ${broker_address} failed: ${err}')
		return
	}
}

// send_with_retry sends a replication message with exponential backoff retry logic.
// Retries on any send failure with delays of 100ms, 200ms, 400ms, etc.
//
// Parameters:
// - broker_address: target broker address in "host:port" format
// - msg: ReplicationMessage to send
// - max_retries: maximum number of send attempts (e.g., 3)
//
// Returns: response ReplicationMessage from the remote broker
//
// Errors:
// - All retry attempts exhausted (includes last error message)
/// send_with_retry sends a replication message with exponential backoff retry logic.
pub fn (mut c Client) send_with_retry(broker_address string, msg domain.ReplicationMessage, max_retries int) !domain.ReplicationMessage {
	mut last_error := ''

	for attempt := 0; attempt < max_retries; attempt++ {
		response := c.send(broker_address, msg) or {
			last_error = err.msg()
			c.logger.warn('Attempt ${attempt + 1}/${max_retries} failed: ${err}')

			// Exponential backoff
			if attempt < max_retries - 1 {
				sleep_ms := 100 * (1 << attempt)
				time.sleep(time.Duration(sleep_ms * time.millisecond))
			}
			continue
		}

		// Success
		return response
	}

	return error('all ${max_retries} attempts failed. last error: ${last_error}')
}

// close releases any resources held by the Client.
// The pool lifecycle is managed externally; close does not shut down a shared pool.
/// close releases any resources held by the Client.
pub fn (mut c Client) close() {
	// Pool lifecycle is managed by the caller, not the client.
}
