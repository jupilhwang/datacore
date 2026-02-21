module replication

import net
import domain
import time
import log

// Client sends replication messages to remote brokers
/// Client sends replication messages to remote brokers.
pub struct Client {
mut:
	protocol   Protocol
	timeout_ms int
	logger     log.Logger
}

// Client.new creates a new replication Client with the given timeout in milliseconds.
/// Client.
pub fn Client.new(timeout_ms int) &Client {
	return &Client{
		protocol:   Protocol.new()
		timeout_ms: timeout_ms
		logger:     log.Log{}
	}
}

// send sends a replication message to a remote broker and waits for response.
// Establishes a new TCP connection for each call (no connection pooling).
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
	// Connect to broker
	mut conn := net.dial_tcp(broker_address) or {
		return error('failed to connect to ${broker_address}: ${err}')
	}

	defer {
		conn.close() or { c.logger.error('Failed to close connection: ${err}') }
	}

	// Set read/write timeout
	conn.set_read_timeout(time.Duration(c.timeout_ms * time.millisecond))
	conn.set_write_timeout(time.Duration(c.timeout_ms * time.millisecond))

	// Send message
	c.protocol.write_message(mut conn, msg) or { return error('failed to write message: ${err}') }

	c.logger.debug('Sent ${msg.msg_type} to ${broker_address}')

	// Wait for response
	response := c.protocol.read_message(mut conn) or {
		return error('failed to read response: ${err}')
	}

	c.logger.debug('Received ${response.msg_type} from ${broker_address}')

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
// Currently a no-op since each send() creates its own connection,
// but kept for interface compatibility and future connection pooling.
/// close releases any resources held by the Client.
pub fn (mut c Client) close() {
	// No persistent connections to close
}
