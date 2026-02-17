module replication

import net
import domain
import time
import log

/// Client sends replication messages to remote brokers
pub struct Client {
mut:
	protocol   Protocol
	timeout_ms int
	logger     log.Logger
}

pub fn Client.new(timeout_ms int) Client {
	return Client{
		protocol:   Protocol.new()
		timeout_ms: timeout_ms
		logger:     log.Log{}
	}
}

/// send sends a message to a remote broker and waits for response
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
	mut msg_mut := msg
	c.protocol.write_message(mut conn, mut msg_mut) or {
		return error('failed to write message: ${err}')
	}

	c.logger.debug('Sent ${msg.msg_type} to ${broker_address}')

	// Wait for response
	response := c.protocol.read_message(mut conn) or {
		return error('failed to read response: ${err}')
	}

	c.logger.debug('Received ${response.msg_type} from ${broker_address}')

	return response
}

/// send_async sends a message without waiting for response (fire-and-forget)
pub fn (mut c Client) send_async(broker_address string, msg domain.ReplicationMessage) ! {
	spawn c.send_fire_forget(broker_address, msg)
}

/// send_fire_forget is the internal implementation for async send
fn (mut c Client) send_fire_forget(broker_address string, msg domain.ReplicationMessage) {
	_ := c.send(broker_address, msg) or {
		c.logger.error('Async send to ${broker_address} failed: ${err}')
		return
	}
}

/// send_with_retry sends a message with retry logic
pub fn (mut c Client) send_with_retry(broker_address string, msg domain.ReplicationMessage, max_retries int) !domain.ReplicationMessage {
	mut last_error := ''

	for attempt := 0; attempt < max_retries; attempt++ {
		response := c.send(broker_address, msg) or {
			last_error = err.msg()
			c.logger.warn('Attempt ${attempt + 1}/${max_retries} failed: ${err}')

			// Exponential backoff
			if attempt < max_retries - 1 {
				sleep_ms := 100 * (1 << attempt) // 100ms, 200ms, 400ms, ...
				time.sleep(time.Duration(sleep_ms * time.millisecond))
			}
			continue
		}

		// Success
		return response
	}

	return error('all ${max_retries} attempts failed. last error: ${last_error}')
}

/// close releases resources (no-op for now, but kept for interface compatibility)
pub fn (mut c Client) close() ! {
	// No persistent connections to close
}
