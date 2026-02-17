module replication

import net
import domain
import sync
import time
import log

/// Server handles incoming replication connections
pub struct Server {
mut:
	port     int
	listener net.TcpListener
	protocol Protocol
	handler  MessageHandler = unsafe { nil }
	running  bool
	mtx      sync.Mutex
	logger   log.Logger
}

/// MessageHandler is a callback interface for handling messages
pub type MessageHandler = fn (domain.ReplicationMessage) !domain.ReplicationMessage

pub fn Server.new(port int, handler MessageHandler) Server {
	return Server{
		port:     port
		protocol: Protocol.new()
		handler:  handler
		running:  false
		logger:   log.Log{}
	}
}

/// start begins listening on the configured port
pub fn (mut s Server) start() ! {
	s.mtx.@lock()
	if s.running {
		s.mtx.unlock()
		return error('server already running')
	}
	s.running = true
	s.mtx.unlock()

	// Create TCP listener
	s.listener = net.listen_tcp(.ip, ':${s.port}')!
	s.logger.info('Replication server listening on port ${s.port}')

	// Accept connections in a loop
	spawn s.accept_loop()
}

/// stop shuts down the server
pub fn (mut s Server) stop() ! {
	s.mtx.@lock()
	if !s.running {
		s.mtx.unlock()
		return
	}
	s.running = false
	s.mtx.unlock()

	s.listener.close()!
	s.logger.info('Replication server stopped')
}

/// accept_loop accepts incoming connections
fn (mut s Server) accept_loop() {
	for {
		s.mtx.@lock()
		if !s.running {
			s.mtx.unlock()
			break
		}
		s.mtx.unlock()

		// Accept connection (with timeout)
		mut conn := s.listener.accept() or {
			if s.running {
				s.logger.error('Failed to accept connection: ${err}')
			}
			continue
		}

		// Handle connection in separate thread
		spawn s.handle_connection(mut conn)
	}
}

/// handle_connection processes a single client connection
fn (mut s Server) handle_connection(mut conn net.TcpConn) {
	defer {
		conn.close() or { s.logger.error('Failed to close connection: ${err}') }
	}

	remote_addr := (conn.peer_addr() or { net.Addr{} }).str()
	s.logger.debug('Accepted connection from ${remote_addr}')

	for {
		// Read message
		msg := s.protocol.read_message(mut conn) or {
			if err.msg().contains('EOF') || err.msg().contains('closed') {
				s.logger.debug('Connection closed by ${remote_addr}')
			} else {
				s.logger.error('Failed to read message from ${remote_addr}: ${err}')
			}
			break
		}

		s.logger.debug('Received ${msg.msg_type} from ${msg.sender_id}')

		// Process message through handler
		response := s.handler(msg) or {
			s.logger.error('Handler failed for ${msg.msg_type}: ${err}')

			// Send error response
			error_response := domain.ReplicationMessage{
				msg_type:       domain.ReplicationType.replicate_ack
				correlation_id: msg.correlation_id
				sender_id:      'unknown'
				timestamp:      time.now().unix_milli()
				topic:          msg.topic
				partition:      msg.partition
				offset:         msg.offset
				success:        false
				error_msg:      err.msg()
			}

			mut error_response_mut := error_response
			s.protocol.write_message(mut conn, mut error_response_mut) or {
				s.logger.error('Failed to send error response: ${err}')
			}
			continue
		}

		// Send response
		mut response_mut := response
		s.protocol.write_message(mut conn, mut response_mut) or {
			s.logger.error('Failed to send response: ${err}')
			break
		}
	}
}

/// is_running checks if server is running
pub fn (s Server) is_running() bool {
	return s.running
}
