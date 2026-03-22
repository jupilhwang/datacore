module replication

import net
import domain
import sync
import sync.stdatomic
import time
import log

// Server handles incoming replication connections
/// Server handles incoming replication connections.
pub struct Server {
mut:
	port            int
	listener        net.TcpListener
	binary_protocol BinaryProtocol
	handler         MessageHandler = unsafe { nil }
	running_flag    i64
	read_timeout_ms i64
	mtx             sync.Mutex
	logger          log.Logger
}

// MessageHandler is a callback interface for handling messages
/// MessageHandler type.
pub type MessageHandler = fn (domain.ReplicationMessage) !domain.ReplicationMessage

// Server.new creates a new replication Server on the given port with the specified message handler.
/// Server.
pub fn Server.new(port int, handler MessageHandler) &Server {
	return &Server{
		port:            port
		binary_protocol: BinaryProtocol.new()
		handler:         handler
		running_flag:    0
		read_timeout_ms: 30000
		logger:          log.Log{}
	}
}

// start begins listening on the configured port
/// start begins listening on the configured port.
pub fn (mut s Server) start() ! {
	s.mtx.@lock()
	if stdatomic.load_i64(&s.running_flag) == 1 {
		s.mtx.unlock()
		return error('server already running')
	}
	stdatomic.store_i64(&s.running_flag, 1)
	s.mtx.unlock()

	// Create TCP listener
	s.listener = net.listen_tcp(.ip, ':${s.port}')!
	s.logger.info('Replication server listening on port ${s.port}')

	// Accept connections in a loop
	spawn s.accept_loop()
}

// stop shuts down the server
/// stop shuts down the server.
pub fn (mut s Server) stop() ! {
	s.mtx.@lock()
	if stdatomic.load_i64(&s.running_flag) != 1 {
		s.mtx.unlock()
		return
	}
	stdatomic.store_i64(&s.running_flag, 0)
	s.mtx.unlock()

	s.listener.close()!
	s.logger.info('Replication server stopped')
}

// accept_loop accepts incoming connections
fn (mut s Server) accept_loop() {
	for {
		if stdatomic.load_i64(&s.running_flag) != 1 {
			break
		}

		// Accept connection (with timeout)
		mut conn := s.listener.accept() or {
			if stdatomic.load_i64(&s.running_flag) == 1 {
				s.logger.error('Failed to accept connection: ${err}')
			}
			continue
		}

		// Handle connection in separate thread
		spawn s.handle_connection(mut conn)
	}
}

// handle_connection processes a single client connection
fn (mut s Server) handle_connection(mut conn net.TcpConn) {
	defer {
		conn.close() or { s.logger.error('Failed to close connection: ${err}') }
	}

	remote_addr := (conn.peer_addr() or { net.Addr{} }).str()
	s.logger.debug('Accepted connection from ${remote_addr}')

	for {
		// Read message using binary protocol
		msg := s.binary_protocol.read_message(mut conn, s.read_timeout_ms) or {
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

			s.binary_protocol.write_message(mut conn, error_response) or {
				s.logger.error('Failed to send error response: ${err}')
			}
			continue
		}

		// Send response
		s.binary_protocol.write_message(mut conn, response) or {
			s.logger.error('Failed to send response: ${err}')
			break
		}
	}
}

// is_running checks if server is running
/// is_running returns whether the server is running.
pub fn (s Server) is_running() bool {
	return stdatomic.load_i64(&s.running_flag) == 1
}
