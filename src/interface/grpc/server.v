// Interface Layer - gRPC Server
//
// TCP server that accepts gRPC connections and delegates to the
// gRPC protocol handler. Runs on a separate port from the Kafka TCP server.
//
// Key features:
// - Accepts gRPC connections (custom binary framing over TCP)
// - Delegates to infra/protocol/grpc/handler.v for request processing
// - Configurable via GrpcConfig (port, max connections, etc.)
// - Supports bidirectional streaming for Produce/Consume/Subscribe
module grpc

import domain
import infra.observability
import infra.protocol.grpc as proto_grpc
import net
import service.port
import service.streaming
import time

const grpc_read_timeout = 30 * time.second
const grpc_write_timeout = 30 * time.second

// GrpcServer configuration

/// GrpcServerConfig holds gRPC server configuration.
pub struct GrpcServerConfig {
pub:
	host       string = '0.0.0.0'
	port       int    = 9093
	broker_id  int    = 1
	cluster_id string = 'datacore-cluster'
}

// GrpcServer provides a gRPC-compatible TCP server.
// Binds on a dedicated port (default: 9093) and handles
// gRPC binary-framed connections.

/// GrpcServer listens for gRPC connections and dispatches them to the handler.
pub struct GrpcServer {
	config GrpcServerConfig
mut:
	handler &proto_grpc.GrpcHandler
	running bool
}

/// new_grpc_server creates a new gRPC server.
pub fn new_grpc_server(config GrpcServerConfig, storage port.StoragePort, grpc_config domain.GrpcConfig) &GrpcServer {
	grpc_service := streaming.new_grpc_service(storage, grpc_config)
	handler := proto_grpc.new_grpc_handler(grpc_service, storage, grpc_config)
	return &GrpcServer{
		config:  config
		handler: handler
		running: false
	}
}

/// start starts the gRPC server (blocking).
pub fn (mut s GrpcServer) start() ! {
	mut listener := net.listen_tcp(.ip, '${s.config.host}:${s.config.port}')!
	s.running = true

	observability.log_with_context('grpc_server', .info, 'Start', 'gRPC server started',
		{
		'host': s.config.host
		'port': s.config.port.str()
	})

	defer {
		listener.close() or {}
		s.running = false
		observability.log_with_context('grpc_server', .info, 'Stop', 'gRPC server stopped',
			{})
	}

	for s.running {
		mut conn := listener.accept() or {
			if !s.running {
				break
			}
			observability.log_with_context('grpc_server', .warn, 'Accept', 'Failed to accept connection',
				{
				'error': err.msg()
			})
			continue
		}

		client_ip := conn.peer_ip() or { '0.0.0.0' }

		observability.log_with_context('grpc_server', .debug, 'Accept', 'New gRPC connection',
			{
			'client_ip': client_ip
		})

		spawn s.handle_connection(mut conn, client_ip)
	}
}

/// start_background starts the gRPC server in a background goroutine.
pub fn (mut s GrpcServer) start_background() {
	spawn fn [mut s] () {
		s.start() or {
			observability.log_with_context('grpc_server', .error, 'Start', 'gRPC server failed to start',
				{
				'error': err.msg()
			})
		}
	}()
}

/// stop stops the gRPC server.
pub fn (mut s GrpcServer) stop() {
	s.running = false
}

/// handle_connection handles a single gRPC connection.
fn (mut s GrpcServer) handle_connection(mut conn net.TcpConn, client_ip string) {
	// Set read/write deadline to avoid hanging on slow clients
	conn.set_read_timeout(time.Duration(grpc_read_timeout))
	conn.set_write_timeout(time.Duration(grpc_write_timeout))

	s.handler.handle_connection(mut conn, client_ip)
}

/// get_stats returns gRPC server statistics.
pub fn (mut s GrpcServer) get_stats() proto_grpc.GrpcHandler {
	return *s.handler
}
