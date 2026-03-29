// Broker startup helper functions
// Extracted from start_broker() for readability and maintainability.
module main

import config as cfg
import domain
import interface.rest
import interface.server
import infra.auth
import infra.compression
import infra.observability
import infra.protocol.http as proto_http
import infra.protocol.kafka
import service.port
import service.schema
import service.streaming
import startup

// init_broker_logging initializes the global logger and writer pool,
// then logs the broker configuration summary.
fn init_broker_logging(conf cfg.Config) &observability.Logger {
	log_level := observability.log_level_from_string(conf.observability.logging.level)
	log_format := if conf.observability.logging.format == 'json' {
		observability.OutputFormat.json
	} else {
		observability.OutputFormat.text
	}
	log_output := observability.log_output_from_string(conf.observability.logging.output)

	observability.init_global_logger(observability.LoggerConfig{
		name:          'datacore'
		level:         log_level
		format:        log_format
		output:        log_output
		service:       'datacore-broker'
		otlp_endpoint: conf.observability.logging.otlp_endpoint
	})

	mut logger := observability.get_logger()
	startup.init_writer_pool(startup.default_writer_pool_config())

	logger.info('Broker configuration summary', port.field_string('host', conf.broker.host),
		port.field_int('port', conf.broker.port), port.field_int('broker_id', conf.broker.broker_id),
		port.field_string('cluster_id', conf.broker.cluster_id), port.field_int('max_conn',
		conf.broker.max_connections), port.field_int('max_req_size', conf.broker.max_request_size))

	logger.info('Observability summary', port.field_bool('metrics_enabled', conf.observability.metrics.enabled),
		port.field_int('metrics_port', conf.observability.metrics.prometheus_port), port.field_bool('tracing_enabled',
		conf.observability.tracing.enabled), port.field_string('log_level', conf.observability.logging.level))

	return logger
}

// init_protocol_services creates the compression service, protocol handler,
// and attaches the audit logger.
fn init_protocol_services(conf cfg.Config, storage port.StoragePort, mut logger observability.Logger) kafka.Handler {
	observability.log_with_context('startup', .info, 'Init', 'Initializing compression service',
		{})
	compression_service := compression.new_default_compression_service() or {
		observability.log_with_context('startup', .error, 'Init', 'Failed to initialize compression service: ${err}',
			{})
		exit(1)
	}
	comp_port := kafka.new_compression_port_adapter(compression_service)

	observability.log_with_context('startup', .info, 'Init', 'Initializing Kafka protocol handler',
		{})
	mut protocol_handler := startup.init_protocol_handler(conf, storage, comp_port)

	audit_logger := auth.new_audit_logger(true)
	protocol_handler.set_audit_logger(audit_logger)
	logger.info('Audit logger initialized', port.field_int('max_buffer_size', audit_logger.max_buffer_size))

	// auth_manager is not configured in the current startup path;
	// warn so operators are aware that all requests bypass authentication.
	observability.log_with_context('startup', .warn, 'Auth', 'Broker running without authentication -- all requests will be accepted without auth checks',
		{})

	return protocol_handler
}

// start_rest_api_server initializes and starts the REST API server with
// optional schema registry support.
fn start_rest_api_server(conf cfg.Config, storage port.StoragePort, mut logger observability.Logger) {
	observability.log_with_context('startup', .info, 'Init', 'Starting REST API server (SSE/WebSocket)',
		{})
	rest_config := rest.RestServerConfig{
		host:            conf.rest.host
		port:            conf.rest.port
		max_connections: conf.rest.max_connections
		static_dir:      conf.rest.static_dir
		sse_config:      domain.SSEConfig{
			heartbeat_interval_ms: conf.rest.sse_heartbeat_interval_ms
			connection_timeout_ms: conf.rest.sse_connection_timeout_ms
		}
		ws_config:       domain.WebSocketConfig{
			max_message_size: conf.rest.ws_max_message_size
			ping_interval_ms: conf.rest.ws_ping_interval_ms
		}
	}
	mut rest_server := rest.new_rest_server(rest_config, storage, create_sse_handler(storage,
		rest_config.sse_config), create_ws_handler(storage, rest_config.ws_config))

	if conf.schema_registry.enabled {
		observability.log_with_context('startup', .info, 'Init', 'Initializing schema registry',
			{})
		schema_config := schema.RegistryConfig{
			default_compatibility: .backward
			auto_register:         true
		}
		mut schema_registry := schema.new_registry(storage, schema_config)
		schema_registry.load_from_storage() or {
			logger.warn('Failed to load schemas from storage', port.field_string('error',
				'${err}'))
		}
		logger.info('Schema registry initialized', port.field_string('topic', conf.schema_registry.topic))
		schema_api := rest.new_schema_api(schema_registry)
		rest_server.set_schema_api(schema_api)
		logger.info('Schema Registry API registered with REST server')
	}

	rest_server.start_background()
	logger.info('REST API server started', port.field_string('host', conf.rest.host),
		port.field_int('port', conf.rest.port))
}

// create_tcp_server builds the TCP server with optional rate limiter.
fn create_tcp_server(conf cfg.Config, protocol_handler kafka.Handler, mut logger observability.Logger) &server.Server {
	server_config := server.ServerConfig{
		host:       conf.broker.host
		port:       conf.broker.port
		broker_id:  conf.broker.broker_id
		cluster_id: conf.broker.cluster_id
	}

	mut tcp_server := server.new_server(server_config, protocol_handler)

	if conf.broker.rate_limit.enabled {
		rl_cfg := conf.broker.rate_limit
		rate_limiter := server.new_rate_limiter(server.RateLimiterConfig{
			max_requests_per_second:        rl_cfg.max_requests_per_sec
			max_bytes_per_second:           rl_cfg.max_bytes_per_sec
			per_ip_max_requests_per_second: rl_cfg.per_ip_max_requests_per_sec
			per_ip_max_connections:         rl_cfg.per_ip_max_connections
			burst_multiplier:               rl_cfg.burst_multiplier
			window_size_ms:                 i64(rl_cfg.window_ms)
		})
		tcp_server.set_rate_limiter(rate_limiter)
		logger.info('Rate limiter enabled', port.field_int('max_rps', rl_cfg.max_requests_per_sec),
			port.field_int('per_ip_max_rps', rl_cfg.per_ip_max_requests_per_sec))
	}

	return tcp_server
}

// create_sse_handler builds the SSE protocol handler at the composition root.
// Concrete types are assembled here and injected into RestServer via SSEHandlerPort.
// vfmt off
// (vfmt strips domain. prefix from params, but the compiler requires it)
fn create_sse_handler(storage port.StoragePort, sse_config domain.SSEConfig) &proto_http.SSEHandler {
	// vfmt on
	sse_service := streaming.new_sse_service(storage, sse_config)
	return proto_http.new_sse_handler(sse_service, storage, sse_config)
}

// create_ws_handler builds the WebSocket protocol handler at the composition root.
// Concrete types are assembled here and injected into RestServer via WebSocketHandlerPort.
// vfmt off
// (vfmt strips domain. prefix from params, but the compiler requires it)
fn create_ws_handler(storage port.StoragePort, ws_config domain.WebSocketConfig) &proto_http.WebSocketHandler {
	// vfmt on
	ws_service := streaming.new_websocket_service(storage, ws_config)
	return proto_http.new_websocket_handler(ws_service, storage, ws_config)
}
