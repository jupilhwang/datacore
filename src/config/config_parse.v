// Configuration parsing: parses individual config sections from TOML documents.
module config

import toml

fn parse_broker_config(cli_args map[string]string, doc toml.Doc) BrokerConfig {
	src := ConfigSource{
		cli_args: cli_args
		doc:      doc
	}
	broker_host := src.get_string('broker-host', 'DATACORE_BROKER_HOST', 'broker.host',
		'0.0.0.0')
	// broker_id: if not set via config/env, generate deterministically from server identity (0 used as sentinel)
	mut broker_id := src.get_int('broker-id', 'DATACORE_BROKER_ID', 'broker.broker_id',
		0)
	if broker_id == 0 {
		broker_id = generate_deterministic_broker_id()
	}
	return BrokerConfig{
		host:               broker_host
		port:               src.get_int('broker-port', 'DATACORE_BROKER_PORT', 'broker.port',
			9092)
		broker_id:          broker_id
		cluster_id:         src.get_string('cluster-id', 'DATACORE_CLUSTER_ID', 'broker.cluster_id',
			'datacore-cluster')
		max_connections:    src.get_int('max-connections', 'DATACORE_MAX_CONNECTIONS',
			'broker.max_connections', 10000)
		max_request_size:   src.get_int('max-request-size', 'DATACORE_MAX_REQUEST_SIZE',
			'broker.max_request_size', 104857600)
		request_timeout_ms: src.get_int('request-timeout-ms', 'DATACORE_REQUEST_TIMEOUT_MS',
			'broker.request_timeout_ms', 30000)
		idle_timeout_ms:    src.get_int('idle-timeout-ms', 'DATACORE_IDLE_TIMEOUT_MS',
			'broker.idle_timeout_ms', 600000)
		advertised_host:    src.get_string('advertised-host', 'DATACORE_ADVERTISED_HOST',
			'broker.advertised_host', broker_host)
		rate_limit:         parse_rate_limit_config(doc)
	}
}

/// parse_rate_limit_config parses the [broker.rate_limit] TOML section.
fn parse_rate_limit_config(doc toml.Doc) RateLimitConfig {
	return RateLimitConfig{
		enabled:                     get_bool(doc, 'broker.rate_limit.enabled', false)
		max_requests_per_sec:        get_int(doc, 'broker.rate_limit.max_requests_per_sec',
			default_rate_limit_max_requests_per_sec)
		max_bytes_per_sec:           get_i64(doc, 'broker.rate_limit.max_bytes_per_sec',
			default_rate_limit_max_bytes_per_sec)
		per_ip_max_requests_per_sec: get_int(doc, 'broker.rate_limit.per_ip_max_requests_per_sec',
			default_rate_limit_per_ip_max_requests_per_sec)
		per_ip_max_connections:      get_int(doc, 'broker.rate_limit.per_ip_max_connections',
			default_rate_limit_per_ip_max_connections)
		burst_multiplier:            get_f64(doc, 'broker.rate_limit.burst_multiplier',
			default_rate_limit_burst_multiplier)
		window_ms:                   get_int(doc, 'broker.rate_limit.window_ms', default_rate_limit_window_ms)
	}
}

fn parse_rest_config(cli_args map[string]string, doc toml.Doc) RestConfig {
	src := ConfigSource{
		cli_args: cli_args
		doc:      doc
	}
	return RestConfig{
		enabled:                   src.get_bool('rest-enabled', 'DATACORE_REST_ENABLED',
			'rest.enabled', true)
		host:                      src.get_string('rest-host', 'DATACORE_REST_HOST', 'rest.host',
			'0.0.0.0')
		port:                      src.get_int('rest-port', 'DATACORE_REST_PORT', 'rest.port',
			8080)
		max_connections:           src.get_int('rest-max-connections', 'DATACORE_REST_MAX_CONNECTIONS',
			'rest.max_connections', 1000)
		static_dir:                src.get_string('rest-static-dir', 'DATACORE_REST_STATIC_DIR',
			'rest.static_dir', 'tests/web')
		sse_heartbeat_interval_ms: get_int(doc, 'rest.sse_heartbeat_interval_ms', 15000)
		sse_connection_timeout_ms: get_int(doc, 'rest.sse_connection_timeout_ms', 3600000)
		ws_max_message_size:       get_int(doc, 'rest.ws_max_message_size', 1048576)
		ws_ping_interval_ms:       get_int(doc, 'rest.ws_ping_interval_ms', 30000)
	}
}

fn parse_grpc_config(cli_args map[string]string, doc toml.Doc) GrpcGatewayConfig {
	src := ConfigSource{
		cli_args: cli_args
		doc:      doc
	}
	return GrpcGatewayConfig{
		enabled:          src.get_bool('grpc-enabled', 'DATACORE_GRPC_ENABLED', 'grpc.enabled',
			false)
		host:             src.get_string('grpc-host', 'DATACORE_GRPC_HOST', 'grpc.host',
			'0.0.0.0')
		port:             src.get_int('grpc-port', 'DATACORE_GRPC_PORT', 'grpc.port',
			9094)
		max_connections:  src.get_int('grpc-max-connections', 'DATACORE_GRPC_MAX_CONNECTIONS',
			'grpc.max_connections', 10000)
		max_message_size: get_int(doc, 'grpc.max_message_size', 4194304)
	}
}

fn parse_s3_config(cli_args map[string]string, doc toml.Doc) !S3StorageConfig {
	src := ConfigSource{
		cli_args: cli_args
		doc:      doc
	}
	mut s3 := S3StorageConfig{
		endpoint:                     src.get_string('s3-endpoint', 'DATACORE_S3_ENDPOINT',
			'storage.s3.endpoint', '')
		bucket:                       src.get_string('s3-bucket', 'DATACORE_S3_BUCKET',
			'storage.s3.bucket', '')
		region:                       src.get_string('s3-region', 'DATACORE_S3_REGION',
			'storage.s3.region', 'us-east-1')
		prefix:                       src.get_string('s3-prefix', 'DATACORE_S3_PREFIX',
			'storage.s3.prefix', 'datacore/')
		timezone:                     src.get_string('s3-timezone', 'DATACORE_S3_TIMEZONE',
			'storage.s3.timezone', 'UTC')
		batch_timeout_ms:             get_int(doc, 'storage.s3.batch_timeout_ms', 25)
		batch_max_bytes:              get_i64(doc, 'storage.s3.batch_max_bytes', 4096000)
		min_flush_bytes:              get_int(doc, 'storage.s3.min_flush_bytes', 65536)
		max_flush_skip_count:         get_int(doc, 'storage.s3.max_flush_skip_count',
			80)
		compaction_interval_ms:       get_int(doc, 'storage.s3.compaction_interval_ms',
			60000)
		target_segment_bytes:         get_i64(doc, 'storage.s3.target_segment_bytes',
			104857600)
		index_cache_ttl_ms:           get_int(doc, 'storage.s3.index_cache_ttl_ms', 60000)
		offset_batch_enabled:         get_bool(doc, 'storage.s3.offset_batch_enabled',
			true)
		offset_flush_interval_ms:     get_int(doc, 'storage.s3.offset_flush_interval_ms',
			100)
		offset_flush_threshold_count: get_int(doc, 'storage.s3.offset_flush_threshold_count',
			50)
		index_batch_size:             get_int(doc, 'storage.s3.index_batch_size', 5)
		index_flush_interval_ms:      get_int(doc, 'storage.s3.index_flush_interval_ms',
			500)
		sync_linger_ms:               get_int(doc, 'storage.s3.sync_linger_ms', 0)
		use_server_side_copy:         get_bool(doc, 'storage.s3.use_server_side_copy',
			true)
		access_key:                   ''
		secret_key:                   ''
	}

	// validate user-provided endpoint for SSRF before applying default
	if s3.endpoint != '' {
		validate_s3_endpoint(s3.endpoint) or { return error('invalid S3 endpoint: ${err}') }
	}

	s3.endpoint = if s3.endpoint == '' {
		'https://${s3.bucket}.s3.${s3.region}.amazonaws.com'
	} else {
		s3.endpoint
	}

	resolve_s3_credentials(mut s3, cli_args, doc)
	parse_iceberg_sub_config(mut s3, doc)

	return s3
}

/// parse_iceberg_sub_config parses [storage.s3.iceberg] section into S3StorageConfig fields.
fn parse_iceberg_sub_config(mut s3 S3StorageConfig, doc toml.Doc) {
	iceberg_enabled := get_bool(doc, 'storage.s3.iceberg.enabled', false)
	if !iceberg_enabled {
		return
	}

	mut partition_by := []string{}
	if partition_by_val := doc.value_opt('storage.s3.iceberg.partition_by') {
		partition_by_array := partition_by_val.array()
		for item in partition_by_array {
			partition_by << item.string()
		}
	} else {
		partition_by = ['timestamp', 'topic']
	}

	s3.iceberg_enabled = iceberg_enabled
	s3.iceberg_format = get_string(doc, 'storage.s3.iceberg.format', 'parquet')
	s3.iceberg_compression = get_string(doc, 'storage.s3.iceberg.compression', 'zstd')
	s3.iceberg_write_mode = get_string(doc, 'storage.s3.iceberg.write_mode', 'append')
	s3.iceberg_partition_by = partition_by
	s3.iceberg_max_rows_per_file = get_int(doc, 'storage.s3.iceberg.max_rows_per_file',
		1000000)
	s3.iceberg_max_file_size_mb = get_int(doc, 'storage.s3.iceberg.max_file_size_mb',
		128)
	s3.iceberg_schema_evolution = get_bool(doc, 'storage.s3.iceberg.schema_evolution',
		true)
	s3.iceberg_format_version = get_int(doc, 'storage.s3.iceberg.format_version', 2)
}

fn parse_storage_config(cli_args map[string]string, doc toml.Doc) !StorageConfig {
	src := ConfigSource{
		cli_args: cli_args
		doc:      doc
	}
	return StorageConfig{
		engine:   src.get_string('storage-engine', 'DATACORE_STORAGE_ENGINE', 'storage.engine',
			'memory')
		memory:   MemoryStorageConfig{
			max_memory_mb:      get_int(doc, 'storage.memory.max_memory_mb', default_max_memory_mb)
			segment_size_bytes: get_int(doc, 'storage.memory.segment_size_bytes', 1073741824)
		}
		s3:       parse_s3_config(cli_args, doc)!
		sqlite:   SqliteStorageConfig{
			path:         get_string(doc, 'storage.sqlite.path', 'datacore.db')
			journal_mode: get_string(doc, 'storage.sqlite.journal_mode', 'WAL')
		}
		postgres: PostgresStorageConfig{
			host:      src.get_string('postgres-host', 'DATACORE_POSTGRES_HOST', 'storage.postgres.host',
				'localhost')
			port:      src.get_int('postgres-port', 'DATACORE_POSTGRES_PORT', 'storage.postgres.port',
				5432)
			database:  src.get_string('postgres-database', 'DATACORE_POSTGRES_DATABASE',
				'storage.postgres.database', 'datacore')
			user:      src.get_string('postgres-user', 'DATACORE_POSTGRES_USER', 'storage.postgres.user',
				'')
			password:  src.get_string('postgres-password', 'DATACORE_POSTGRES_PASSWORD',
				'storage.postgres.password', '')
			pool_size: src.get_int('postgres-pool-size', 'DATACORE_POSTGRES_POOL_SIZE',
				'storage.postgres.pool_size', 10)
			sslmode:   src.get_string('postgres-sslmode', 'DATACORE_POSTGRES_SSLMODE',
				'storage.postgres.sslmode', 'disable')
		}
	}
}

fn parse_schema_registry_config(doc toml.Doc) SchemaRegistryConfig {
	return SchemaRegistryConfig{
		enabled: get_bool(doc, 'schema_registry.enabled', true)
		topic:   get_string(doc, 'schema_registry.topic', '__schemas')
	}
}

fn parse_observability_config(doc toml.Doc) ObservabilityConfig {
	return ObservabilityConfig{
		otel:    OtelConfig{
			enabled:             get_bool(doc, 'observability.otel.enabled', true)
			service_name:        get_string(doc, 'observability.otel.service_name', 'datacore')
			service_version:     get_string(doc, 'observability.otel.service_version',
				'0.44.4')
			instance_id:         get_string(doc, 'observability.otel.instance_id', '')
			environment:         get_string(doc, 'observability.otel.environment', 'development')
			otlp_endpoint:       get_string(doc, 'observability.otel.otlp_endpoint', 'http://localhost:4317')
			otlp_http_endpoint:  get_string(doc, 'observability.otel.otlp_http_endpoint',
				'')
			resource_attributes: get_string(doc, 'observability.otel.resource_attributes',
				'')
		}
		metrics: parse_metrics_config(doc)
		logging: LoggingConfig{
			enabled:              get_bool(doc, 'observability.logging.enabled', true)
			level:                get_string(doc, 'observability.logging.level', 'debug')
			format:               get_string(doc, 'observability.logging.format', 'json')
			output:               get_string(doc, 'observability.logging.output', 'stdout')
			otlp_endpoint:        get_string(doc, 'observability.logging.otlp_endpoint',
				'')
			otlp_export:          get_bool(doc, 'observability.logging.otlp_export', false)
			console_output:       get_bool(doc, 'observability.logging.console_output',
				true)
			inject_trace_context: get_bool(doc, 'observability.logging.inject_trace_context',
				true)
		}
		tracing: parse_tracing_config(doc)
	}
}

/// parse_metrics_config parses [observability.metrics] section.
fn parse_metrics_config(doc toml.Doc) MetricsConfig {
	return MetricsConfig{
		enabled:             get_bool(doc, 'observability.metrics.enabled', true)
		exporter:            get_string(doc, 'observability.metrics.exporter', 'prometheus')
		prometheus_endpoint: get_string(doc, 'observability.metrics.prometheus_endpoint',
			'/metrics')
		prometheus_port:     get_int(doc, 'observability.metrics.prometheus_port', 9093)
		otlp_endpoint:       get_string(doc, 'observability.metrics.otlp_endpoint', '')
		collection_interval: get_int(doc, 'observability.metrics.collection_interval',
			15)
	}
}

/// parse_tracing_config parses [observability.tracing] section.
fn parse_tracing_config(doc toml.Doc) TracingConfig {
	return TracingConfig{
		enabled:                 get_bool(doc, 'observability.tracing.enabled', false)
		otlp_endpoint:           get_string(doc, 'observability.tracing.otlp_endpoint',
			'')
		sampler:                 get_string(doc, 'observability.tracing.sampler', 'trace_id_ratio')
		sample_rate:             get_f64(doc, 'observability.tracing.sample_rate', 1.0)
		batch_timeout_ms:        get_int(doc, 'observability.tracing.batch_timeout_ms',
			5000)
		max_batch_size:          get_int(doc, 'observability.tracing.max_batch_size',
			512)
		max_queue_size:          get_int(doc, 'observability.tracing.max_queue_size',
			2048)
		max_attributes_per_span: get_int(doc, 'observability.tracing.max_attributes_per_span',
			128)
		max_events_per_span:     get_int(doc, 'observability.tracing.max_events_per_span',
			128)
		max_links_per_span:      get_int(doc, 'observability.tracing.max_links_per_span',
			128)
	}
}
