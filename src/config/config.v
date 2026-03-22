// Configuration management module
// Loads and manages configuration files in TOML format.
module config

import os
import strings
import toml

/// ConfigSource bundles shared data sources for configuration lookups.
/// Created once per parse function to avoid passing cli_args and doc repeatedly.
struct ConfigSource {
	cli_args map[string]string
	doc      toml.Doc
}

/// load_config loads configuration from a TOML file.
/// path: path to the configuration file
/// returns: loaded Config or error
pub fn load_config(path string) !Config {
	return load_config_with_args(path, map[string]string{})
}

/// load_config_with_args loads configuration including CLI arguments.
/// Priority: CLI args > environment variables > TOML > defaults
/// path: path to the configuration file
/// cli_args: CLI argument map (parsed by parse_cli_args)
/// returns: loaded Config or error
pub fn load_config_with_args(path string, cli_args map[string]string) !Config {
	if !os.exists(path) {
		return load_default_config_with_overrides(cli_args)
	}

	content := os.read_file(path) or { return error('Failed to read config file: ${err}') }
	doc := toml.parse_text(content) or { return error('Failed to parse config file: ${err}') }

	mut cfg := Config{
		broker:          parse_broker_config(cli_args, doc)
		rest:            parse_rest_config(cli_args, doc)
		grpc:            parse_grpc_config(cli_args, doc)
		storage:         parse_storage_config(cli_args, doc)!
		schema_registry: parse_schema_registry_config(doc)
		observability:   parse_observability_config(doc)
		telemetry:       parse_telemetry_config(doc)
	}

	cfg.validate()!
	return cfg
}

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

/// resolve_s3_credentials resolves S3 access/secret keys using 4-tier priority.
/// Priority: CLI args > env vars > ~/.aws/credentials > config.toml
fn resolve_s3_credentials(mut s3 S3StorageConfig, cli_args map[string]string, doc toml.Doc) {
	// priority 1: CLI arguments
	if cli_access_key := cli_args['s3-access-key'] {
		s3.access_key = cli_access_key
	}
	if cli_secret_key := cli_args['s3-secret-key'] {
		s3.secret_key = cli_secret_key
	}

	// priority 2: environment variables
	if s3.access_key == '' {
		s3.access_key = os.getenv('AWS_ACCESS_KEY_ID')
	}
	if s3.secret_key == '' {
		s3.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
	}

	// priority 3: ~/.aws/credentials file
	if s3.access_key == '' || s3.secret_key == '' {
		file_access, file_secret := load_s3_credentials_from_file()
		if s3.access_key == '' {
			s3.access_key = file_access
		}
		if s3.secret_key == '' {
			s3.secret_key = file_secret
		}
	}

	// priority 4: config.toml
	if s3.access_key == '' {
		s3.access_key = get_string(doc, 'storage.s3.access_key', '')
	}
	if s3.secret_key == '' {
		s3.secret_key = get_string(doc, 'storage.s3.secret_key', '')
	}
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

fn parse_telemetry_config(doc toml.Doc) TelemetryRootConfig {
	return TelemetryRootConfig{
		enabled:      get_bool(doc, 'telemetry.enabled', true)
		service_name: get_string(doc, 'telemetry.service_name', 'datacore')
		otlp:         TelemetryOtlpConfig{
			endpoint:      get_string(doc, 'telemetry.otlp.endpoint', 'http://localhost:4317')
			http_endpoint: get_string(doc, 'telemetry.otlp.http_endpoint', '')
			insecure:      get_bool(doc, 'telemetry.otlp.insecure', true)
		}
		metrics:      TelemetryMetricsConfig{
			interval:       get_int(doc, 'telemetry.metrics.interval', 10)
			export_timeout: get_int(doc, 'telemetry.metrics.export_timeout', 30000)
		}
		traces:       TelemetryTracesConfig{
			sample_rate: get_f64(doc, 'telemetry.traces.sample_rate', 1.0)
		}
	}
}

// helper functions

/// get_string retrieves a string value from a TOML document.
fn get_string(doc toml.Doc, key string, default_val string) string {
	val := doc.value_opt(key) or { return default_val }
	return val.string()
}

/// get_int retrieves an integer value from a TOML document.
fn get_int(doc toml.Doc, key string, default_val int) int {
	val := doc.value_opt(key) or { return default_val }
	return val.int()
}

/// get_i64 retrieves a 64-bit integer value from a TOML document.
fn get_i64(doc toml.Doc, key string, default_val i64) i64 {
	val := doc.value_opt(key) or { return default_val }
	return val.i64()
}

/// get_f64 retrieves a floating-point value from a TOML document.
fn get_f64(doc toml.Doc, key string, default_val f64) f64 {
	val := doc.value_opt(key) or { return default_val }
	return val.f64()
}

/// get_bool retrieves a boolean value from a TOML document.
fn get_bool(doc toml.Doc, key string, default_val bool) bool {
	val := doc.value_opt(key) or { return default_val }
	return val.bool()
}

// Priority cascade helper functions (methods on ConfigSource)
// Configuration value priority: CLI args > env vars > TOML > defaults

/// get_string retrieves a string configuration value according to priority.
/// 1. CLI argument (cli_key)
/// 2. environment variable (env_key)
/// 3. TOML file (toml_key)
/// 4. default value (default_val)
fn (s &ConfigSource) get_string(cli_key string, env_key string, toml_key string, default_val string) string {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.string()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_int retrieves an integer configuration value according to priority.
fn (s &ConfigSource) get_int(cli_key string, env_key string, toml_key string, default_val int) int {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val.int()
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val.int()
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.int()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_bool retrieves a boolean configuration value according to priority.
fn (s &ConfigSource) get_bool(cli_key string, env_key string, toml_key string, default_val bool) bool {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val == 'true' || cli_val == '1' || cli_val == 'yes'
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val == 'true' || env_val == '1' || env_val == 'yes'
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.bool()
		}
	}

	// priority 4: default value
	return default_val
}

/// parse_cli_args parses command-line arguments and returns them as a map.
/// format: --key=value or --key value
pub fn parse_cli_args(args []string) map[string]string {
	mut result := map[string]string{}

	for i := 0; i < args.len; i++ {
		arg := args[i]

		// --key=value format
		if arg.starts_with('--') && arg.contains('=') {
			key, val := arg[2..].split_once('=') or { continue }
			result[key] = val
		}
		// --key value format
		else if arg.starts_with('--') && i + 1 < args.len {
			key := arg[2..]
			value := args[i + 1]
			if !value.starts_with('--') {
				result[key] = value
				i++
			}
		}
	}

	return result
}

/// escape_toml_string escapes special characters in a string for TOML output.
/// Prevents TOML injection when config values contain quotes, backslashes, or control characters.
fn escape_toml_string(s string) string {
	if s.len == 0 {
		return s
	}
	mut sb := strings.new_builder(s.len * 2)
	for i := 0; i < s.len; i++ {
		ch := s[i]
		if ch == 0x5C {
			// backslash
			sb.write_string('\\\\')
		} else if ch == 0x22 {
			// double-quote
			sb.write_string('\\"')
		} else if ch == 0x0A {
			// newline
			sb.write_string('\\n')
		} else if ch == 0x0D {
			// carriage return
			sb.write_string('\\r')
		} else if ch == 0x09 {
			// tab
			sb.write_string('\\t')
		} else {
			sb.write_u8(ch)
		}
	}
	return sb.str()
}

/// save saves the configuration to a TOML file.
/// Serializes all config sections using escape_toml_string() for string safety.
pub fn (c Config) save(path string) ! {
	mut b := strings.new_builder(4096)
	b.write_string('# DataCore Configuration\n\n')
	c.save_broker_section(mut b)
	c.save_rest_section(mut b)
	c.save_grpc_section(mut b)
	c.save_storage_section(mut b)
	c.save_s3_section(mut b)
	c.save_postgres_section(mut b)
	c.save_schema_registry_section(mut b)
	c.save_observability_section(mut b)
	c.save_telemetry_section(mut b)
	os.write_file(path, b.str())!
}

/// save_broker_section writes the [broker] and [broker.rate_limit] TOML sections.
fn (c Config) save_broker_section(mut b strings.Builder) {
	b.write_string('[broker]\n')
	b.write_string('host = "${escape_toml_string(c.broker.host)}"\n')
	b.write_string('port = ${c.broker.port}\n')
	b.write_string('broker_id = ${c.broker.broker_id}\n')
	b.write_string('cluster_id = "${escape_toml_string(c.broker.cluster_id)}"\n')
	b.write_string('max_connections = ${c.broker.max_connections}\n')
	b.write_string('max_request_size = ${c.broker.max_request_size}\n')
	b.write_string('request_timeout_ms = ${c.broker.request_timeout_ms}\n')
	b.write_string('idle_timeout_ms = ${c.broker.idle_timeout_ms}\n')
	b.write_string('advertised_host = "${escape_toml_string(c.broker.advertised_host)}"\n')
	b.write_string('\n')
	// [broker.rate_limit] sub-section
	rl := c.broker.rate_limit
	b.write_string('[broker.rate_limit]\n')
	b.write_string('enabled = ${rl.enabled}\n')
	b.write_string('max_requests_per_sec = ${rl.max_requests_per_sec}\n')
	b.write_string('max_bytes_per_sec = ${rl.max_bytes_per_sec}\n')
	b.write_string('per_ip_max_requests_per_sec = ${rl.per_ip_max_requests_per_sec}\n')
	b.write_string('per_ip_max_connections = ${rl.per_ip_max_connections}\n')
	b.write_string('burst_multiplier = ${rl.burst_multiplier}\n')
	b.write_string('window_ms = ${rl.window_ms}\n')
	b.write_string('\n')
}

/// save_rest_section writes the [rest] TOML section.
fn (c Config) save_rest_section(mut b strings.Builder) {
	b.write_string('[rest]\n')
	b.write_string('enabled = ${c.rest.enabled}\n')
	b.write_string('host = "${escape_toml_string(c.rest.host)}"\n')
	b.write_string('port = ${c.rest.port}\n')
	b.write_string('max_connections = ${c.rest.max_connections}\n')
	b.write_string('static_dir = "${escape_toml_string(c.rest.static_dir)}"\n')
	b.write_string('sse_heartbeat_interval_ms = ${c.rest.sse_heartbeat_interval_ms}\n')
	b.write_string('sse_connection_timeout_ms = ${c.rest.sse_connection_timeout_ms}\n')
	b.write_string('ws_max_message_size = ${c.rest.ws_max_message_size}\n')
	b.write_string('ws_ping_interval_ms = ${c.rest.ws_ping_interval_ms}\n')
	b.write_string('\n')
}

/// save_grpc_section writes the [grpc] TOML section.
fn (c Config) save_grpc_section(mut b strings.Builder) {
	b.write_string('[grpc]\n')
	b.write_string('enabled = ${c.grpc.enabled}\n')
	b.write_string('host = "${escape_toml_string(c.grpc.host)}"\n')
	b.write_string('port = ${c.grpc.port}\n')
	b.write_string('max_connections = ${c.grpc.max_connections}\n')
	b.write_string('max_message_size = ${c.grpc.max_message_size}\n')
	b.write_string('\n')
}

/// save_storage_section writes [storage], [storage.memory], and [storage.sqlite] TOML sections.
fn (c Config) save_storage_section(mut b strings.Builder) {
	b.write_string('[storage]\n')
	b.write_string('engine = "${escape_toml_string(c.storage.engine)}"\n')
	b.write_string('\n')
	b.write_string('[storage.memory]\n')
	b.write_string('max_memory_mb = ${c.storage.memory.max_memory_mb}\n')
	b.write_string('segment_size_bytes = ${c.storage.memory.segment_size_bytes}\n')
	b.write_string('\n')
	b.write_string('[storage.sqlite]\n')
	b.write_string('path = "${escape_toml_string(c.storage.sqlite.path)}"\n')
	b.write_string('journal_mode = "${escape_toml_string(c.storage.sqlite.journal_mode)}"\n')
	b.write_string('\n')
}

/// save_s3_section writes [storage.s3] and [storage.s3.iceberg] TOML sections.
fn (c Config) save_s3_section(mut b strings.Builder) {
	s3 := c.storage.s3
	b.write_string('[storage.s3]\n')
	b.write_string('endpoint = "${escape_toml_string(s3.endpoint)}"\n')
	b.write_string('bucket = "${escape_toml_string(s3.bucket)}"\n')
	b.write_string('region = "${escape_toml_string(s3.region)}"\n')
	b.write_string('access_key = "${escape_toml_string(s3.access_key)}"\n') // sensitive
	b.write_string('secret_key = "${escape_toml_string(s3.secret_key)}"\n') // sensitive
	b.write_string('prefix = "${escape_toml_string(s3.prefix)}"\n')
	b.write_string('timezone = "${escape_toml_string(s3.timezone)}"\n')
	b.write_string('batch_timeout_ms = ${s3.batch_timeout_ms}\n')
	b.write_string('batch_max_bytes = ${s3.batch_max_bytes}\n')
	b.write_string('min_flush_bytes = ${s3.min_flush_bytes}\n')
	b.write_string('max_flush_skip_count = ${s3.max_flush_skip_count}\n')
	b.write_string('compaction_interval_ms = ${s3.compaction_interval_ms}\n')
	b.write_string('target_segment_bytes = ${s3.target_segment_bytes}\n')
	b.write_string('index_cache_ttl_ms = ${s3.index_cache_ttl_ms}\n')
	b.write_string('offset_batch_enabled = ${s3.offset_batch_enabled}\n')
	b.write_string('offset_flush_interval_ms = ${s3.offset_flush_interval_ms}\n')
	b.write_string('offset_flush_threshold_count = ${s3.offset_flush_threshold_count}\n')
	b.write_string('index_batch_size = ${s3.index_batch_size}\n')
	b.write_string('index_flush_interval_ms = ${s3.index_flush_interval_ms}\n')
	b.write_string('sync_linger_ms = ${s3.sync_linger_ms}\n')
	b.write_string('use_server_side_copy = ${s3.use_server_side_copy}\n')
	b.write_string('\n')
	// [storage.s3.iceberg] sub-section
	b.write_string('[storage.s3.iceberg]\n')
	b.write_string('enabled = ${s3.iceberg_enabled}\n')
	b.write_string('format = "${escape_toml_string(s3.iceberg_format)}"\n')
	b.write_string('compression = "${escape_toml_string(s3.iceberg_compression)}"\n')
	b.write_string('write_mode = "${escape_toml_string(s3.iceberg_write_mode)}"\n')
	b.write_string('partition_by = [')
	for i, val in s3.iceberg_partition_by {
		if i > 0 {
			b.write_string(', ')
		}
		b.write_string('"${escape_toml_string(val)}"')
	}
	b.write_string(']\n')
	b.write_string('max_rows_per_file = ${s3.iceberg_max_rows_per_file}\n')
	b.write_string('max_file_size_mb = ${s3.iceberg_max_file_size_mb}\n')
	b.write_string('schema_evolution = ${s3.iceberg_schema_evolution}\n')
	b.write_string('format_version = ${s3.iceberg_format_version}\n')
	b.write_string('\n')
}

/// save_postgres_section writes the [storage.postgres] TOML section.
fn (c Config) save_postgres_section(mut b strings.Builder) {
	pg := c.storage.postgres
	b.write_string('[storage.postgres]\n')
	b.write_string('host = "${escape_toml_string(pg.host)}"\n')
	b.write_string('port = ${pg.port}\n')
	b.write_string('database = "${escape_toml_string(pg.database)}"\n')
	b.write_string('user = "${escape_toml_string(pg.user)}"\n')
	b.write_string('password = "${escape_toml_string(pg.password)}"\n') // sensitive
	b.write_string('pool_size = ${pg.pool_size}\n')
	b.write_string('sslmode = "${escape_toml_string(pg.sslmode)}"\n')
	b.write_string('\n')
}

/// save_schema_registry_section writes the [schema_registry] TOML section.
fn (c Config) save_schema_registry_section(mut b strings.Builder) {
	b.write_string('[schema_registry]\n')
	b.write_string('enabled = ${c.schema_registry.enabled}\n')
	b.write_string('topic = "${escape_toml_string(c.schema_registry.topic)}"\n')
	b.write_string('\n')
}

/// save_observability_section writes all [observability.*] TOML sections.
fn (c Config) save_observability_section(mut b strings.Builder) {
	o := c.observability
	// [observability.otel]
	b.write_string('[observability.otel]\n')
	b.write_string('enabled = ${o.otel.enabled}\n')
	b.write_string('service_name = "${escape_toml_string(o.otel.service_name)}"\n')
	b.write_string('service_version = "${escape_toml_string(o.otel.service_version)}"\n')
	b.write_string('instance_id = "${escape_toml_string(o.otel.instance_id)}"\n')
	b.write_string('environment = "${escape_toml_string(o.otel.environment)}"\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.otel.otlp_endpoint)}"\n')
	b.write_string('otlp_http_endpoint = "${escape_toml_string(o.otel.otlp_http_endpoint)}"\n')
	b.write_string('resource_attributes = "${escape_toml_string(o.otel.resource_attributes)}"\n')
	b.write_string('\n')
	// [observability.metrics]
	b.write_string('[observability.metrics]\n')
	b.write_string('enabled = ${o.metrics.enabled}\n')
	b.write_string('exporter = "${escape_toml_string(o.metrics.exporter)}"\n')
	b.write_string('prometheus_endpoint = "${escape_toml_string(o.metrics.prometheus_endpoint)}"\n')
	b.write_string('prometheus_port = ${o.metrics.prometheus_port}\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.metrics.otlp_endpoint)}"\n')
	b.write_string('collection_interval = ${o.metrics.collection_interval}\n')
	b.write_string('\n')
	// [observability.logging]
	b.write_string('[observability.logging]\n')
	b.write_string('enabled = ${o.logging.enabled}\n')
	b.write_string('level = "${escape_toml_string(o.logging.level)}"\n')
	b.write_string('format = "${escape_toml_string(o.logging.format)}"\n')
	b.write_string('output = "${escape_toml_string(o.logging.output)}"\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.logging.otlp_endpoint)}"\n')
	b.write_string('otlp_export = ${o.logging.otlp_export}\n')
	b.write_string('console_output = ${o.logging.console_output}\n')
	b.write_string('inject_trace_context = ${o.logging.inject_trace_context}\n')
	b.write_string('\n')
	// [observability.tracing]
	b.write_string('[observability.tracing]\n')
	b.write_string('enabled = ${o.tracing.enabled}\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.tracing.otlp_endpoint)}"\n')
	b.write_string('sampler = "${escape_toml_string(o.tracing.sampler)}"\n')
	b.write_string('sample_rate = ${o.tracing.sample_rate}\n')
	b.write_string('batch_timeout_ms = ${o.tracing.batch_timeout_ms}\n')
	b.write_string('max_batch_size = ${o.tracing.max_batch_size}\n')
	b.write_string('max_queue_size = ${o.tracing.max_queue_size}\n')
	b.write_string('max_attributes_per_span = ${o.tracing.max_attributes_per_span}\n')
	b.write_string('max_events_per_span = ${o.tracing.max_events_per_span}\n')
	b.write_string('max_links_per_span = ${o.tracing.max_links_per_span}\n')
	b.write_string('\n')
}

/// save_telemetry_section writes all [telemetry.*] TOML sections.
fn (c Config) save_telemetry_section(mut b strings.Builder) {
	t := c.telemetry
	b.write_string('[telemetry]\n')
	b.write_string('enabled = ${t.enabled}\n')
	b.write_string('service_name = "${escape_toml_string(t.service_name)}"\n')
	b.write_string('\n')
	b.write_string('[telemetry.otlp]\n')
	b.write_string('endpoint = "${escape_toml_string(t.otlp.endpoint)}"\n')
	b.write_string('http_endpoint = "${escape_toml_string(t.otlp.http_endpoint)}"\n')
	b.write_string('insecure = ${t.otlp.insecure}\n')
	b.write_string('\n')
	b.write_string('[telemetry.metrics]\n')
	b.write_string('interval = ${t.metrics.interval}\n')
	b.write_string('export_timeout = ${t.metrics.export_timeout}\n')
	b.write_string('\n')
	b.write_string('[telemetry.traces]\n')
	b.write_string('sample_rate = ${t.traces.sample_rate}\n')
}

/// validate checks the validity of the configuration.
pub fn (c Config) validate() ! {
	// validate broker configuration
	if c.broker.port < 1 || c.broker.port > 65535 {
		return error('Invalid broker port: ${c.broker.port}')
	}
	if c.broker.broker_id < 1 {
		return error('Invalid broker_id: ${c.broker.broker_id}')
	}

	// validate rate limit configuration
	c.validate_rate_limit()!

	// validate storage configuration
	match c.storage.engine {
		'memory' {
			if c.storage.memory.max_memory_mb < 1 {
				return error('Invalid max_memory_mb: ${c.storage.memory.max_memory_mb}')
			}
		}
		's3' {
			if c.storage.s3.bucket == '' {
				return error('S3 bucket is required when storage.engine = "s3"')
			}
			if c.storage.s3.region == '' {
				return error('S3 region is required when storage.engine = "s3"')
			}
			if c.storage.s3.access_key == '' {
				return error('S3 access_key is required (set DATACORE_S3_ACCESS_KEY env var)')
			}
			if c.storage.s3.secret_key == '' {
				return error('S3 secret_key is required (set DATACORE_S3_SECRET_KEY env var)')
			}
		}
		'sqlite' {
			if c.storage.sqlite.path == '' {
				return error('SQLite path is required')
			}
		}
		'postgres' {
			if c.storage.postgres.database == '' {
				return error('PostgreSQL database is required')
			}
		}
		else {
			return error('Unknown storage engine: ${c.storage.engine}')
		}
	}
}

/// validate_rate_limit checks the validity of rate limit configuration values.
fn (c Config) validate_rate_limit() ! {
	rl := c.broker.rate_limit
	if rl.max_requests_per_sec < 0 {
		return error('Invalid rate_limit.max_requests_per_sec: ${rl.max_requests_per_sec} (must be >= 0)')
	}
	if rl.max_bytes_per_sec < 0 {
		return error('Invalid rate_limit.max_bytes_per_sec: ${rl.max_bytes_per_sec} (must be >= 0)')
	}
	if rl.per_ip_max_requests_per_sec < 0 {
		return error('Invalid rate_limit.per_ip_max_requests_per_sec: ${rl.per_ip_max_requests_per_sec} (must be >= 0)')
	}
	if rl.per_ip_max_connections < 0 {
		return error('Invalid rate_limit.per_ip_max_connections: ${rl.per_ip_max_connections} (must be >= 0)')
	}
	if rl.burst_multiplier < 0.0 {
		return error('Invalid rate_limit.burst_multiplier: ${rl.burst_multiplier} (must be >= 0.0)')
	}
	if rl.window_ms < 0 {
		return error('Invalid rate_limit.window_ms: ${rl.window_ms} (must be >= 0)')
	}
}

/// get_storage_engine returns the storage engine name.
pub fn (c Config) get_storage_engine() string {
	return c.storage.engine
}

/// is_s3_storage checks whether S3 storage is configured.
pub fn (c Config) is_s3_storage() bool {
	return c.storage.engine == 's3'
}

/// is_metrics_enabled checks whether metrics are enabled.
pub fn (c Config) is_metrics_enabled() bool {
	return c.observability.metrics.enabled
}

/// is_tracing_enabled checks whether tracing is enabled.
pub fn (c Config) is_tracing_enabled() bool {
	return c.observability.tracing.enabled
}

/// load_default_config_with_overrides creates a configuration without a config file, using CLI/env overrides.
/// cli_args: CLI argument map
/// returns: Config with defaults applied and CLI/env overrides, validated
fn load_default_config_with_overrides(cli_args map[string]string) !Config {
	// toml.Doc{} has a nil ast pointer that causes segfault on value_opt calls.
	// toml.parse_text('') produces a properly initialized empty document.
	empty_doc := toml.parse_text('') or {
		return error('Failed to initialize empty config document: ${err}')
	}
	mut cfg := Config{
		broker:          parse_broker_config(cli_args, empty_doc)
		rest:            parse_rest_config(cli_args, empty_doc)
		grpc:            parse_grpc_config(cli_args, empty_doc)
		storage:         parse_storage_config(cli_args, empty_doc)!
		schema_registry: parse_schema_registry_config(empty_doc)
		observability:   parse_observability_config(empty_doc)
		telemetry:       parse_telemetry_config(empty_doc)
	}
	cfg.validate()!
	return cfg
}

/// === Environment variable utility functions (supports both upper and lower case) ===

/// toml_key_to_env_key_upper converts a TOML key to an uppercase environment variable name.
/// example: broker.host -> BROKER_HOST
fn toml_key_to_env_key_upper(toml_key string) string {
	mut env_key := toml_key.replace('.', '_')
	env_key = env_key.to_upper()
	return env_key
}

/// toml_key_to_env_key_lower converts a TOML key to a lowercase environment variable name.
/// example: broker.host -> broker_host
fn toml_key_to_env_key_lower(toml_key string) string {
	mut env_key := toml_key.replace('.', '_')
	env_key = env_key.to_lower()
	return env_key
}

/// EnvMapping represents a section and its configurable keys for environment variable display.
struct EnvMapping {
	section string
	keys    []string
}

/// print_env_mapping prints the mapping between TOML keys and environment variable names.
pub fn print_env_mapping() {
	println('=== TOML Key to Environment Variable Mapping ===')
	println('')
	println('Search order:')
	println('  1. uppercase (e.g. BROKER_HOST)')
	println('  2. lowercase (e.g. broker_host)')
	println('  3. DATACORE_ prefix + uppercase (e.g. DATACORE_BROKER_HOST)')
	println('')

	env_mappings := [
		EnvMapping{'broker', ['broker.host', 'broker.port', 'broker.cluster_id']},
		EnvMapping{'storage', ['storage.engine']},
		EnvMapping{'s3', ['s3.endpoint', 's3.bucket']},
		EnvMapping{'postgres', ['postgres.host', 'postgres.password']},
		EnvMapping{'logging', ['logging.level']},
	]

	mut max_key_len := 0
	for mapping in env_mappings {
		for key in mapping.keys {
			if key.len > max_key_len {
				max_key_len = key.len
			}
		}
	}
	max_key_len += 2

	for mapping in env_mappings {
		println('[${mapping.section}]')
		for key in mapping.keys {
			padded := key + ' '.repeat(max_key_len - key.len)
			upper := toml_key_to_env_key_upper(key)
			lower := toml_key_to_env_key_lower(key)
			println('  ${padded} -> ${upper}, ${lower}, DATACORE_${upper}')
		}
		println('')
	}
}
