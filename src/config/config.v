// Configuration management module
// Loads and manages configuration files in TOML format.
module config

import os
import strings
import toml
import infra.storage.plugins.s3 as s3_plugin

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
	broker_host := get_config_string(cli_args, 'broker-host', 'DATACORE_BROKER_HOST',
		doc, 'broker.host', '0.0.0.0')
	// broker_id: if not set via config/env, generate deterministically from server identity (0 used as sentinel)
	mut broker_id := get_config_int(cli_args, 'broker-id', 'DATACORE_BROKER_ID', doc,
		'broker.broker_id', 0)
	if broker_id == 0 {
		broker_id = generate_deterministic_broker_id()
	}
	return BrokerConfig{
		host:               broker_host
		port:               get_config_int(cli_args, 'broker-port', 'DATACORE_BROKER_PORT',
			doc, 'broker.port', 9092)
		broker_id:          broker_id
		cluster_id:         get_config_string(cli_args, 'cluster-id', 'DATACORE_CLUSTER_ID',
			doc, 'broker.cluster_id', 'datacore-cluster')
		max_connections:    get_config_int(cli_args, 'max-connections', 'DATACORE_MAX_CONNECTIONS',
			doc, 'broker.max_connections', 10000)
		max_request_size:   get_config_int(cli_args, 'max-request-size', 'DATACORE_MAX_REQUEST_SIZE',
			doc, 'broker.max_request_size', 104857600)
		request_timeout_ms: get_config_int(cli_args, 'request-timeout-ms', 'DATACORE_REQUEST_TIMEOUT_MS',
			doc, 'broker.request_timeout_ms', 30000)
		idle_timeout_ms:    get_config_int(cli_args, 'idle-timeout-ms', 'DATACORE_IDLE_TIMEOUT_MS',
			doc, 'broker.idle_timeout_ms', 600000)
		advertised_host:    get_config_string(cli_args, 'advertised-host', 'DATACORE_ADVERTISED_HOST',
			doc, 'broker.advertised_host', broker_host)
	}
}

fn parse_rest_config(cli_args map[string]string, doc toml.Doc) RestConfig {
	return RestConfig{
		enabled:                   get_config_bool(cli_args, 'rest-enabled', 'DATACORE_REST_ENABLED',
			doc, 'rest.enabled', true)
		host:                      get_config_string(cli_args, 'rest-host', 'DATACORE_REST_HOST',
			doc, 'rest.host', '0.0.0.0')
		port:                      get_config_int(cli_args, 'rest-port', 'DATACORE_REST_PORT',
			doc, 'rest.port', 8080)
		max_connections:           get_config_int(cli_args, 'rest-max-connections', 'DATACORE_REST_MAX_CONNECTIONS',
			doc, 'rest.max_connections', 1000)
		static_dir:                get_config_string(cli_args, 'rest-static-dir', 'DATACORE_REST_STATIC_DIR',
			doc, 'rest.static_dir', 'tests/web')
		sse_heartbeat_interval_ms: get_int(doc, 'rest.sse_heartbeat_interval_ms', 15000)
		sse_connection_timeout_ms: get_int(doc, 'rest.sse_connection_timeout_ms', 3600000)
		ws_max_message_size:       get_int(doc, 'rest.ws_max_message_size', 1048576)
		ws_ping_interval_ms:       get_int(doc, 'rest.ws_ping_interval_ms', 30000)
	}
}

fn parse_grpc_config(cli_args map[string]string, doc toml.Doc) GrpcGatewayConfig {
	return GrpcGatewayConfig{
		enabled:          get_config_bool(cli_args, 'grpc-enabled', 'DATACORE_GRPC_ENABLED',
			doc, 'grpc.enabled', false)
		host:             get_config_string(cli_args, 'grpc-host', 'DATACORE_GRPC_HOST',
			doc, 'grpc.host', '0.0.0.0')
		port:             get_config_int(cli_args, 'grpc-port', 'DATACORE_GRPC_PORT',
			doc, 'grpc.port', 9094)
		max_connections:  get_config_int(cli_args, 'grpc-max-connections', 'DATACORE_GRPC_MAX_CONNECTIONS',
			doc, 'grpc.max_connections', 10000)
		max_message_size: get_int(doc, 'grpc.max_message_size', 4194304)
	}
}

fn parse_s3_config(cli_args map[string]string, doc toml.Doc) !S3StorageConfig {
	mut s3 := S3StorageConfig{
		endpoint:                     get_config_string(cli_args, 's3-endpoint', 'DATACORE_S3_ENDPOINT',
			doc, 'storage.s3.endpoint', '')
		bucket:                       get_config_string(cli_args, 's3-bucket', 'DATACORE_S3_BUCKET',
			doc, 'storage.s3.bucket', '')
		region:                       get_config_string(cli_args, 's3-region', 'DATACORE_S3_REGION',
			doc, 'storage.s3.region', 'us-east-1')
		prefix:                       get_config_string(cli_args, 's3-prefix', 'DATACORE_S3_PREFIX',
			doc, 'storage.s3.prefix', 'datacore/')
		timezone:                     get_config_string(cli_args, 's3-timezone', 'DATACORE_S3_TIMEZONE',
			doc, 'storage.s3.timezone', 'UTC')
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
		s3_plugin.validate_s3_endpoint(s3.endpoint) or {
			return error('invalid S3 endpoint: ${err}')
		}
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
	return StorageConfig{
		engine:   get_config_string(cli_args, 'storage-engine', 'DATACORE_STORAGE_ENGINE',
			doc, 'storage.engine', 'memory')
		memory:   MemoryStorageConfig{
			max_memory_mb:      get_int(doc, 'storage.memory.max_memory_mb', 20240)
			segment_size_bytes: get_int(doc, 'storage.memory.segment_size_bytes', 1073741824)
		}
		s3:       parse_s3_config(cli_args, doc)!
		sqlite:   SqliteStorageConfig{
			path:         get_string(doc, 'storage.sqlite.path', 'datacore.db')
			journal_mode: get_string(doc, 'storage.sqlite.journal_mode', 'WAL')
		}
		postgres: PostgresStorageConfig{
			host:      get_config_string(cli_args, 'postgres-host', 'DATACORE_POSTGRES_HOST',
				doc, 'storage.postgres.host', 'localhost')
			port:      get_config_int(cli_args, 'postgres-port', 'DATACORE_POSTGRES_PORT',
				doc, 'storage.postgres.port', 5432)
			database:  get_config_string(cli_args, 'postgres-database', 'DATACORE_POSTGRES_DATABASE',
				doc, 'storage.postgres.database', 'datacore')
			user:      get_config_string(cli_args, 'postgres-user', 'DATACORE_POSTGRES_USER',
				doc, 'storage.postgres.user', '')
			password:  get_config_string(cli_args, 'postgres-password', 'DATACORE_POSTGRES_PASSWORD',
				doc, 'storage.postgres.password', '')
			pool_size: get_config_int(cli_args, 'postgres-pool-size', 'DATACORE_POSTGRES_POOL_SIZE',
				doc, 'storage.postgres.pool_size', 10)
			sslmode:   get_config_string(cli_args, 'postgres-sslmode', 'DATACORE_POSTGRES_SSLMODE',
				doc, 'storage.postgres.sslmode', 'disable')
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

// Priority cascade helper functions
// Configuration value priority: CLI args > env vars > TOML > defaults

/// get_config_string retrieves a string configuration value according to priority.
/// 1. CLI argument (cli_key)
/// 2. environment variable (env_key)
/// 3. TOML file (toml_key)
/// 4. default value (default_val)
fn get_config_string(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val string) string {
	// priority 1: CLI arguments
	if cli_val := cli_args[cli_key] {
		return cli_val
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := doc.value_opt(toml_key) {
			return toml_val.string()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_config_int retrieves an integer configuration value according to priority.
fn get_config_int(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val int) int {
	// priority 1: CLI arguments
	if cli_val := cli_args[cli_key] {
		return cli_val.int()
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val.int()
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := doc.value_opt(toml_key) {
			return toml_val.int()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_config_bool retrieves a boolean configuration value according to priority.
fn get_config_bool(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val bool) bool {
	// priority 1: CLI arguments
	if cli_val := cli_args[cli_key] {
		return cli_val == 'true' || cli_val == '1' || cli_val == 'yes'
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val == 'true' || env_val == '1' || env_val == 'yes'
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := doc.value_opt(toml_key) {
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
pub fn (c Config) save(path string) ! {
	mut content := '# DataCore Configuration\n\n'

	content += '[broker]\n'
	content += 'host = "${escape_toml_string(c.broker.host)}"\n'
	content += 'port = ${c.broker.port}\n'
	content += 'broker_id = ${c.broker.broker_id}\n'
	content += 'cluster_id = "${escape_toml_string(c.broker.cluster_id)}"\n'
	content += 'max_connections = ${c.broker.max_connections}\n'
	content += 'max_request_size = ${c.broker.max_request_size}\n'
	content += '\n'

	content += '[storage]\n'
	content += 'engine = "${escape_toml_string(c.storage.engine)}"\n'
	content += '\n'

	content += '[storage.memory]\n'
	content += 'max_memory_mb = ${c.storage.memory.max_memory_mb}\n'
	content += '\n'

	content += '[schema_registry]\n'
	content += 'enabled = ${c.schema_registry.enabled}\n'
	content += 'topic = "${escape_toml_string(c.schema_registry.topic)}"\n'
	content += '\n'

	content += '[observability.metrics]\n'
	content += 'enabled = ${c.observability.metrics.enabled}\n'
	content += 'prometheus_port = ${c.observability.metrics.prometheus_port}\n'
	content += '\n'

	content += '[observability.logging]\n'
	content += 'level = "${escape_toml_string(c.observability.logging.level)}"\n'
	content += 'format = "${escape_toml_string(c.observability.logging.format)}"\n'

	os.write_file(path, content)!
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
