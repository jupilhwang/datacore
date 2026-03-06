// Configuration management module
// Loads and manages configuration files in TOML format.
module config

import os
import rand
import toml

/// Config represents the entire configuration for DataCore.
/// broker: broker configuration
/// rest: REST API configuration
/// grpc: gRPC gateway configuration
/// storage: storage configuration
/// schema_registry: schema registry configuration
/// observability: observability configuration (metrics, logging, tracing)
/// telemetry: simplified telemetry configuration (Task #15, mirrors [telemetry] in config.toml)
pub struct Config {
pub:
	broker          BrokerConfig
	rest            RestConfig
	grpc            GrpcGatewayConfig
	storage         StorageConfig
	schema_registry SchemaRegistryConfig
	observability   ObservabilityConfig
	telemetry       TelemetryRootConfig
}

/// TelemetryRootConfig mirrors the [telemetry] section in config.toml (Task #15).
pub struct TelemetryRootConfig {
pub:
	enabled      bool   = true
	service_name string = 'datacore'
	otlp         TelemetryOtlpConfig
	metrics      TelemetryMetricsConfig
	traces       TelemetryTracesConfig
}

/// TelemetryOtlpConfig holds OTLP endpoint settings.
pub struct TelemetryOtlpConfig {
pub:
	// gRPC OTLP endpoint (default port 4317)
	endpoint string = 'http://localhost:4317'
	// HTTP OTLP endpoint (optional)
	http_endpoint string
	insecure      bool = true
}

/// TelemetryMetricsConfig holds metrics export settings.
pub struct TelemetryMetricsConfig {
pub:
	// Export interval in seconds
	interval int = 10
	// Export timeout in milliseconds
	export_timeout int = 30000
}

/// TelemetryTracesConfig holds tracing settings.
pub struct TelemetryTracesConfig {
pub:
	// Sampling ratio (1.0 = 100%)
	sample_rate f64 = 1.0
}

/// BrokerConfig represents the Kafka broker configuration.
/// host: host address to bind
/// port: port number to bind
/// broker_id: unique broker ID
/// cluster_id: cluster ID
/// max_connections: maximum number of connections
/// max_request_size: maximum request size (bytes)
/// request_timeout_ms: request timeout (milliseconds)
/// idle_timeout_ms: idle connection timeout (milliseconds)
/// advertised_host: host address to advertise to clients
pub struct BrokerConfig {
pub:
	host               string = '0.0.0.0'
	port               int    = 9092
	broker_id          int    = 1
	cluster_id         string = 'datacore-cluster'
	max_connections    int    = 10000
	max_request_size   int    = 104857600
	request_timeout_ms int    = 30000
	idle_timeout_ms    int    = 600000
	advertised_host    string = '127.0.0.1'
}

/// GrpcGatewayConfig represents the gRPC gateway configuration.
/// enabled: whether the gRPC gateway is enabled
/// host: host address to bind
/// port: port number to bind (default: 9094 to avoid conflict with metrics on 9093)
/// max_connections: maximum number of concurrent gRPC connections
/// max_message_size: maximum message size in bytes
pub struct GrpcGatewayConfig {
pub:
	enabled          bool
	host             string = '0.0.0.0'
	port             int    = 9094
	max_connections  int    = 10000
	max_message_size int    = 4194304
}

/// RestConfig represents the REST API server configuration.
/// enabled: whether REST API is enabled
/// host: host address to bind
/// port: port number to bind
/// max_connections: maximum number of connections
/// static_dir: static file directory
/// sse_heartbeat_interval_ms: SSE heartbeat interval (milliseconds)
/// sse_connection_timeout_ms: SSE connection timeout (milliseconds)
/// ws_max_message_size: WebSocket maximum message size
/// ws_ping_interval_ms: WebSocket ping interval (milliseconds)
pub struct RestConfig {
pub:
	enabled                   bool   = true
	host                      string = '0.0.0.0'
	port                      int    = 8080
	max_connections           int    = 1000
	static_dir                string = 'tests/web'
	sse_heartbeat_interval_ms int    = 15000
	sse_connection_timeout_ms int    = 3600000
	ws_max_message_size       int    = 1048576
	ws_ping_interval_ms       int    = 30000
}

/// StorageConfig represents the storage engine configuration.
/// engine: storage engine type ('memory', 's3', 'sqlite', 'postgres')
/// memory: memory storage configuration
/// s3: S3 storage configuration
/// sqlite: SQLite storage configuration
/// postgres: PostgreSQL storage configuration
pub struct StorageConfig {
pub:
	engine   string = 'memory'
	memory   MemoryStorageConfig
	s3       S3StorageConfig
	sqlite   SqliteStorageConfig
	postgres PostgresStorageConfig
}

/// MemoryStorageConfig represents the memory storage configuration.
/// max_memory_mb: maximum memory usage (MB)
/// segment_size_bytes: segment size (bytes)
pub struct MemoryStorageConfig {
pub:
	max_memory_mb      int = 20240
	segment_size_bytes int = 1073741824
}

/// S3StorageConfig represents the S3 storage configuration.
/// endpoint: S3 endpoint URL
/// bucket: S3 bucket name
/// access_key: AWS access key
/// secret_key: AWS secret key
/// region: AWS region
/// prefix: object key prefix
/// batch_timeout_ms: batch timeout (milliseconds)
/// batch_max_bytes: maximum batch size (bytes)
/// compaction_interval_ms: compaction interval (milliseconds)
/// target_segment_bytes: target segment size (bytes)
/// index_cache_ttl_ms: partition index cache TTL (milliseconds)
/// iceberg_enabled: whether to use Iceberg format
/// iceberg_format: file format (parquet, orc, avro)
/// iceberg_compression: compression method (none, snappy, gzip, zstd)
/// iceberg_write_mode: write mode (append, overwrite)
/// iceberg_partition_by: list of partitioning columns
/// iceberg_max_rows_per_file: maximum rows per file
/// iceberg_max_file_size_mb: maximum file size (MB)
/// iceberg_schema_evolution: whether schema evolution is supported
/// iceberg_format_version: Iceberg format version (default 2 - stable spec)
pub struct S3StorageConfig {
pub mut:
	endpoint   string
	bucket     string
	access_key string
	secret_key string
	region     string = 'us-west-2'
	prefix     string = 'datacore/'
	timezone   string = 'UTC'
	// batch configuration
	batch_timeout_ms int = 25
	batch_max_bytes  i64 = 4096000
	// flush threshold: skip flush when buffer < min_flush_bytes to prevent micro-segments
	min_flush_bytes      int = 4096
	max_flush_skip_count int = 40
	// compaction configuration
	compaction_interval_ms int = 30000
	target_segment_bytes   i64 = 104857600
	index_cache_ttl_ms     int = 30000 // partition index cache TTL (default 30 seconds)
	// offset batch configuration
	offset_batch_enabled         bool = true
	offset_flush_interval_ms     int  = 100
	offset_flush_threshold_count int  = 50
	// index batch configuration: accumulate N segments before writing index to S3
	index_batch_size        int = 5
	index_flush_interval_ms int = 500
	// sync linger: batch acks=1/-1 produce requests within a short window (ms)
	// 0 = disabled (immediate per-request write); default 5ms
	sync_linger_ms int = 5
	// Iceberg table format configuration (flattened from IcebergConfig for TOML parsing)
	iceberg_enabled           bool
	iceberg_format            string   = 'parquet'
	iceberg_compression       string   = 'zstd'
	iceberg_write_mode        string   = 'append'
	iceberg_partition_by      []string = ['timestamp', 'topic']
	iceberg_max_rows_per_file int      = 1000000
	iceberg_max_file_size_mb  int      = 128
	iceberg_schema_evolution  bool     = true
	iceberg_format_version    int      = 2
}

/// SqliteStorageConfig represents the SQLite storage configuration.
/// path: database file path
/// journal_mode: journal mode ('WAL' recommended)
pub struct SqliteStorageConfig {
pub:
	path         string = 'datacore.db'
	journal_mode string = 'WAL'
}

/// PostgresStorageConfig represents the PostgreSQL storage configuration.
/// host: database host
/// port: database port
/// database: database name
/// user: username
/// password: password
/// pool_size: connection pool size
/// sslmode: SSL mode (disable, allow, prefer, require, verify-ca, verify-full)
pub struct PostgresStorageConfig {
pub:
	host      string = 'localhost'
	port      int    = 5432
	database  string = 'datacore'
	user      string
	password  string
	pool_size int    = 10
	sslmode   string = 'disable'
}

/// SchemaRegistryConfig represents the schema registry configuration.
/// enabled: whether schema registry is enabled
/// topic: internal topic name for storing schemas
pub struct SchemaRegistryConfig {
pub:
	enabled bool   = true
	topic   string = '__schemas'
}

/// ObservabilityConfig represents the observability configuration.
/// otel: OpenTelemetry common configuration
/// metrics: metrics configuration
/// logging: logging configuration
/// tracing: tracing configuration
pub struct ObservabilityConfig {
pub:
	otel    OtelConfig
	metrics MetricsConfig
	logging LoggingConfig
	tracing TracingConfig
}

/// OtelConfig represents the OpenTelemetry common configuration.
/// enabled: whether OTEL is enabled
/// service_name: service name
/// service_version: service version
/// instance_id: instance ID
/// environment: environment (development, staging, production)
/// otlp_endpoint: OTLP gRPC endpoint
/// otlp_http_endpoint: OTLP HTTP endpoint
/// resource_attributes: additional resource attributes
pub struct OtelConfig {
pub:
	enabled             bool   = true
	service_name        string = 'datacore'
	service_version     string = '0.44.4'
	instance_id         string
	environment         string = 'development'
	otlp_endpoint       string = 'http://localhost:4317'
	otlp_http_endpoint  string
	resource_attributes string
}

/// MetricsConfig represents the metrics configuration.
/// enabled: whether metrics are enabled
/// exporter: export method ('prometheus', 'otlp')
/// prometheus_endpoint: Prometheus endpoint path
/// prometheus_port: Prometheus metrics port
/// collection_interval: collection interval (seconds)
pub struct MetricsConfig {
pub:
	enabled             bool   = true
	exporter            string = 'prometheus'
	prometheus_endpoint string = '/metrics'
	prometheus_port     int    = 9093
	otlp_endpoint       string
	collection_interval int = 15
}

/// LoggingConfig represents the logging configuration.
/// enabled: whether logging is enabled
/// level: log level (trace, debug, info, warn, error, fatal)
/// format: log format (json, text)
/// output: output destination (stdout, otel, both, none)
/// inject_trace_context: whether to inject trace context
pub struct LoggingConfig {
pub:
	enabled              bool   = true
	level                string = 'debug'  // trace, debug, info, warn, error, fatal
	format               string = 'json'   // json, text
	output               string = 'stdout' // stdout, otel, both, none
	otlp_endpoint        string // OTLP endpoint for log export
	otlp_export          bool   // Deprecated: use output = 'otel' or 'both'
	console_output       bool = true // Deprecated: use output = 'stdout' or 'both'
	inject_trace_context bool = true
}

/// TracingConfig represents the tracing configuration.
/// enabled: whether tracing is enabled
/// otlp_endpoint: OTLP endpoint
/// sampler: sampler type ('trace_id_ratio', 'always_on', 'always_off')
/// sample_rate: sampling rate (0.0 ~ 1.0)
/// batch_timeout_ms: batch timeout (milliseconds)
/// max_batch_size: maximum batch size
/// max_queue_size: maximum queue size
pub struct TracingConfig {
pub:
	enabled                 bool
	otlp_endpoint           string
	sampler                 string = 'trace_id_ratio'
	sample_rate             f64    = 1.0
	batch_timeout_ms        int    = 5000
	max_batch_size          int    = 512
	max_queue_size          int    = 2048
	max_attributes_per_span int    = 128
	max_events_per_span     int    = 128
	max_links_per_span      int    = 128
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
		storage:         parse_storage_config(cli_args, doc)
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

fn parse_s3_config(cli_args map[string]string, doc toml.Doc) S3StorageConfig {
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
		min_flush_bytes:              get_int(doc, 'storage.s3.min_flush_bytes', 4096)
		max_flush_skip_count:         get_int(doc, 'storage.s3.max_flush_skip_count',
			40)
		compaction_interval_ms:       get_int(doc, 'storage.s3.compaction_interval_ms',
			30000)
		target_segment_bytes:         get_i64(doc, 'storage.s3.target_segment_bytes',
			104857600)
		index_cache_ttl_ms:           get_int(doc, 'storage.s3.index_cache_ttl_ms', 30000)
		offset_batch_enabled:         get_bool(doc, 'storage.s3.offset_batch_enabled',
			true)
		offset_flush_interval_ms:     get_int(doc, 'storage.s3.offset_flush_interval_ms',
			100)
		offset_flush_threshold_count: get_int(doc, 'storage.s3.offset_flush_threshold_count',
			50)
		index_batch_size:             get_int(doc, 'storage.s3.index_batch_size', 5)
		index_flush_interval_ms:      get_int(doc, 'storage.s3.index_flush_interval_ms',
			500)
		sync_linger_ms:               get_int(doc, 'storage.s3.sync_linger_ms', 5)
		access_key:                   ''
		secret_key:                   ''
	}

	s3.endpoint = if s3.endpoint == '' {
		'https://${s3.bucket}.s3.${s3.region}.amazonaws.com'
	} else {
		s3.endpoint
	}

	// S3 credentials priority: CLI args > env vars > ~/.aws/credentials > config.toml
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

	// parse Iceberg configuration from [storage.s3.iceberg] section
	iceberg_enabled := get_bool(doc, 'storage.s3.iceberg.enabled', false)
	if iceberg_enabled {
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
		s3.iceberg_format_version = get_int(doc, 'storage.s3.iceberg.format_version',
			2)
	}

	return s3
}

fn parse_storage_config(cli_args map[string]string, doc toml.Doc) StorageConfig {
	return StorageConfig{
		engine:   get_config_string(cli_args, 'storage-engine', 'DATACORE_STORAGE_ENGINE',
			doc, 'storage.engine', 'memory')
		memory:   MemoryStorageConfig{
			max_memory_mb:      get_int(doc, 'storage.memory.max_memory_mb', 20240)
			segment_size_bytes: get_int(doc, 'storage.memory.segment_size_bytes', 1073741824)
		}
		s3:       parse_s3_config(cli_args, doc)
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
		metrics: MetricsConfig{
			enabled:             get_bool(doc, 'observability.metrics.enabled', true)
			exporter:            get_string(doc, 'observability.metrics.exporter', 'prometheus')
			prometheus_endpoint: get_string(doc, 'observability.metrics.prometheus_endpoint',
				'/metrics')
			prometheus_port:     get_int(doc, 'observability.metrics.prometheus_port',
				9093)
			otlp_endpoint:       get_string(doc, 'observability.metrics.otlp_endpoint',
				'')
			collection_interval: get_int(doc, 'observability.metrics.collection_interval',
				15)
		}
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
		tracing: TracingConfig{
			enabled:                 get_bool(doc, 'observability.tracing.enabled', false)
			otlp_endpoint:           get_string(doc, 'observability.tracing.otlp_endpoint',
				'')
			sampler:                 get_string(doc, 'observability.tracing.sampler',
				'trace_id_ratio')
			sample_rate:             get_f64(doc, 'observability.tracing.sample_rate',
				1.0)
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

/// get_config_i64 retrieves a 64-bit integer configuration value according to priority.
fn get_config_i64(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val i64) i64 {
	// priority 1: CLI arguments
	if cli_val := cli_args[cli_key] {
		return cli_val.i64()
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val.i64()
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := doc.value_opt(toml_key) {
			return toml_val.i64()
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
			}
		}
	}

	return result
}

/// save saves the configuration to a TOML file.
pub fn (c Config) save(path string) ! {
	mut content := '# DataCore Configuration\n\n'

	content += '[broker]\n'
	content += 'host = "${c.broker.host}"\n'
	content += 'port = ${c.broker.port}\n'
	content += 'broker_id = ${c.broker.broker_id}\n'
	content += 'cluster_id = "${c.broker.cluster_id}"\n'
	content += 'max_connections = ${c.broker.max_connections}\n'
	content += 'max_request_size = ${c.broker.max_request_size}\n'
	content += '\n'

	content += '[storage]\n'
	content += 'engine = "${c.storage.engine}"\n'
	content += '\n'

	content += '[storage.memory]\n'
	content += 'max_memory_mb = ${c.storage.memory.max_memory_mb}\n'
	content += '\n'

	content += '[schema_registry]\n'
	content += 'enabled = ${c.schema_registry.enabled}\n'
	content += 'topic = "${c.schema_registry.topic}"\n'
	content += '\n'

	content += '[observability.metrics]\n'
	content += 'enabled = ${c.observability.metrics.enabled}\n'
	content += 'prometheus_port = ${c.observability.metrics.prometheus_port}\n'
	content += '\n'

	content += '[observability.logging]\n'
	content += 'level = "${c.observability.logging.level}"\n'
	content += 'format = "${c.observability.logging.format}"\n'

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
/// returns: Config with defaults applied and CLI/env overrides
fn load_default_config_with_overrides(cli_args map[string]string) Config {
	// toml.Doc{} has a nil ast pointer that causes segfault on value_opt calls.
	// toml.parse_text('') produces a properly initialized empty document.
	// If even that fails, return a zero-value Config to avoid a nil ast segfault.
	empty_doc := toml.parse_text('') or { return Config{} }
	return Config{
		broker:          parse_broker_config(cli_args, empty_doc)
		rest:            parse_rest_config(cli_args, empty_doc)
		grpc:            parse_grpc_config(cli_args, empty_doc)
		storage:         parse_storage_config(cli_args, empty_doc)
		schema_registry: parse_schema_registry_config(empty_doc)
		observability:   parse_observability_config(empty_doc)
		telemetry:       parse_telemetry_config(empty_doc)
	}
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

/// get_env_value searches for an environment variable value by TOML key.
/// search order: 1) uppercase version, 2) lowercase version, 3) DATACORE_ prefix + uppercase
/// returns: (value, found)
fn get_env_value(toml_key string) (string, bool) {
	// 1. search uppercase version (e.g. BROKER_HOST)
	env_key_upper := toml_key_to_env_key_upper(toml_key)
	if env_val := os.getenv_opt(env_key_upper) {
		return env_val, true
	}

	// 2. search lowercase version (e.g. broker_host)
	env_key_lower := toml_key_to_env_key_lower(toml_key)
	if env_val := os.getenv_opt(env_key_lower) {
		return env_val, true
	}

	// 3. search DATACORE_ prefix + uppercase (e.g. DATACORE_BROKER_HOST)
	env_key_prefixed := 'DATACORE_' + env_key_upper
	if env_val := os.getenv_opt(env_key_prefixed) {
		return env_val, true
	}

	return '', false
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

	// Broker
	println('[broker]')
	println('  broker.host        -> ' + toml_key_to_env_key_upper('broker.host') + ', ' +
		toml_key_to_env_key_lower('broker.host') + ', DATACORE_' +
		toml_key_to_env_key_upper('broker.host'))
	println('  broker.port        -> ' + toml_key_to_env_key_upper('broker.port') + ', ' +
		toml_key_to_env_key_lower('broker.port') + ', DATACORE_' +
		toml_key_to_env_key_upper('broker.port'))
	println('  broker.cluster_id  -> ' + toml_key_to_env_key_upper('broker.cluster_id') + ', ' +
		toml_key_to_env_key_lower('broker.cluster_id') + ', DATACORE_' +
		toml_key_to_env_key_upper('broker.cluster_id'))
	println('')

	// Storage
	println('[storage]')
	println('  storage.engine     -> ' + toml_key_to_env_key_upper('storage.engine') + ', ' +
		toml_key_to_env_key_lower('storage.engine') + ', DATACORE_' +
		toml_key_to_env_key_upper('storage.engine'))
	println('')

	// S3
	println('[s3]')
	println('  s3.endpoint   -> ' + toml_key_to_env_key_upper('s3.endpoint') + ', ' +
		toml_key_to_env_key_lower('s3.endpoint') + ', DATACORE_' +
		toml_key_to_env_key_upper('s3.endpoint'))
	println('  s3.bucket     -> ' + toml_key_to_env_key_upper('s3.bucket') + ', ' +
		toml_key_to_env_key_lower('s3.bucket') + ', DATACORE_' +
		toml_key_to_env_key_upper('s3.bucket'))
	println('')

	// PostgreSQL
	println('[postgres]')
	println('  postgres.host     -> ' + toml_key_to_env_key_upper('postgres.host') + ', ' +
		toml_key_to_env_key_lower('postgres.host') + ', DATACORE_' +
		toml_key_to_env_key_upper('postgres.host'))
	println('  postgres.password -> ' + toml_key_to_env_key_upper('postgres.password') + ', ' +
		toml_key_to_env_key_lower('postgres.password') + ', DATACORE_' +
		toml_key_to_env_key_upper('postgres.password'))
	println('')

	// Logging
	println('[logging]')
	println('  logging.level     -> ' + toml_key_to_env_key_upper('logging.level') + ', ' +
		toml_key_to_env_key_lower('logging.level') + ', DATACORE_' +
		toml_key_to_env_key_upper('logging.level'))
	println('')
}

// Deterministic Broker ID generation
// Generates the same broker_id on the same server every time,
// based on unique server identifiers (MAC address, IP address, hostname).
// Fallback order: MAC address -> IP address -> hostname -> random

/// generate_deterministic_broker_id generates a deterministic broker_id based on server identity.
/// Always returns the same value when run on the same server.
fn generate_deterministic_broker_id() int {
	// priority 1: MAC address
	mac := get_mac_address()
	if mac.len > 0 {
		return string_to_broker_id(mac)
	}

	// priority 2: IP address
	ip := get_primary_ip()
	if ip.len > 0 {
		return string_to_broker_id(ip)
	}

	// priority 3: hostname
	hostname := os.hostname() or { '' }
	if hostname.len > 0 {
		return string_to_broker_id(hostname)
	}

	// last resort: random fallback
	return rand.int_in_range(1, 99999999) or { 1 }
}

/// get_mac_address returns the MAC address of the first physical network interface.
/// Selects the first valid MAC that is not loopback (00:00:00:00:00:00).
fn get_mac_address() string {
	// Linux: read from /sys/class/net/ directory
	if os.exists('/sys/class/net') {
		result := os.execute('for iface in /sys/class/net/*; do cat "\${iface}/address" 2>/dev/null; done')
		if result.exit_code == 0 && result.output.len > 0 {
			lines := result.output.split('\n')
			for line in lines {
				mac := line.trim_space()
				if mac.len > 0 && mac != '00:00:00:00:00:00' {
					return mac
				}
			}
		}
	}

	// macOS: read from ifconfig
	result := os.execute('ifconfig 2>/dev/null | grep ether | head -1')
	if result.exit_code == 0 && result.output.len > 0 {
		parts := result.output.trim_space().split(' ')
		for i, part in parts {
			if part == 'ether' && i + 1 < parts.len {
				mac := parts[i + 1].trim_space()
				if mac.len > 0 && mac != '00:00:00:00:00:00' {
					return mac
				}
			}
		}
	}

	return ''
}

/// get_primary_ip returns the primary IP address used for external connections.
fn get_primary_ip() string {
	// Linux: get source IP via ip route
	result_ip := os.execute("ip route get 1.1.1.1 2>/dev/null | grep -oP 'src \\K[^ ]+'")
	if result_ip.exit_code == 0 && result_ip.output.len > 0 {
		ip := result_ip.output.trim_space()
		if ip.len > 0 && ip != '127.0.0.1' {
			return ip
		}
	}

	// macOS: get default interface then extract IP
	result_mac := os.execute('ipconfig getifaddr en0 2>/dev/null')
	if result_mac.exit_code == 0 && result_mac.output.len > 0 {
		ip := result_mac.output.trim_space()
		if ip.len > 0 && ip != '127.0.0.1' {
			return ip
		}
	}

	return ''
}

/// string_to_broker_id hashes a string using FNV-1a and maps it to a broker_id in the range 1~99999999.
fn string_to_broker_id(s string) int {
	// FNV-1a 32-bit hash
	mut hash := u64(0x811c9dc5)
	for b in s.bytes() {
		hash = hash ^ u64(b)
		hash = (hash * u64(0x01000193)) & u64(0xFFFFFFFF)
	}
	// map to range 1 ~ 99999999
	return int(hash % 99999998) + 1
}
