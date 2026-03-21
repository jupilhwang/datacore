// Configuration type definitions
// All struct types used across the config module.
module config

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
	min_flush_bytes      int = 65536
	max_flush_skip_count int = 80
	// compaction configuration
	compaction_interval_ms int = 60000
	target_segment_bytes   i64 = 104857600
	index_cache_ttl_ms     int = 60000 // partition index cache TTL (default 1 minute)
	// offset batch configuration
	offset_batch_enabled         bool = true
	offset_flush_interval_ms     int  = 100
	offset_flush_threshold_count int  = 50
	// index batch configuration: accumulate N segments before writing index to S3
	index_batch_size        int = 5
	index_flush_interval_ms int = 500
	// sync linger: batch acks=1/-1 produce requests within a short window (ms)
	// 0 = disabled (immediate per-request write, safe default)
	// >0 = linger window in ms (reduces PUT cost but adds latency)
	sync_linger_ms int
	// Server-side copy: use S3 Multipart Copy for compaction to avoid data transfer
	// When true, compaction tries server-side copy first, falls back to download-reupload
	use_server_side_copy bool = true
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
