// Configuration Management
module config

import os
import toml

pub struct Config {
pub:
	broker          BrokerConfig
	rest            RestConfig
	storage         StorageConfig
	schema_registry SchemaRegistryConfig
	observability   ObservabilityConfig
}

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

pub struct StorageConfig {
pub:
	engine   string = 'memory'
	memory   MemoryStorageConfig
	s3       S3StorageConfig
	sqlite   SqliteStorageConfig
	postgres PostgresStorageConfig
}

pub struct MemoryStorageConfig {
pub:
	max_memory_mb      int = 1024
	segment_size_bytes int = 1073741824
}

pub struct S3StorageConfig {
pub mut:
	endpoint   string
	bucket     string
	access_key string
	secret_key string
	region     string = 'us-east-1'
	prefix     string = 'datacore/'
	timezone   string = 'UTC'
	// Batching
	batch_timeout_ms int = 1000
	batch_max_bytes  i64 = 10485760
	// Compaction
	compaction_interval_ms int = 60000
	target_segment_bytes   i64 = 104857600
	index_cache_ttl_ms     int = 30000 // New field: 30 seconds default TTL for partition index cache
}

pub struct SqliteStorageConfig {
pub:
	path         string = 'datacore.db'
	journal_mode string = 'WAL'
}

pub struct PostgresStorageConfig {
pub:
	host      string = 'localhost'
	port      int    = 5432
	database  string = 'datacore'
	user      string
	password  string
	pool_size int = 10
}

pub struct SchemaRegistryConfig {
pub:
	enabled bool   = true
	topic   string = '__schemas'
}

pub struct ObservabilityConfig {
pub:
	otel    OtelConfig
	metrics MetricsConfig
	logging LoggingConfig
	tracing TracingConfig
}

// OtelConfig - OpenTelemetry 공통 설정
pub struct OtelConfig {
pub:
	enabled             bool   = true
	service_name        string = 'datacore'
	service_version     string = '0.2.0'
	instance_id         string
	environment         string = 'development'
	otlp_endpoint       string = 'http://localhost:4317'
	otlp_http_endpoint  string
	resource_attributes string
}

pub struct MetricsConfig {
pub:
	enabled             bool   = true
	exporter            string = 'prometheus'
	prometheus_endpoint string = '/metrics'
	prometheus_port     int    = 9093
	otlp_endpoint       string
	collection_interval int = 15
}

pub struct LoggingConfig {
pub:
	enabled              bool   = true
	level                string = 'info'
	format               string = 'json'
	otlp_export          bool
	console_output       bool = true
	inject_trace_context bool = true
}

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

// Load configuration from TOML file
pub fn load_config(path string) !Config {
	// Check if file exists
	if !os.exists(path) {
		// Return default config if no file
		return Config{}
	}

	content := os.read_file(path) or { return error('Failed to read config file: ${err}') }

	doc := toml.parse_text(content) or { return error('Failed to parse config file: ${err}') }

	// Parse broker config
	broker := BrokerConfig{
		host:               get_string(doc, 'broker.host', '0.0.0.0')
		port:               get_int(doc, 'broker.port', 9092)
		broker_id:          get_int(doc, 'broker.broker_id', 1)
		cluster_id:         get_string(doc, 'broker.cluster_id', 'datacore-cluster')
		max_connections:    get_int(doc, 'broker.max_connections', 10000)
		max_request_size:   get_int(doc, 'broker.max_request_size', 104857600)
		request_timeout_ms: get_int(doc, 'broker.request_timeout_ms', 30000)
		idle_timeout_ms:    get_int(doc, 'broker.idle_timeout_ms', 600000)
		advertised_host:    get_string(doc, 'broker.advertised_host', get_string(doc,
			'broker.host', '127.0.0.1'))
	}

	// Parse REST config
	rest := RestConfig{
		enabled:                   get_bool(doc, 'rest.enabled', true)
		host:                      get_string(doc, 'rest.host', '0.0.0.0')
		port:                      get_int(doc, 'rest.port', 8080)
		max_connections:           get_int(doc, 'rest.max_connections', 1000)
		static_dir:                get_string(doc, 'rest.static_dir', 'tests/web')
		sse_heartbeat_interval_ms: get_int(doc, 'rest.sse_heartbeat_interval_ms', 15000)
		sse_connection_timeout_ms: get_int(doc, 'rest.sse_connection_timeout_ms', 3600000)
		ws_max_message_size:       get_int(doc, 'rest.ws_max_message_size', 1048576)
		ws_ping_interval_ms:       get_int(doc, 'rest.ws_ping_interval_ms', 30000)
	}

	// Parse storage config
	storage_engine := get_string(doc, 'storage.engine', 'memory')

	// Parse Memory config
	memory := MemoryStorageConfig{
		max_memory_mb:      get_int(doc, 'storage.memory.max_memory_mb', 1024)
		segment_size_bytes: get_int(doc, 'storage.memory.segment_size_bytes', 1073741824)
	}

	// Parse S3 config with environment variable override
	// Parse S3 config (base values from TOML)
	mut s3 := S3StorageConfig{
		endpoint:               get_string(doc, 'storage.s3.endpoint', '')
		bucket:                 get_string(doc, 'storage.s3.bucket', '')
		region:                 get_string(doc, 'storage.s3.region', 'us-east-1')
		prefix:                 get_string(doc, 'storage.s3.prefix', 'datacore/')
		timezone:               get_string(doc, 'storage.s3.timezone', 'UTC')
		batch_timeout_ms:       get_int(doc, 'storage.s3.batch_timeout_ms', 1000)
		batch_max_bytes:        get_i64(doc, 'storage.s3.batch_max_bytes', 10485760)
		compaction_interval_ms: get_int(doc, 'storage.s3.compaction_interval_ms', 60000)
		target_segment_bytes:   get_i64(doc, 'storage.s3.target_segment_bytes', 104857600)
		index_cache_ttl_ms:     get_int(doc, 'storage.s3.index_cache_ttl_ms', 30000) // Added TTL parsing
		access_key:             ''
		secret_key:             ''
	}

	// 1. Priority: Environment variables (*)
	s3.access_key = os.getenv('AWS_ACCESS_KEY_ID')
	s3.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

	// 2. Priority: ~/.aws/credentials
	if s3.access_key == '' || s3.secret_key == '' {
		file_access, file_secret := load_s3_credentials_from_file()
		if s3.access_key == '' {
			s3.access_key = file_access
		}
		if s3.secret_key == '' {
			s3.secret_key = file_secret
		}
	}

	// 3. Priority: config.toml
	if s3.access_key == '' {
		s3.access_key = get_string(doc, 'storage.s3.access_key', '')
	}
	if s3.secret_key == '' {
		s3.secret_key = get_string(doc, 'storage.s3.secret_key', '')
	}

	if env_endpoint := os.getenv_opt('DATACORE_S3_ENDPOINT') {
		s3.endpoint = env_endpoint
	}
	if env_bucket := os.getenv_opt('DATACORE_S3_BUCKET') {
		s3.bucket = env_bucket
	}
	if env_region := os.getenv_opt('DATACORE_S3_REGION') {
		s3.region = env_region
	}

	// Parse SQLite config
	sqlite := SqliteStorageConfig{
		path:         get_string(doc, 'storage.sqlite.path', 'datacore.db')
		journal_mode: get_string(doc, 'storage.sqlite.journal_mode', 'WAL')
	}

	// Parse PostgreSQL config
	postgres := PostgresStorageConfig{
		host:      get_string(doc, 'storage.postgres.host', 'localhost')
		port:      get_int(doc, 'storage.postgres.port', 5432)
		database:  get_string(doc, 'storage.postgres.database', 'datacore')
		user:      get_string(doc, 'storage.postgres.user', '')
		password:  get_string(doc, 'storage.postgres.password', '')
		pool_size: get_int(doc, 'storage.postgres.pool_size', 10)
	}

	storage := StorageConfig{
		engine:   storage_engine
		memory:   memory
		s3:       s3
		sqlite:   sqlite
		postgres: postgres
	}

	// Parse schema registry config
	schema_registry := SchemaRegistryConfig{
		enabled: get_bool(doc, 'schema_registry.enabled', true)
		topic:   get_string(doc, 'schema_registry.topic', '__schemas')
	}

	// Parse observability config - OTel common
	otel := OtelConfig{
		enabled:             get_bool(doc, 'observability.otel.enabled', true)
		service_name:        get_string(doc, 'observability.otel.service_name', 'datacore')
		service_version:     get_string(doc, 'observability.otel.service_version', '0.10.0')
		instance_id:         get_string(doc, 'observability.otel.instance_id', '')
		environment:         get_string(doc, 'observability.otel.environment', 'development')
		otlp_endpoint:       get_string(doc, 'observability.otel.otlp_endpoint', 'http://localhost:4317')
		otlp_http_endpoint:  get_string(doc, 'observability.otel.otlp_http_endpoint',
			'')
		resource_attributes: get_string(doc, 'observability.otel.resource_attributes',
			'')
	}

	// Parse metrics config
	metrics := MetricsConfig{
		enabled:             get_bool(doc, 'observability.metrics.enabled', true)
		exporter:            get_string(doc, 'observability.metrics.exporter', 'prometheus')
		prometheus_endpoint: get_string(doc, 'observability.metrics.prometheus_endpoint',
			'/metrics')
		prometheus_port:     get_int(doc, 'observability.metrics.prometheus_port', 9093)
		otlp_endpoint:       get_string(doc, 'observability.metrics.otlp_endpoint', '')
		collection_interval: get_int(doc, 'observability.metrics.collection_interval',
			15)
	}

	// Parse logging config
	logging := LoggingConfig{
		enabled:              get_bool(doc, 'observability.logging.enabled', true)
		level:                get_string(doc, 'observability.logging.level', 'info')
		format:               get_string(doc, 'observability.logging.format', 'json')
		otlp_export:          get_bool(doc, 'observability.logging.otlp_export', false)
		console_output:       get_bool(doc, 'observability.logging.console_output', true)
		inject_trace_context: get_bool(doc, 'observability.logging.inject_trace_context',
			true)
	}

	// Parse tracing config
	tracing := TracingConfig{
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

	observability := ObservabilityConfig{
		otel:    otel
		metrics: metrics
		logging: logging
		tracing: tracing
	}

	mut cfg := Config{
		broker:          broker
		rest:            rest
		storage:         storage
		schema_registry: schema_registry
		observability:   observability
	}

	// Validate configuration
	cfg.validate()!

	return cfg
}

// Helper function to get string value from TOML
fn get_string(doc toml.Doc, key string, default_val string) string {
	val := doc.value_opt(key) or { return default_val }
	return val.string()
}

// Helper function to get int value from TOML
fn get_int(doc toml.Doc, key string, default_val int) int {
	val := doc.value_opt(key) or { return default_val }
	return val.int()
}

// Helper function to get i64 value from TOML
fn get_i64(doc toml.Doc, key string, default_val i64) i64 {
	val := doc.value_opt(key) or { return default_val }
	return val.i64()
}

// Helper function to get f64 value from TOML
fn get_f64(doc toml.Doc, key string, default_val f64) f64 {
	val := doc.value_opt(key) or { return default_val }
	return val.f64()
}

// Helper function to get bool value from TOML
fn get_bool(doc toml.Doc, key string, default_val bool) bool {
	val := doc.value_opt(key) or { return default_val }
	return val.bool()
}

// Save configuration to TOML file
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

// Validate configuration
pub fn (c Config) validate() ! {
	// Validate broker config
	if c.broker.port < 1 || c.broker.port > 65535 {
		return error('Invalid broker port: ${c.broker.port}')
	}
	if c.broker.broker_id < 1 {
		return error('Invalid broker_id: ${c.broker.broker_id}')
	}

	// Validate storage config
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

// Get storage engine name
pub fn (c Config) get_storage_engine() string {
	return c.storage.engine
}

// Check if S3 storage is configured
pub fn (c Config) is_s3_storage() bool {
	return c.storage.engine == 's3'
}

// Check if metrics are enabled
pub fn (c Config) is_metrics_enabled() bool {
	return c.observability.metrics.enabled
}

// Check if tracing is enabled
pub fn (c Config) is_tracing_enabled() bool {
	return c.observability.tracing.enabled
}
