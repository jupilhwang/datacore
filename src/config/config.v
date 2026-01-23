// 설정 관리 모듈
// TOML 형식의 설정 파일을 로드하고 관리합니다.
module config

import os
import toml

/// Config는 DataCore의 전체 설정을 나타냅니다.
/// broker: 브로커 설정
/// rest: REST API 설정
/// storage: 스토리지 설정
/// schema_registry: 스키마 레지스트리 설정
/// observability: 관측성 설정 (메트릭, 로깅, 트레이싱)
pub struct Config {
pub:
	broker          BrokerConfig
	rest            RestConfig
	storage         StorageConfig
	schema_registry SchemaRegistryConfig
	observability   ObservabilityConfig
}

/// BrokerConfig는 Kafka 브로커 설정을 나타냅니다.
/// host: 바인딩할 호스트 주소
/// port: 바인딩할 포트 번호
/// broker_id: 브로커 고유 ID
/// cluster_id: 클러스터 ID
/// max_connections: 최대 연결 수
/// max_request_size: 최대 요청 크기 (바이트)
/// request_timeout_ms: 요청 타임아웃 (밀리초)
/// idle_timeout_ms: 유휴 연결 타임아웃 (밀리초)
/// advertised_host: 클라이언트에게 광고할 호스트 주소
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

/// RestConfig는 REST API 서버 설정을 나타냅니다.
/// enabled: REST API 활성화 여부
/// host: 바인딩할 호스트 주소
/// port: 바인딩할 포트 번호
/// max_connections: 최대 연결 수
/// static_dir: 정적 파일 디렉토리
/// sse_heartbeat_interval_ms: SSE 하트비트 간격 (밀리초)
/// sse_connection_timeout_ms: SSE 연결 타임아웃 (밀리초)
/// ws_max_message_size: WebSocket 최대 메시지 크기
/// ws_ping_interval_ms: WebSocket 핑 간격 (밀리초)
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

/// StorageConfig는 스토리지 엔진 설정을 나타냅니다.
/// engine: 스토리지 엔진 유형 ('memory', 's3', 'sqlite', 'postgres')
/// memory: 메모리 스토리지 설정
/// s3: S3 스토리지 설정
/// sqlite: SQLite 스토리지 설정
/// postgres: PostgreSQL 스토리지 설정
pub struct StorageConfig {
pub:
	engine   string = 'memory'
	memory   MemoryStorageConfig
	s3       S3StorageConfig
	sqlite   SqliteStorageConfig
	postgres PostgresStorageConfig
}

/// MemoryStorageConfig는 메모리 스토리지 설정을 나타냅니다.
/// max_memory_mb: 최대 메모리 사용량 (MB)
/// segment_size_bytes: 세그먼트 크기 (바이트)
pub struct MemoryStorageConfig {
pub:
	max_memory_mb      int = 1024
	segment_size_bytes int = 1073741824
}

/// S3StorageConfig는 S3 스토리지 설정을 나타냅니다.
/// endpoint: S3 엔드포인트 URL
/// bucket: S3 버킷 이름
/// access_key: AWS 액세스 키
/// secret_key: AWS 시크릿 키
/// region: AWS 리전
/// prefix: 객체 키 접두사
/// batch_timeout_ms: 배치 타임아웃 (밀리초)
/// batch_max_bytes: 배치 최대 크기 (바이트)
/// compaction_interval_ms: 컴팩션 간격 (밀리초)
/// target_segment_bytes: 목표 세그먼트 크기 (바이트)
/// index_cache_ttl_ms: 파티션 인덱스 캐시 TTL (밀리초)
pub struct S3StorageConfig {
pub mut:
	endpoint   string
	bucket     string
	access_key string
	secret_key string
	region     string = 'us-east-1'
	prefix     string = 'datacore/'
	timezone   string = 'UTC'
	// 배치 설정
	batch_timeout_ms int = 1000
	batch_max_bytes  i64 = 10485760
	// 컴팩션 설정
	compaction_interval_ms int = 60000
	target_segment_bytes   i64 = 104857600
	index_cache_ttl_ms     int = 30000 // 파티션 인덱스 캐시 TTL (기본 30초)
}

/// SqliteStorageConfig는 SQLite 스토리지 설정을 나타냅니다.
/// path: 데이터베이스 파일 경로
/// journal_mode: 저널 모드 ('WAL' 권장)
pub struct SqliteStorageConfig {
pub:
	path         string = 'datacore.db'
	journal_mode string = 'WAL'
}

/// PostgresStorageConfig는 PostgreSQL 스토리지 설정을 나타냅니다.
/// host: 데이터베이스 호스트
/// port: 데이터베이스 포트
/// database: 데이터베이스 이름
/// user: 사용자명
/// password: 비밀번호
/// pool_size: 연결 풀 크기
/// sslmode: SSL 모드 (disable, allow, prefer, require, verify-ca, verify-full)
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

/// SchemaRegistryConfig는 스키마 레지스트리 설정을 나타냅니다.
/// enabled: 스키마 레지스트리 활성화 여부
/// topic: 스키마를 저장할 내부 토픽 이름
pub struct SchemaRegistryConfig {
pub:
	enabled bool   = true
	topic   string = '__schemas'
}

/// ObservabilityConfig는 관측성 설정을 나타냅니다.
/// otel: OpenTelemetry 공통 설정
/// metrics: 메트릭 설정
/// logging: 로깅 설정
/// tracing: 트레이싱 설정
pub struct ObservabilityConfig {
pub:
	otel    OtelConfig
	metrics MetricsConfig
	logging LoggingConfig
	tracing TracingConfig
}

/// OtelConfig는 OpenTelemetry 공통 설정을 나타냅니다.
/// enabled: OTEL 활성화 여부
/// service_name: 서비스 이름
/// service_version: 서비스 버전
/// instance_id: 인스턴스 ID
/// environment: 환경 (development, staging, production)
/// otlp_endpoint: OTLP gRPC 엔드포인트
/// otlp_http_endpoint: OTLP HTTP 엔드포인트
/// resource_attributes: 추가 리소스 속성
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

/// MetricsConfig는 메트릭 설정을 나타냅니다.
/// enabled: 메트릭 활성화 여부
/// exporter: 내보내기 방식 ('prometheus', 'otlp')
/// prometheus_endpoint: Prometheus 엔드포인트 경로
/// prometheus_port: Prometheus 메트릭 포트
/// collection_interval: 수집 간격 (초)
pub struct MetricsConfig {
pub:
	enabled             bool   = true
	exporter            string = 'prometheus'
	prometheus_endpoint string = '/metrics'
	prometheus_port     int    = 9093
	otlp_endpoint       string
	collection_interval int = 15
}

/// LoggingConfig는 로깅 설정을 나타냅니다.
/// enabled: 로깅 활성화 여부
/// level: 로그 레벨 (trace, debug, info, warn, error, fatal)
/// format: 로그 형식 (json, text)
/// output: 출력 대상 (stdout, otel, both, none)
/// inject_trace_context: 트레이스 컨텍스트 주입 여부
pub struct LoggingConfig {
pub:
	enabled              bool   = true
	level                string = 'info'   // trace, debug, info, warn, error, fatal
	format               string = 'json'   // json, text
	output               string = 'stdout' // stdout, otel, both, none
	otlp_endpoint        string // 로그 내보내기용 OTLP 엔드포인트
	otlp_export          bool   // Deprecated: output = 'otel' 또는 'both' 사용
	console_output       bool = true // Deprecated: output = 'stdout' 또는 'both' 사용
	inject_trace_context bool = true
}

/// TracingConfig는 트레이싱 설정을 나타냅니다.
/// enabled: 트레이싱 활성화 여부
/// otlp_endpoint: OTLP 엔드포인트
/// sampler: 샘플러 유형 ('trace_id_ratio', 'always_on', 'always_off')
/// sample_rate: 샘플링 비율 (0.0 ~ 1.0)
/// batch_timeout_ms: 배치 타임아웃 (밀리초)
/// max_batch_size: 최대 배치 크기
/// max_queue_size: 최대 큐 크기
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

/// load_config는 TOML 파일에서 설정을 로드합니다.
/// path: 설정 파일 경로
/// 반환값: 로드된 Config 또는 에러
pub fn load_config(path string) !Config {
	return load_config_with_args(path, map[string]string{})
}

/// load_config_with_args는 CLI 인자를 포함하여 설정을 로드합니다.
/// 우선순위: CLI args > 환경변수 > TOML > 기본값
/// path: 설정 파일 경로
/// cli_args: CLI 인자 맵 (parse_cli_args로 파싱된 값)
/// 반환값: 로드된 Config 또는 에러
pub fn load_config_with_args(path string, cli_args map[string]string) !Config {
	// 파일 존재 확인
	if !os.exists(path) {
		// 파일이 없으면 기본 설정 + CLI/환경변수 오버라이드
		return load_default_config_with_overrides(cli_args)
	}

	content := os.read_file(path) or { return error('Failed to read config file: ${err}') }

	doc := toml.parse_text(content) or { return error('Failed to parse config file: ${err}') }

	// 브로커 설정 파싱 (우선순위 cascade 적용)
	broker_host := get_config_string(cli_args, 'broker-host', 'DATACORE_BROKER_HOST',
		doc, 'broker.host', '0.0.0.0')
	broker := BrokerConfig{
		host:               broker_host
		port:               get_config_int(cli_args, 'broker-port', 'DATACORE_BROKER_PORT',
			doc, 'broker.port', 9092)
		broker_id:          get_config_int(cli_args, 'broker-id', 'DATACORE_BROKER_ID',
			doc, 'broker.broker_id', 1)
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

	// REST 설정 파싱 (우선순위 cascade 적용)
	rest := RestConfig{
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

	// 스토리지 설정 파싱 (우선순위 cascade 적용)
	storage_engine := get_config_string(cli_args, 'storage-engine', 'DATACORE_STORAGE_ENGINE',
		doc, 'storage.engine', 'memory')

	// 메모리 설정 파싱
	memory := MemoryStorageConfig{
		max_memory_mb:      get_int(doc, 'storage.memory.max_memory_mb', 1024)
		segment_size_bytes: get_int(doc, 'storage.memory.segment_size_bytes', 1073741824)
	}

	// S3 설정 파싱 (우선순위 cascade 적용)
	mut s3 := S3StorageConfig{
		endpoint:               get_config_string(cli_args, 's3-endpoint', 'DATACORE_S3_ENDPOINT',
			doc, 'storage.s3.endpoint', '')
		bucket:                 get_config_string(cli_args, 's3-bucket', 'DATACORE_S3_BUCKET',
			doc, 'storage.s3.bucket', '')
		region:                 get_config_string(cli_args, 's3-region', 'DATACORE_S3_REGION',
			doc, 'storage.s3.region', 'us-east-1')
		prefix:                 get_config_string(cli_args, 's3-prefix', 'DATACORE_S3_PREFIX',
			doc, 'storage.s3.prefix', 'datacore/')
		timezone:               get_config_string(cli_args, 's3-timezone', 'DATACORE_S3_TIMEZONE',
			doc, 'storage.s3.timezone', 'UTC')
		batch_timeout_ms:       get_int(doc, 'storage.s3.batch_timeout_ms', 1000)
		batch_max_bytes:        get_i64(doc, 'storage.s3.batch_max_bytes', 10485760)
		compaction_interval_ms: get_int(doc, 'storage.s3.compaction_interval_ms', 60000)
		target_segment_bytes:   get_i64(doc, 'storage.s3.target_segment_bytes', 104857600)
		index_cache_ttl_ms:     get_int(doc, 'storage.s3.index_cache_ttl_ms', 30000)
		access_key:             ''
		secret_key:             ''
	}

	// S3 자격 증명 우선순위: CLI args > 환경변수 > ~/.aws/credentials > config.toml
	// 1순위: CLI 인자
	if cli_access_key := cli_args['s3-access-key'] {
		s3.access_key = cli_access_key
	}
	if cli_secret_key := cli_args['s3-secret-key'] {
		s3.secret_key = cli_secret_key
	}

	// 2순위: 환경변수
	if s3.access_key == '' {
		s3.access_key = os.getenv('AWS_ACCESS_KEY_ID')
	}
	if s3.secret_key == '' {
		s3.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
	}

	// 3순위: ~/.aws/credentials 파일
	if s3.access_key == '' || s3.secret_key == '' {
		file_access, file_secret := load_s3_credentials_from_file()
		if s3.access_key == '' {
			s3.access_key = file_access
		}
		if s3.secret_key == '' {
			s3.secret_key = file_secret
		}
	}

	// 4순위: config.toml
	if s3.access_key == '' {
		s3.access_key = get_string(doc, 'storage.s3.access_key', '')
	}
	if s3.secret_key == '' {
		s3.secret_key = get_string(doc, 'storage.s3.secret_key', '')
	}

	// SQLite 설정 파싱
	sqlite := SqliteStorageConfig{
		path:         get_string(doc, 'storage.sqlite.path', 'datacore.db')
		journal_mode: get_string(doc, 'storage.sqlite.journal_mode', 'WAL')
	}

	// PostgreSQL 설정 파싱 (우선순위 cascade 적용)
	postgres := PostgresStorageConfig{
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

	storage := StorageConfig{
		engine:   storage_engine
		memory:   memory
		s3:       s3
		sqlite:   sqlite
		postgres: postgres
	}

	// 스키마 레지스트리 설정 파싱
	schema_registry := SchemaRegistryConfig{
		enabled: get_bool(doc, 'schema_registry.enabled', true)
		topic:   get_string(doc, 'schema_registry.topic', '__schemas')
	}

	// 관측성 설정 파싱 - OTel 공통
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

	// 메트릭 설정 파싱
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

	// 로깅 설정 파싱
	logging := LoggingConfig{
		enabled:              get_bool(doc, 'observability.logging.enabled', true)
		level:                get_string(doc, 'observability.logging.level', 'info')
		format:               get_string(doc, 'observability.logging.format', 'json')
		output:               get_string(doc, 'observability.logging.output', 'stdout')
		otlp_endpoint:        get_string(doc, 'observability.logging.otlp_endpoint', '')
		otlp_export:          get_bool(doc, 'observability.logging.otlp_export', false)
		console_output:       get_bool(doc, 'observability.logging.console_output', true)
		inject_trace_context: get_bool(doc, 'observability.logging.inject_trace_context',
			true)
	}

	// 트레이싱 설정 파싱
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

	// 설정 유효성 검사
	cfg.validate()!

	return cfg
}

// ============================================================================
// 헬퍼 함수
// ============================================================================

/// get_string은 TOML 문서에서 문자열 값을 가져옵니다.
fn get_string(doc toml.Doc, key string, default_val string) string {
	val := doc.value_opt(key) or { return default_val }
	return val.string()
}

/// get_int는 TOML 문서에서 정수 값을 가져옵니다.
fn get_int(doc toml.Doc, key string, default_val int) int {
	val := doc.value_opt(key) or { return default_val }
	return val.int()
}

/// get_i64는 TOML 문서에서 64비트 정수 값을 가져옵니다.
fn get_i64(doc toml.Doc, key string, default_val i64) i64 {
	val := doc.value_opt(key) or { return default_val }
	return val.i64()
}

/// get_f64는 TOML 문서에서 실수 값을 가져옵니다.
fn get_f64(doc toml.Doc, key string, default_val f64) f64 {
	val := doc.value_opt(key) or { return default_val }
	return val.f64()
}

/// get_bool은 TOML 문서에서 불리언 값을 가져옵니다.
fn get_bool(doc toml.Doc, key string, default_val bool) bool {
	val := doc.value_opt(key) or { return default_val }
	return val.bool()
}

// ============================================================================
// 우선순위 Cascade 헬퍼 함수
// ============================================================================
// 설정 값 우선순위: CLI args > 환경변수 > TOML > 기본값

/// get_config_string은 우선순위에 따라 문자열 설정 값을 가져옵니다.
/// 1. CLI 인자 (cli_key)
/// 2. 환경변수 (env_key)
/// 3. TOML 파일 (toml_key)
/// 4. 기본값 (default_val)
fn get_config_string(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val string) string {
	// 1순위: CLI 인자
	if cli_val := cli_args[cli_key] {
		return cli_val
	}

	// 2순위: 환경변수
	if env_val := os.getenv_opt(env_key) {
		return env_val
	}

	// 3순위: TOML 파일
	if toml_val := doc.value_opt(toml_key) {
		return toml_val.string()
	}

	// 4순위: 기본값
	return default_val
}

/// get_config_int는 우선순위에 따라 정수 설정 값을 가져옵니다.
fn get_config_int(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val int) int {
	// 1순위: CLI 인자
	if cli_val := cli_args[cli_key] {
		return cli_val.int()
	}

	// 2순위: 환경변수
	if env_val := os.getenv_opt(env_key) {
		return env_val.int()
	}

	// 3순위: TOML 파일
	if toml_val := doc.value_opt(toml_key) {
		return toml_val.int()
	}

	// 4순위: 기본값
	return default_val
}

/// get_config_i64는 우선순위에 따라 64비트 정수 설정 값을 가져옵니다.
fn get_config_i64(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val i64) i64 {
	// 1순위: CLI 인자
	if cli_val := cli_args[cli_key] {
		return cli_val.i64()
	}

	// 2순위: 환경변수
	if env_val := os.getenv_opt(env_key) {
		return env_val.i64()
	}

	// 3순위: TOML 파일
	if toml_val := doc.value_opt(toml_key) {
		return toml_val.i64()
	}

	// 4순위: 기본값
	return default_val
}

/// get_config_bool은 우선순위에 따라 불리언 설정 값을 가져옵니다.
fn get_config_bool(cli_args map[string]string, cli_key string, env_key string, doc toml.Doc, toml_key string, default_val bool) bool {
	// 1순위: CLI 인자
	if cli_val := cli_args[cli_key] {
		return cli_val == 'true' || cli_val == '1' || cli_val == 'yes'
	}

	// 2순위: 환경변수
	if env_val := os.getenv_opt(env_key) {
		return env_val == 'true' || env_val == '1' || env_val == 'yes'
	}

	// 3순위: TOML 파일
	if toml_val := doc.value_opt(toml_key) {
		return toml_val.bool()
	}

	// 4순위: 기본값
	return default_val
}

/// parse_cli_args는 커맨드라인 인자를 파싱하여 map으로 반환합니다.
/// 형식: --key=value 또는 --key value
pub fn parse_cli_args(args []string) map[string]string {
	mut result := map[string]string{}

	for i := 0; i < args.len; i++ {
		arg := args[i]

		// --key=value 형식
		if arg.starts_with('--') && arg.contains('=') {
			key, val := arg[2..].split_once('=') or { continue }
			result[key] = val
		}
		// --key value 형식
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

/// save는 설정을 TOML 파일로 저장합니다.
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

/// validate는 설정의 유효성을 검사합니다.
pub fn (c Config) validate() ! {
	// 브로커 설정 검증
	if c.broker.port < 1 || c.broker.port > 65535 {
		return error('Invalid broker port: ${c.broker.port}')
	}
	if c.broker.broker_id < 1 {
		return error('Invalid broker_id: ${c.broker.broker_id}')
	}

	// 스토리지 설정 검증
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

/// get_storage_engine은 스토리지 엔진 이름을 반환합니다.
pub fn (c Config) get_storage_engine() string {
	return c.storage.engine
}

/// is_s3_storage는 S3 스토리지가 설정되어 있는지 확인합니다.
pub fn (c Config) is_s3_storage() bool {
	return c.storage.engine == 's3'
}

/// is_metrics_enabled는 메트릭이 활성화되어 있는지 확인합니다.
pub fn (c Config) is_metrics_enabled() bool {
	return c.observability.metrics.enabled
}

/// is_tracing_enabled는 트레이싱이 활성화되어 있는지 확인합니다.
pub fn (c Config) is_tracing_enabled() bool {
	return c.observability.tracing.enabled
}

/// load_default_config_with_overrides는 설정 파일 없이 CLI/환경변수로 설정을 생성합니다.
/// cli_args: CLI 인자 맵
/// 반환값: 기본값 + CLI/환경변수 오버라이드가 적용된 Config
fn load_default_config_with_overrides(cli_args map[string]string) Config {
	// 빈 TOML doc 생성
	empty_doc := toml.Doc{}

	// 브로커 설정 (우선순위 cascade 적용)
	broker_host := get_config_string(cli_args, 'broker-host', 'DATACORE_BROKER_HOST',
		empty_doc, '', '0.0.0.0')
	broker := BrokerConfig{
		host:               broker_host
		port:               get_config_int(cli_args, 'broker-port', 'DATACORE_BROKER_PORT',
			empty_doc, '', 9092)
		broker_id:          get_config_int(cli_args, 'broker-id', 'DATACORE_BROKER_ID',
			empty_doc, '', 1)
		cluster_id:         get_config_string(cli_args, 'cluster-id', 'DATACORE_CLUSTER_ID',
			empty_doc, '', 'datacore-cluster')
		max_connections:    get_config_int(cli_args, 'max-connections', 'DATACORE_MAX_CONNECTIONS',
			empty_doc, '', 10000)
		max_request_size:   get_config_int(cli_args, 'max-request-size', 'DATACORE_MAX_REQUEST_SIZE',
			empty_doc, '', 104857600)
		request_timeout_ms: get_config_int(cli_args, 'request-timeout-ms', 'DATACORE_REQUEST_TIMEOUT_MS',
			empty_doc, '', 30000)
		idle_timeout_ms:    get_config_int(cli_args, 'idle-timeout-ms', 'DATACORE_IDLE_TIMEOUT_MS',
			empty_doc, '', 600000)
		advertised_host:    get_config_string(cli_args, 'advertised-host', 'DATACORE_ADVERTISED_HOST',
			empty_doc, '', broker_host)
	}

	// REST 설정 (우선순위 cascade 적용)
	rest := RestConfig{
		enabled:                   get_config_bool(cli_args, 'rest-enabled', 'DATACORE_REST_ENABLED',
			empty_doc, '', true)
		host:                      get_config_string(cli_args, 'rest-host', 'DATACORE_REST_HOST',
			empty_doc, '', '0.0.0.0')
		port:                      get_config_int(cli_args, 'rest-port', 'DATACORE_REST_PORT',
			empty_doc, '', 8080)
		max_connections:           get_config_int(cli_args, 'rest-max-connections', 'DATACORE_REST_MAX_CONNECTIONS',
			empty_doc, '', 1000)
		static_dir:                get_config_string(cli_args, 'rest-static-dir', 'DATACORE_REST_STATIC_DIR',
			empty_doc, '', 'tests/web')
		sse_heartbeat_interval_ms: 15000
		sse_connection_timeout_ms: 3600000
		ws_max_message_size:       1048576
		ws_ping_interval_ms:       30000
	}

	// 스토리지 설정 (우선순위 cascade 적용)
	storage_engine := get_config_string(cli_args, 'storage-engine', 'DATACORE_STORAGE_ENGINE',
		empty_doc, '', 'memory')

	memory := MemoryStorageConfig{
		max_memory_mb:      1024
		segment_size_bytes: 1073741824
	}

	// S3 설정 (우선순위 cascade 적용)
	mut s3 := S3StorageConfig{
		endpoint:               get_config_string(cli_args, 's3-endpoint', 'DATACORE_S3_ENDPOINT',
			empty_doc, '', '')
		bucket:                 get_config_string(cli_args, 's3-bucket', 'DATACORE_S3_BUCKET',
			empty_doc, '', '')
		region:                 get_config_string(cli_args, 's3-region', 'DATACORE_S3_REGION',
			empty_doc, '', 'us-east-1')
		prefix:                 get_config_string(cli_args, 's3-prefix', 'DATACORE_S3_PREFIX',
			empty_doc, '', 'datacore/')
		timezone:               get_config_string(cli_args, 's3-timezone', 'DATACORE_S3_TIMEZONE',
			empty_doc, '', 'UTC')
		batch_timeout_ms:       1000
		batch_max_bytes:        10485760
		compaction_interval_ms: 60000
		target_segment_bytes:   104857600
		index_cache_ttl_ms:     30000
		access_key:             ''
		secret_key:             ''
	}

	// S3 자격 증명 우선순위: CLI args > 환경변수 > ~/.aws/credentials
	if cli_access_key := cli_args['s3-access-key'] {
		s3.access_key = cli_access_key
	}
	if cli_secret_key := cli_args['s3-secret-key'] {
		s3.secret_key = cli_secret_key
	}

	if s3.access_key == '' {
		s3.access_key = os.getenv('AWS_ACCESS_KEY_ID')
	}
	if s3.secret_key == '' {
		s3.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
	}

	if s3.access_key == '' || s3.secret_key == '' {
		file_access, file_secret := load_s3_credentials_from_file()
		if s3.access_key == '' {
			s3.access_key = file_access
		}
		if s3.secret_key == '' {
			s3.secret_key = file_secret
		}
	}

	sqlite := SqliteStorageConfig{
		path:         'datacore.db'
		journal_mode: 'WAL'
	}

	// PostgreSQL 설정 (우선순위 cascade 적용)
	postgres := PostgresStorageConfig{
		host:      get_config_string(cli_args, 'postgres-host', 'DATACORE_POSTGRES_HOST',
			empty_doc, '', 'localhost')
		port:      get_config_int(cli_args, 'postgres-port', 'DATACORE_POSTGRES_PORT',
			empty_doc, '', 5432)
		database:  get_config_string(cli_args, 'postgres-database', 'DATACORE_POSTGRES_DATABASE',
			empty_doc, '', 'datacore')
		user:      get_config_string(cli_args, 'postgres-user', 'DATACORE_POSTGRES_USER',
			empty_doc, '', '')
		password:  get_config_string(cli_args, 'postgres-password', 'DATACORE_POSTGRES_PASSWORD',
			empty_doc, '', '')
		pool_size: get_config_int(cli_args, 'postgres-pool-size', 'DATACORE_POSTGRES_POOL_SIZE',
			empty_doc, '', 10)
		sslmode:   get_config_string(cli_args, 'postgres-sslmode', 'DATACORE_POSTGRES_SSLMODE',
			empty_doc, '', 'disable')
	}

	storage := StorageConfig{
		engine:   storage_engine
		memory:   memory
		s3:       s3
		sqlite:   sqlite
		postgres: postgres
	}

	// 스키마 레지스트리 설정
	schema_registry := SchemaRegistryConfig{
		enabled: true
		topic:   '__schemas'
	}

	// 관측성 설정 (기본값)
	otel := OtelConfig{
		enabled:             true
		service_name:        'datacore'
		service_version:     '0.10.0'
		instance_id:         ''
		environment:         'development'
		otlp_endpoint:       'http://localhost:4317'
		otlp_http_endpoint:  ''
		resource_attributes: ''
	}

	metrics := MetricsConfig{
		enabled:             true
		exporter:            'prometheus'
		prometheus_endpoint: '/metrics'
		prometheus_port:     9093
		otlp_endpoint:       ''
		collection_interval: 15
	}

	logging := LoggingConfig{
		enabled:              true
		level:                'info'
		format:               'json'
		output:               'stdout'
		otlp_endpoint:        ''
		otlp_export:          false
		console_output:       true
		inject_trace_context: true
	}

	tracing := TracingConfig{
		enabled:                 false
		otlp_endpoint:           ''
		sampler:                 'trace_id_ratio'
		sample_rate:             1.0
		batch_timeout_ms:        5000
		max_batch_size:          512
		max_queue_size:          2048
		max_attributes_per_span: 128
		max_events_per_span:     128
		max_links_per_span:      128
	}

	observability := ObservabilityConfig{
		otel:    otel
		metrics: metrics
		logging: logging
		tracing: tracing
	}

	return Config{
		broker:          broker
		rest:            rest
		storage:         storage
		schema_registry: schema_registry
		observability:   observability
	}
}
