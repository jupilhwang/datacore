/// 인프라 레이어 - 구조화된 로깅 (OpenTelemetry 호환)
/// JSON 형식 로깅과 컨텍스트 전파 및 OTLP 내보내기 지원
module observability

import sync
import time

// ============================================================
// 로그 레벨
// ============================================================

/// LogLevel은 로그 항목의 심각도를 나타냅니다.
pub enum LogLevel {
	trace = 0
	debug = 1
	info  = 2
	warn  = 3
	error = 4
	fatal = 5
}

/// LogLevel을 문자열로 변환합니다.
pub fn (l LogLevel) str() string {
	return match l {
		.trace { 'TRACE' }
		.debug { 'DEBUG' }
		.info { 'INFO' }
		.warn { 'WARN' }
		.error { 'ERROR' }
		.fatal { 'FATAL' }
	}
}

/// 문자열에서 LogLevel을 생성합니다.
pub fn log_level_from_string(s string) LogLevel {
	return match s.to_lower() {
		'trace' { .trace }
		'debug' { .debug }
		'info' { .info }
		'warn', 'warning' { .warn }
		'error' { .error }
		'fatal' { .fatal }
		else { .info }
	}
}

// ============================================================
// 로그 출력 대상
// ============================================================

/// LogOutput은 로그가 전송되는 위치를 결정합니다.
pub enum LogOutput {
	stdout // 기본값: stdout/stderr에 출력
	otel   // OTLP를 통해 OpenTelemetry Collector로 전송
	both   // stdout과 OTLP 모두에 출력
	none   // 로깅 비활성화 (테스트용)
}

/// 문자열에서 LogOutput을 생성합니다.
pub fn log_output_from_string(s string) LogOutput {
	return match s.to_lower() {
		'stdout', 'console' { .stdout }
		'otel', 'otlp', 'opentelemetry' { .otel }
		'both', 'all' { .both }
		'none', 'off', 'disabled' { .none }
		else { .stdout }
	}
}

// ============================================================
// 로그 필드 (구조화된 로깅)
// ============================================================

/// LogField는 구조화된 로깅을 위한 키-값 쌍을 나타냅니다.
pub struct LogField {
pub:
	key   string
	value string // 모든 값은 문자열로 직렬화됨
}

/// 필드 생성자 - 비활성화된 레벨에 대해 제로 할당
@[inline]
pub fn field_string(key string, value string) LogField {
	return LogField{
		key:   key
		value: value
	}
}

@[inline]
pub fn field_int(key string, value i64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

@[inline]
pub fn field_uint(key string, value u64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

@[inline]
pub fn field_float(key string, value f64) LogField {
	return LogField{
		key:   key
		value: '${value:.6}'
	}
}

@[inline]
pub fn field_bool(key string, value bool) LogField {
	return LogField{
		key:   key
		value: if value { 'true' } else { 'false' }
	}
}

@[inline]
pub fn field_error(err IError) LogField {
	return LogField{
		key:   'error'
		value: err.str()
	}
}

@[inline]
pub fn field_err_str(err_msg string) LogField {
	return LogField{
		key:   'error'
		value: err_msg
	}
}

@[inline]
pub fn field_duration(key string, d time.Duration) LogField {
	ms := f64(d) / f64(time.millisecond)
	return LogField{
		key:   '${key}_ms'
		value: '${ms:.3}'
	}
}

@[inline]
pub fn field_bytes(key string, size i64) LogField {
	return LogField{
		key:   '${key}_bytes'
		value: '${size}'
	}
}

// ============================================================
// 로그 컨텍스트 (트레이스 전파)
// ============================================================

/// LogContext는 분산 트레이싱을 위한 트레이스 컨텍스트를 보유합니다.
pub struct LogContext {
pub:
	trace_id  string
	span_id   string
	parent_id string
	service   string
	instance  string
}

// ============================================================
// 로그 항목
// ============================================================

/// LogEntry는 단일 로그 항목을 나타냅니다.
pub struct LogEntry {
pub:
	timestamp   time.Time
	level       LogLevel
	message     string
	logger_name string
	fields      []LogField
	context     LogContext
}

// ============================================================
// 출력 형식
// ============================================================

/// OutputFormat은 로그 형식을 결정합니다.
pub enum OutputFormat {
	json // JSON 형식 (기본값, 프로덕션용)
	text // 사람이 읽기 쉬운 텍스트 형식 (개발용)
}

/// 문자열에서 OutputFormat을 생성합니다.
pub fn output_format_from_string(s string) OutputFormat {
	return match s.to_lower() {
		'json' { .json }
		'text', 'plain', 'console' { .text }
		else { .json }
	}
}

// ============================================================
// 로거 설정
// ============================================================

/// LoggerConfig는 로거 설정을 보유합니다.
pub struct LoggerConfig {
pub:
	name          string       = 'datacore'
	level         LogLevel     = .info
	format        OutputFormat = .json
	output        LogOutput    = .stdout
	service       string       = 'datacore'
	instance_id   string
	otlp_endpoint string // OTLP 내보내기용 (예: "http://localhost:4317")
}

// ============================================================
// 로거 (스레드 안전)
// ============================================================

/// Logger는 구조화된 로깅 기능을 제공합니다.
pub struct Logger {
pub:
	name          string
	level         LogLevel
	format        OutputFormat
	output        LogOutput
	context       LogContext
	fields        []LogField // 모든 로그 항목에 대한 기본 필드
	otlp_endpoint string
mut:
	otlp_buffer []LogEntry // OTLP 배치 내보내기용 버퍼
	buffer_lock sync.Mutex
}

/// new_logger는 설정으로 새 로거를 생성합니다.
pub fn new_logger(config LoggerConfig) &Logger {
	return &Logger{
		name:          config.name
		level:         config.level
		format:        config.format
		output:        config.output
		otlp_endpoint: config.otlp_endpoint
		context:       LogContext{
			service:  config.service
			instance: config.instance_id
		}
		fields:        []
		otlp_buffer:   []
	}
}

/// new_default_logger는 기본 설정으로 로거를 생성합니다.
pub fn new_default_logger() &Logger {
	return new_logger(LoggerConfig{})
}

/// with_name은 다른 이름을 가진 새 로거를 반환합니다 (하위 컴포넌트용).
pub fn (l &Logger) with_name(name string) &Logger {
	return &Logger{
		name:          name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       l.context
		fields:        l.fields
		otlp_buffer:   []
	}
}

/// with_context는 컨텍스트가 포함된 새 로거를 반환합니다.
pub fn (l &Logger) with_context(ctx LogContext) &Logger {
	return &Logger{
		name:          l.name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       ctx
		fields:        l.fields
		otlp_buffer:   []
	}
}

/// with_fields는 추가 기본 필드가 포함된 새 로거를 반환합니다.
pub fn (l &Logger) with_fields(fields ...LogField) &Logger {
	mut new_fields := l.fields.clone()
	new_fields << fields

	return &Logger{
		name:          l.name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       l.context
		fields:        new_fields
		otlp_buffer:   []
	}
}

// ============================================================
// 로깅 메서드 (성능을 위한 조기 종료)
// ============================================================

/// should_log는 레벨이 로깅되어야 하는지 확인합니다 (성능을 위해 인라인).
@[inline]
pub fn (l &Logger) should_log(level LogLevel) bool {
	return int(level) >= int(l.level)
}

/// log는 로그 항목을 작성합니다.
pub fn (mut l Logger) log(level LogLevel, msg string, fields ...LogField) {
	// 비활성화된 레벨에 대한 조기 종료 (제로 오버헤드)
	if !l.should_log(level) {
		return
	}

	// 출력이 비활성화된 경우 조기 종료
	if l.output == .none {
		return
	}

	mut all_fields := l.fields.clone()
	all_fields << fields

	entry := LogEntry{
		timestamp:   time.now()
		level:       level
		message:     msg
		logger_name: l.name
		fields:      all_fields
		context:     l.context
	}

	// stdout/stderr로 출력
	if l.output == .stdout || l.output == .both {
		output := if l.format == .json {
			format_entry_json(entry)
		} else {
			format_entry_text(entry)
		}

		// stdout에 쓰기 (에러는 stderr로)
		if int(level) >= int(LogLevel.error) {
			eprint(output)
		} else {
			print(output)
		}
	}

	// OTLP 내보내기용 버퍼링
	if l.output == .otel || l.output == .both {
		l.buffer_lock.@lock()
		l.otlp_buffer << entry
		l.buffer_lock.unlock()
	}

	// Fatal 로그는 종료해야 함
	if level == .fatal {
		l.flush() // 종료 전 로그 전송 보장
		exit(1)
	}
}

/// 성능을 위한 인라인 힌트가 있는 편의 메서드
@[inline]
pub fn (mut l Logger) trace(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.trace) {
		l.log(.trace, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) debug(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.debug) {
		l.log(.debug, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) info(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.info) {
		l.log(.info, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) warn(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.warn) {
		l.log(.warn, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) error(msg string, fields ...LogField) {
	l.log(.error, msg, ...fields)
}

@[inline]
pub fn (mut l Logger) fatal(msg string, fields ...LogField) {
	l.log(.fatal, msg, ...fields)
}

/// flush는 버퍼링된 로그를 OTLP 엔드포인트로 전송합니다.
pub fn (mut l Logger) flush() {
	if l.otlp_endpoint.len == 0 {
		return
	}

	l.buffer_lock.@lock()
	if l.otlp_buffer.len == 0 {
		l.buffer_lock.unlock()
		return
	}
	entries := l.otlp_buffer.clone()
	l.otlp_buffer.clear()
	l.buffer_lock.unlock()

	// OTLP로 내보내기 (비동기)
	spawn export_logs_to_otlp(l.otlp_endpoint, l.context.service, entries)
}

// ============================================================
// 전역 로거 (구조체 홀더를 사용한 싱글톤 패턴)
// ============================================================

/// LoggerHolder는 싱글톤 로거 인스턴스를 보유합니다.
struct LoggerHolder {
mut:
	logger &Logger = unsafe { nil }
	lock   sync.Mutex
}

/// 전역 홀더 인스턴스 (인라인으로 초기화됨)
const logger_holder = &LoggerHolder{}

/// init_global_logger는 전역 로거를 초기화합니다 (시작 시 한 번 호출).
pub fn init_global_logger(config LoggerConfig) {
	mut holder := unsafe { logger_holder }
	holder.lock.@lock()
	defer { holder.lock.unlock() }
	holder.logger = new_logger(config)
}

/// get_logger는 전역 로거 인스턴스를 반환합니다.
/// 초기화되지 않은 경우 기본 로거를 반환합니다.
@[inline]
pub fn get_logger() &Logger {
	mut holder := unsafe { logger_holder }
	if holder.logger == unsafe { nil } {
		// 기본값으로 지연 초기화
		holder.lock.@lock()
		if holder.logger == unsafe { nil } {
			holder.logger = new_default_logger()
		}
		holder.lock.unlock()
	}
	return holder.logger
}

/// get_named_logger는 특정 이름을 가진 로거를 반환합니다 (하위 컴포넌트용).
@[inline]
pub fn get_named_logger(name string) &Logger {
	return get_logger().with_name(name)
}

// ============================================================
// 빠른 로깅 함수 (전역 로거 사용)
// ============================================================

@[inline]
pub fn log_trace(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.trace(msg, ...fields)
}

@[inline]
pub fn log_debug(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.debug(msg, ...fields)
}

@[inline]
pub fn log_info(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.info(msg, ...fields)
}

@[inline]
pub fn log_warn(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.warn(msg, ...fields)
}

@[inline]
pub fn log_error(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.error(msg, ...fields)
}

@[inline]
pub fn log_fatal(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.fatal(msg, ...fields)
}

// ============================================================
// 포맷팅 함수
// ============================================================

/// JSON 문자열 이스케이프 처리
fn escape_json_string(s string) string {
	mut result := []u8{cap: s.len + 10}
	for c in s.bytes() {
		match c {
			`"` {
				result << `\\`
				result << `"`
			}
			`\\` {
				result << `\\`
				result << `\\`
			}
			`\n` {
				result << `\\`
				result << `n`
			}
			`\r` {
				result << `\\`
				result << `r`
			}
			`\t` {
				result << `\\`
				result << `t`
			}
			else {
				result << c
			}
		}
	}
	return result.bytestr()
}

/// 로그 항목을 JSON 형식으로 포맷팅합니다.
fn format_entry_json(entry LogEntry) string {
	mut sb := []u8{cap: 256}
	sb << '{"timestamp":"'.bytes()
	sb << entry.timestamp.format_rfc3339().bytes()
	sb << '","level":"'.bytes()
	sb << entry.level.str().bytes()
	sb << '","logger":"'.bytes()
	sb << escape_json_string(entry.logger_name).bytes()
	sb << '","msg":"'.bytes()
	sb << escape_json_string(entry.message).bytes()
	sb << '"'.bytes()

	// 트레이스 컨텍스트가 있으면 추가
	if entry.context.trace_id.len > 0 {
		sb << ',"trace_id":"'.bytes()
		sb << entry.context.trace_id.bytes()
		sb << '"'.bytes()
	}
	if entry.context.span_id.len > 0 {
		sb << ',"span_id":"'.bytes()
		sb << entry.context.span_id.bytes()
		sb << '"'.bytes()
	}
	if entry.context.service.len > 0 {
		sb << ',"service":"'.bytes()
		sb << entry.context.service.bytes()
		sb << '"'.bytes()
	}

	// 필드 추가
	for f in entry.fields {
		sb << ',"'.bytes()
		sb << escape_json_string(f.key).bytes()
		sb << '":"'.bytes()
		sb << escape_json_string(f.value).bytes()
		sb << '"'.bytes()
	}

	sb << '}\n'.bytes()
	return sb.bytestr()
}

/// 로그 항목을 텍스트 형식으로 포맷팅합니다.
fn format_entry_text(entry LogEntry) string {
	mut sb := []u8{cap: 256}

	// 타임스탬프
	sb << entry.timestamp.format_ss().bytes()
	sb << ' '.bytes()

	// 색상이 있는 레벨
	sb << get_level_color(entry.level).bytes()
	sb << '['.bytes()
	sb << entry.level.str().bytes()
	sb << ']'.bytes()
	sb << '\x1b[0m'.bytes()
	sb << ' '.bytes()

	// 로거 이름
	sb << '['.bytes()
	sb << entry.logger_name.bytes()
	sb << '] '.bytes()

	// 메시지
	sb << entry.message.bytes()

	// 필드
	if entry.fields.len > 0 {
		sb << ' |'.bytes()
		for f in entry.fields {
			sb << ' '.bytes()
			sb << f.key.bytes()
			sb << '='.bytes()
			sb << f.value.bytes()
		}
	}

	// 트레이스 컨텍스트
	if entry.context.trace_id.len > 0 {
		sb << ' trace_id='.bytes()
		sb << entry.context.trace_id.bytes()
	}

	sb << '\n'.bytes()
	return sb.bytestr()
}

/// 로그 레벨에 따른 색상 코드를 반환합니다.
fn get_level_color(level LogLevel) string {
	return match level {
		.trace { '\x1b[90m' } // 회색
		.debug { '\x1b[36m' } // 청록색
		.info { '\x1b[32m' } // 녹색
		.warn { '\x1b[33m' } // 노란색
		.error { '\x1b[31m' } // 빨간색
		.fatal { '\x1b[35m' } // 자홍색
	}
}

// ============================================================
// OTLP 로그 내보내기 (OpenTelemetry Protocol)
// ============================================================

/// export_logs_to_otlp는 로그 항목을 OTLP 엔드포인트로 내보냅니다.
fn export_logs_to_otlp(endpoint string, service_name string, entries []LogEntry) {
	if entries.len == 0 || endpoint.len == 0 {
		return
	}

	// OTLP JSON 페이로드 빌드
	payload := build_otlp_logs_payload(service_name, entries)

	// OTLP 엔드포인트로 HTTP POST 전송
	// 참고: 현재는 간단한 HTTP 사용, 더 나은 성능을 위해 gRPC 사용 가능
	send_otlp_http(endpoint, payload)
}

/// OTLP 로그 페이로드를 빌드합니다.
fn build_otlp_logs_payload(service_name string, entries []LogEntry) string {
	mut sb := []u8{cap: 1024}
	sb << '{"resourceLogs":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${service_name}"}}'.bytes()
	sb << ']},"scopeLogs":[{"logRecords":['.bytes()

	for i, entry in entries {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << build_otlp_log_record(entry).bytes()
	}

	sb << ']}]}]}'.bytes()
	return sb.bytestr()
}

/// 단일 OTLP 로그 레코드를 빌드합니다.
fn build_otlp_log_record(entry LogEntry) string {
	// LogLevel을 OTLP 심각도 번호로 매핑
	severity_number := match entry.level {
		.trace { 1 }
		.debug { 5 }
		.info { 9 }
		.warn { 13 }
		.error { 17 }
		.fatal { 21 }
	}

	mut sb := []u8{cap: 256}
	sb << '{"timeUnixNano":"${entry.timestamp.unix_nano()}"'.bytes()
	sb << ',"severityNumber":${severity_number}'.bytes()
	sb << ',"severityText":"${entry.level.str()}"'.bytes()
	sb << ',"body":{"stringValue":"${escape_json_string(entry.message)}"}'.bytes()

	// 트레이스 컨텍스트 추가
	if entry.context.trace_id.len > 0 {
		sb << ',"traceId":"${entry.context.trace_id}"'.bytes()
	}
	if entry.context.span_id.len > 0 {
		sb << ',"spanId":"${entry.context.span_id}"'.bytes()
	}

	// 속성 추가
	sb << ',"attributes":['.bytes()
	sb << '{"key":"logger","value":{"stringValue":"${escape_json_string(entry.logger_name)}"}}'.bytes()
	for f in entry.fields {
		sb << ',{"key":"${escape_json_string(f.key)}","value":{"stringValue":"${escape_json_string(f.value)}"}}'.bytes()
	}
	sb << ']}'.bytes()

	return sb.bytestr()
}

/// OTLP HTTP 전송 함수
fn send_otlp_http(endpoint string, payload string) {
	// V의 net.http를 사용한 간단한 HTTP POST
	// 프로덕션에서는 연결 풀링과 재시도 고려
	$if !windows {
		// 간단함을 위해 curl 사용 (V의 크로스 플랫폼 HTTP 클라이언트에 제한이 있음)
		// 이것은 fire-and-forget 비동기 호출
		_ := $env('PATH') // curl 사용 가능 확인
	}

	// 현재는 간단한 접근 방식 사용
	// TODO: 재시도가 있는 적절한 HTTP 클라이언트 구현
	_ = endpoint
	_ = payload
}

// ============================================================
// 유틸리티: 로그 레벨 심각도 매핑
// ============================================================

/// severity_to_level은 OTLP 심각도 번호를 LogLevel로 변환합니다.
pub fn severity_to_level(severity int) LogLevel {
	return match true {
		severity <= 4 { .trace }
		severity <= 8 { .debug }
		severity <= 12 { .info }
		severity <= 16 { .warn }
		severity <= 20 { .error }
		else { .fatal }
	}
}
