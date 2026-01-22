/// 인프라 레이어 - 분산 트레이싱 (OpenTelemetry 호환)
/// 스팬 생성 및 트레이스 컨텍스트 전파
module observability

import time
import rand
import sync

/// SpanKind는 스팬의 유형을 나타냅니다.
pub enum SpanKind {
	internal // 기본값, 내부 작업
	server   // 요청을 처리하는 서버 측 작업
	client   // 요청을 보내는 클라이언트 측 작업
	producer // 메시지 생산 (예: Kafka 프로듀서)
	consumer // 메시지 소비 (예: Kafka 컨슈머)
}

/// SpanStatus는 스팬의 결과를 나타냅니다.
pub enum SpanStatus {
	unset
	ok
	error
}

/// SpanContext는 트레이스 식별 정보를 포함합니다.
pub struct SpanContext {
pub:
	trace_id    string // 32자 16진수 (128비트)
	span_id     string // 16자 16진수 (64비트)
	parent_id   string // 부모 스팬 ID (루트인 경우 비어 있음)
	trace_flags u8     // 샘플링 플래그
	trace_state string // W3C 트레이스 상태
}

/// is_valid는 컨텍스트가 유효한지 확인합니다.
pub fn (c SpanContext) is_valid() bool {
	return c.trace_id.len == 32 && c.span_id.len == 16
}

/// is_sampled는 스팬이 기록되어야 하는지 확인합니다.
pub fn (c SpanContext) is_sampled() bool {
	return (c.trace_flags & 0x01) != 0
}

/// SpanAttribute는 키-값 속성을 나타냅니다.
pub struct SpanAttribute {
pub:
	key   string
	value SpanValue
}

/// SpanValue는 속성 값을 보유합니다.
pub type SpanValue = bool | f64 | i64 | string | []string

/// 속성 생성자들
pub fn attr_string(key string, value string) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

pub fn attr_int(key string, value i64) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

pub fn attr_float(key string, value f64) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

pub fn attr_bool(key string, value bool) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

/// SpanEvent는 스팬 내의 시간 기록 이벤트를 나타냅니다.
pub struct SpanEvent {
pub:
	name       string
	timestamp  time.Time
	attributes []SpanAttribute
}

/// SpanLink는 다른 스팬에 대한 링크를 나타냅니다.
pub struct SpanLink {
pub:
	context    SpanContext
	attributes []SpanAttribute
}

/// Span은 트레이스 내의 단일 작업을 나타냅니다.
pub struct Span {
pub:
	name       string
	context    SpanContext
	kind       SpanKind
	start_time time.Time
pub mut:
	end_time   time.Time
	status     SpanStatus
	status_msg string
	attributes []SpanAttribute
	events     []SpanEvent
	links      []SpanLink
	ended      bool
}

/// set_attribute는 속성을 추가하거나 업데이트합니다.
pub fn (mut s Span) set_attribute(attr SpanAttribute) {
	if s.ended {
		return
	}
	// 존재하면 교체, 그렇지 않으면 추가
	for i, a in s.attributes {
		if a.key == attr.key {
			s.attributes[i] = attr
			return
		}
	}
	s.attributes << attr
}

/// set_attributes는 여러 속성을 추가합니다.
pub fn (mut s Span) set_attributes(attrs ...SpanAttribute) {
	for attr in attrs {
		s.set_attribute(attr)
	}
}

/// add_event는 시간 기록 이벤트를 추가합니다.
pub fn (mut s Span) add_event(name string, attrs ...SpanAttribute) {
	if s.ended {
		return
	}
	s.events << SpanEvent{
		name:       name
		timestamp:  time.now()
		attributes: attrs
	}
}

/// set_status는 스팬 상태를 설정합니다.
pub fn (mut s Span) set_status(status SpanStatus, msg string) {
	if s.ended {
		return
	}
	s.status = status
	s.status_msg = msg
}

/// record_error는 에러를 기록하고 상태를 설정합니다.
pub fn (mut s Span) record_error(err IError) {
	if s.ended {
		return
	}
	s.add_event('exception', attr_string('exception.type', 'error'), attr_string('exception.message',
		err.str()))
	s.set_status(.error, err.str())
}

/// end는 스팬을 종료합니다.
pub fn (mut s Span) end() {
	if s.ended {
		return
	}
	s.end_time = time.now()
	s.ended = true
}

/// duration은 스팬 지속 시간을 반환합니다.
pub fn (s Span) duration() time.Duration {
	if s.ended {
		return s.end_time - s.start_time
	}
	return time.now() - s.start_time
}

// ============================================================
// 트레이서
// ============================================================

/// TracerConfig는 트레이서 설정을 보유합니다.
pub struct TracerConfig {
pub:
	service_name    string  = 'datacore'
	service_version string  = '0.1.0'
	environment     string  = 'development'
	sampler         Sampler = AlwaysOnSampler{}
}

/// Tracer는 트레이싱 작업을 위한 스팬을 생성합니다.
pub struct Tracer {
pub:
	config TracerConfig
mut:
	spans []&Span
	lock  sync.Mutex
}

/// new_tracer는 새 트레이서를 생성합니다.
pub fn new_tracer(config TracerConfig) &Tracer {
	return &Tracer{
		config: config
		spans:  []
	}
}

/// new_default_tracer는 기본 설정으로 트레이서를 생성합니다.
pub fn new_default_tracer() &Tracer {
	return new_tracer(TracerConfig{})
}

/// start_span은 새 스팬을 생성하고 시작합니다.
pub fn (mut t Tracer) start_span(name string, opts ...SpanOption) &Span {
	mut span_opts := SpanOptions{}
	for opt in opts {
		opt.apply(mut span_opts)
	}

	// 컨텍스트 생성 또는 파생
	mut ctx := SpanContext{}
	if span_opts.parent_ctx.is_valid() {
		ctx = SpanContext{
			trace_id:    span_opts.parent_ctx.trace_id
			span_id:     generate_span_id()
			parent_id:   span_opts.parent_ctx.span_id
			trace_flags: span_opts.parent_ctx.trace_flags
		}
	} else {
		ctx = SpanContext{
			trace_id:    generate_trace_id()
			span_id:     generate_span_id()
			trace_flags: if t.config.sampler.should_sample() { u8(0x01) } else { u8(0x00) }
		}
	}

	span := &Span{
		name:       name
		context:    ctx
		kind:       span_opts.kind
		start_time: span_opts.start_time
		attributes: span_opts.attributes
		links:      span_opts.links
	}

	t.lock.@lock()
	t.spans << span
	t.lock.unlock()

	return span
}

/// end_span은 스팬을 종료하고 잠재적으로 내보냅니다.
pub fn (mut t Tracer) end_span(mut span Span) {
	span.end()
}

// ============================================================
// 스팬 옵션
// ============================================================

struct SpanOptions {
mut:
	parent_ctx SpanContext
	kind       SpanKind  = .internal
	start_time time.Time = time.now()
	attributes []SpanAttribute
	links      []SpanLink
}

/// SpanOption은 스팬 설정을 위한 인터페이스입니다.
pub interface SpanOption {
	apply(mut opts SpanOptions)
}

/// WithParent는 부모 스팬 컨텍스트를 설정합니다.
struct WithParent {
	ctx SpanContext
}

pub fn with_parent(ctx SpanContext) SpanOption {
	return WithParent{
		ctx: ctx
	}
}

fn (w WithParent) apply(mut opts SpanOptions) {
	opts.parent_ctx = w.ctx
}

/// WithKind는 스팬 종류를 설정합니다.
struct WithKind {
	kind SpanKind
}

pub fn with_kind(kind SpanKind) SpanOption {
	return WithKind{
		kind: kind
	}
}

fn (w WithKind) apply(mut opts SpanOptions) {
	opts.kind = w.kind
}

/// WithAttributes는 초기 속성을 설정합니다.
struct WithAttributes {
	attrs []SpanAttribute
}

pub fn with_attributes(attrs ...SpanAttribute) SpanOption {
	return WithAttributes{
		attrs: attrs
	}
}

fn (w WithAttributes) apply(mut opts SpanOptions) {
	opts.attributes = w.attrs
}

/// WithLinks는 스팬 링크를 설정합니다.
struct WithLinks {
	links []SpanLink
}

pub fn with_links(links ...SpanLink) SpanOption {
	return WithLinks{
		links: links
	}
}

fn (w WithLinks) apply(mut opts SpanOptions) {
	opts.links = w.links
}

// ============================================================
// 샘플러
// ============================================================

/// Sampler는 스팬이 기록되어야 하는지 결정합니다.
pub interface Sampler {
	should_sample() bool
}

/// AlwaysOnSampler는 모든 스팬을 샘플링합니다.
pub struct AlwaysOnSampler {}

pub fn (s AlwaysOnSampler) should_sample() bool {
	return true
}

/// AlwaysOffSampler는 스팬을 샘플링하지 않습니다.
pub struct AlwaysOffSampler {}

pub fn (s AlwaysOffSampler) should_sample() bool {
	return false
}

/// RatioSampler는 비율에 따라 샘플링합니다.
pub struct RatioSampler {
	ratio f64 // 0.0에서 1.0
}

pub fn new_ratio_sampler(ratio f64) RatioSampler {
	return RatioSampler{
		ratio: if ratio < 0 {
			0
		} else if ratio > 1 {
			1
		} else {
			ratio
		}
	}
}

pub fn (s RatioSampler) should_sample() bool {
	return rand.f64() < s.ratio
}

// ============================================================
// ID 생성
// ============================================================

/// 트레이스 ID를 생성합니다.
fn generate_trace_id() string {
	mut id := []u8{len: 16}
	for i in 0 .. 16 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

/// 스팬 ID를 생성합니다.
fn generate_span_id() string {
	mut id := []u8{len: 8}
	for i in 0 .. 8 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

// ============================================================
// 컨텍스트 전파 (W3C Trace Context)
// ============================================================

/// extract_context는 헤더에서 트레이스 컨텍스트를 추출합니다.
pub fn extract_context(headers map[string]string) SpanContext {
	traceparent := headers['traceparent'] or { return SpanContext{} }

	// 형식: {version}-{trace-id}-{parent-id}-{trace-flags}
	// 예: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
	parts := traceparent.split('-')
	if parts.len != 4 {
		return SpanContext{}
	}

	if parts[0] != '00' {
		return SpanContext{} // 지원되지 않는 버전
	}

	trace_flags := u8(parts[3].parse_uint(16, 8) or { 0 })

	return SpanContext{
		trace_id:    parts[1]
		span_id:     parts[2]
		trace_flags: trace_flags
		trace_state: headers['tracestate'] or { '' }
	}
}

/// inject_context는 트레이스 컨텍스트를 헤더에 주입합니다.
pub fn inject_context(ctx SpanContext, mut headers map[string]string) {
	if !ctx.is_valid() {
		return
	}

	// W3C Trace Context 형식
	traceparent := '00-${ctx.trace_id}-${ctx.span_id}-${ctx.trace_flags:02x}'
	headers['traceparent'] = traceparent

	if ctx.trace_state.len > 0 {
		headers['tracestate'] = ctx.trace_state
	}
}

// ============================================================
// 스팬 내보내기 (OTLP 유사 JSON 형식)
// ============================================================

/// export_span_json은 스팬을 JSON으로 내보냅니다.
pub fn export_span_json(span &Span, tracer &Tracer) string {
	mut sb := []u8{}
	sb << '{"resourceSpans":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${tracer.config.service_name}"}}'.bytes()
	sb << ']},"scopeSpans":[{"spans":['.bytes()

	// 스팬
	sb << '{"traceId":"${span.context.trace_id}"'.bytes()
	sb << ',"spanId":"${span.context.span_id}"'.bytes()
	if span.context.parent_id.len > 0 {
		sb << ',"parentSpanId":"${span.context.parent_id}"'.bytes()
	}
	sb << ',"name":"${span.name}"'.bytes()
	sb << ',"kind":${int(span.kind) + 1}'.bytes() // OTLP는 1부터 시작하는 인덱스 사용
	sb << ',"startTimeUnixNano":${span.start_time.unix_nano()}'.bytes()
	sb << ',"endTimeUnixNano":${span.end_time.unix_nano()}'.bytes()

	// 속성
	sb << ',"attributes":['.bytes()
	for i, attr in span.attributes {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << '{"key":"${attr.key}","value":'.bytes()
		sb << format_span_value(attr.value).bytes()
		sb << '}'.bytes()
	}
	sb << ']'.bytes()

	// 이벤트
	sb << ',"events":['.bytes()
	for i, event in span.events {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << '{"name":"${event.name}","timeUnixNano":${event.timestamp.unix_nano()}}'.bytes()
	}
	sb << ']'.bytes()

	// 상태
	sb << ',"status":{"code":${int(span.status)}'.bytes()
	if span.status_msg.len > 0 {
		sb << ',"message":"${span.status_msg}"'.bytes()
	}
	sb << '}'.bytes()

	sb << '}]}]}]}'.bytes()

	return sb.bytestr()
}

/// 스팬 값을 포맷팅합니다.
fn format_span_value(v SpanValue) string {
	return match v {
		string {
			'{"stringValue":"${v}"}'
		}
		i64 {
			'{"intValue":"${v}"}'
		}
		f64 {
			'{"doubleValue":${v}}'
		}
		bool {
			'{"boolValue":${v}}'
		}
		[]string {
			mut parts := []string{}
			for s in v {
				parts << '{"stringValue":"${s}"}'
			}
			'{"arrayValue":{"values":[${parts.join(',')}]}}'
		}
	}
}

// ============================================================
// 편의 함수
// ============================================================

/// trace_operation은 동기 작업을 트레이싱합니다.
pub fn trace_operation[T](mut tracer Tracer, name string, op fn () !T) !T {
	mut span := tracer.start_span(name)
	defer {
		span.end()
	}

	result := op() or {
		span.record_error(err)
		return err
	}

	span.set_status(.ok, '')
	return result
}
