// Infra Layer - Distributed Tracing (OpenTelemetry Compatible)
// Span creation and trace context propagation
module observability

import time
import rand
import sync

// SpanKind indicates the type of span
pub enum SpanKind {
	internal // Default, internal operation
	server   // Server-side operation handling a request
	client   // Client-side operation making a request
	producer // Producing messages (e.g., Kafka producer)
	consumer // Consuming messages (e.g., Kafka consumer)
}

// SpanStatus represents the outcome of a span
pub enum SpanStatus {
	unset
	ok
	error
}

// SpanContext contains identifying trace information
pub struct SpanContext {
pub:
	trace_id    string // 32-char hex (128-bit)
	span_id     string // 16-char hex (64-bit)
	parent_id   string // Parent span ID (empty if root)
	trace_flags u8     // Sampling flags
	trace_state string // W3C trace state
}

// is_valid checks if the context is valid
pub fn (c SpanContext) is_valid() bool {
	return c.trace_id.len == 32 && c.span_id.len == 16
}

// is_sampled checks if the span should be recorded
pub fn (c SpanContext) is_sampled() bool {
	return (c.trace_flags & 0x01) != 0
}

// SpanAttribute represents a key-value attribute
pub struct SpanAttribute {
pub:
	key   string
	value SpanValue
}

// SpanValue holds attribute values
pub type SpanValue = bool | f64 | i64 | string | []string

// Attribute constructors
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

// SpanEvent represents a timed event within a span
pub struct SpanEvent {
pub:
	name       string
	timestamp  time.Time
	attributes []SpanAttribute
}

// SpanLink represents a link to another span
pub struct SpanLink {
pub:
	context    SpanContext
	attributes []SpanAttribute
}

// Span represents a single operation within a trace
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

// set_attribute adds or updates an attribute
pub fn (mut s Span) set_attribute(attr SpanAttribute) {
	if s.ended {
		return
	}
	// Replace if exists, otherwise append
	for i, a in s.attributes {
		if a.key == attr.key {
			s.attributes[i] = attr
			return
		}
	}
	s.attributes << attr
}

// set_attributes adds multiple attributes
pub fn (mut s Span) set_attributes(attrs ...SpanAttribute) {
	for attr in attrs {
		s.set_attribute(attr)
	}
}

// add_event adds a timed event
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

// set_status sets the span status
pub fn (mut s Span) set_status(status SpanStatus, msg string) {
	if s.ended {
		return
	}
	s.status = status
	s.status_msg = msg
}

// record_error records an error and sets status
pub fn (mut s Span) record_error(err IError) {
	if s.ended {
		return
	}
	s.add_event('exception', attr_string('exception.type', 'error'), attr_string('exception.message',
		err.str()))
	s.set_status(.error, err.str())
}

// end ends the span
pub fn (mut s Span) end() {
	if s.ended {
		return
	}
	s.end_time = time.now()
	s.ended = true
}

// duration returns the span duration
pub fn (s Span) duration() time.Duration {
	if s.ended {
		return s.end_time - s.start_time
	}
	return time.now() - s.start_time
}

// ============================================================
// Tracer
// ============================================================

// TracerConfig holds tracer configuration
pub struct TracerConfig {
pub:
	service_name    string  = 'datacore'
	service_version string  = '0.1.0'
	environment     string  = 'development'
	sampler         Sampler = AlwaysOnSampler{}
}

// Tracer creates spans for tracing operations
pub struct Tracer {
pub:
	config TracerConfig
mut:
	spans []&Span
	lock  sync.Mutex
}

// new_tracer creates a new tracer
pub fn new_tracer(config TracerConfig) &Tracer {
	return &Tracer{
		config: config
		spans:  []
	}
}

// new_default_tracer creates a tracer with default settings
pub fn new_default_tracer() &Tracer {
	return new_tracer(TracerConfig{})
}

// start_span creates and starts a new span
pub fn (mut t Tracer) start_span(name string, opts ...SpanOption) &Span {
	mut span_opts := SpanOptions{}
	for opt in opts {
		opt.apply(mut span_opts)
	}

	// Generate or derive context
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

// end_span ends a span and potentially exports it
pub fn (mut t Tracer) end_span(mut span Span) {
	span.end()
}

// ============================================================
// Span Options
// ============================================================

struct SpanOptions {
mut:
	parent_ctx SpanContext
	kind       SpanKind  = .internal
	start_time time.Time = time.now()
	attributes []SpanAttribute
	links      []SpanLink
}

// SpanOption is an interface for span configuration
pub interface SpanOption {
	apply(mut opts SpanOptions)
}

// WithParent sets the parent span context
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

// WithKind sets the span kind
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

// WithAttributes sets initial attributes
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

// WithLinks sets span links
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
// Sampler
// ============================================================

// Sampler determines if a span should be recorded
pub interface Sampler {
	should_sample() bool
}

// AlwaysOnSampler samples all spans
pub struct AlwaysOnSampler {}

pub fn (s AlwaysOnSampler) should_sample() bool {
	return true
}

// AlwaysOffSampler samples no spans
pub struct AlwaysOffSampler {}

pub fn (s AlwaysOffSampler) should_sample() bool {
	return false
}

// RatioSampler samples based on a ratio
pub struct RatioSampler {
	ratio f64 // 0.0 to 1.0
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
// ID Generation
// ============================================================

fn generate_trace_id() string {
	mut id := []u8{len: 16}
	for i in 0 .. 16 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

fn generate_span_id() string {
	mut id := []u8{len: 8}
	for i in 0 .. 8 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

// ============================================================
// Context Propagation (W3C Trace Context)
// ============================================================

// extract_context extracts trace context from headers
pub fn extract_context(headers map[string]string) SpanContext {
	traceparent := headers['traceparent'] or { return SpanContext{} }

	// Format: {version}-{trace-id}-{parent-id}-{trace-flags}
	// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
	parts := traceparent.split('-')
	if parts.len != 4 {
		return SpanContext{}
	}

	if parts[0] != '00' {
		return SpanContext{} // Unsupported version
	}

	trace_flags := u8(parts[3].parse_uint(16, 8) or { 0 })

	return SpanContext{
		trace_id:    parts[1]
		span_id:     parts[2]
		trace_flags: trace_flags
		trace_state: headers['tracestate'] or { '' }
	}
}

// inject_context injects trace context into headers
pub fn inject_context(ctx SpanContext, mut headers map[string]string) {
	if !ctx.is_valid() {
		return
	}

	// W3C Trace Context format
	traceparent := '00-${ctx.trace_id}-${ctx.span_id}-${ctx.trace_flags:02x}'
	headers['traceparent'] = traceparent

	if ctx.trace_state.len > 0 {
		headers['tracestate'] = ctx.trace_state
	}
}

// ============================================================
// Span Export (OTLP-like JSON format)
// ============================================================

// export_span_json exports a span as JSON
pub fn export_span_json(span &Span, tracer &Tracer) string {
	mut sb := []u8{}
	sb << '{"resourceSpans":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${tracer.config.service_name}"}}'.bytes()
	sb << ']},"scopeSpans":[{"spans":['.bytes()

	// Span
	sb << '{"traceId":"${span.context.trace_id}"'.bytes()
	sb << ',"spanId":"${span.context.span_id}"'.bytes()
	if span.context.parent_id.len > 0 {
		sb << ',"parentSpanId":"${span.context.parent_id}"'.bytes()
	}
	sb << ',"name":"${span.name}"'.bytes()
	sb << ',"kind":${int(span.kind) + 1}'.bytes() // OTLP uses 1-indexed
	sb << ',"startTimeUnixNano":${span.start_time.unix_nano()}'.bytes()
	sb << ',"endTimeUnixNano":${span.end_time.unix_nano()}'.bytes()

	// Attributes
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

	// Events
	sb << ',"events":['.bytes()
	for i, event in span.events {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << '{"name":"${event.name}","timeUnixNano":${event.timestamp.unix_nano()}}'.bytes()
	}
	sb << ']'.bytes()

	// Status
	sb << ',"status":{"code":${int(span.status)}'.bytes()
	if span.status_msg.len > 0 {
		sb << ',"message":"${span.status_msg}"'.bytes()
	}
	sb << '}'.bytes()

	sb << '}]}]}]}'.bytes()

	return sb.bytestr()
}

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
// Convenience Functions
// ============================================================

// trace_operation traces a synchronous operation
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
