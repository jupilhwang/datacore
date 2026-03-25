/// Infrastructure layer - Distributed tracing (OpenTelemetry compatible)
/// Span creation and trace context propagation
module observability

import time
import rand
import sync

/// SpanKind represents the type of a span.
pub enum SpanKind {
	internal
	server
	client
	producer
	consumer
}

/// SpanStatus represents the result of a span.
pub enum SpanStatus {
	unset
	ok
	error
}

/// SpanContext contains trace identification information.
pub struct SpanContext {
pub:
	trace_id    string
	span_id     string
	parent_id   string
	trace_flags u8
	trace_state string
}

/// is_valid checks whether the context is valid.
fn (c SpanContext) is_valid() bool {
	return c.trace_id.len == 32 && c.span_id.len == 16
}

/// is_sampled checks whether the span should be recorded.
fn (c SpanContext) is_sampled() bool {
	return (c.trace_flags & 0x01) != 0
}

/// SpanAttribute represents a key-value attribute.
pub struct SpanAttribute {
pub:
	key   string
	value SpanValue
}

/// SpanValue holds an attribute value.
pub type SpanValue = bool | f64 | i64 | string | []string

/// Attribute constructors
fn attr_string(key string, value string) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

/// attr_int creates an integer span attribute.
fn attr_int(key string, value i64) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

/// attr_float creates a float span attribute.
fn attr_float(key string, value f64) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

/// attr_bool creates a boolean span attribute.
fn attr_bool(key string, value bool) SpanAttribute {
	return SpanAttribute{
		key:   key
		value: SpanValue(value)
	}
}

/// SpanEvent represents a timestamped event within a span.
pub struct SpanEvent {
pub:
	name       string
	timestamp  time.Time
	attributes []SpanAttribute
}

/// SpanLink represents a link to another span.
pub struct SpanLink {
pub:
	context    SpanContext
	attributes []SpanAttribute
}

/// Span represents a single operation within a trace.
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

/// set_attribute adds or updates an attribute.
fn (mut s Span) set_attribute(attr SpanAttribute) {
	if s.ended {
		return
	}
	// Replace if exists, otherwise add
	for i, a in s.attributes {
		if a.key == attr.key {
			s.attributes[i] = attr
			return
		}
	}
	s.attributes << attr
}

/// set_attributes adds multiple attributes.
fn (mut s Span) set_attributes(attrs ...SpanAttribute) {
	for attr in attrs {
		s.set_attribute(attr)
	}
}

/// add_event adds a timestamped event.
fn (mut s Span) add_event(name string, attrs ...SpanAttribute) {
	if s.ended {
		return
	}
	s.events << SpanEvent{
		name:       name
		timestamp:  time.now()
		attributes: attrs
	}
}

/// set_status sets the span status.
fn (mut s Span) set_status(status SpanStatus, msg string) {
	if s.ended {
		return
	}
	s.status = status
	s.status_msg = msg
}

/// record_error records an error and sets the status.
fn (mut s Span) record_error(err IError) {
	if s.ended {
		return
	}
	s.add_event('exception', attr_string('exception.type', 'error'), attr_string('exception.message',
		err.str()))
	s.set_status(.error, err.str())
}

/// end finalizes the span.
fn (mut s Span) end() {
	if s.ended {
		return
	}
	s.end_time = time.now()
	s.ended = true
}

/// duration returns the span duration.
fn (s Span) duration() time.Duration {
	if s.ended {
		return s.end_time - s.start_time
	}
	return time.now() - s.start_time
}

// Tracer

/// TracerConfig holds tracer configuration.
pub struct TracerConfig {
pub:
	service_name    string  = 'datacore'
	service_version string  = '0.1.0'
	environment     string  = 'development'
	sampler         Sampler = AlwaysOnSampler{}
}

/// Tracer creates spans for tracing operations.
pub struct Tracer {
pub:
	config TracerConfig
mut:
	spans []&Span
	lock  sync.Mutex
}

/// new_tracer creates a new tracer.
fn new_tracer(config TracerConfig) &Tracer {
	return &Tracer{
		config: config
		spans:  []
	}
}

/// new_default_tracer creates a tracer with default configuration.
fn new_default_tracer() &Tracer {
	return new_tracer(TracerConfig{})
}

/// start_span creates and starts a new span.
fn (mut t Tracer) start_span(name string, opts ...SpanOption) &Span {
	mut span_opts := SpanOptions{}
	for opt in opts {
		opt.apply(mut span_opts)
	}

	// Create or derive context
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

/// end_span finalizes the span and potentially exports it.
fn (mut t Tracer) end_span(mut span Span) {
	span.end()
}

// Span options

struct SpanOptions {
mut:
	parent_ctx SpanContext
	kind       SpanKind  = .internal
	start_time time.Time = time.now()
	attributes []SpanAttribute
	links      []SpanLink
}

/// SpanOption is an interface for configuring spans.
pub interface SpanOption {
	apply(mut opts SpanOptions)
}

/// WithParent sets the parent span context.
struct WithParent {
	ctx SpanContext
}

/// with_parent creates a SpanOption that sets the parent context.
fn with_parent(ctx SpanContext) SpanOption {
	return WithParent{
		ctx: ctx
	}
}

fn (w WithParent) apply(mut opts SpanOptions) {
	opts.parent_ctx = w.ctx
}

/// WithKind sets the span kind.
struct WithKind {
	kind SpanKind
}

/// with_kind creates a SpanOption that sets the span kind.
fn with_kind(kind SpanKind) SpanOption {
	return WithKind{
		kind: kind
	}
}

fn (w WithKind) apply(mut opts SpanOptions) {
	opts.kind = w.kind
}

/// WithAttributes sets initial attributes.
struct WithAttributes {
	attrs []SpanAttribute
}

/// with_attributes creates a SpanOption that sets initial attributes.
fn with_attributes(attrs ...SpanAttribute) SpanOption {
	return WithAttributes{
		attrs: attrs
	}
}

fn (w WithAttributes) apply(mut opts SpanOptions) {
	opts.attributes = w.attrs
}

/// WithLinks sets span links.
struct WithLinks {
	links []SpanLink
}

/// with_links creates a SpanOption that sets span links.
fn with_links(links ...SpanLink) SpanOption {
	return WithLinks{
		links: links
	}
}

fn (w WithLinks) apply(mut opts SpanOptions) {
	opts.links = w.links
}

// Samplers

/// Sampler decides whether a span should be recorded.
pub interface Sampler {
	should_sample() bool
}

/// AlwaysOnSampler samples all spans.
pub struct AlwaysOnSampler {}

/// should_sample returns whether to sample the span.
fn (s AlwaysOnSampler) should_sample() bool {
	return true
}

/// AlwaysOffSampler never samples spans.
pub struct AlwaysOffSampler {}

/// should_sample returns whether to sample the span.
fn (s AlwaysOffSampler) should_sample() bool {
	return false
}

/// RatioSampler samples spans according to a ratio.
pub struct RatioSampler {
	ratio f64
}

/// new_ratio_sampler creates a RatioSampler with the given ratio.
fn new_ratio_sampler(ratio f64) RatioSampler {
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

/// should_sample returns whether to sample the span.
fn (s RatioSampler) should_sample() bool {
	return rand.f64() < s.ratio
}

// ID generation

/// Generates a trace ID.
fn generate_trace_id() string {
	mut id := []u8{len: 16}
	for i in 0 .. 16 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

/// Generates a span ID.
fn generate_span_id() string {
	mut id := []u8{len: 8}
	for i in 0 .. 8 {
		id[i] = u8(rand.intn(256) or { 0 })
	}
	return id.hex()
}

// Context propagation (W3C Trace Context)

/// extract_context extracts the trace context from headers.
fn extract_context(headers map[string]string) SpanContext {
	traceparent := headers['traceparent'] or { return SpanContext{} }

	// Format: {version}-{trace-id}-{parent-id}-{trace-flags}
	// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
	parts := traceparent.split('-')
	if parts.len != 4 {
		return SpanContext{}
	}

	if parts[0] != '00' {
		return SpanContext{}
	}

	trace_flags := u8(parts[3].parse_uint(16, 8) or { 0 })

	return SpanContext{
		trace_id:    parts[1]
		span_id:     parts[2]
		trace_flags: trace_flags
		trace_state: headers['tracestate'] or { '' }
	}
}

/// inject_context injects the trace context into headers.
fn inject_context(ctx SpanContext, mut headers map[string]string) {
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

// Span export (OTLP-like JSON format)

/// export_span_json exports a span as JSON.
fn export_span_json(span &Span, tracer &Tracer) string {
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
	sb << ',"kind":${int(span.kind) + 1}'.bytes()
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

	sb << ',"status":{"code":${int(span.status)}'.bytes()
	if span.status_msg.len > 0 {
		sb << ',"message":"${span.status_msg}"'.bytes()
	}
	sb << '}'.bytes()

	sb << '}]}]}]}'.bytes()

	return sb.bytestr()
}

/// Formats a span value.
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

// Convenience functions

/// trace_operation traces a synchronous operation.
fn trace_operation[T](mut tracer Tracer, name string, op fn () !T) !T {
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
