// Unit Tests - Distributed Tracing
module observability

import time

fn test_generate_trace_id() {
	id := generate_trace_id()
	assert id.len == 32 // 16 bytes = 32 hex chars
}

fn test_generate_span_id() {
	id := generate_span_id()
	assert id.len == 16 // 8 bytes = 16 hex chars
}

fn test_span_context_is_valid() {
	valid := SpanContext{
		trace_id: 'a'.repeat(32)
		span_id:  'b'.repeat(16)
	}
	assert valid.is_valid() == true

	invalid1 := SpanContext{
		trace_id: 'a'.repeat(31) // Too short
		span_id:  'b'.repeat(16)
	}
	assert invalid1.is_valid() == false

	invalid2 := SpanContext{
		trace_id: 'a'.repeat(32)
		span_id:  'b'.repeat(15) // Too short
	}
	assert invalid2.is_valid() == false
}

fn test_span_context_is_sampled() {
	sampled := SpanContext{
		trace_id:    'a'.repeat(32)
		span_id:     'b'.repeat(16)
		trace_flags: 0x01
	}
	assert sampled.is_sampled() == true

	not_sampled := SpanContext{
		trace_id:    'a'.repeat(32)
		span_id:     'b'.repeat(16)
		trace_flags: 0x00
	}
	assert not_sampled.is_sampled() == false
}

fn test_tracer_start_span() {
	mut tracer := new_default_tracer()

	mut span := tracer.start_span('test-operation')

	assert span.name == 'test-operation'
	assert span.context.is_valid()
	assert span.kind == .internal
	assert span.ended == false
}

fn test_span_with_parent() {
	mut tracer := new_default_tracer()

	parent_ctx := SpanContext{
		trace_id:    'a'.repeat(32)
		span_id:     'b'.repeat(16)
		trace_flags: 0x01
	}

	mut child := tracer.start_span('child-span', with_parent(parent_ctx))

	// Should inherit trace_id from parent
	assert child.context.trace_id == parent_ctx.trace_id
	// Should have parent's span_id as parent_id
	assert child.context.parent_id == parent_ctx.span_id
	// Should have its own span_id
	assert child.context.span_id != parent_ctx.span_id
}

fn test_span_with_kind() {
	mut tracer := new_default_tracer()

	mut server_span := tracer.start_span('handle-request', with_kind(.server))
	assert server_span.kind == .server

	mut client_span := tracer.start_span('call-service', with_kind(.client))
	assert client_span.kind == .client
}

fn test_span_set_attribute() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	span.set_attribute(attr_string('http.method', 'GET'))
	span.set_attribute(attr_int('http.status_code', 200))
	span.set_attribute(attr_bool('error', false))

	assert span.attributes.len == 3
	assert span.attributes[0].key == 'http.method'
	assert span.attributes[1].key == 'http.status_code'
}

fn test_span_set_attributes() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	span.set_attributes(attr_string('key1', 'val1'), attr_string('key2', 'val2'), attr_int('key3',
		3))

	assert span.attributes.len == 3
}

fn test_span_add_event() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	span.add_event('request_received')
	span.add_event('response_sent', attr_int('bytes', 1024))

	assert span.events.len == 2
	assert span.events[0].name == 'request_received'
	assert span.events[1].name == 'response_sent'
}

fn test_span_set_status() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	span.set_status(.ok, 'success')

	assert span.status == .ok
	assert span.status_msg == 'success'
}

fn test_span_end() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	time.sleep(10 * time.millisecond)
	span.end()

	assert span.ended == true
	assert span.end_time > span.start_time
}

fn test_span_duration() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	time.sleep(50 * time.millisecond)
	span.end()

	dur := span.duration()
	assert dur >= 50 * time.millisecond
	assert dur < 100 * time.millisecond
}

fn test_span_immutable_after_end() {
	mut tracer := new_default_tracer()
	mut span := tracer.start_span('test')

	span.end()

	// These should be no-ops after end
	span.set_attribute(attr_string('key', 'value'))
	span.add_event('should_not_add')
	span.set_status(.error, 'should not set')

	assert span.attributes.len == 0
	assert span.events.len == 0
	assert span.status != .error
}

fn test_always_on_sampler() {
	s := AlwaysOnSampler{}

	// Should always return true
	for _ in 0 .. 100 {
		assert s.should_sample() == true
	}
}

fn test_always_off_sampler() {
	s := AlwaysOffSampler{}

	// Should always return false
	for _ in 0 .. 100 {
		assert s.should_sample() == false
	}
}

fn test_ratio_sampler() {
	s := new_ratio_sampler(1.0)
	assert s.should_sample() == true

	s2 := new_ratio_sampler(0.0)
	assert s2.should_sample() == false
}

fn test_extract_context() {
	headers := {
		'traceparent': '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
		'tracestate':  'vendor=value'
	}

	ctx := extract_context(headers)

	assert ctx.trace_id == '0af7651916cd43dd8448eb211c80319c'
	assert ctx.span_id == 'b7ad6b7169203331'
	assert ctx.trace_flags == 1
	assert ctx.trace_state == 'vendor=value'
}

fn test_extract_context_invalid() {
	// Invalid format
	headers := {
		'traceparent': 'invalid-format'
	}
	ctx := extract_context(headers)
	assert ctx.is_valid() == false

	// Missing header
	empty_headers := map[string]string{}
	ctx2 := extract_context(empty_headers)
	assert ctx2.is_valid() == false
}

fn test_inject_context() {
	ctx := SpanContext{
		trace_id:    '0af7651916cd43dd8448eb211c80319c'
		span_id:     'b7ad6b7169203331'
		trace_flags: 0x01
		trace_state: 'vendor=value'
	}

	mut headers := map[string]string{}
	inject_context(ctx, mut headers)

	assert 'traceparent' in headers
	assert headers['traceparent'] == '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
	assert headers['tracestate'] == 'vendor=value'
}

fn test_inject_context_invalid() {
	invalid_ctx := SpanContext{}

	mut headers := map[string]string{}
	inject_context(invalid_ctx, mut headers)

	// Should not inject anything
	assert 'traceparent' !in headers
}

fn test_attr_constructors() {
	s := attr_string('key', 'value')
	assert s.key == 'key'

	i := attr_int('num', 42)
	assert i.key == 'num'

	f := attr_float('pi', 3.14)
	assert f.key == 'pi'

	b := attr_bool('flag', true)
	assert b.key == 'flag'
}

fn test_span_kind_values() {
	assert int(SpanKind.internal) == 0
	assert int(SpanKind.server) == 1
	assert int(SpanKind.client) == 2
	assert int(SpanKind.producer) == 3
	assert int(SpanKind.consumer) == 4
}

fn test_tracer_config() {
	config := TracerConfig{
		service_name:    'my-service'
		service_version: '1.0.0'
		environment:     'production'
	}

	tracer := new_tracer(config)

	assert tracer.config.service_name == 'my-service'
	assert tracer.config.service_version == '1.0.0'
	assert tracer.config.environment == 'production'
}

fn test_export_span_json() {
	mut tracer := new_tracer(TracerConfig{
		service_name: 'test-service'
	})

	mut span := tracer.start_span('test-operation')
	span.set_attribute(attr_string('key', 'value'))
	span.end()

	json := export_span_json(span, tracer)

	assert json.contains('test-service')
	assert json.contains('test-operation')
	assert json.contains(span.context.trace_id)
	assert json.contains(span.context.span_id)
}

fn test_with_attributes() {
	mut tracer := new_default_tracer()

	mut span := tracer.start_span('test', with_attributes(attr_string('k1', 'v1'), attr_int('k2',
		2)))

	assert span.attributes.len == 2
	assert span.attributes[0].key == 'k1'
	assert span.attributes[1].key == 'k2'
}
