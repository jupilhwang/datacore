/// Unit tests - Task #15: Extended metrics and TelemetryProvider
module observability

// Test Share Group metrics

fn test_share_group_metrics_acquire() {
	mut m := new_datacore_metrics()
	before := m.share_group.acquired.value
	m.record_share_group_acquire(10)
	assert m.share_group.acquired.value == before + 10.0

	m.record_share_group_acquire(5)
	assert m.share_group.acquired.value == before + 15.0
}

fn test_share_group_metrics_ack() {
	mut m := new_datacore_metrics()
	before := m.share_group.acked.value
	m.record_share_group_ack(7)
	assert m.share_group.acked.value == before + 7.0
}

fn test_share_group_metrics_release() {
	mut m := new_datacore_metrics()
	before := m.share_group.released.value
	m.record_share_group_release(3)
	assert m.share_group.released.value == before + 3.0
}

fn test_share_group_metrics_reject() {
	mut m := new_datacore_metrics()
	before := m.share_group.rejected.value
	m.record_share_group_reject(2)
	assert m.share_group.rejected.value == before + 2.0
}

fn test_share_group_active_sessions() {
	mut m := new_datacore_metrics()
	m.update_share_group_sessions(5)
	assert m.share_group.active_sessions.value == 5.0

	m.update_share_group_sessions(3)
	assert m.share_group.active_sessions.value == 3.0
}

// Test gRPC Gateway metrics

fn test_grpc_gateway_request_success() {
	mut m := new_datacore_metrics()
	before_req := m.grpc_gateway.requests_total.value
	before_err := m.grpc_gateway.errors_total.value
	before_hist := m.grpc_gateway.latency_seconds.count
	m.record_grpc_request(true, 0.05)
	assert m.grpc_gateway.requests_total.value == before_req + 1.0
	assert m.grpc_gateway.errors_total.value == before_err
	assert m.grpc_gateway.latency_seconds.count == before_hist + 1
}

fn test_grpc_gateway_request_failure() {
	mut m := new_datacore_metrics()
	before_req := m.grpc_gateway.requests_total.value
	before_err := m.grpc_gateway.errors_total.value
	m.record_grpc_request(false, 0.1)
	assert m.grpc_gateway.requests_total.value == before_req + 1.0
	assert m.grpc_gateway.errors_total.value == before_err + 1.0
}

fn test_grpc_gateway_active_connections() {
	mut m := new_datacore_metrics()
	m.update_grpc_connections(42)
	assert m.grpc_gateway.active_connections.value == 42.0
}

// Test Partition detail metrics

fn test_partition_detail_metrics() {
	mut m := new_datacore_metrics()
	m.update_partition_metrics(1024 * 1024, 1000, 50)
	assert m.partition_detail.log_size.value == f64(1024 * 1024)
	assert m.partition_detail.current_offset.value == 1000.0
	assert m.partition_detail.lag.value == 50.0
}

fn test_partition_lag_zero() {
	mut m := new_datacore_metrics()
	m.update_partition_metrics(0, 0, 0)
	assert m.partition_detail.lag.value == 0.0
}

// Test Consumer group detail metrics

fn test_consumer_group_detail_metrics() {
	mut m := new_datacore_metrics()
	m.update_consumer_group_metrics(3, 100)
	assert m.consumer_group_detail.members.value == 3.0
	assert m.consumer_group_detail.lag.value == 100.0
}

// Test Prometheus export contains new metrics

fn test_prometheus_export_contains_share_group() {
	// Use a fresh registry for this test
	mut reg := new_registry()
	_ = reg.register('datacore_share_group_acquired', 'test', .counter)
	output := reg.export_prometheus()
	assert output.contains('datacore_share_group_acquired')
}

fn test_prometheus_export_contains_grpc() {
	mut reg := new_registry()
	_ = reg.register('datacore_grpc_requests_total', 'test', .counter)
	_ = reg.register('datacore_grpc_latency_seconds', 'test', .histogram)
	output := reg.export_prometheus()
	assert output.contains('datacore_grpc_requests_total')
	assert output.contains('datacore_grpc_latency_seconds')
}

fn test_prometheus_export_contains_partition_metrics() {
	mut reg := new_registry()
	_ = reg.register('datacore_partition_log_size', 'test', .gauge)
	_ = reg.register('datacore_partition_offset', 'test', .gauge)
	_ = reg.register('datacore_partition_lag', 'test', .gauge)
	output := reg.export_prometheus()
	assert output.contains('datacore_partition_log_size')
	assert output.contains('datacore_partition_offset')
	assert output.contains('datacore_partition_lag')
}

fn test_prometheus_export_contains_consumer_group_detail() {
	mut reg := new_registry()
	_ = reg.register('datacore_consumer_group_members', 'test', .gauge)
	_ = reg.register('datacore_consumer_group_lag', 'test', .gauge)
	output := reg.export_prometheus()
	assert output.contains('datacore_consumer_group_members')
	assert output.contains('datacore_consumer_group_lag')
}

// Test OTLP metrics payload builder

fn test_otlp_metrics_payload_counter() {
	cfg := OTLPConfig{
		endpoint:        'http://localhost:4318'
		service_name:    'test-service'
		service_version: '1.0.0'
	}
	exporter := new_otlp_exporter(cfg)

	mut reg := new_registry()
	mut c := reg.register('test_counter', 'A test counter', .counter)
	c.inc_by(42)

	payload := exporter.build_metrics_payload(mut reg)
	assert payload.len > 0
	assert payload.contains('test_counter')
	assert payload.contains('test-service')
	assert payload.contains('"isMonotonic":true')
}

fn test_otlp_metrics_payload_gauge() {
	cfg := OTLPConfig{
		endpoint:        'http://localhost:4318'
		service_name:    'test-service'
		service_version: '1.0.0'
	}
	exporter := new_otlp_exporter(cfg)

	mut reg := new_registry()
	mut g := reg.register('test_gauge', 'A test gauge', .gauge)
	g.set(99.5)

	payload := exporter.build_metrics_payload(mut reg)
	assert payload.contains('test_gauge')
	assert payload.contains('"gauge"')
}

fn test_otlp_metrics_payload_histogram() {
	cfg := OTLPConfig{
		endpoint:        'http://localhost:4318'
		service_name:    'test-service'
		service_version: '1.0.0'
	}
	exporter := new_otlp_exporter(cfg)

	mut reg := new_registry()
	mut h := reg.register('test_histogram', 'A test histogram', .histogram)
	h.observe(0.05)
	h.observe(0.15)
	h.observe(1.0)

	payload := exporter.build_metrics_payload(mut reg)
	assert payload.contains('test_histogram')
	assert payload.contains('"histogram"')
	assert payload.contains('"count":3')
}

fn test_otlp_metrics_payload_empty_registry() {
	cfg := OTLPConfig{
		endpoint:     'http://localhost:4318'
		service_name: 'test-service'
	}
	exporter := new_otlp_exporter(cfg)

	mut reg := new_registry()
	payload := exporter.build_metrics_payload(mut reg)
	// Empty registry should return empty string
	assert payload == ''
}

// Test TelemetryProvider

fn test_telemetry_provider_creation() {
	cfg := TelemetryConfig{
		enabled:      true
		service_name: 'test-datacore'
	}
	mut p := new_telemetry_provider(cfg)
	assert p.config.service_name == 'test-datacore'
	assert p.metrics != unsafe { nil }
}

fn test_telemetry_provider_default() {
	mut p := new_telemetry_provider_default()
	assert p.config.service_name == 'datacore'
	assert p.config.enabled == true
}

fn test_telemetry_provider_init_no_endpoint() {
	// Provider with tracing disabled and no OTLP endpoint
	cfg := TelemetryConfig{
		enabled:       true
		service_name:  'test-service'
		otlp_endpoint: ''
	}
	mut p := new_telemetry_provider(cfg)
	p.init()
	// Tracer should be created even without endpoint
	assert p.tracer != unsafe { nil }
}

fn test_telemetry_provider_tracer_disabled() {
	cfg := TelemetryConfig{
		traces_enabled: false
		service_name:   'test-service'
	}
	mut p := new_telemetry_provider(cfg)
	p.init()
	// AlwaysOffSampler should be used
	assert p.tracer.config.sampler.should_sample() == false
}

fn test_telemetry_provider_tracer_enabled() {
	cfg := TelemetryConfig{
		traces_enabled: true
		sample_rate:    1.0
		service_name:   'test-service'
	}
	mut p := new_telemetry_provider(cfg)
	p.init()
	// RatioSampler(1.0) should always sample
	assert p.tracer.config.sampler.should_sample() == true
}

fn test_telemetry_provider_shutdown() {
	cfg := TelemetryConfig{
		enabled:      true
		service_name: 'test-service'
	}
	mut p := new_telemetry_provider(cfg)
	p.init()
	// Shutdown should not panic
	p.shutdown()
	assert p.metrics_export_running == false
}
