/// Infrastructure layer - Telemetry Provider
/// Centralizes initialization of metrics, tracing, and logging observability components.
/// Reads configuration and wires together MetricsRegistry, Tracer, and OTLPExporter.
module observability

import time

/// TelemetryConfig holds top-level telemetry configuration (mirrors config.toml [telemetry]).
pub struct TelemetryConfig {
pub:
	enabled      bool   = true
	service_name string = 'datacore'
	// OTLP endpoint (gRPC or HTTP)
	otlp_endpoint      string = 'http://localhost:4317'
	otlp_http_endpoint string
	// Metrics
	metrics_enabled           bool   = true
	metrics_exporter          string = 'prometheus' // 'prometheus' or 'otlp'
	metrics_interval_s        int    = 10
	metrics_export_timeout_ms int    = 30000
	// Tracing
	traces_enabled bool
	sample_rate    f64 = 1.0
	// Shared
	service_version string = '0.44.4'
	instance_id     string
	environment     string = 'development'
}

/// TelemetryProvider manages lifecycle of all telemetry components.
@[heap]
pub struct TelemetryProvider {
	config TelemetryConfig
pub mut:
	metrics &DataCoreMetrics = unsafe { nil }
	tracer  &Tracer          = unsafe { nil }
mut:
	otlp_exporter          &OTLPExporter  = unsafe { nil }
	metrics_server         &MetricsServer = unsafe { nil }
	metrics_export_running bool
}

/// new_telemetry_provider creates a TelemetryProvider from the given TelemetryConfig.
pub fn new_telemetry_provider(cfg TelemetryConfig) &TelemetryProvider {
	mut p := &TelemetryProvider{
		config: cfg
	}
	p.metrics = new_datacore_metrics_ref()
	return p
}

/// new_telemetry_provider_default returns a TelemetryProvider initialized with default settings.
pub fn new_telemetry_provider_default() &TelemetryProvider {
	return new_telemetry_provider(TelemetryConfig{})
}

/// init sets up all telemetry subsystems (tracer, OTLP exporter) according to configuration.
pub fn (mut p TelemetryProvider) init() {
	if !p.config.enabled {
		return
	}

	p.init_tracer()
	p.init_otlp_exporter()
}

/// shutdown stops all background goroutines and flushes buffered telemetry data.
pub fn (mut p TelemetryProvider) shutdown() {
	p.metrics_export_running = false
	if p.otlp_exporter != unsafe { nil } {
		p.otlp_exporter.stop()
	}
}

/// start_metrics_export launches a background goroutine that periodically exports metrics via OTLP.
/// Only active when metrics_exporter == 'otlp'.
pub fn (mut p TelemetryProvider) start_metrics_export() {
	if !p.config.metrics_enabled {
		return
	}
	if p.config.metrics_exporter != 'otlp' {
		return
	}
	if p.otlp_exporter == unsafe { nil } {
		return
	}
	p.metrics_export_running = true
	spawn p.metrics_export_loop()
}

// metrics_export_loop periodically exports metrics via OTLP.
fn (mut p TelemetryProvider) metrics_export_loop() {
	interval := time.second * p.config.metrics_interval_s
	for p.metrics_export_running {
		time.sleep(interval)
		if p.metrics_export_running {
			p.otlp_exporter.export_metrics_snapshot()
		}
	}
}

// init_tracer sets up the tracer based on configuration.
fn (mut p TelemetryProvider) init_tracer() {
	sampler := if p.config.traces_enabled {
		Sampler(new_ratio_sampler(p.config.sample_rate))
	} else {
		Sampler(AlwaysOffSampler{})
	}

	p.tracer = new_tracer(TracerConfig{
		service_name:    p.config.service_name
		service_version: p.config.service_version
		environment:     p.config.environment
		sampler:         sampler
	})
}

// init_otlp_exporter creates and starts the OTLP exporter if an endpoint is configured.
fn (mut p TelemetryProvider) init_otlp_exporter() {
	endpoint := if p.config.otlp_http_endpoint.len > 0 {
		p.config.otlp_http_endpoint
	} else {
		// Prefer HTTP OTLP (port 4318) when the endpoint looks like a gRPC endpoint (port 4317).
		// The HTTP OTLP exporter uses JSON over HTTP/1.1 which is simpler to implement.
		p.config.otlp_endpoint.replace(':4317', ':4318')
	}

	if endpoint.len == 0 {
		return
	}

	otlp_cfg := OTLPConfig{
		endpoint:        endpoint
		service_name:    p.config.service_name
		service_version: p.config.service_version
		instance_id:     p.config.instance_id
		environment:     p.config.environment
	}

	p.otlp_exporter = new_otlp_exporter(otlp_cfg)
	p.otlp_exporter.start()

	// Register as global exporter
	unsafe {
		mut holder := otlp_holder
		holder.lock.@lock()
		holder.exporter = p.otlp_exporter
		holder.lock.unlock()
	}
}

// new_datacore_metrics_ref is a heap-allocated wrapper used by TelemetryProvider.
fn new_datacore_metrics_ref() &DataCoreMetrics {
	m := new_datacore_metrics()
	return &m
}
