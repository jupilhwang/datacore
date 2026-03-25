// Configuration serialization module
// Saves configuration to TOML format files.
module config

import os
import strings

/// escape_toml_string escapes special characters in a string for TOML output.
/// Prevents TOML injection when config values contain quotes, backslashes, or control characters.
fn escape_toml_string(s string) string {
	if s.len == 0 {
		return s
	}
	mut sb := strings.new_builder(s.len * 2)
	for i := 0; i < s.len; i++ {
		ch := s[i]
		if ch == 0x5C {
			// backslash
			sb.write_string('\\\\')
		} else if ch == 0x22 {
			// double-quote
			sb.write_string('\\"')
		} else if ch == 0x0A {
			// newline
			sb.write_string('\\n')
		} else if ch == 0x0D {
			// carriage return
			sb.write_string('\\r')
		} else if ch == 0x09 {
			// tab
			sb.write_string('\\t')
		} else {
			sb.write_u8(ch)
		}
	}
	return sb.str()
}

/// save saves the configuration to a TOML file.
/// Serializes all config sections using escape_toml_string() for string safety.
pub fn (c Config) save(path string) ! {
	mut b := strings.new_builder(4096)
	b.write_string('# DataCore Configuration\n\n')
	c.save_broker_section(mut b)
	c.save_rest_section(mut b)
	c.save_grpc_section(mut b)
	c.save_storage_section(mut b)
	c.save_s3_section(mut b)
	c.save_postgres_section(mut b)
	c.save_schema_registry_section(mut b)
	c.save_observability_section(mut b)
	os.write_file(path, b.str())!
	// Restrict permissions: config may contain credentials (S3 keys, DB password)
	os.chmod(path, 0o600)!
}

/// save_broker_section writes the [broker] and [broker.rate_limit] TOML sections.
fn (c Config) save_broker_section(mut b strings.Builder) {
	b.write_string('[broker]\n')
	b.write_string('host = "${escape_toml_string(c.broker.host)}"\n')
	b.write_string('port = ${c.broker.port}\n')
	b.write_string('broker_id = ${c.broker.broker_id}\n')
	b.write_string('cluster_id = "${escape_toml_string(c.broker.cluster_id)}"\n')
	b.write_string('max_connections = ${c.broker.max_connections}\n')
	b.write_string('max_request_size = ${c.broker.max_request_size}\n')
	b.write_string('request_timeout_ms = ${c.broker.request_timeout_ms}\n')
	b.write_string('idle_timeout_ms = ${c.broker.idle_timeout_ms}\n')
	b.write_string('advertised_host = "${escape_toml_string(c.broker.advertised_host)}"\n')
	b.write_string('\n')
	// [broker.rate_limit] sub-section
	rl := c.broker.rate_limit
	b.write_string('[broker.rate_limit]\n')
	b.write_string('enabled = ${rl.enabled}\n')
	b.write_string('max_requests_per_sec = ${rl.max_requests_per_sec}\n')
	b.write_string('max_bytes_per_sec = ${rl.max_bytes_per_sec}\n')
	b.write_string('per_ip_max_requests_per_sec = ${rl.per_ip_max_requests_per_sec}\n')
	b.write_string('per_ip_max_connections = ${rl.per_ip_max_connections}\n')
	b.write_string('burst_multiplier = ${rl.burst_multiplier}\n')
	b.write_string('window_ms = ${rl.window_ms}\n')
	b.write_string('\n')
}

/// save_rest_section writes the [rest] TOML section.
fn (c Config) save_rest_section(mut b strings.Builder) {
	b.write_string('[rest]\n')
	b.write_string('enabled = ${c.rest.enabled}\n')
	b.write_string('host = "${escape_toml_string(c.rest.host)}"\n')
	b.write_string('port = ${c.rest.port}\n')
	b.write_string('max_connections = ${c.rest.max_connections}\n')
	b.write_string('static_dir = "${escape_toml_string(c.rest.static_dir)}"\n')
	b.write_string('sse_heartbeat_interval_ms = ${c.rest.sse_heartbeat_interval_ms}\n')
	b.write_string('sse_connection_timeout_ms = ${c.rest.sse_connection_timeout_ms}\n')
	b.write_string('ws_max_message_size = ${c.rest.ws_max_message_size}\n')
	b.write_string('ws_ping_interval_ms = ${c.rest.ws_ping_interval_ms}\n')
	b.write_string('\n')
}

/// save_grpc_section writes the [grpc] TOML section.
fn (c Config) save_grpc_section(mut b strings.Builder) {
	b.write_string('[grpc]\n')
	b.write_string('enabled = ${c.grpc.enabled}\n')
	b.write_string('host = "${escape_toml_string(c.grpc.host)}"\n')
	b.write_string('port = ${c.grpc.port}\n')
	b.write_string('max_connections = ${c.grpc.max_connections}\n')
	b.write_string('max_message_size = ${c.grpc.max_message_size}\n')
	b.write_string('\n')
}

/// save_storage_section writes [storage] and [storage.memory] TOML sections.
fn (c Config) save_storage_section(mut b strings.Builder) {
	b.write_string('[storage]\n')
	b.write_string('engine = "${escape_toml_string(c.storage.engine)}"\n')
	b.write_string('\n')
	b.write_string('[storage.memory]\n')
	b.write_string('max_memory_mb = ${c.storage.memory.max_memory_mb}\n')
	b.write_string('segment_size_bytes = ${c.storage.memory.segment_size_bytes}\n')
	b.write_string('\n')
}

/// save_s3_section writes [storage.s3] and [storage.s3.iceberg] TOML sections.
fn (c Config) save_s3_section(mut b strings.Builder) {
	s3 := c.storage.s3
	b.write_string('[storage.s3]\n')
	b.write_string('endpoint = "${escape_toml_string(s3.endpoint)}"\n')
	b.write_string('bucket = "${escape_toml_string(s3.bucket)}"\n')
	b.write_string('region = "${escape_toml_string(s3.region)}"\n')
	b.write_string('access_key = "${escape_toml_string(s3.access_key)}"\n') // sensitive
	b.write_string('secret_key = "${escape_toml_string(s3.secret_key)}"\n') // sensitive
	b.write_string('prefix = "${escape_toml_string(s3.prefix)}"\n')
	b.write_string('timezone = "${escape_toml_string(s3.timezone)}"\n')
	b.write_string('batch_timeout_ms = ${s3.batch_timeout_ms}\n')
	b.write_string('batch_max_bytes = ${s3.batch_max_bytes}\n')
	b.write_string('min_flush_bytes = ${s3.min_flush_bytes}\n')
	b.write_string('max_flush_skip_count = ${s3.max_flush_skip_count}\n')
	b.write_string('compaction_interval_ms = ${s3.compaction_interval_ms}\n')
	b.write_string('target_segment_bytes = ${s3.target_segment_bytes}\n')
	b.write_string('index_cache_ttl_ms = ${s3.index_cache_ttl_ms}\n')
	b.write_string('offset_batch_enabled = ${s3.offset_batch_enabled}\n')
	b.write_string('offset_flush_interval_ms = ${s3.offset_flush_interval_ms}\n')
	b.write_string('offset_flush_threshold_count = ${s3.offset_flush_threshold_count}\n')
	b.write_string('index_batch_size = ${s3.index_batch_size}\n')
	b.write_string('index_flush_interval_ms = ${s3.index_flush_interval_ms}\n')
	b.write_string('sync_linger_ms = ${s3.sync_linger_ms}\n')
	b.write_string('use_server_side_copy = ${s3.use_server_side_copy}\n')
	b.write_string('\n')
	// [storage.s3.iceberg] sub-section
	b.write_string('[storage.s3.iceberg]\n')
	b.write_string('enabled = ${s3.iceberg_enabled}\n')
	b.write_string('format = "${escape_toml_string(s3.iceberg_format)}"\n')
	b.write_string('compression = "${escape_toml_string(s3.iceberg_compression)}"\n')
	b.write_string('write_mode = "${escape_toml_string(s3.iceberg_write_mode)}"\n')
	b.write_string('partition_by = [')
	for i, val in s3.iceberg_partition_by {
		if i > 0 {
			b.write_string(', ')
		}
		b.write_string('"${escape_toml_string(val)}"')
	}
	b.write_string(']\n')
	b.write_string('max_rows_per_file = ${s3.iceberg_max_rows_per_file}\n')
	b.write_string('max_file_size_mb = ${s3.iceberg_max_file_size_mb}\n')
	b.write_string('schema_evolution = ${s3.iceberg_schema_evolution}\n')
	b.write_string('format_version = ${s3.iceberg_format_version}\n')
	b.write_string('\n')
}

/// save_postgres_section writes the [storage.postgres] TOML section.
fn (c Config) save_postgres_section(mut b strings.Builder) {
	pg := c.storage.postgres
	b.write_string('[storage.postgres]\n')
	b.write_string('host = "${escape_toml_string(pg.host)}"\n')
	b.write_string('port = ${pg.port}\n')
	b.write_string('database = "${escape_toml_string(pg.database)}"\n')
	b.write_string('user = "${escape_toml_string(pg.user)}"\n')
	b.write_string('password = "${escape_toml_string(pg.password)}"\n') // sensitive
	b.write_string('pool_size = ${pg.pool_size}\n')
	b.write_string('sslmode = "${escape_toml_string(pg.sslmode)}"\n')
	b.write_string('\n')
}

/// save_schema_registry_section writes the [schema_registry] TOML section.
fn (c Config) save_schema_registry_section(mut b strings.Builder) {
	b.write_string('[schema_registry]\n')
	b.write_string('enabled = ${c.schema_registry.enabled}\n')
	b.write_string('topic = "${escape_toml_string(c.schema_registry.topic)}"\n')
	b.write_string('\n')
}

/// save_observability_section writes all [observability.*] TOML sections.
fn (c Config) save_observability_section(mut b strings.Builder) {
	o := c.observability
	// [observability.otel]
	b.write_string('[observability.otel]\n')
	b.write_string('enabled = ${o.otel.enabled}\n')
	b.write_string('service_name = "${escape_toml_string(o.otel.service_name)}"\n')
	b.write_string('service_version = "${escape_toml_string(o.otel.service_version)}"\n')
	b.write_string('instance_id = "${escape_toml_string(o.otel.instance_id)}"\n')
	b.write_string('environment = "${escape_toml_string(o.otel.environment)}"\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.otel.otlp_endpoint)}"\n')
	b.write_string('otlp_http_endpoint = "${escape_toml_string(o.otel.otlp_http_endpoint)}"\n')
	b.write_string('\n')
	// [observability.metrics]
	b.write_string('[observability.metrics]\n')
	b.write_string('enabled = ${o.metrics.enabled}\n')
	b.write_string('exporter = "${escape_toml_string(o.metrics.exporter)}"\n')
	b.write_string('prometheus_endpoint = "${escape_toml_string(o.metrics.prometheus_endpoint)}"\n')
	b.write_string('prometheus_port = ${o.metrics.prometheus_port}\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.metrics.otlp_endpoint)}"\n')
	b.write_string('collection_interval = ${o.metrics.collection_interval}\n')
	b.write_string('\n')
	// [observability.logging]
	b.write_string('[observability.logging]\n')
	b.write_string('enabled = ${o.logging.enabled}\n')
	b.write_string('level = "${escape_toml_string(o.logging.level)}"\n')
	b.write_string('format = "${escape_toml_string(o.logging.format)}"\n')
	b.write_string('output = "${escape_toml_string(o.logging.output)}"\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.logging.otlp_endpoint)}"\n')
	b.write_string('otlp_export = ${o.logging.otlp_export}\n')
	b.write_string('console_output = ${o.logging.console_output}\n')
	b.write_string('inject_trace_context = ${o.logging.inject_trace_context}\n')
	b.write_string('\n')
	// [observability.tracing]
	b.write_string('[observability.tracing]\n')
	b.write_string('enabled = ${o.tracing.enabled}\n')
	b.write_string('otlp_endpoint = "${escape_toml_string(o.tracing.otlp_endpoint)}"\n')
	b.write_string('sampler = "${escape_toml_string(o.tracing.sampler)}"\n')
	b.write_string('sample_rate = ${o.tracing.sample_rate}\n')
	b.write_string('batch_timeout_ms = ${o.tracing.batch_timeout_ms}\n')
	b.write_string('max_batch_size = ${o.tracing.max_batch_size}\n')
	b.write_string('max_queue_size = ${o.tracing.max_queue_size}\n')
	b.write_string('max_attributes_per_span = ${o.tracing.max_attributes_per_span}\n')
	b.write_string('max_events_per_span = ${o.tracing.max_events_per_span}\n')
	b.write_string('max_links_per_span = ${o.tracing.max_links_per_span}\n')
	b.write_string('\n')
}
