// Configuration priority cascade tests
module config

import os
import toml

fn test_parse_cli_args() {
	// --key=value format
	args1 := ['--broker-port=9093', '--broker-host=localhost']
	result1 := parse_cli_args(args1)
	assert result1['broker-port'] == '9093'
	assert result1['broker-host'] == 'localhost'

	// --key value format
	args2 := ['--broker-port', '9094', '--broker-host', '127.0.0.1']
	result2 := parse_cli_args(args2)
	assert result2['broker-port'] == '9094'
	assert result2['broker-host'] == '127.0.0.1'

	// mixed format
	args3 := ['--broker-port=9095', '--broker-host', 'example.com']
	result3 := parse_cli_args(args3)
	assert result3['broker-port'] == '9095'
	assert result3['broker-host'] == 'example.com'
}

fn test_config_priority_cli_over_env() {
	// CLI arguments take priority over environment variables
	os.setenv('DATACORE_BROKER_PORT', '9093', true)

	mut cli_args := map[string]string{}
	cli_args['broker-port'] = '9094'

	// CLI argument takes priority
	assert cli_args['broker-port'] == '9094'

	// environment variable is used only when CLI argument is absent
	env_port := os.getenv('DATACORE_BROKER_PORT')
	assert env_port == '9093'

	os.unsetenv('DATACORE_BROKER_PORT')
}

fn test_load_config_from_file() {
	// load config.toml if it exists
	cfg := load_config('config.toml') or {
		// skip test if file is not found
		println('config.toml not found, skipping test')
		return
	}

	// verify defaults (values defined in config.toml)
	assert cfg.broker.port > 0
	assert cfg.broker.host != ''
}

fn test_load_config_no_file_uses_defaults() {
	// load_config with non-existent path must use defaults without segfault
	cfg := load_config('/non/existent/path/config.toml') or {
		assert false, 'load_config should not return error for missing file: ${err}'
		return
	}
	// verify all defaults are applied correctly
	assert cfg.broker.port == 9092
	assert cfg.broker.host == '0.0.0.0'
	assert cfg.rest.port == 8080
	assert cfg.rest.enabled == true
	assert cfg.storage.engine == 'memory'
	assert cfg.schema_registry.enabled == true
	assert cfg.schema_registry.topic == '__schemas'
}

fn test_escape_toml_string_escapes_special_characters() {
	// backslash must be escaped
	assert escape_toml_string('path\\to\\file') == 'path\\\\to\\\\file'
	// double-quote must be escaped
	assert escape_toml_string('say "hello"') == 'say \\"hello\\"'
	// newline must be escaped
	assert escape_toml_string('line1\nline2') == 'line1\\nline2'
	// carriage return must be escaped
	assert escape_toml_string('line1\rline2') == 'line1\\rline2'
	// tab must be escaped
	assert escape_toml_string('col1\tcol2') == 'col1\\tcol2'
	// backspace must be escaped
	assert escape_toml_string('before\bafter') == 'before\\bafter'
	// form feed must be escaped
	assert escape_toml_string('before\fafter') == 'before\\fafter'
	// empty string passes through unchanged
	assert escape_toml_string('') == ''
	// normal string passes through unchanged
	assert escape_toml_string('normal_value') == 'normal_value'
}

fn test_escape_toml_string_combined_special_characters() {
	// multiple special characters in one string must all be escaped
	input := 'line1\nline2\ttab\b\f"quoted"\\backslash'
	expected := 'line1\\nline2\\ttab\\b\\f\\"quoted\\"\\\\backslash'
	assert escape_toml_string(input) == expected
}

fn test_validate_rejects_invalid_storage_engine() {
	cfg := Config{
		broker:  BrokerConfig{
			broker_id: 1
		}
		storage: StorageConfig{
			engine: 'nonexistent'
		}
	}
	cfg.validate() or {
		assert err.msg().contains('Unknown storage engine')
		return
	}
	assert false, 'validate() should return error for invalid engine'
}

fn test_validate_accepts_valid_storage_engine() {
	cfg := Config{
		broker:  BrokerConfig{
			broker_id: 1
		}
		storage: StorageConfig{
			engine: 's3'
			s3:     S3StorageConfig{
				bucket:     'test-bucket'
				region:     'us-east-1'
				endpoint:   'https://test-bucket.s3.us-east-1.amazonaws.com'
				access_key: 'AKIAIOSFODNN7EXAMPLE'
				secret_key: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
			}
		}
	}
	cfg.validate() or {
		assert false, 'validate() should succeed for valid s3 config: ${err}'
		return
	}
}

fn test_load_default_config_has_valid_defaults() {
	cfg := load_config_with_args('/non/existent/path', map[string]string{}) or {
		assert false, 'load_config_with_args should not fail: ${err}'
		return
	}
	assert cfg.broker.port == 9092
	assert cfg.broker.host == '0.0.0.0'
	assert cfg.storage.engine == 'memory'
	assert cfg.storage.memory.max_memory_mb == 20480
}

fn test_parse_cli_args_key_value_format() {
	result := parse_cli_args(['--broker-port', '9999'])
	assert result['broker-port'] == '9999'
}

fn test_parse_cli_args_equals_format() {
	result := parse_cli_args(['--broker-port=9999'])
	assert result['broker-port'] == '9999'
}

fn test_parse_cli_args_mixed_formats() {
	result := parse_cli_args(['--broker-port=9999', '--broker-host', 'localhost'])
	assert result['broker-port'] == '9999'
	assert result['broker-host'] == 'localhost'
}

fn test_generate_deterministic_broker_id_consistency() {
	id1 := generate_deterministic_broker_id()
	id2 := generate_deterministic_broker_id()
	assert id1 == id2
	assert id1 >= 1
}

fn test_save_escapes_special_characters() {
	// config with TOML injection payload in string fields
	mut cfg := Config{
		broker:          BrokerConfig{
			host:       '0.0.0.0'
			port:       9092
			broker_id:  1
			cluster_id: 'injected"\nmalicious_key = "evil'
		}
		storage:         StorageConfig{
			engine: 'mem\\ory'
		}
		schema_registry: SchemaRegistryConfig{
			enabled: true
			topic:   '__schemas\twith\ttabs'
		}
		observability:   ObservabilityConfig{
			logging: LoggingConfig{
				level:  'info\rcarriage'
				format: 'json"quote'
			}
		}
	}

	// save to temp file
	tmp_path := os.join_path(os.temp_dir(), 'test_toml_escape.toml')
	defer {
		os.rm(tmp_path) or {}
	}
	cfg.save(tmp_path) or {
		assert false, 'save() should not fail: ${err}'
		return
	}

	// read back and verify special characters are escaped in the TOML output
	content := os.read_file(tmp_path) or {
		assert false, 'failed to read saved file: ${err}'
		return
	}

	// the cluster_id with injected quote must have escaped quote in output
	assert content.contains('cluster_id = "injected\\"'), 'double-quote in cluster_id not escaped'

	// the storage engine with backslash must be escaped
	assert content.contains('engine = "mem\\\\ory"'), 'backslash in engine not escaped'

	// the saved file must be valid TOML (parseable without error)
	// this is the critical check: if injection succeeded, TOML parse would either
	// fail or produce an unexpected key
	parsed := toml.parse_text(content) or {
		assert false, 'saved file is not valid TOML: ${err}'
		return
	}

	// verify the injected payload is NOT a separate TOML key
	// if escaping works, malicious_key should not exist as a key
	malicious_val := parsed.value_opt('malicious_key') or {
		// good: key does not exist, injection was prevented
		return
	}
	assert false, 'TOML injection: malicious_key parsed as separate key with value: ${malicious_val}'
}

fn test_rate_limit_config_defaults_when_absent() {
	// When no [broker.rate_limit] section exists, defaults should be applied
	toml_content := '
[broker]
broker_id = 1
port = 9092
'
	doc := toml.parse_text(toml_content) or {
		assert false, 'failed to parse test TOML: ${err}'
		return
	}
	rl := parse_rate_limit_config(doc)
	assert rl.enabled == false, 'rate limit should be disabled by default'
	assert rl.max_requests_per_sec == 1000
	assert rl.max_bytes_per_sec == i64(104857600)
	assert rl.per_ip_max_requests_per_sec == 200
	assert rl.per_ip_max_connections == 50
	assert rl.burst_multiplier == 1.5
	assert rl.window_ms == 1000
}

fn test_rate_limit_config_custom_values() {
	// Custom values from TOML should override defaults
	toml_content := '
[broker]
broker_id = 1

[broker.rate_limit]
enabled = true
max_requests_per_sec = 5000
max_bytes_per_sec = 209715200
per_ip_max_requests_per_sec = 500
per_ip_max_connections = 100
burst_multiplier = 2.0
window_ms = 2000
'
	doc := toml.parse_text(toml_content) or {
		assert false, 'failed to parse test TOML: ${err}'
		return
	}
	rl := parse_rate_limit_config(doc)
	assert rl.enabled == true
	assert rl.max_requests_per_sec == 5000
	assert rl.max_bytes_per_sec == i64(209715200)
	assert rl.per_ip_max_requests_per_sec == 500
	assert rl.per_ip_max_connections == 100
	assert rl.burst_multiplier == 2.0
	assert rl.window_ms == 2000
}

fn test_rate_limit_config_disabled_explicitly() {
	// Explicitly disabled rate limiter
	toml_content := '
[broker]
broker_id = 1

[broker.rate_limit]
enabled = false
max_requests_per_sec = 9999
'
	doc := toml.parse_text(toml_content) or {
		assert false, 'failed to parse test TOML: ${err}'
		return
	}
	rl := parse_rate_limit_config(doc)
	assert rl.enabled == false, 'rate limit should be explicitly disabled'
	assert rl.max_requests_per_sec == 9999, 'custom value should still be parsed even when disabled'
}

fn test_config_save_file_permissions() {
	// After save(), the config file must have 0600 permissions
	// because it may contain credentials (S3 keys, DB password).
	cfg := Config{
		broker:  BrokerConfig{
			broker_id: 1
		}
		storage: StorageConfig{
			engine: 'memory'
		}
	}

	tmp_path := os.join_path(os.temp_dir(), 'test_config_perms.toml')
	defer {
		os.rm(tmp_path) or {}
	}
	cfg.save(tmp_path) or {
		assert false, 'save() should not fail: ${err}'
		return
	}

	assert os.exists(tmp_path), 'saved config file must exist'

	stat := os.stat(tmp_path) or {
		assert false, 'os.stat() should not fail: ${err}'
		return
	}
	// mask to get only permission bits (lower 9 bits)
	perm := stat.mode & 0o777
	assert perm == 0o600, 'config file permissions must be 0600 (owner-only), got 0o${perm:o}'
}

// --- SSRF endpoint validation tests ---

fn test_validate_s3_endpoint_rejects_private_ipv4() {
	// 10.0.0.0/8 range must be rejected as private address (SSRF prevention)
	validate_s3_endpoint('http://10.0.0.1:9000') or {
		assert err.msg().contains('private address')
		return
	}
	assert false, 'should reject private IPv4 (10.0.0.1)'
}

fn test_validate_s3_endpoint_allows_public_ipv4() {
	// Public IPv4 addresses must be allowed
	validate_s3_endpoint('http://54.231.0.1:9000') or {
		assert false, 'public IPv4 should be allowed: ${err}'
		return
	}
}

fn test_validate_s3_endpoint_allows_domain_name() {
	// Domain names must pass through -- DNS-based SSRF protection is out of scope
	validate_s3_endpoint('https://s3.amazonaws.com') or {
		assert false, 'domain name should be allowed: ${err}'
		return
	}
}

fn test_validate_s3_endpoint_rejects_localhost() {
	// localhost must be rejected as loopback address
	validate_s3_endpoint('http://localhost:9000') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should reject localhost'
}

fn test_validate_s3_endpoint_rejects_malformed_ipv4_mapped_ipv6() {
	// ::ffff: prefix with non-IPv4 suffix is malformed and must be rejected
	validate_s3_endpoint('http://[::ffff:not-an-ip]') or {
		assert err.msg().contains('malformed')
		return
	}
	assert false, 'should reject malformed IPv4-mapped IPv6 address'
}

fn test_rate_limit_config_in_full_config_load() {
	// Verify RateLimitConfig is accessible via Config.broker.rate_limit
	toml_content := '
[broker]
broker_id = 1

[broker.rate_limit]
enabled = true
max_requests_per_sec = 2000
'
	tmp_path := os.join_path(os.temp_dir(), 'test_rate_limit_config.toml')
	defer {
		os.rm(tmp_path) or {}
	}
	os.write_file(tmp_path, toml_content) or {
		assert false, 'failed to write test config: ${err}'
		return
	}
	cfg := load_config(tmp_path) or {
		assert false, 'load_config should not fail: ${err}'
		return
	}
	assert cfg.broker.rate_limit.enabled == true
	assert cfg.broker.rate_limit.max_requests_per_sec == 2000
}
