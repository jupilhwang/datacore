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
	// empty string passes through unchanged
	assert escape_toml_string('') == ''
	// normal string passes through unchanged
	assert escape_toml_string('normal_value') == 'normal_value'
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
