// Configuration management module
// Loads and manages configuration files in TOML format.
module config

import os
import toml

/// ConfigSource bundles shared data sources for configuration lookups.
/// Created once per parse function to avoid passing cli_args and doc repeatedly.
struct ConfigSource {
	cli_args map[string]string
	doc      toml.Doc
}

/// load_config loads configuration from a TOML file.
/// path: path to the configuration file
/// returns: loaded Config or error
pub fn load_config(path string) !Config {
	return load_config_with_args(path, map[string]string{})
}

/// load_config_with_args loads configuration including CLI arguments.
/// Priority: CLI args > environment variables > TOML > defaults
/// path: path to the configuration file
/// cli_args: CLI argument map (parsed by parse_cli_args)
/// returns: loaded Config or error
pub fn load_config_with_args(path string, cli_args map[string]string) !Config {
	if !os.exists(path) {
		return load_default_config_with_overrides(cli_args)
	}

	content := os.read_file(path) or { return error('Failed to read config file: ${err}') }
	doc := toml.parse_text(content) or { return error('Failed to parse config file: ${err}') }

	mut cfg := Config{
		broker:          parse_broker_config(cli_args, doc)
		rest:            parse_rest_config(cli_args, doc)
		grpc:            parse_grpc_config(cli_args, doc)
		storage:         parse_storage_config(cli_args, doc)!
		schema_registry: parse_schema_registry_config(doc)
		observability:   parse_observability_config(doc)
	}

	cfg.validate()!
	return cfg
}

/// load_default_config_with_overrides creates a configuration without a config file, using CLI/env overrides.
/// cli_args: CLI argument map
/// returns: Config with defaults applied and CLI/env overrides, validated
fn load_default_config_with_overrides(cli_args map[string]string) !Config {
	// toml.Doc{} has a nil ast pointer that causes segfault on value_opt calls.
	// toml.parse_text('') produces a properly initialized empty document.
	empty_doc := toml.parse_text('') or {
		return error('Failed to initialize empty config document: ${err}')
	}
	mut cfg := Config{
		broker:          parse_broker_config(cli_args, empty_doc)
		rest:            parse_rest_config(cli_args, empty_doc)
		grpc:            parse_grpc_config(cli_args, empty_doc)
		storage:         parse_storage_config(cli_args, empty_doc)!
		schema_registry: parse_schema_registry_config(empty_doc)
		observability:   parse_observability_config(empty_doc)
	}
	cfg.validate()!
	return cfg
}

// TOML document helper functions

/// get_string retrieves a string value from a TOML document.
fn get_string(doc toml.Doc, key string, default_val string) string {
	val := doc.value_opt(key) or { return default_val }
	return val.string()
}

/// get_int retrieves an integer value from a TOML document.
fn get_int(doc toml.Doc, key string, default_val int) int {
	val := doc.value_opt(key) or { return default_val }
	return val.int()
}

/// get_i64 retrieves a 64-bit integer value from a TOML document.
fn get_i64(doc toml.Doc, key string, default_val i64) i64 {
	val := doc.value_opt(key) or { return default_val }
	return val.i64()
}

/// get_f64 retrieves a floating-point value from a TOML document.
fn get_f64(doc toml.Doc, key string, default_val f64) f64 {
	val := doc.value_opt(key) or { return default_val }
	return val.f64()
}

/// get_bool retrieves a boolean value from a TOML document.
fn get_bool(doc toml.Doc, key string, default_val bool) bool {
	val := doc.value_opt(key) or { return default_val }
	return val.bool()
}

// Priority cascade helper functions (methods on ConfigSource)
// Configuration value priority: CLI args > env vars > TOML > defaults

/// get_string retrieves a string configuration value according to priority.
/// 1. CLI argument (cli_key)
/// 2. environment variable (env_key)
/// 3. TOML file (toml_key)
/// 4. default value (default_val)
fn (s &ConfigSource) get_string(cli_key string, env_key string, toml_key string, default_val string) string {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.string()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_int retrieves an integer configuration value according to priority.
fn (s &ConfigSource) get_int(cli_key string, env_key string, toml_key string, default_val int) int {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val.int()
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val.int()
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.int()
		}
	}

	// priority 4: default value
	return default_val
}

/// get_bool retrieves a boolean configuration value according to priority.
fn (s &ConfigSource) get_bool(cli_key string, env_key string, toml_key string, default_val bool) bool {
	// priority 1: CLI arguments
	if cli_val := s.cli_args[cli_key] {
		return cli_val == 'true' || cli_val == '1' || cli_val == 'yes'
	}

	// priority 2: environment variables
	if env_val := os.getenv_opt(env_key) {
		return env_val == 'true' || env_val == '1' || env_val == 'yes'
	}

	// priority 3: TOML file (skip empty keys)
	if toml_key != '' {
		if toml_val := s.doc.value_opt(toml_key) {
			return toml_val.bool()
		}
	}

	// priority 4: default value
	return default_val
}

// Config accessor methods

/// get_storage_engine returns the storage engine name.
pub fn (c Config) get_storage_engine() string {
	return c.storage.engine
}

/// is_s3_storage checks whether S3 storage is configured.
pub fn (c Config) is_s3_storage() bool {
	return c.storage.engine == 's3'
}

/// is_metrics_enabled checks whether metrics are enabled.
pub fn (c Config) is_metrics_enabled() bool {
	return c.observability.metrics.enabled
}

/// is_tracing_enabled checks whether tracing is enabled.
pub fn (c Config) is_tracing_enabled() bool {
	return c.observability.tracing.enabled
}
