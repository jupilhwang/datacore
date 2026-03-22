// CLI argument parsing and environment variable mapping
// Handles command-line argument parsing and environment variable utilities.
module config

/// parse_cli_args parses command-line arguments and returns them as a map.
/// format: --key=value or --key value
pub fn parse_cli_args(args []string) map[string]string {
	mut result := map[string]string{}

	for i := 0; i < args.len; i++ {
		arg := args[i]

		// --key=value format
		if arg.starts_with('--') && arg.contains('=') {
			key, val := arg[2..].split_once('=') or { continue }
			result[key] = val
		}
		// --key value format
		else if arg.starts_with('--') && i + 1 < args.len {
			key := arg[2..]
			value := args[i + 1]
			if !value.starts_with('--') {
				result[key] = value
				i++
			}
		}
	}

	return result
}

/// toml_key_to_env_key_upper converts a TOML key to an uppercase environment variable name.
/// example: broker.host -> BROKER_HOST
fn toml_key_to_env_key_upper(toml_key string) string {
	mut env_key := toml_key.replace('.', '_')
	env_key = env_key.to_upper()
	return env_key
}

/// toml_key_to_env_key_lower converts a TOML key to a lowercase environment variable name.
/// example: broker.host -> broker_host
fn toml_key_to_env_key_lower(toml_key string) string {
	mut env_key := toml_key.replace('.', '_')
	env_key = env_key.to_lower()
	return env_key
}

/// EnvMapping represents a section and its configurable keys for environment variable display.
struct EnvMapping {
	section string
	keys    []string
}

/// print_env_mapping prints the mapping between TOML keys and environment variable names.
pub fn print_env_mapping() {
	println('=== TOML Key to Environment Variable Mapping ===')
	println('')
	println('Search order:')
	println('  1. uppercase (e.g. BROKER_HOST)')
	println('  2. lowercase (e.g. broker_host)')
	println('  3. DATACORE_ prefix + uppercase (e.g. DATACORE_BROKER_HOST)')
	println('')

	env_mappings := [
		EnvMapping{'broker', ['broker.host', 'broker.port', 'broker.cluster_id']},
		EnvMapping{'storage', ['storage.engine']},
		EnvMapping{'s3', ['s3.endpoint', 's3.bucket']},
		EnvMapping{'postgres', ['postgres.host', 'postgres.password']},
		EnvMapping{'logging', ['logging.level']},
	]

	mut max_key_len := 0
	for mapping in env_mappings {
		for key in mapping.keys {
			if key.len > max_key_len {
				max_key_len = key.len
			}
		}
	}
	max_key_len += 2

	for mapping in env_mappings {
		println('[${mapping.section}]')
		for key in mapping.keys {
			padded := key + ' '.repeat(max_key_len - key.len)
			upper := toml_key_to_env_key_upper(key)
			lower := toml_key_to_env_key_lower(key)
			println('  ${padded} -> ${upper}, ${lower}, DATACORE_${upper}')
		}
		println('')
	}
}
