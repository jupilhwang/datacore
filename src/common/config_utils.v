/// Common utilities shared across architecture layers.
/// These are pure functions with no external dependencies,
/// safe to use from Domain, Service, and Infra layers.
module common

/// parse_config_i64 parses an i64 value from a configuration map.
pub fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
	if val := configs[key] {
		return val.i64()
	}
	return default_val
}

/// parse_config_int parses an int value from a configuration map.
pub fn parse_config_int(configs map[string]string, key string, default_val int) int {
	if val := configs[key] {
		return val.int()
	}
	return default_val
}
