// Configuration validation module
// Validates configuration values for correctness.
module config

/// validate checks the validity of the configuration.
pub fn (c Config) validate() ! {
	// validate broker configuration
	if c.broker.port < 1 || c.broker.port > 65535 {
		return error('Invalid broker port: ${c.broker.port}')
	}
	if c.broker.broker_id < 1 {
		return error('Invalid broker_id: ${c.broker.broker_id}')
	}

	// validate rate limit configuration
	c.validate_rate_limit()!

	// validate storage configuration
	match c.storage.engine {
		'memory' {
			if c.storage.memory.max_memory_mb < 1 {
				return error('Invalid max_memory_mb: ${c.storage.memory.max_memory_mb}')
			}
		}
		's3' {
			if c.storage.s3.bucket == '' {
				return error('S3 bucket is required when storage.engine = "s3"')
			}
			if c.storage.s3.region == '' {
				return error('S3 region is required when storage.engine = "s3"')
			}
			if c.storage.s3.access_key == '' {
				return error('S3 access_key is required (set DATACORE_S3_ACCESS_KEY env var)')
			}
			if c.storage.s3.secret_key == '' {
				return error('S3 secret_key is required (set DATACORE_S3_SECRET_KEY env var)')
			}
		}
		'postgres' {
			if c.storage.postgres.database == '' {
				return error('PostgreSQL database is required')
			}
		}
		else {
			return error('Unknown storage engine: ${c.storage.engine}')
		}
	}
}

/// validate_rate_limit checks the validity of rate limit configuration values.
fn (c Config) validate_rate_limit() ! {
	rl := c.broker.rate_limit
	if rl.max_requests_per_sec < 0 {
		return error('Invalid rate_limit.max_requests_per_sec: ${rl.max_requests_per_sec} (must be >= 0)')
	}
	if rl.max_bytes_per_sec < 0 {
		return error('Invalid rate_limit.max_bytes_per_sec: ${rl.max_bytes_per_sec} (must be >= 0)')
	}
	if rl.per_ip_max_requests_per_sec < 0 {
		return error('Invalid rate_limit.per_ip_max_requests_per_sec: ${rl.per_ip_max_requests_per_sec} (must be >= 0)')
	}
	if rl.per_ip_max_connections < 0 {
		return error('Invalid rate_limit.per_ip_max_connections: ${rl.per_ip_max_connections} (must be >= 0)')
	}
	if rl.burst_multiplier < 0.0 {
		return error('Invalid rate_limit.burst_multiplier: ${rl.burst_multiplier} (must be >= 0.0)')
	}
	if rl.window_ms < 0 {
		return error('Invalid rate_limit.window_ms: ${rl.window_ms} (must be >= 0)')
	}
}
