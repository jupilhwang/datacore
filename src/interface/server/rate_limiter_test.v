// Rate Limiter unit tests
module server

import time

fn test_rate_limiter_config_defaults() {
	config := RateLimiterConfig{}

	assert config.max_requests_per_second == 1000
	assert config.max_bytes_per_second == i64(100_000_000)
	assert config.per_ip_max_requests_per_second == 200
	assert config.per_ip_max_connections == 50
	assert config.burst_multiplier == 1.5
	assert config.window_size_ms == i64(1000)
}

fn test_token_bucket_creation() {
	bucket := new_token_bucket(100.0, 0.1)

	assert bucket.max_tokens == 100.0
	assert bucket.refill_rate == 0.1
	assert bucket.tokens == 100.0
	assert bucket.last_refill > 0
}

fn test_token_bucket_consume_success() {
	mut bucket := new_token_bucket(10.0, 1.0)

	// Should succeed: 10 tokens available, consuming 1
	result := bucket.try_consume(1.0)
	assert result == true
	assert bucket.tokens < 10.0
}

fn test_token_bucket_exhaustion() {
	mut bucket := new_token_bucket(5.0, 0.001)

	// Consume all tokens one by one
	for _ in 0 .. 5 {
		assert bucket.try_consume(1.0) == true
	}

	// Bucket should be exhausted now
	assert bucket.try_consume(1.0) == false
}

fn test_token_bucket_refill_over_time() {
	mut bucket := new_token_bucket(10.0, 10.0) // 10 tokens/ms refill

	// Exhaust all tokens
	for _ in 0 .. 10 {
		bucket.try_consume(1.0)
	}
	assert bucket.try_consume(1.0) == false

	// Wait for refill (sleep 5ms should refill ~50 tokens, capped at 10)
	time.sleep(5 * time.millisecond)

	// Should have tokens again after refill
	assert bucket.try_consume(1.0) == true
}

fn test_token_bucket_burst_handling() {
	config := RateLimiterConfig{
		max_requests_per_second: 100
		burst_multiplier:        2.0
		window_size_ms:          i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// The global bucket should have max_tokens = 100 * 2.0 = 200 (burst capacity)
	// Rapid requests should succeed up to burst limit
	mut allowed := 0
	for _ in 0 .. 250 {
		if rl.allow_request('10.0.0.1') {
			allowed += 1
		}
	}

	// Should allow roughly up to burst capacity (200 global)
	// but per-IP default is 200 * 2.0 = 400, so global is the bottleneck
	assert allowed > 0
	assert allowed <= 200
}

fn test_global_rate_limit_enforcement() {
	config := RateLimiterConfig{
		max_requests_per_second:        10
		per_ip_max_requests_per_second: 1000 // high per-IP so global is the limit
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Send 20 requests; only ~10 should be allowed (global limit = 10, burst = 1.0x)
	mut allowed := 0
	for _ in 0 .. 20 {
		if rl.allow_request('10.0.0.1') {
			allowed += 1
		}
	}

	assert allowed == 10
}

fn test_per_ip_rate_limit_enforcement() {
	config := RateLimiterConfig{
		max_requests_per_second:        1000 // high global limit
		per_ip_max_requests_per_second: 5
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Send 10 requests from same IP; only 5 should be allowed (per-IP limit)
	mut allowed := 0
	for _ in 0 .. 10 {
		if rl.allow_request('10.0.0.1') {
			allowed += 1
		}
	}

	assert allowed == 5
}

fn test_byte_rate_limit() {
	config := RateLimiterConfig{
		max_bytes_per_second: i64(1000)
		burst_multiplier:     1.0
		window_size_ms:       i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// 500 bytes should be allowed
	assert rl.allow_bytes('10.0.0.1', i64(500)) == true

	// Another 500 bytes should be allowed (total = 1000)
	assert rl.allow_bytes('10.0.0.1', i64(500)) == true

	// 1 more byte should be rejected (over limit)
	assert rl.allow_bytes('10.0.0.1', i64(1)) == false
}

fn test_allow_when_under_limit() {
	config := RateLimiterConfig{
		max_requests_per_second:        100
		per_ip_max_requests_per_second: 50
		burst_multiplier:               1.5
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// A single request should always be allowed
	assert rl.allow_request('192.168.1.1') == true
}

fn test_throttle_when_over_limit() {
	config := RateLimiterConfig{
		max_requests_per_second:        3
		per_ip_max_requests_per_second: 3
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Exhaust the limit
	for _ in 0 .. 3 {
		rl.allow_request('10.0.0.1')
	}

	// Next request should be throttled
	assert rl.allow_request('10.0.0.1') == false
}

fn test_stats_tracking() {
	config := RateLimiterConfig{
		max_requests_per_second:        5
		per_ip_max_requests_per_second: 5
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Send 8 requests; 5 allowed, 3 throttled
	for _ in 0 .. 8 {
		rl.allow_request('10.0.0.1')
	}

	stats := rl.get_stats()
	assert stats.total_requests == 8
	assert stats.throttled_requests == 3
}

fn test_cleanup_stale_buckets() {
	config := RateLimiterConfig{
		max_requests_per_second:        100
		per_ip_max_requests_per_second: 50
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Send requests from multiple IPs to create per-IP buckets
	rl.allow_request('10.0.0.1')
	rl.allow_request('10.0.0.2')
	rl.allow_request('10.0.0.3')

	// Cleanup with very large max_age should remove nothing
	removed := rl.cleanup_stale_buckets(i64(999_999_999))
	assert removed == 0

	// Wait a bit, then cleanup with tiny max_age should remove all
	time.sleep(5 * time.millisecond)
	removed2 := rl.cleanup_stale_buckets(i64(1))
	assert removed2 == 3
}

fn test_multiple_ips_independent_tracking() {
	config := RateLimiterConfig{
		max_requests_per_second:        1000 // high global
		per_ip_max_requests_per_second: 3
		burst_multiplier:               1.0
		window_size_ms:                 i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Exhaust IP1 limit
	for _ in 0 .. 3 {
		rl.allow_request('10.0.0.1')
	}
	assert rl.allow_request('10.0.0.1') == false

	// IP2 should still have its own independent bucket
	assert rl.allow_request('10.0.0.2') == true
	assert rl.allow_request('10.0.0.2') == true
	assert rl.allow_request('10.0.0.2') == true
	assert rl.allow_request('10.0.0.2') == false
}

fn test_rate_limiter_creation() {
	config := RateLimiterConfig{
		max_requests_per_second: 500
		max_bytes_per_second:    i64(50_000_000)
	}

	mut rl := new_rate_limiter(config)

	stats := rl.get_stats()
	assert stats.total_requests == 0
	assert stats.throttled_requests == 0
	assert stats.throttled_bytes == 0
}

fn test_byte_stats_tracking() {
	config := RateLimiterConfig{
		max_bytes_per_second: i64(100)
		burst_multiplier:     1.0
		window_size_ms:       i64(1000)
	}

	mut rl := new_rate_limiter(config)

	// Allow 100 bytes
	rl.allow_bytes('10.0.0.1', i64(100))

	// Reject 50 bytes (over limit)
	rl.allow_bytes('10.0.0.1', i64(50))

	stats := rl.get_stats()
	assert stats.throttled_bytes == 1
}
