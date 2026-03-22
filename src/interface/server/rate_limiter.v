/// Interface Layer - Rate Limiter
///
/// This module implements token bucket based rate limiting for the TCP server.
/// Provides both global and per-IP request rate limiting,
/// as well as byte throughput rate limiting.
///
/// Key features:
/// - Token bucket algorithm with configurable refill rates
/// - Global request rate limiting
/// - Per-IP request rate limiting
/// - Byte throughput rate limiting
/// - Burst capacity via configurable multiplier
/// - Thread-safe with Mutex protection
/// - Stale bucket cleanup for memory management
module server

import sync
import time

/// RateLimiterConfig holds rate limiting configuration.
pub struct RateLimiterConfig {
pub:
	max_requests_per_second        int = 1000
	max_bytes_per_second           i64 = i64(100_000_000)
	per_ip_max_requests_per_second int = 200
	per_ip_max_connections         int = 50
	burst_multiplier               f64 = 1.5
	window_size_ms                 i64 = i64(1000)
}

/// TokenBucket implements a token bucket algorithm for rate limiting.
/// Tokens are consumed on each request and refilled over time.
pub struct TokenBucket {
pub mut:
	tokens      f64
	max_tokens  f64
	refill_rate f64 // tokens per millisecond
	last_refill i64 // timestamp in milliseconds
}

/// RateLimiterStats tracks rate limiter statistics.
pub struct RateLimiterStats {
pub mut:
	total_requests     i64
	throttled_requests i64
	throttled_bytes    i64
}

/// RateLimiter provides global and per-IP rate limiting.
/// Uses token bucket algorithm for smooth rate enforcement.
pub struct RateLimiter {
mut:
	mtx                 sync.Mutex
	config              RateLimiterConfig
	global_bucket       TokenBucket
	per_ip_buckets      map[string]TokenBucket
	global_bytes_bucket TokenBucket
	stats               RateLimiterStats
}

/// new_token_bucket creates a new token bucket.
/// max_tokens: maximum burst capacity.
/// refill_rate: tokens added per millisecond.
pub fn new_token_bucket(max_tokens f64, refill_rate f64) TokenBucket {
	now_ms := time.now().unix_milli()
	return TokenBucket{
		tokens:      max_tokens
		max_tokens:  max_tokens
		refill_rate: refill_rate
		last_refill: now_ms
	}
}

/// try_consume attempts to consume the given number of tokens.
/// Refills tokens based on elapsed time before consuming.
/// Returns true if tokens were consumed, false if insufficient.
pub fn (mut tb TokenBucket) try_consume(tokens f64) bool {
	tb.refill()

	if tb.tokens >= tokens {
		tb.tokens -= tokens
		return true
	}
	return false
}

/// can_consume checks if the given number of tokens are available.
/// Refills tokens based on elapsed time but does NOT consume them.
/// Use for pre-checking before committing to a multi-bucket consume.
pub fn (mut tb TokenBucket) can_consume(amount f64) bool {
	tb.refill()
	return tb.tokens >= amount
}

/// refill adds tokens based on elapsed time since last refill.
/// Caps tokens at max_tokens to prevent unbounded accumulation.
fn (mut tb TokenBucket) refill() {
	now_ms := time.now().unix_milli()
	elapsed := now_ms - tb.last_refill

	if elapsed > 0 {
		tb.tokens += f64(elapsed) * tb.refill_rate
		if tb.tokens > tb.max_tokens {
			tb.tokens = tb.max_tokens
		}
		tb.last_refill = now_ms
	}
}

/// new_rate_limiter creates a new rate limiter with the given configuration.
/// Initializes global request and byte buckets based on config.
pub fn new_rate_limiter(config RateLimiterConfig) &RateLimiter {
	global_max := f64(config.max_requests_per_second) * config.burst_multiplier
	global_refill := f64(config.max_requests_per_second) / f64(config.window_size_ms)

	bytes_max := f64(config.max_bytes_per_second) * config.burst_multiplier
	bytes_refill := f64(config.max_bytes_per_second) / f64(config.window_size_ms)

	return &RateLimiter{
		config:              config
		global_bucket:       new_token_bucket(global_max, global_refill)
		per_ip_buckets:      map[string]TokenBucket{}
		global_bytes_bucket: new_token_bucket(bytes_max, bytes_refill)
	}
}

/// allow_request checks if a request from the given IP is allowed.
/// Checks both global and per-IP rate limits.
/// Returns true if the request is within limits.
pub fn (mut rl RateLimiter) allow_request(client_ip string) bool {
	rl.mtx.@lock()
	defer { rl.mtx.unlock() }

	rl.stats.total_requests += 1

	// Check global limit first
	if !rl.global_bucket.try_consume(1.0) {
		rl.stats.throttled_requests += 1
		return false
	}

	// Check per-IP limit
	if !rl.consume_per_ip(client_ip) {
		rl.stats.throttled_requests += 1
		return false
	}

	return true
}

/// allow_request_with_bytes checks both request and byte limits under a single lock.
/// Uses a pre-check pattern: all buckets are checked (non-consuming) first,
/// then consumed only if ALL checks pass. This prevents token leakage
/// when a later bucket rejects the request.
/// Returns true only if both request and byte limits are within bounds.
pub fn (mut rl RateLimiter) allow_request_with_bytes(client_ip string, bytes i64) bool {
	rl.mtx.@lock()
	defer { rl.mtx.unlock() }

	rl.stats.total_requests += 1

	// Phase 1: Pre-check all buckets (non-consuming)
	if !rl.global_bucket.can_consume(1.0) {
		rl.stats.throttled_requests += 1
		return false
	}

	if !rl.can_consume_per_ip(client_ip) {
		rl.stats.throttled_requests += 1
		return false
	}

	if bytes > 0 && !rl.global_bytes_bucket.can_consume(f64(bytes)) {
		rl.stats.throttled_bytes += 1
		return false
	}

	// Phase 2: Consume from all buckets (all checks passed)
	rl.global_bucket.try_consume(1.0)
	rl.consume_per_ip(client_ip)
	if bytes > 0 {
		rl.global_bytes_bucket.try_consume(f64(bytes))
	}

	return true
}

/// consume_per_ip checks and consumes a token from the per-IP bucket.
/// Creates a new bucket for unseen IPs.
/// Must be called while holding the mutex.
fn (mut rl RateLimiter) consume_per_ip(client_ip string) bool {
	per_ip_max := f64(rl.config.per_ip_max_requests_per_second) * rl.config.burst_multiplier
	per_ip_refill := f64(rl.config.per_ip_max_requests_per_second) / f64(rl.config.window_size_ms)

	if client_ip in rl.per_ip_buckets {
		mut bucket := rl.per_ip_buckets[client_ip]
		result := bucket.try_consume(1.0)
		rl.per_ip_buckets[client_ip] = bucket
		return result
	}

	// Create new bucket for this IP
	mut bucket := new_token_bucket(per_ip_max, per_ip_refill)
	result := bucket.try_consume(1.0)
	rl.per_ip_buckets[client_ip] = bucket
	return result
}

/// can_consume_per_ip checks if a per-IP token is available without consuming.
/// Creates a new bucket for unseen IPs so subsequent consume_per_ip works correctly.
/// Must be called while holding the mutex.
fn (mut rl RateLimiter) can_consume_per_ip(client_ip string) bool {
	per_ip_max := f64(rl.config.per_ip_max_requests_per_second) * rl.config.burst_multiplier
	per_ip_refill := f64(rl.config.per_ip_max_requests_per_second) / f64(rl.config.window_size_ms)

	if client_ip in rl.per_ip_buckets {
		mut bucket := rl.per_ip_buckets[client_ip]
		result := bucket.can_consume(1.0)
		rl.per_ip_buckets[client_ip] = bucket
		return result
	}

	// New IP: create bucket (full tokens -> always available)
	rl.per_ip_buckets[client_ip] = new_token_bucket(per_ip_max, per_ip_refill)
	return true
}

/// allow_bytes checks if a byte transfer is within the rate limit.
/// Returns true if the bytes are within the global byte rate limit.
pub fn (mut rl RateLimiter) allow_bytes(client_ip string, bytes i64) bool {
	rl.mtx.@lock()
	defer { rl.mtx.unlock() }

	if !rl.global_bytes_bucket.try_consume(f64(bytes)) {
		rl.stats.throttled_bytes += 1
		return false
	}

	return true
}

/// get_stats returns a snapshot of rate limiter statistics.
pub fn (mut rl RateLimiter) get_stats() RateLimiterStats {
	rl.mtx.@lock()
	defer { rl.mtx.unlock() }

	return rl.stats
}

/// cleanup_stale_buckets removes per-IP buckets not used within max_age_ms.
/// Returns the number of buckets removed.
pub fn (mut rl RateLimiter) cleanup_stale_buckets(max_age_ms i64) int {
	rl.mtx.@lock()
	defer { rl.mtx.unlock() }

	now_ms := time.now().unix_milli()
	mut to_remove := []string{cap: 8}

	for ip, bucket in rl.per_ip_buckets {
		age := now_ms - bucket.last_refill
		if age > max_age_ms {
			to_remove << ip
		}
	}

	for ip in to_remove {
		rl.per_ip_buckets.delete(ip)
	}

	return to_remove.len
}
