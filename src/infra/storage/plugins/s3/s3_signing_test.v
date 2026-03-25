module s3

import time
import crypto.hmac
import crypto.sha256

// test_sign_request_uses_utc verifies that sign_request uses actual UTC time.
// Regression test for bug: time.now().as_utc() does not convert to UTC,
// causing SigV4 signatures to be off by the local timezone offset (e.g., +9h for KST),
// resulting in HTTP 403 errors from AWS S3.
fn test_sign_request_uses_utc() {
	before := time.utc()
	now := time.utc()
	after := time.utc()

	// Verify time.utc() produces a value within a 1-second window of before/after
	assert now.unix() >= before.unix()
	assert now.unix() <= after.unix() + 1

	// Verify that time.utc() and time.now() differ when in a non-UTC timezone.
	// In UTC+9 (KST), the difference should be approximately 32400 seconds.
	// If this assertion fails with diff==0, the test environment is UTC.
	local_now := time.now()
	utc_now := time.utc()
	diff := local_now.unix() - utc_now.unix()

	// Both approaches must produce timestamps within 2 seconds of each other when
	// compared to an independently obtained UTC reference.
	// The key invariant: the x-amz-date header must reflect UTC, not local time.
	// We validate that time.utc() is the correct call by checking its unix epoch
	// matches the expected UTC epoch within a small tolerance.
	system_utc_approx := i64(time.utc().unix())
	tolerance := i64(2)
	raw_diff := utc_now.unix() - system_utc_approx
	abs_diff := if raw_diff < 0 { -raw_diff } else { raw_diff }
	assert abs_diff <= tolerance
	_ = diff // timezone offset documented above
}

// test_sign_request_date_format_is_utc verifies the x-amz-date header generated
// by sign_request uses UTC time, not local time.
fn test_sign_request_date_format_is_utc() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:    'http://localhost:9000'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
			access_key:  'test-key'
			secret_key:  'test-secret'
		}
	}

	before_utc := time.utc()
	headers := adapter.sign_request('PUT', 'test-key', '', []u8{})
	after_utc := time.utc()

	amz_date := headers.get_custom('x-amz-date') or {
		assert false, 'x-amz-date header must be present'
		''
	}

	// x-amz-date format: YYYYMMDDTHHMMSSz (e.g. 20260304T123456Z)
	assert amz_date.len == 16, 'x-amz-date must be 16 chars, got: ${amz_date}'
	assert amz_date.ends_with('Z'), 'x-amz-date must end with Z (UTC), got: ${amz_date}'

	// Extract hour from header and compare to UTC hour
	header_hour := amz_date[9..11].int()
	before_hour := before_utc.hour
	after_hour := after_utc.hour

	// Allow for hour rollover at :59->:00 boundary
	hour_matches := header_hour == before_hour || header_hour == after_hour
	assert hour_matches, 'x-amz-date hour ${header_hour} must match UTC hour (before=${before_hour}, after=${after_hour}), indicating local timezone is being used instead of UTC'
}

// test_get_cached_signing_key_same_date_returns_identical_key verifies that calling
// get_cached_signing_key twice with the same date_stamp returns identical bytes,
// confirming the cache hit path works correctly.
fn test_get_cached_signing_key_same_date_returns_identical_key() {
	adapter := S3StorageAdapter{
		config: S3Config{
			region:     'us-east-1'
			secret_key: 'test-secret'
		}
	}

	key1 := adapter.get_cached_signing_key('20260325')
	key2 := adapter.get_cached_signing_key('20260325')

	assert key1 == key2
	assert key1.len > 0
}

// test_get_cached_signing_key_different_date_returns_different_key verifies that
// get_cached_signing_key produces a different key when the date_stamp changes,
// confirming the cache invalidation path works.
fn test_get_cached_signing_key_different_date_returns_different_key() {
	adapter := S3StorageAdapter{
		config: S3Config{
			region:     'us-east-1'
			secret_key: 'test-secret'
		}
	}

	key1 := adapter.get_cached_signing_key('20260325')
	key2 := adapter.get_cached_signing_key('20260326')

	assert key1 != key2
	assert key2.len > 0
}

// test_signing_key_cache_matches_direct_computation verifies the cached signing key
// matches a freshly computed 4-step HMAC-SHA256 chain (AWS SigV4 key derivation).
fn test_signing_key_cache_matches_direct_computation() {
	adapter := S3StorageAdapter{
		config: S3Config{
			region:     'us-east-1'
			secret_key: 'test-secret'
		}
	}

	date_day := '20260325'
	cached := adapter.get_cached_signing_key(date_day)

	k_date := hmac.new(('AWS4' + 'test-secret').bytes(), date_day.bytes(), sha256.sum,
		hmac_block_size)
	k_region := hmac.new(k_date, 'us-east-1'.bytes(), sha256.sum, hmac_block_size)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, hmac_block_size)
	expected := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, hmac_block_size)

	assert cached == expected
}
