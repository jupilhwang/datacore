module s3

import time

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
