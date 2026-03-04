module s3

import net.http
import time

// test_inc_error_increments_count verifies inc_error() atomically increments
// the s3_error_count metric under the metrics_lock.
fn test_inc_error_increments_count() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}

	assert adapter.metrics.s3_error_count == 0
	adapter.inc_error()
	assert adapter.metrics.s3_error_count == 1
	adapter.inc_error()
	assert adapter.metrics.s3_error_count == 2
}

// test_build_object_url_path_style verifies URL construction when path-style is enabled.
fn test_build_object_url_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://localhost:9000'
			bucket_name:    'my-bucket'
			region:         'us-east-1'
			use_path_style: true
		}
	}

	url := adapter.build_object_url('my-key')
	assert url == 'http://localhost:9000/my-bucket/my-key'
}

// test_build_object_url_virtual_hosted_style verifies URL construction when virtual-hosted style.
fn test_build_object_url_virtual_hosted_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://localhost:9000'
			bucket_name:    'my-bucket'
			region:         'us-east-1'
			use_path_style: false
		}
	}

	url := adapter.build_object_url('my-key')
	assert url == 'http://localhost:9000/my-key'
}

// test_configure_request_timeouts verifies timeout values are correctly set on requests.
fn test_configure_request_timeouts() {
	adapter := S3StorageAdapter{}
	mut req := http.prepare(http.FetchConfig{
		url:    'http://localhost'
		method: .get
	}) or { return }

	adapter.configure_request_timeouts(mut req)

	assert req.read_timeout == i64(s3_read_timeout_ms) * i64(time.millisecond)
	assert req.write_timeout == i64(s3_write_timeout_ms) * i64(time.millisecond)
}

// test_get_host_returns_correct_host_for_aws verifies the Host header value
// is correctly derived for standard AWS S3 endpoints.
fn test_get_host_returns_correct_host_for_aws() {
	// path-style
	path_adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name:    'my-bucket'
			region:         'us-east-1'
			use_path_style: true
		}
	}
	assert path_adapter.get_host() == 's3.us-east-1.amazonaws.com'

	// virtual-hosted style
	vhost_adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name:    'my-bucket'
			region:         'us-east-1'
			use_path_style: false
		}
	}
	assert vhost_adapter.get_host() == 'my-bucket.s3.us-east-1.amazonaws.com'
}

// test_get_host_returns_correct_host_for_custom_endpoint verifies the Host header
// value for custom endpoints (MinIO/LocalStack).
fn test_get_host_returns_correct_host_for_custom_endpoint() {
	// path-style with custom endpoint
	path_adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://localhost:9000'
			bucket_name:    'my-bucket'
			use_path_style: true
		}
	}
	assert path_adapter.get_host() == 'localhost:9000'

	// virtual-hosted style with custom endpoint
	vhost_adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://localhost:9000'
			bucket_name:    'my-bucket'
			use_path_style: false
		}
	}
	assert vhost_adapter.get_host() == 'my-bucket.localhost:9000'
}
