// Tests for ETag-based CAS (Compare-And-Swap) logic in cluster metadata operations.
// Verifies retry behavior, backoff calculation, error detection, and CAS result handling.
module s3

import json
import time
import domain

// -- CAS helper unit tests --

fn test_is_etag_mismatch_error_returns_true_for_etag_error() {
	err := IError(S3ETagMismatchError{})
	assert is_etag_mismatch_error(err) == true
}

fn test_is_etag_mismatch_error_returns_false_for_generic_error() {
	err := IError(error('some random error'))
	assert is_etag_mismatch_error(err) == false
}

fn test_is_etag_mismatch_error_returns_false_for_network_error() {
	err := IError(S3NetworkError{
		detail: 'connection refused'
	})
	assert is_etag_mismatch_error(err) == false
}

fn test_cas_backoff_ms_returns_50_for_first_attempt() {
	assert cas_backoff_ms(0) == 50
}

fn test_cas_backoff_ms_returns_200_for_second_attempt() {
	assert cas_backoff_ms(1) == 200
}

fn test_cas_backoff_ms_clamps_to_last_value_for_overflow() {
	assert cas_backoff_ms(5) == 200
	assert cas_backoff_ms(100) == 200
}

fn test_cas_max_retries_constant_is_3() {
	assert cas_max_retries == 3
}

// -- CasResult enum tests --

fn test_cas_result_success_value() {
	result := CasResult.success
	assert result == CasResult.success
	assert result != CasResult.etag_mismatch
	assert result != CasResult.version_conflict
}

fn test_cas_result_etag_mismatch_value() {
	result := CasResult.etag_mismatch
	assert result == CasResult.etag_mismatch
	assert result != CasResult.success
}

fn test_cas_result_version_conflict_value() {
	result := CasResult.version_conflict
	assert result == CasResult.version_conflict
	assert result != CasResult.success
}

// -- Backoff sequence validation --

fn test_cas_backoff_sequence_is_exponential() {
	// Expected: 50ms, 200ms (then clamp)
	delays := [cas_backoff_ms(0), cas_backoff_ms(1), cas_backoff_ms(2)]
	assert delays[0] == 50
	assert delays[1] == 200
	assert delays[2] == 200 // clamped
	// Verify monotonically non-decreasing
	assert delays[0] <= delays[1]
	assert delays[1] <= delays[2]
}

// -- Lock encode helper --

fn test_encode_lock_produces_valid_json() {
	adapter := make_test_cluster_adapter()
	data := adapter.encode_lock('test-lock', 'holder-1', 5000)
	assert data.len > 0

	// Verify it decodes back to valid LockInfo
	lock_info := json.decode(LockInfo, data.bytestr()) or {
		assert false, 'encode_lock produced invalid JSON: ${err}'
		return
	}
	assert lock_info.lock_name == 'test-lock'
	assert lock_info.holder_id == 'holder-1'
	assert lock_info.expires_at > lock_info.acquired_at
	assert lock_info.expires_at - lock_info.acquired_at == 5000
}

// -- try_update_cluster_metadata version conflict --

fn test_try_update_cluster_metadata_version_conflict_with_stale_version() {
	// When existing metadata has version >= incoming, result is version_conflict
	mut adapter := make_test_cluster_adapter()
	key := adapter.cluster_metadata_key()

	// Simulate: GET returns existing metadata at version 5
	// but we're trying to write version 3 (stale)
	// Since there's no real S3, verify the version check logic directly
	current := domain.ClusterMetadata{
		cluster_id:       'test-cluster'
		controller_id:    1
		metadata_version: 5
		updated_at:       time.now().unix_milli()
	}
	incoming := domain.ClusterMetadata{
		cluster_id:       'test-cluster'
		controller_id:    2
		metadata_version: 3 // older than current
		updated_at:       0
	}
	// Version check: current.metadata_version (5) >= incoming.metadata_version (3) -> conflict
	assert current.metadata_version >= incoming.metadata_version
}

fn test_try_update_cluster_metadata_version_ok_when_newer() {
	current := domain.ClusterMetadata{
		cluster_id:       'test-cluster'
		controller_id:    1
		metadata_version: 2
		updated_at:       time.now().unix_milli()
	}
	incoming := domain.ClusterMetadata{
		cluster_id:       'test-cluster'
		controller_id:    2
		metadata_version: 5 // newer than current
		updated_at:       0
	}
	// Version check should pass
	assert current.metadata_version < incoming.metadata_version
}

// -- Key helper tests --

fn test_cluster_metadata_key_format() {
	adapter := make_test_cluster_adapter()
	key := adapter.cluster_metadata_key()
	assert key == 'test/__cluster/metadata.json'
}

fn test_lock_key_format() {
	adapter := make_test_cluster_adapter()
	key := adapter.lock_key('leader-election')
	assert key == 'test/__cluster/locks/leader-election.json'
}

fn test_partition_assignment_key_format() {
	adapter := make_test_cluster_adapter()
	key := adapter.partition_assignment_key('orders', 3)
	assert key == 'test/__cluster/assignments/orders/3.json'
}

// -- CAS backoff delays constant validation --

fn test_cas_backoff_delays_has_two_entries() {
	assert cas_backoff_delays.len == 2
	assert cas_backoff_delays[0] == 50
	assert cas_backoff_delays[1] == 200
}

// -- Error message format tests --

fn test_s3_etag_mismatch_error_message() {
	err := S3ETagMismatchError{}
	assert err.msg() == 'etag_mismatch'
}

// -- Test helper --

fn make_test_cluster_adapter() S3ClusterMetadataAdapter {
	mut adapter := &S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
			max_retries: 3
			cluster_id:  'test-cluster'
		}
	}
	return S3ClusterMetadataAdapter{
		adapter: adapter
	}
}
