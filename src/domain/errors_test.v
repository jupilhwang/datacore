// Unit tests - domain layer: StorageError typed error codes
module domain

// --- StorageErrorCode enum tests ---

fn test_storage_error_code_values_are_distinct() {
	codes := [
		StorageErrorCode.topic_not_found,
		StorageErrorCode.partition_not_found,
		StorageErrorCode.offset_out_of_range,
		StorageErrorCode.storage_error,
		StorageErrorCode.unauthorized,
		StorageErrorCode.invalid_argument,
	]
	for i in 0 .. codes.len {
		for j in i + 1 .. codes.len {
			assert codes[i] != codes[j], 'enum values at index ${i} and ${j} must be distinct'
		}
	}
}

// --- StorageError construction tests ---

fn test_new_storage_error_sets_code_and_message() {
	ierr := new_storage_error(.topic_not_found, 'topic not found: my-topic')
	err := ierr as StorageError

	assert err.error_code == StorageErrorCode.topic_not_found
	assert err.message == 'topic not found: my-topic'
}

fn test_storage_error_msg_returns_message() {
	ierr := new_storage_error(.offset_out_of_range, 'offset 99 out of range')
	err := ierr as StorageError

	assert err.msg() == 'offset 99 out of range'
}

fn test_storage_error_code_returns_int() {
	ierr := new_storage_error(.topic_not_found, 'topic not found')
	err := ierr as StorageError

	assert err.code() == int(StorageErrorCode.topic_not_found)
}

// --- is_not_found tests ---

fn test_is_not_found_true_for_topic_not_found() {
	ierr := new_storage_error(.topic_not_found, 'topic not found')
	err := ierr as StorageError

	assert err.is_not_found() == true
}

fn test_is_not_found_true_for_partition_not_found() {
	ierr := new_storage_error(.partition_not_found, 'partition not found')
	err := ierr as StorageError

	assert err.is_not_found() == true
}

fn test_is_not_found_false_for_other_codes() {
	codes := [
		StorageErrorCode.offset_out_of_range,
		StorageErrorCode.storage_error,
		StorageErrorCode.unauthorized,
		StorageErrorCode.invalid_argument,
	]
	for code in codes {
		ierr := new_storage_error(code, 'some error')
		err := ierr as StorageError
		assert err.is_not_found() == false, 'is_not_found should be false for ${code}'
	}
}

// --- is_out_of_range tests ---

fn test_is_out_of_range_true_for_offset_out_of_range() {
	ierr := new_storage_error(.offset_out_of_range, 'offset out of range')
	err := ierr as StorageError

	assert err.is_out_of_range() == true
}

fn test_is_out_of_range_false_for_other_codes() {
	codes := [
		StorageErrorCode.topic_not_found,
		StorageErrorCode.partition_not_found,
		StorageErrorCode.storage_error,
		StorageErrorCode.unauthorized,
		StorageErrorCode.invalid_argument,
	]
	for code in codes {
		ierr := new_storage_error(code, 'some error')
		err := ierr as StorageError
		assert err.is_out_of_range() == false, 'is_out_of_range should be false for ${code}'
	}
}

// --- IError interface compliance ---

fn test_storage_error_works_as_v_error() {
	result := fn () !string {
		return new_storage_error(.topic_not_found, 'topic X not found')
	}() or {
		assert err.msg().contains('topic X not found')
		return
	}

	// Should not reach here
	assert false, 'expected error but got: ${result}'
}

fn test_storage_error_type_check_with_is() {
	result := fn () !string {
		return new_storage_error(.offset_out_of_range, 'offset 42 out of range')
	}() or {
		if err is StorageError {
			assert err.error_code == StorageErrorCode.offset_out_of_range
			assert err.is_out_of_range() == true
		} else {
			assert false, 'expected StorageError type'
		}
		return
	}

	assert false, 'expected error but got: ${result}'
}
