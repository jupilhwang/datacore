module domain

/// StorageErrorCode represents typed error codes from the storage layer.
/// Replaces fragile string matching in hot-path error classification.
pub enum StorageErrorCode {
	topic_not_found
	partition_not_found
	offset_out_of_range
	storage_error
	unauthorized
	invalid_argument
}

/// StorageError is a structured error with a typed code.
/// Implements IError for use with V's error propagation (`!` and `or` blocks).
pub struct StorageError {
pub:
	error_code StorageErrorCode
	message    string
}

/// new_storage_error creates a new StorageError wrapped as IError.
pub fn new_storage_error(code StorageErrorCode, message string) IError {
	return IError(StorageError{
		error_code: code
		message:    message
	})
}

/// msg returns the error message (IError interface).
pub fn (e StorageError) msg() string {
	return e.message
}

/// code returns the error code as int (IError interface).
pub fn (e StorageError) code() int {
	return int(e.error_code)
}

/// is_not_found checks if the error is a not-found type.
pub fn (e StorageError) is_not_found() bool {
	return e.error_code == .topic_not_found || e.error_code == .partition_not_found
}

/// is_out_of_range checks if the error is an offset-out-of-range type.
pub fn (e StorageError) is_out_of_range() bool {
	return e.error_code == .offset_out_of_range
}
