// Offset-specific error types.
// Request/response types are defined in service/port/offset_port.v.
module offset

import domain

/// OffsetError represents errors that occur during offset operations.
pub enum OffsetError {
	none
	invalid_group_id
	group_not_found
	invalid_topic
	topic_not_found
	partition_out_of_range
	storage_error
	unknown_error
}

/// to_error_code converts an OffsetError to a Kafka error code.
fn (e OffsetError) to_error_code() i16 {
	return match e {
		.none { 0 }
		.invalid_group_id { i16(domain.ErrorCode.invalid_group_id) }
		.group_not_found { i16(domain.ErrorCode.group_id_not_found) }
		.invalid_topic { i16(domain.ErrorCode.invalid_topic_exception) }
		.topic_not_found { i16(domain.ErrorCode.unknown_topic_or_partition) }
		.partition_out_of_range { i16(domain.ErrorCode.unknown_topic_or_partition) }
		.storage_error { i16(domain.ErrorCode.unknown_server_error) }
		.unknown_error { i16(domain.ErrorCode.unknown_server_error) }
	}
}
