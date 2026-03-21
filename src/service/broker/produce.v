// Handles business logic for Kafka Produce requests.
// Used when producers write messages to topic/partition.
module broker

import domain
import service.port

/// ProduceUseCase handles produce request business logic.
/// Responsible for message validation, topic verification, and storage writes.
pub struct ProduceUseCase {
	topic_storage  port.TopicStoragePort
	record_storage port.RecordStoragePort
}

/// new_produce_usecase creates a new ProduceUseCase.
pub fn new_produce_usecase(topic_storage port.TopicStoragePort, record_storage port.RecordStoragePort) &ProduceUseCase {
	return &ProduceUseCase{
		topic_storage:  topic_storage
		record_storage: record_storage
	}
}

/// ProduceRequest represents a produce request.
pub struct ProduceRequest {
pub:
	topic      string
	partition  int
	records    []domain.Record
	acks       i16
	timeout_ms i32
}

/// ProduceResponse represents a produce response.
pub struct ProduceResponse {
pub:
	topic           string
	partition       int
	error_code      i16
	base_offset     i64
	log_append_time i64
}

/// execute processes a produce request.
pub fn (u &ProduceUseCase) execute(req ProduceRequest) !ProduceResponse {
	// Validation
	if req.topic.len == 0 {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.invalid_topic_exception)
		}
	}

	if req.records.len == 0 {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.invalid_request)
		}
	}

	// Verify topic exists
	_ := u.topic_storage.get_topic(req.topic) or {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}

	// Store records to storage
	result := u.record_storage.append(req.topic, req.partition, req.records, req.acks) or {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	return ProduceResponse{
		topic:           req.topic
		partition:       req.partition
		error_code:      0
		base_offset:     result.base_offset
		log_append_time: result.log_append_time
	}
}
