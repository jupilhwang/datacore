// UseCase Layer - Produce UseCase
module broker

import domain
import service.port

// ProduceUseCase handles produce request business logic
pub struct ProduceUseCase {
	storage port.StoragePort
}

pub fn new_produce_usecase(storage port.StoragePort) &ProduceUseCase {
	return &ProduceUseCase{
		storage: storage
	}
}

// ProduceRequest represents a produce request
pub struct ProduceRequest {
pub:
	topic      string
	partition  int
	records    []domain.Record
	acks       i16
	timeout_ms i32
}

// ProduceResponse represents a produce response
pub struct ProduceResponse {
pub:
	topic           string
	partition       int
	error_code      i16
	base_offset     i64
	log_append_time i64
}

// Execute processes the produce request
pub fn (u &ProduceUseCase) execute(req ProduceRequest) !ProduceResponse {
	// Validate request
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

	// Check topic exists
	_ := u.storage.get_topic(req.topic) or {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}

	// Append records to storage
	result := u.storage.append(req.topic, req.partition, req.records) or {
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
