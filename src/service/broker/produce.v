// Kafka Produce 요청의 비즈니스 로직을 처리합니다.
// 프로듀서가 토픽/파티션에 메시지를 기록할 때 사용됩니다.
module broker

import domain
import service.port

/// ProduceUseCase는 produce 요청 비즈니스 로직을 처리합니다.
/// 메시지 유효성 검사, 토픽 확인, 스토리지 기록을 담당합니다.
pub struct ProduceUseCase {
	storage port.StoragePort
}

/// new_produce_usecase는 새로운 ProduceUseCase를 생성합니다.
pub fn new_produce_usecase(storage port.StoragePort) &ProduceUseCase {
	return &ProduceUseCase{
		storage: storage
	}
}

/// ProduceRequest는 produce 요청을 나타냅니다.
pub struct ProduceRequest {
pub:
	topic      string
	partition  int
	records    []domain.Record
	acks       i16
	timeout_ms i32
}

/// ProduceResponse는 produce 응답을 나타냅니다.
pub struct ProduceResponse {
pub:
	topic           string
	partition       int
	error_code      i16
	base_offset     i64
	log_append_time i64
}

/// execute는 produce 요청을 처리합니다.
pub fn (u &ProduceUseCase) execute(req ProduceRequest) !ProduceResponse {
	// 유효성 검사
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

	// 토픽 존재 확인
	_ := u.storage.get_topic(req.topic) or {
		return ProduceResponse{
			topic:      req.topic
			partition:  req.partition
			error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}

	// 스토리지에 레코드 저장
	result := u.storage.append(req.topic, req.partition, req.records, req.acks) or {
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
