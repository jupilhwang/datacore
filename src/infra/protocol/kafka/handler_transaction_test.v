// Unit tests - Transaction handler 테스트
// InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn,
// WriteTxnMarkers, TxnOffsetCommit 핸들러 동작을 검증한다.
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.transaction as infra_txn
import service.transaction
import service.port

// -- 핸들러 생성 헬퍼 --

fn create_txn_handler_with_coordinator() kafka.Handler {
	storage := TxnHandlerMockStorage{}
	txn_store := infra_txn.new_memory_transaction_store()
	txn_coordinator := transaction.new_transaction_coordinator(txn_store)
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none,
		none, *txn_coordinator, kafka.new_compression_port_adapter(cs))
}

fn create_txn_handler_without_coordinator() kafka.Handler {
	storage := TxnHandlerMockStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, kafka.new_compression_port_adapter(cs))
}

// InitProducerId 실행 후 producer_id, epoch을 반환하는 헬퍼
fn init_producer(mut handler kafka.Handler, txn_id string) (i64, i16) {
	mut req := kafka.new_writer()
	req.write_i32(0)
	req.write_i16(22) // InitProducerId
	req.write_i16(3)
	req.write_i32(1)
	req.write_nullable_string('test-client')
	req.write_tagged_fields()
	req.write_compact_nullable_string(txn_id)
	req.write_i32(60000)
	req.write_i64(-1)
	req.write_i16(-1)
	req.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	resp := handler.handle_request(req.bytes()[4..], mut conn) or {
		panic('InitProducerId 실패: ${err}')
	}

	mut reader := kafka.new_reader(resp)
	_ := reader.read_i32() or { panic('parse 실패') }
	_ := reader.read_i32() or { panic('parse 실패') }
	_ := reader.read_uvarint() or { panic('parse 실패') }
	_ := reader.read_i32() or { panic('parse 실패') }
	_ := reader.read_i16() or { panic('parse 실패') }
	pid := reader.read_i64() or { panic('parse 실패') }
	epoch := reader.read_i16() or { panic('parse 실패') }
	return pid, epoch
}

// -- InitProducerId 핸들러 테스트 --

fn test_init_producer_id_handler_with_coordinator() {
	mut handler := create_txn_handler_with_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(22)
	request.write_i16(3) // flexible version
	request.write_i32(10)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	request.write_compact_nullable_string('txn-1')
	request.write_i32(60000)
	request.write_i64(-1)
	request.write_i16(-1)
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0

	producer_id := reader.read_i64()!
	assert producer_id > 0

	producer_epoch := reader.read_i16()!
	assert producer_epoch == 0
}

fn test_init_producer_id_handler_no_coordinator_idempotent() {
	// coordinator 없이 idempotent 모드 (transactional_id 없음)
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(22)
	request.write_i16(3)
	request.write_i32(11)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	// transactional_id = null (compact nullable string)
	request.write_compact_nullable_string(none)
	request.write_i32(60000)
	request.write_i64(-1)
	request.write_i16(-1)
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0

	producer_id := reader.read_i64()!
	assert producer_id > 0
}

fn test_init_producer_id_handler_no_coordinator_transactional() {
	// coordinator 없이 transactional_id를 지정하면 COORDINATOR_NOT_AVAILABLE
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(22)
	request.write_i16(3)
	request.write_i32(12)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	request.write_compact_nullable_string('my-txn-id')
	request.write_i32(60000)
	request.write_i64(-1)
	request.write_i16(-1)
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 15 // COORDINATOR_NOT_AVAILABLE
}

// -- AddPartitionsToTxn 핸들러 테스트 --

fn test_add_partitions_to_txn_handler_success() {
	mut handler := create_txn_handler_with_coordinator()
	pid, epoch := init_producer(mut handler, 'add-part-txn')

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(24) // AddPartitionsToTxn
	request.write_i16(3) // flexible
	request.write_i32(20)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('add-part-txn')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_compact_array_len(1)
	request.write_compact_string('test-topic')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields()
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!
	_ := reader.read_i32()!

	count := reader.read_compact_array_len()!
	assert count == 1

	name := reader.read_compact_string()!
	assert name == 'test-topic'

	p_count := reader.read_compact_array_len()!
	assert p_count == 1

	p_idx := reader.read_i32()!
	assert p_idx == 0

	error_code := reader.read_i16()!
	assert error_code == 0
}

fn test_add_partitions_to_txn_handler_no_coordinator() {
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(24)
	request.write_i16(3)
	request.write_i32(21)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('some-txn')
	request.write_i64(100)
	request.write_i16(0)
	request.write_compact_array_len(1)
	request.write_compact_string('topic-x')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields()
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!
	_ := reader.read_i32()!

	count := reader.read_compact_array_len()!
	assert count == 1

	_ := reader.read_compact_string()!

	p_count := reader.read_compact_array_len()!
	assert p_count == 1

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 15 // COORDINATOR_NOT_AVAILABLE
}

// -- EndTxn 핸들러 테스트 --

fn test_end_txn_handler_commit_success() {
	mut handler := create_txn_handler_with_coordinator()
	pid, epoch := init_producer(mut handler, 'end-txn-commit')

	// AddPartitions로 상태를 Ongoing으로 전환
	mut add_req := kafka.new_writer()
	add_req.write_i32(0)
	add_req.write_i16(24)
	add_req.write_i16(3)
	add_req.write_i32(30)
	add_req.write_nullable_string('test-client')
	add_req.write_tagged_fields()
	add_req.write_compact_string('end-txn-commit')
	add_req.write_i64(pid)
	add_req.write_i16(epoch)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('topic-1')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()
	mut conn := ?&domain.AuthConnection(none)
	_ := handler.handle_request(add_req.bytes()[4..], mut conn) or { panic('AddPartitions 실패') }

	// EndTxn commit
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(26) // EndTxn
	request.write_i16(3)
	request.write_i32(31)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('end-txn-commit')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_i8(1) // committed = true
	request.write_tagged_fields()

	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0
}

fn test_end_txn_handler_no_coordinator() {
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(26)
	request.write_i16(3)
	request.write_i32(32)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('some-txn')
	request.write_i64(100)
	request.write_i16(0)
	request.write_i8(1)
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 15 // COORDINATOR_NOT_AVAILABLE
}

// -- AddOffsetsToTxn 핸들러 테스트 --

fn test_add_offsets_to_txn_handler_success() {
	mut handler := create_txn_handler_with_coordinator()
	pid, epoch := init_producer(mut handler, 'offsets-txn')

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(25) // AddOffsetsToTxn
	request.write_i16(3)
	request.write_i32(40)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('offsets-txn')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_compact_string('my-group')
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0
}

fn test_add_offsets_to_txn_handler_no_coordinator() {
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(25)
	request.write_i16(3)
	request.write_i32(41)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('some-txn')
	request.write_i64(100)
	request.write_i16(0)
	request.write_compact_string('group-a')
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 15 // COORDINATOR_NOT_AVAILABLE
}

// -- TxnOffsetCommit 핸들러 테스트 --

fn test_txn_offset_commit_handler_no_coordinator() {
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(28) // TxnOffsetCommit
	request.write_i16(3)
	request.write_i32(50)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('txn-no-coord')
	request.write_compact_string('group-1')
	request.write_i64(100)
	request.write_i16(0)
	request.write_i32(-1) // generation_id
	request.write_compact_string('') // member_id
	request.write_compact_nullable_string('') // group_instance_id
	request.write_compact_array_len(1)
	request.write_compact_string('topic-1')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_i64(50)
	request.write_i32(-1)
	request.write_compact_nullable_string('')
	request.write_tagged_fields()
	request.write_tagged_fields()
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()! // throttle_time_ms
	topic_count := reader.read_compact_array_len()!
	assert topic_count == 1

	_ := reader.read_compact_string()! // topic name
	partition_count := reader.read_compact_array_len()!
	assert partition_count == 1

	_ := reader.read_i32()! // partition_index
	error_code := reader.read_i16()!
	assert error_code == 15 // COORDINATOR_NOT_AVAILABLE
}

fn test_txn_offset_commit_handler_success() {
	mut handler := create_txn_handler_with_coordinator()
	pid, epoch := init_producer(mut handler, 'oc-txn')

	// AddPartitions로 상태를 Ongoing으로 전환
	mut add_req := kafka.new_writer()
	add_req.write_i32(0)
	add_req.write_i16(24)
	add_req.write_i16(3)
	add_req.write_i32(51)
	add_req.write_nullable_string('test-client')
	add_req.write_tagged_fields()
	add_req.write_compact_string('oc-txn')
	add_req.write_i64(pid)
	add_req.write_i16(epoch)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('test-topic')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()
	mut conn := ?&domain.AuthConnection(none)
	_ := handler.handle_request(add_req.bytes()[4..], mut conn) or { panic('AddPartitions 실패') }

	// TxnOffsetCommit
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(28)
	request.write_i16(3)
	request.write_i32(52)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('oc-txn')
	request.write_compact_string('consumer-group')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_i32(-1)
	request.write_compact_string('')
	request.write_compact_nullable_string('')
	request.write_compact_array_len(1)
	request.write_compact_string('test-topic')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_i64(100)
	request.write_i32(-1)
	request.write_compact_nullable_string('')
	request.write_tagged_fields()
	request.write_tagged_fields()
	request.write_tagged_fields()

	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	topic_count := reader.read_compact_array_len()!
	assert topic_count == 1

	_ := reader.read_compact_string()!
	partition_count := reader.read_compact_array_len()!
	assert partition_count == 1

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0
}

// -- build_txn_control_records 테스트 --

fn test_build_txn_control_records_commit() {
	records := kafka.build_txn_control_records(12345, 0, true)
	assert records.len == 1

	rec := records[0]
	assert rec.is_control_record == true
	assert rec.control_type == domain.ControlRecordType.commit
	assert rec.producer_id == 12345
	assert rec.producer_epoch == 0
	assert rec.key.len > 0
	assert rec.value.len > 0
}

fn test_build_txn_control_records_abort() {
	records := kafka.build_txn_control_records(99999, 3, false)
	assert records.len == 1

	rec := records[0]
	assert rec.is_control_record == true
	assert rec.control_type == domain.ControlRecordType.abort
	assert rec.producer_id == 99999
	assert rec.producer_epoch == 3
}

// -- WriteTxnMarkers 핸들러 테스트 --

fn test_write_txn_markers_handler_with_coordinator() {
	// coordinator가 있는 경우 WriteTxnMarkers 요청이 정상 처리되어야 한다.
	// mock storage는 topic이 없으므로 UNKNOWN_TOPIC_OR_PARTITION 반환.
	mut handler := create_txn_handler_with_coordinator()
	pid, epoch := init_producer(mut handler, 'wtm-txn')

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(27) // WriteTxnMarkers
	request.write_i16(1) // flexible version
	request.write_i32(60)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	// body: 1 marker
	request.write_compact_array_len(1)
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_i8(1) // committed = true
	// 1 topic
	request.write_compact_array_len(1)
	request.write_compact_string('test-topic')
	// 1 partition
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields() // topic tagged fields
	request.write_i32(0) // coordinator_epoch
	request.write_tagged_fields() // marker tagged fields
	request.write_tagged_fields() // top-level tagged fields

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('WriteTxnMarkers 요청 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // message size
	_ := reader.read_i32()! // correlation id
	_ := reader.read_uvarint()! // tagged fields

	// Response body
	marker_count := reader.read_compact_array_len()!
	assert marker_count == 1

	resp_pid := reader.read_i64()!
	assert resp_pid == pid

	topic_count := reader.read_compact_array_len()!
	assert topic_count == 1

	topic_name := reader.read_compact_string()!
	assert topic_name == 'test-topic'

	partition_count := reader.read_compact_array_len()!
	assert partition_count == 1

	partition_idx := reader.read_i32()!
	assert partition_idx == 0

	// mock storage에 topic이 없으므로 UNKNOWN_TOPIC_OR_PARTITION (3)
	error_code := reader.read_i16()!
	assert error_code == 3
}

fn test_write_txn_markers_handler_without_coordinator() {
	// coordinator 없이도 WriteTxnMarkers가 정상적으로 처리되어야 한다.
	mut handler := create_txn_handler_without_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(27) // WriteTxnMarkers
	request.write_i16(1) // flexible version
	request.write_i32(61)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	request.write_compact_array_len(1)
	request.write_i64(12345)
	request.write_i16(0)
	request.write_i8(0) // abort
	request.write_compact_array_len(1)
	request.write_compact_string('some-topic')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields()
	request.write_i32(0)
	request.write_tagged_fields()
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('WriteTxnMarkers 요청 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	marker_count := reader.read_compact_array_len()!
	assert marker_count == 1

	resp_pid := reader.read_i64()!
	assert resp_pid == 12345

	topic_count := reader.read_compact_array_len()!
	assert topic_count == 1
}

fn test_write_txn_markers_handler_empty_markers() {
	// 빈 markers 요청은 빈 응답을 반환해야 한다.
	mut handler := create_txn_handler_with_coordinator()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(27)
	request.write_i16(1)
	request.write_i32(62)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()
	request.write_compact_array_len(0) // 0 markers
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	marker_count := reader.read_compact_array_len()!
	assert marker_count == 0
}

// -- TxnHandlerMockStorage --

struct TxnHandlerMockStorage {}

fn (m TxnHandlerMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m TxnHandlerMockStorage) delete_topic(name string) ! {}

fn (m TxnHandlerMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m TxnHandlerMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m TxnHandlerMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m TxnHandlerMockStorage) add_partitions(name string, new_count int) ! {}

fn (m TxnHandlerMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m TxnHandlerMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m TxnHandlerMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m TxnHandlerMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m TxnHandlerMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m TxnHandlerMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m TxnHandlerMockStorage) delete_group(group_id string) ! {}

fn (m TxnHandlerMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m TxnHandlerMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m TxnHandlerMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m TxnHandlerMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &TxnHandlerMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &TxnHandlerMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m TxnHandlerMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m TxnHandlerMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m TxnHandlerMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m TxnHandlerMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
