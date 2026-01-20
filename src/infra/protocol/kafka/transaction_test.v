module kafka_test

import domain
import infra.protocol.kafka
import infra.transaction as infra_txn
import service.transaction
import service.port

// Helper: Create Transaction handler
fn create_test_handler_with_transaction() kafka.Handler {
	storage := TransactionMockStorage{}
	txn_store := infra_txn.new_memory_transaction_store()
	txn_coordinator := transaction.new_transaction_coordinator(txn_store)
	
	// Use new_handler_full
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none, none, *txn_coordinator)
}

struct TransactionMockStorage {}
fn (m TransactionMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata { return domain.TopicMetadata{} }
fn (m TransactionMockStorage) delete_topic(name string) ! {}
fn (m TransactionMockStorage) list_topics() ![]domain.TopicMetadata { return []domain.TopicMetadata{} }
fn (m TransactionMockStorage) get_topic(name string) !domain.TopicMetadata { return error('topic not found') }
fn (m TransactionMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata { return error('topic not found') }
fn (m TransactionMockStorage) add_partitions(name string, new_count int) ! {}
fn (m TransactionMockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult { return domain.AppendResult{} }
fn (m TransactionMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult { return domain.FetchResult{} }
fn (m TransactionMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}
fn (m TransactionMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo { return domain.PartitionInfo{} }
fn (m TransactionMockStorage) save_group(group domain.ConsumerGroup) ! {}
fn (m TransactionMockStorage) load_group(group_id string) !domain.ConsumerGroup { return error('group not found') }
fn (m TransactionMockStorage) delete_group(group_id string) ! {}
fn (m TransactionMockStorage) list_groups() ![]domain.GroupInfo { return []domain.GroupInfo{} }
fn (m TransactionMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}
fn (m TransactionMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult { return []domain.OffsetFetchResult{} }
fn (m TransactionMockStorage) health_check() !port.HealthStatus { return .healthy }

fn test_handler_init_producer_id_transactional() {
	mut handler := create_test_handler_with_transaction()

	// InitProducerId with transactional_id
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(22) // InitProducerId
	request.write_i16(3) // version 3
	request.write_i32(1)
	request.write_nullable_string('test-client')
	request.write_tagged_fields() // Header tagged fields (v2 header)
	
	// transactional_id (compact nullable string in v3 flexible)
	// But wait, InitProducerId v3 is flexible?
	// request.v: is_flexible_version(.init_producer_id, version) -> version >= 2
	// So v3 is flexible.
	
	// transactional_id: COMPACT_NULLABLE_STRING
	request.write_compact_nullable_string('my-transactional-id')
	// transaction_timeout_ms: INT32
	request.write_i32(60000)
	// producer_id: INT64 (-1 for new)
	request.write_i64(-1)
	// producer_epoch: INT16 (-1 for new)
	request.write_i16(-1)
	
	// Tagged fields
	request.write_tagged_fields()

	response := handler.handle_request(request.bytes()) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()! // size
	_ = reader.read_i32()! // correlation_id
	_ = reader.read_uvarint()! // tag_buffer (header v1)

	// Body
	_ = reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0
	
	producer_id := reader.read_i64()!
	assert producer_id > 0
	producer_epoch := reader.read_i16()!
	assert producer_epoch == 0
}

fn test_handler_add_partitions_to_txn() {
	mut handler := create_test_handler_with_transaction()

	// 1. InitProducerId to get PID
	mut init_req := kafka.new_writer()
	init_req.write_i32(0)
	init_req.write_i16(22)
	init_req.write_i16(3)
	init_req.write_i32(1)
	init_req.write_nullable_string('test-client')
	init_req.write_tagged_fields() // Header tagged fields
	init_req.write_compact_nullable_string('my-txn-id')
	init_req.write_i32(60000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()
	
	init_resp := handler.handle_request(init_req.bytes()) or { panic(err) }
	mut init_reader := kafka.new_reader(init_resp)
	_ = init_reader.read_i32()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_uvarint()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_i16()!
	pid := init_reader.read_i64()!
	epoch := init_reader.read_i16()!

	// 2. AddPartitionsToTxn
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(24) // AddPartitionsToTxn
	request.write_i16(3) // version 3 (flexible)
	request.write_i32(2)
	request.write_nullable_string('test-client')
	request.write_tagged_fields() // Header tagged fields
	
	// transactional_id
	request.write_compact_string('my-txn-id')
	// producer_id
	request.write_i64(pid)
	// producer_epoch
	request.write_i16(epoch)
	
	// topics array
	request.write_compact_array_len(1)
	// topic 1
	request.write_compact_string('test-topic')
	// partitions array
	request.write_compact_array_len(1)
	request.write_i32(0) // partition 0
	// tagged fields (topic)
	request.write_tagged_fields()
	// tagged fields (request)
	request.write_tagged_fields()

	response := handler.handle_request(request.bytes()) or { panic(err) }
	eprintln('DEBUG: Response len=${response.len} hex=${response.hex()}')

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	_ = reader.read_i32()! // throttle
	
	// results array
	count := reader.read_compact_array_len()!
	eprintln('DEBUG: results count=${count}')
	assert count == 1
	
	eprintln('DEBUG: remaining before name=${reader.remaining()}')
	name := reader.read_compact_string()!
	eprintln('DEBUG: topic name=${name}')
	assert name == 'test-topic'
	
	p_count := reader.read_compact_array_len()!
	assert p_count == 1
	
	p_idx := reader.read_i32()!
	assert p_idx == 0
	
	error_code := reader.read_i16()!
	assert error_code == 0
	
	// Skip partition tagged fields
	reader.skip_tagged_fields()!
	
	// Skip topic tagged fields
	reader.skip_tagged_fields()!
	
	// Skip response tagged fields
	reader.skip_tagged_fields()!
}

fn test_handler_end_txn_commit() {
	mut handler := create_test_handler_with_transaction()

	// 1. Init
	mut init_req := kafka.new_writer()
	init_req.write_i32(0)
	init_req.write_i16(22)
	init_req.write_i16(3)
	init_req.write_i32(1)
	init_req.write_nullable_string('test-client')
	init_req.write_tagged_fields() // Header tagged fields
	init_req.write_compact_nullable_string('my-txn-id')
	init_req.write_i32(60000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()
	init_resp := handler.handle_request(init_req.bytes()) or { panic(err) }
	mut init_reader := kafka.new_reader(init_resp)
	_ = init_reader.read_i32()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_uvarint()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_i16()!
	pid := init_reader.read_i64()!
	epoch := init_reader.read_i16()!

	// 2. Add Partitions (to make state Ongoing)
	mut add_req := kafka.new_writer()
	add_req.write_i32(0)
	add_req.write_i16(24)
	add_req.write_i16(3)
	add_req.write_i32(2)
	add_req.write_nullable_string('test-client')
	add_req.write_tagged_fields() // Header tagged fields
	add_req.write_compact_string('my-txn-id')
	add_req.write_i64(pid)
	add_req.write_i16(epoch)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('test-topic')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()
	_ = handler.handle_request(add_req.bytes()) or { panic(err) }

	// 3. EndTxn (Commit)
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(26) // EndTxn
	request.write_i16(3) // version 3 (flexible)
	request.write_i32(3)
	request.write_nullable_string('test-client')
	request.write_tagged_fields() // Header tagged fields
	
	request.write_compact_string('my-txn-id')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_i8(1) // transaction_result: true (COMMIT)
	request.write_tagged_fields()

	response := handler.handle_request(request.bytes()) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	_ = reader.read_i32()! // throttle
	error_code := reader.read_i16()!
	assert error_code == 0
	
	// Skip tagged fields
	reader.skip_tagged_fields()!
}
