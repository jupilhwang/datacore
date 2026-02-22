module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.transaction as infra_txn
import service.transaction
import service.port

// helper: create transaction handler
fn create_test_handler_with_transaction() kafka.Handler {
	storage := TransactionMockStorage{}
	txn_store := infra_txn.new_memory_transaction_store()
	txn_coordinator := transaction.new_transaction_coordinator(txn_store)

	// Create compression service
	compression_service := compression.new_default_compression_service() or {
		panic('failed to create compression service: ${err}')
	}

	// Use new_handler_full
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none,
		none, *txn_coordinator, compression_service)
}

struct TransactionMockStorage {}

fn (m TransactionMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m TransactionMockStorage) delete_topic(name string) ! {}

fn (m TransactionMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m TransactionMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m TransactionMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m TransactionMockStorage) add_partitions(name string, new_count int) ! {}

fn (m TransactionMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m TransactionMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m TransactionMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m TransactionMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m TransactionMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m TransactionMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m TransactionMockStorage) delete_group(group_id string) ! {}

fn (m TransactionMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m TransactionMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m TransactionMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m TransactionMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &TransactionMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m TransactionMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m TransactionMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m TransactionMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m TransactionMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn (m &TransactionMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn test_handler_init_producer_id_transactional() {
	mut handler := create_test_handler_with_transaction()

	// InitProducerId with transactional_id
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(22)
	request.write_i16(3)
	request.write_i32(1)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

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

	mut txn_conn_1 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut txn_conn_1) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	// Body
	_ = reader.read_i32()!
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
	init_req.write_tagged_fields()
	init_req.write_compact_nullable_string('my-txn-id')
	init_req.write_i32(60000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()

	mut txn_conn_2 := ?&domain.AuthConnection(none)
	init_resp := handler.handle_request(init_req.bytes()[4..], mut txn_conn_2) or { panic(err) }
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
	request.write_i16(24)
	request.write_i16(3)
	request.write_i32(2)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

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
	request.write_i32(0)
	// tagged fields (topic)
	request.write_tagged_fields()
	// tagged fields (request)
	request.write_tagged_fields()

	mut txn_conn_3 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut txn_conn_3) or { panic(err) }
	eprintln('DEBUG: Response len=${response.len} hex=${response.hex()}')

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	_ = reader.read_i32()!

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
	init_req.write_tagged_fields()
	init_req.write_compact_nullable_string('my-txn-id')
	init_req.write_i32(60000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()
	mut txn_conn_4 := ?&domain.AuthConnection(none)
	init_resp := handler.handle_request(init_req.bytes()[4..], mut txn_conn_4) or { panic(err) }
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
	add_req.write_tagged_fields()
	add_req.write_compact_string('my-txn-id')
	add_req.write_i64(pid)
	add_req.write_i16(epoch)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('test-topic')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()
	mut txn_conn_5 := ?&domain.AuthConnection(none)
	_ = handler.handle_request(add_req.bytes()[4..], mut txn_conn_5) or { panic(err) }

	// 3. EndTxn (Commit)
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(26)
	request.write_i16(3)
	request.write_i32(3)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_string('my-txn-id')
	request.write_i64(pid)
	request.write_i16(epoch)
	request.write_i8(1)
	request.write_tagged_fields()

	mut txn_conn_6 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut txn_conn_6) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	_ = reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0

	// Skip tagged fields
	reader.skip_tagged_fields()!
}

fn test_handler_write_txn_markers() {
	// Create a handler with a mock storage that supports topics
	mut handler := create_test_handler_with_write_txn_markers_storage()!

	// WriteTxnMarkers (API Key 27) v1 is flexible
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(27)
	request.write_i16(1)
	request.write_i32(1)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	// Body: markers array
	request.write_compact_array_len(1)

	// Marker 1:
	request.write_i64(12345)
	request.write_i16(0)
	request.write_i8(1)

	// topics array
	request.write_compact_array_len(1)
	request.write_compact_string('test-topic')
	// partition_indexes array
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields()

	request.write_i32(1)
	request.write_tagged_fields()

	request.write_tagged_fields()

	mut txn_conn_7 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut txn_conn_7) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	// Body: markers array
	markers_count := reader.read_compact_array_len()!
	assert markers_count == 1

	// Marker result
	producer_id := reader.read_i64()!
	assert producer_id == 12345

	// topics array
	topics_count := reader.read_compact_array_len()!
	assert topics_count == 1

	topic_name := reader.read_compact_string()!
	assert topic_name == 'test-topic'

	// partitions array
	partitions_count := reader.read_compact_array_len()!
	assert partitions_count == 1

	partition_index := reader.read_i32()!
	assert partition_index == 0

	error_code := reader.read_i16()!
	assert error_code == 0

	// Skip tagged fields
	reader.skip_tagged_fields()!
	reader.skip_tagged_fields()!
	reader.skip_tagged_fields()!
	reader.skip_tagged_fields()!
}

fn test_write_txn_markers_unknown_topic() {
	// Use the original mock storage that returns topic not found
	mut handler := create_test_handler_with_transaction()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(27)
	request.write_i16(1)
	request.write_i32(1)
	request.write_nullable_string('test-client')
	request.write_tagged_fields()

	request.write_compact_array_len(1)
	request.write_i64(12345)
	request.write_i16(0)
	request.write_i8(1)
	request.write_compact_array_len(1)
	request.write_compact_string('nonexistent-topic')
	request.write_compact_array_len(1)
	request.write_i32(0)
	request.write_tagged_fields()
	request.write_i32(1)
	request.write_tagged_fields()
	request.write_tagged_fields()

	mut txn_conn_8 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut txn_conn_8) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_uvarint()!

	markers_count := reader.read_compact_array_len()!
	assert markers_count == 1

	producer_id := reader.read_i64()!
	assert producer_id == 12345

	topics_count := reader.read_compact_array_len()!
	assert topics_count == 1

	topic_name := reader.read_compact_string()!
	assert topic_name == 'nonexistent-topic'

	partitions_count := reader.read_compact_array_len()!
	assert partitions_count == 1

	partition_index := reader.read_i32()!
	assert partition_index == 0

	error_code := reader.read_i16()!
	assert error_code == 3
}

// helper: create handler with WriteTxnMarkers-capable mock storage
fn create_test_handler_with_write_txn_markers_storage() !kafka.Handler {
	storage := WriteTxnMarkersMockStorage{}
	txn_store := infra_txn.new_memory_transaction_store()
	txn_coordinator := transaction.new_transaction_coordinator(txn_store)
	compression_service := compression.new_compression_service(compression.CompressionConfig{})!
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none,
		none, *txn_coordinator, compression_service)
}

// Mock storage with topic support for WriteTxnMarkers tests
struct WriteTxnMarkersMockStorage {}

fn (m WriteTxnMarkersMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m WriteTxnMarkersMockStorage) delete_topic(name string) ! {}

fn (m WriteTxnMarkersMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m WriteTxnMarkersMockStorage) get_topic(name string) !domain.TopicMetadata {
	if name == 'test-topic' {
		return domain.TopicMetadata{
			name:            'test-topic'
			partition_count: 3
		}
	}
	return error('topic not found')
}

fn (m WriteTxnMarkersMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m WriteTxnMarkersMockStorage) add_partitions(name string, new_count int) ! {}

fn (m WriteTxnMarkersMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{
		base_offset:      0
		log_append_time:  0
		log_start_offset: 0
		record_count:     records.len
	}
}

fn (m WriteTxnMarkersMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m WriteTxnMarkersMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m WriteTxnMarkersMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m WriteTxnMarkersMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m WriteTxnMarkersMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m WriteTxnMarkersMockStorage) delete_group(group_id string) ! {}

fn (m WriteTxnMarkersMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m WriteTxnMarkersMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m WriteTxnMarkersMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m WriteTxnMarkersMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &WriteTxnMarkersMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m WriteTxnMarkersMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m WriteTxnMarkersMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m WriteTxnMarkersMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m WriteTxnMarkersMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn (m &WriteTxnMarkersMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}
