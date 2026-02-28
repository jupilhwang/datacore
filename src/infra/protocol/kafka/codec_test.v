// Unit Tests - Infra Layer: Kafka Codec
module kafka_test

import infra.protocol.kafka

fn test_binary_reader_i16() {
	data := [u8(0x01), 0x02]
	mut reader := kafka.new_reader(data)

	val := reader.read_i16()!
	assert val == 0x0102
}

fn test_binary_reader_i32() {
	data := [u8(0x00), 0x00, 0x01, 0x00]
	mut reader := kafka.new_reader(data)

	val := reader.read_i32()!
	assert val == 256
}

fn test_binary_reader_string() {
	// length (2 bytes) + "hello" (5 bytes)
	data := [u8(0x00), 0x05, u8(`h`), u8(`e`), u8(`l`), u8(`l`), u8(`o`)]
	mut reader := kafka.new_reader(data)

	val := reader.read_string()!
	assert val == 'hello'
}

fn test_binary_writer_i32() {
	mut writer := kafka.new_writer()
	writer.write_i32(256)

	bytes := writer.bytes()
	assert bytes == [u8(0x00), 0x00, 0x01, 0x00]
}

fn test_binary_writer_string() {
	mut writer := kafka.new_writer()
	writer.write_string('hello')

	bytes := writer.bytes()
	// length (2 bytes) + "hello" (5 bytes)
	assert bytes == [u8(0x00), 0x05, u8(`h`), u8(`e`), u8(`l`), u8(`l`), u8(`o`)]
}

fn test_varint_encode_decode() {
	mut writer := kafka.new_writer()
	writer.write_varint(150)

	mut reader := kafka.new_reader(writer.bytes())
	val := reader.read_varint()!
	assert val == 150
}

fn test_varint_negative() {
	mut writer := kafka.new_writer()
	writer.write_varint(-100)

	mut reader := kafka.new_reader(writer.bytes())
	val := reader.read_varint()!
	assert val == -100
}

// test_parse_nested_record_batch_direct_records tests that decompressed Kafka
// RecordBatch data (a sequence of Records, NOT a nested RecordBatch header)
// is parsed correctly.
//
// Hex data: 22 00 00 01 16 74 65 73 74 20 73 6e 61 70 70 79 00
// Decoded:
//   22 = varint 17 (record_length)
//   00 = i8 attributes
//   00 = varint 0 (timestamp_delta, zigzag: 0)
//   00 = varint 0 (offset_delta, zigzag: 0)
//   01 = varint -1 (key_length, zigzag: 1 -> -1, null key)
//   16 = varint 11 (value_length, zigzag: 22 -> 11)
//   74 65 73 74 20 73 6e 61 70 70 79 = "test snappy" (11 bytes)
//   00 = varint 0 (headers_count)
fn test_parse_nested_record_batch_direct_records() {
	data := [
		u8(0x22), // varint record_length = 17
		u8(0x00), // attributes
		u8(0x00), // timestamp_delta (zigzag 0)
		u8(0x00), // offset_delta (zigzag 0)
		u8(0x01), // key_length (zigzag 1 -> -1, null)
		u8(0x16), // value_length (zigzag 22 -> 11)
		u8(0x74),
		u8(0x65),
		u8(0x73),
		u8(0x74),
		u8(0x20), // "test "
		u8(0x73),
		u8(0x6e),
		u8(0x61),
		u8(0x70),
		u8(0x70),
		u8(0x79), // "snappy"
		u8(0x00), // headers_count = 0
	]

	result := kafka.parse_nested_record_batch(data)!

	assert result.records.len == 1, 'expected 1 record, got ${result.records.len}'
	assert result.records[0].value == 'test snappy'.bytes(), 'expected value "test snappy"'
	assert result.records[0].key.len == 0, 'expected null/empty key'
}

// test_parse_nested_record_batch_empty tests empty data returns empty records.
fn test_parse_nested_record_batch_empty() {
	result := kafka.parse_nested_record_batch([])!
	assert result.records.len == 0
}
