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
