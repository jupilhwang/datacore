module replication

import domain
import encoding.binary
import net

// --- Write primitives ---

// write_i16 appends a big-endian i16 to the buffer.
fn write_i16(mut buf []u8, val i16) {
	buf << u8(val >> 8)
	buf << u8(val)
}

// write_i32 appends a big-endian i32 to the buffer.
fn write_i32(mut buf []u8, val i32) {
	buf << u8(val >> 24)
	buf << u8(val >> 16)
	buf << u8(val >> 8)
	buf << u8(val)
}

// write_i64 appends a big-endian i64 to the buffer.
fn write_i64(mut buf []u8, val i64) {
	buf << u8(val >> 56)
	buf << u8(val >> 48)
	buf << u8(val >> 40)
	buf << u8(val >> 32)
	buf << u8(val >> 24)
	buf << u8(val >> 16)
	buf << u8(val >> 8)
	buf << u8(val)
}

// write_string appends a 2-byte length-prefixed UTF-8 string to the buffer.
// Returns error if the string exceeds the i16 maximum (32767 bytes).
fn write_string(mut buf []u8, s string) ! {
	if s.len > 32767 {
		return error('string too long for binary protocol: ${s.len} bytes (max 32767)')
	}
	write_i16(mut buf, i16(s.len))
	if s != '' {
		buf << s.bytes()
	}
}

// write_bytes appends a 4-byte length-prefixed byte slice to the buffer.
fn write_bytes(mut buf []u8, data []u8) {
	write_i32(mut buf, i32(data.len))
	if data.len > 0 {
		buf << data
	}
}

// --- Read primitives ---

// read_i16 reads a big-endian i16 at the given offset.
// Returns error if insufficient data remains.
fn read_i16(data []u8, offset int) !i16 {
	if offset + 2 > data.len {
		return error('insufficient data: need 2 bytes at offset ${offset}, have ${data.len}')
	}
	return i16(binary.big_endian_u16(data[offset..offset + 2]))
}

// read_i32 reads a big-endian i32 at the given offset.
// Returns error if insufficient data remains.
fn read_i32(data []u8, offset int) !i32 {
	if offset + 4 > data.len {
		return error('insufficient data: need 4 bytes at offset ${offset}, have ${data.len}')
	}
	return i32(binary.big_endian_u32(data[offset..offset + 4]))
}

// read_i64 reads a big-endian i64 at the given offset.
// Returns error if insufficient data remains.
fn read_i64(data []u8, offset int) !i64 {
	if offset + 8 > data.len {
		return error('insufficient data: need 8 bytes at offset ${offset}, have ${data.len}')
	}
	return i64(binary.big_endian_u64(data[offset..offset + 8]))
}

// read_string reads a 2-byte length-prefixed string at the given offset.
// Returns the string and the number of bytes consumed (2 + string length).
fn read_string(data []u8, offset int) !(string, int) {
	if offset + 2 > data.len {
		return error('read_string: not enough data for length at offset ${offset}')
	}
	str_len := int(read_i16(data, offset)!)
	if str_len < 0 {
		return error('negative string length: ${str_len}')
	}
	end := offset + 2 + str_len
	if end > data.len {
		return error('read_string: not enough data for string at offset ${offset}, need ${str_len} bytes')
	}
	if str_len == 0 {
		return '', 2
	}
	return data[offset + 2..end].bytestr(), 2 + str_len
}

// read_bytes reads a 4-byte length-prefixed byte slice at the given offset.
// Returns the byte slice and the number of bytes consumed (4 + data length).
fn read_bytes(data []u8, offset int) !([]u8, int) {
	if offset + 4 > data.len {
		return error('read_bytes: not enough data for length at offset ${offset}')
	}
	data_len := int(read_i32(data, offset)!)
	if data_len < 0 {
		return error('negative bytes length: ${data_len}')
	}
	end := offset + 4 + data_len
	if end > data.len {
		return error('read_bytes: not enough data at offset ${offset}, need ${data_len} bytes')
	}
	if data_len == 0 {
		return []u8{}, 4
	}
	return data[offset + 4..end].clone(), 4 + data_len
}

// --- Field decoding ---

// decode_fields reads all message fields starting from the given position.
fn decode_fields(data []u8, start_pos int) !domain.ReplicationMessage {
	mut pos := start_pos

	if pos >= data.len {
		return error('insufficient data: need msg_type byte at offset ${pos}, have ${data.len}')
	}
	msg_type_byte := data[pos]
	pos++
	msg_type := decode_msg_type(msg_type_byte)!

	corr_id, corr_len := read_string(data, pos)!
	pos += corr_len

	sender, sender_len := read_string(data, pos)!
	pos += sender_len

	ts := read_i64(data, pos)!
	pos += 8

	topic, topic_len := read_string(data, pos)!
	pos += topic_len

	partition := read_i32(data, pos)!
	pos += 4

	offset := read_i64(data, pos)!
	pos += 8

	if pos >= data.len {
		return error('insufficient data: need success byte at offset ${pos}, have ${data.len}')
	}
	success := data[pos] == 1
	pos++

	error_msg, err_len := read_string(data, pos)!
	pos += err_len

	records, _ := read_bytes(data, pos)!

	return domain.ReplicationMessage{
		msg_type:       msg_type
		correlation_id: corr_id
		sender_id:      sender
		timestamp:      ts
		topic:          topic
		partition:      partition
		offset:         offset
		records_data:   records
		success:        success
		error_msg:      error_msg
	}
}

// decode_msg_type converts a byte ordinal to a ReplicationType enum value.
fn decode_msg_type(b u8) !domain.ReplicationType {
	return match b {
		0 { domain.ReplicationType.replicate }
		1 { domain.ReplicationType.replicate_ack }
		2 { domain.ReplicationType.flush_ack }
		3 { domain.ReplicationType.heartbeat }
		4 { domain.ReplicationType.recover }
		else { error('unknown binary msg_type ordinal: ${b}') }
	}
}

// --- TCP read ---

// read_exact reads exactly buf.len bytes from the connection.
fn read_exact(mut conn net.TcpConn, mut buf []u8) ! {
	mut total := 0
	for total < buf.len {
		n := conn.read(mut buf[total..]) or { return error('read_exact: ${err}') }
		if n == 0 {
			return error('read_exact: connection closed after ${total}/${buf.len} bytes')
		}
		total += n
	}
}
