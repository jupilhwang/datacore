// Thrift Compact Protocol (TCompactProtocol) decoder.
// Reads Parquet file metadata (footer) encoded by ThriftWriter.
// Spec: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
module encoding

// ThriftReader decodes data using the Thrift Compact Protocol.
pub struct ThriftReader {
mut:
	buf []u8
	pos int
}

fn new_thrift_reader(data []u8) ThriftReader {
	return ThriftReader{
		buf: data
		pos: 0
	}
}

// unzigzag32 converts unsigned zigzag-encoded value to signed int32.
fn unzigzag32(n u32) i32 {
	return i32((n >> 1) ^ -(n & 1))
}

// unzigzag64 converts unsigned zigzag-encoded value to signed int64.
fn unzigzag64(n u64) i64 {
	return i64((n >> 1) ^ -(n & 1))
}

// read_varint32 reads a varint from the buffer.
// Returns an error if more than 5 bytes are consumed (varint32 max).
fn (mut r ThriftReader) read_varint32() !u32 {
	mut result := u32(0)
	mut shift := 0
	mut bytes_read := 0
	for r.pos < r.buf.len {
		if bytes_read >= 5 {
			return error('varint32 exceeds maximum of 5 bytes')
		}
		b := r.buf[r.pos]
		r.pos++
		bytes_read++
		result |= u32(b & 0x7F) << shift
		if b & 0x80 == 0 {
			return result
		}
		shift += 7
	}
	return error('unexpected EOF while reading varint32')
}

// read_varint64 reads a varint from the buffer.
// Returns an error if more than 10 bytes are consumed (varint64 max).
fn (mut r ThriftReader) read_varint64() !u64 {
	mut result := u64(0)
	mut shift := 0
	mut bytes_read := 0
	for r.pos < r.buf.len {
		if bytes_read >= 10 {
			return error('varint64 exceeds maximum of 10 bytes')
		}
		b := r.buf[r.pos]
		r.pos++
		bytes_read++
		result |= u64(b & 0x7F) << shift
		if b & 0x80 == 0 {
			return result
		}
		shift += 7
	}
	return error('unexpected EOF while reading varint64')
}

// skip_raw_bytes advances the position by n bytes without decoding.
// Returns an error if there are not enough bytes remaining.
fn (mut r ThriftReader) skip_raw_bytes(n int) ! {
	if r.pos + n > r.buf.len {
		return error('skip_raw_bytes: not enough data (need ${n}, have ${r.buf.len - r.pos})')
	}
	r.pos += n
}

// read_struct_begin starts reading a struct (saves field context).
fn (mut r ThriftReader) read_struct_begin() {}

// read_struct_end ends reading a struct (expects stop byte).
fn (mut r ThriftReader) read_struct_end() ! {
	if r.pos < r.buf.len && r.buf[r.pos] == thrift_stop {
		r.pos++
	}
}

// read_field_header reads a field header byte(s).
fn (mut r ThriftReader) read_field_header() !(u8, i16) {
	if r.pos >= r.buf.len {
		return error('unexpected EOF reading field header')
	}
	b := r.buf[r.pos]
	r.pos++

	if b == thrift_stop {
		return error('stop byte encountered')
	}

	field_type := b & 0x0F
	delta := (b >> 4) & 0x0F
	if delta == 0 {
		field_id := r.read_varint32()!
		return field_type, i16(field_id)
	} else {
		return field_type, i16(delta)
	}
}

// read_i32 reads a zigzag-encoded i32.
fn (mut r ThriftReader) read_i32() !i32 {
	n := r.read_varint32()!
	return unzigzag32(n)
}

// read_i64 reads a zigzag-encoded i64.
fn (mut r ThriftReader) read_i64() !i64 {
	n := r.read_varint64()!
	return unzigzag64(n)
}

// read_string reads a length-prefixed string.
fn (mut r ThriftReader) read_string() !string {
	len := r.read_varint32()!
	if r.pos + int(len) > r.buf.len {
		return error('unexpected EOF reading string')
	}
	s := r.buf[r.pos..r.pos + int(len)].bytestr()
	r.pos += int(len)
	return s
}

// read_binary reads a length-prefixed binary blob.
fn (mut r ThriftReader) read_binary() ![]u8 {
	len := r.read_varint32()!
	if r.pos + int(len) > r.buf.len {
		return error('unexpected EOF reading binary')
	}
	data := r.buf[r.pos..r.pos + int(len)].clone()
	r.pos += int(len)
	return data
}

// read_list_begin reads list header (element type and count).
fn (mut r ThriftReader) read_list_begin() !(u8, int) {
	if r.pos >= r.buf.len {
		return error('unexpected EOF reading list')
	}
	b := r.buf[r.pos]
	r.pos++

	mut count := int((b >> 4) & 0x0F)
	elem_type := b & 0x0F

	if count == 15 {
		count = int(r.read_varint32()!)
	}

	return elem_type, count
}

// read_raw_i32 reads a raw i32 value (no field header).
fn (mut r ThriftReader) read_raw_i32() !i32 {
	n := r.read_varint32()!
	return unzigzag32(n)
}

// read_raw_i64 reads a raw i64 value (no field header).
fn (mut r ThriftReader) read_raw_i64() !i64 {
	n := r.read_varint64()!
	return unzigzag64(n)
}
