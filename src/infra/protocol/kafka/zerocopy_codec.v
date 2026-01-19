module kafka

import infra.performance.io

pub struct ZeroCopyReader {
mut:
	reader io.SliceReader
}

pub fn new_zerocopy_reader(data []u8) ZeroCopyReader {
	return ZeroCopyReader{
		reader: io.SliceReader.new(data)
	}
}

// ZeroCopyReader: 고성능 Kafka 프로토콜 파싱용 래퍼
pub fn (r &ZeroCopyReader) remaining() int {
	return r.reader.remaining()
}

pub fn (r &ZeroCopyReader) position() int {
	return r.reader.position
}

pub fn (r &ZeroCopyReader) has_remaining() bool {
	return r.reader.has_remaining()
}

pub fn (mut r ZeroCopyReader) read_i8() !i8 {
	return i8(r.reader.read_u8()!)
}

pub fn (mut r ZeroCopyReader) read_i16() !i16 {
	return r.reader.read_i16_be()!
}

pub fn (mut r ZeroCopyReader) read_i32() !i32 {
	return r.reader.read_i32_be()!
}

pub fn (mut r ZeroCopyReader) read_i64() !i64 {
	return r.reader.read_i64_be()!
}

pub struct SimpleReader {
	data []u8
mut:
	pos int
}

pub fn new_simple_reader(data []u8) SimpleReader {
	return SimpleReader{
		data: data
	}
}

pub fn (r &SimpleReader) remaining() int {
	return r.data.len - r.pos
}

pub fn (r &SimpleReader) position() int {
	return r.pos
}

pub fn (r &SimpleReader) has_remaining() bool {
	return r.pos < r.data.len
}

pub fn (mut r SimpleReader) read_i8() !i8 {
	if r.pos >= r.data.len {
		return error('eof')
	}
	val := r.data[r.pos]
	r.pos++
	return i8(val)
}

pub fn (mut r SimpleReader) read_i16() !i16 {
	if r.pos + 2 > r.data.len {
		return error('eof')
	}
	val := (u16(r.data[r.pos]) << 8) | u16(r.data[r.pos + 1])
	r.pos += 2
	return i16(val)
}

pub fn (mut r SimpleReader) read_i32() !i32 {
	if r.pos + 4 > r.data.len {
		return error('eof')
	}
	val := (u32(r.data[r.pos]) << 24) | (u32(r.data[r.pos + 1]) << 16) | (u32(r.data[r.pos + 2]) << 8) | u32(r.data[
		r.pos + 3])
	r.pos += 4
	return i32(val)
}

pub fn (mut r SimpleReader) read_i64() !i64 {
	if r.pos + 8 > r.data.len {
		return error('eof')
	}
	mut val := (u64(r.data[r.pos]) << 56) | (u64(r.data[r.pos + 1]) << 48) | (u64(r.data[r.pos + 2]) << 40) | (u64(r.data[
		r.pos + 3]) << 32)
	val |= (u64(r.data[r.pos + 4]) << 24) | (u64(r.data[r.pos + 5]) << 16) | (u64(r.data[r.pos + 6]) << 8) | u64(r.data[
		r.pos + 7])
	r.pos += 8
	return i64(val)
}

pub fn (mut r SimpleReader) read_bool() !bool {
	return r.read_i8()! != 0
}

pub fn (mut r SimpleReader) read_uvarint() !u64 {
	mut result := u64(0)
	mut shift := u32(0)
	for {
		if r.pos >= r.data.len {
			return error('eof')
		}
		b := r.data[r.pos]
		r.pos++
		result |= u64(b & 0x7F) << shift
		if b & 0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return error('uvarint overflow')
		}
	}
	return result
}

pub fn (mut r SimpleReader) read_varint() !i64 {
	uval := r.read_uvarint()!
	return i64((uval >> 1) ^ (-(uval & 1)))
}

pub fn (mut r SimpleReader) read_bytes(len int) ![]u8 {
	if r.pos + len > r.data.len {
		return error('eof')
	}
	val := r.data[r.pos..r.pos + len]
	r.pos += len
	return val
}

pub fn (mut r SimpleReader) read_i32_bytes() ![]u8 {
	length := r.read_i32()!
	if length < 0 {
		return []u8{}
	}
	return r.read_bytes(int(length))!
}

fn read_len_string(mut r SimpleReader, len int) !string {
	if len <= 0 {
		return ''
	}
	return r.read_bytes(len)!.bytestr()
}

pub fn (mut r SimpleReader) read_string() !string {
	return read_len_string(mut r, int(r.read_i16()!))
}

pub fn (mut r SimpleReader) read_nullable_string() !string {
	return read_len_string(mut r, int(r.read_i16()!))
}

pub fn (mut r SimpleReader) read_compact_string() !string {
	return read_len_string(mut r, int(r.read_uvarint()!) - 1)
}

pub fn (mut r SimpleReader) read_compact_nullable_string() !string {
	return read_len_string(mut r, int(r.read_uvarint()!) - 1)
}

pub fn (mut r SimpleReader) read_compact_bytes() ![]u8 {
	length := r.read_uvarint()!
	if length == 0 {
		return []u8{}
	}
	actual_len := int(length - 1)
	return r.read_bytes(actual_len)!
}

pub fn (mut r SimpleReader) read_array_length() !int {
	return int(r.read_i32()!)
}

pub fn (mut r SimpleReader) read_compact_array_length() !int {
	length := r.read_uvarint()!
	if length == 0 {
		return -1
	}
	return int(length - 1)
}

pub fn (mut r SimpleReader) skip(n int) ! {
	if r.pos + n > r.data.len {
		return error('eof')
	}
	r.pos += n
}

pub fn (mut r SimpleReader) skip_tagged_fields() ! {
	num_tags := r.read_uvarint()!
	for _ in 0 .. int(num_tags) {
		_ = r.read_uvarint()!
		tag_len := r.read_uvarint()!
		r.skip(int(tag_len))!
	}
}

pub fn (mut r SimpleReader) read_uuid() ![]u8 {
	return r.read_bytes(16)!
}

// 기존 ZeroCopyRequest → SimpleRequest로 이름 변경
pub struct SimpleRequest {
pub:
	header RequestHeader
	body   []u8
}

pub fn parse_request_simple(data []u8) !SimpleRequest {
	mut reader := new_simple_reader(data)
	api_key := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!
	is_flexible := is_flexible_request(api_key, api_version)
	mut client_id := ''
	if !is_flexible {
		client_id = reader.read_string()!
	} else {
		client_id = reader.read_compact_string()!
		reader.skip_tagged_fields()!
	}
	header := RequestHeader{
		api_key:        api_key
		api_version:    api_version
		correlation_id: correlation_id
		client_id:      client_id
	}
	remaining := reader.remaining()
	body := reader.read_bytes(remaining)!
	return SimpleRequest{
		header: header
		body:   body
	}
}

// is_flexible_request 함수는 기존과 동일하게 유지
fn is_flexible_request(api_key i16, version i16) bool {
	match unsafe { ApiKey(api_key) } {
		.api_versions { return false } // Header is always non-flexible
		.metadata { return version >= 9 }
		.produce { return version >= 9 }
		.fetch { return version >= 12 }
		.list_offsets { return version >= 6 }
		.find_coordinator { return version >= 3 }
		.join_group { return version >= 6 }
		.sync_group { return version >= 4 }
		.heartbeat { return version >= 4 }
		.leave_group { return version >= 4 }
		.offset_commit { return version >= 8 }
		.offset_fetch { return version >= 6 }
		.create_topics { return version >= 5 }
		.delete_topics { return version >= 4 }
		.init_producer_id { return version >= 4 }
		.consumer_group_heartbeat { return true }
		.sasl_handshake { return false } // Header is always non-flexible
		.sasl_authenticate { return version >= 2 }
		else { return false }
	}
}
