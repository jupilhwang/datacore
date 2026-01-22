// 어댑터 레이어 - Kafka 바이너리 코덱
// 빅 엔디안 형식의 바이너리 리더/라이터 (Kafka 와이어 프로토콜)
module kafka

import domain
import time
import infra.protocol.kafka.crc32c

/// ByteView - 바이트 배열에 대한 Zero-copy 뷰 (경량 대안)
/// 메모리 할당 없이 고성능 파싱을 위해 내부적으로 사용됩니다.
pub struct ByteView {
pub:
	data   []u8 // 기본 데이터에 대한 참조 (소유하지 않음)
	offset int  // 원본 배열에서의 시작 오프셋
	length int  // 이 뷰의 길이
}

/// new는 바이트 배열로부터 새로운 ByteView를 생성합니다.
pub fn ByteView.new(data []u8) ByteView {
	return ByteView{
		data:   data
		offset: 0
		length: data.len
	}
}

// 데이터 복사 없이 서브 뷰를 생성합니다.
/// slice는 데이터 복사 없이 서브 뷰를 생성합니다.
pub fn (v ByteView) slice(start int, end int) !ByteView {
	if start < 0 || end > v.length || start > end {
		return error('slice bounds out of range')
	}
	return ByteView{
		data:   v.data
		offset: v.offset + start
		length: end - start
	}
}

// 실제 바이트를 반환합니다 (오프셋이 0일 때 zero-copy).
/// bytes는 작성된 바이트를 반환합니다.
pub fn (v ByteView) bytes() []u8 {
	if v.length == 0 {
		return []u8{}
	}
	return v.data[v.offset..v.offset + v.length]
}

// 데이터의 복사본을 반환합니다 (소유권이 필요할 때).
/// to_owned는 데이터의 복사본을 반환합니다.
pub fn (v ByteView) to_owned() []u8 {
	if v.length == 0 {
		return []u8{}
	}
	return v.data[v.offset..v.offset + v.length].clone()
}

// 문자열로 변환합니다 (가능하면 zero-copy).
/// to_string은 뷰를 문자열로 변환합니다.
pub fn (v ByteView) to_string() string {
	if v.length == 0 {
		return ''
	}
	return v.data[v.offset..v.offset + v.length].bytestr()
}

// 뷰의 길이를 반환합니다.
/// len은 길이를 반환합니다.
pub fn (v ByteView) len() int {
	return v.length
}

// 뷰가 비어있는지 확인합니다.
/// is_empty는 비어있는지 확인합니다.
pub fn (v ByteView) is_empty() bool {
	return v.length == 0
}

/// BinaryReader - Kafka 바이너리 프로토콜을 파싱하기 위한 리더
pub struct BinaryReader {
pub mut:
	data []u8
	pos  int
}

/// new_reader는 바이트 배열로부터 새로운 BinaryReader를 생성합니다.
pub fn new_reader(data []u8) BinaryReader {
	return BinaryReader{
		data: data
		pos:  0
	}
}

pub fn (r &BinaryReader) remaining() int {
	return r.data.len - r.pos
}

pub fn (r &BinaryReader) position() int {
	return r.pos
}

/// read_i8은 버퍼에서 부호 있는 8비트 정수를 읽습니다.
pub fn (mut r BinaryReader) read_i8() !i8 {
	if r.remaining() < 1 {
		return error('not enough data for i8')
	}
	val := i8(r.data[r.pos])
	r.pos += 1
	return val
}

/// read_i16은 빅 엔디안 형식의 부호 있는 16비트 정수를 읽습니다.
pub fn (mut r BinaryReader) read_i16() !i16 {
	if r.remaining() < 2 {
		return error('not enough data for i16')
	}
	val := i16(u16(r.data[r.pos]) << 8 | u16(r.data[r.pos + 1]))
	r.pos += 2
	return val
}

/// read_i32는 빅 엔디안 형식의 부호 있는 32비트 정수를 읽습니다.
pub fn (mut r BinaryReader) read_i32() !i32 {
	if r.remaining() < 4 {
		return error('not enough data for i32')
	}
	val := i32(u32(r.data[r.pos]) << 24 | u32(r.data[r.pos + 1]) << 16 | u32(r.data[r.pos + 2]) << 8 | u32(r.data[
		r.pos + 3]))
	r.pos += 4
	return val
}

/// read_i64는 빅 엔디안 형식의 부호 있는 64비트 정수를 읽습니다.
pub fn (mut r BinaryReader) read_i64() !i64 {
	if r.remaining() < 8 {
		return error('not enough data for i64')
	}
	val := i64(u64(r.data[r.pos]) << 56 | u64(r.data[r.pos + 1]) << 48 | u64(r.data[r.pos + 2]) << 40 | u64(r.data[
		r.pos + 3]) << 32 | u64(r.data[r.pos + 4]) << 24 | u64(r.data[r.pos + 5]) << 16 | u64(r.data[
		r.pos + 6]) << 8 | u64(r.data[r.pos + 7]))
	r.pos += 8
	return val
}

/// read_uvarint는 부호 없는 가변 길이 정수를 읽습니다 (zigzag 인코딩).
pub fn (mut r BinaryReader) read_uvarint() !u64 {
	mut result := u64(0)
	mut shift := u32(0)

	for {
		if r.remaining() < 1 {
			return error('not enough data for uvarint')
		}
		b := r.data[r.pos]
		r.pos += 1

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

/// read_varint는 부호 있는 가변 길이 정수를 읽습니다 (zigzag 인코딩).
pub fn (mut r BinaryReader) read_varint() !i64 {
	uval := r.read_uvarint()!
	return i64((uval >> 1) ^ (-(uval & 1)))
}

/// read_string은 길이 접두사가 있는 문자열을 읽습니다 (i16 길이).
pub fn (mut r BinaryReader) read_string() !string {
	len := r.read_i16()!
	if len < 0 {
		return ''
	}
	if r.remaining() < int(len) {
		return error('not enough data for string')
	}
	str := r.data[r.pos..r.pos + int(len)].bytestr()
	r.pos += int(len)
	return str
}

/// read_nullable_string은 nullable 길이 접두사 문자열을 읽습니다 (i16 길이, null은 -1).
pub fn (mut r BinaryReader) read_nullable_string() !string {
	len := r.read_i16()!
	if len < 0 {
		return ''
	}
	if r.remaining() < int(len) {
		return error('not enough data for nullable string')
	}
	str := r.data[r.pos..r.pos + int(len)].bytestr()
	r.pos += int(len)
	return str
}

/// read_compact_string은 compact 문자열을 읽습니다 (부호 없는 varint 길이 + 1).
pub fn (mut r BinaryReader) read_compact_string() !string {
	len := r.read_uvarint()!
	if len == 0 {
		return ''
	}
	actual_len := int(len) - 1
	if r.remaining() < actual_len {
		return error('not enough data for compact string')
	}
	str := r.data[r.pos..r.pos + actual_len].bytestr()
	r.pos += actual_len
	return str
}

/// read_compact_nullable_string은 compact nullable 문자열을 읽습니다 (부호 없는 varint 길이, null은 0).
pub fn (mut r BinaryReader) read_compact_nullable_string() !string {
	// Compact nullable string: 길이 0은 null, 길이 N은 N-1 바이트를 의미
	len := r.read_uvarint()!
	if len == 0 {
		return '' // null
	}
	actual_len := int(len) - 1
	if actual_len == 0 {
		return '' // empty string
	}
	if r.remaining() < actual_len {
		return error('not enough data for compact nullable string')
	}
	str := r.data[r.pos..r.pos + actual_len].bytestr()
	r.pos += actual_len
	return str
}

// 지정된 길이만큼 바이트를 읽습니다.
/// read_bytes_len은 버퍼에서 정확히 n바이트를 읽습니다.
pub fn (mut r BinaryReader) read_bytes_len(len int) ![]u8 {
	if len < 0 {
		return []u8{}
	}
	if r.remaining() < len {
		return error('not enough data for bytes')
	}
	bytes := r.data[r.pos..r.pos + len].clone()
	r.pos += len
	return bytes
}

// i32 길이 접두사가 있는 바이트를 읽습니다.
/// read_bytes는 길이 접두사(i32)가 있는 바이트 배열을 읽습니다.
pub fn (mut r BinaryReader) read_bytes() ![]u8 {
	len := r.read_i32()!
	return r.read_bytes_len(int(len))!
}

// UUID를 읽습니다 (16바이트, 고정 길이).
/// read_uuid는 16바이트 UUID를 읽습니다.
pub fn (mut r BinaryReader) read_uuid() ![]u8 {
	if r.remaining() < 16 {
		return error('not enough data for UUID')
	}
	uuid := r.data[r.pos..r.pos + 16].clone()
	r.pos += 16
	return uuid
}

/// read_compact_bytes는 compact 바이트 배열을 읽습니다 (부호 없는 varint 길이 + 1).
pub fn (mut r BinaryReader) read_compact_bytes() ![]u8 {
	len := r.read_uvarint()!
	if len == 0 {
		return []u8{}
	}
	actual_len := int(len) - 1
	if r.remaining() < actual_len {
		return error('not enough data for compact bytes')
	}
	bytes := r.data[r.pos..r.pos + actual_len].clone()
	r.pos += actual_len
	return bytes
}

/// read_array_len은 배열 길이를 읽습니다 (i32, null 배열은 -1).
pub fn (mut r BinaryReader) read_array_len() !int {
	len := r.read_i32()!
	return int(len)
}

/// read_compact_array_len은 compact 배열 길이를 읽습니다 (부호 없는 varint, null은 0).
pub fn (mut r BinaryReader) read_compact_array_len() !int {
	len := r.read_uvarint()!
	if len == 0 {
		return -1
	}
	return int(len) - 1
}

/// skip은 읽기 위치를 n바이트 앞으로 이동합니다.
pub fn (mut r BinaryReader) skip(n int) ! {
	if r.remaining() < n {
		return error('not enough data to skip')
	}
	r.pos += n
}

// ============================================================
// Zero-Copy 뷰 메서드 (고성능 파싱용)
// ============================================================

// 바이트를 zero-copy 뷰로 읽습니다 (메모리 할당 없음).
/// read_bytes_view는 n바이트를 zero-copy ByteView로 읽습니다.
pub fn (mut r BinaryReader) read_bytes_view(len int) !ByteView {
	if len < 0 {
		return ByteView{}
	}
	if r.remaining() < len {
		return error('not enough data for bytes view')
	}
	view := ByteView{
		data:   r.data
		offset: r.pos
		length: len
	}
	r.pos += len
	return view
}

// i32 길이 접두사가 있는 바이트를 zero-copy 뷰로 읽습니다.
/// read_bytes_as_view는 길이 접두사가 있는 바이트 배열을 ByteView로 읽습니다.
pub fn (mut r BinaryReader) read_bytes_as_view() !ByteView {
	len := r.read_i32()!
	return r.read_bytes_view(int(len))!
}

// compact 바이트를 zero-copy 뷰로 읽습니다.
/// read_compact_bytes_view는 compact 바이트를 ByteView로 읽습니다.
pub fn (mut r BinaryReader) read_compact_bytes_view() !ByteView {
	len := r.read_uvarint()!
	if len == 0 {
		return ByteView{}
	}
	actual_len := int(len) - 1
	return r.read_bytes_view(actual_len)!
}

// 위치를 이동하지 않고 다음 바이트를 미리 봅니다 (zero-copy).
pub fn (r &BinaryReader) peek_bytes_view(len int) !ByteView {
	if r.remaining() < len {
		return error('not enough data to peek')
	}
	return ByteView{
		data:   r.data
		offset: r.pos
		length: len
	}
}

// 남은 데이터를 뷰로 반환합니다.
pub fn (r &BinaryReader) remaining_view() ByteView {
	return ByteView{
		data:   r.data
		offset: r.pos
		length: r.remaining()
	}
}

/// read_i32는 빅 엔디안 형식의 부호 있는 32비트 정수를 읽습니다.
pub fn (mut r BinaryReader) read_i32_bytes() ![]u8 {
	length := r.read_i32()!
	return r.read_bytes_len(int(length))!
}

/// skip_tagged_fields는 Kafka 태그 필드를 건너뜁니다 (flexible 메시지 형식).
pub fn (mut r BinaryReader) skip_tagged_fields() ! {
	num_fields := r.read_uvarint()!
	for _ in 0 .. num_fields {
		_ = r.read_uvarint()!
		size := r.read_uvarint()!
		if r.remaining() < int(size) {
			return error('not enough data for tagged field')
		}
		r.pos += int(size)
	}
}

// ============================================================================
// Flexible 버전 헬퍼
// ============================================================================
// 이 헬퍼들은 flexible/non-flexible 메시지 파싱 시 코드 중복을 줄여줍니다.

/// read_flex_string은 flexible이면 compact 형식, 아니면 표준 형식으로 문자열을 읽습니다.
pub fn (mut r BinaryReader) read_flex_string(flexible bool) !string {
	return if flexible { r.read_compact_string()! } else { r.read_string()! }
}

/// read_flex_nullable_string은 flexible이면 compact 형식으로 nullable 문자열을 읽습니다.
pub fn (mut r BinaryReader) read_flex_nullable_string(flexible bool) !string {
	return if flexible { r.read_compact_nullable_string()! } else { r.read_nullable_string()! }
}

/// read_flex_bytes는 flexible이면 compact 형식, 아니면 표준 형식으로 바이트를 읽습니다.
pub fn (mut r BinaryReader) read_flex_bytes(flexible bool) ![]u8 {
	return if flexible { r.read_compact_bytes()! } else { r.read_bytes()! }
}

/// read_flex_array_len은 flexible이면 compact 형식으로 배열 길이를 읽습니다.
pub fn (mut r BinaryReader) read_flex_array_len(flexible bool) !int {
	return if flexible { r.read_compact_array_len()! } else { r.read_array_len()! }
}

/// skip_flex_tagged_fields는 flexible 형식일 때만 태그 필드를 건너뜁니다.
pub fn (mut r BinaryReader) skip_flex_tagged_fields(flexible bool) ! {
	if flexible {
		r.skip_tagged_fields()!
	}
}

/// BinaryWriter - Kafka 바이너리 프로토콜을 인코딩하기 위한 라이터
pub struct BinaryWriter {
pub mut:
	data []u8
}

/// new_writer는 새로운 BinaryWriter를 생성합니다.
pub fn new_writer() BinaryWriter {
	return BinaryWriter{
		data: []u8{}
	}
}

/// new_writer_with_capacity는 지정된 용량으로 새로운 BinaryWriter를 생성합니다.
pub fn new_writer_with_capacity(capacity int) BinaryWriter {
	return BinaryWriter{
		data: []u8{cap: capacity}
	}
}

pub fn (w &BinaryWriter) bytes() []u8 {
	return w.data
}

pub fn (w &BinaryWriter) len() int {
	return w.data.len
}

/// write_i8은 부호 있는 8비트 정수를 씁니다.
pub fn (mut w BinaryWriter) write_i8(val i8) {
	w.data << u8(val)
}

/// write_i16은 빅 엔디안 형식의 부호 있는 16비트 정수를 씁니다.
pub fn (mut w BinaryWriter) write_i16(val i16) {
	w.data << u8(val >> 8)
	w.data << u8(val)
}

/// write_i32는 빅 엔디안 형식의 부호 있는 32비트 정수를 씁니다.
pub fn (mut w BinaryWriter) write_i32(val i32) {
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_u32(val u32) {
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

/// write_i64는 빅 엔디안 형식의 부호 있는 64비트 정수를 씁니다.
pub fn (mut w BinaryWriter) write_i64(val i64) {
	w.data << u8(val >> 56)
	w.data << u8(val >> 48)
	w.data << u8(val >> 40)
	w.data << u8(val >> 32)
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

/// write_uvarint는 부호 없는 가변 길이 정수를 씁니다.
pub fn (mut w BinaryWriter) write_uvarint(val u64) {
	mut v := val
	for v >= 0x80 {
		w.data << u8(v | 0x80)
		v >>= 7
	}
	w.data << u8(v)
}

/// write_varint는 부호 있는 가변 길이 정수를 씁니다 (zigzag 인코딩).
pub fn (mut w BinaryWriter) write_varint(val i64) {
	// ZigZag 인코딩: 부호 있는 값을 부호 없는 값으로 변환
	// 음수를 홀수 양수로, 양수를 짝수로 매핑
	// -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, 등
	uval := (u64(val) << 1) ^ u64(val >> 63)
	w.write_uvarint(uval)
}

/// write_string은 길이 접두사가 있는 문자열을 씁니다 (i16 길이).
pub fn (mut w BinaryWriter) write_string(s string) {
	w.write_i16(i16(s.len))
	w.data << s.bytes()
}

/// write_nullable_string은 nullable 문자열을 씁니다 (i16 길이, null은 -1).
pub fn (mut w BinaryWriter) write_nullable_string(s ?string) {
	if str := s {
		w.write_i16(i16(str.len))
		w.data << str.bytes()
	} else {
		w.write_i16(-1)
	}
}

/// write_compact_string은 compact 문자열을 씁니다 (부호 없는 varint 길이 + 1).
pub fn (mut w BinaryWriter) write_compact_string(s string) {
	w.write_uvarint(u64(s.len) + 1)
	w.data << s.bytes()
}

/// write_compact_nullable_string은 compact nullable 문자열을 씁니다.
pub fn (mut w BinaryWriter) write_compact_nullable_string(s ?string) {
	if str := s {
		w.write_uvarint(u64(str.len) + 1)
		w.data << str.bytes()
	} else {
		w.write_uvarint(0)
	}
}

/// write_bytes는 길이 접두사가 있는 바이트 배열을 씁니다 (i32 길이).
pub fn (mut w BinaryWriter) write_bytes(b []u8) {
	w.write_i32(i32(b.len))
	w.data << b
}

/// write_compact_bytes는 compact 바이트 배열을 씁니다.
pub fn (mut w BinaryWriter) write_compact_bytes(b []u8) {
	w.write_uvarint(u64(b.len) + 1)
	w.data << b
}

// UUID를 씁니다 (16바이트, 고정 길이).
// uuid가 비어있거나 16바이트 미만이면 zero UUID를 씁니다.
/// write_uuid는 16바이트 UUID를 씁니다.
pub fn (mut w BinaryWriter) write_uuid(uuid []u8) {
	if uuid.len >= 16 {
		w.data << uuid[0..16]
	} else {
		// zero UUID 쓰기 (16바이트의 0) - 배열 한 번에 추가
		w.data << []u8{len: 16, init: 0}
	}
}

/// write_array_len은 배열 길이를 씁니다 (i32).
pub fn (mut w BinaryWriter) write_array_len(len int) {
	w.write_i32(i32(len))
}

/// write_compact_array_len은 compact 배열 길이를 씁니다.
pub fn (mut w BinaryWriter) write_compact_array_len(len int) {
	if len < 0 {
		w.write_uvarint(0)
	} else {
		w.write_uvarint(u64(len) + 1)
	}
}

/// write_tagged_fields는 빈 태그 필드 섹션을 씁니다.
pub fn (mut w BinaryWriter) write_tagged_fields() {
	w.write_uvarint(0)
}

/// write_tagged_field_header는 태그 필드 헤더를 씁니다 (tag와 size).
pub fn (mut w BinaryWriter) write_tagged_field_header(tag int, size int) {
	w.write_uvarint(u64(tag))
	w.write_uvarint(u64(size))
}

pub fn (mut w BinaryWriter) write_raw(b []u8) {
	w.data << b
}

// ============================================================
// RecordBatch 파싱 (Kafka Message Format v2)
// ============================================================

/// ParsedRecordBatch는 파싱된 Kafka RecordBatch를 나타냅니다.
pub struct ParsedRecordBatch {
pub:
	base_offset            i64
	partition_leader_epoch i32
	magic                  i8
	attributes             i16
	last_offset_delta      i32
	first_timestamp        i64
	max_timestamp          i64
	producer_id            i64
	producer_epoch         i16
	base_sequence          i32
	records                []domain.Record
}

/// parse_record_batch는 Kafka RecordBatch(v2 형식, magic=2)를 파싱합니다.
/// 파싱된 레코드를 반환하거나, 파싱 실패 시 빈 배열을 반환합니다.
pub fn parse_record_batch(data []u8) !ParsedRecordBatch {
	if data.len < 12 {
		return error('data too small')
	}

	mut reader := new_reader(data)

	// RecordBatch header
	base_offset := reader.read_i64()!
	batch_length := reader.read_i32()!

	if data.len < 12 + int(batch_length) {
		return error('incomplete record batch')
	}

	partition_leader_epoch := reader.read_i32()!
	magic := reader.read_i8()!

	if magic != 2 {
		// MessageSet v0, v1 (magic 0, 1) 지원
		// 레거시 MessageSet 형식으로 파싱
		reader.pos = 0 // 처음으로 리셋
		return parse_message_set(data)!
	}

	// CRC32-C 검증: RecordBatch의 무결성을 확인
	// CRC는 attributes 필드부터 배치 끝까지의 데이터를 커버
	stored_crc := u32(reader.read_i32()!)
	crc_start_pos := reader.pos
	crc_data := data[crc_start_pos..12 + int(batch_length)]
	calculated_crc := crc32c_checksum(crc_data)
	if stored_crc != calculated_crc {
		return error('CRC32-C 검증 실패: 저장된 값=${stored_crc}, 계산된 값=${calculated_crc}')
	}

	attributes := reader.read_i16()!
	last_offset_delta := reader.read_i32()!
	first_timestamp := reader.read_i64()!
	max_timestamp := reader.read_i64()!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	base_sequence := reader.read_i32()!
	record_count := reader.read_i32()!

	// 레코드 파싱
	mut records := []domain.Record{cap: int(record_count)}

	for _ in 0 .. record_count {
		record := parse_record(mut reader, first_timestamp) or { break }
		records << record
	}

	return ParsedRecordBatch{
		base_offset:            base_offset
		partition_leader_epoch: partition_leader_epoch
		magic:                  magic
		attributes:             attributes
		last_offset_delta:      last_offset_delta
		first_timestamp:        first_timestamp
		max_timestamp:          max_timestamp
		producer_id:            producer_id
		producer_epoch:         producer_epoch
		base_sequence:          base_sequence
		records:                records
	}
}

/// parse_message_set은 레거시 MessageSet 형식(v0, v1 - magic 0, 1)을 파싱합니다.
fn parse_message_set(data []u8) !ParsedRecordBatch {
	mut reader := new_reader(data)
	mut records := []domain.Record{}

	// MessageSet은 [offset, size, message]의 배열
	for reader.remaining() > 12 {
		_ := reader.read_i64() or { break } // offset (MessageSet 파싱에서 사용 안 함)
		message_size := reader.read_i32() or { break }

		if reader.remaining() < int(message_size) {
			break
		}

		// 메시지 파싱: [crc, magic, attributes, key, value]
		start_pos := reader.pos
		_ = reader.read_i32() or { break } // crc
		magic := reader.read_i8() or { break }
		_ = reader.read_i8() or { break } // attributes

		// v1은 timestamp 추가
		mut timestamp := time.now()
		if magic >= 1 {
			ts_millis := reader.read_i64() or { break }
			timestamp = time.unix(ts_millis / 1000)
		}

		// 키
		key := reader.read_bytes() or { break }
		// 값
		value := reader.read_bytes() or { break }

		records << domain.Record{
			key:       key
			value:     value
			headers:   map[string][]u8{}
			timestamp: timestamp
		}

		// 정확히 message_size 바이트를 소비했는지 확인
		reader.pos = start_pos + int(message_size)
	}

	return ParsedRecordBatch{
		base_offset:            0
		partition_leader_epoch: -1
		magic:                  0
		attributes:             0
		last_offset_delta:      i32(records.len - 1)
		first_timestamp:        0
		max_timestamp:          0
		producer_id:            -1
		producer_epoch:         -1
		base_sequence:          -1
		records:                records
	}
}

/// parse_record는 RecordBatch v2에서 단일 Record를 파싱합니다.
/// 최적화: 가능한 경우 zero-copy를 위해 ByteView를 사용합니다.
fn parse_record(mut reader BinaryReader, base_timestamp i64) !domain.Record {
	_ = reader.read_varint()! // length (배치에 이미 포함됨)
	_ = reader.read_i8()! // attributes (v2에서 사용 안 함)
	timestamp_delta := reader.read_varint()!
	_ = reader.read_varint()! // offset_delta

	// 키 - 뷰를 사용하고 저장할 때만 소유권 복사로 변환
	key_length := reader.read_varint()!
	mut key := []u8{}
	if key_length > 0 {
		key_view := reader.read_bytes_view(int(key_length))!
		key = key_view.to_owned()
	}

	// 값 - 뷰를 사용하고 저장할 때만 소유권 복사로 변환
	value_length := reader.read_varint()!
	mut value := []u8{}
	if value_length > 0 {
		value_view := reader.read_bytes_view(int(value_length))!
		value = value_view.to_owned()
	}

	// 헤더
	headers_count := reader.read_varint()!
	mut headers := map[string][]u8{}

	for _ in 0 .. headers_count {
		header_key_len := reader.read_varint()!
		mut header_key := ''
		if header_key_len > 0 {
			key_view := reader.read_bytes_view(int(header_key_len))!
			header_key = key_view.to_string()
		}

		header_value_len := reader.read_varint()!
		mut header_value := []u8{}
		if header_value_len > 0 {
			val_view := reader.read_bytes_view(int(header_value_len))!
			header_value = val_view.to_owned()
		}

		if header_key.len > 0 {
			headers[header_key] = header_value
		}
	}

	// 타임스탬프 계산
	ts_millis := base_timestamp + timestamp_delta
	ts := time.unix(ts_millis / 1000)

	return domain.Record{
		key:       key
		value:     value
		headers:   headers
		timestamp: ts
	}
}

// ============================================================
// RecordBatch 인코딩 (Fetch 응답용)
// ============================================================

/// crc32c_checksum은 CRC-32C (Castagnoli) 체크섬을 계산합니다.
/// 최적화된 crc32c 모듈을 사용합니다.
fn crc32c_checksum(data []u8) u32 {
	return crc32c.calculate(data)
}

/// encode_record_batch는 레코드들을 Kafka RecordBatch v2 형식으로 인코딩합니다.
pub fn encode_record_batch(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	mut writer := new_writer()

	// 레코드에서 타임스탬프 계산
	// 첫 번째 레코드의 타임스탬프를 기준으로 사용 (타임스탬프가 없으면 현재 시간)
	first_timestamp := if records[0].timestamp.unix() > 0 {
		records[0].timestamp.unix() * 1000
	} else {
		time.now().unix() * 1000
	}

	// 최대 타임스탬프 찾기
	mut max_timestamp := first_timestamp
	for record in records {
		ts := record.timestamp.unix() * 1000
		if ts > max_timestamp {
			max_timestamp = ts
		}
	}

	// 배치 크기 계산을 위해 먼저 레코드 인코딩
	mut records_data := new_writer()
	for i, record in records {
		encode_record(mut records_data, record, i, first_timestamp)
	}

	records_bytes := records_data.bytes()

	// CRC 계산을 위한 데이터 구성
	// 중요: CRC-32C는 attributes부터 배치 끝까지 커버 (partitionLeaderEpoch부터가 아님!)
	// Kafka 스펙에 따라: CRC = crc32c(attributes...end_of_batch)
	mut crc_data := new_writer()
	crc_data.write_i16(0) // attributes
	crc_data.write_i32(i32(records.len - 1)) // lastOffsetDelta
	crc_data.write_i64(first_timestamp) // baseTimestamp
	crc_data.write_i64(max_timestamp) // maxTimestamp
	crc_data.write_i64(-1) // producerId
	crc_data.write_i16(-1) // producerEpoch
	crc_data.write_i32(-1) // baseSequence
	crc_data.write_i32(i32(records.len)) // recordCount
	// 참고: delete_horizon_ms가 지원되면 (attributes bit 5가 control batch인 경우),
	// baseSequence 이후, recordCount 이전에 삽입되어야 함.
	// 현재는 delete_horizon_ms를 지원하지 않음.
	crc_data.write_raw(records_bytes) // records

	crc_bytes := crc_data.bytes()
	// crc_data에 대한 CRC-32C (Castagnoli)
	crc := crc32c_checksum(crc_bytes)

	// RecordBatch 헤더 필드
	// batchLength = partitionLeaderEpoch부터 끝까지
	batch_length := 4 + 1 + 4 + crc_bytes.len // epoch + magic + crc + crc_data

	writer.write_i64(base_offset) // baseOffset
	writer.write_i32(i32(batch_length)) // batchLength
	writer.write_i32(0) // partitionLeaderEpoch
	writer.write_i8(2) // magic (v2)
	writer.write_u32(crc) // crc (CRC-32C)
	writer.write_raw(crc_bytes) // attributes to records

	return writer.bytes()
}

/// encode_record는 단일 레코드를 RecordBatch v2 형식으로 인코딩합니다.
fn encode_record(mut writer BinaryWriter, record domain.Record, offset_delta int, base_timestamp i64) {
	// 길이 계산을 위해 먼저 레코드 본문 구성
	mut body := new_writer()

	body.write_i8(0) // attributes

	// 타임스탬프 델타
	ts_millis := record.timestamp.unix() * 1000
	body.write_varint(ts_millis - base_timestamp)

	// 오프셋 델타
	body.write_varint(i64(offset_delta))

	// 키
	if record.key.len > 0 {
		body.write_varint(i64(record.key.len))
		body.write_raw(record.key)
	} else {
		body.write_varint(-1) // null 키
	}

	// 값
	if record.value.len > 0 {
		body.write_varint(i64(record.value.len))
		body.write_raw(record.value)
	} else {
		body.write_varint(-1) // null 값
	}

	// 레코드 헤더 (키-값 쌍)
	// 참고: 헤더 키와 값 모두 VARINT 길이 접두사가 있는 원시 바이트로 처리됨
	// 헤더 키 형식: VARINT(key_length) + BYTES(key)
	// 헤더 값 형식: VARINT(value_length) + BYTES(value)
	body.write_varint(i64(record.headers.len))
	for key, value in record.headers {
		// 헤더 키를 바이트로 (STRING 타입이 아님 - null 마커 없음)
		body.write_varint(i64(key.len))
		body.write_raw(key.bytes())
		// 헤더 값을 바이트로
		body.write_varint(i64(value.len))
		body.write_raw(value)
	}

	// 길이 + 본문 쓰기
	body_bytes := body.bytes()
	writer.write_varint(i64(body_bytes.len))
	writer.write_raw(body_bytes)
}
