/// 고성능 데이터 처리를 위한 제로카피 슬라이스 연산
/// 메모리 복사 없이 효율적인 바이트 슬라이스 뷰 제공
module io

import time

/// ByteSlice는 바이트 배열에 대한 제로카피 뷰입니다.
/// 기본 데이터를 소유하지 않고 뷰만 제공합니다.
pub struct ByteSlice {
pub:
	data   []u8
	offset int
	length int
}

/// new는 바이트 배열에서 새 ByteSlice를 생성합니다.
pub fn ByteSlice.new(data []u8) ByteSlice {
	return ByteSlice{
		data:   data
		offset: 0
		length: data.len
	}
}

/// slice는 데이터 복사 없이 서브 슬라이스를 생성합니다.
pub fn (s ByteSlice) slice(start int, end int) !ByteSlice {
	if start < 0 || end > s.length || start > end {
		return error('slice bounds out of range: [${start}:${end}] with length ${s.length}')
	}
	return ByteSlice{
		data:   s.data
		offset: s.offset + start
		length: end - start
	}
}

/// at은 주어진 인덱스의 바이트를 반환합니다.
pub fn (s ByteSlice) at(index int) !u8 {
	if index < 0 || index >= s.length {
		return error('index out of range: ${index} with length ${s.length}')
	}
	return s.data[s.offset + index]
}

/// to_owned는 슬라이스 데이터의 복사본을 반환합니다 (소유권이 필요할 때).
pub fn (s ByteSlice) to_owned() []u8 {
	if s.length == 0 {
		return []u8{}
	}
	mut result := []u8{len: s.length}
	for i in 0 .. s.length {
		result[i] = s.data[s.offset + i]
	}
	return result
}

/// bytes는 원시 바이트를 뷰로 반환합니다 (가능하면 제로카피).
pub fn (s ByteSlice) bytes() []u8 {
	if s.offset == 0 && s.length == s.data.len {
		return s.data
	}
	return s.data[s.offset..s.offset + s.length]
}

/// len은 슬라이스의 길이를 반환합니다.
pub fn (s ByteSlice) len() int {
	return s.length
}

/// is_empty는 슬라이스가 비어있는지 확인합니다.
pub fn (s ByteSlice) is_empty() bool {
	return s.length == 0
}

/// RecordView는 Kafka 레코드에 대한 제로카피 뷰입니다.
/// 데이터 복사 없이 레코드 필드를 지연 파싱합니다.
pub struct RecordView {
pub:
	raw_data     ByteSlice
	key_offset   int
	key_length   int
	value_offset int
	value_length int
	timestamp    i64
	offset       i64
}

/// parse는 원시 바이트에서 RecordView를 생성합니다 (제로카피 파싱).
pub fn RecordView.parse(data []u8) !RecordView {
	if data.len < 26 {
		return error('record too small: ${data.len} bytes, minimum 26 required')
	}

	slice := ByteSlice.new(data)

	// 헤더 필드 파싱
	// Kafka 레코드 형식: offset(8) + timestamp(8) + key_len(4) + key + value_len(4) + value
	record_offset := parse_i64_be(data, 0)
	timestamp := parse_i64_be(data, 8)
	key_length := parse_i32_be(data, 16)

	if key_length < -1 {
		return error('invalid key length: ${key_length}')
	}

	key_offset := 20
	actual_key_len := if key_length == -1 { 0 } else { key_length }

	value_len_offset := key_offset + actual_key_len
	if value_len_offset + 4 > data.len {
		return error('record truncated: cannot read value length')
	}

	value_length := parse_i32_be(data, value_len_offset)
	if value_length < -1 {
		return error('invalid value length: ${value_length}')
	}

	value_offset := value_len_offset + 4
	actual_value_len := if value_length == -1 { 0 } else { value_length }

	if value_offset + actual_value_len > data.len {
		return error('record truncated: value extends beyond data')
	}

	return RecordView{
		raw_data:     slice
		key_offset:   key_offset
		key_length:   actual_key_len
		value_offset: value_offset
		value_length: actual_value_len
		timestamp:    timestamp
		offset:       record_offset
	}
}

/// key는 키를 제로카피 슬라이스로 반환합니다.
pub fn (r RecordView) key() !ByteSlice {
	if r.key_length == 0 {
		return ByteSlice{}
	}
	return r.raw_data.slice(r.key_offset, r.key_offset + r.key_length)
}

/// value는 값을 제로카피 슬라이스로 반환합니다.
pub fn (r RecordView) value() !ByteSlice {
	if r.value_length == 0 {
		return ByteSlice{}
	}
	return r.raw_data.slice(r.value_offset, r.value_offset + r.value_length)
}

/// key_bytes는 키를 소유된 바이트로 반환합니다 (데이터 복사).
pub fn (r RecordView) key_bytes() []u8 {
	if r.key_length == 0 {
		return []u8{}
	}
	key_slice := r.key() or { return []u8{} }
	return key_slice.to_owned()
}

/// value_bytes는 값을 소유된 바이트로 반환합니다 (데이터 복사).
pub fn (r RecordView) value_bytes() []u8 {
	if r.value_length == 0 {
		return []u8{}
	}
	value_slice := r.value() or { return []u8{} }
	return value_slice.to_owned()
}

/// SliceReader는 바이트 스트림 파싱을 위한 효율적인 순차 리더입니다.
pub struct SliceReader {
pub mut:
	slice    ByteSlice
	position int
}

/// new는 새 SliceReader를 생성합니다.
pub fn SliceReader.new(data []u8) SliceReader {
	return SliceReader{
		slice:    ByteSlice.new(data)
		position: 0
	}
}

/// from_slice는 기존 ByteSlice에서 SliceReader를 생성합니다.
pub fn SliceReader.from_slice(slice ByteSlice) SliceReader {
	return SliceReader{
		slice:    slice
		position: 0
	}
}

/// remaining은 읽을 수 있는 남은 바이트 수를 반환합니다.
pub fn (r SliceReader) remaining() int {
	return r.slice.length - r.position
}

/// has_remaining은 리더에 더 많은 데이터가 있는지 확인합니다.
pub fn (r SliceReader) has_remaining() bool {
	return r.position < r.slice.length
}

/// read_u8은 단일 바이트를 읽습니다.
pub fn (mut r SliceReader) read_u8() !u8 {
	if r.position >= r.slice.length {
		return error('read_u8: buffer underflow')
	}
	val := r.slice.at(r.position)!
	r.position += 1
	return val
}

/// read_i16_be는 빅엔디안 i16을 읽습니다.
pub fn (mut r SliceReader) read_i16_be() !i16 {
	if r.remaining() < 2 {
		return error('read_i16_be: buffer underflow')
	}
	bytes := r.slice.bytes()
	pos := r.slice.offset + r.position
	val := i16((u16(bytes[pos]) << 8) | u16(bytes[pos + 1]))
	r.position += 2
	return val
}

/// read_i32_be는 빅엔디안 i32를 읽습니다.
pub fn (mut r SliceReader) read_i32_be() !i32 {
	if r.remaining() < 4 {
		return error('read_i32_be: buffer underflow')
	}
	bytes := r.slice.bytes()
	pos := r.slice.offset + r.position
	val := i32((u32(bytes[pos]) << 24) | (u32(bytes[pos + 1]) << 16) | (u32(bytes[pos + 2]) << 8) | u32(bytes[
		pos + 3]))
	r.position += 4
	return val
}

/// read_i64_be는 빅엔디안 i64를 읽습니다.
pub fn (mut r SliceReader) read_i64_be() !i64 {
	if r.remaining() < 8 {
		return error('read_i64_be: buffer underflow')
	}
	bytes := r.slice.bytes()
	pos := r.slice.offset + r.position
	val := parse_i64_be(bytes, pos)
	r.position += 8
	return val
}

/// read_slice는 n 바이트를 새 ByteSlice로 읽습니다 (제로카피).
pub fn (mut r SliceReader) read_slice(n int) !ByteSlice {
	if r.remaining() < n {
		return error('read_slice: buffer underflow, requested ${n} but only ${r.remaining()} available')
	}
	result := r.slice.slice(r.position, r.position + n)!
	r.position += n
	return result
}

/// read_bytes는 n 바이트를 소유된 배열로 읽습니다 (데이터 복사).
pub fn (mut r SliceReader) read_bytes(n int) ![]u8 {
	slice := r.read_slice(n)!
	return slice.to_owned()
}

/// skip은 n 바이트를 건너뜁니다.
pub fn (mut r SliceReader) skip(n int) ! {
	if r.remaining() < n {
		return error('skip: buffer underflow')
	}
	r.position += n
}

/// reset은 리더 위치를 처음으로 재설정합니다.
pub fn (mut r SliceReader) reset() {
	r.position = 0
}

/// seek는 절대 위치로 이동합니다.
pub fn (mut r SliceReader) seek(pos int) ! {
	if pos < 0 || pos > r.slice.length {
		return error('seek: position ${pos} out of bounds [0, ${r.slice.length}]')
	}
	r.position = pos
}

/// BatchSliceIterator는 레코드 배치에 대한 제로카피 이터레이터입니다.
pub struct BatchSliceIterator {
mut:
	reader       SliceReader
	record_count int
	current      int
}

/// new는 새 배치 이터레이터를 생성합니다.
pub fn BatchSliceIterator.new(data []u8, record_count int) BatchSliceIterator {
	return BatchSliceIterator{
		reader:       SliceReader.new(data)
		record_count: record_count
		current:      0
	}
}

/// count는 배치의 레코드 수를 반환합니다.
pub fn (b BatchSliceIterator) count() int {
	return b.record_count
}

/// has_next는 더 많은 레코드가 있는지 확인합니다.
pub fn (b BatchSliceIterator) has_next() bool {
	return b.current < b.record_count && b.reader.has_remaining()
}

/// SlicePool은 재사용 가능한 ByteSlice 객체 풀입니다.
pub struct SlicePool {
mut:
	pool      []ByteSlice
	pool_size int
	created   u64
	reused    u64
}

/// new는 새 SlicePool을 생성합니다.
pub fn SlicePool.new(initial_size int) SlicePool {
	return SlicePool{
		pool:      []ByteSlice{cap: initial_size}
		pool_size: initial_size
		created:   0
		reused:    0
	}
}

/// acquire는 풀에서 ByteSlice를 획득하거나 새로 생성합니다.
pub fn (mut p SlicePool) acquire(data []u8) ByteSlice {
	if p.pool.len > 0 {
		p.reused++
		_ = p.pool.pop()
		// 새 데이터로 재초기화
		return ByteSlice{
			data:   data
			offset: 0
			length: data.len
		}
	}
	p.created++
	return ByteSlice.new(data)
}

/// release는 ByteSlice를 풀에 반환합니다.
pub fn (mut p SlicePool) release(slice ByteSlice) {
	if p.pool.len < p.pool_size {
		p.pool << slice
	}
}

/// stats는 풀 통계를 반환합니다.
pub fn (p SlicePool) stats() (u64, u64, int) {
	return p.created, p.reused, p.pool.len
}

// 빅엔디안 파싱을 위한 헬퍼 함수

/// parse_i32_be는 빅엔디안 i32를 파싱합니다.
fn parse_i32_be(data []u8, offset int) int {
	return int((u32(data[offset]) << 24) | (u32(data[offset + 1]) << 16) | (u32(data[offset + 2]) << 8) | u32(data[
		offset + 3]))
}

/// parse_i64_be는 빅엔디안 i64를 파싱합니다.
fn parse_i64_be(data []u8, offset int) i64 {
	high := u64((u32(data[offset]) << 24) | (u32(data[offset + 1]) << 16) | (u32(data[offset + 2]) << 8) | u32(data[
		offset + 3]))
	low := u64((u32(data[offset + 4]) << 24) | (u32(data[offset + 5]) << 16) | (u32(data[offset + 6]) << 8) | u32(data[
		offset + 7]))
	return i64((high << 32) | low)
}

// 슬라이스 연산을 위한 벤치마크 유틸리티

/// SliceBenchmark는 슬라이스 연산 벤치마크입니다.
pub struct SliceBenchmark {
mut:
	iterations u64
	data_size  int
}

/// new는 새 SliceBenchmark를 생성합니다.
pub fn SliceBenchmark.new(iterations u64, data_size int) SliceBenchmark {
	return SliceBenchmark{
		iterations: iterations
		data_size:  data_size
	}
}

/// run_comparison은 제로카피 슬라이스 생성 vs 배열 복사를 벤치마크합니다.
pub fn (b SliceBenchmark) run_comparison() (i64, i64) {
	// 테스트 데이터 생성
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// 제로카피 슬라이스 벤치마크
	sw1 := time.new_stopwatch()
	for _ in 0 .. b.iterations {
		slice := ByteSlice.new(data)
		_ := slice.slice(0, b.data_size / 2) or { ByteSlice{} }
	}
	slice_time := sw1.elapsed().nanoseconds()

	// 배열 복사 벤치마크
	sw2 := time.new_stopwatch()
	for _ in 0 .. b.iterations {
		_ := data[0..b.data_size / 2].clone()
	}
	copy_time := sw2.elapsed().nanoseconds()

	return slice_time, copy_time
}
