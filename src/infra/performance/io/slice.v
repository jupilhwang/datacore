/// Zero-copy slice operations for high-performance data processing
/// Provides efficient byte slice views without memory copies
module io

import time

/// ByteSlice is a zero-copy view over a byte array.
/// It provides a view only and does not own the underlying data.
pub struct ByteSlice {
pub:
	data   []u8
	offset int
	length int
}

/// new creates a new ByteSlice from a byte array.
pub fn ByteSlice.new(data []u8) ByteSlice {
	return ByteSlice{
		data:   data
		offset: 0
		length: data.len
	}
}

/// slice creates a sub-slice without copying data.
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

/// at returns the byte at the given index.
pub fn (s ByteSlice) at(index int) !u8 {
	if index < 0 || index >= s.length {
		return error('index out of range: ${index} with length ${s.length}')
	}
	return s.data[s.offset + index]
}

/// to_owned returns a copy of the slice data (use when ownership is required).
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

/// bytes returns the raw bytes as a view (zero-copy when possible).
pub fn (s ByteSlice) bytes() []u8 {
	if s.offset == 0 && s.length == s.data.len {
		return s.data
	}
	return s.data[s.offset..s.offset + s.length]
}

/// len returns the length of the slice.
pub fn (s ByteSlice) len() int {
	return s.length
}

/// is_empty checks whether the slice is empty.
pub fn (s ByteSlice) is_empty() bool {
	return s.length == 0
}

/// RecordView is a zero-copy view over a Kafka record.
/// Lazily parses record fields without copying data.
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

/// parse creates a RecordView from raw bytes (zero-copy parsing).
pub fn RecordView.parse(data []u8) !RecordView {
	if data.len < 26 {
		return error('record too small: ${data.len} bytes, minimum 26 required')
	}

	slice := ByteSlice.new(data)

	// Parse header fields
	// Kafka record format: offset(8) + timestamp(8) + key_len(4) + key + value_len(4) + value
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

/// key returns the key as a zero-copy slice.
pub fn (r RecordView) key() !ByteSlice {
	if r.key_length == 0 {
		return ByteSlice{}
	}
	return r.raw_data.slice(r.key_offset, r.key_offset + r.key_length)
}

/// value returns the value as a zero-copy slice.
pub fn (r RecordView) value() !ByteSlice {
	if r.value_length == 0 {
		return ByteSlice{}
	}
	return r.raw_data.slice(r.value_offset, r.value_offset + r.value_length)
}

/// key_bytes returns the key as owned bytes (data is copied).
pub fn (r RecordView) key_bytes() []u8 {
	if r.key_length == 0 {
		return []u8{}
	}
	key_slice := r.key() or { return []u8{} }
	return key_slice.to_owned()
}

/// value_bytes returns the value as owned bytes (data is copied).
pub fn (r RecordView) value_bytes() []u8 {
	if r.value_length == 0 {
		return []u8{}
	}
	value_slice := r.value() or { return []u8{} }
	return value_slice.to_owned()
}

/// SliceReader is an efficient sequential reader for parsing byte streams.
pub struct SliceReader {
pub mut:
	slice    ByteSlice
	position int
}

/// new creates a new SliceReader.
pub fn SliceReader.new(data []u8) SliceReader {
	return SliceReader{
		slice:    ByteSlice.new(data)
		position: 0
	}
}

/// from_slice creates a SliceReader from an existing ByteSlice.
pub fn SliceReader.from_slice(slice ByteSlice) SliceReader {
	return SliceReader{
		slice:    slice
		position: 0
	}
}

/// remaining returns the number of bytes remaining to be read.
pub fn (r SliceReader) remaining() int {
	return r.slice.length - r.position
}

/// has_remaining checks whether the reader has more data.
pub fn (r SliceReader) has_remaining() bool {
	return r.position < r.slice.length
}

/// read_u8 reads a single byte.
pub fn (mut r SliceReader) read_u8() !u8 {
	if r.position >= r.slice.length {
		return error('read_u8: buffer underflow')
	}
	val := r.slice.at(r.position)!
	r.position += 1
	return val
}

/// read_i16_be reads a big-endian i16.
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

/// read_i32_be reads a big-endian i32.
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

/// read_i64_be reads a big-endian i64.
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

/// read_slice reads n bytes as a new ByteSlice (zero-copy).
pub fn (mut r SliceReader) read_slice(n int) !ByteSlice {
	if r.remaining() < n {
		return error('read_slice: buffer underflow, requested ${n} but only ${r.remaining()} available')
	}
	result := r.slice.slice(r.position, r.position + n)!
	r.position += n
	return result
}

/// read_bytes reads n bytes into an owned array (data is copied).
pub fn (mut r SliceReader) read_bytes(n int) ![]u8 {
	slice := r.read_slice(n)!
	return slice.to_owned()
}

/// skip skips n bytes.
pub fn (mut r SliceReader) skip(n int) ! {
	if r.remaining() < n {
		return error('skip: buffer underflow')
	}
	r.position += n
}

/// reset resets the reader position to the beginning.
pub fn (mut r SliceReader) reset() {
	r.position = 0
}

/// seek moves to an absolute position.
pub fn (mut r SliceReader) seek(pos int) ! {
	if pos < 0 || pos > r.slice.length {
		return error('seek: position ${pos} out of bounds [0, ${r.slice.length}]')
	}
	r.position = pos
}

/// BatchSliceIterator is a zero-copy iterator over a record batch.
pub struct BatchSliceIterator {
mut:
	reader       SliceReader
	record_count int
	current      int
}

/// new creates a new batch iterator.
pub fn BatchSliceIterator.new(data []u8, record_count int) BatchSliceIterator {
	return BatchSliceIterator{
		reader:       SliceReader.new(data)
		record_count: record_count
		current:      0
	}
}

/// count returns the number of records in the batch.
pub fn (b BatchSliceIterator) count() int {
	return b.record_count
}

/// has_next checks whether more records are available.
pub fn (b BatchSliceIterator) has_next() bool {
	return b.current < b.record_count && b.reader.has_remaining()
}

/// SlicePool is a pool of reusable ByteSlice objects.
pub struct SlicePool {
mut:
	pool      []ByteSlice
	pool_size int
	created   u64
	reused    u64
}

/// new creates a new SlicePool.
pub fn SlicePool.new(initial_size int) SlicePool {
	return SlicePool{
		pool:      []ByteSlice{cap: initial_size}
		pool_size: initial_size
		created:   0
		reused:    0
	}
}

/// acquire obtains a ByteSlice from the pool or creates a new one.
pub fn (mut p SlicePool) acquire(data []u8) ByteSlice {
	if p.pool.len > 0 {
		p.reused++
		_ = p.pool.pop()
		// Reinitialize with new data
		return ByteSlice{
			data:   data
			offset: 0
			length: data.len
		}
	}
	p.created++
	return ByteSlice.new(data)
}

/// release returns a ByteSlice to the pool.
pub fn (mut p SlicePool) release(slice ByteSlice) {
	if p.pool.len < p.pool_size {
		p.pool << slice
	}
}

/// stats returns pool statistics.
pub fn (p SlicePool) stats() (u64, u64, int) {
	return p.created, p.reused, p.pool.len
}

// Helper functions for big-endian parsing

/// parse_i32_be parses a big-endian i32.
fn parse_i32_be(data []u8, offset int) int {
	return int((u32(data[offset]) << 24) | (u32(data[offset + 1]) << 16) | (u32(data[offset + 2]) << 8) | u32(data[
		offset + 3]))
}

/// parse_i64_be parses a big-endian i64.
fn parse_i64_be(data []u8, offset int) i64 {
	high := u64((u32(data[offset]) << 24) | (u32(data[offset + 1]) << 16) | (u32(data[offset + 2]) << 8) | u32(data[
		offset + 3]))
	low := u64((u32(data[offset + 4]) << 24) | (u32(data[offset + 5]) << 16) | (u32(data[offset + 6]) << 8) | u32(data[
		offset + 7]))
	return i64((high << 32) | low)
}

// Benchmark utility for slice operations

/// SliceBenchmark benchmarks slice operations.
pub struct SliceBenchmark {
mut:
	iterations u64
	data_size  int
}

/// new creates a new SliceBenchmark.
pub fn SliceBenchmark.new(iterations u64, data_size int) SliceBenchmark {
	return SliceBenchmark{
		iterations: iterations
		data_size:  data_size
	}
}

/// run_comparison benchmarks zero-copy slice creation vs array copying.
pub fn (b SliceBenchmark) run_comparison() (i64, i64) {
	// Generate test data
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// Zero-copy slice benchmark
	sw1 := time.new_stopwatch()
	for _ in 0 .. b.iterations {
		slice := ByteSlice.new(data)
		_ := slice.slice(0, b.data_size / 2) or { ByteSlice{} }
	}
	slice_time := sw1.elapsed().nanoseconds()

	// Array copy benchmark
	sw2 := time.new_stopwatch()
	for _ in 0 .. b.iterations {
		_ := data[0..b.data_size / 2].clone()
	}
	copy_time := sw2.elapsed().nanoseconds()

	return slice_time, copy_time
}
