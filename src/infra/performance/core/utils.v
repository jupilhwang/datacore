/// Infrastructure layer - Performance utilities
/// Hash functions, CRC32, varint encoding, and other utilities
module core

import common
import rand

// Utilities

/// generate_uuid generates a random UUID v4 (16 bytes).
pub fn generate_uuid() []u8 {
	// Initialize array with random values
	mut uuid := []u8{len: 16}
	for i in 0 .. 16 {
		uuid[i] = u8(rand.intn(256) or { 0 })
	}
	// Set version (4) and variant (RFC 4122) bits
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return uuid
}

// parse_config_i64 and parse_config_int have been moved to common/config_utils.v

// MurmurHash3 - Kafka-compatible partitioning hash

const c1 = u32(0xcc9e2d51)
const c2 = u32(0x1b873593)

/// murmur3_32 computes MurmurHash3 (32-bit) - Kafka partition hash
pub fn murmur3_32(data []u8, seed u32) u32 {
	mut h := seed
	len := data.len

	// Process 4-byte chunks
	nblocks := len / 4
	for i := 0; i < nblocks; i++ {
		idx := i * 4
		mut k := u32(data[idx]) | (u32(data[idx + 1]) << 8) | (u32(data[idx + 2]) << 16) | (u32(data[
			idx + 3]) << 24)

		k *= c1
		k = rotl32(k, 15)
		k *= c2

		h ^= k
		h = rotl32(h, 13)
		h = h * 5 + 0xe6546b64
	}

	// Process remaining bytes
	tail_idx := nblocks * 4
	mut k1 := u32(0)

	remaining := len & 3
	if remaining >= 3 {
		k1 ^= u32(data[tail_idx + 2]) << 16
	}
	if remaining >= 2 {
		k1 ^= u32(data[tail_idx + 1]) << 8
	}
	if remaining >= 1 {
		k1 ^= u32(data[tail_idx])
		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2
		h ^= k1
	}

	// Finalize
	h ^= u32(len)
	h = fmix32(h)

	return h
}

/// rotl32 performs a 32-bit left rotation.
fn rotl32(x u32, r int) u32 {
	return (x << u32(r)) | (x >> (32 - u32(r)))
}

/// fmix32 is the final mixing function.
fn fmix32(h_in u32) u32 {
	mut h := h_in
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

/// kafka_partition computes the Kafka partition for a key.
pub fn kafka_partition(key []u8, num_partitions int) int {
	if key.len == 0 || num_partitions <= 0 {
		return 0
	}
	hash := murmur3_32(key, 0)
	// Use positive modulo like Kafka
	return int(hash & 0x7fffffff) % num_partitions
}

// CRC32 - IEEE polynomial (Kafka record checksum)
// Delegates to common/crc32_ieee.v for the canonical implementation.

/// crc32_ieee computes CRC32 using the IEEE polynomial.
@[inline]
pub fn crc32_ieee(data []u8) u32 {
	return common.crc32_ieee(data)
}

/// crc32_update updates a running CRC32.
@[inline]
fn crc32_update(crc u32, data []u8) u32 {
	return common.crc32_ieee_update(crc, data)
}

// Varint encoding - delegates to common module

/// encode_varint encodes a signed integer as a varint.
@[inline]
pub fn encode_varint(value i64) []u8 {
	return common.encode_varint(value)
}

/// encode_uvarint encodes an unsigned integer as a varint.
@[inline]
pub fn encode_uvarint(value u64) []u8 {
	return common.encode_uvarint(value)
}

/// decode_varint decodes a signed varint.
@[inline]
pub fn decode_varint(data []u8) (i64, int) {
	return common.decode_varint(data)
}

/// decode_uvarint decodes an unsigned varint.
@[inline]
pub fn decode_uvarint(data []u8) (u64, int) {
	return common.decode_uvarint(data)
}

/// varint_size returns the encoded size of a value.
@[inline]
pub fn varint_size(value i64) int {
	return common.varint_size(value)
}

/// uvarint_size returns the encoded size of an unsigned value.
@[inline]
pub fn uvarint_size(value u64) int {
	return common.uvarint_size(value)
}

// Unique big-endian helpers (not available in common module)

/// hex_nibble_to_char converts a 4-bit value (0-15) to a hexadecimal character (a-f).
pub fn hex_nibble_to_char(n u8) string {
	if n < 10 {
		return (u8(`0`) + n).ascii_str()
	} else {
		return (u8(`a`) + (n - 10)).ascii_str()
	}
}

/// write_u32_be writes an unsigned 32-bit integer in big-endian order to a buffer.
pub fn write_u32_be(mut buf []u8, val u32) {
	buf << [u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)]
}

/// write_i32_be_at writes a 32-bit integer in big-endian order at a specific position in the buffer.
pub fn write_i32_be_at(mut buf []u8, pos int, val i32) {
	buf[pos] = u8(val >> 24)
	buf[pos + 1] = u8(val >> 16)
	buf[pos + 2] = u8(val >> 8)
	buf[pos + 3] = u8(val)
}

/// write_u32_be_at writes an unsigned 32-bit integer in big-endian order at a specific position in the buffer.
pub fn write_u32_be_at(mut buf []u8, pos int, val u32) {
	buf[pos] = u8(val >> 24)
	buf[pos + 1] = u8(val >> 16)
	buf[pos + 2] = u8(val >> 8)
	buf[pos + 3] = u8(val)
}

/// read_u16_be reads an unsigned 16-bit integer in big-endian order from a buffer.
pub fn read_u16_be(data []u8) u16 {
	return u16(data[0]) << 8 | u16(data[1])
}

/// read_u32_be reads an unsigned 32-bit integer in big-endian order from a buffer.
pub fn read_u32_be(data []u8) u32 {
	return u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3])
}

// Binary I/O wrappers - delegates to common module

/// write_i16_be writes a 16-bit integer in big-endian order to a buffer.
@[inline]
pub fn write_i16_be(mut buf []u8, val i16) {
	common.write_i16_be(mut buf, val)
}

/// write_i32_be writes a 32-bit integer in big-endian order to a buffer.
@[inline]
pub fn write_i32_be(mut buf []u8, val i32) {
	common.write_i32_be(mut buf, val)
}

/// write_i64_be writes a 64-bit integer in big-endian order to a buffer.
@[inline]
pub fn write_i64_be(mut buf []u8, val i64) {
	common.write_i64_be(mut buf, val)
}

/// read_i16_be reads a 16-bit integer in big-endian order from a buffer.
pub fn read_i16_be(data []u8) i16 {
	return i16(u16(data[0]) << 8 | u16(data[1]))
}

/// read_i32_be reads a 32-bit integer in big-endian order from a buffer.
pub fn read_i32_be(data []u8) i32 {
	return i32(u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3]))
}

/// read_i64_be reads a 64-bit integer in big-endian order from a buffer.
pub fn read_i64_be(data []u8) i64 {
	return i64(u64(data[0]) << 56 | u64(data[1]) << 48 | u64(data[2]) << 40 | u64(data[3]) << 32 | u64(data[4]) << 24 | u64(data[5]) << 16 | u64(data[6]) << 8 | u64(data[7]))
}

/// hex_char_to_nibble converts a hexadecimal character to a 4-bit value.
@[inline]
pub fn hex_char_to_nibble(c u8) int {
	return common.hex_char_to_nibble(c)
}

/// escape_json_string escapes a string for use in a JSON value.
@[inline]
pub fn escape_json_string(s string) string {
	return common.escape_json_string(s)
}

// Ring buffer - lock-free single producer, single consumer

/// RingBuffer is a fixed-size circular buffer.
@[heap]
pub struct RingBuffer {
mut:
	buffer   []u8
	capacity int
	mask     int
	head     int
	tail     int
}

/// new_ring_buffer creates a new ring buffer with the specified size.
pub fn new_ring_buffer(size int) &RingBuffer {
	// Round up to power of two
	mut cap := 1
	for cap < size {
		cap *= 2
	}

	return &RingBuffer{
		buffer:   []u8{len: cap}
		capacity: cap
		mask:     cap - 1
	}
}

/// write writes data to the buffer.
pub fn (mut r RingBuffer) write(data []u8) int {
	available := r.free_space()
	to_write := if data.len < available { data.len } else { available }

	if to_write == 0 {
		return 0
	}

	for i := 0; i < to_write; i++ {
		r.buffer[(r.head + i) & r.mask] = data[i]
	}
	r.head = (r.head + to_write) & r.mask

	return to_write
}

/// read reads data from the buffer.
pub fn (mut r RingBuffer) read(mut buf []u8) int {
	available := r.available()
	to_read := if buf.len < available { buf.len } else { available }

	if to_read == 0 {
		return 0
	}

	for i := 0; i < to_read; i++ {
		buf[i] = r.buffer[(r.tail + i) & r.mask]
	}
	r.tail = (r.tail + to_read) & r.mask

	return to_read
}

/// available returns the number of bytes available for reading.
pub fn (r &RingBuffer) available() int {
	if r.head >= r.tail {
		return r.head - r.tail
	}
	return r.capacity - r.tail + r.head
}

/// free_space returns the available free space for writing.
pub fn (r &RingBuffer) free_space() int {
	return r.capacity - r.available() - 1
}

/// is_empty checks if the buffer is empty.
pub fn (r &RingBuffer) is_empty() bool {
	return r.head == r.tail
}

/// is_full checks if the buffer is full.
pub fn (r &RingBuffer) is_full() bool {
	return r.free_space() == 0
}

/// clear empties the buffer.
pub fn (mut r RingBuffer) clear() {
	r.head = 0
	r.tail = 0
}

// Bit manipulation utilities

/// count_leading_zeros counts the number of leading zero bits.
pub fn count_leading_zeros(x u64) int {
	if x == 0 {
		return 64
	}

	mut n := 0
	mut v := x

	if v & 0xffffffff00000000 == 0 {
		n += 32
		v <<= 32
	}
	if v & 0xffff000000000000 == 0 {
		n += 16
		v <<= 16
	}
	if v & 0xff00000000000000 == 0 {
		n += 8
		v <<= 8
	}
	if v & 0xf000000000000000 == 0 {
		n += 4
		v <<= 4
	}
	if v & 0xc000000000000000 == 0 {
		n += 2
		v <<= 2
	}
	if v & 0x8000000000000000 == 0 {
		n += 1
	}

	return n
}

/// count_trailing_zeros counts the number of trailing zero bits.
pub fn count_trailing_zeros(x u64) int {
	if x == 0 {
		return 64
	}

	mut n := 0
	mut v := x

	if v & 0x00000000ffffffff == 0 {
		n += 32
		v >>= 32
	}
	if v & 0x000000000000ffff == 0 {
		n += 16
		v >>= 16
	}
	if v & 0x00000000000000ff == 0 {
		n += 8
		v >>= 8
	}
	if v & 0x000000000000000f == 0 {
		n += 4
		v >>= 4
	}
	if v & 0x0000000000000003 == 0 {
		n += 2
		v >>= 2
	}
	if v & 0x0000000000000001 == 0 {
		n += 1
	}

	return n
}

/// popcount counts the number of set bits.
pub fn popcount(x u64) int {
	mut v := x
	v = v - ((v >> 1) & 0x5555555555555555)
	v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
	v = (v + (v >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((v * 0x0101010101010101) >> 56)
}
