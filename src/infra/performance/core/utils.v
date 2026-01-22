/// 인프라 레이어 - 성능 유틸리티
/// 해시 함수, CRC32, varint 인코딩 및 기타 유틸리티
module core

// ============================================================================
// MurmurHash3 - Kafka 호환 파티셔닝 해시
// ============================================================================

const c1 = u32(0xcc9e2d51)
const c2 = u32(0x1b873593)

/// murmur3_32는 MurmurHash3 (32비트)를 계산합니다 - Kafka 파티션 해시
pub fn murmur3_32(data []u8, seed u32) u32 {
	mut h := seed
	len := data.len

	// 4바이트 청크 처리
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

	// 나머지 바이트 처리
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

	// 최종화
	h ^= u32(len)
	h = fmix32(h)

	return h
}

/// rotl32는 32비트 왼쪽 회전을 수행합니다.
fn rotl32(x u32, r int) u32 {
	return (x << u32(r)) | (x >> (32 - u32(r)))
}

/// fmix32는 최종 믹싱 함수입니다.
fn fmix32(h_in u32) u32 {
	mut h := h_in
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

/// kafka_partition은 키에 대한 Kafka 파티션을 계산합니다.
pub fn kafka_partition(key []u8, num_partitions int) int {
	if key.len == 0 || num_partitions <= 0 {
		return 0
	}
	hash := murmur3_32(key, 0)
	// Kafka처럼 양수 모듈로 사용
	return int(hash & 0x7fffffff) % num_partitions
}

// ============================================================================
// CRC32 - IEEE 다항식 (Kafka 레코드 체크섬)
// ============================================================================

/// 사전 계산된 CRC32 테이블 (IEEE 다항식)
const crc32_table = init_crc32_table()

/// init_crc32_table은 CRC32 룩업 테이블을 초기화합니다.
fn init_crc32_table() []u32 {
	mut table := []u32{len: 256}
	for i := 0; i < 256; i++ {
		mut crc := u32(i)
		for _ in 0 .. 8 {
			if crc & 1 != 0 {
				crc = (crc >> 1) ^ 0xedb88320
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

/// crc32_ieee는 IEEE 다항식을 사용하여 CRC32를 계산합니다.
pub fn crc32_ieee(data []u8) u32 {
	mut crc := u32(0xffffffff)
	for b in data {
		idx := int((crc ^ u32(b)) & 0xff)
		crc = (crc >> 8) ^ crc32_table[idx]
	}
	return crc ^ 0xffffffff
}

/// crc32_update는 실행 중인 CRC32를 업데이트합니다.
pub fn crc32_update(crc u32, data []u8) u32 {
	mut result := crc ^ 0xffffffff
	for b in data {
		idx := int((result ^ u32(b)) & 0xff)
		result = (result >> 8) ^ crc32_table[idx]
	}
	return result ^ 0xffffffff
}

// ============================================================================
// Varint 인코딩 - Kafka 프로토콜은 부호 있는 varint 사용
// ============================================================================

/// encode_varint는 부호 있는 정수를 varint로 인코딩합니다.
pub fn encode_varint(value i64) []u8 {
	// ZigZag 인코딩: (value << 1) ^ (value >> 63)
	shift_v := if value < 0 { u64(0xffffffffffffffff) } else { u64(0) }
	zigzag := (u64(value) << 1) ^ shift_v
	return encode_uvarint(u64(zigzag))
}

/// encode_uvarint는 부호 없는 정수를 varint로 인코딩합니다.
pub fn encode_uvarint(value u64) []u8 {
	mut result := []u8{cap: 10}
	mut v := value

	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)

	return result
}

/// decode_varint는 부호 있는 varint를 디코딩합니다.
pub fn decode_varint(data []u8) (i64, int) {
	uval, n := decode_uvarint(data)
	if n <= 0 {
		return 0, n
	}
	// ZigZag 디코딩: (value >> 1) ^ -(value & 1)
	val := i64(uval >> 1) ^ -(i64(uval) & 1)
	return val, n
}

/// decode_uvarint는 부호 없는 varint를 디코딩합니다.
pub fn decode_uvarint(data []u8) (u64, int) {
	mut result := u64(0)
	mut shift := u64(0)

	for i, b in data {
		if i >= 10 {
			return 0, -1 // 오버플로우
		}

		result |= u64(b & 0x7f) << shift

		if b & 0x80 == 0 {
			return result, i + 1
		}

		shift += 7
	}

	return 0, 0 // 불완전
}

/// varint_size는 값의 인코딩된 크기를 반환합니다.
pub fn varint_size(value i64) int {
	shift_v := if value < 0 { u64(0xffffffffffffffff) } else { u64(0) }
	zigzag := (u64(value) << 1) ^ shift_v
	return uvarint_size(zigzag)
}

/// uvarint_size는 부호 없는 값의 인코딩된 크기를 반환합니다.
pub fn uvarint_size(value u64) int {
	mut size := 1
	mut v := value
	for v >= 0x80 {
		size += 1
		v >>= 7
	}
	return size
}

// ============================================================================
// 링 버퍼 - 락프리 단일 생산자, 단일 소비자
// ============================================================================

/// RingBuffer는 고정 크기의 순환 버퍼입니다.
@[heap]
pub struct RingBuffer {
mut:
	buffer   []u8 // 기본 버퍼
	capacity int  // 용량
	mask     int  // 비트 마스크 (용량 - 1)
	head     int  // 쓰기 위치
	tail     int  // 읽기 위치
}

/// new_ring_buffer는 지정된 크기로 새 링 버퍼를 생성합니다.
pub fn new_ring_buffer(size int) &RingBuffer {
	// 2의 거듭제곱으로 올림
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

/// write는 데이터를 버퍼에 씁니다.
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

/// read는 버퍼에서 데이터를 읽습니다.
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

/// available은 읽을 수 있는 바이트 수를 반환합니다.
pub fn (r &RingBuffer) available() int {
	if r.head >= r.tail {
		return r.head - r.tail
	}
	return r.capacity - r.tail + r.head
}

/// free_space는 쓸 수 있는 여유 공간을 반환합니다.
pub fn (r &RingBuffer) free_space() int {
	return r.capacity - r.available() - 1
}

/// is_empty는 버퍼가 비어있는지 확인합니다.
pub fn (r &RingBuffer) is_empty() bool {
	return r.head == r.tail
}

/// is_full은 버퍼가 가득 찼는지 확인합니다.
pub fn (r &RingBuffer) is_full() bool {
	return r.free_space() == 0
}

/// clear는 버퍼를 비웁니다.
pub fn (mut r RingBuffer) clear() {
	r.head = 0
	r.tail = 0
}

// ============================================================================
// 비트 조작 유틸리티
// ============================================================================

/// count_leading_zeros는 선행 제로 비트 수를 셉니다.
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

/// count_trailing_zeros는 후행 제로 비트 수를 셉니다.
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

/// popcount는 설정된 비트 수를 셉니다.
pub fn popcount(x u64) int {
	mut v := x
	v = v - ((v >> 1) & 0x5555555555555555)
	v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
	v = (v + (v >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((v * 0x0101010101010101) >> 56)
}
