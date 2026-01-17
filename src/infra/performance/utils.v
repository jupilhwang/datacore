// Infra Layer - Performance Utilities
// Hash functions, CRC32, varint encoding, and other utilities
module performance

// ============================================================================
// MurmurHash3 - Kafka-compatible partitioning hash
// ============================================================================

const c1 = u32(0xcc9e2d51)
const c2 = u32(0x1b873593)

// murmur3_32 computes MurmurHash3 (32-bit) - Kafka partition hash
pub fn murmur3_32(data []u8, seed u32) u32 {
    mut h := seed
    len := data.len
    
    // Process 4-byte chunks
    nblocks := len / 4
    for i := 0; i < nblocks; i++ {
        idx := i * 4
        mut k := u32(data[idx]) | 
                 (u32(data[idx + 1]) << 8) |
                 (u32(data[idx + 2]) << 16) |
                 (u32(data[idx + 3]) << 24)
        
        k *= performance.c1
        k = rotl32(k, 15)
        k *= performance.c2
        
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
        k1 *= performance.c1
        k1 = rotl32(k1, 15)
        k1 *= performance.c2
        h ^= k1
    }
    
    // Finalization
    h ^= u32(len)
    h = fmix32(h)
    
    return h
}

fn rotl32(x u32, r int) u32 {
    return (x << u32(r)) | (x >> (32 - u32(r)))
}

fn fmix32(h_in u32) u32 {
    mut h := h_in
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    return h
}

// kafka_partition computes Kafka partition for a key
pub fn kafka_partition(key []u8, num_partitions int) int {
    if key.len == 0 || num_partitions <= 0 {
        return 0
    }
    hash := murmur3_32(key, 0)
    // Use positive modulo like Kafka
    return int(hash & 0x7fffffff) % num_partitions
}

// ============================================================================
// CRC32 - IEEE polynomial (Kafka record checksum)
// ============================================================================

// Pre-computed CRC32 table (IEEE polynomial)
const crc32_table = init_crc32_table()

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

// crc32_ieee computes CRC32 using IEEE polynomial
pub fn crc32_ieee(data []u8) u32 {
    mut crc := u32(0xffffffff)
    for b in data {
        idx := int((crc ^ u32(b)) & 0xff)
        crc = (crc >> 8) ^ performance.crc32_table[idx]
    }
    return crc ^ 0xffffffff
}

// crc32_update updates running CRC32
pub fn crc32_update(crc u32, data []u8) u32 {
    mut result := crc ^ 0xffffffff
    for b in data {
        idx := int((result ^ u32(b)) & 0xff)
        result = (result >> 8) ^ performance.crc32_table[idx]
    }
    return result ^ 0xffffffff
}

// ============================================================================
// Varint Encoding - Kafka protocol uses signed varint
// ============================================================================

// encode_varint encodes a signed integer as varint
pub fn encode_varint(value i64) []u8 {
    // ZigZag encode: (value << 1) ^ (value >> 63)
    zigzag := (value << 1) ^ (value >> 63)
    return encode_uvarint(u64(zigzag))
}

// encode_uvarint encodes an unsigned integer as varint
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

// decode_varint decodes a signed varint
pub fn decode_varint(data []u8) (i64, int) {
    uval, n := decode_uvarint(data)
    if n <= 0 {
        return 0, n
    }
    // ZigZag decode: (value >> 1) ^ -(value & 1)
    val := i64(uval >> 1) ^ -(i64(uval) & 1)
    return val, n
}

// decode_uvarint decodes an unsigned varint
pub fn decode_uvarint(data []u8) (u64, int) {
    mut result := u64(0)
    mut shift := u64(0)
    
    for i, b in data {
        if i >= 10 {
            return 0, -1 // Overflow
        }
        
        result |= u64(b & 0x7f) << shift
        
        if b & 0x80 == 0 {
            return result, i + 1
        }
        
        shift += 7
    }
    
    return 0, 0 // Incomplete
}

// varint_size returns the encoded size of a value
pub fn varint_size(value i64) int {
    zigzag := u64((value << 1) ^ (value >> 63))
    return uvarint_size(zigzag)
}

// uvarint_size returns the encoded size of an unsigned value
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
// Ring Buffer - Lock-free single producer, single consumer
// ============================================================================

@[heap]
pub struct RingBuffer {
mut:
    buffer     []u8
    capacity   int
    mask       int
    head       int  // Write position
    tail       int  // Read position
}

pub fn new_ring_buffer(size int) &RingBuffer {
    // Round up to power of 2
    mut cap := 1
    for cap < size {
        cap *= 2
    }
    
    return &RingBuffer{
        buffer: []u8{len: cap}
        capacity: cap
        mask: cap - 1
    }
}

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

pub fn (r &RingBuffer) available() int {
    if r.head >= r.tail {
        return r.head - r.tail
    }
    return r.capacity - r.tail + r.head
}

pub fn (r &RingBuffer) free_space() int {
    return r.capacity - r.available() - 1
}

pub fn (r &RingBuffer) is_empty() bool {
    return r.head == r.tail
}

pub fn (r &RingBuffer) is_full() bool {
    return r.free_space() == 0
}

pub fn (mut r RingBuffer) clear() {
    r.head = 0
    r.tail = 0
}

// ============================================================================
// Bit manipulation utilities
// ============================================================================

// count_leading_zeros counts leading zero bits
pub fn count_leading_zeros(x u64) int {
    if x == 0 {
        return 64
    }
    
    mut n := 0
    mut v := x
    
    if v & 0xffffffff00000000 == 0 { n += 32; v <<= 32 }
    if v & 0xffff000000000000 == 0 { n += 16; v <<= 16 }
    if v & 0xff00000000000000 == 0 { n += 8; v <<= 8 }
    if v & 0xf000000000000000 == 0 { n += 4; v <<= 4 }
    if v & 0xc000000000000000 == 0 { n += 2; v <<= 2 }
    if v & 0x8000000000000000 == 0 { n += 1 }
    
    return n
}

// count_trailing_zeros counts trailing zero bits
pub fn count_trailing_zeros(x u64) int {
    if x == 0 {
        return 64
    }
    
    mut n := 0
    mut v := x
    
    if v & 0x00000000ffffffff == 0 { n += 32; v >>= 32 }
    if v & 0x000000000000ffff == 0 { n += 16; v >>= 16 }
    if v & 0x00000000000000ff == 0 { n += 8; v >>= 8 }
    if v & 0x000000000000000f == 0 { n += 4; v >>= 4 }
    if v & 0x0000000000000003 == 0 { n += 2; v >>= 2 }
    if v & 0x0000000000000001 == 0 { n += 1 }
    
    return n
}

// popcount counts set bits
pub fn popcount(x u64) int {
    mut v := x
    v = v - ((v >> 1) & 0x5555555555555555)
    v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
    v = (v + (v >> 4)) & 0x0f0f0f0f0f0f0f0f
    return int((v * 0x0101010101010101) >> 56)
}
