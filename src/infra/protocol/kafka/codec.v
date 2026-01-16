// Adapter Layer - Kafka Binary Codec
// Binary Reader/Writer in Big Endian format (Kafka wire format)
module kafka

// Binary Reader
pub struct BinaryReader {
pub mut:
    data    []u8
    pos     int
}

pub fn new_reader(data []u8) BinaryReader {
    return BinaryReader{
        data: data
        pos: 0
    }
}

pub fn (r &BinaryReader) remaining() int {
    return r.data.len - r.pos
}

pub fn (mut r BinaryReader) read_i8() !i8 {
    if r.remaining() < 1 {
        return error('not enough data for i8')
    }
    val := i8(r.data[r.pos])
    r.pos += 1
    return val
}

pub fn (mut r BinaryReader) read_i16() !i16 {
    if r.remaining() < 2 {
        return error('not enough data for i16')
    }
    val := i16(u16(r.data[r.pos]) << 8 | u16(r.data[r.pos + 1]))
    r.pos += 2
    return val
}

pub fn (mut r BinaryReader) read_i32() !i32 {
    if r.remaining() < 4 {
        return error('not enough data for i32')
    }
    val := i32(u32(r.data[r.pos]) << 24 | 
               u32(r.data[r.pos + 1]) << 16 | 
               u32(r.data[r.pos + 2]) << 8 | 
               u32(r.data[r.pos + 3]))
    r.pos += 4
    return val
}

pub fn (mut r BinaryReader) read_i64() !i64 {
    if r.remaining() < 8 {
        return error('not enough data for i64')
    }
    val := i64(u64(r.data[r.pos]) << 56 | 
               u64(r.data[r.pos + 1]) << 48 | 
               u64(r.data[r.pos + 2]) << 40 | 
               u64(r.data[r.pos + 3]) << 32 |
               u64(r.data[r.pos + 4]) << 24 | 
               u64(r.data[r.pos + 5]) << 16 | 
               u64(r.data[r.pos + 6]) << 8 | 
               u64(r.data[r.pos + 7]))
    r.pos += 8
    return val
}

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

pub fn (mut r BinaryReader) read_varint() !i64 {
    uval := r.read_uvarint()!
    return i64((uval >> 1) ^ (-(uval & 1)))
}

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

pub fn (mut r BinaryReader) read_compact_nullable_string() !string {
    // Compact nullable string: length 0 means null, length N means N-1 bytes
    len := r.read_uvarint()!
    if len == 0 {
        return ''  // null
    }
    actual_len := int(len) - 1
    if actual_len == 0 {
        return ''  // empty string
    }
    if r.remaining() < actual_len {
        return error('not enough data for compact nullable string')
    }
    str := r.data[r.pos..r.pos + actual_len].bytestr()
    r.pos += actual_len
    return str
}

pub fn (mut r BinaryReader) read_bytes() ![]u8 {
    len := r.read_i32()!
    if len < 0 {
        return []u8{}
    }
    if r.remaining() < int(len) {
        return error('not enough data for bytes')
    }
    bytes := r.data[r.pos..r.pos + int(len)].clone()
    r.pos += int(len)
    return bytes
}

// Read UUID (16 bytes, fixed length)
pub fn (mut r BinaryReader) read_uuid() ![]u8 {
    if r.remaining() < 16 {
        return error('not enough data for UUID')
    }
    uuid := r.data[r.pos..r.pos + 16].clone()
    r.pos += 16
    return uuid
}

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

pub fn (mut r BinaryReader) read_array_len() !int {
    len := r.read_i32()!
    return int(len)
}

pub fn (mut r BinaryReader) read_compact_array_len() !int {
    len := r.read_uvarint()!
    if len == 0 {
        return -1
    }
    return int(len) - 1
}

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

// Binary Writer
pub struct BinaryWriter {
pub mut:
    data    []u8
}

pub fn new_writer() BinaryWriter {
    return BinaryWriter{
        data: []u8{}
    }
}

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

pub fn (mut w BinaryWriter) write_i8(val i8) {
    w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_i16(val i16) {
    w.data << u8(val >> 8)
    w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_i32(val i32) {
    w.data << u8(val >> 24)
    w.data << u8(val >> 16)
    w.data << u8(val >> 8)
    w.data << u8(val)
}

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

pub fn (mut w BinaryWriter) write_uvarint(val u64) {
    mut v := val
    for v >= 0x80 {
        w.data << u8(v | 0x80)
        v >>= 7
    }
    w.data << u8(v)
}

pub fn (mut w BinaryWriter) write_varint(val i64) {
    // ZigZag encoding: convert signed to unsigned
    // Maps negative numbers to odd positive numbers, positive to even
    // -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
    uval := (u64(val) << 1) ^ u64(val >> 63)
    w.write_uvarint(uval)
}

pub fn (mut w BinaryWriter) write_string(s string) {
    w.write_i16(i16(s.len))
    w.data << s.bytes()
}

pub fn (mut w BinaryWriter) write_nullable_string(s ?string) {
    if str := s {
        w.write_i16(i16(str.len))
        w.data << str.bytes()
    } else {
        w.write_i16(-1)
    }
}

pub fn (mut w BinaryWriter) write_compact_string(s string) {
    w.write_uvarint(u64(s.len) + 1)
    w.data << s.bytes()
}

pub fn (mut w BinaryWriter) write_compact_nullable_string(s ?string) {
    if str := s {
        w.write_uvarint(u64(str.len) + 1)
        w.data << str.bytes()
    } else {
        w.write_uvarint(0)
    }
}

pub fn (mut w BinaryWriter) write_bytes(b []u8) {
    w.write_i32(i32(b.len))
    w.data << b
}

pub fn (mut w BinaryWriter) write_compact_bytes(b []u8) {
    w.write_uvarint(u64(b.len) + 1)
    w.data << b
}

// Write UUID (16 bytes, fixed length)
// If uuid is empty or less than 16 bytes, writes zero UUID
pub fn (mut w BinaryWriter) write_uuid(uuid []u8) {
    if uuid.len >= 16 {
        w.data << uuid[0..16]
    } else {
        // Write zero UUID (16 bytes of zeros)
        for _ in 0 .. 16 {
            w.data << u8(0)
        }
    }
}

pub fn (mut w BinaryWriter) write_array_len(len int) {
    w.write_i32(i32(len))
}

pub fn (mut w BinaryWriter) write_compact_array_len(len int) {
    if len < 0 {
        w.write_uvarint(0)
    } else {
        w.write_uvarint(u64(len) + 1)
    }
}

pub fn (mut w BinaryWriter) write_tagged_fields() {
    w.write_uvarint(0)
}

pub fn (mut w BinaryWriter) write_raw(b []u8) {
    w.data << b
}
