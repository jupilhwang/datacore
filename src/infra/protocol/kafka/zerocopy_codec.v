// Zero-Copy Kafka Protocol Codec
// High-performance protocol parsing using SliceReader
module kafka

import infra.performance

// ============================================================================
// Zero-Copy Binary Reader
// ============================================================================

// ZeroCopyReader wraps SliceReader for Kafka protocol parsing
pub struct ZeroCopyReader {
mut:
    reader performance.SliceReader
}

// new_zerocopy_reader creates a reader from raw bytes
pub fn new_zerocopy_reader(data []u8) ZeroCopyReader {
    return ZeroCopyReader{
        reader: performance.SliceReader.new(data)
    }
}

// remaining returns bytes left to read
pub fn (r &ZeroCopyReader) remaining() int {
    return r.reader.remaining()
}

// position returns current read position
pub fn (r &ZeroCopyReader) position() int {
    return r.reader.position
}

// has_remaining checks if more data available
pub fn (r &ZeroCopyReader) has_remaining() bool {
    return r.reader.has_remaining()
}

// read_i8 reads a signed 8-bit integer
pub fn (mut r ZeroCopyReader) read_i8() !i8 {
    return i8(r.reader.read_u8()!)
}

// read_i16 reads a big-endian signed 16-bit integer
pub fn (mut r ZeroCopyReader) read_i16() !i16 {
    return r.reader.read_i16_be()!
}

// read_i32 reads a big-endian signed 32-bit integer
pub fn (mut r ZeroCopyReader) read_i32() !i32 {
    return r.reader.read_i32_be()!
}

// read_i64 reads a big-endian signed 64-bit integer
pub fn (mut r ZeroCopyReader) read_i64() !i64 {
    return r.reader.read_i64_be()!
}

// read_bool reads a boolean (single byte)
pub fn (mut r ZeroCopyReader) read_bool() !bool {
    return r.reader.read_u8()! != 0
}

// read_uvarint reads an unsigned variable-length integer
pub fn (mut r ZeroCopyReader) read_uvarint() !u64 {
    mut result := u64(0)
    mut shift := u32(0)
    
    for {
        b := r.reader.read_u8()!
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

// read_varint reads a signed variable-length integer (zigzag encoded)
pub fn (mut r ZeroCopyReader) read_varint() !i64 {
    uval := r.read_uvarint()!
    return i64((uval >> 1) ^ (-(uval & 1)))
}

// ============================================================================
// Zero-Copy String/Bytes Reading
// ============================================================================

// read_bytes_slice returns zero-copy slice (no data copied)
pub fn (mut r ZeroCopyReader) read_bytes_slice(len int) !performance.ByteSlice {
    return r.reader.read_slice(len)!
}

// read_bytes reads bytes with length prefix (copies data)
pub fn (mut r ZeroCopyReader) read_bytes() ![]u8 {
    length := r.read_i32()!
    if length < 0 {
        return []u8{}
    }
    return r.reader.read_bytes(length)!
}

// read_bytes_view returns zero-copy view of bytes with length prefix
pub fn (mut r ZeroCopyReader) read_bytes_view() !performance.ByteSlice {
    length := r.read_i32()!
    if length < 0 {
        return performance.ByteSlice{}
    }
    return r.reader.read_slice(length)!
}

// read_string reads a length-prefixed string
pub fn (mut r ZeroCopyReader) read_string() !string {
    length := r.read_i16()!
    if length < 0 {
        return ''
    }
    bytes := r.reader.read_bytes(int(length))!
    return bytes.bytestr()
}

// read_nullable_string reads a nullable length-prefixed string
// Returns empty string for null (length < 0)
pub fn (mut r ZeroCopyReader) read_nullable_string() !string {
    length := r.read_i16()!
    if length < 0 {
        return ''
    }
    bytes := r.reader.read_bytes(int(length))!
    return bytes.bytestr()
}

// read_compact_string reads a compact (varint-prefixed) string
pub fn (mut r ZeroCopyReader) read_compact_string() !string {
    length := r.read_uvarint()!
    if length == 0 {
        return ''
    }
    // Compact strings: length = actual_length + 1
    actual_len := int(length - 1)
    bytes := r.reader.read_bytes(actual_len)!
    return bytes.bytestr()
}

// read_compact_nullable_string reads a compact nullable string
// Returns empty string for null (length == 0)
pub fn (mut r ZeroCopyReader) read_compact_nullable_string() !string {
    length := r.read_uvarint()!
    if length == 0 {
        return ''
    }
    actual_len := int(length - 1)
    bytes := r.reader.read_bytes(actual_len)!
    return bytes.bytestr()
}

// read_compact_bytes reads compact (varint-prefixed) bytes
pub fn (mut r ZeroCopyReader) read_compact_bytes() ![]u8 {
    length := r.read_uvarint()!
    if length == 0 {
        return []u8{}
    }
    actual_len := int(length - 1)
    return r.reader.read_bytes(actual_len)!
}

// read_compact_bytes_view returns zero-copy view of compact bytes
pub fn (mut r ZeroCopyReader) read_compact_bytes_view() !performance.ByteSlice {
    length := r.read_uvarint()!
    if length == 0 {
        return performance.ByteSlice{}
    }
    actual_len := int(length - 1)
    return r.reader.read_slice(actual_len)!
}

// ============================================================================
// Array Reading
// ============================================================================

// read_array_length reads array length (i32 prefix)
pub fn (mut r ZeroCopyReader) read_array_length() !int {
    return int(r.read_i32()!)
}

// read_compact_array_length reads compact array length (varint prefix)
pub fn (mut r ZeroCopyReader) read_compact_array_length() !int {
    length := r.read_uvarint()!
    if length == 0 {
        return -1 // null array
    }
    return int(length - 1)
}

// skip skips n bytes
pub fn (mut r ZeroCopyReader) skip(n int) ! {
    r.reader.skip(n)!
}

// skip_tagged_fields skips flexible protocol tagged fields
pub fn (mut r ZeroCopyReader) skip_tagged_fields() ! {
    num_tags := r.read_uvarint()!
    for _ in 0 .. int(num_tags) {
        _ = r.read_uvarint()! // tag
        tag_len := r.read_uvarint()!
        r.skip(int(tag_len))!
    }
}

// ============================================================================
// UUID Reading
// ============================================================================

// read_uuid reads a 16-byte UUID
pub fn (mut r ZeroCopyReader) read_uuid() ![]u8 {
    return r.reader.read_bytes(16)!
}

// read_uuid_view returns zero-copy view of UUID
pub fn (mut r ZeroCopyReader) read_uuid_view() !performance.ByteSlice {
    return r.reader.read_slice(16)!
}

// ============================================================================
// Request Parsing with Zero-Copy
// ============================================================================

// ZeroCopyRequest holds parsed request with zero-copy body
pub struct ZeroCopyRequest {
pub:
    header RequestHeader
    body   performance.ByteSlice  // Zero-copy reference to body bytes
}

// parse_request_zerocopy parses Kafka request with minimal copying
pub fn parse_request_zerocopy(data []u8) !ZeroCopyRequest {
    mut reader := new_zerocopy_reader(data)
    
    api_key := reader.read_i16()!
    api_version := reader.read_i16()!
    correlation_id := reader.read_i32()!
    
    // Check if this is a flexible version (v2+ header)
    is_flexible := is_flexible_request(api_key, api_version)
    
    mut client_id := ''
    if !is_flexible {
        // v0/v1 header: standard string
        client_id = reader.read_string()!
    } else {
        // v2 header: compact string + tagged fields
        client_id = reader.read_compact_string()!
        reader.skip_tagged_fields()!
    }
    
    header := RequestHeader{
        api_key: api_key
        api_version: api_version
        correlation_id: correlation_id
        client_id: client_id
    }
    
    // Get remaining data as zero-copy slice for body
    remaining := reader.remaining()
    body := reader.read_bytes_slice(remaining)!
    
    return ZeroCopyRequest{
        header: header
        body: body
    }
}

// ============================================================================
// Fetch Response Zero-Copy Building
// ============================================================================

// FetchPartitionData holds partition data for zero-copy fetch response
pub struct FetchPartitionData {
pub:
    partition_index  i32
    error_code       i16
    high_watermark   i64
    last_stable_offset i64
    log_start_offset i64
    records          performance.ByteSlice  // Zero-copy reference to records
}

// FetchTopicData holds topic data for fetch response
pub struct FetchTopicData {
pub:
    topic_id    []u8
    topic_name  string
    partitions  []FetchPartitionData
}

// ZeroCopyFetchResponse represents a fetch response with zero-copy record data
pub struct ZeroCopyFetchResponse {
pub:
    throttle_time_ms i32
    error_code       i16
    session_id       i32
    topics           []FetchTopicData
}

// ============================================================================
// Helper: Check if request uses flexible encoding
// ============================================================================

fn is_flexible_request(api_key i16, version i16) bool {
    // Based on Kafka protocol - flexible versions vary by API
    match unsafe { ApiKey(api_key) } {
        .api_versions { return version >= 3 }
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
        .consumer_group_heartbeat { return true } // KIP-848 always flexible
        .sasl_handshake { return version >= 1 }
        .sasl_authenticate { return version >= 2 }
        else { return false }
    }
}
