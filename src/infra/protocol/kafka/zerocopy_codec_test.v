// Zero-Copy Protocol Integration Tests
module kafka

import infra.performance

// ============================================================================
// Zero-Copy Reader Tests
// ============================================================================

fn test_zerocopy_reader_basic() {
    data := [u8(0x00), 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04]
    mut reader := new_zerocopy_reader(data)
    
    // Read i16
    val16 := reader.read_i16()!
    assert val16 == 1
    
    // Read another i16
    val16_2 := reader.read_i16()!
    assert val16_2 == 2
    
    // Read i32
    val32 := reader.read_i32()!
    assert val32 == 4
    
    // Should have no remaining
    assert reader.remaining() == 0
}

fn test_zerocopy_reader_i64() {
    data := [u8(0x00), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]  // 100 in big-endian
    mut reader := new_zerocopy_reader(data)
    
    val := reader.read_i64()!
    assert val == 100
}

fn test_zerocopy_reader_varint() {
    // Test unsigned varint
    data := [u8(0x96), 0x01]  // 150 in unsigned varint
    mut reader := new_zerocopy_reader(data)
    
    uval := reader.read_uvarint()!
    assert uval == 150
    
    // Test signed varint (zigzag)
    data2 := [u8(0x02)]  // 1 in zigzag (2 >> 1 = 1)
    mut reader2 := new_zerocopy_reader(data2)
    
    sval := reader2.read_varint()!
    assert sval == 1
    
    // Negative zigzag
    data3 := [u8(0x01)]  // -1 in zigzag
    mut reader3 := new_zerocopy_reader(data3)
    
    neg_val := reader3.read_varint()!
    assert neg_val == -1
}

fn test_zerocopy_reader_string() {
    // "test" with i16 length prefix
    data := [u8(0x00), 0x04, 0x74, 0x65, 0x73, 0x74]  // len=4, "test"
    mut reader := new_zerocopy_reader(data)
    
    s := reader.read_string()!
    assert s == 'test'
}

fn test_zerocopy_reader_nullable_string() {
    // Null string (-1) returns empty
    data := [u8(0xFF), 0xFF]
    mut reader := new_zerocopy_reader(data)
    
    s := reader.read_nullable_string()!
    assert s == ''
    
    // Non-null string
    data2 := [u8(0x00), 0x03, 0x61, 0x62, 0x63]  // len=3, "abc"
    mut reader2 := new_zerocopy_reader(data2)
    
    s2 := reader2.read_nullable_string()!
    assert s2 == 'abc'
}

fn test_zerocopy_reader_compact_string() {
    // Compact string: length = varint(actual_len + 1)
    // "test" (4 bytes) -> length = 5 in varint = 0x05
    data := [u8(0x05), 0x74, 0x65, 0x73, 0x74]
    mut reader := new_zerocopy_reader(data)
    
    s := reader.read_compact_string()!
    assert s == 'test'
}

fn test_zerocopy_reader_bytes_view() {
    data := [u8(0x00), 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04]  // len=4, [1,2,3,4]
    mut reader := new_zerocopy_reader(data)
    
    view := reader.read_bytes_view()!
    assert view.len() == 4
    
    // Check it's a view (zero-copy)
    assert view.at(0)! == 0x01
    assert view.at(3)! == 0x04
}

fn test_zerocopy_reader_slice_zerocopy() {
    // Verify that read_bytes_slice doesn't copy data
    data := []u8{len: 1000, init: u8(index)}
    mut reader := new_zerocopy_reader(data)
    
    // Read a slice
    slice := reader.read_bytes_slice(500)!
    
    // The slice should reference the same data
    assert slice.len() == 500
    for i in 0 .. 500 {
        assert slice.at(i)! == u8(i)
    }
}

fn test_zerocopy_reader_array_length() {
    data := [u8(0x00), 0x00, 0x00, 0x03]  // array length 3
    mut reader := new_zerocopy_reader(data)
    
    len := reader.read_array_length()!
    assert len == 3
}

fn test_zerocopy_reader_compact_array_length() {
    // Compact array: length = varint(actual_len + 1), null = 0
    data := [u8(0x04)]  // 4 - 1 = 3 elements
    mut reader := new_zerocopy_reader(data)
    
    len := reader.read_compact_array_length()!
    assert len == 3
    
    // Null array
    data2 := [u8(0x00)]
    mut reader2 := new_zerocopy_reader(data2)
    
    null_len := reader2.read_compact_array_length()!
    assert null_len == -1
}

fn test_zerocopy_reader_skip() {
    data := [u8(0x01), 0x02, 0x03, 0x04, 0x05]
    mut reader := new_zerocopy_reader(data)
    
    reader.skip(3)!
    assert reader.remaining() == 2
    
    val := reader.read_i8()!
    assert val == 4
}

fn test_zerocopy_reader_skip_tagged_fields() {
    // Tagged fields: num_tags (varint), then for each: tag (varint), len (varint), data
    // 2 tags:
    //   tag 0, len 2, data [0xAA, 0xBB]
    //   tag 1, len 1, data [0xCC]
    data := [u8(0x02), 0x00, 0x02, 0xAA, 0xBB, 0x01, 0x01, 0xCC]
    mut reader := new_zerocopy_reader(data)
    
    reader.skip_tagged_fields()!
    assert reader.remaining() == 0
}

fn test_zerocopy_reader_uuid() {
    uuid := []u8{len: 16, init: u8(index)}
    mut reader := new_zerocopy_reader(uuid)
    
    result := reader.read_uuid()!
    assert result.len == 16
    for i in 0 .. 16 {
        assert result[i] == u8(i)
    }
}

// ============================================================================
// Request Parsing Tests
// ============================================================================

fn test_parse_request_zerocopy_non_flexible() {
    mut writer := new_writer()
    writer.write_i16(18)  // api_key: ApiVersions
    writer.write_i16(0)   // api_version: 0 (non-flexible)
    writer.write_i32(12345)  // correlation_id
    writer.write_string('test-client')  // client_id
    writer.write_i8(42)  // body data
    
    data := writer.bytes()
    req := parse_request_zerocopy(data)!
    
    assert req.header.api_key == 18
    assert req.header.api_version == 0
    assert req.header.correlation_id == 12345
    assert req.header.client_id == 'test-client'
    assert req.body.len() == 1
}

// ============================================================================
// Performance Comparison Tests
// ============================================================================

fn test_zerocopy_vs_copy_performance() {
    // Create a large data buffer
    data := []u8{len: 1_000_000, init: u8(index % 256)}
    
    // Test zero-copy reads
    mut zc_reader := new_zerocopy_reader(data)
    mut total_zerocopy := u64(0)
    
    for _ in 0 .. 100 {
        slice := zc_reader.read_bytes_slice(10000)!
        total_zerocopy += u64(slice.len())
    }
    
    assert total_zerocopy == 1_000_000
    
    // Test copy reads using BinaryReader (read small chunks)
    mut copy_reader := new_reader(data)
    mut total_copy := u64(0)
    
    for _ in 0 .. 10000 {
        if copy_reader.remaining() >= 100 {
            copy_reader.pos += 100  // Skip by advancing position
            total_copy += 100
        }
    }
    
    assert total_copy == 1_000_000
}

fn test_slice_reader_integration() {
    // Test that SliceReader from performance module integrates correctly
    data := [u8(0x00), 0x00, 0x00, 0x64, 0xDE, 0xAD, 0xBE, 0xEF]
    
    slice := performance.ByteSlice.new(data)
    mut reader := performance.SliceReader.from_slice(slice)
    
    // Read i32
    val := reader.read_i32_be()!
    assert val == 100  // 0x64 = 100
    
    // Read remaining as slice
    remaining := reader.read_slice(4)!
    assert remaining.at(0)! == 0xDE
    assert remaining.at(3)! == 0xEF
}

// ============================================================================
// RecordView Zero-Copy Tests
// ============================================================================

fn test_record_view_zerocopy() {
    // Create a simple record with known structure
    // offset(8) + timestamp(8) + key_len(4) + key + value_len(4) + value
    mut data := []u8{}
    
    // offset = 1
    data << [u8(0x00), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
    // timestamp = 1000
    data << [u8(0x00), 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]
    // key_len = 3
    data << [u8(0x00), 0x00, 0x00, 0x03]
    // key = "key"
    data << [u8(0x6B), 0x65, 0x79]
    // value_len = 5
    data << [u8(0x00), 0x00, 0x00, 0x05]
    // value = "value"
    data << [u8(0x76), 0x61, 0x6C, 0x75, 0x65]
    
    record_view := performance.RecordView.parse(data)!
    
    assert record_view.offset == 1
    assert record_view.timestamp == 1000
    assert record_view.key_length == 3
    assert record_view.value_length == 5
    
    // Get key as zero-copy slice
    key_slice := record_view.key()!
    assert key_slice.len() == 3
    
    // Get value as zero-copy slice
    value_slice := record_view.value()!
    assert value_slice.len() == 5
    
    // Get as owned bytes when needed
    key_bytes := record_view.key_bytes()
    assert key_bytes == [u8(0x6B), 0x65, 0x79]  // "key"
    
    value_bytes := record_view.value_bytes()
    assert value_bytes == [u8(0x76), 0x61, 0x6C, 0x75, 0x65]  // "value"
}

// ============================================================================
// Flexible Protocol Tests
// ============================================================================

fn test_is_flexible_request() {
    // ApiVersions v0-2 non-flexible, v3+ flexible
    assert is_flexible_request(18, 0) == false
    assert is_flexible_request(18, 2) == false
    assert is_flexible_request(18, 3) == true
    
    // Metadata v0-8 non-flexible, v9+ flexible
    assert is_flexible_request(3, 0) == false
    assert is_flexible_request(3, 8) == false
    assert is_flexible_request(3, 9) == true
    
    // Fetch v0-11 non-flexible, v12+ flexible
    assert is_flexible_request(1, 0) == false
    assert is_flexible_request(1, 11) == false
    assert is_flexible_request(1, 12) == true
    
    // ConsumerGroupHeartbeat (KIP-848) always flexible
    assert is_flexible_request(68, 0) == true
}
