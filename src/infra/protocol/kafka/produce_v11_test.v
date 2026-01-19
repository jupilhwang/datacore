module kafka

import encoding.binary

fn test_produce_response_v11_flexible_encoding() {
    resp := ProduceResponse{
        topics: [
            ProduceResponseTopic{
                name: "test-topic",
                partitions: [
                    ProduceResponsePartition{
                        index: 0,
                        error_code: 0,
                        base_offset: 100,
                        log_append_time: -1,
                        log_start_offset: 0
                    }
                ]
            }
        ],
        throttle_time_ms: 0
    }

    // v11 is flexible (v9+), but uses Name (not Topic ID)
    encoded := resp.encode(11)
    
    mut offset := 0
    
    // 1. Array length (Compact Array)
    // 1 topic -> len=2 (N+1)
    assert encoded[offset] == 2
    offset += 1
    
    // 2. Topic Name (Compact String)
    // "test-topic" len=10. Compact len = 11 (0x0b)
    assert encoded[offset] == 11
    offset += 1
    name_bytes := encoded[offset..offset+10]
    assert name_bytes.bytestr() == "test-topic"
    offset += 10
    
    // 3. Partitions array length (Compact Array)
    // 1 partition -> len=2 (N+1)
    assert encoded[offset] == 2
    offset += 1
    
    // 4. Partition index (int32)
    p_index := binary.big_endian_u32(encoded[offset..offset+4])
    assert p_index == 0
    offset += 4
    
    // 5. Error Code (int16)
    offset += 2
    
    // 6. Base Offset (int64)
    offset += 8
    
    // 7. Log Append Time (int64) - v2+
    offset += 8
    
    // 8. Log Start Offset (int64) - v5+
    offset += 8
    
    // 9. Record Errors (Compact Array) - v8+
    // Empty -> len=1 (0+1)
    assert encoded[offset] == 1
    offset += 1
    
    // 10. Error Message (Compact Nullable String) - v8+
    // None -> len=0
    assert encoded[offset] == 0
    offset += 1
    
    // 11. Tagged Fields (Partition level)
    // Empty -> 0x00
    assert encoded[offset] == 0
    offset += 1
    
    // 12. Tagged Fields (Topic level)
    // Empty -> 0x00
    assert encoded[offset] == 0
    offset += 1
    
    // 13. Throttle Time (int32)
    offset += 4
    
    // 14. Tagged Fields (Response level)
    // Empty -> 0x00
    assert encoded[offset] == 0
    offset += 1
    
    println('ProduceResponse v11 encoding test passed')
}
