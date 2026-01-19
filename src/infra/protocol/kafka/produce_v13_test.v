module kafka

import encoding.binary

fn test_produce_response_v13_encoding() {
    // 16-byte UUID
    uuid := [u8(0x00), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f]
    
    resp := ProduceResponse{
        topics: [
            ProduceResponseTopic{
                name: "ignored-name",
                topic_id: uuid,
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
    
    // v13 is flexible
    encoded := resp.encode(13)
    
    // Check structure
    mut offset := 0
    
    // 1. Array length (Compact Array)
    // 1 topic -> len=2 (N+1)
    assert encoded[offset] == 2
    offset += 1
    
    // 2. Topic ID (UUID, 16 bytes) - v13 uses this instead of name
    read_uuid := encoded[offset..offset+16]
    assert read_uuid == uuid
    offset += 16
    
    // 3. Partitions array length (Compact Array)
    // 1 partition -> len=2 (N+1)
    assert encoded[offset] == 2
    offset += 1
    
    // 4. Partition index (int32)
    p_index := binary.big_endian_u32(encoded[offset..offset+4])
    assert p_index == 0
    offset += 4
    
    println('ProduceResponse v13 encoding test passed')
}
