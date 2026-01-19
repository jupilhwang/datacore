module kafka

fn test_apiversions_v3() {
    // Test ApiVersions v3 (Flexible Request/Response)
    // Note: Request header is v2 (flexible), Response header is v0 (non-flexible)
    
    // 1. Response v3 Encoding
    resp := new_api_versions_response()
    // Manual check of v3 encoding
    encoded := resp.encode(3)
    
    // Check structure:
    // ErrorCode (i16)
    // CompactArray length (api_keys)
    // ... keys ...
    // ThrottleTimeMs (i32)
    // SupportedFeatures (CompactArray)
    // FinalizedFeaturesEpoch (i64)
    // FinalizedFeatures (CompactArray)
    // TaggedFields (0)
    
    // Just minimal assertions that it doesn't panic and length > 0
    assert encoded.len > 10
    
    println('ApiVersions v3 test passed')
}

fn test_findcoordinator_v4() {
    // Test FindCoordinator v4 (Batch)
    req_bytes := [
        // Key (Compact String) "group1"
        u8(7), 0x67, 0x72, 0x6f, 0x75, 0x70, 0x31,
        // KeyType (0 = Group)
        0,
        // CoordinatorKeys (Compact Array Len 2 -> 1 key)
        2,
        // Key "group2" (Compact String)
        7, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x32,
        // Tagged Fields (per key)
        0,
        // Tagged Fields (Request)
        0
    ]
    
    mut reader := new_reader(req_bytes)
    // v4 is flexible
    req := parse_find_coordinator_request(mut reader, 4, true) or {
        panic(err)
    }
    
    assert req.key == "group1"
    assert req.key_type == 0
    assert req.coordinator_keys.len == 1
    assert req.coordinator_keys[0] == "group2"
    
    println('FindCoordinator v4 test passed')
}
