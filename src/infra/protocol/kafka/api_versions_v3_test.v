module kafka

fn test_api_versions_v3_encoding() {
    // 1. Create Response
    response := ApiVersionsResponse{
        error_code: 0
        api_versions: [
            ApiVersionsResponseKey{api_key: 18, min_version: 0, max_version: 3}
        ]
        throttle_time_ms: 0
        supported_features: []
        finalized_features_epoch: -1
        finalized_features: []
    }

    // 2. Encode Body (v3)
    body_bytes := response.encode(3)

    // 3. Build Full Response (Header v0)
    full_response := build_response(12345, body_bytes)

    println('Full Response Hex: ${full_response.hex()}')

    // 4. Verify Bytes
    mut reader := new_reader(full_response)
    
    // Size (4 bytes)
    size := reader.read_i32()!
    assert size == full_response.len - 4

    // Correlation ID (4 bytes)
    correlation_id := reader.read_i32()!
    assert correlation_id == 12345

    // -- Body Start --
    // Error Code (2 bytes)
    error_code := reader.read_i16()!
    assert error_code == 0

    // ApiKeys Compact Array Length (varint)
    // 1 element -> length 2 (1 item + 1)
    // 2 in varint is 0x02
    api_keys_len := reader.read_uvarint()!
    assert api_keys_len == 2

    // ApiKey (2 bytes) = 18
    api_key := reader.read_i16()!
    assert api_key == 18

    // MinVersion (2 bytes) = 0
    min_ver := reader.read_i16()!
    assert min_ver == 0

    // MaxVersion (2 bytes) = 3
    max_ver := reader.read_i16()!
    assert max_ver == 3

    // Tagged Fields for ApiKey (varint 0)
    tags1 := reader.read_uvarint()!
    assert tags1 == 0

    // ThrottleTimeMs (4 bytes) = 0
    throttle := reader.read_i32()!
    assert throttle == 0

    // SupportedFeatures Compact Array Length (varint)
    // 0 elements -> length 1 (0 + 1) -> 0x01
    supp_feat_len := reader.read_uvarint()!
    assert supp_feat_len == 1

    // FinalizedFeaturesEpoch (8 bytes) = -1
    epoch := reader.read_i64()!
    assert epoch == -1

    // FinalizedFeatures Compact Array Length (varint)
    // 0 elements -> length 1 (0 + 1) -> 0x01
    fin_feat_len := reader.read_uvarint()!
    assert fin_feat_len == 1

    // Tagged Fields for Body (varint 0)
    body_tags := reader.read_uvarint()!
    assert body_tags == 0

    println('Verification successful')
}
