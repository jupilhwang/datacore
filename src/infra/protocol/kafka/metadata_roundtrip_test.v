module kafka

// Roundtrip test: encode MetadataResponse (flexible v12) and decode with our parser
fn test_metadata_response_roundtrip_v12() {
	// Build a sample metadata response with one broker and one topic
	mut resp := MetadataResponse{
		throttle_time_ms:       0
		brokers:                [
			MetadataResponseBroker{
				node_id: 1
				host:    '0.0.0.0'
				port:    9092
				rack:    none
			},
		]
		cluster_id:             'datacore-cluster'
		controller_id:          1
		topics:                 [
			MetadataResponseTopic{
				error_code:           0
				name:                 'roundtrip-topic'
				topic_id:             []u8{len: 16}
				is_internal:          false
				partitions:           [
					MetadataResponsePartition{
						error_code:       0
						partition_index:  0
						leader_id:        1
						leader_epoch:     0
						replica_nodes:    [i32(1)]
						isr_nodes:        [i32(1)]
						offline_replicas: []
					},
				]
				topic_authorized_ops: -2147483648
			},
		]
		cluster_authorized_ops: -2147483648
	}

	// Encode as flexible v12
	body := resp.encode(12)

	// Now parse the body using our BinaryReader/parsers to verify structure
	mut reader := new_reader(body)
	// throttle_time_ms
	_ = reader.read_i32()!

	// brokers compact array len
	bcount := reader.read_uvarint()!
	// bcount is N+1, expect at least 1
	assert bcount > 0
	// iterate brokers
	for _ in 0 .. int(bcount - 1) {
		node_id := reader.read_i32()!
		host := reader.read_compact_string()!
		port := reader.read_i32()!
		_ = reader.read_compact_nullable_string()!
		// skip tagged fields
		reader.skip_tagged_fields()!
		assert node_id == 1
		assert host == '0.0.0.0'
		assert port == 9092
	}

	// cluster_id
	_ = reader.read_compact_nullable_string()!
	// controller id
	ctrl := reader.read_i32()!
	assert ctrl == 1

	// topics compact array len
	tcount := reader.read_uvarint()!
	assert tcount > 0
	// iterate topics
	for _ in 0 .. int(tcount - 1) {
		_ = reader.read_i16()! // error_code
		name := reader.read_compact_string()!
		// topic_id (16)
		tid := reader.read_uuid()!
		is_internal := reader.read_i8()!
		pv := reader.read_uvarint()!
		// partitions
		for _ in 0 .. int(pv - 1) {
			_ = reader.read_i16()!
			_ = reader.read_i32()!
			_ = reader.read_i32()!
			// now replicas/isr/offline arrays
			rv := reader.read_uvarint()!
			for _ in 0 .. int(rv - 1) {
				_ = reader.read_i32()!
			}
			iv := reader.read_uvarint()!
			for _ in 0 .. int(iv - 1) {
				_ = reader.read_i32()!
			}
			ov := reader.read_uvarint()!
			for _ in 0 .. int(ov - 1) {
				_ = reader.read_i32()!
			}
			// skip partition tagged fields
			reader.skip_tagged_fields()!
		}
		// skip topic tagged fields
		reader.skip_tagged_fields()!
		assert name == 'roundtrip-topic'
		assert tid.len == 16
	}
}
