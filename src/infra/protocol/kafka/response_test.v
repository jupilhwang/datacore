module kafka

// Simple unit tests for response framing
fn test_flexible_response_correlation_id_endianness() {
	// small body to make assertion simple
	body := [u8(0), u8(1), u8(2), u8(3)]
	resp := build_flexible_response(1, body)

	// resp layout: [size:4][correlation_id:4][body...]
	// correlation_id == 1 should be encoded big-endian: 00 00 00 01
	assert resp.len >= 8
	assert resp[4] == 0
	assert resp[5] == 0
	assert resp[6] == 0
	assert resp[7] == 1
}

fn test_metadata_response_brokers_not_null() {
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
		topics:                 []MetadataResponseTopic{}
		cluster_authorized_ops: -2147483648
	}

	body := resp.encode(12)
	// throttle_time_ms (4 bytes) then compact_array_len for brokers
	assert body.len >= 5
	// compact array length byte must not be 0 (0 means NULL)
	assert body[4] != 0
}
