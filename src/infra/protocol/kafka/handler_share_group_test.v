// Share Group 핸들러 단위 테스트
// ShareGroupHeartbeat, ShareFetch, ShareAcknowledge 응답 인코딩 및 구조 검증
module kafka

// -- ShareGroupHeartbeatResponse.encode 테스트 --

fn test_share_group_heartbeat_response_encode_basic() {
	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            0
		error_message:         ''
		member_id:             'member-1'
		member_epoch:          1
		heartbeat_interval_ms: 5000
		assignment:            none
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'heartbeat 응답 인코딩 결과는 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == 0
}

fn test_share_group_heartbeat_response_encode_with_error() {
	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            i16(ErrorCode.coordinator_not_available)
		error_message:         'coordinator not available'
		member_id:             ''
		member_epoch:          0
		heartbeat_interval_ms: 5000
		assignment:            none
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == i16(ErrorCode.coordinator_not_available)
}

fn test_share_group_heartbeat_response_encode_with_assignment() {
	topic_uuid := []u8{len: 16, init: u8(0xAB)}

	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            0
		error_message:         ''
		member_id:             'member-abc'
		member_epoch:          3
		heartbeat_interval_ms: 5000
		assignment:            ShareGroupAssignment{
			topic_partitions: [
				ShareGroupTopicPartitions{
					topic_id:   topic_uuid
					partitions: [i32(0), 1, 2]
				},
			]
		}
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'assignment 포함 인코딩은 비어있으면 안 된다'

	// 기본 구조 검증
	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == 0
}

fn test_share_group_heartbeat_response_encode_empty_assignment() {
	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      100
		error_code:            0
		error_message:         ''
		member_id:             'member-x'
		member_epoch:          5
		heartbeat_interval_ms: 3000
		assignment:            ShareGroupAssignment{
			topic_partitions: []
		}
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

fn test_share_group_heartbeat_response_encode_multiple_topic_partitions() {
	uuid1 := []u8{len: 16, init: u8(0x01)}
	uuid2 := []u8{len: 16, init: u8(0x02)}

	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            0
		error_message:         ''
		member_id:             'multi-member'
		member_epoch:          2
		heartbeat_interval_ms: 5000
		assignment:            ShareGroupAssignment{
			topic_partitions: [
				ShareGroupTopicPartitions{
					topic_id:   uuid1
					partitions: [i32(0), 1]
				},
				ShareGroupTopicPartitions{
					topic_id:   uuid2
					partitions: [i32(0), 1, 2, 3]
				},
			]
		}
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, '다중 토픽 파티션 assignment 인코딩은 비어있으면 안 된다'
}

// -- ShareFetchResponse.encode 테스트 --

fn test_share_fetch_response_encode_empty() {
	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 30000
		responses:                   []
		node_endpoints:              []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 빈 응답 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == 0
}

fn test_share_fetch_response_encode_with_error() {
	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  i16(ErrorCode.coordinator_not_available)
		error_message:               'coordinator not available'
		acquisition_lock_timeout_ms: 30000
		responses:                   []
		node_endpoints:              []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == i16(ErrorCode.coordinator_not_available)
}

fn test_share_fetch_response_encode_with_partition_data() {
	topic_uuid := []u8{len: 16, init: u8(0xCC)}

	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 30000
		responses:                   [
			ShareFetchTopicResponse{
				topic_id:   topic_uuid
				partitions: [
					ShareFetchPartitionResponse{
						partition_index:           0
						error_code:                0
						error_message:             ''
						acknowledge_error_code:    0
						acknowledge_error_message: ''
						current_leader:            ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
						records:                   []u8{}
						acquired_records:          [
							ShareAcquiredRecords{
								first_offset:   0
								last_offset:    9
								delivery_count: 1
							},
						]
					},
				]
			},
		]
		node_endpoints:              []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, '파티션 데이터 포함 인코딩은 비어있으면 안 된다'
}

fn test_share_fetch_response_encode_v1_with_lock_timeout() {
	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 60000
		responses:                   []
		node_endpoints:              []
	}

	// v1 includes acquisition_lock_timeout_ms
	encoded := resp.encode(1)
	assert encoded.len > 0, 'v1 인코딩은 비어있으면 안 된다'
}

fn test_share_fetch_response_encode_with_node_endpoints() {
	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 30000
		responses:                   []
		node_endpoints:              [
			ShareNodeEndpoint{
				node_id: 1
				host:    'broker-1'
				port:    9092
				rack:    'rack-a'
			},
			ShareNodeEndpoint{
				node_id: 2
				host:    'broker-2'
				port:    9092
				rack:    ''
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'node endpoints 포함 인코딩은 비어있으면 안 된다'
}

fn test_share_fetch_response_encode_multiple_acquired_records() {
	topic_uuid := []u8{len: 16, init: u8(0xDD)}

	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 30000
		responses:                   [
			ShareFetchTopicResponse{
				topic_id:   topic_uuid
				partitions: [
					ShareFetchPartitionResponse{
						partition_index:           0
						error_code:                0
						error_message:             ''
						acknowledge_error_code:    0
						acknowledge_error_message: ''
						current_leader:            ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
						records:                   []u8{}
						acquired_records:          [
							ShareAcquiredRecords{
								first_offset:   0
								last_offset:    4
								delivery_count: 1
							},
							ShareAcquiredRecords{
								first_offset:   5
								last_offset:    9
								delivery_count: 2
							},
						]
					},
				]
			},
		]
		node_endpoints:              []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

// -- ShareAcknowledgeResponse.encode 테스트 --

fn test_share_acknowledge_response_encode_empty() {
	resp := ShareAcknowledgeResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		error_message:    ''
		responses:        []
		node_endpoints:   []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'acknowledge 빈 응답 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == default_throttle_time_ms

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == 0
}

fn test_share_acknowledge_response_encode_with_error() {
	resp := ShareAcknowledgeResponse{
		throttle_time_ms: 0
		error_code:       i16(ErrorCode.coordinator_not_available)
		error_message:    'coordinator not available'
		responses:        []
		node_endpoints:   []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == i16(ErrorCode.coordinator_not_available)
}

fn test_share_acknowledge_response_encode_with_partitions() {
	topic_uuid := []u8{len: 16, init: u8(0xEE)}

	resp := ShareAcknowledgeResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    ''
		responses:        [
			ShareAcknowledgeTopicResponse{
				topic_id:   topic_uuid
				partitions: [
					ShareAcknowledgePartitionResponse{
						partition_index: 0
						error_code:      0
						error_message:   ''
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
					ShareAcknowledgePartitionResponse{
						partition_index: 1
						error_code:      0
						error_message:   ''
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
				]
			},
		]
		node_endpoints:   []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'partition 응답 포함 인코딩은 비어있으면 안 된다'
}

fn test_share_acknowledge_response_encode_partition_error() {
	topic_uuid := []u8{len: 16, init: u8(0xFF)}

	resp := ShareAcknowledgeResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    ''
		responses:        [
			ShareAcknowledgeTopicResponse{
				topic_id:   topic_uuid
				partitions: [
					ShareAcknowledgePartitionResponse{
						partition_index: 0
						error_code:      i16(ErrorCode.unknown_server_error)
						error_message:   'acknowledge failed'
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
				]
			},
		]
		node_endpoints:   []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

fn test_share_acknowledge_response_encode_multiple_topics() {
	uuid1 := []u8{len: 16, init: u8(0x11)}
	uuid2 := []u8{len: 16, init: u8(0x22)}

	resp := ShareAcknowledgeResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    ''
		responses:        [
			ShareAcknowledgeTopicResponse{
				topic_id:   uuid1
				partitions: [
					ShareAcknowledgePartitionResponse{
						partition_index: 0
						error_code:      0
						error_message:   ''
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
				]
			},
			ShareAcknowledgeTopicResponse{
				topic_id:   uuid2
				partitions: [
					ShareAcknowledgePartitionResponse{
						partition_index: 0
						error_code:      0
						error_message:   ''
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
					ShareAcknowledgePartitionResponse{
						partition_index: 1
						error_code:      0
						error_message:   ''
						current_leader:  ShareLeaderIdAndEpoch{
							leader_id:    1
							leader_epoch: 0
						}
					},
				]
			},
		]
		node_endpoints:   []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, '다중 토픽 acknowledge 인코딩은 비어있으면 안 된다'
}

fn test_share_acknowledge_response_encode_with_node_endpoints() {
	resp := ShareAcknowledgeResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    ''
		responses:        []
		node_endpoints:   [
			ShareNodeEndpoint{
				node_id: 1
				host:    'broker-1'
				port:    9092
				rack:    'us-east-1a'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

// -- ShareGroupHeartbeatRequest 구조 검증 --

fn test_share_group_heartbeat_request_struct() {
	req := ShareGroupHeartbeatRequest{
		group_id:               'share-group-1'
		member_id:              'member-1'
		member_epoch:           0
		rack_id:                'rack-a'
		subscribed_topic_names: ['topic-a', 'topic-b']
	}

	assert req.group_id == 'share-group-1'
	assert req.member_id == 'member-1'
	assert req.subscribed_topic_names.len == 2
	assert req.rack_id == 'rack-a'
}

// -- ShareFetchRequest 구조 검증 --

fn test_share_fetch_request_struct() {
	topic_uuid := []u8{len: 16, init: u8(0xAA)}

	req := ShareFetchRequest{
		group_id:            'share-group-1'
		member_id:           'member-1'
		share_session_epoch: 0
		max_wait_ms:         500
		min_bytes:           1
		max_bytes:           1048576
		max_records:         100
		batch_size:          0
		topics:              [
			ShareFetchTopic{
				topic_id:   topic_uuid
				partitions: [
					ShareFetchPartition{
						partition_index:         0
						acknowledgement_batches: []
					},
				]
			},
		]
		forgotten_topics:    []
	}

	assert req.group_id == 'share-group-1'
	assert req.topics.len == 1
	assert req.max_records == 100
}

// -- ShareAcknowledgeRequest 구조 검증 --

fn test_share_acknowledge_request_struct() {
	topic_uuid := []u8{len: 16, init: u8(0xBB)}

	req := ShareAcknowledgeRequest{
		group_id:            'share-group-1'
		member_id:           'member-1'
		share_session_epoch: 1
		topics:              [
			ShareAcknowledgeTopic{
				topic_id:   topic_uuid
				partitions: [
					ShareAcknowledgePartition{
						partition_index:         0
						acknowledgement_batches: [
							ShareAcknowledgementBatch{
								first_offset:      0
								last_offset:       9
								acknowledge_types: [u8(1), u8(1), u8(1)]
							},
						]
					},
				]
			},
		]
	}

	assert req.group_id == 'share-group-1'
	assert req.topics.len == 1
	assert req.topics[0].partitions[0].acknowledgement_batches.len == 1
	assert req.topics[0].partitions[0].acknowledgement_batches[0].first_offset == 0
	assert req.topics[0].partitions[0].acknowledgement_batches[0].last_offset == 9
}
