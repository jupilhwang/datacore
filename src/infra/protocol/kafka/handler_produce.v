// Produce handlers - Produce, Fetch, ListOffsets
module kafka

import domain

// Produce handler - stores records in storage
fn (mut h Handler) handle_produce(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!

	// Validate transactional producer if transactional_id is present
	if txn_id := req.transactional_id {
		if txn_id.len > 0 {
			if mut txn_coord := h.txn_coordinator {
				// Verify the transaction is in a valid state for producing
				meta := txn_coord.get_transaction(txn_id) or {
					return build_produce_error_response(req, i16(ErrorCode.transactional_id_not_found),
						version)
				}

				if meta.state != .ongoing {
					return build_produce_error_response(req, i16(ErrorCode.invalid_txn_state),
						version)
				}

				for t in req.topic_data {
					topic_name := if t.name.len > 0 {
						t.name
					} else {
						if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
							topic_meta.name
						} else {
							continue
						}
					}

					for p in t.partition_data {
						mut found := false
						for tp in meta.topic_partitions {
							if tp.topic == topic_name && tp.partition == int(p.index) {
								found = true
								break
							}
						}
						if !found {
							return build_produce_error_response(req, i16(ErrorCode.invalid_txn_state),
								version)
						}
					}
				}
			} else {
				return build_produce_error_response(req, i16(ErrorCode.coordinator_not_available),
					version)
			}
		}
	}

	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				mut partitions := []ProduceResponsePartition{}
				for p in t.partition_data {
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       i16(ErrorCode.unknown_topic_id)
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
				}
				topics << ProduceResponseTopic{
					name:       topic_name
					topic_id:   topic_id
					partitions: partitions
				}
				continue
			}
		}

		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			parsed := parse_record_batch(p.records) or {
				partitions << ProduceResponsePartition{
					index:            p.index
					error_code:       i16(ErrorCode.corrupt_message)
					base_offset:      -1
					log_append_time:  -1
					log_start_offset: -1
				}
				continue
			}

			if parsed.records.len == 0 {
				partitions << ProduceResponsePartition{
					index:            p.index
					error_code:       0
					base_offset:      0
					log_append_time:  -1
					log_start_offset: 0
				}
				continue
			}

			result := h.storage.append(topic_name, int(p.index), parsed.records) or {
				if err.str().contains('not found') {
					num_partitions := if int(p.index) >= 1 { int(p.index) + 1 } else { 1 }
					h.storage.create_topic(topic_name, num_partitions, domain.TopicConfig{}) or {
						partitions << ProduceResponsePartition{
							index:            p.index
							error_code:       i16(ErrorCode.unknown_server_error)
							base_offset:      -1
							log_append_time:  -1
							log_start_offset: -1
						}
						continue
					}
					retry_result := h.storage.append(topic_name, int(p.index), parsed.records) or {
						partitions << ProduceResponsePartition{
							index:            p.index
							error_code:       i16(ErrorCode.unknown_server_error)
							base_offset:      -1
							log_append_time:  -1
							log_start_offset: -1
						}
						continue
					}
					retry_result
				} else {
					error_code := if err.str().contains('out of range') {
						i16(ErrorCode.unknown_topic_or_partition)
					} else {
						i16(ErrorCode.unknown_server_error)
					}
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       error_code
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
					continue
				}
			}

			partitions << ProduceResponsePartition{
				index:            p.index
				error_code:       0
				base_offset:      result.base_offset
				log_append_time:  result.log_append_time
				log_start_offset: result.log_start_offset
			}
		}
		topics << ProduceResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	resp := ProduceResponse{
		topics:           topics
		throttle_time_ms: 0
	}

	return resp.encode(version)
}

fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
	flexible := is_flexible_version(.fetch, version)
	req := parse_fetch_request_simple(body, version, flexible)!

	eprintln('[Fetch] Request: version=${version}, topics=${req.topics.len}')

	mut topics := []FetchResponseTopic{}
	for t in req.topics {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			}
		}

		mut partitions := []FetchResponsePartition{}
		for p in t.partitions {
			result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
				error_code := if err.str().contains('not found') {
					i16(ErrorCode.unknown_topic_or_partition)
				} else if err.str().contains('out of range') {
					i16(ErrorCode.offset_out_of_range)
				} else {
					i16(ErrorCode.unknown_server_error)
				}

				partitions << FetchResponsePartition{
					partition_index:    p.partition
					error_code:         error_code
					high_watermark:     0
					last_stable_offset: 0
					log_start_offset:   0
					records:            []u8{}
				}
				continue
			}

			records_data := encode_record_batch_zerocopy(result.records, p.fetch_offset)
			eprintln('[Fetch] Topic=${topic_name} partition=${p.partition} has ${result.records.len} records - returning ${records_data.len} bytes')

			partitions << FetchResponsePartition{
				partition_index:    p.partition
				error_code:         0
				high_watermark:     result.high_watermark
				last_stable_offset: result.last_stable_offset
				log_start_offset:   result.log_start_offset
				records:            records_data
			}
		}
		topics << FetchResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	resp := FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           topics
	}

	encoded := resp.encode(version)
	eprintln('[Fetch] Response version=${version}, size=${encoded.len} bytes')
	if encoded.len > 0 && encoded.len < 200 {
		eprintln('[Fetch] First 100 bytes: ${encoded[..if encoded.len > 100 {
			100
		} else {
			encoded.len
		}].hex()}')
	}

	return encoded
}

// ListOffsets handler - retrieves partition offset info from storage
fn (mut h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets,
		version))!

	mut topics := []ListOffsetsResponseTopic{}
	for t in req.topics {
		mut partitions := []ListOffsetsResponsePartition{}
		for p in t.partitions {
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				partitions << ListOffsetsResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
					timestamp:       -1
					offset:          -1
					leader_epoch:    -1
				}
				continue
			}

			offset := match p.timestamp {
				-1 { info.latest_offset }
				-2 { info.earliest_offset }
				else { info.latest_offset }
			}

			partitions << ListOffsetsResponsePartition{
				partition_index: p.partition_index
				error_code:      0
				timestamp:       p.timestamp
				offset:          offset
				leader_epoch:    0
			}
		}
		topics << ListOffsetsResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}

	resp := ListOffsetsResponse{
		throttle_time_ms: 0
		topics:           topics
	}

	return resp.encode(version)
}

// build_produce_error_response builds a ProduceResponse with error for all partitions
fn build_produce_error_response(req ProduceRequest, error_code i16, version i16) []u8 {
	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			partitions << ProduceResponsePartition{
				index:            p.index
				error_code:       error_code
				base_offset:      -1
				log_append_time:  -1
				log_start_offset: -1
			}
		}
		topics << ProduceResponseTopic{
			name:       t.name
			topic_id:   t.topic_id.clone()
			partitions: partitions
		}
	}
	return ProduceResponse{
		topics:           topics
		throttle_time_ms: 0
	}.encode(version)
}

// Process produce request (Frame-based stub)
fn (mut h Handler) process_produce(req ProduceRequest, version i16) !ProduceResponse {
	return ProduceResponse{
		topics:           []
		throttle_time_ms: 0
	}
}

// Process fetch request (Frame-based stub)
fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           []
	}
}

// Process list offsets request (Frame-based stub)
fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
	return ListOffsetsResponse{
		throttle_time_ms: 0
		topics:           []
	}
}
