// Kafka Protocol - Share Group Encoders (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// Response encoding methods
module kafka

// encode encodes a ShareGroupHeartbeatResponse to bytes
pub fn (r ShareGroupHeartbeatResponse) encode(version i16) []u8 {
	is_flexible := true // Share Group APIs are always flexible
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// MemberId (nullable)
	if is_flexible {
		if r.member_id.len > 0 {
			writer.write_compact_string(r.member_id)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.member_id)
	}

	// MemberEpoch
	writer.write_i32(r.member_epoch)

	// HeartbeatIntervalMs
	writer.write_i32(r.heartbeat_interval_ms)

	// Assignment (nullable)
	if assignment := r.assignment {
		// Write assignment
		if is_flexible {
			writer.write_compact_array_len(assignment.topic_partitions.len)
		} else {
			writer.write_array_len(assignment.topic_partitions.len)
		}
		for tp in assignment.topic_partitions {
			// TopicId (UUID)
			writer.write_uuid(tp.topic_id)
			// Partitions
			if is_flexible {
				writer.write_compact_array_len(tp.partitions.len)
			} else {
				writer.write_array_len(tp.partitions.len)
			}
			for p in tp.partitions {
				writer.write_i32(p)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	} else {
		// Null assignment
		if is_flexible {
			writer.write_compact_array_len(-1)
		} else {
			writer.write_array_len(-1)
		}
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes a ShareFetchResponse to bytes
pub fn (r ShareFetchResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// AcquisitionLockTimeoutMs (v1+)
	if version >= 1 {
		writer.write_i32(r.acquisition_lock_timeout_ms)
	}

	// Responses array
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId (UUID)
		writer.write_uuid(topic_resp.topic_id)

		// Partitions array
		if is_flexible {
			writer.write_compact_array_len(topic_resp.partitions.len)
		} else {
			writer.write_array_len(topic_resp.partitions.len)
		}
		for part_resp in topic_resp.partitions {
			// PartitionIndex
			writer.write_i32(part_resp.partition_index)
			// ErrorCode
			writer.write_i16(part_resp.error_code)
			// ErrorMessage (nullable)
			if is_flexible {
				if part_resp.error_message.len > 0 {
					writer.write_compact_string(part_resp.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// AcknowledgeErrorCode
			writer.write_i16(part_resp.acknowledge_error_code)
			// AcknowledgeErrorMessage (nullable)
			if is_flexible {
				if part_resp.acknowledge_error_message.len > 0 {
					writer.write_compact_string(part_resp.acknowledge_error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// CurrentLeader
			writer.write_i32(part_resp.current_leader.leader_id)
			writer.write_i32(part_resp.current_leader.leader_epoch)
			if is_flexible {
				writer.write_tagged_fields()
			}
			// Records (nullable bytes)
			if is_flexible {
				if part_resp.records.len > 0 {
					writer.write_compact_bytes(part_resp.records)
				} else {
					// Write null as empty compact bytes (length 0+1 = 1 in unsigned varint)
					writer.write_uvarint(1) // length + 1 = 1 means 0 bytes
				}
			} else {
				if part_resp.records.len > 0 {
					writer.write_bytes(part_resp.records)
				} else {
					writer.write_i32(-1) // null bytes
				}
			}
			// AcquiredRecords array
			if is_flexible {
				writer.write_compact_array_len(part_resp.acquired_records.len)
			} else {
				writer.write_array_len(part_resp.acquired_records.len)
			}
			for ar in part_resp.acquired_records {
				writer.write_i64(ar.first_offset)
				writer.write_i64(ar.last_offset)
				writer.write_i16(ar.delivery_count)
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// NodeEndpoints array
	if is_flexible {
		writer.write_compact_array_len(r.node_endpoints.len)
	} else {
		writer.write_array_len(r.node_endpoints.len)
	}
	for ep in r.node_endpoints {
		writer.write_i32(ep.node_id)
		if is_flexible {
			writer.write_compact_string(ep.host)
		} else {
			writer.write_string(ep.host)
		}
		writer.write_i32(ep.port)
		if is_flexible {
			if ep.rack.len > 0 {
				writer.write_compact_string(ep.rack)
			} else {
				writer.write_compact_nullable_string(none)
			}
		} else {
			writer.write_nullable_string(ep.rack)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes a ShareAcknowledgeResponse to bytes
pub fn (r ShareAcknowledgeResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// Responses array
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId
		writer.write_uuid(topic_resp.topic_id)

		// Partitions array
		if is_flexible {
			writer.write_compact_array_len(topic_resp.partitions.len)
		} else {
			writer.write_array_len(topic_resp.partitions.len)
		}
		for part_resp in topic_resp.partitions {
			writer.write_i32(part_resp.partition_index)
			writer.write_i16(part_resp.error_code)
			if is_flexible {
				if part_resp.error_message.len > 0 {
					writer.write_compact_string(part_resp.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// CurrentLeader
			writer.write_i32(part_resp.current_leader.leader_id)
			writer.write_i32(part_resp.current_leader.leader_epoch)
			if is_flexible {
				writer.write_tagged_fields()
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// NodeEndpoints array
	if is_flexible {
		writer.write_compact_array_len(r.node_endpoints.len)
	} else {
		writer.write_array_len(r.node_endpoints.len)
	}
	for ep in r.node_endpoints {
		writer.write_i32(ep.node_id)
		if is_flexible {
			writer.write_compact_string(ep.host)
		} else {
			writer.write_string(ep.host)
		}
		writer.write_i32(ep.port)
		if is_flexible {
			if ep.rack.len > 0 {
				writer.write_compact_string(ep.rack)
			} else {
				writer.write_compact_nullable_string(none)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
