// Infra Layer - Kafka Request Parsing - Offset Operations
// OffsetCommit, OffsetFetch request parsing
module kafka

// OffsetCommit Request
pub struct OffsetCommitRequest {
pub:
	group_id string
	topics   []OffsetCommitRequestTopic
}

pub struct OffsetCommitRequestTopic {
pub:
	name       string
	partitions []OffsetCommitRequestPartition
}

pub struct OffsetCommitRequestPartition {
pub:
	partition_index    i32
	committed_offset   i64
	committed_metadata string
}

fn parse_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetCommitRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	// v1+: generation_id
	if version >= 1 {
		_ = reader.read_i32()!
	}
	// v1+: member_id
	if version >= 1 {
		_ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}
	// v7+: group_instance_id
	if version >= 7 {
		_ = if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
	}
	// v2-v4: retention_time_ms (deprecated, removed in v5)
	if version >= 2 && version <= 4 {
		_ = reader.read_i64()!
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []OffsetCommitRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		pcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut partitions := []OffsetCommitRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			co := reader.read_i64()!
			// v6+: committed_leader_epoch
			if version >= 6 {
				_ = reader.read_i32()!
			}
			// v1-v4: commit_timestamp (deprecated, removed in v5)
			if version >= 1 && version <= 4 {
				_ = reader.read_i64()!
			}
			cm := if is_flexible { reader.read_compact_nullable_string() or { '' } } else { reader.read_nullable_string() or {
					''} }
			partitions << OffsetCommitRequestPartition{
				partition_index:    pi
				committed_offset:   co
				committed_metadata: cm
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
		topics << OffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	return OffsetCommitRequest{
		group_id: group_id
		topics:   topics
	}
}

pub struct OffsetFetchRequest {
pub:
	group_id       string
	topics         []OffsetFetchRequestTopic
	groups         []OffsetFetchRequestGroup
	require_stable bool
}

pub struct OffsetFetchRequestTopic {
pub:
	name       string
	partitions []i32
}

pub struct OffsetFetchRequestGroup {
pub:
	group_id     string
	member_id    ?string
	member_epoch i32
	topics       []OffsetFetchRequestGroupTopic
}

pub struct OffsetFetchRequestGroupTopic {
pub:
	name       string
	partitions []i32
}

fn parse_offset_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetFetchRequest {
	mut group_id := ''
	mut topics := []OffsetFetchRequestTopic{}
	mut groups := []OffsetFetchRequestGroup{}
	mut require_stable := false

	if version <= 7 {
		group_id = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if count >= 0 {
			for _ in 0 .. count {
				name := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				pcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut partitions := []i32{}
				for _ in 0 .. pcount {
					partitions << reader.read_i32()!
				}
				topics << OffsetFetchRequestTopic{
					name:       name
					partitions: partitions
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	} else {
		gcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if gcount >= 0 {
			for _ in 0 .. gcount {
				gid := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				mut member_id := ?string(none)
				mut member_epoch := i32(-1)
				if version >= 9 {
					if is_flexible {
						mid := reader.read_compact_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					} else {
						mid := reader.read_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					}
					member_epoch = reader.read_i32()!
				}
				tcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut gtopics := []OffsetFetchRequestGroupTopic{}
				if tcount >= 0 {
					for _ in 0 .. tcount {
						name := if is_flexible {
							reader.read_compact_string()!
						} else {
							reader.read_string()!
						}
						pcount := if is_flexible {
							reader.read_compact_array_len()!
						} else {
							reader.read_array_len()!
						}
						mut partitions := []i32{}
						for _ in 0 .. pcount {
							partitions << reader.read_i32()!
						}
						gtopics << OffsetFetchRequestGroupTopic{
							name:       name
							partitions: partitions
						}
						if is_flexible {
							reader.skip_tagged_fields()!
						}
					}
				}
				groups << OffsetFetchRequestGroup{
					group_id:     gid
					member_id:    member_id
					member_epoch: member_epoch
					topics:       gtopics
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	}

	// v7+: require_stable
	if version >= 7 {
		require_stable = reader.read_i8()! != 0
	}

	return OffsetFetchRequest{
		group_id:       group_id
		topics:         topics
		groups:         groups
		require_stable: require_stable
	}
}
