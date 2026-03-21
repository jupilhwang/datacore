// Share partition state operations for S3StorageAdapter.
// V's json module requires maps with string keys, so this file provides
// a JSON-serializable intermediate struct (SharePartitionStateJson) that
// converts map[i64]T fields to map[string]T for S3 JSON storage.
module s3

import domain
import json

// SharePartitionStateJson is a JSON-safe mirror of domain.SharePartitionState.
// All map keys are converted from i64 to string for json.encode/decode.
struct SharePartitionStateJson {
pub mut:
	group_id           string
	topic_name         string
	partition          i32
	start_offset       i64
	end_offset         i64
	record_states      map[string]u8
	acquired_records   map[string]domain.AcquiredRecordState
	delivery_counts    map[string]i32
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// share_partition_to_json converts a domain state to a JSON-serializable form.
fn share_partition_to_json(state domain.SharePartitionState) SharePartitionStateJson {
	mut rs := map[string]u8{}
	for k, v in state.record_states {
		rs[k.str()] = v
	}
	mut ar := map[string]domain.AcquiredRecordState{}
	for k, v in state.acquired_records {
		ar[k.str()] = v
	}
	mut dc := map[string]i32{}
	for k, v in state.delivery_counts {
		dc[k.str()] = v
	}
	return SharePartitionStateJson{
		group_id:           state.group_id
		topic_name:         state.topic_name
		partition:          state.partition
		start_offset:       state.start_offset
		end_offset:         state.end_offset
		record_states:      rs
		acquired_records:   ar
		delivery_counts:    dc
		total_acquired:     state.total_acquired
		total_acknowledged: state.total_acknowledged
		total_released:     state.total_released
		total_rejected:     state.total_rejected
	}
}

/// share_partition_from_json converts a JSON-deserialized form back to a domain state.
fn share_partition_from_json(j SharePartitionStateJson) domain.SharePartitionState {
	mut rs := map[i64]u8{}
	for k, v in j.record_states {
		rs[k.i64()] = v
	}
	mut ar := map[i64]domain.AcquiredRecordState{}
	for k, v in j.acquired_records {
		ar[k.i64()] = v
	}
	mut dc := map[i64]i32{}
	for k, v in j.delivery_counts {
		dc[k.i64()] = v
	}
	return domain.SharePartitionState{
		group_id:           j.group_id
		topic_name:         j.topic_name
		partition:          j.partition
		start_offset:       j.start_offset
		end_offset:         j.end_offset
		record_states:      rs
		acquired_records:   ar
		delivery_counts:    dc
		total_acquired:     j.total_acquired
		total_acknowledged: j.total_acknowledged
		total_released:     j.total_released
		total_rejected:     j.total_rejected
	}
}

/// save_share_partition_state saves a SharePartition state to S3 as JSON.
pub fn (mut a S3StorageAdapter) save_share_partition_state(state domain.SharePartitionState) ! {
	validate_identifier(state.group_id, 'group_id')!
	validate_identifier(state.topic_name, 'topic_name')!
	key := a.share_partition_key(state.group_id, state.topic_name, state.partition)
	data := json.encode(share_partition_to_json(state))
	a.put_object(key, data.bytes())!
}

/// load_share_partition_state loads a SharePartition state from S3.
/// Returns none if not found.
pub fn (mut a S3StorageAdapter) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	validate_identifier(group_id, 'group_id') or { return none }
	validate_identifier(topic_name, 'topic_name') or { return none }
	key := a.share_partition_key(group_id, topic_name, partition)
	data, _ := a.get_object(key, -1, -1) or { return none }
	j := json.decode(SharePartitionStateJson, data.bytestr()) or { return none }
	return share_partition_from_json(j)
}

/// delete_share_partition_state deletes a SharePartition state from S3.
pub fn (mut a S3StorageAdapter) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {
	validate_identifier(group_id, 'group_id')!
	validate_identifier(topic_name, 'topic_name')!
	key := a.share_partition_key(group_id, topic_name, partition)
	a.delete_objects_with_prefix(key)!
}

/// load_all_share_partition_states loads all SharePartition states for a group from S3.
/// Skips objects that fail to decode.
pub fn (mut a S3StorageAdapter) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	validate_identifier(group_id, 'group_id') or { return [] }
	prefix := '${a.config.prefix}share-partitions/${group_id}/'
	objects := a.list_objects(prefix) or { return [] }
	mut result := []domain.SharePartitionState{cap: objects.len}
	for obj in objects {
		data, _ := a.get_object(obj.key, -1, -1) or { continue }
		j := json.decode(SharePartitionStateJson, data.bytestr()) or { continue }
		result << share_partition_from_json(j)
	}
	return result
}
