// Infra Layer - S3 Offset Snapshot Binary Codec
// Encodes/decodes consumer group offsets as binary snapshots.
//
// Binary format v1:
//   +-------+----------+-----------+-----------+-------------+----------+-------+
//   | magic | version  | broker_id | timestamp | entry_count | entries  | crc32 |
//   | 2B    | 4B       | 4B        | 8B        | 4B          | variable | 4B    |
//   +-------+----------+-----------+-----------+-------------+----------+-------+
//
// Entry format:
//   +----------+-------+-----------+--------+--------------+----------+----------+--------------+
//   | topic_len| topic | partition | offset | leader_epoch | meta_len | metadata | committed_at |
//   | 2B       | var   | 4B        | 8B     | 4B           | 2B       | var      | 8B           |
//   +----------+-------+-----------+--------+--------------+----------+----------+--------------+
//
// All integer fields use big-endian byte order.
// CRC32 checksum is computed over data from magic through the last entry using the IEEE polynomial.
module s3

import json
import time
import domain
import infra.performance.core

// Offset snapshot magic bytes (DataCore Offset Snapshot v1)
const offset_snapshot_magic = u16(0xDC01)

// Header size: magic(2) + version(4) + broker_id(4) + timestamp(8) + entry_count(4)
const offset_snapshot_header_size = 22

// Minimum valid snapshot size: header(22) + crc32(4)
const offset_snapshot_min_size = 26

// Entry fixed field size: topic_len(2) + partition(4) + offset(8) + leader_epoch(4) + metadata_len(2) + committed_at(8)
const offset_entry_fixed_size = 28

// CRC32 checksum size
const offset_snapshot_crc_size = 4

// OffsetEntry represents committed offset information for a single partition.
struct OffsetEntry {
pub mut:
	offset       i64
	leader_epoch i32
	metadata     string
	committed_at i64
}

// OffsetSnapshot manages all offsets of a consumer group as a binary snapshot.
struct OffsetSnapshot {
pub mut:
	version   i32
	broker_id i32
	timestamp i64
	offsets   map[string]OffsetEntry
}

// new_offset_snapshot creates an empty snapshot with the specified broker_id.
fn new_offset_snapshot(broker_id i32) OffsetSnapshot {
	return OffsetSnapshot{
		version:   0
		broker_id: broker_id
		timestamp: time.now().unix_milli()
		offsets:   map[string]OffsetEntry{}
	}
}

// Encoding

// calculate_snapshot_size accurately calculates the encoded binary size of a snapshot.
fn calculate_snapshot_size(snapshot OffsetSnapshot) int {
	mut size := offset_snapshot_header_size
	for key, entry in snapshot.offsets {
		parts := key.split(':')
		topic := if parts.len >= 2 {
			parts[0..parts.len - 1].join(':')
		} else {
			key
		}
		size += offset_entry_fixed_size + topic.len + entry.metadata.len
	}
	size += offset_snapshot_crc_size
	return size
}

// encode_snapshot_header writes the snapshot header (magic, version, broker_id, timestamp, entry_count).
fn encode_snapshot_header(mut buf []u8, snapshot OffsetSnapshot) {
	buf << u8(offset_snapshot_magic >> 8)
	buf << u8(offset_snapshot_magic)
	core.write_i32_be(mut buf, snapshot.version)
	core.write_i32_be(mut buf, snapshot.broker_id)
	core.write_i64_be(mut buf, snapshot.timestamp)
	core.write_i32_be(mut buf, i32(snapshot.offsets.len))
}

// parse_snapshot_composite_key splits a composite key into topic name and partition.
fn parse_snapshot_composite_key(composite_key string) (string, int) {
	parts := composite_key.split(':')
	topic_name := if parts.len >= 2 {
		parts[0..parts.len - 1].join(':')
	} else {
		composite_key
	}
	partition := if parts.len >= 2 { parts[parts.len - 1].int() } else { 0 }
	return topic_name, partition
}

// encode_snapshot_entry writes a single offset entry to the buffer.
fn encode_snapshot_entry(mut buf []u8, composite_key string, entry OffsetEntry) {
	topic_name, partition := parse_snapshot_composite_key(composite_key)
	topic_bytes := topic_name.bytes()
	core.write_i16_be(mut buf, i16(topic_bytes.len))
	buf << topic_bytes
	core.write_i32_be(mut buf, i32(partition))
	core.write_i64_be(mut buf, entry.offset)
	core.write_i32_be(mut buf, entry.leader_epoch)
	metadata_bytes := entry.metadata.bytes()
	core.write_i16_be(mut buf, i16(metadata_bytes.len))
	buf << metadata_bytes
	core.write_i64_be(mut buf, entry.committed_at)
}

// encode_offset_snapshot encodes an OffsetSnapshot to binary format.
fn encode_offset_snapshot(snapshot OffsetSnapshot) []u8 {
	exact_size := calculate_snapshot_size(snapshot)
	mut buf := []u8{cap: exact_size}

	encode_snapshot_header(mut buf, snapshot)

	for composite_key, entry in snapshot.offsets {
		encode_snapshot_entry(mut buf, composite_key, entry)
	}

	checksum := core.crc32_ieee(buf)
	core.write_u32_be(mut buf, checksum)
	return buf
}

// Decoding

// verify_snapshot_crc verifies the CRC32 checksum of snapshot data.
// The last 4 bytes contain the CRC32; computed over all preceding data and compared.
fn verify_snapshot_crc(data []u8) ! {
	payload := data[0..data.len - offset_snapshot_crc_size]
	stored_crc := core.read_u32_be(data[data.len - offset_snapshot_crc_size..])
	computed_crc := core.crc32_ieee(payload)
	if stored_crc != computed_crc {
		return error('offset snapshot CRC32 mismatch: stored=0x${stored_crc:08X}, computed=0x${computed_crc:08X}')
	}
}

// decode_snapshot_header decodes header fields after the magic bytes.
// Returns: (version, broker_id, timestamp, entry_count, next read position)
fn decode_snapshot_header(data []u8) !(i32, i32, i64, i32, int) {
	mut pos := 2

	version := core.read_i32_be(data[pos..])
	pos += 4

	broker_id := core.read_i32_be(data[pos..])
	pos += 4

	timestamp := core.read_i64_be(data[pos..])
	pos += 8

	entry_count := core.read_i32_be(data[pos..])
	pos += 4

	if entry_count < 0 {
		return error('invalid entry count: ${entry_count}')
	}

	return version, broker_id, timestamp, entry_count, pos
}

// read_length_prefixed_string reads a 2-byte length-prefixed string from data.
// Returns the string and the new position.
fn read_length_prefixed_string(data []u8, start_pos int, payload_len int, field_name string) !(string, int) {
	mut pos := start_pos
	if pos + 2 > payload_len {
		return error('unexpected end of data while reading ${field_name} length at pos ${pos}')
	}
	str_len := int(core.read_i16_be(data[pos..]))
	pos += 2
	if str_len < 0 || pos + str_len > payload_len {
		return error('invalid ${field_name} length: ${str_len} at pos ${pos}')
	}
	result := data[pos..pos + str_len].bytestr()
	pos += str_len
	return result, pos
}

// read_fixed_field reads a fixed-size integer field with bounds checking.
fn read_fixed_field_i32(data []u8, pos int, payload_len int, field_name string) !(i32, int) {
	if pos + 4 > payload_len {
		return error('unexpected end of data while reading ${field_name} at pos ${pos}')
	}
	return core.read_i32_be(data[pos..]), pos + 4
}

// read_fixed_field_i64 reads a fixed-size i64 field with bounds checking.
fn read_fixed_field_i64(data []u8, pos int, payload_len int, field_name string) !(i64, int) {
	if pos + 8 > payload_len {
		return error('unexpected end of data while reading ${field_name} at pos ${pos}')
	}
	return core.read_i64_be(data[pos..]), pos + 8
}

// decode_snapshot_entry decodes a single offset entry at the specified position.
// Returns: (composite_key, entry, next read position)
fn decode_snapshot_entry(data []u8, start_pos int, payload_len int) !(string, OffsetEntry, int) {
	topic_name, mut pos := read_length_prefixed_string(data, start_pos, payload_len, 'topic')!
	partition, pos2 := read_fixed_field_i32(data, pos, payload_len, 'partition')!
	pos = pos2
	offset, pos3 := read_fixed_field_i64(data, pos, payload_len, 'offset')!
	pos = pos3
	leader_epoch, pos4 := read_fixed_field_i32(data, pos, payload_len, 'leader_epoch')!
	pos = pos4
	metadata, pos5 := read_length_prefixed_string(data, pos, payload_len, 'metadata')!
	pos = pos5
	committed_at, pos6 := read_fixed_field_i64(data, pos, payload_len, 'committed_at')!
	pos = pos6

	return '${topic_name}:${partition}', OffsetEntry{
		offset:       offset
		leader_epoch: leader_epoch
		metadata:     metadata
		committed_at: committed_at
	}, pos
}

// decode_offset_snapshot decodes binary data into an OffsetSnapshot.
// Validates magic bytes and CRC32 checksum.
fn decode_offset_snapshot(data []u8) !OffsetSnapshot {
	if data.len < offset_snapshot_min_size {
		return error('offset snapshot data too short: ${data.len} bytes')
	}

	// Verify magic bytes
	magic := core.read_u16_be(data[0..])
	if magic != offset_snapshot_magic {
		return error('invalid offset snapshot magic: 0x${magic:04X}, expected 0xDC01')
	}

	// Verify CRC32 checksum
	verify_snapshot_crc(data)!

	// Decode header
	version, broker_id, timestamp, entry_count, mut pos := decode_snapshot_header(data)!

	// Decode entries
	payload_len := data.len - offset_snapshot_crc_size
	mut offsets := map[string]OffsetEntry{}
	for _ in 0 .. entry_count {
		key, entry, new_pos := decode_snapshot_entry(data, pos, payload_len)!
		pos = new_pos
		offsets[key] = entry
	}

	return OffsetSnapshot{
		version:   version
		broker_id: broker_id
		timestamp: timestamp
		offsets:   offsets
	}
}

// Auto-detection decoding

// try_decode_offset_data checks the magic bytes and auto-detects binary or JSON format.
// If 0xDC01: decode as binary; if '{': decode as legacy JSON.
fn try_decode_offset_data(data []u8) !OffsetSnapshot {
	if data.len < 2 {
		return error('offset data too short: ${data.len} bytes')
	}

	// Check for binary format (magic = 0xDC01)
	magic := core.read_u16_be(data[0..])
	if magic == offset_snapshot_magic {
		return decode_offset_snapshot(data)
	}

	// Check for legacy JSON format
	if data[0] == u8(`{`) {
		return decode_legacy_json_snapshot(data)
	}

	return error('unknown offset data format: first bytes=0x${data[0]:02X}${data[1]:02X}')
}

// Legacy JSON compatibility

// decode_legacy_json_snapshot converts the legacy individual JSON format (domain.PartitionOffset)
// into an OffsetSnapshot.
fn decode_legacy_json_snapshot(data []u8) !OffsetSnapshot {
	offset_data := json.decode(domain.PartitionOffset, data.bytestr()) or {
		return error('failed to decode legacy JSON offset: ${err.msg()}')
	}

	composite_key := '${offset_data.topic}:${offset_data.partition}'
	mut offsets := map[string]OffsetEntry{}
	offsets[composite_key] = OffsetEntry{
		offset:       offset_data.offset
		leader_epoch: offset_data.leader_epoch
		metadata:     offset_data.metadata
		committed_at: time.now().unix_milli()
	}

	return OffsetSnapshot{
		version:   0
		broker_id: 0
		timestamp: time.now().unix_milli()
		offsets:   offsets
	}
}

// Snapshot merging

// merge_offset_snapshots merges local and remote snapshots.
// Selects the higher offset for each "topic:partition",
// and sets version to max(local, remote) + 1.
fn merge_offset_snapshots(local OffsetSnapshot, remote OffsetSnapshot) OffsetSnapshot {
	mut merged := map[string]OffsetEntry{}

	// Copy local entries
	for key, entry in local.offsets {
		merged[key] = entry
	}

	// Merge remote entries (select higher offset)
	for key, remote_entry in remote.offsets {
		if key in merged {
			local_entry := merged[key]
			if remote_entry.offset > local_entry.offset {
				merged[key] = remote_entry
			}
		} else {
			merged[key] = remote_entry
		}
	}

	// version = max(local, remote) + 1
	max_version := if local.version > remote.version {
		local.version
	} else {
		remote.version
	}

	return OffsetSnapshot{
		version:   max_version + 1
		broker_id: local.broker_id
		timestamp: time.now().unix_milli()
		offsets:   merged
	}
}
