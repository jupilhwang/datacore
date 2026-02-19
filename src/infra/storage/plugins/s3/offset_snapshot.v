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

// CRC32 IEEE checksum (inline implementation)

// Lookup table based on IEEE polynomial 0xEDB88320
const snapshot_crc32_table = init_snapshot_crc32_table()

fn init_snapshot_crc32_table() []u32 {
	mut table := []u32{len: 256}
	for i in 0 .. 256 {
		mut crc := u32(i)
		for _ in 0 .. 8 {
			if crc & 1 != 0 {
				crc = (crc >> 1) ^ u32(0xedb88320)
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

// crc32_checksum computes a CRC32 checksum using the IEEE polynomial.
fn crc32_checksum(data []u8) u32 {
	mut crc := u32(0xffffffff)
	for b in data {
		idx := int((crc ^ u32(b)) & 0xff)
		crc = (crc >> 8) ^ snapshot_crc32_table[idx]
	}
	return crc ^ u32(0xffffffff)
}

// Big-endian write helpers

fn write_i16_be(mut buf []u8, val i16) {
	buf << u8(val >> 8)
	buf << u8(val)
}

fn write_i32_be(mut buf []u8, val i32) {
	buf << u8(val >> 24)
	buf << u8(val >> 16)
	buf << u8(val >> 8)
	buf << u8(val)
}

fn write_i64_be(mut buf []u8, val i64) {
	buf << u8(val >> 56)
	buf << u8(val >> 48)
	buf << u8(val >> 40)
	buf << u8(val >> 32)
	buf << u8(val >> 24)
	buf << u8(val >> 16)
	buf << u8(val >> 8)
	buf << u8(val)
}

fn write_u32_be(mut buf []u8, val u32) {
	buf << u8(val >> 24)
	buf << u8(val >> 16)
	buf << u8(val >> 8)
	buf << u8(val)
}

// Big-endian read helpers

fn read_i16_be(data []u8, pos int) i16 {
	return i16(u16(data[pos]) << 8 | u16(data[pos + 1]))
}

fn read_i32_be(data []u8, pos int) i32 {
	return i32(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
}

fn read_i64_be(data []u8, pos int) i64 {
	return i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))
}

fn read_u16_be(data []u8, pos int) u16 {
	return u16(data[pos]) << 8 | u16(data[pos + 1])
}

fn read_u32_be(data []u8, pos int) u32 {
	return u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3])
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

// encode_offset_snapshot encodes an OffsetSnapshot to binary format.
fn encode_offset_snapshot(snapshot OffsetSnapshot) []u8 {
	exact_size := calculate_snapshot_size(snapshot)
	mut buf := []u8{cap: exact_size}

	// magic (2 bytes)
	buf << u8(offset_snapshot_magic >> 8)
	buf << u8(offset_snapshot_magic)

	// version (4 bytes)
	write_i32_be(mut buf, snapshot.version)

	// broker_id (4 bytes)
	write_i32_be(mut buf, snapshot.broker_id)

	// timestamp (8 bytes)
	write_i64_be(mut buf, snapshot.timestamp)

	// entry_count (4 bytes)
	write_i32_be(mut buf, i32(snapshot.offsets.len))

	// Encode each entry
	for composite_key, entry in snapshot.offsets {
		parts := composite_key.split(':')
		topic_name := if parts.len >= 2 {
			parts[0..parts.len - 1].join(':')
		} else {
			composite_key
		}
		partition := if parts.len >= 2 {
			parts[parts.len - 1].int()
		} else {
			0
		}

		// topic_len (2 bytes) + topic (N bytes)
		topic_bytes := topic_name.bytes()
		write_i16_be(mut buf, i16(topic_bytes.len))
		buf << topic_bytes

		// partition (4 bytes)
		write_i32_be(mut buf, i32(partition))

		// offset (8 bytes)
		write_i64_be(mut buf, entry.offset)

		// leader_epoch (4 bytes)
		write_i32_be(mut buf, entry.leader_epoch)

		// metadata_len (2 bytes) + metadata (N bytes)
		metadata_bytes := entry.metadata.bytes()
		write_i16_be(mut buf, i16(metadata_bytes.len))
		buf << metadata_bytes

		// committed_at (8 bytes)
		write_i64_be(mut buf, entry.committed_at)
	}

	// CRC32 checksum (from magic through the last entry)
	checksum := crc32_checksum(buf)
	write_u32_be(mut buf, checksum)

	return buf
}

// Decoding

// verify_snapshot_crc verifies the CRC32 checksum of snapshot data.
// The last 4 bytes contain the CRC32; computed over all preceding data and compared.
fn verify_snapshot_crc(data []u8) ! {
	payload := data[0..data.len - offset_snapshot_crc_size]
	stored_crc := read_u32_be(data, data.len - offset_snapshot_crc_size)
	computed_crc := crc32_checksum(payload)
	if stored_crc != computed_crc {
		return error('offset snapshot CRC32 mismatch: stored=0x${stored_crc:08X}, computed=0x${computed_crc:08X}')
	}
}

// decode_snapshot_header decodes header fields after the magic bytes.
// Returns: (version, broker_id, timestamp, entry_count, next read position)
fn decode_snapshot_header(data []u8) !(i32, i32, i64, i32, int) {
	mut pos := 2

	version := read_i32_be(data, pos)
	pos += 4

	broker_id := read_i32_be(data, pos)
	pos += 4

	timestamp := read_i64_be(data, pos)
	pos += 8

	entry_count := read_i32_be(data, pos)
	pos += 4

	if entry_count < 0 {
		return error('invalid entry count: ${entry_count}')
	}

	return version, broker_id, timestamp, entry_count, pos
}

// decode_snapshot_entry decodes a single offset entry at the specified position.
// Returns: (composite_key, entry, next read position)
fn decode_snapshot_entry(data []u8, start_pos int, payload_len int) !(string, OffsetEntry, int) {
	mut pos := start_pos

	// topic_len (2 bytes) + topic (N bytes)
	if pos + 2 > payload_len {
		return error('unexpected end of data while reading topic length at pos ${pos}')
	}
	topic_len := int(read_i16_be(data, pos))
	pos += 2
	if topic_len < 0 || pos + topic_len > payload_len {
		return error('invalid topic length: ${topic_len} at pos ${pos}')
	}
	topic_name := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// partition (4 bytes)
	if pos + 4 > payload_len {
		return error('unexpected end of data while reading partition at pos ${pos}')
	}
	partition := read_i32_be(data, pos)
	pos += 4

	// offset (8 bytes)
	if pos + 8 > payload_len {
		return error('unexpected end of data while reading offset at pos ${pos}')
	}
	offset := read_i64_be(data, pos)
	pos += 8

	// leader_epoch (4 bytes)
	if pos + 4 > payload_len {
		return error('unexpected end of data while reading leader_epoch at pos ${pos}')
	}
	leader_epoch := read_i32_be(data, pos)
	pos += 4

	// metadata_len (2 bytes) + metadata (N bytes)
	if pos + 2 > payload_len {
		return error('unexpected end of data while reading metadata length at pos ${pos}')
	}
	metadata_len := int(read_i16_be(data, pos))
	pos += 2
	if metadata_len < 0 || pos + metadata_len > payload_len {
		return error('invalid metadata length: ${metadata_len} at pos ${pos}')
	}
	metadata := data[pos..pos + metadata_len].bytestr()
	pos += metadata_len

	// committed_at (8 bytes)
	if pos + 8 > payload_len {
		return error('unexpected end of data while reading committed_at at pos ${pos}')
	}
	committed_at := read_i64_be(data, pos)
	pos += 8

	composite_key := '${topic_name}:${partition}'
	entry := OffsetEntry{
		offset:       offset
		leader_epoch: leader_epoch
		metadata:     metadata
		committed_at: committed_at
	}
	return composite_key, entry, pos
}

// decode_offset_snapshot decodes binary data into an OffsetSnapshot.
// Validates magic bytes and CRC32 checksum.
fn decode_offset_snapshot(data []u8) !OffsetSnapshot {
	if data.len < offset_snapshot_min_size {
		return error('offset snapshot data too short: ${data.len} bytes')
	}

	// Verify magic bytes
	magic := read_u16_be(data, 0)
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
	magic := read_u16_be(data, 0)
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
