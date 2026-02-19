// Infra Layer - S3 오프셋 스냅샷 바이너리 코덱
// 컨슈머 그룹 오프셋을 바이너리 스냅샷으로 인코딩/디코딩합니다.
//
// 바이너리 포맷 v1:
//   +-------+----------+-----------+-----------+-------------+----------+-------+
//   | magic | version  | broker_id | timestamp | entry_count | entries  | crc32 |
//   | 2B    | 4B       | 4B        | 8B        | 4B          | variable | 4B    |
//   +-------+----------+-----------+-----------+-------------+----------+-------+
//
// 엔트리 포맷:
//   +----------+-------+-----------+--------+--------------+----------+----------+--------------+
//   | topic_len| topic | partition | offset | leader_epoch | meta_len | metadata | committed_at |
//   | 2B       | var   | 4B        | 8B     | 4B           | 2B       | var      | 8B           |
//   +----------+-------+-----------+--------+--------------+----------+----------+--------------+
//
// 모든 정수 필드는 빅엔디안(big-endian) 바이트 순서를 사용합니다.
// CRC32 체크섬은 magic부터 마지막 엔트리까지의 데이터에 대해 IEEE 다항식으로 계산됩니다.
module s3

import json
import time
import domain

// 오프셋 스냅샷 매직 바이트 (DataCore Offset Snapshot v1)
const offset_snapshot_magic = u16(0xDC01)

// 헤더 크기: magic(2) + version(4) + broker_id(4) + timestamp(8) + entry_count(4)
const offset_snapshot_header_size = 22

// 최소 유효 스냅샷 크기: header(22) + crc32(4)
const offset_snapshot_min_size = 26

// 엔트리 고정 필드 크기: topic_len(2) + partition(4) + offset(8) + leader_epoch(4) + metadata_len(2) + committed_at(8)
const offset_entry_fixed_size = 28

// CRC32 체크섬 크기
const offset_snapshot_crc_size = 4

// OffsetEntry는 단일 파티션의 커밋된 오프셋 정보를 나타냅니다.
struct OffsetEntry {
pub mut:
	offset       i64
	leader_epoch i32
	metadata     string
	committed_at i64
}

// OffsetSnapshot은 컨슈머 그룹의 모든 오프셋을 바이너리 스냅샷으로 관리합니다.
struct OffsetSnapshot {
pub mut:
	version   i32                    // 낙관적 잠금용 버전
	broker_id i32                    // 마지막 쓰기 브로커 ID
	timestamp i64                    // 스냅샷 생성 시각 (unix millis)
	offsets   map[string]OffsetEntry // "topic:partition" -> entry
}

// new_offset_snapshot은 지정된 broker_id로 빈 스냅샷을 생성합니다.
fn new_offset_snapshot(broker_id i32) OffsetSnapshot {
	return OffsetSnapshot{
		version:   0
		broker_id: broker_id
		timestamp: time.now().unix_milli()
		offsets:   map[string]OffsetEntry{}
	}
}

// CRC32 IEEE 체크섬 (인라인 구현)

// IEEE 다항식 0xEDB88320 기반 룩업 테이블
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

// crc32_checksum은 IEEE 다항식으로 CRC32 체크섬을 계산합니다.
fn crc32_checksum(data []u8) u32 {
	mut crc := u32(0xffffffff)
	for b in data {
		idx := int((crc ^ u32(b)) & 0xff)
		crc = (crc >> 8) ^ snapshot_crc32_table[idx]
	}
	return crc ^ u32(0xffffffff)
}

// 빅엔디안 쓰기 헬퍼

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

// 빅엔디안 읽기 헬퍼

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

// 인코딩

// calculate_snapshot_size는 스냅샷의 인코딩된 바이너리 크기를 정확히 계산합니다.
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

// encode_offset_snapshot은 OffsetSnapshot을 바이너리 형식으로 인코딩합니다.
fn encode_offset_snapshot(snapshot OffsetSnapshot) []u8 {
	exact_size := calculate_snapshot_size(snapshot)
	mut buf := []u8{cap: exact_size}

	// magic (2바이트)
	buf << u8(offset_snapshot_magic >> 8)
	buf << u8(offset_snapshot_magic)

	// version (4바이트)
	write_i32_be(mut buf, snapshot.version)

	// broker_id (4바이트)
	write_i32_be(mut buf, snapshot.broker_id)

	// timestamp (8바이트)
	write_i64_be(mut buf, snapshot.timestamp)

	// entry_count (4바이트)
	write_i32_be(mut buf, i32(snapshot.offsets.len))

	// 각 엔트리 인코딩
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

		// topic_len (2바이트) + topic (N바이트)
		topic_bytes := topic_name.bytes()
		write_i16_be(mut buf, i16(topic_bytes.len))
		buf << topic_bytes

		// partition (4바이트)
		write_i32_be(mut buf, i32(partition))

		// offset (8바이트)
		write_i64_be(mut buf, entry.offset)

		// leader_epoch (4바이트)
		write_i32_be(mut buf, entry.leader_epoch)

		// metadata_len (2바이트) + metadata (N바이트)
		metadata_bytes := entry.metadata.bytes()
		write_i16_be(mut buf, i16(metadata_bytes.len))
		buf << metadata_bytes

		// committed_at (8바이트)
		write_i64_be(mut buf, entry.committed_at)
	}

	// CRC32 체크섬 (magic부터 마지막 entry까지)
	checksum := crc32_checksum(buf)
	write_u32_be(mut buf, checksum)

	return buf
}

// 디코딩

// verify_snapshot_crc는 스냅샷 데이터의 CRC32 체크섬을 검증합니다.
// 마지막 4바이트가 CRC32이며, 그 앞까지의 데이터로 계산하여 비교합니다.
fn verify_snapshot_crc(data []u8) ! {
	payload := data[0..data.len - offset_snapshot_crc_size]
	stored_crc := read_u32_be(data, data.len - offset_snapshot_crc_size)
	computed_crc := crc32_checksum(payload)
	if stored_crc != computed_crc {
		return error('offset snapshot CRC32 mismatch: stored=0x${stored_crc:08X}, computed=0x${computed_crc:08X}')
	}
}

// decode_snapshot_header는 magic 이후의 헤더 필드를 디코딩합니다.
// 반환: (version, broker_id, timestamp, entry_count, 다음 읽기 위치)
fn decode_snapshot_header(data []u8) !(i32, i32, i64, i32, int) {
	mut pos := 2 // magic 이후

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

// decode_snapshot_entry는 지정된 위치에서 단일 오프셋 엔트리를 디코딩합니다.
// 반환: (composite_key, entry, 다음 읽기 위치)
fn decode_snapshot_entry(data []u8, start_pos int, payload_len int) !(string, OffsetEntry, int) {
	mut pos := start_pos

	// topic_len (2바이트) + topic (N바이트)
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

	// partition (4바이트)
	if pos + 4 > payload_len {
		return error('unexpected end of data while reading partition at pos ${pos}')
	}
	partition := read_i32_be(data, pos)
	pos += 4

	// offset (8바이트)
	if pos + 8 > payload_len {
		return error('unexpected end of data while reading offset at pos ${pos}')
	}
	offset := read_i64_be(data, pos)
	pos += 8

	// leader_epoch (4바이트)
	if pos + 4 > payload_len {
		return error('unexpected end of data while reading leader_epoch at pos ${pos}')
	}
	leader_epoch := read_i32_be(data, pos)
	pos += 4

	// metadata_len (2바이트) + metadata (N바이트)
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

	// committed_at (8바이트)
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

// decode_offset_snapshot은 바이너리 데이터를 OffsetSnapshot으로 디코딩합니다.
// magic 바이트 확인, CRC32 체크섬 검증을 수행합니다.
fn decode_offset_snapshot(data []u8) !OffsetSnapshot {
	if data.len < offset_snapshot_min_size {
		return error('offset snapshot data too short: ${data.len} bytes')
	}

	// magic 바이트 확인
	magic := read_u16_be(data, 0)
	if magic != offset_snapshot_magic {
		return error('invalid offset snapshot magic: 0x${magic:04X}, expected 0xDC01')
	}

	// CRC32 체크섬 검증
	verify_snapshot_crc(data)!

	// 헤더 디코딩
	version, broker_id, timestamp, entry_count, mut pos := decode_snapshot_header(data)!

	// 엔트리 디코딩
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

// 자동 판별 디코딩

// try_decode_offset_data는 데이터의 magic 바이트를 확인하여 바이너리 또는 JSON 형식을 자동 판별합니다.
// 0xDC01이면 바이너리, '{'이면 레거시 JSON으로 디코딩합니다.
fn try_decode_offset_data(data []u8) !OffsetSnapshot {
	if data.len < 2 {
		return error('offset data too short: ${data.len} bytes')
	}

	// 바이너리 포맷 확인 (magic = 0xDC01)
	magic := read_u16_be(data, 0)
	if magic == offset_snapshot_magic {
		return decode_offset_snapshot(data)
	}

	// 레거시 JSON 포맷 확인
	if data[0] == u8(`{`) {
		return decode_legacy_json_snapshot(data)
	}

	return error('unknown offset data format: first bytes=0x${data[0]:02X}${data[1]:02X}')
}

// 레거시 JSON 호환

// decode_legacy_json_snapshot은 기존 개별 JSON 형식(domain.PartitionOffset)을
// OffsetSnapshot으로 변환합니다.
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

// 스냅샷 병합

// merge_offset_snapshots은 로컬과 리모트 스냅샷을 병합합니다.
// 각 "topic:partition"에 대해 더 큰 오프셋을 선택하고,
// version은 양쪽 최대값 + 1로 설정합니다.
fn merge_offset_snapshots(local OffsetSnapshot, remote OffsetSnapshot) OffsetSnapshot {
	mut merged := map[string]OffsetEntry{}

	// 로컬 엔트리 복사
	for key, entry in local.offsets {
		merged[key] = entry
	}

	// 리모트 엔트리 병합 (더 큰 오프셋 선택)
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
