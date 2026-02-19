module domain

import time

/// Record represents a single Kafka message.
/// key: message key (used for partitioning)
/// value: message body
/// headers: message headers (metadata)
/// timestamp: message timestamp
pub struct Record {
pub:
	key       []u8
	value     []u8
	headers   map[string][]u8
	timestamp time.Time
	// original compression type (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
	// used to preserve original compression info during cross-broker fetch
	compression_type u8
	// transaction control record metadata
	// if is_control_record is true, this record is a transaction marker (commit/abort)
	is_control_record bool
	// for control records: true = COMMIT, false = ABORT
	control_type ControlRecordType = .none
	// Producer ID for transactional records
	producer_id i64 = -1
	// Producer Epoch for transactional records
	producer_epoch i16 = -1
}

/// ControlRecordType represents the type of a control record.
/// none: not a control record
/// abort: transaction rollback marker
/// commit: transaction commit marker
pub enum ControlRecordType {
	none   = 0
	abort  = 1
	commit = 2
}

/// RecordBatch represents a batch of records.
/// Note: DataCore Stateless architecture
/// - partition_leader_epoch: always 0 (no leader election)
/// - producer_id/producer_epoch: used for idempotency, handled at storage level
pub struct RecordBatch {
pub:
	base_offset            i64
	partition_leader_epoch i32
	magic                  i8 = 2
	crc                    u32
	attributes             i16
	last_offset_delta      i32
	first_timestamp        i64
	max_timestamp          i64
	producer_id            i64 = -1
	producer_epoch         i16 = -1
	base_sequence          i32 = -1
	records                []Record
}

/// AppendResult represents the result of appending records.
/// base_offset: offset of the first record
/// log_append_time: log append time
/// log_start_offset: log start offset
/// record_count: number of records appended
pub struct AppendResult {
pub:
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
	record_count     int
}

/// FetchResult represents the result of fetching records.
/// records: list of fetched records
/// first_offset: actual offset of the first returned record (v0.41.1)
/// high_watermark: high watermark offset
/// last_stable_offset: last stable offset (for transactions)
/// log_start_offset: log start offset
pub struct FetchResult {
pub:
	records            []Record
	first_offset       i64
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
}
