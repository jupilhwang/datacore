// Entity Layer - Record Domain Model
module domain

import time

// Record represents a single message in Kafka
pub struct Record {
pub:
    key         []u8
    value       []u8
    headers     map[string][]u8
    timestamp   time.Time
}

// RecordBatch represents a batch of records
pub struct RecordBatch {
pub:
    base_offset         i64
    partition_leader_epoch i32
    magic               i8 = 2  // v2 format
    crc                 u32
    attributes          i16
    last_offset_delta   i32
    first_timestamp     i64
    max_timestamp       i64
    producer_id         i64 = -1
    producer_epoch      i16 = -1
    base_sequence       i32 = -1
    records             []Record
}

// AppendResult represents the result of appending records
pub struct AppendResult {
pub:
    base_offset         i64
    log_append_time     i64
    log_start_offset    i64
    record_count        int
}

// FetchResult represents the result of fetching records
pub struct FetchResult {
pub:
    records             []Record
    high_watermark      i64
    last_stable_offset  i64
    log_start_offset    i64
}
