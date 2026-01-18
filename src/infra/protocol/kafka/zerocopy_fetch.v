// Zero-Copy Fetch Handler Integration
// High-performance fetch response with minimal data copying
module kafka

import infra.performance
import domain

// ============================================================================
// Zero-Copy Fetch Request Parser
// ============================================================================

// ZeroCopyFetchRequest holds parsed fetch request with zero-copy body references
// NOTE: DataCore Stateless Architecture
// - session_id/session_epoch: parsed for compatibility but ignored
// - forgotten_topics: parsed for compatibility but ignored (no session state)
// - All fetch requests are treated as independent (session_id=0 in response)
pub struct ZeroCopyFetchRequest {
pub:
    replica_id       i32
    max_wait_ms      i32
    min_bytes        i32
    max_bytes        i32
    isolation_level  i8
    session_id       i32   // Stateless: ignored, always respond with 0
    session_epoch    i32   // Stateless: ignored
    topics           []ZeroCopyFetchTopic
    forgotten_topics []ForgottenTopic  // Stateless: ignored
    rack_id          string
}

// ZeroCopyFetchTopic holds topic data from fetch request
pub struct ZeroCopyFetchTopic {
pub:
    topic_id   []u8
    name       string
    partitions []ZeroCopyFetchPartition
}

// ZeroCopyFetchPartition holds partition data from fetch request
pub struct ZeroCopyFetchPartition {
pub:
    partition           i32
    current_leader_epoch i32
    fetch_offset        i64
    last_fetched_epoch  i32
    log_start_offset    i64
    partition_max_bytes i32
}

// ForgottenTopic for session tracking
pub struct ForgottenTopic {
pub:
    topic_id    []u8
    name        string
    partitions  []i32
}

// parse_fetch_request_zerocopy parses fetch request with zero-copy
pub fn parse_fetch_request_zerocopy(data []u8, version i16, flexible bool) !ZeroCopyFetchRequest {
    first_bytes := if data.len >= 40 { data[..40].hex() } else { data.hex() }
    eprintln('[DEBUG] parse_fetch_request_zerocopy: version=${version} flexible=${flexible} data.len=${data.len}')
    eprintln('[DEBUG] first 40 bytes: ${first_bytes}')
    
    mut reader := new_zerocopy_reader(data)
    
    // In v15+, replica_id was removed from main body and replaced with replica_state tagged field
    // For v0-14: replica_id (INT32) is first field
    // For v15+: replica_id is NOT in the body (it's in tagged fields as replica_state)
    mut replica_id := i32(-1)  // Default for consumers
    if version < 15 {
        replica_id = reader.read_i32()!
    }
    
    max_wait_ms := reader.read_i32()!
    min_bytes := reader.read_i32()!
    
    // v3+ has max_bytes
    max_bytes := if version >= 3 { reader.read_i32()! } else { i32(0x7FFFFFFF) }
    
    // v4+ has isolation level
    isolation_level := if version >= 4 { reader.read_i8()! } else { i8(0) }
    
    // v7+ has session_id and session_epoch
    session_id := if version >= 7 { reader.read_i32()! } else { i32(0) }
    session_epoch := if version >= 7 { reader.read_i32()! } else { i32(-1) }
    
    eprintln('[DEBUG] Fetch v${version}: parsed fields - replica_id=${replica_id}, max_wait_ms=${max_wait_ms}, min_bytes=${min_bytes}, max_bytes=${max_bytes}')
    eprintln('[DEBUG] Fetch: isolation_level=${isolation_level}, session_id=${session_id}, session_epoch=${session_epoch}')
    eprintln('[DEBUG] Fetch: after header, pos=${reader.position()}, remaining=${reader.remaining()}')
    
    // Parse topics
    mut topics := []ZeroCopyFetchTopic{}
    eprintln('[DEBUG] Fetch: before reading topic_count, pos=${reader.position()}, remaining=${reader.remaining()}')
    topic_count := if flexible { reader.read_compact_array_length()! } else { reader.read_array_length()! }
    
    eprintln('[DEBUG] Fetch: topic_count=${topic_count}, pos=${reader.position()}')
    
    for ti in 0 .. topic_count {
        // v13+ uses topic_id
        mut topic_id := []u8{}
        mut topic_name := ''
        
        if version >= 13 {
            topic_id = reader.read_uuid()!
            eprintln('[DEBUG] Fetch: topic ${ti} - read topic_id=${topic_id.hex()}')
        }
        if version < 13 || !flexible {
            topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
            eprintln('[DEBUG] Fetch: topic ${ti} - read topic_name=${topic_name}')
        }
        
        // Parse partitions
        mut partitions := []ZeroCopyFetchPartition{}
        partition_count := if flexible { reader.read_compact_array_length()! } else { reader.read_array_length()! }
        
        for _ in 0 .. partition_count {
            partition := reader.read_i32()!
            
            // v9+ has current_leader_epoch
            current_leader_epoch := if version >= 9 { reader.read_i32()! } else { i32(-1) }
            
            fetch_offset := reader.read_i64()!
            
            // v12+ has last_fetched_epoch
            last_fetched_epoch := if version >= 12 { reader.read_i32()! } else { i32(-1) }
            
            // v5+ has log_start_offset
            log_start_offset := if version >= 5 { reader.read_i64()! } else { i64(-1) }
            
            partition_max_bytes := reader.read_i32()!
            
            if flexible {
                reader.skip_tagged_fields()!
            }
            
            partitions << ZeroCopyFetchPartition{
                partition: partition
                current_leader_epoch: current_leader_epoch
                fetch_offset: fetch_offset
                last_fetched_epoch: last_fetched_epoch
                log_start_offset: log_start_offset
                partition_max_bytes: partition_max_bytes
            }
        }
        
        if flexible {
            reader.skip_tagged_fields()!
        }
        
        topics << ZeroCopyFetchTopic{
            topic_id: topic_id
            name: topic_name
            partitions: partitions
        }
    }
    
    // v7+ has forgotten topics
    mut forgotten_topics := []ForgottenTopic{}
    if version >= 7 {
        forgotten_count := if flexible { reader.read_compact_array_length()! } else { reader.read_array_length()! }
        
        for _ in 0 .. forgotten_count {
            mut f_topic_id := []u8{}
            mut f_name := ''
            
            if version >= 13 {
                f_topic_id = reader.read_uuid()!
            }
            if version < 13 || !flexible {
                f_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
            }
            
            mut f_partitions := []i32{}
            f_partition_count := if flexible { reader.read_compact_array_length()! } else { reader.read_array_length()! }
            
            for _ in 0 .. f_partition_count {
                f_partitions << reader.read_i32()!
            }
            
            if flexible {
                reader.skip_tagged_fields()!
            }
            
            forgotten_topics << ForgottenTopic{
                topic_id: f_topic_id
                name: f_name
                partitions: f_partitions
            }
        }
    }
    
    // v11+ has rack_id
    rack_id := if version >= 11 {
        if flexible { reader.read_compact_string()! } else { reader.read_string()! }
    } else {
        ''
    }
    
    if flexible {
        reader.skip_tagged_fields()!
    }
    
    return ZeroCopyFetchRequest{
        replica_id: replica_id
        max_wait_ms: max_wait_ms
        min_bytes: min_bytes
        max_bytes: max_bytes
        isolation_level: isolation_level
        session_id: session_id
        session_epoch: session_epoch
        topics: topics
        forgotten_topics: forgotten_topics
        rack_id: rack_id
    }
}

// ============================================================================
// Zero-Copy Fetch Response Building
// ============================================================================

// ZeroCopyResponseBuilder builds fetch response with minimal copying
pub struct ZeroCopyResponseBuilder {
mut:
    parts []performance.ByteSlice  // Zero-copy parts
    owned [][]u8                    // Owned data (headers, lengths)
}

// new_zerocopy_response_builder creates a new builder
pub fn new_zerocopy_response_builder() &ZeroCopyResponseBuilder {
    return &ZeroCopyResponseBuilder{
        parts: []performance.ByteSlice{}
        owned: [][]u8{}
    }
}

// add_owned adds owned bytes (copied)
pub fn (mut b ZeroCopyResponseBuilder) add_owned(data []u8) {
    b.owned << data.clone()
    b.parts << performance.ByteSlice.new(b.owned[b.owned.len - 1])
}

// add_slice adds zero-copy slice reference
pub fn (mut b ZeroCopyResponseBuilder) add_slice(slice performance.ByteSlice) {
    b.parts << slice
}

// total_size returns total size of all parts
pub fn (b &ZeroCopyResponseBuilder) total_size() int {
    mut size := 0
    for part in b.parts {
        size += part.len()
    }
    return size
}

// build assembles all parts into final response
pub fn (b &ZeroCopyResponseBuilder) build() []u8 {
    total := b.total_size()
    mut result := []u8{cap: total}
    
    for part in b.parts {
        result << part.to_owned()
    }
    
    return result
}

// ============================================================================
// RecordBatch Zero-Copy Encoding
// ============================================================================

// encode_record_batch_zerocopy encodes records using zero-copy where possible
pub fn encode_record_batch_zerocopy(records []domain.Record, base_offset i64) []u8 {
    if records.len == 0 {
        return []u8{}
    }
    
    // Use optimized encoding path
    // For now, delegate to existing encoder but with pre-allocated buffer
    estimated_size := estimate_batch_size(records)
    mut result := []u8{cap: estimated_size}
    
    // RecordBatch header
    mut writer := new_writer()
    
    // Base offset (8 bytes)
    writer.write_i64(base_offset)
    
    // Placeholder for batch length (4 bytes) - will be filled later
    length_pos := writer.data.len
    writer.write_i32(0)
    
    // Partition leader epoch (4 bytes)
    writer.write_i32(-1)
    
    // Magic byte (1 byte) - version 2
    writer.write_i8(2)
    
    // CRC placeholder (4 bytes)
    crc_pos := writer.data.len
    writer.write_i32(0)
    
    // Attributes (2 bytes) - no compression, no timestamps
    writer.write_i16(0)
    
    // Last offset delta
    writer.write_i32(i32(records.len - 1))
    
    // First timestamp (use first record)
    first_timestamp := if records.len > 0 { records[0].timestamp.unix_milli() } else { i64(0) }
    writer.write_i64(first_timestamp)
    
    // Max timestamp (use last record)
    max_timestamp := if records.len > 0 { records[records.len - 1].timestamp.unix_milli() } else { first_timestamp }
    writer.write_i64(max_timestamp)
    
    // Producer ID (-1 for non-idempotent)
    writer.write_i64(-1)
    
    // Producer epoch (-1 for non-idempotent)
    writer.write_i16(-1)
    
    // Base sequence (-1 for non-idempotent)
    writer.write_i32(-1)
    
    // Record count
    writer.write_i32(i32(records.len))
    
    // Encode records
    for i, record in records {
        offset_delta := i32(i)
        timestamp_delta := record.timestamp.unix_milli() - first_timestamp
        
        // Calculate record size first
        mut record_writer := new_writer()
        record_writer.write_varint(i64(0)) // length placeholder
        record_writer.write_i8(0) // attributes
        record_writer.write_varint(timestamp_delta)
        record_writer.write_varint(i64(offset_delta))
        
        // Key
        if record.key.len > 0 {
            record_writer.write_varint(i64(record.key.len))
            for b in record.key {
                record_writer.write_i8(i8(b))
            }
        } else {
            record_writer.write_varint(-1)
        }
        
        // Value
        if record.value.len > 0 {
            record_writer.write_varint(i64(record.value.len))
            for b in record.value {
                record_writer.write_i8(i8(b))
            }
        } else {
            record_writer.write_varint(-1)
        }
        
        // Headers (none)
        record_writer.write_varint(0)
        
        // Calculate actual record size (without length field)
        record_data := record_writer.bytes()
        record_size := record_data.len - 1 // -1 for the placeholder
        
        // Write actual record with correct length
        writer.write_varint(i64(record_size))
        writer.write_i8(0) // attributes
        writer.write_varint(timestamp_delta)
        writer.write_varint(i64(offset_delta))
        
        // Key
        if record.key.len > 0 {
            writer.write_varint(i64(record.key.len))
            for b in record.key {
                writer.write_i8(i8(b))
            }
        } else {
            writer.write_varint(-1)
        }
        
        // Value
        if record.value.len > 0 {
            writer.write_varint(i64(record.value.len))
            for b in record.value {
                writer.write_i8(i8(b))
            }
        } else {
            writer.write_varint(-1)
        }
        
        // Headers (none)
        writer.write_varint(0)
    }
    
    // Get final data
    mut batch_data := writer.bytes()
    
    // Calculate and fill batch length (total - base_offset - batch_length_field)
    batch_length := batch_data.len - 12
    batch_data[8] = u8(batch_length >> 24)
    batch_data[9] = u8(batch_length >> 16)
    batch_data[10] = u8(batch_length >> 8)
    batch_data[11] = u8(batch_length)
    
    // Calculate CRC32c for the batch (from attributes to end)
    crc := calculate_crc32c(batch_data[crc_pos + 4..])
    batch_data[crc_pos] = u8(crc >> 24)
    batch_data[crc_pos + 1] = u8(crc >> 16)
    batch_data[crc_pos + 2] = u8(crc >> 8)
    batch_data[crc_pos + 3] = u8(crc)
    
    return batch_data
}

// estimate_batch_size estimates the size of encoded record batch
fn estimate_batch_size(records []domain.Record) int {
    // Base batch overhead: 61 bytes (header)
    mut size := 61
    
    for record in records {
        // Per-record overhead: ~20 bytes (varints, attributes)
        size += 20
        size += record.key.len
        size += record.value.len
    }
    
    return size
}

// calculate_crc32c calculates CRC32-C checksum
fn calculate_crc32c(data []u8) u32 {
    // CRC32-C polynomial table (Castagnoli)
    crc_table := [
        u32(0x00000000), 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
        0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
        0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
        0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
        0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
        0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
        0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
        0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
        0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
        0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
        0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
        0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
        0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
        0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
        0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
        0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
        0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
        0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
        0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
        0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
        0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
        0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
        0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
        0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
        0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
        0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
        0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
        0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
        0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
        0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
        0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
        0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
    ]
    
    mut crc := u32(0xFFFFFFFF)
    for b in data {
        index := (crc ^ u32(b)) & 0xFF
        crc = (crc >> 8) ^ crc_table[index]
    }
    return crc ^ 0xFFFFFFFF
}
