// Infra Layer - Memory Storage Adapter
// In-memory storage with partition-level locking for better concurrency
module memory

import domain
import service.port
import sync
import time
import rand

// MemoryStorageAdapter implements port.StoragePort
pub struct MemoryStorageAdapter {
pub mut:
    config      MemoryConfig
mut:
    topics      map[string]&TopicStore
    groups      map[string]domain.ConsumerGroup
    offsets     map[string]map[string]i64
    global_lock sync.RwMutex
}

// MemoryConfig holds memory storage configuration
pub struct MemoryConfig {
pub:
    max_messages_per_partition  int = 1000000    // Default 1M messages
    max_bytes_per_partition     i64 = -1         // -1 = unlimited
    retention_ms                i64 = 604800000  // 7 days default
}

// TopicStore stores topic data
struct TopicStore {
pub mut:
    metadata    domain.TopicMetadata
    config      domain.TopicConfig
    partitions  []&PartitionStore
    lock        sync.RwMutex
}

// PartitionStore stores partition data with fine-grained locking
struct PartitionStore {
mut:
    records         []domain.Record
    base_offset     i64
    high_watermark  i64
    lock            sync.RwMutex
}

// new_memory_adapter creates a new memory storage adapter
pub fn new_memory_adapter() &MemoryStorageAdapter {
    return new_memory_adapter_with_config(MemoryConfig{})
}

// new_memory_adapter_with_config creates adapter with custom config
pub fn new_memory_adapter_with_config(config MemoryConfig) &MemoryStorageAdapter {
    return &MemoryStorageAdapter{
        config: config
        topics: map[string]&TopicStore{}
        groups: map[string]domain.ConsumerGroup{}
        offsets: map[string]map[string]i64{}
    }
}

// ============================================================
// Topic Operations
// ============================================================

pub fn (mut a MemoryStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    if name in a.topics {
        return error('topic already exists')
    }
    
    // Generate UUID for topic_id
    mut topic_id := []u8{len: 16}
    for i in 0 .. 16 {
        topic_id[i] = u8(rand.intn(256) or { 0 })
    }
    // Set UUID version 4 (random)
    topic_id[6] = (topic_id[6] & 0x0f) | 0x40
    topic_id[8] = (topic_id[8] & 0x3f) | 0x80
    
    // Create partition stores
    mut partition_stores := []&PartitionStore{cap: partitions}
    for _ in 0 .. partitions {
        partition_stores << &PartitionStore{
            records: []domain.Record{}
            base_offset: 0
            high_watermark: 0
        }
    }
    
    metadata := domain.TopicMetadata{
        name: name
        topic_id: topic_id
        partition_count: partitions
        config: map[string]string{}
        is_internal: name.starts_with('__')
    }
    
    a.topics[name] = &TopicStore{
        metadata: metadata
        config: config
        partitions: partition_stores
    }
    
    return metadata
}

pub fn (mut a MemoryStorageAdapter) delete_topic(name string) ! {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    if name !in a.topics {
        return error('topic not found')
    }
    a.topics.delete(name)
}

pub fn (mut a MemoryStorageAdapter) list_topics() ![]domain.TopicMetadata {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    mut result := []domain.TopicMetadata{}
    for _, topic in a.topics {
        result << topic.metadata
    }
    return result
}

pub fn (mut a MemoryStorageAdapter) get_topic(name string) !domain.TopicMetadata {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    if topic := a.topics[name] {
        return topic.metadata
    }
    return error('topic not found')
}

pub fn (mut a MemoryStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    for _, topic in a.topics {
        if topic.metadata.topic_id == topic_id {
            return topic.metadata
        }
    }
    return error('topic not found')
}

pub fn (mut a MemoryStorageAdapter) add_partitions(name string, new_count int) ! {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    mut topic := a.topics[name] or { return error('topic not found') }
    
    current := topic.partitions.len
    if new_count <= current {
        return error('new partition count must be greater than current')
    }
    
    topic.lock.@lock()
    defer { topic.lock.unlock() }
    
    for _ in current .. new_count {
        topic.partitions << &PartitionStore{
            records: []domain.Record{}
            base_offset: 0
            high_watermark: 0
        }
    }
    
    topic.metadata = domain.TopicMetadata{
        ...topic.metadata
        partition_count: new_count
    }
}

// ============================================================
// Record Operations (Partition-level locking)
// ============================================================

pub fn (mut a MemoryStorageAdapter) append(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
    // Get topic with read lock
    a.global_lock.rlock()
    topic := a.topics[topic_name] or {
        a.global_lock.runlock()
        return error('topic not found')
    }
    a.global_lock.runlock()
    
    if partition < 0 || partition >= topic.partitions.len {
        return error('partition out of range')
    }
    
    // Lock only the specific partition (write)
    mut part := topic.partitions[partition]
    part.lock.@lock()
    defer { part.lock.unlock() }
    
    base_offset := part.high_watermark
    now := time.now()
    
    // Add records with timestamp
    for record in records {
        mut r := record
        if r.timestamp.unix() == 0 {
            r = domain.Record{
                ...r
                timestamp: now
            }
        }
        part.records << r
    }
    part.high_watermark += i64(records.len)
    
    // Apply retention policy (max messages)
    if a.config.max_messages_per_partition > 0 {
        excess := part.records.len - a.config.max_messages_per_partition
        if excess > 0 {
            part.records = part.records[excess..].clone()
            part.base_offset += i64(excess)
        }
    }
    
    return domain.AppendResult{
        base_offset: base_offset
        log_append_time: now.unix()
        log_start_offset: part.base_offset
        record_count: records.len
    }
}

pub fn (mut a MemoryStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
    // Get topic with read lock
    a.global_lock.rlock()
    topic := a.topics[topic_name] or {
        a.global_lock.runlock()
        return error('topic not found')
    }
    a.global_lock.runlock()
    
    if partition < 0 || partition >= topic.partitions.len {
        return error('partition out of range')
    }
    
    // Lock partition for read
    mut part := topic.partitions[partition]
    part.lock.rlock()
    defer { part.lock.runlock() }
    
    // Empty result if offset out of range
    if offset < part.base_offset {
        return domain.FetchResult{
            records: []
            high_watermark: part.high_watermark
            last_stable_offset: part.high_watermark
            log_start_offset: part.base_offset
        }
    }
    
    start_idx := int(offset - part.base_offset)
    if start_idx >= part.records.len {
        return domain.FetchResult{
            records: []
            high_watermark: part.high_watermark
            last_stable_offset: part.high_watermark
            log_start_offset: part.base_offset
        }
    }
    
    // Calculate end index based on max_bytes
    mut end_idx := start_idx
    mut total_bytes := 0
    max_fetch_bytes := if max_bytes <= 0 { 1048576 } else { max_bytes }
    
    for end_idx < part.records.len {
        record_size := part.records[end_idx].key.len + part.records[end_idx].value.len + 50
        if total_bytes + record_size > max_fetch_bytes && end_idx > start_idx {
            break
        }
        total_bytes += record_size
        end_idx++
        
        if end_idx - start_idx >= 1000 {
            break
        }
    }
    
    return domain.FetchResult{
        records: part.records[start_idx..end_idx].clone()
        high_watermark: part.high_watermark
        last_stable_offset: part.high_watermark
        log_start_offset: part.base_offset
    }
}

pub fn (mut a MemoryStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
    a.global_lock.rlock()
    topic := a.topics[topic_name] or {
        a.global_lock.runlock()
        return error('topic not found')
    }
    a.global_lock.runlock()
    
    if partition < 0 || partition >= topic.partitions.len {
        return error('partition out of range')
    }
    
    mut part := topic.partitions[partition]
    part.lock.@lock()
    defer { part.lock.unlock() }
    
    delete_count := int(before_offset - part.base_offset)
    if delete_count > 0 && delete_count <= part.records.len {
        part.records = part.records[delete_count..].clone()
        part.base_offset = before_offset
    }
}

// ============================================================
// Offset Operations
// ============================================================

pub fn (mut a MemoryStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
    a.global_lock.rlock()
    topic := a.topics[topic_name] or {
        a.global_lock.runlock()
        return error('topic not found')
    }
    a.global_lock.runlock()
    
    if partition < 0 || partition >= topic.partitions.len {
        return error('partition out of range')
    }
    
    mut part := topic.partitions[partition]
    part.lock.rlock()
    defer { part.lock.runlock() }
    
    return domain.PartitionInfo{
        topic: topic_name
        partition: partition
        earliest_offset: part.base_offset
        latest_offset: part.high_watermark
        high_watermark: part.high_watermark
    }
}

// ============================================================
// Consumer Group Operations
// ============================================================

pub fn (mut a MemoryStorageAdapter) save_group(group domain.ConsumerGroup) ! {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    a.groups[group.group_id] = group
}

pub fn (mut a MemoryStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    if group := a.groups[group_id] {
        return group
    }
    return error('group not found')
}

pub fn (mut a MemoryStorageAdapter) delete_group(group_id string) ! {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    if group_id !in a.groups {
        return error('group not found')
    }
    a.groups.delete(group_id)
    a.offsets.delete(group_id)
}

pub fn (mut a MemoryStorageAdapter) list_groups() ![]domain.GroupInfo {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    mut result := []domain.GroupInfo{}
    for _, group in a.groups {
        result << domain.GroupInfo{
            group_id: group.group_id
            protocol_type: group.protocol_type
            state: match group.state {
                .empty { 'Empty' }
                .preparing_rebalance { 'PreparingRebalance' }
                .completing_rebalance { 'CompletingRebalance' }
                .stable { 'Stable' }
                .dead { 'Dead' }
            }
        }
    }
    return result
}

// ============================================================
// Offset Commit/Fetch
// ============================================================

pub fn (mut a MemoryStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    if group_id !in a.offsets {
        a.offsets[group_id] = map[string]i64{}
    }
    
    for offset in offsets {
        key := '${offset.topic}:${offset.partition}'
        a.offsets[group_id][key] = offset.offset
    }
}

pub fn (mut a MemoryStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    mut results := []domain.OffsetFetchResult{}
    
    if group_id !in a.offsets {
        for part in partitions {
            results << domain.OffsetFetchResult{
                topic: part.topic
                partition: part.partition
                offset: -1
                metadata: ''
                error_code: 0
            }
        }
        return results
    }
    
    for part in partitions {
        key := '${part.topic}:${part.partition}'
        offset := a.offsets[group_id][key] or { -1 }
        results << domain.OffsetFetchResult{
            topic: part.topic
            partition: part.partition
            offset: offset
            metadata: ''
            error_code: 0
        }
    }
    
    return results
}

// ============================================================
// Health Check
// ============================================================

pub fn (mut a MemoryStorageAdapter) health_check() !port.HealthStatus {
    return .healthy
}

// ============================================================
// Stats and Utilities
// ============================================================

// StorageStats provides storage statistics
pub struct StorageStats {
pub:
    topic_count       int
    total_partitions  int
    total_records     i64
    total_bytes       i64
    group_count       int
}

// get_stats returns current storage statistics
pub fn (mut a MemoryStorageAdapter) get_stats() StorageStats {
    a.global_lock.rlock()
    defer { a.global_lock.runlock() }
    
    mut total_partitions := 0
    mut total_records := i64(0)
    mut total_bytes := i64(0)
    
    for _, topic in a.topics {
        total_partitions += topic.partitions.len
        
        for i in 0 .. topic.partitions.len {
            part := topic.partitions[i]
            total_records += part.high_watermark - part.base_offset
            // Note: total_bytes is approximate without iterating records
        }
    }
    
    return StorageStats{
        topic_count: a.topics.len
        total_partitions: total_partitions
        total_records: total_records
        total_bytes: total_bytes
        group_count: a.groups.len
    }
}

// clear removes all data (for testing)
pub fn (mut a MemoryStorageAdapter) clear() {
    a.global_lock.@lock()
    defer { a.global_lock.unlock() }
    
    a.topics.clear()
    a.groups.clear()
    a.offsets.clear()
}

// ============================================================
// SharedAdapter - Thread-safe wrapper for concurrent tests
// V language requires 'shared' keyword for cross-thread access
// ============================================================

// SharedAdapter wraps MemoryStorageAdapter for concurrent access in V
// Usage: shared adapter := SharedAdapter{ inner: new_memory_adapter() }
pub struct SharedAdapter {
pub mut:
    inner &MemoryStorageAdapter
}

// new_shared_adapter creates a new SharedAdapter
pub fn new_shared_adapter() SharedAdapter {
    return SharedAdapter{
        inner: new_memory_adapter()
    }
}

// new_shared_adapter_with_config creates SharedAdapter with custom config
pub fn new_shared_adapter_with_config(config MemoryConfig) SharedAdapter {
    return SharedAdapter{
        inner: new_memory_adapter_with_config(config)
    }
}

// Thread-safe create_topic with shared receiver
pub fn (shared a SharedAdapter) create_topic_safe(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
    lock a {
        return a.inner.create_topic(name, partitions, config)
    }
}

// Thread-safe append with shared receiver
pub fn (shared a SharedAdapter) append_safe(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
    lock a {
        return a.inner.append(topic_name, partition, records)
    }
}

// Thread-safe fetch with shared receiver
pub fn (shared a SharedAdapter) fetch_safe(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
    lock a {
        return a.inner.fetch(topic_name, partition, offset, max_bytes)
    }
}

// Thread-safe get_partition_info with shared receiver
pub fn (shared a SharedAdapter) get_partition_info_safe(topic_name string, partition int) !domain.PartitionInfo {
    lock a {
        return a.inner.get_partition_info(topic_name, partition)
    }
}

// Thread-safe commit_offsets with shared receiver
pub fn (shared a SharedAdapter) commit_offsets_safe(group_id string, offsets []domain.PartitionOffset) ! {
    lock a {
        return a.inner.commit_offsets(group_id, offsets)
    }
}

// Thread-safe fetch_offsets with shared receiver
pub fn (shared a SharedAdapter) fetch_offsets_safe(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
    lock a {
        return a.inner.fetch_offsets(group_id, partitions)
    }
}

// Thread-safe get_stats with shared receiver
pub fn (shared a SharedAdapter) get_stats_safe() StorageStats {
    lock a {
        return a.inner.get_stats()
    }
}
