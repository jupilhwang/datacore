// UseCase Layer - Storage Port Interface
// This interface is defined in UseCase layer and implemented by Adapter layer
module port

import domain

// StoragePort defines the interface for storage operations
// Implemented by infra/storage
pub interface StoragePort {
mut:
    // Topic operations
    create_topic(name string, partitions int, config domain.TopicConfig) !
    delete_topic(name string) !
    list_topics() ![]domain.TopicMetadata
    get_topic(name string) !domain.TopicMetadata
    add_partitions(name string, new_count int) !
    
    // Record operations
    append(topic string, partition int, records []domain.Record) !domain.AppendResult
    fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult
    delete_records(topic string, partition int, before_offset i64) !
    
    // Offset operations
    get_partition_info(topic string, partition int) !domain.PartitionInfo
    
    // Consumer Group operations
    save_group(group domain.ConsumerGroup) !
    load_group(group_id string) !domain.ConsumerGroup
    delete_group(group_id string) !
    list_groups() ![]domain.GroupInfo
    
    // Offset commit/fetch
    commit_offsets(group_id string, offsets []domain.PartitionOffset) !
    fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult
    
    // Health check
    health_check() !HealthStatus
}

// HealthStatus represents the health status of storage
pub enum HealthStatus {
    healthy
    degraded
    unhealthy
}

// Lock interface for partition-level locking
pub interface Lock {
    release() !
}

// LockableStorage extends StoragePort with locking capability
pub interface LockableStorage {
    StoragePort
    acquire_lock(topic string, partition int) !Lock
}
