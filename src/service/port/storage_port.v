// Storage operation interfaces defined in the use-case layer and implemented in the adapter layer.
// These interfaces follow the Dependency Inversion Principle of Clean Architecture.
module port

import domain

/// StoragePort defines the interface for storage operations.
/// Implemented in infra/storage; provides topic/partition/record management and consumer group functionality.
pub interface StoragePort {
mut:
	/// Creates a new topic.
	/// name: topic name
	/// partitions: number of partitions
	/// config: topic configuration (retention period, segment size, etc.)
	create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata

	/// Deletes a topic.
	delete_topic(name string) !

	/// Returns a list of all topics.
	list_topics() ![]domain.TopicMetadata

	/// Retrieves topic metadata by topic name.
	get_topic(name string) !domain.TopicMetadata

	/// Retrieves topic metadata by topic ID.
	get_topic_by_id(topic_id []u8) !domain.TopicMetadata

	/// Adds partitions to a topic.
	/// new_count: new total partition count (must be greater than current)
	add_partitions(name string, new_count int) !
	/// Appends records to a specific partition of a topic.
	/// Returns a result containing the base offset and log append time.
	append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult

	/// Fetches records from a specific partition of a topic.
	/// offset: starting offset
	/// max_bytes: maximum number of bytes
	fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult

	/// Deletes records before the specified offset.
	delete_records(topic string, partition int, before_offset i64) !
	/// Retrieves partition information (earliest/latest offset, high watermark, etc.).
	get_partition_info(topic string, partition int) !domain.PartitionInfo
	/// Saves a consumer group.
	save_group(group domain.ConsumerGroup) !

	/// Loads a consumer group.
	load_group(group_id string) !domain.ConsumerGroup

	/// Deletes a consumer group.
	delete_group(group_id string) !

	/// Returns a list of all consumer groups.
	list_groups() ![]domain.GroupInfo
	/// Commits offsets for a consumer group.
	commit_offsets(group_id string, offsets []domain.PartitionOffset) !

	/// Retrieves committed offsets for a consumer group.
	fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult
	/// Saves a SharePartition state for persistence.
	save_share_partition_state(state domain.SharePartitionState) !

	/// Loads a SharePartition state.
	/// Returns none if not found.
	load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState

	/// Deletes a SharePartition state.
	delete_share_partition_state(group_id string, topic_name string, partition i32) !

	/// Loads all SharePartition states for a given group.
	load_all_share_partition_states(group_id string) []domain.SharePartitionState
	/// Checks storage health status.
	health_check() !HealthStatus
	/// Returns storage capability information.
	get_storage_capability() domain.StorageCapability

	/// Returns the cluster metadata port (used only in multi-broker mode).
	get_cluster_metadata_port() ?&ClusterMetadataPort
}

/// HealthStatus represents the state of the storage.
pub enum HealthStatus {
	healthy
	degraded
	unhealthy
}

/// Lock is an interface for partition-level locking.
pub interface Lock {
	/// Releases the lock.
	release() !
}

/// LockableStorage is a StoragePort with added locking capability.
/// Used when concurrency control is required.
pub interface LockableStorage {
	StoragePort /// Acquires a lock for a specific topic/partition.

	acquire_lock(topic string, partition int) !Lock
}
