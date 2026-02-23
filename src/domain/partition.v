module domain

/// Partition represents a topic partition.
/// Note: DataCore Stateless architecture
/// - leader_id: the broker that always responds (all brokers are equal)
/// - leader_epoch: always 0 (no leader election in Stateless model)
/// - replica_nodes/isr_nodes: always [broker_id] (no replication, shared storage)
/// These fields are maintained only for Kafka protocol compatibility.
pub struct Partition {
pub:
	topic         string
	index         int
	leader_id     i32
	leader_epoch  i32
	replica_nodes []i32
	isr_nodes     []i32
}

/// PartitionInfo contains partition offset information.
/// topic: topic name
/// partition: partition number
/// earliest_offset: earliest available offset
/// latest_offset: most recent offset
/// high_watermark: high watermark
pub struct PartitionInfo {
pub:
	topic           string
	partition       int
	earliest_offset i64
	latest_offset   i64
	high_watermark  i64
}

/// TopicPartition is a topic-partition pair.
/// Used to jointly identify a topic and partition.
pub struct TopicPartition {
pub:
	topic     string
	partition int
}
