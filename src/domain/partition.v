// Entity Layer - Partition Domain Model
module domain

// Partition represents a topic partition
// NOTE: DataCore Stateless Architecture
// - leader_id: always the responding broker (all brokers equivalent)
// - leader_epoch: always 0 (no leader election in stateless model)
// - replica_nodes/isr_nodes: always [broker_id] (no replication, shared storage)
// These fields are maintained for Kafka protocol compatibility only.
pub struct Partition {
pub:
    topic               string
    index               int
    leader_id           i32         // Stateless: always current broker
    leader_epoch        i32         // Stateless: always 0
    replica_nodes       []i32       // Stateless: always [broker_id]
    isr_nodes           []i32       // Stateless: always [broker_id]
}

// PartitionInfo contains partition offset information
pub struct PartitionInfo {
pub:
    topic               string
    partition           int
    earliest_offset     i64
    latest_offset       i64
    high_watermark      i64
}

// TopicPartition is a topic-partition pair
pub struct TopicPartition {
pub:
    topic               string
    partition           int
}
