// Entity Layer - Partition Domain Model
module domain

// Partition represents a topic partition
pub struct Partition {
pub:
    topic               string
    index               int
    leader_id           i32
    leader_epoch        i32
    replica_nodes       []i32
    isr_nodes           []i32
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
