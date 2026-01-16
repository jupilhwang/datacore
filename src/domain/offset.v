// Entity Layer - Offset Domain Model
module domain

// PartitionOffset represents committed offset for a partition
pub struct PartitionOffset {
pub:
    topic               string
    partition           int
    offset              i64
    leader_epoch        i32 = -1
    metadata            string
}

// OffsetCommit represents an offset commit request
pub struct OffsetCommit {
pub:
    group_id            string
    generation_id       int
    member_id           string
    offsets             []PartitionOffset
}

// OffsetFetchResult represents offset fetch result for a partition
pub struct OffsetFetchResult {
pub:
    topic               string
    partition           int
    offset              i64
    metadata            string
    error_code          i16
}
