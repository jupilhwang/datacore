module domain

/// PartitionOffset represents a committed offset for a partition.
/// topic: topic name
/// partition: partition number
/// offset: committed offset
/// leader_epoch: leader epoch (default -1)
/// metadata: user-defined metadata
pub struct PartitionOffset {
pub:
	topic        string
	partition    int
	offset       i64
	leader_epoch i32 = -1
	metadata     string
}

/// OffsetCommit represents an offset commit request.
/// group_id: consumer group ID
/// generation_id: generation ID
/// member_id: member ID
/// offsets: list of partition offsets to commit
pub struct OffsetCommit {
pub:
	group_id      string
	generation_id int
	member_id     string
	offsets       []PartitionOffset
}

/// OffsetFetchResult represents the result of an offset fetch.
/// topic: topic name
/// partition: partition number
/// offset: fetched offset
/// metadata: user-defined metadata
/// error_code: error code (0 = success)
pub struct OffsetFetchResult {
pub:
	topic      string
	partition  int
	offset     i64
	metadata   string
	error_code i16
}
