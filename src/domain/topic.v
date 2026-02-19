module domain

/// Topic represents a Kafka topic.
/// name: topic name
/// partition_count: number of partitions
/// replication_factor: replication factor (always 1 in Stateless mode)
/// config: topic configuration
/// is_internal: whether it is an internal topic (e.g. __consumer_offsets)
pub struct Topic {
pub:
	name               string
	partition_count    int
	replication_factor int = 1
	config             TopicConfig
	is_internal        bool
}

/// TopicConfig represents topic configuration.
/// retention_ms: message retention period (milliseconds, default 7 days)
/// retention_bytes: maximum retention size (-1 = unlimited)
/// segment_bytes: segment file size (default 1GB)
/// cleanup_policy: cleanup policy ('delete' or 'compact')
/// min_insync_replicas: minimum number of in-sync replicas
/// max_message_bytes: maximum message size
/// compression_type: compression type ('none', 'gzip', 'snappy', 'lz4', 'zstd')
pub struct TopicConfig {
pub:
	retention_ms        i64    = 604800000
	retention_bytes     i64    = -1
	segment_bytes       i64    = 1073741824
	cleanup_policy      string = 'delete'
	min_insync_replicas int    = 1
	max_message_bytes   int    = 1048576
	compression_type    string = 'none'
}

/// TopicMetadata represents topic metadata for API responses.
/// name: topic name
/// topic_id: topic UUID (16 bytes)
/// partition_count: number of partitions
/// config: configuration map
/// is_internal: whether it is an internal topic
pub struct TopicMetadata {
pub:
	name            string
	topic_id        []u8 // UUID, 16 bytes
	partition_count int
	config          map[string]string
	is_internal     bool
}
