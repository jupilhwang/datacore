// Entity Layer - Topic Domain Model
module domain

// Topic represents a Kafka topic
pub struct Topic {
pub:
	name               string
	partition_count    int
	replication_factor int = 1
	config             TopicConfig
	is_internal        bool
}

// TopicConfig represents topic configuration
pub struct TopicConfig {
pub:
	retention_ms        i64    = 604800000  // 7 days
	retention_bytes     i64    = -1         // unlimited
	segment_bytes       i64    = 1073741824 // 1GB
	cleanup_policy      string = 'delete'
	min_insync_replicas int    = 1
	max_message_bytes   int    = 1048576 // 1MB
}

// TopicMetadata represents topic metadata for API responses
pub struct TopicMetadata {
pub:
	name            string
	topic_id        []u8 // UUID, 16 bytes
	partition_count int
	config          map[string]string
	is_internal     bool
}
