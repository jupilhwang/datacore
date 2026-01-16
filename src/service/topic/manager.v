// UseCase Layer - Topic Manager UseCase
module topic

import domain
import service.port

// TopicManager handles topic management business logic
pub struct TopicManager {
    storage port.StoragePort
}

pub fn new_topic_manager(storage port.StoragePort) &TopicManager {
    return &TopicManager{
        storage: storage
    }
}

// CreateTopicRequest represents a create topic request
pub struct CreateTopicRequest {
pub:
    name                string
    num_partitions      int
    replication_factor  i16
    configs             map[string]string
}

// CreateTopicResponse represents a create topic response
pub struct CreateTopicResponse {
pub:
    name                string
    error_code          i16
    error_message       string
    num_partitions      int
    replication_factor  i16
}

// CreateTopic creates a new topic
pub fn (m &TopicManager) create_topic(req CreateTopicRequest) CreateTopicResponse {
    // Validate topic name
    if req.name.len == 0 || req.name.starts_with('__') && req.name != '__schemas' {
        return CreateTopicResponse{
            name: req.name
            error_code: i16(domain.ErrorCode.invalid_topic_exception)
            error_message: 'Invalid topic name'
        }
    }
    
    // Validate partitions
    if req.num_partitions <= 0 {
        return CreateTopicResponse{
            name: req.name
            error_code: i16(domain.ErrorCode.invalid_partitions)
            error_message: 'Number of partitions must be positive'
        }
    }
    
    // Check if topic already exists
    if _ := m.storage.get_topic(req.name) {
        return CreateTopicResponse{
            name: req.name
            error_code: i16(domain.ErrorCode.topic_already_exists)
            error_message: 'Topic already exists'
        }
    }
    
    // Build topic config
    config := domain.TopicConfig{
        retention_ms: parse_config_i64(req.configs, 'retention.ms', 604800000)
        retention_bytes: parse_config_i64(req.configs, 'retention.bytes', -1)
        segment_bytes: parse_config_i64(req.configs, 'segment.bytes', 1073741824)
        cleanup_policy: req.configs['cleanup.policy'] or { 'delete' }
    }
    
    // Create topic
    m.storage.create_topic(req.name, req.num_partitions, config) or {
        return CreateTopicResponse{
            name: req.name
            error_code: i16(domain.ErrorCode.unknown_server_error)
            error_message: err.str()
        }
    }
    
    return CreateTopicResponse{
        name: req.name
        error_code: 0
        num_partitions: req.num_partitions
        replication_factor: req.replication_factor
    }
}

// DeleteTopic deletes a topic
pub fn (m &TopicManager) delete_topic(name string) !{
    // Prevent deletion of internal topics
    if name.starts_with('__') && name != '__schemas' {
        return error('Cannot delete internal topic')
    }
    
    m.storage.delete_topic(name)!
}

// ListTopics lists all topics
pub fn (m &TopicManager) list_topics() ![]domain.TopicMetadata {
    return m.storage.list_topics()
}

// GetTopic gets topic metadata
pub fn (m &TopicManager) get_topic(name string) !domain.TopicMetadata {
    return m.storage.get_topic(name)
}

// AddPartitions adds partitions to a topic
pub fn (m &TopicManager) add_partitions(name string, new_count int) ! {
    // Get current topic
    topic := m.storage.get_topic(name)!
    
    if new_count <= topic.partition_count {
        return error('New partition count must be greater than current')
    }
    
    m.storage.add_partitions(name, new_count)!
}

// Helper function to parse config value
fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
    if val := configs[key] {
        return val.i64()
    }
    return default_val
}
