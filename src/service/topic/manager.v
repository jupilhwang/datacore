// Handles topic management business logic.
// Provides functionality for creating, deleting, listing topics, and adding partitions.
module topic

import domain
import service.port
import common

/// TopicManager handles topic management business logic.
/// Responsible for topic lifecycle management and configuration validation.
pub struct TopicManager {
mut:
	storage port.TopicStoragePort
}

/// new_topic_manager creates a new TopicManager.
fn new_topic_manager(storage port.TopicStoragePort) &TopicManager {
	return &TopicManager{
		storage: storage
	}
}

/// CreateTopicRequest represents a topic creation request.
pub struct CreateTopicRequest {
pub:
	name               string
	num_partitions     int
	replication_factor i16
	configs            map[string]string
}

/// CreateTopicResponse represents a topic creation response.
pub struct CreateTopicResponse {
pub:
	name               string
	error_code         i16
	error_message      string
	num_partitions     int
	replication_factor i16
}

/// create_topic creates a new topic.
/// Performs validation on topic name and partition count.
fn (mut m TopicManager) create_topic(req CreateTopicRequest) CreateTopicResponse {
	// Validate topic name
	// Internal topics (prefixed with __) are only allowed for __schemas
	if req.name.len == 0 || (req.name.starts_with('__') && req.name != '__schemas') {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.invalid_topic_exception)
			error_message: 'Invalid topic name'
		}
	}

	// Validate partition count
	if req.num_partitions <= 0 {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.invalid_partitions)
			error_message: 'Number of partitions must be positive'
		}
	}

	// Check for duplicate topic
	if _ := m.storage.get_topic(req.name) {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.topic_already_exists)
			error_message: 'Topic already exists'
		}
	}

	// Build topic configuration
	config := domain.TopicConfig{
		retention_ms:    common.parse_config_i64(req.configs, 'retention.ms', 604800000)
		retention_bytes: common.parse_config_i64(req.configs, 'retention.bytes', -1)
		segment_bytes:   common.parse_config_i64(req.configs, 'segment.bytes', 1073741824)
		cleanup_policy:  req.configs['cleanup.policy'] or { 'delete' }
	}

	// Create topic
	m.storage.create_topic(req.name, req.num_partitions, config) or {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.unknown_server_error)
			error_message: err.str()
		}
	}

	return CreateTopicResponse{
		name:               req.name
		error_code:         0
		num_partitions:     req.num_partitions
		replication_factor: req.replication_factor
	}
}

/// delete_topic deletes a topic.
/// Internal topics (prefixed with __) cannot be deleted except for __schemas.
fn (mut m TopicManager) delete_topic(name string) ! {
	// Prevent deletion of internal topics
	if name.starts_with('__') && name != '__schemas' {
		return error('Cannot delete internal topic')
	}

	m.storage.delete_topic(name)!
}

/// list_topics returns a list of all topics.
fn (mut m TopicManager) list_topics() ![]domain.TopicMetadata {
	return m.storage.list_topics()
}

/// get_topic retrieves topic metadata.
fn (mut m TopicManager) get_topic(name string) !domain.TopicMetadata {
	return m.storage.get_topic(name)
}

/// add_partitions adds partitions to a topic.
/// The new partition count must be greater than the current count.
fn (mut m TopicManager) add_partitions(name string, new_count int) ! {
	// Retrieve current topic information
	topic := m.storage.get_topic(name)!

	// Validate new partition count
	if new_count <= topic.partition_count {
		return error('New partition count must be greater than current')
	}

	m.storage.add_partitions(name, new_count)!
}

// parse_config_i64 is now in common/config_utils.v.
