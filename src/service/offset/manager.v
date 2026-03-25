// Handles OffsetCommit and OffsetFetch business logic.
// Responsible for managing consumer group offsets.
module offset

import domain
import service.port

/// OffsetManager handles offset management business logic.
/// Responsible for offset commits, retrieval, and validation.
pub struct OffsetManager {
mut:
	topic_storage  port.TopicStoragePort
	offset_storage port.OffsetStoragePort
	logger         port.LoggerPort
}

/// new_offset_manager creates a new OffsetManager.
pub fn new_offset_manager(topic_storage port.TopicStoragePort, offset_storage port.OffsetStoragePort, logger port.LoggerPort) &OffsetManager {
	return &OffsetManager{
		topic_storage:  topic_storage
		offset_storage: offset_storage
		logger:         logger
	}
}

/// commit_offsets handles an offset commit request.
/// Validates the request and saves offsets to storage.
pub fn (mut m OffsetManager) commit_offsets(req port.OffsetCommitRequest) !port.OffsetCommitResponse {
	// Validate group ID
	if req.group_id.len == 0 {
		m.logger.warn('Invalid group_id: empty string')
		return error('invalid group_id')
	}

	// Validate offset list
	if req.offsets.len == 0 {
		m.logger.warn('No offsets to commit group_id=${req.group_id}')
		return port.OffsetCommitResponse{
			results: []
		}
	}

	m.logger.debug('Committing offsets group_id=${req.group_id} count=${req.offsets.len}')

	// Commit offsets to storage
	m.offset_storage.commit_offsets(req.group_id, req.offsets) or {
		m.logger.error('Failed to commit offsets group_id=${req.group_id} error=${err.str()}')

		// Create error results for all partitions
		mut results := []port.OffsetCommitResult{cap: req.offsets.len}
		for o in req.offsets {
			results << port.OffsetCommitResult{
				topic:         o.topic
				partition:     o.partition
				error_code:    i16(domain.ErrorCode.unknown_server_error)
				error_message: err.str()
			}
		}
		return port.OffsetCommitResponse{
			results: results
		}
	}

	// Build success results
	mut results := []port.OffsetCommitResult{cap: req.offsets.len}
	for o in req.offsets {
		results << port.OffsetCommitResult{
			topic:         o.topic
			partition:     o.partition
			error_code:    0
			error_message: ''
		}
	}

	m.logger.debug('Offsets committed successfully group_id=${req.group_id} count=${req.offsets.len}')

	return port.OffsetCommitResponse{
		results: results
	}
}

/// fetch_offsets handles an offset fetch request.
/// Retrieves committed offsets from storage and handles TopicId conversion.
pub fn (mut m OffsetManager) fetch_offsets(req port.OffsetFetchRequest) !port.OffsetFetchResponse {
	// Validate group ID
	if req.group_id.len == 0 {
		m.logger.warn('Invalid group_id: empty string')
		return port.OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.invalid_group_id)
		}
	}

	// Return empty result if partition list is empty
	if req.partitions.len == 0 {
		m.logger.debug('No partitions to fetch group_id=${req.group_id}')
		return port.OffsetFetchResponse{
			results:    []
			error_code: 0
		}
	}

	m.logger.debug('Fetching offsets group_id=${req.group_id} partitions=${req.partitions.len}')

	// Retrieve offsets from storage
	fetched := m.offset_storage.fetch_offsets(req.group_id, req.partitions) or {
		m.logger.error('Failed to fetch offsets group_id=${req.group_id} error=${err.str()}')
		return port.OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	// Convert results (domain.OffsetFetchResult -> port.OffsetFetchResult)
	mut results := []port.OffsetFetchResult{cap: fetched.len}
	for f in fetched {
		// Attempt to look up TopicId (for v10 support)
		topic_meta := m.topic_storage.get_topic(f.topic) or {
			// Return result with error code even if topic is not found
			results << create_fetch_result_with_error(f, i16(domain.ErrorCode.unknown_topic_or_partition))
			continue
		}

		// Set TopicId if available
		mut topic_id := ?[]u8(none)
		if topic_meta.topic_id.len > 0 {
			topic_id = topic_meta.topic_id.clone()
		}

		results << create_fetch_result_with_topic_id(f, topic_id)
	}

	m.logger.debug('Offsets fetched successfully group_id=${req.group_id} count=${results.len}')

	return port.OffsetFetchResponse{
		results:    results
		error_code: 0
	}
}

/// fetch_offsets_by_topic_id fetches offsets by TopicId (v10+).
/// Converts TopicId to TopicName before retrieving offsets.
fn (mut m OffsetManager) fetch_offsets_by_topic_id(group_id string, topic_id []u8, partitions []int) !port.OffsetFetchResponse {
	// Look up topic by TopicId
	topic_meta := m.topic_storage.get_topic_by_id(topic_id) or {
		m.logger.warn('Topic not found by ID group_id=${group_id}')
		return port.OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.unknown_topic_id)
		}
	}

	// Build TopicPartition list
	mut topic_partitions := []domain.TopicPartition{cap: partitions.len}
	for p in partitions {
		topic_partitions << domain.TopicPartition{
			topic:     topic_meta.name
			partition: p
		}
	}

	// Delegate to standard fetch_offsets
	return m.fetch_offsets(port.OffsetFetchRequest{
		group_id:       group_id
		partitions:     topic_partitions
		require_stable: false
	})
}

// Helper Functions

/// create_fetch_result_with_topic_id creates an OffsetFetchResult with a TopicId.
fn create_fetch_result_with_topic_id(f domain.OffsetFetchResult, topic_id ?[]u8) port.OffsetFetchResult {
	return port.OffsetFetchResult{
		topic:                  f.topic
		topic_id:               topic_id
		partition:              f.partition
		committed_offset:       f.offset
		committed_leader_epoch: -1
		metadata:               f.metadata
		error_code:             f.error_code
	}
}

/// create_fetch_result_with_error creates an OffsetFetchResult with an error code.
fn create_fetch_result_with_error(f domain.OffsetFetchResult, error_code i16) port.OffsetFetchResult {
	return port.OffsetFetchResult{
		topic:                  f.topic
		topic_id:               none
		partition:              f.partition
		committed_offset:       f.offset
		committed_leader_epoch: -1
		metadata:               f.metadata
		error_code:             error_code
	}
}
