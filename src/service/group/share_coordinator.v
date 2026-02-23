// Manages share groups, partition assignments, and the state of in-progress records.
// Unlike traditional consumer groups, share groups allow multiple consumers to share the same partition.
module group

import domain
import service.port
import sync
import time
import rand

// Share group coordinator

/// ShareGroupCoordinator manages share groups.
/// Responsible for membership management, partition assignment, and record state tracking.
pub struct ShareGroupCoordinator {
mut:
	// Share groups keyed by group_id
	groups map[string]&domain.ShareGroup
	config domain.ShareGroupConfig
	// Storage for persistence
	storage port.StoragePort
	// Thread safety
	lock sync.RwMutex
	// Assignor
	assignor &ShareGroupSimpleAssignor
	// Partition manager
	partition_manager &SharePartitionManager
	// Session manager
	session_manager &ShareSessionManager
}

/// new_share_group_coordinator creates a new share group coordinator.
pub fn new_share_group_coordinator(storage port.StoragePort, config domain.ShareGroupConfig) &ShareGroupCoordinator {
	return &ShareGroupCoordinator{
		groups:            map[string]&domain.ShareGroup{}
		config:            config
		storage:           storage
		assignor:          new_simple_assignor()
		partition_manager: new_share_partition_manager(storage)
		session_manager:   new_share_session_manager()
	}
}

// Group management

/// get_or_create_group gets or creates a share group.
pub fn (mut c ShareGroupCoordinator) get_or_create_group(group_id string) &domain.ShareGroup {
	c.lock.@lock()
	defer { c.lock.unlock() }

	if group := c.groups[group_id] {
		return group
	}

	// Create new group
	mut new_group := &domain.ShareGroup{
		...domain.new_share_group(group_id, c.config)
	}
	c.groups[group_id] = new_group
	return new_group
}

/// get_group returns a share group by ID.
pub fn (mut c ShareGroupCoordinator) get_group(group_id string) ?&domain.ShareGroup {
	c.lock.rlock()
	defer { c.lock.runlock() }
	return c.groups[group_id] or { return none }
}

/// delete_group deletes a share group.
/// Groups with active members cannot be deleted.
pub fn (mut c ShareGroupCoordinator) delete_group(group_id string) ! {
	c.lock.@lock()
	defer { c.lock.unlock() }

	group := c.groups[group_id] or { return error('group not found: ${group_id}') }

	if group.members.len > 0 {
		return error('cannot delete group with active members')
	}

	// Delete associated partitions
	c.partition_manager.delete_partitions_for_group(group_id)

	// Delete sessions
	c.session_manager.delete_sessions_for_group(group_id)

	c.groups.delete(group_id)
}

/// list_groups returns all share groups.
pub fn (mut c ShareGroupCoordinator) list_groups() []string {
	c.lock.rlock()
	defer { c.lock.runlock() }

	mut groups := []string{}
	for group_id, _ in c.groups {
		groups << group_id
	}
	return groups
}

// Member management

/// ShareGroupHeartbeatRequest represents a heartbeat request.
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

/// ShareGroupHeartbeatResponse represents a heartbeat response.
pub struct ShareGroupHeartbeatResponse {
pub:
	error_code                i16
	error_message             string
	member_id                 string
	member_epoch              i32
	heartbeat_interval        i32
	assignment                []domain.SharePartitionAssignment
	should_compute_assignment bool
}

/// heartbeat handles a share group heartbeat.
/// Handles new member joins, subscription changes, and rebalancing.
pub fn (mut c ShareGroupCoordinator) heartbeat(req ShareGroupHeartbeatRequest) ShareGroupHeartbeatResponse {
	c.lock.@lock()
	defer { c.lock.unlock() }

	mut group := c.get_or_create_group_internal(req.group_id)
	now := time.now().unix_milli()

	// Generate member ID for new members (epoch = 0)
	mut member_id := req.member_id
	mut is_new_member := false

	if req.member_epoch == 0 || member_id.len == 0 {
		// New member joining
		member_id = generate_member_id()
		is_new_member = true
	}

	// Get or create member
	mut member := group.members[member_id] or {
		// Create new member
		mut new_member := &domain.ShareMember{
			member_id:              member_id
			rack_id:                req.rack_id
			client_id:              ''
			client_host:            ''
			subscribed_topic_names: req.subscribed_topic_names
			member_epoch:           0
			state:                  .joining
			assigned_partitions:    []domain.SharePartitionAssignment{}
			last_heartbeat:         now
			joined_at:              now
		}
		group.members[member_id] = new_member
		is_new_member = true
		new_member
	}

	// Update member info
	member.last_heartbeat = now
	member.rack_id = req.rack_id

	// Verify epoch
	if !is_new_member && req.member_epoch != member.member_epoch {
		// Fenced member
		return ShareGroupHeartbeatResponse{
			error_code:    22
			error_message: 'member epoch mismatch'
			member_id:     member_id
			member_epoch:  member.member_epoch
		}
	}

	// Check for subscription changes
	subscriptions_changed := !arrays_equal(member.subscribed_topic_names, req.subscribed_topic_names)
	if subscriptions_changed {
		member.subscribed_topic_names = req.subscribed_topic_names.clone()
		// Update group subscribed topics
		for topic in req.subscribed_topic_names {
			group.subscribed_topics[topic] = true
		}
	}

	// Trigger rebalancing if needed
	mut needs_rebalance := is_new_member || subscriptions_changed

	if needs_rebalance {
		group.group_epoch += 1
		c.compute_assignment(mut group)
	}

	// Update member epoch and state
	if is_new_member || needs_rebalance {
		member.member_epoch = group.assignment_epoch
		member.state = .stable
	}

	// Get member's assignment
	assignment := group.target_assignment[member_id] or { []domain.SharePartitionAssignment{} }
	member.assigned_partitions = assignment

	// Update group state
	if group.members.len > 0 {
		group.state = .stable
	}
	group.updated_at = now

	return ShareGroupHeartbeatResponse{
		error_code:         0
		member_id:          member_id
		member_epoch:       member.member_epoch
		heartbeat_interval: group.heartbeat_interval_ms
		assignment:         assignment
	}
}

/// leave_group handles a member leaving the group.
pub fn (mut c ShareGroupCoordinator) leave_group(group_id string, member_id string) ! {
	c.lock.@lock()
	defer { c.lock.unlock() }

	mut group := c.groups[group_id] or { return error('group not found') }

	if member_id !in group.members {
		return error('member not found')
	}

	// Remove member
	group.members.delete(member_id)
	group.target_assignment.delete(member_id)

	// Increment epoch and recompute assignment
	group.group_epoch += 1
	c.compute_assignment(mut group)

	// Update group state
	if group.members.len == 0 {
		group.state = .empty
	}

	// Release records acquired by this member
	c.partition_manager.release_member_records_internal(group_id, member_id)

	// Delete session
	c.session_manager.delete_session(group_id, member_id)
}

/// remove_expired_members removes timed-out members.
pub fn (mut c ShareGroupCoordinator) remove_expired_members() {
	c.lock.@lock()
	defer { c.lock.unlock() }

	now := time.now().unix_milli()

	for group_id, mut group in c.groups {
		mut expired_members := []string{}

		for member_id, member in group.members {
			if now - member.last_heartbeat > group.session_timeout_ms {
				expired_members << member_id
			}
		}

		for member_id in expired_members {
			group.members.delete(member_id)
			group.target_assignment.delete(member_id)
			c.partition_manager.release_member_records_internal(group_id, member_id)

			// Delete session
			c.session_manager.delete_session(group_id, member_id)
		}

		if expired_members.len > 0 {
			group.group_epoch += 1
			c.compute_assignment(mut group)

			if group.members.len == 0 {
				group.state = .empty
			}
		}
	}
}

// Assignment

/// compute_assignment computes partition assignments for all members.
fn (mut c ShareGroupCoordinator) compute_assignment(mut group domain.ShareGroup) {
	// Collect member subscriptions
	mut member_subs := []ShareMemberSubscription{}
	for _, member in group.members {
		member_subs << ShareMemberSubscription{
			member_id: member.member_id
			rack_id:   member.rack_id
			topics:    member.subscribed_topic_names
		}
	}

	// Collect topic metadata
	mut topic_meta := map[string]ShareTopicMetadata{}
	for topic_name, _ in group.subscribed_topics {
		// Get partition count from storage
		topic := c.storage.get_topic(topic_name) or { continue }
		topic_meta[topic_name] = ShareTopicMetadata{
			topic_id:        topic.topic_id
			topic_name:      topic_name
			partition_count: topic.partition_count
		}
	}

	// Compute assignment using SimpleAssignor
	new_assignment := c.assignor.assign(member_subs, topic_meta)

	// Update target assignment - clone the map
	group.target_assignment.clear()
	for k, v in new_assignment {
		group.target_assignment[k] = v
	}
	group.assignment_epoch = group.group_epoch
}

/// get_or_create_group_internal gets or creates a group without acquiring a lock.
fn (mut c ShareGroupCoordinator) get_or_create_group_internal(group_id string) &domain.ShareGroup {
	if group := c.groups[group_id] {
		return group
	}

	mut new_group := &domain.ShareGroup{
		...domain.new_share_group(group_id, c.config)
	}
	c.groups[group_id] = new_group
	return new_group
}

// Partition management (delegation)

/// get_or_create_partition gets or creates a share partition.
pub fn (mut c ShareGroupCoordinator) get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition {
	return c.partition_manager.get_or_create_partition(group_id, topic_name, partition)
}

/// get_partition returns a share partition.
pub fn (mut c ShareGroupCoordinator) get_partition(group_id string, topic_name string, partition i32) ?&domain.SharePartition {
	return c.partition_manager.get_partition(group_id, topic_name, partition)
}

// Record acquisition (delegation)

/// acquire_records acquires records for a consumer.
pub fn (mut c ShareGroupCoordinator) acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int) []domain.AcquiredRecordInfo {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return []domain.AcquiredRecordInfo{}
	}
	lock_duration := group.record_lock_duration_ms
	max_partition_locks := group.max_partition_locks
	c.lock.runlock()

	return c.partition_manager.acquire_records(group_id, member_id, topic_name, partition,
		max_records, lock_duration, max_partition_locks)
}

/// acknowledge_records handles record acknowledgements.
pub fn (mut c ShareGroupCoordinator) acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch) domain.ShareAcknowledgeResult {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return domain.ShareAcknowledgeResult{
			topic_name:    batch.topic_name
			partition:     batch.partition
			error_code:    69
			error_message: 'share group not found'
		}
	}
	delivery_attempt_limit := group.delivery_attempt_limit
	c.lock.runlock()

	return c.partition_manager.acknowledge_records(group_id, member_id, batch, delivery_attempt_limit)
}

/// release_expired_locks releases records whose acquisition locks have expired.
pub fn (mut c ShareGroupCoordinator) release_expired_locks() {
	c.lock.rlock()
	// Extract delivery attempt limits for each group
	mut group_limits := map[string]i32{}
	for k, g in c.groups {
		group_limits[k] = g.delivery_attempt_limit
	}
	c.lock.runlock()

	c.partition_manager.release_expired_locks_with_limits(group_limits)
}

// Session management (delegation)

/// get_or_create_session gets or creates a share session.
pub fn (mut c ShareGroupCoordinator) get_or_create_session(group_id string, member_id string) &domain.ShareSession {
	return c.session_manager.get_or_create_session(group_id, member_id)
}

/// update_session updates a share session.
pub fn (mut c ShareGroupCoordinator) update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession {
	return c.session_manager.update_session(group_id, member_id, epoch, partitions_to_add,
		partitions_to_remove)
}

/// close_session closes a share session.
pub fn (mut c ShareGroupCoordinator) close_session(group_id string, member_id string) {
	// Release acquired locks
	c.partition_manager.release_member_records(group_id, member_id)

	c.session_manager.close_session(group_id, member_id)
}

// Statistics

/// ShareGroupStats holds statistics for a share group.
pub struct ShareGroupStats {
pub:
	group_id           string
	member_count       int
	partition_count    int
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// get_stats returns statistics for a share group.
pub fn (mut c ShareGroupCoordinator) get_stats(group_id string) ShareGroupStats {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return ShareGroupStats{
			group_id: group_id
		}
	}
	member_count := group.members.len
	c.lock.runlock()

	partition_count, total_acquired, total_acknowledged, total_released, total_rejected := c.partition_manager.get_partition_stats(group_id)

	return ShareGroupStats{
		group_id:           group_id
		member_count:       member_count
		partition_count:    partition_count
		total_acquired:     total_acquired
		total_acknowledged: total_acknowledged
		total_released:     total_released
		total_rejected:     total_rejected
	}
}

/// generate_member_id generates a UUID-format member ID.
fn generate_member_id() string {
	mut bytes := []u8{len: 16}
	for i in 0 .. 16 {
		bytes[i] = u8(rand.intn(256) or { 0 })
	}
	// Set version 4 and variant bits
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80

	return '${bytes[0..4].hex()}-${bytes[4..6].hex()}-${bytes[6..8].hex()}-${bytes[8..10].hex()}-${bytes[10..16].hex()}'
}

/// arrays_equal checks whether two string arrays are equal.
fn arrays_equal(a []string, b []string) bool {
	if a.len != b.len {
		return false
	}
	for i, v in a {
		if v != b[i] {
			return false
		}
	}
	return true
}
