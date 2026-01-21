// Service Layer - Share Group Coordinator (KIP-932)
// Manages share groups, partition assignment, and in-flight record state
module group

import domain
import service.port
import sync
import time
import rand

// ============================================================================
// Share Group Coordinator
// ============================================================================

// ShareGroupCoordinator manages share groups
pub struct ShareGroupCoordinator {
mut:
	// Share groups keyed by group_id
	groups map[string]&domain.ShareGroup
	// Share partitions keyed by "group_id:topic:partition"
	partitions map[string]&domain.SharePartition
	// Share sessions keyed by "group_id:member_id"
	sessions map[string]&domain.ShareSession
	// Configuration
	config domain.ShareGroupConfig
	// Storage for persistence
	storage port.StoragePort
	// Thread safety
	lock sync.RwMutex
	// Assignor
	assignor &ShareGroupSimpleAssignor
}

// new_share_group_coordinator creates a new share group coordinator
pub fn new_share_group_coordinator(storage port.StoragePort, config domain.ShareGroupConfig) &ShareGroupCoordinator {
	return &ShareGroupCoordinator{
		groups:     map[string]&domain.ShareGroup{}
		partitions: map[string]&domain.SharePartition{}
		sessions:   map[string]&domain.ShareSession{}
		config:     config
		storage:    storage
		assignor:   new_simple_assignor()
	}
}

// ============================================================================
// Group Management
// ============================================================================

// get_or_create_group gets or creates a share group
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

// get_group returns a share group by ID
pub fn (mut c ShareGroupCoordinator) get_group(group_id string) ?&domain.ShareGroup {
	c.lock.rlock()
	defer { c.lock.runlock() }
	return c.groups[group_id] or { return none }
}

// delete_group deletes a share group
pub fn (mut c ShareGroupCoordinator) delete_group(group_id string) ! {
	c.lock.@lock()
	defer { c.lock.unlock() }

	group := c.groups[group_id] or { return error('group not found: ${group_id}') }

	if group.members.len > 0 {
		return error('cannot delete group with active members')
	}

	// Delete associated partitions
	mut to_delete := []string{}
	for key, _ in c.partitions {
		if key.starts_with('${group_id}:') {
			to_delete << key
		}
	}
	for key in to_delete {
		c.partitions.delete(key)
	}

	// Delete sessions
	mut sessions_to_delete := []string{}
	for key, _ in c.sessions {
		if key.starts_with('${group_id}:') {
			sessions_to_delete << key
		}
	}
	for key in sessions_to_delete {
		c.sessions.delete(key)
	}

	c.groups.delete(group_id)
}

// ============================================================================
// Member Management
// ============================================================================

// ShareGroupHeartbeatRequest represents a heartbeat request
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

// ShareGroupHeartbeatResponse represents a heartbeat response
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

// heartbeat processes a share group heartbeat
pub fn (mut c ShareGroupCoordinator) heartbeat(req ShareGroupHeartbeatRequest) ShareGroupHeartbeatResponse {
	c.lock.@lock()
	defer { c.lock.unlock() }

	mut group := c.get_or_create_group_internal(req.group_id)
	now := time.now().unix_milli()

	// Generate member ID if this is a new member (epoch = 0)
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

	// Check epoch
	if !is_new_member && req.member_epoch != member.member_epoch {
		// Fenced member
		return ShareGroupHeartbeatResponse{
			error_code:    22 // FENCED_MEMBER_EPOCH
			error_message: 'member epoch mismatch'
			member_id:     member_id
			member_epoch:  member.member_epoch
		}
	}

	// Check if subscriptions changed
	subscriptions_changed := !arrays_equal(member.subscribed_topic_names, req.subscribed_topic_names)
	if subscriptions_changed {
		member.subscribed_topic_names = req.subscribed_topic_names.clone()
		// Update group's subscribed topics
		for topic in req.subscribed_topic_names {
			group.subscribed_topics[topic] = true
		}
	}

	// Trigger rebalance if needed
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

// leave_group handles a member leaving the group
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

	// Release any acquired records for this member
	c.release_member_records_internal(group_id, member_id)

	// Delete session
	session_key := '${group_id}:${member_id}'
	c.sessions.delete(session_key)
}

// remove_expired_members removes members that have timed out
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
			c.release_member_records_internal(group_id, member_id)

			// Delete session
			session_key := '${group_id}:${member_id}'
			c.sessions.delete(session_key)
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

// ============================================================================
// Assignment
// ============================================================================

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

// ============================================================================
// Share Partition Management
// ============================================================================

// get_or_create_partition gets or creates a share partition
pub fn (mut c ShareGroupCoordinator) get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition {
	c.lock.@lock()
	defer { c.lock.unlock() }

	return c.get_or_create_partition_internal(group_id, topic_name, partition)
}

// get_partition returns a share partition
pub fn (mut c ShareGroupCoordinator) get_partition(group_id string, topic_name string, partition i32) ?&domain.SharePartition {
	c.lock.rlock()
	defer { c.lock.runlock() }

	key := '${group_id}:${topic_name}:${partition}'
	return c.partitions[key] or { return none }
}

// ============================================================================
// Record Acquisition
// ============================================================================

// acquire_records acquires records for a consumer
pub fn (mut c ShareGroupCoordinator) acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int) []domain.AcquiredRecordInfo {
	c.lock.@lock()
	defer { c.lock.unlock() }

	group := c.groups[group_id] or { return []domain.AcquiredRecordInfo{} }
	mut sp := c.get_or_create_partition_internal(group_id, topic_name, partition)
	now := time.now().unix_milli()
	lock_duration := group.record_lock_duration_ms

	mut acquired := []domain.AcquiredRecordInfo{}
	mut offset := sp.start_offset

	// Find available records to acquire
	for acquired.len < max_records && offset <= sp.end_offset {
		state := sp.record_states[offset] or { domain.RecordState.available }

		if state == .available {
			// Check if we've hit the max partition locks
			if sp.acquired_records.len >= group.max_partition_locks {
				break
			}

			// Get or initialize delivery count
			mut delivery_count := i32(1)
			if existing := sp.acquired_records[offset] {
				delivery_count = existing.delivery_count + 1
			}

			// Acquire the record
			sp.record_states[offset] = .acquired
			sp.acquired_records[offset] = domain.AcquiredRecord{
				offset:          offset
				member_id:       member_id
				delivery_count:  delivery_count
				acquired_at:     now
				lock_expires_at: now + lock_duration
			}

			acquired << domain.AcquiredRecordInfo{
				offset:         offset
				delivery_count: delivery_count
				timestamp:      now
			}

			sp.total_acquired += 1
		}

		offset += 1
	}

	// Advance end offset if needed
	if offset > sp.end_offset {
		sp.end_offset = offset
	}

	return acquired
}

// acknowledge_records processes acknowledgements for records
pub fn (mut c ShareGroupCoordinator) acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch) domain.ShareAcknowledgeResult {
	c.lock.@lock()
	defer { c.lock.unlock() }

	group := c.groups[group_id] or {
		return domain.ShareAcknowledgeResult{
			topic_name:    batch.topic_name
			partition:     batch.partition
			error_code:    69 // UNKNOWN_SHARE_GROUP
			error_message: 'share group not found'
		}
	}

	mut sp := c.get_or_create_partition_internal(group_id, batch.topic_name, batch.partition)

	// Process each offset in the batch
	for offset := batch.first_offset; offset <= batch.last_offset; offset++ {
		// Skip gap offsets
		if offset in batch.gap_offsets {
			continue
		}

		// Check if record is acquired by this member
		acquired := sp.acquired_records[offset] or { continue }

		if acquired.member_id != member_id {
			// Record not owned by this member
			continue
		}

		match batch.acknowledge_type {
			.accept {
				// Successfully processed
				sp.record_states[offset] = .acknowledged
				sp.acquired_records.delete(offset)
				sp.total_acknowledged += 1
			}
			.release {
				// Release for redelivery
				if acquired.delivery_count < group.delivery_attempt_limit {
					sp.record_states[offset] = .available
				} else {
					// Max attempts reached - archive
					sp.record_states[offset] = .archived
				}
				sp.acquired_records.delete(offset)
				sp.total_released += 1
			}
			.reject {
				// Rejected as unprocessable
				sp.record_states[offset] = .archived
				sp.acquired_records.delete(offset)
				sp.total_rejected += 1
			}
		}
	}

	// Advance SPSO if possible
	c.advance_spso_internal(mut sp)

	return domain.ShareAcknowledgeResult{
		topic_name: batch.topic_name
		partition:  batch.partition
		error_code: 0
	}
}

// release_expired_locks releases records with expired acquisition locks
pub fn (mut c ShareGroupCoordinator) release_expired_locks() {
	c.lock.@lock()
	defer { c.lock.unlock() }

	now := time.now().unix_milli()

	for _, mut sp in c.partitions {
		group := c.groups[sp.group_id] or { continue }

		mut expired := []i64{}
		for offset, acquired in sp.acquired_records {
			if now > acquired.lock_expires_at {
				expired << offset
			}
		}

		for offset in expired {
			acquired := sp.acquired_records[offset] or { continue }

			// Check delivery count
			if acquired.delivery_count >= group.delivery_attempt_limit {
				// Max attempts reached - archive
				sp.record_states[offset] = .archived
			} else {
				// Release for redelivery
				sp.record_states[offset] = .available
			}
			sp.acquired_records.delete(offset)
			sp.total_released += 1
		}

		// Advance SPSO if possible
		c.advance_spso_internal(mut sp)
	}
}

fn (mut c ShareGroupCoordinator) advance_spso_internal(mut sp domain.SharePartition) {
	// Advance SPSO past all acknowledged/archived records
	mut new_start := sp.start_offset
	for new_start <= sp.end_offset {
		state := sp.record_states[new_start] or { break }
		if state == .acknowledged || state == .archived {
			// Clean up state for this offset
			sp.record_states.delete(new_start)
			new_start += 1
		} else {
			break
		}
	}
	sp.start_offset = new_start
}

fn (mut c ShareGroupCoordinator) release_member_records_internal(group_id string, member_id string) {
	for _, mut sp in c.partitions {
		if sp.group_id != group_id {
			continue
		}

		mut to_release := []i64{}
		for offset, acquired in sp.acquired_records {
			if acquired.member_id == member_id {
				to_release << offset
			}
		}

		for offset in to_release {
			sp.record_states[offset] = .available
			sp.acquired_records.delete(offset)
		}
	}
}

fn (mut c ShareGroupCoordinator) get_or_create_partition_internal(group_id string, topic_name string, partition i32) &domain.SharePartition {
	key := '${group_id}:${topic_name}:${partition}'

	if sp := c.partitions[key] {
		return sp
	}

	mut start_offset := i64(0)
	if info := c.storage.get_partition_info(topic_name, partition) {
		start_offset = info.high_watermark
	}

	mut sp := &domain.SharePartition{
		...domain.new_share_partition(topic_name, partition, group_id, start_offset)
	}
	c.partitions[key] = sp
	return sp
}

// ============================================================================
// Share Session Management
// ============================================================================

// get_or_create_session gets or creates a share session
pub fn (mut c ShareGroupCoordinator) get_or_create_session(group_id string, member_id string) &domain.ShareSession {
	c.lock.@lock()
	defer { c.lock.unlock() }

	key := '${group_id}:${member_id}'

	if session := c.sessions[key] {
		return session
	}

	now := time.now().unix_milli()
	session := &domain.ShareSession{
		group_id:       group_id
		member_id:      member_id
		session_epoch:  1
		partitions:     []domain.ShareSessionPartition{}
		acquired_locks: map[string][]i64{}
		created_at:     now
		last_used:      now
	}
	c.sessions[key] = session
	return session
}

// update_session updates a share session
pub fn (mut c ShareGroupCoordinator) update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession {
	c.lock.@lock()
	defer { c.lock.unlock() }

	key := '${group_id}:${member_id}'
	mut session := c.sessions[key] or { return error('session not found') }

	// Check epoch
	if epoch != session.session_epoch && epoch != -1 {
		return error('invalid session epoch')
	}

	now := time.now().unix_milli()
	session.last_used = now

	// Remove partitions
	for to_remove in partitions_to_remove {
		session.partitions = session.partitions.filter(fn [to_remove] (p domain.ShareSessionPartition) bool {
			return p.topic_name != to_remove.topic_name || p.partition != to_remove.partition
		})
	}

	// Add partitions
	for to_add in partitions_to_add {
		// Check if already exists
		mut exists := false
		for existing in session.partitions {
			if existing.topic_name == to_add.topic_name && existing.partition == to_add.partition {
				exists = true
				break
			}
		}
		if !exists {
			session.partitions << to_add
		}
	}

	// Increment epoch
	session.session_epoch += 1
	if session.session_epoch > 2147483647 {
		session.session_epoch = 1 // Wrap around
	}

	return session
}

// close_session closes a share session
pub fn (mut c ShareGroupCoordinator) close_session(group_id string, member_id string) {
	c.lock.@lock()
	defer { c.lock.unlock() }

	key := '${group_id}:${member_id}'

	// Release any acquired locks
	c.release_member_records_internal(group_id, member_id)

	c.sessions.delete(key)
}

// ============================================================================
// Statistics
// ============================================================================

// ShareGroupStats contains statistics for a share group
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

// get_stats returns statistics for a share group
pub fn (mut c ShareGroupCoordinator) get_stats(group_id string) ShareGroupStats {
	c.lock.rlock()
	defer { c.lock.runlock() }

	group := c.groups[group_id] or { return ShareGroupStats{
		group_id: group_id
	} }

	mut partition_count := 0
	mut total_acquired := i64(0)
	mut total_acknowledged := i64(0)
	mut total_released := i64(0)
	mut total_rejected := i64(0)

	for key, sp in c.partitions {
		if key.starts_with('${group_id}:') {
			partition_count += 1
			total_acquired += sp.total_acquired
			total_acknowledged += sp.total_acknowledged
			total_released += sp.total_released
			total_rejected += sp.total_rejected
		}
	}

	return ShareGroupStats{
		group_id:           group_id
		member_count:       group.members.len
		partition_count:    partition_count
		total_acquired:     total_acquired
		total_acknowledged: total_acknowledged
		total_released:     total_released
		total_rejected:     total_rejected
	}
}

// list_groups returns all share groups
pub fn (mut c ShareGroupCoordinator) list_groups() []string {
	c.lock.rlock()
	defer { c.lock.runlock() }

	mut groups := []string{}
	for group_id, _ in c.groups {
		groups << group_id
	}
	return groups
}

// ============================================================================
// Helper Functions
// ============================================================================

fn generate_member_id() string {
	// Generate UUID-like member ID
	mut bytes := []u8{len: 16}
	for i in 0 .. 16 {
		bytes[i] = u8(rand.intn(256) or { 0 })
	}
	// Set version 4 and variant bits
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80

	return '${bytes[0..4].hex()}-${bytes[4..6].hex()}-${bytes[6..8].hex()}-${bytes[8..10].hex()}-${bytes[10..16].hex()}'
}

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

// ============================================================================
// SimpleAssignor (KIP-932)
// ============================================================================

// ShareMemberSubscription contains member's subscription info
pub struct ShareMemberSubscription {
pub:
	member_id string
	rack_id   string
	topics    []string
}

// ShareTopicMetadata contains topic info for assignment
pub struct ShareTopicMetadata {
pub:
	topic_id        []u8
	topic_name      string
	partition_count int
}

// ShareGroupSimpleAssignor implements the SimpleAssignor from KIP-932
// It balances the number of consumers assigned to each partition
pub struct ShareGroupSimpleAssignor {}

pub fn new_simple_assignor() &ShareGroupSimpleAssignor {
	return &ShareGroupSimpleAssignor{}
}

pub fn (a &ShareGroupSimpleAssignor) name() string {
	return 'simple'
}

// assign computes partition assignments for share group members
// Unlike consumer groups, share groups can assign the same partition to multiple members
pub fn (a &ShareGroupSimpleAssignor) assign(members []ShareMemberSubscription, topics map[string]ShareTopicMetadata) map[string][]domain.SharePartitionAssignment {
	mut assignments := map[string][]domain.SharePartitionAssignment{}

	// Initialize empty assignments
	for m in members {
		assignments[m.member_id] = []domain.SharePartitionAssignment{}
	}

	if members.len == 0 {
		return assignments
	}

	// Group members by their subscribed topics
	mut topic_assignments := map[string][]string{} // topic -> member_ids

	for m in members {
		for topic in m.topics {
			if topic !in topic_assignments {
				topic_assignments[topic] = []string{}
			}
			topic_assignments[topic] << m.member_id
		}
	}

	// For each topic, assign partitions to subscribed members
	for topic_name, topic_meta in topics {
		subscribed := topic_assignments[topic_name] or { continue }
		if subscribed.len == 0 {
			continue
		}

		num_partitions := topic_meta.partition_count
		num_members := subscribed.len

		// Create partition list for this topic
		mut partitions := []i32{}
		for p in 0 .. num_partitions {
			partitions << p
		}

		// Assign partitions round-robin
		mut member_idx := 0
		for partition in partitions {
			member_id := subscribed[member_idx % subscribed.len]

			// Find or create topic assignment for this member
			mut found := false
			mut member_assignments := assignments[member_id]
			for i, ta in member_assignments {
				if ta.topic_name == topic_name {
					// Create new assignment with added partition
					mut new_partitions := ta.partitions.clone()
					new_partitions << partition
					member_assignments[i] = domain.SharePartitionAssignment{
						topic_id:   ta.topic_id
						topic_name: ta.topic_name
						partitions: new_partitions
					}
					found = true
					break
				}
			}

			if !found {
				member_assignments << domain.SharePartitionAssignment{
					topic_id:   topic_meta.topic_id
					topic_name: topic_name
					partitions: [partition]
				}
			}
			assignments[member_id] = member_assignments

			member_idx += 1

			// For share groups, if we have more members than partitions,
			// assign each partition to multiple members
			if num_members > num_partitions && member_idx < num_members {
				// Continue assigning this partition to more members
				extra_assignments := (num_members / num_partitions) - 1
				for _ in 0 .. extra_assignments {
					extra_member_id := subscribed[member_idx % subscribed.len]
					member_idx += 1

					mut extra_found := false
					mut extra_member_assignments := assignments[extra_member_id]
					for i, ta in extra_member_assignments {
						if ta.topic_name == topic_name {
							if partition !in ta.partitions {
								mut new_partitions := ta.partitions.clone()
								new_partitions << partition
								extra_member_assignments[i] = domain.SharePartitionAssignment{
									topic_id:   ta.topic_id
									topic_name: ta.topic_name
									partitions: new_partitions
								}
							}
							extra_found = true
							break
						}
					}

					if !extra_found {
						extra_member_assignments << domain.SharePartitionAssignment{
							topic_id:   topic_meta.topic_id
							topic_name: topic_name
							partitions: [partition]
						}
					}
					assignments[extra_member_id] = extra_member_assignments
				}
			}
		}
	}

	return assignments
}
