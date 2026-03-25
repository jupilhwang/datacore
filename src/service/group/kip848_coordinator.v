// KIP-848 new consumer protocol - Group Coordinator
// Server-side partition assignment with support for incremental rebalancing
//

// DataCore Stateless Architecture Note

// DataCore uses a stateless broker architecture where all brokers access
// shared storage (S3, PostgreSQL, etc.). This simplifies partition assignment:
//
// - No broker-partition affinity: all brokers can serve all partitions
// - No rebalancing cost: assignment changes do not cause data movement
// - Simple assignors: Sticky/Cooperative algorithms provide no benefit
//
// See kip848_types.v for type definitions and kip848_assignors.v for assignor implementations.

module group

import service.port
import time
import rand

// KIP-848 Group Coordinator

/// KIP848GroupCoordinator manages consumer groups using the new protocol.
/// Supports server-side partition assignment and incremental rebalancing.
pub struct KIP848GroupCoordinator {
	assignors        map[string]ServerAssignor
	default_assignor string
mut:
	storage               port.TopicStoragePort
	groups                map[string]&KIP848ConsumerGroup
	heartbeat_interval_ms i32
	session_timeout_ms    i32
	rebalance_timeout_ms  i32
}

/// new_kip848_coordinator creates a new KIP-848 group coordinator.
pub fn new_kip848_coordinator(storage port.TopicStoragePort) &KIP848GroupCoordinator {
	mut assignors := map[string]ServerAssignor{}
	assignors['range'] = new_range_assignor()
	assignors['roundrobin'] = new_round_robin_assignor()
	assignors['sticky'] = new_sticky_assignor()
	assignors['cooperative-sticky'] = new_cooperative_sticky_assignor()
	assignors['uniform'] = new_uniform_assignor()

	return &KIP848GroupCoordinator{
		storage:               storage
		assignors:             assignors
		default_assignor:      'sticky'
		groups:                map[string]&KIP848ConsumerGroup{}
		heartbeat_interval_ms: 3000
		session_timeout_ms:    45000
		rebalance_timeout_ms:  300000
	}
}

/// HeartbeatResult represents the result of a heartbeat operation.
pub struct HeartbeatResult {
pub:
	error_code            i16
	error_message         ?string
	member_id             string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?[]TopicPartition
}

/// process_heartbeat handles a ConsumerGroupHeartbeat request.
/// Handles new member joins, leaves, and regular heartbeats.
pub fn (mut c KIP848GroupCoordinator) process_heartbeat(group_id string,
	member_id string,
	member_epoch i32,
	instance_id ?string,
	rack_id ?string,
	rebalance_timeout_ms i32,
	subscribed_topic_names []string,
	server_assignor ?string) !HeartbeatResult {
	now := time.now().unix_milli()

	// Validate group_id
	if group_id == '' {
		return HeartbeatResult{
			error_code:    24
			error_message: 'Group ID cannot be empty'
			member_epoch:  -1
		}
	}

	// Get or create group
	mut group := c.get_or_create_group(group_id)

	// Process based on member_epoch
	if member_epoch == 0 && member_id == '' {
		// New member joining
		return c.handle_join(mut group, instance_id, rack_id, rebalance_timeout_ms, subscribed_topic_names,
			server_assignor, now)
	} else if member_epoch == -1 {
		// Member leaving
		return c.handle_leave(mut group, member_id, now)
	} else if member_epoch > 0 {
		// Regular heartbeat
		return c.handle_heartbeat(mut group, member_id, member_epoch, subscribed_topic_names,
			now)
	} else {
		// Invalid epoch
		return HeartbeatResult{
			error_code:    25
			error_message: 'Invalid member epoch'
			member_epoch:  -1
		}
	}
}

/// get_or_create_group returns an existing group or creates a new one.
fn (mut c KIP848GroupCoordinator) get_or_create_group(group_id string) &KIP848ConsumerGroup {
	// Return group if it exists
	if group := c.groups[group_id] {
		return group
	}

	now := time.now().unix_milli()
	group := &KIP848ConsumerGroup{
		group_id:           group_id
		group_epoch:        0
		assignment_epoch:   0
		state:              .empty
		protocol_type:      'consumer'
		server_assignor:    c.default_assignor
		members:            map[string]&KIP848Member{}
		target_assignment:  map[string][]TopicPartition{}
		current_assignment: map[string][]TopicPartition{}
		subscribed_topics:  map[string]bool{}
		created_at:         now
		updated_at:         now
	}
	c.groups[group_id] = group
	return group
}

/// handle_join handles a new member joining.
fn (mut c KIP848GroupCoordinator) handle_join(mut group KIP848ConsumerGroup,
	instance_id ?string,
	rack_id ?string,
	rebalance_timeout_ms i32,
	subscribed_topic_names []string,
	server_assignor ?string,
	now i64) !HeartbeatResult {
	// Generate new member_id
	member_id := 'consumer-${group.group_id}-${rand.i64()}'

	// Check for static member (instance_id)
	if inst_id := instance_id {
		// Static member - check for existing member with the same instance_id
		for member_key, _ in group.members {
			if mut existing_member := group.members[member_key] {
				if m_inst := existing_member.instance_id {
					if m_inst == inst_id {
						// Fence and reuse previous member
						existing_member.state = .fenced
					}
				}
			}
		}
	}

	// Create new member
	member := &KIP848Member{
		member_id:              member_id
		instance_id:            instance_id
		rack_id:                rack_id
		subscribed_topic_names: subscribed_topic_names
		server_assignor:        server_assignor
		member_epoch:           1
		previous_member_epoch:  0
		state:                  .subscribing
		assigned_partitions:    []TopicPartition{}
		pending_partitions:     []TopicPartition{}
		revoking_partitions:    []TopicPartition{}
		rebalance_timeout_ms:   rebalance_timeout_ms
		session_timeout_ms:     c.session_timeout_ms
		last_heartbeat:         now
		joined_at:              now
	}

	group.members[member_id] = member

	// Update subscribed topics
	for topic in subscribed_topic_names {
		group.subscribed_topics[topic] = true
	}

	// Set assignor if specified
	if assignor := server_assignor {
		if assignor in c.assignors {
			group.server_assignor = assignor
		}
	}

	// Trigger rebalancing
	group.group_epoch++
	group.state = .assigning
	group.updated_at = now

	// Compute new assignment
	c.compute_assignment(mut group) or {
		return HeartbeatResult{
			error_code:    -1
			error_message: 'Failed to compute assignment: ${err.msg()}'
			member_id:     member_id
			member_epoch:  -1
		}
	}

	// Get this member's assignment
	assignment := group.target_assignment[member_id] or { []TopicPartition{} }

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          1
		heartbeat_interval_ms: c.heartbeat_interval_ms
		assignment:            assignment
	}
}

/// handle_leave handles a member leaving.
fn (mut c KIP848GroupCoordinator) handle_leave(mut group KIP848ConsumerGroup,
	member_id string,
	now i64) !HeartbeatResult {
	// Remove member
	if member_id in group.members {
		group.members.delete(member_id)
		group.target_assignment.delete(member_id)
		group.current_assignment.delete(member_id)
	}

	// Update group state
	if group.members.len == 0 {
		group.state = .empty
	} else {
		// Trigger rebalancing
		group.group_epoch++
		group.state = .assigning
		c.compute_assignment(mut group)!
	}

	group.updated_at = now

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          -1
		heartbeat_interval_ms: 0
	}
}

/// handle_heartbeat handles a regular heartbeat.
fn (mut c KIP848GroupCoordinator) handle_heartbeat(mut group KIP848ConsumerGroup,
	member_id string,
	member_epoch i32,
	subscribed_topic_names []string,
	now i64) !HeartbeatResult {
	// Find member
	mut member := group.members[member_id] or {
		return HeartbeatResult{
			error_code:    25
			error_message: 'Unknown member ID'
			member_epoch:  -1
		}
	}

	// Verify epoch
	if member_epoch < member.member_epoch {
		return HeartbeatResult{
			error_code:    82
			error_message: 'Member epoch is stale'
			member_id:     member_id
			member_epoch:  member.member_epoch
		}
	}

	// Update last heartbeat
	member.last_heartbeat = now

	// Check for subscription changes
	mut subscription_changed := false
	if subscribed_topic_names.len != member.subscribed_topic_names.len {
		subscription_changed = true
	} else {
		for topic in subscribed_topic_names {
			if topic !in member.subscribed_topic_names {
				subscription_changed = true
				break
			}
		}
	}

	if subscription_changed {
		member.subscribed_topic_names = subscribed_topic_names.clone()
		member.member_epoch++

		// Update group subscribed topics
		group.subscribed_topics.clear()
		for _, m in group.members {
			for topic in m.subscribed_topic_names {
				group.subscribed_topics[topic] = true
			}
		}

		// Trigger rebalancing
		group.group_epoch++
		group.state = .assigning
		c.compute_assignment(mut group)!
	}

	// Get current assignment
	assignment := group.target_assignment[member_id] or { []TopicPartition{} }

	// Check for assignment changes
	if assignment.len != member.assigned_partitions.len {
		member.state = .reconciling
	} else {
		member.state = .stable
	}

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          member.member_epoch
		heartbeat_interval_ms: c.heartbeat_interval_ms
		assignment:            assignment
	}
}

/// compute_assignment computes a new partition assignment for all members.
fn (mut c KIP848GroupCoordinator) compute_assignment(mut group KIP848ConsumerGroup) ! {
	if group.members.len == 0 {
		group.target_assignment.clear()
		group.assignment_epoch = group.group_epoch
		group.state = .empty
		return
	}

	// Build member subscriptions
	mut subscriptions := []MemberSubscription{}
	for member_id, member in group.members {
		subscriptions << MemberSubscription{
			member_id:        member_id
			instance_id:      member.instance_id
			rack_id:          member.rack_id
			topics:           member.subscribed_topic_names
			owned_partitions: member.assigned_partitions
		}
	}

	// Build topic metadata
	mut topics := map[string]TopicMetadata{}
	for topic_name, _ in group.subscribed_topics {
		topic_meta := c.storage.get_topic(topic_name) or { continue }
		topics[topic_name] = TopicMetadata{
			topic_id:        topic_meta.topic_id
			topic_name:      topic_name
			partition_count: topic_meta.partition_count
		}
	}

	// Get assignor
	assignor := c.assignors[group.server_assignor] or {
		c.assignors[c.default_assignor] or { return error('No assignor available') }
	}

	// Compute assignment
	assignment := assignor.assign(subscriptions, topics)!

	// Update target assignment
	group.target_assignment = assignment.clone()
	group.assignment_epoch = group.group_epoch

	// Update member assignments
	for member_id, partitions in assignment {
		if mut member := group.members[member_id] {
			member.pending_partitions = partitions
		}
	}

	// Check if all members are stable
	mut all_stable := true
	for _, member in group.members {
		if member.state != .stable {
			all_stable = false
			break
		}
	}

	if all_stable {
		group.state = .stable
		group.current_assignment = group.target_assignment.clone()
	} else {
		group.state = .reconciling
	}
}

/// get_group returns a group by ID.
pub fn (c &KIP848GroupCoordinator) get_group(group_id string) ?&KIP848ConsumerGroup {
	return c.groups[group_id] or { return none }
}

/// list_groups returns all groups.
pub fn (c &KIP848GroupCoordinator) list_groups() []&KIP848ConsumerGroup {
	mut result := []&KIP848ConsumerGroup{}
	for _, g in c.groups {
		result << g
	}
	return result
}

/// expire_members removes members that have not sent a heartbeat.
pub fn (mut c KIP848GroupCoordinator) expire_members() ! {
	now := time.now().unix_milli()

	for group_id, mut group in c.groups {
		mut expired := []string{}

		for member_id, member in group.members {
			if now - member.last_heartbeat > member.session_timeout_ms {
				expired << member_id
			}
		}

		for member_id in expired {
			group.members.delete(member_id)
			group.target_assignment.delete(member_id)
			group.current_assignment.delete(member_id)
		}

		if expired.len > 0 {
			if group.members.len == 0 {
				group.state = .empty
			} else {
				group.group_epoch++
				group.state = .assigning
				c.compute_assignment(mut group) or {
					return error('failed to compute assignment for group ${group_id}: ${err}')
				}
			}
		}

		// Remove empty group after timeout
		if group.state == .empty && now - group.updated_at > 300000 {
			c.groups.delete(group_id)
		}
	}
}
