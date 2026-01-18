// KIP-848 New Consumer Protocol - Group Coordinator Extension
// Server-side partition assignment with incremental rebalancing
module group

import service.port
import time
import rand

// ============================================================================
// KIP-848 Member State Machine
// ============================================================================

// MemberState represents the state of a member in the new protocol
pub enum MemberState {
    unsubscribed     // Member has no subscriptions
    subscribing      // Member is subscribing to topics
    stable           // Member has a stable assignment
    reconciling      // Member is reconciling assignment changes
    assigning        // Server is computing new assignment
    unsubscribing    // Member is leaving the group
    fenced           // Member has been fenced (epoch mismatch)
}

// KIP848Member represents a consumer group member using KIP-848 protocol
pub struct KIP848Member {
pub mut:
    member_id                string
    instance_id              ?string
    rack_id                  ?string
    client_id                string
    client_host              string
    subscribed_topic_names   []string
    subscribed_topic_regex   ?string
    server_assignor          ?string
    member_epoch             i32
    previous_member_epoch    i32
    state                    MemberState
    assigned_partitions      []TopicPartition
    pending_partitions       []TopicPartition   // Partitions to be assigned
    revoking_partitions      []TopicPartition   // Partitions to be revoked
    rebalance_timeout_ms     i32
    session_timeout_ms       i32
    last_heartbeat           i64                // Unix timestamp ms
    joined_at                i64
}

// TopicPartition represents a topic-partition pair
pub struct TopicPartition {
pub:
    topic_id    []u8    // UUID (16 bytes)
    topic_name  string
    partition   i32
}

// ============================================================================
// KIP-848 Consumer Group
// ============================================================================

// KIP848ConsumerGroup represents a consumer group using the new protocol
pub struct KIP848ConsumerGroup {
pub mut:
    group_id            string
    group_epoch         i32
    assignment_epoch    i32
    state               KIP848GroupState
    protocol_type       string
    server_assignor     string
    members             map[string]&KIP848Member
    target_assignment   map[string][]TopicPartition   // member_id -> assigned partitions
    current_assignment  map[string][]TopicPartition   // Current stable assignment
    subscribed_topics   map[string]bool               // All subscribed topics
    created_at          i64
    updated_at          i64
}

// KIP848GroupState represents the state of a KIP-848 group
pub enum KIP848GroupState {
    empty             // No members
    assigning         // Computing new assignment
    reconciling       // Members are reconciling
    stable            // All members have stable assignment
    dead              // Group is being deleted
}

// ============================================================================
// Server-side Assignor Interface
// ============================================================================

// ServerAssignor computes partition assignments on the server side
pub interface ServerAssignor {
    name() string
    assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition
}

// MemberSubscription contains member's subscription info for assignment
pub struct MemberSubscription {
pub:
    member_id       string
    instance_id     ?string
    rack_id         ?string
    topics          []string
    owned_partitions []TopicPartition  // Currently owned for sticky assignment
}

// TopicMetadata contains topic info for assignment
pub struct TopicMetadata {
pub:
    topic_id         []u8
    topic_name       string
    partition_count  int
}

// ============================================================================
// Range Assignor
// ============================================================================

pub struct RangeAssignor {}

pub fn new_range_assignor() &RangeAssignor {
    return &RangeAssignor{}
}

pub fn (a &RangeAssignor) name() string {
    return 'range'
}

pub fn (a &RangeAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
    mut assignments := map[string][]TopicPartition{}
    
    // Initialize empty assignments for all members
    for m in members {
        assignments[m.member_id] = []TopicPartition{}
    }
    
    // For each topic, assign partitions to subscribed members
    for topic_name, topic_meta in topics {
        // Get members subscribed to this topic
        mut subscribed_members := []string{}
        for m in members {
            if topic_name in m.topics {
                subscribed_members << m.member_id
            }
        }
        
        if subscribed_members.len == 0 {
            continue
        }
        
        // Sort members for consistent assignment
        subscribed_members.sort()
        
        // Range assignment: divide partitions evenly
        num_partitions := topic_meta.partition_count
        num_members := subscribed_members.len
        partitions_per_member := num_partitions / num_members
        extra_partitions := num_partitions % num_members
        
        mut partition_idx := 0
        for i, member_id in subscribed_members {
            // Members with lower indices get one extra partition
            count := partitions_per_member + if i < extra_partitions { 1 } else { 0 }
            
            for _ in 0 .. count {
                assignments[member_id] << TopicPartition{
                    topic_id: topic_meta.topic_id
                    topic_name: topic_name
                    partition: partition_idx
                }
                partition_idx++
            }
        }
    }
    
    return assignments
}

// ============================================================================
// Round Robin Assignor
// ============================================================================

pub struct RoundRobinAssignor {}

pub fn new_round_robin_assignor() &RoundRobinAssignor {
    return &RoundRobinAssignor{}
}

pub fn (a &RoundRobinAssignor) name() string {
    return 'roundrobin'
}

pub fn (a &RoundRobinAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
    mut assignments := map[string][]TopicPartition{}
    
    // Initialize empty assignments
    for m in members {
        assignments[m.member_id] = []TopicPartition{}
    }
    
    if members.len == 0 {
        return assignments
    }
    
    // Collect all partitions from all topics
    mut all_partitions := []TopicPartition{}
    for topic_name, topic_meta in topics {
        for p in 0 .. topic_meta.partition_count {
            all_partitions << TopicPartition{
                topic_id: topic_meta.topic_id
                topic_name: topic_name
                partition: p
            }
        }
    }
    
    // Sort partitions for consistent ordering
    all_partitions.sort(a.topic_name < b.topic_name)
    
    // Get sorted member list
    mut member_ids := []string{}
    for m in members {
        member_ids << m.member_id
    }
    member_ids.sort()
    
    // Round-robin assign
    for i, tp in all_partitions {
        // Check if any member is subscribed to this topic
        mut assigned := false
        for j in 0 .. member_ids.len {
            member_idx := (i + j) % member_ids.len
            member_id := member_ids[member_idx]
            
            // Find member subscription
            for m in members {
                if m.member_id == member_id && tp.topic_name in m.topics {
                    assignments[member_id] << tp
                    assigned = true
                    break
                }
            }
            if assigned {
                break
            }
        }
    }
    
    return assignments
}

// ============================================================================
// Sticky Assignor
// Implements Apache Kafka's StickyAssignor algorithm
// Goals: 1) Balanced assignment 2) Minimize partition movements
// ============================================================================

pub struct StickyAssignor {
    // Configuration
    allow_overlap bool  // Whether to allow unbalanced assignments temporarily
}

pub fn new_sticky_assignor() &StickyAssignor {
    return &StickyAssignor{
        allow_overlap: false
    }
}

pub fn (a &StickyAssignor) name() string {
    return 'sticky'
}

pub fn (a &StickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
    if members.len == 0 {
        return map[string][]TopicPartition{}
    }
    
    // Step 1: Build partition to member mapping from current assignments
    mut current_partition_owners := map[string]string{}  // partition_key -> member_id
    mut member_assignments := map[string][]TopicPartition{}
    
    for m in members {
        member_assignments[m.member_id] = []TopicPartition{}
        for tp in m.owned_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            current_partition_owners[key] = m.member_id
        }
    }
    
    // Step 2: Calculate total partitions and target assignment size
    mut total_partitions := 0
    for _, meta in topics {
        total_partitions += meta.partition_count
    }
    
    min_partitions := total_partitions / members.len
    max_partitions := min_partitions + if total_partitions % members.len > 0 { 1 } else { 0 }
    
    // Step 3: Build subscription map (topic -> subscribed members)
    mut topic_subscribers := map[string][]string{}
    for topic_name, _ in topics {
        topic_subscribers[topic_name] = []string{}
        for m in members {
            if topic_name in m.topics {
                topic_subscribers[topic_name] << m.member_id
            }
        }
    }
    
    // Step 4: Keep valid current assignments (sticky part)
    mut assigned_partitions := map[string]bool{}
    
    for m in members {
        for tp in m.owned_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            
            // Keep if: topic still exists, partition valid, member still subscribed
            if meta := topics[tp.topic_name] {
                if tp.partition < meta.partition_count && tp.topic_name in m.topics {
                    // Check if keeping this would exceed max_partitions
                    if member_assignments[m.member_id].len < max_partitions {
                        member_assignments[m.member_id] << tp
                        assigned_partitions[key] = true
                    }
                }
            }
        }
    }
    
    // Step 5: Collect unassigned partitions
    mut unassigned := []TopicPartition{}
    for topic_name, meta in topics {
        for p in 0 .. meta.partition_count {
            key := '${topic_name}:${p}'
            if key !in assigned_partitions {
                unassigned << TopicPartition{
                    topic_id: meta.topic_id
                    topic_name: topic_name
                    partition: p
                }
            }
        }
    }
    
    // Step 6: Sort unassigned partitions for deterministic assignment
    unassigned.sort(a.partition < b.partition)
    
    // Step 7: Assign unassigned partitions to least-loaded eligible members
    for tp in unassigned {
        subscribers := topic_subscribers[tp.topic_name]
        if subscribers.len == 0 {
            continue
        }
        
        // Find member with minimum assignment count
        mut best_member := ''
        mut min_count := -1
        
        for member_id in subscribers {
            count := member_assignments[member_id].len
            if min_count < 0 || count < min_count {
                min_count = count
                best_member = member_id
            }
        }
        
        if best_member.len > 0 {
            member_assignments[best_member] << tp
        }
    }
    
    // Step 8: Rebalance if needed (move partitions from overloaded members)
    a.rebalance(mut member_assignments, topic_subscribers, min_partitions, max_partitions)
    
    return member_assignments
}

// rebalance moves partitions from overloaded members to underloaded ones
fn (a &StickyAssignor) rebalance(mut assignments map[string][]TopicPartition, topic_subscribers map[string][]string, min_partitions int, max_partitions int) {
    // Find overloaded and underloaded members
    mut overloaded := []string{}
    mut underloaded := []string{}
    
    for member_id, partitions in assignments {
        if partitions.len > max_partitions {
            overloaded << member_id
        } else if partitions.len < min_partitions {
            underloaded << member_id
        }
    }
    
    // Move partitions from overloaded to underloaded
    for src_member in overloaded {
        for assignments[src_member].len > max_partitions && underloaded.len > 0 {
            // Find a partition that can be moved
            mut moved := false
            
            for i := assignments[src_member].len - 1; i >= 0 && !moved; i-- {
                tp := assignments[src_member][i]
                subscribers := topic_subscribers[tp.topic_name]
                
                // Find an underloaded member subscribed to this topic
                for j, dst_member in underloaded {
                    if dst_member in subscribers && assignments[dst_member].len < min_partitions {
                        // Move partition
                        assignments[dst_member] << tp
                        assignments[src_member].delete(i)
                        moved = true
                        
                        // Check if dst_member is still underloaded
                        if assignments[dst_member].len >= min_partitions {
                            underloaded.delete(j)
                        }
                        break
                    }
                }
            }
            
            if !moved {
                break  // Can't move any more partitions
            }
        }
    }
}

// ============================================================================
// Cooperative Sticky Assignor
// Implements incremental rebalancing (COOPERATIVE protocol)
// Only revokes partitions that need to move, not all partitions
// ============================================================================

pub struct CooperativeStickyAssignor {
    sticky &StickyAssignor
}

pub fn new_cooperative_sticky_assignor() &CooperativeStickyAssignor {
    return &CooperativeStickyAssignor{
        sticky: new_sticky_assignor()
    }
}

pub fn (a &CooperativeStickyAssignor) name() string {
    return 'cooperative-sticky'
}

pub fn (a &CooperativeStickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
    // Cooperative sticky uses the same algorithm as sticky
    // The difference is in the rebalancing protocol (handled by coordinator)
    return a.sticky.assign(members, topics)
}

// compute_revocations determines which partitions need to be revoked
// This is used for incremental rebalancing
pub fn (a &CooperativeStickyAssignor) compute_revocations(current map[string][]TopicPartition, target map[string][]TopicPartition) map[string][]TopicPartition {
    mut revocations := map[string][]TopicPartition{}
    
    for member_id, current_partitions in current {
        target_partitions := target[member_id] or { []TopicPartition{} }
        
        // Build set of target partition keys
        mut target_keys := map[string]bool{}
        for tp in target_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            target_keys[key] = true
        }
        
        // Find partitions to revoke
        for tp in current_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            if key !in target_keys {
                if member_id !in revocations {
                    revocations[member_id] = []TopicPartition{}
                }
                revocations[member_id] << tp
            }
        }
    }
    
    return revocations
}

// compute_additions determines which partitions need to be added
pub fn (a &CooperativeStickyAssignor) compute_additions(current map[string][]TopicPartition, target map[string][]TopicPartition) map[string][]TopicPartition {
    mut additions := map[string][]TopicPartition{}
    
    for member_id, target_partitions in target {
        current_partitions := current[member_id] or { []TopicPartition{} }
        
        // Build set of current partition keys
        mut current_keys := map[string]bool{}
        for tp in current_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            current_keys[key] = true
        }
        
        // Find partitions to add
        for tp in target_partitions {
            key := '${tp.topic_name}:${tp.partition}'
            if key !in current_keys {
                if member_id !in additions {
                    additions[member_id] = []TopicPartition{}
                }
                additions[member_id] << tp
            }
        }
    }
    
    return additions
}

// ============================================================================
// Uniform Assignor (KIP-848)
// Server-side assignor for the new consumer protocol
// ============================================================================

pub struct UniformAssignor {}

pub fn new_uniform_assignor() &UniformAssignor {
    return &UniformAssignor{}
}

pub fn (a &UniformAssignor) name() string {
    return 'uniform'
}

pub fn (a &UniformAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
    if members.len == 0 {
        return map[string][]TopicPartition{}
    }
    
    mut assignments := map[string][]TopicPartition{}
    for m in members {
        assignments[m.member_id] = []TopicPartition{}
    }
    
    // Calculate target assignment sizes
    for _, meta in topics {
    }
    
    // Build rack-aware assignment if rack info available
    mut rack_members := map[string][]string{}  // rack_id -> member_ids
    mut no_rack_members := []string{}
    
    for m in members {
        if rack := m.rack_id {
            if rack !in rack_members {
                rack_members[rack] = []string{}
            }
            rack_members[rack] << m.member_id
        } else {
            no_rack_members << m.member_id
        }
    }
    
    // Collect all partitions
    mut all_partitions := []TopicPartition{}
    for topic_name, meta in topics {
        for p in 0 .. meta.partition_count {
            all_partitions << TopicPartition{
                topic_id: meta.topic_id
                topic_name: topic_name
                partition: p
            }
        }
    }
    
    // Sort for deterministic assignment
    all_partitions.sort(a.partition < b.partition)
    
    // Uniform assignment: distribute partitions evenly
    mut member_list := []string{}
    for m in members {
        member_list << m.member_id
    }
    member_list.sort()
    
    for i, tp in all_partitions {
        member_idx := i % member_list.len
        member_id := member_list[member_idx]
        
        // Check subscription
        for m in members {
            if m.member_id == member_id && tp.topic_name in m.topics {
                assignments[member_id] << tp
                break
            }
        }
    }
    
    return assignments
}

// ============================================================================
// KIP-848 Group Coordinator
// ============================================================================

// KIP848GroupCoordinator manages consumer groups using the new protocol
pub struct KIP848GroupCoordinator {
    assignors       map[string]ServerAssignor
    default_assignor string
mut:
    storage         port.StoragePort
    groups          map[string]&KIP848ConsumerGroup
    heartbeat_interval_ms i32
    session_timeout_ms    i32
    rebalance_timeout_ms  i32
}

// new_kip848_coordinator creates a new KIP-848 group coordinator
pub fn new_kip848_coordinator(storage port.StoragePort) &KIP848GroupCoordinator {
    mut assignors := map[string]ServerAssignor{}
    assignors['range'] = new_range_assignor()
    assignors['roundrobin'] = new_round_robin_assignor()
    assignors['sticky'] = new_sticky_assignor()
    assignors['cooperative-sticky'] = new_cooperative_sticky_assignor()
    assignors['uniform'] = new_uniform_assignor()
    
    return &KIP848GroupCoordinator{
        storage: storage
        assignors: assignors
        default_assignor: 'sticky'  // Sticky is now the default
        groups: map[string]&KIP848ConsumerGroup{}
        heartbeat_interval_ms: 3000
        session_timeout_ms: 45000
        rebalance_timeout_ms: 300000
    }
}

// HeartbeatResult represents the result of a heartbeat operation
pub struct HeartbeatResult {
pub:
    error_code            i16
    error_message         ?string
    member_id             string
    member_epoch          i32
    heartbeat_interval_ms i32
    assignment            ?[]TopicPartition
}

// process_heartbeat handles a ConsumerGroupHeartbeat request
pub fn (mut c KIP848GroupCoordinator) process_heartbeat(
    group_id string,
    member_id string,
    member_epoch i32,
    instance_id ?string,
    rack_id ?string,
    rebalance_timeout_ms i32,
    subscribed_topic_names []string,
    server_assignor ?string
) !HeartbeatResult {
    now := time.now().unix_milli()
    
    // Validate group_id
    if group_id.len == 0 {
        return HeartbeatResult{
            error_code: 24  // INVALID_GROUP_ID
            error_message: 'Group ID cannot be empty'
            member_epoch: -1
        }
    }
    
    // Get or create group
    mut group := c.get_or_create_group(group_id)
    
    // Handle based on member_epoch
    if member_epoch == 0 && member_id.len == 0 {
        // New member joining
        return c.handle_join(mut group, instance_id, rack_id, rebalance_timeout_ms, 
                            subscribed_topic_names, server_assignor, now)
    } else if member_epoch == -1 {
        // Member leaving
        return c.handle_leave(mut group, member_id, now)
    } else if member_epoch > 0 {
        // Regular heartbeat
        return c.handle_heartbeat(mut group, member_id, member_epoch, 
                                 subscribed_topic_names, now)
    } else {
        // Invalid epoch
        return HeartbeatResult{
            error_code: 25  // UNKNOWN_MEMBER_ID
            error_message: 'Invalid member epoch'
            member_epoch: -1
        }
    }
}

// get_or_create_group returns existing group or creates new one
fn (mut c KIP848GroupCoordinator) get_or_create_group(group_id string) &KIP848ConsumerGroup {
    if group_id in c.groups {
        return c.groups[group_id] or { panic('unreachable') }
    }
    
    now := time.now().unix_milli()
    group := &KIP848ConsumerGroup{
        group_id: group_id
        group_epoch: 0
        assignment_epoch: 0
        state: .empty
        protocol_type: 'consumer'
        server_assignor: c.default_assignor
        members: map[string]&KIP848Member{}
        target_assignment: map[string][]TopicPartition{}
        current_assignment: map[string][]TopicPartition{}
        subscribed_topics: map[string]bool{}
        created_at: now
        updated_at: now
    }
    c.groups[group_id] = group
    return group
}

// handle_join processes a new member joining
fn (mut c KIP848GroupCoordinator) handle_join(
    mut group KIP848ConsumerGroup,
    instance_id ?string,
    rack_id ?string,
    rebalance_timeout_ms i32,
    subscribed_topic_names []string,
    server_assignor ?string,
    now i64
) !HeartbeatResult {
    // Generate new member_id
    member_id := 'consumer-${group.group_id}-${rand.i64()}'
    
    // Check for static member (instance_id)
    if inst_id := instance_id {
        // Static member - check for existing member with same instance_id
        for member_key, _ in group.members {
            if mut existing_member := group.members[member_key] {
                if m_inst := existing_member.instance_id {
                    if m_inst == inst_id {
                        // Fence the old member and reuse
                        existing_member.state = .fenced
                    }
                }
            }
        }
    }
    
    // Create new member
    member := &KIP848Member{
        member_id: member_id
        instance_id: instance_id
        rack_id: rack_id
        subscribed_topic_names: subscribed_topic_names
        server_assignor: server_assignor
        member_epoch: 1
        previous_member_epoch: 0
        state: .subscribing
        assigned_partitions: []TopicPartition{}
        pending_partitions: []TopicPartition{}
        revoking_partitions: []TopicPartition{}
        rebalance_timeout_ms: rebalance_timeout_ms
        session_timeout_ms: c.session_timeout_ms
        last_heartbeat: now
        joined_at: now
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
    
    // Trigger rebalance
    group.group_epoch++
    group.state = .assigning
    group.updated_at = now
    
    // Compute new assignment
    c.compute_assignment(mut group) or {
        return HeartbeatResult{
            error_code: -1  // UNKNOWN_SERVER_ERROR
            error_message: 'Failed to compute assignment: ${err.msg()}'
            member_id: member_id
            member_epoch: -1
        }
    }
    
    // Get assignment for this member
    assignment := group.target_assignment[member_id] or { []TopicPartition{} }
    
    return HeartbeatResult{
        error_code: 0
        member_id: member_id
        member_epoch: 1
        heartbeat_interval_ms: c.heartbeat_interval_ms
        assignment: assignment
    }
}

// handle_leave processes a member leaving
fn (mut c KIP848GroupCoordinator) handle_leave(
    mut group KIP848ConsumerGroup,
    member_id string,
    now i64
) !HeartbeatResult {
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
        // Trigger rebalance
        group.group_epoch++
        group.state = .assigning
        c.compute_assignment(mut group)!
    }
    
    group.updated_at = now
    
    return HeartbeatResult{
        error_code: 0
        member_id: member_id
        member_epoch: -1
        heartbeat_interval_ms: 0
    }
}

// handle_heartbeat processes a regular heartbeat
fn (mut c KIP848GroupCoordinator) handle_heartbeat(
    mut group KIP848ConsumerGroup,
    member_id string,
    member_epoch i32,
    subscribed_topic_names []string,
    now i64
) !HeartbeatResult {
    // Find member
    mut member := group.members[member_id] or {
        return HeartbeatResult{
            error_code: 25  // UNKNOWN_MEMBER_ID
            error_message: 'Unknown member ID'
            member_epoch: -1
        }
    }
    
    // Check epoch
    if member_epoch < member.member_epoch {
        return HeartbeatResult{
            error_code: 82  // FENCED_INSTANCE_ID
            error_message: 'Member epoch is stale'
            member_id: member_id
            member_epoch: member.member_epoch
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
        
        // Trigger rebalance
        group.group_epoch++
        group.state = .assigning
        c.compute_assignment(mut group)!
    }
    
    // Get current assignment
    assignment := group.target_assignment[member_id] or { []TopicPartition{} }
    
    // Check if assignment changed
    if assignment.len != member.assigned_partitions.len {
        member.state = .reconciling
    } else {
        member.state = .stable
    }
    
    return HeartbeatResult{
        error_code: 0
        member_id: member_id
        member_epoch: member.member_epoch
        heartbeat_interval_ms: c.heartbeat_interval_ms
        assignment: assignment
    }
}

// compute_assignment computes new partition assignments for all members
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
            member_id: member_id
            instance_id: member.instance_id
            rack_id: member.rack_id
            topics: member.subscribed_topic_names
            owned_partitions: member.assigned_partitions
        }
    }
    
    // Build topic metadata
    mut topics := map[string]TopicMetadata{}
    for topic_name, _ in group.subscribed_topics {
        topic_meta := c.storage.get_topic(topic_name) or { continue }
        topics[topic_name] = TopicMetadata{
            topic_id: topic_meta.topic_id
            topic_name: topic_name
            partition_count: topic_meta.partition_count
        }
    }
    
    // Get assignor
    assignor := c.assignors[group.server_assignor] or { c.assignors[c.default_assignor] or { return error('No assignor available') } }
    
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

// get_group returns a group by ID
pub fn (c &KIP848GroupCoordinator) get_group(group_id string) ?&KIP848ConsumerGroup {
    return c.groups[group_id] or { return none }
}

// list_groups returns all groups
pub fn (c &KIP848GroupCoordinator) list_groups() []&KIP848ConsumerGroup {
    mut result := []&KIP848ConsumerGroup{}
    for _, g in c.groups {
        result << g
    }
    return result
}

// expire_members removes members that haven't sent heartbeats
pub fn (mut c KIP848GroupCoordinator) expire_members() {
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
                c.compute_assignment(mut group) or {}
            }
        }
        
        // Remove empty groups after timeout
        if group.state == .empty && now - group.updated_at > 300000 {
            c.groups.delete(group_id)
        }
    }
}
