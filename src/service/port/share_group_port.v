// Share group coordinator interface following DIP (KIP-932).
// Abstracts share group lifecycle from the concrete ShareGroupCoordinator.
module port

import domain

/// ShareGroupHeartbeatRequest represents a share group heartbeat request.
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

/// ShareGroupHeartbeatResponse represents a share group heartbeat response.
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

/// ShareGroupCoordinatorPort abstracts share group coordination (KIP-932).
/// Implemented by service/group ShareGroupCoordinator.
pub interface ShareGroupCoordinatorPort {
mut:
	/// Processes a heartbeat from a share group member.
	heartbeat(req ShareGroupHeartbeatRequest) ShareGroupHeartbeatResponse

	/// Acquires records for a member from a topic-partition.
	acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int) []domain.AcquiredRecordInfo

	/// Acknowledges processed records.
	acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch) domain.ShareAcknowledgeResult

	/// Updates the share session by adding/removing partitions.
	update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession

	/// Gets or creates a share partition.
	get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition
}
