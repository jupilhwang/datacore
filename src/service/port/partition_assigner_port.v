// Partition assigner interface following DIP.
// Abstracts partition leader lookup from the concrete PartitionAssigner.
module port

import domain

/// PartitionAssignerPort abstracts partition-to-broker leader resolution.
/// Implemented by service/cluster PartitionAssigner.
pub interface PartitionAssignerPort {
mut:
	/// Returns the broker ID of the leader for a given topic-partition.
	get_partition_leader(topic_name string, partition i32) !i32

	/// Assigns partitions for a topic across the given brokers.
	assign_partitions(topic_name string, partition_count int, brokers []domain.BrokerInfo) ![]domain.PartitionAssignment
}
