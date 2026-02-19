// Implemented in infra/replication/Manager (V duck typing).
module port

import domain

/// ReplicationPort is an interface for inter-broker data replication.
/// infra/replication/Manager implements this interface.
pub interface ReplicationPort {
mut:
	/// Starts the replication manager.
	start() !

	/// Stops the replication manager.
	stop() !

	/// Sends record data for a specific partition to replica brokers.
	/// topic: topic name
	/// partition: partition index
	/// offset: record offset
	/// records_data: serialized record bytes
	send_replicate(topic string, partition i32, offset i64, records_data []u8) !

	/// Notifies replica brokers that an S3 flush has completed.
	/// topic: topic name
	/// partition: partition index
	/// offset: last flushed offset
	send_flush_ack(topic string, partition i32, offset i64) !

	/// Stores replicated record data in the in-memory buffer.
	store_replica_buffer(buffer domain.ReplicaBuffer) !

	/// Deletes replica buffers at or below the specified offset.
	delete_replica_buffer(topic string, partition i32, offset i64) !

	/// Returns all in-memory replica buffers (used for crash recovery).
	get_all_replica_buffers() ![]domain.ReplicaBuffer

	/// Returns a snapshot of current replication statistics.
	get_stats() domain.ReplicationStats

	/// Updates the health status of a specific broker.
	update_broker_health(broker_id string, health domain.ReplicationHealth)
}
