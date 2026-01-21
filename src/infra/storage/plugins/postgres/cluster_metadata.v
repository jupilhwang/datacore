// Infra Layer - PostgreSQL Cluster Metadata Implementation
// Implements ClusterMetadataPort for multi-broker coordination
module postgres

import db.pg
import domain
import time
import sync

// PostgresClusterMetadataPort implements port.ClusterMetadataPort
pub struct PostgresClusterMetadataPort {
mut:
	pool       &pg.ConnectionPool
	cluster_id string
	lock       sync.RwMutex
}

// new_cluster_metadata_port creates a new PostgreSQL cluster metadata port
pub fn new_cluster_metadata_port(pool &pg.ConnectionPool, cluster_id string) !&PostgresClusterMetadataPort {
	mut cmp := &PostgresClusterMetadataPort{
		pool:       pool
		cluster_id: cluster_id
	}

	cmp.init_cluster_schema()!
	return cmp
}

// init_cluster_schema initializes cluster-related tables
fn (mut p PostgresClusterMetadataPort) init_cluster_schema() ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	// Cluster metadata table
	db.exec('
		CREATE TABLE IF NOT EXISTS cluster_metadata (
			cluster_id VARCHAR(255) PRIMARY KEY,
			controller_id INT DEFAULT -1,
			metadata_version BIGINT DEFAULT 0,
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	')!

	// Brokers table
	db.exec("
		CREATE TABLE IF NOT EXISTS brokers (
			broker_id INT PRIMARY KEY,
			host VARCHAR(255) NOT NULL,
			port INT NOT NULL,
			rack VARCHAR(255) DEFAULT '',
			security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',
			status VARCHAR(50) NOT NULL DEFAULT 'starting',
			version VARCHAR(50) DEFAULT '',
			registered_at BIGINT NOT NULL,
			last_heartbeat BIGINT NOT NULL
		)
	")!

	// Broker endpoints table
	db.exec("
		CREATE TABLE IF NOT EXISTS broker_endpoints (
			broker_id INT NOT NULL REFERENCES brokers(broker_id) ON DELETE CASCADE,
			name VARCHAR(50) NOT NULL,
			host VARCHAR(255) NOT NULL,
			port INT NOT NULL,
			security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',
			PRIMARY KEY (broker_id, name)
		)
	")!

	// Partition assignments table
	db.exec("
		CREATE TABLE IF NOT EXISTS partition_assignments (
			topic_name VARCHAR(255) NOT NULL,
			topic_id BYTEA,
			partition_id INT NOT NULL,
			preferred_broker INT DEFAULT -1,
			replica_brokers INT[] DEFAULT '{}',
			isr_brokers INT[] DEFAULT '{}',
			partition_epoch INT DEFAULT 0,
			PRIMARY KEY (topic_name, partition_id)
		)
	")!

	// Distributed locks table
	db.exec('
		CREATE TABLE IF NOT EXISTS distributed_locks (
			lock_name VARCHAR(255) PRIMARY KEY,
			holder_id VARCHAR(255) NOT NULL,
			acquired_at TIMESTAMPTZ DEFAULT NOW(),
			expires_at TIMESTAMPTZ NOT NULL
		)
	')!

	// Initialize cluster metadata if not exists
	db.exec_param_many('
		INSERT INTO cluster_metadata (cluster_id, controller_id, metadata_version)
		VALUES (\$1, -1, 0)
		ON CONFLICT (cluster_id) DO NOTHING
	',
		[p.cluster_id])!

	// Create indexes
	db.exec('CREATE INDEX IF NOT EXISTS idx_brokers_status ON brokers(status)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_partition_assignments_topic ON partition_assignments(topic_name)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_distributed_locks_expires ON distributed_locks(expires_at)')!
}

// ============================================================
// Broker Registration
// ============================================================

pub fn (mut p PostgresClusterMetadataPort) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	now := time.now().unix_milli()
	status_str := broker_status_to_string(info.status)

	db.begin()!

	// Insert or update broker
	db.exec_param_many('
		INSERT INTO brokers (broker_id, host, port, rack, security_protocol, status, version, registered_at, last_heartbeat)
		VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9)
		ON CONFLICT (broker_id) DO UPDATE SET
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			rack = EXCLUDED.rack,
			security_protocol = EXCLUDED.security_protocol,
			status = EXCLUDED.status,
			version = EXCLUDED.version,
			last_heartbeat = EXCLUDED.last_heartbeat
	',
		[
		info.broker_id.str(),
		info.host,
		info.port.str(),
		info.rack,
		info.security_protocol,
		status_str,
		info.version,
		now.str(),
		now.str(),
	])!

	// Insert endpoints
	for endpoint in info.endpoints {
		db.exec_param_many('
			INSERT INTO broker_endpoints (broker_id, name, host, port, security_protocol)
			VALUES (\$1, \$2, \$3, \$4, \$5)
			ON CONFLICT (broker_id, name) DO UPDATE SET
				host = EXCLUDED.host,
				port = EXCLUDED.port,
				security_protocol = EXCLUDED.security_protocol
		',
			[
			info.broker_id.str(),
			endpoint.name,
			endpoint.host,
			endpoint.port.str(),
			endpoint.security_protocol,
		])!
	}

	// Increment metadata version
	db.exec_param('
		UPDATE cluster_metadata SET metadata_version = metadata_version + 1, updated_at = NOW()
		WHERE cluster_id = \$1
	',
		p.cluster_id)!

	db.commit()!

	return domain.BrokerInfo{
		...info
		registered_at:  now
		last_heartbeat: now
	}
}

pub fn (mut p PostgresClusterMetadataPort) deregister_broker(broker_id i32) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.begin()!

	db.exec_param('DELETE FROM broker_endpoints WHERE broker_id = $1', broker_id.str())!
	db.exec_param('DELETE FROM brokers WHERE broker_id = $1', broker_id.str())!

	// Update metadata version
	db.exec_param('
		UPDATE cluster_metadata SET metadata_version = metadata_version + 1, updated_at = NOW()
		WHERE cluster_id = \$1
	',
		p.cluster_id)!

	db.commit()!
}

pub fn (mut p PostgresClusterMetadataPort) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	status := if heartbeat.wants_shutdown { 'draining' } else { 'active' }

	db.exec_param_many('
		UPDATE brokers SET last_heartbeat = \$1, status = \$2 WHERE broker_id = \$3
	',
		[heartbeat.timestamp.str(), status, heartbeat.broker_id.str()])!
}

pub fn (mut p PostgresClusterMetadataPort) get_broker(broker_id i32) !domain.BrokerInfo {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec_param('
		SELECT broker_id, host, port, rack, security_protocol, status, version, registered_at, last_heartbeat
		FROM brokers WHERE broker_id = \$1
	',
		broker_id.str())!

	if rows.len == 0 {
		return error('broker not found')
	}

	row := rows[0]
	mut info := domain.BrokerInfo{
		broker_id:         i32(get_row_int(&row, 0, 0))
		host:              get_row_str(&row, 1, '')
		port:              i32(get_row_int(&row, 2, 0))
		rack:              get_row_str(&row, 3, '')
		security_protocol: get_row_str(&row, 4, 'PLAINTEXT')
		status:            string_to_broker_status(get_row_str(&row, 5, 'starting'))
		version:           get_row_str(&row, 6, '')
		registered_at:     get_row_i64(&row, 7, 0)
		last_heartbeat:    get_row_i64(&row, 8, 0)
		endpoints:         []domain.BrokerEndpoint{}
	}

	// Get endpoints
	endpoint_rows := db.exec_param('
		SELECT name, host, port, security_protocol FROM broker_endpoints WHERE broker_id = \$1
	',
		broker_id.str())!

	for ep_row in endpoint_rows {
		info.endpoints << domain.BrokerEndpoint{
			name:              get_row_str(&ep_row, 0, '')
			host:              get_row_str(&ep_row, 1, '')
			port:              i32(get_row_int(&ep_row, 2, 0))
			security_protocol: get_row_str(&ep_row, 3, 'PLAINTEXT')
		}
	}

	return info
}

pub fn (mut p PostgresClusterMetadataPort) list_brokers() ![]domain.BrokerInfo {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec('
		SELECT broker_id, host, port, rack, security_protocol, status, version, registered_at, last_heartbeat
		FROM brokers ORDER BY broker_id
	')!

	mut brokers := []domain.BrokerInfo{}
	for row in rows {
		brokers << domain.BrokerInfo{
			broker_id:         i32(get_row_int(&row, 0, 0))
			host:              get_row_str(&row, 1, '')
			port:              i32(get_row_int(&row, 2, 0))
			rack:              get_row_str(&row, 3, '')
			security_protocol: get_row_str(&row, 4, 'PLAINTEXT')
			status:            string_to_broker_status(get_row_str(&row, 5, 'starting'))
			version:           get_row_str(&row, 6, '')
			registered_at:     get_row_i64(&row, 7, 0)
			last_heartbeat:    get_row_i64(&row, 8, 0)
			endpoints:         []domain.BrokerEndpoint{}
		}
	}

	return brokers
}

pub fn (mut p PostgresClusterMetadataPort) list_active_brokers() ![]domain.BrokerInfo {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec("
		SELECT broker_id, host, port, rack, security_protocol, status, version, registered_at, last_heartbeat
		FROM brokers WHERE status IN ('active', 'starting') ORDER BY broker_id
	")!

	mut brokers := []domain.BrokerInfo{}
	for row in rows {
		brokers << domain.BrokerInfo{
			broker_id:         i32(get_row_int(&row, 0, 0))
			host:              get_row_str(&row, 1, '')
			port:              i32(get_row_int(&row, 2, 0))
			rack:              get_row_str(&row, 3, '')
			security_protocol: get_row_str(&row, 4, 'PLAINTEXT')
			status:            string_to_broker_status(get_row_str(&row, 5, 'starting'))
			version:           get_row_str(&row, 6, '')
			registered_at:     get_row_i64(&row, 7, 0)
			last_heartbeat:    get_row_i64(&row, 8, 0)
			endpoints:         []domain.BrokerEndpoint{}
		}
	}

	return brokers
}

// ============================================================
// Cluster Metadata
// ============================================================

pub fn (mut p PostgresClusterMetadataPort) get_cluster_metadata() !domain.ClusterMetadata {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec_param('
		SELECT cluster_id, controller_id, metadata_version, EXTRACT(EPOCH FROM updated_at)::BIGINT * 1000
		FROM cluster_metadata WHERE cluster_id = \$1
	',
		p.cluster_id)!

	if rows.len == 0 {
		return error('cluster metadata not found')
	}

	row := rows[0]
	brokers := p.list_brokers()!

	return domain.ClusterMetadata{
		cluster_id:       get_row_str(&row, 0, p.cluster_id)
		controller_id:    i32(get_row_int(&row, 1, -1))
		brokers:          brokers
		metadata_version: get_row_i64(&row, 2, 0)
		updated_at:       get_row_i64(&row, 3, 0)
	}
}

pub fn (mut p PostgresClusterMetadataPort) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.begin()!

	// Optimistic locking: check version before update
	rows := db.exec_param('
		SELECT metadata_version FROM cluster_metadata WHERE cluster_id = \$1 FOR UPDATE
	',
		p.cluster_id)!

	if rows.len == 0 {
		db.rollback()!
		return error('cluster metadata not found')
	}

	current_version := get_row_i64(&rows[0], 0, 0)
	if current_version != metadata.metadata_version {
		db.rollback()!
		return error('version mismatch: concurrent modification detected')
	}

	db.exec_param_many('
		UPDATE cluster_metadata SET controller_id = \$1, metadata_version = \$2, updated_at = NOW()
		WHERE cluster_id = \$3
	',
		[metadata.controller_id.str(), (metadata.metadata_version + 1).str(), p.cluster_id])!

	db.commit()!
}

// ============================================================
// Partition Assignment
// ============================================================

pub fn (mut p PostgresClusterMetadataPort) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec_param_many('
		SELECT topic_name, topic_id, partition_id, preferred_broker, replica_brokers, isr_brokers, partition_epoch
		FROM partition_assignments WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		return error('partition assignment not found')
	}

	row := rows[0]
	return domain.PartitionAssignment{
		topic_name:       get_row_str(&row, 0, '')
		topic_id:         []u8{} // TODO: decode topic_id
		partition:        i32(get_row_int(&row, 2, 0))
		preferred_broker: i32(get_row_int(&row, 3, -1))
		replica_brokers:  []i32{} // TODO: parse array
		isr_brokers:      []i32{} // TODO: parse array
		partition_epoch:  i32(get_row_int(&row, 6, 0))
	}
}

pub fn (mut p PostgresClusterMetadataPort) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec_param('
		SELECT topic_name, topic_id, partition_id, preferred_broker, partition_epoch
		FROM partition_assignments WHERE topic_name = \$1 ORDER BY partition_id
	',
		topic_name)!

	mut assignments := []domain.PartitionAssignment{}
	for row in rows {
		assignments << domain.PartitionAssignment{
			topic_name:       get_row_str(&row, 0, '')
			topic_id:         []u8{}
			partition:        i32(get_row_int(&row, 2, 0))
			preferred_broker: i32(get_row_int(&row, 3, -1))
			replica_brokers:  []i32{}
			isr_brokers:      []i32{}
			partition_epoch:  i32(get_row_int(&row, 4, 0))
		}
	}

	return assignments
}

pub fn (mut p PostgresClusterMetadataPort) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.exec_param_many('
		INSERT INTO partition_assignments (topic_name, partition_id, preferred_broker, partition_epoch)
		VALUES (\$1, \$2, \$3, \$4)
		ON CONFLICT (topic_name, partition_id) DO UPDATE SET
			preferred_broker = EXCLUDED.preferred_broker,
			partition_epoch = EXCLUDED.partition_epoch
	',
		[
		assignment.topic_name,
		assignment.partition.str(),
		assignment.preferred_broker.str(),
		assignment.partition_epoch.str(),
	])!
}

// ============================================================
// Distributed Locking
// ============================================================

pub fn (mut p PostgresClusterMetadataPort) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	now := time.now()
	expires_at := now.add(time.Duration(ttl_ms) * time.millisecond)

	db.begin()!

	// Clean expired locks
	db.exec('DELETE FROM distributed_locks WHERE expires_at < NOW()')!

	// Try to acquire lock
	rows := db.exec_param_many('
		INSERT INTO distributed_locks (lock_name, holder_id, expires_at)
		VALUES (\$1, \$2, \$3)
		ON CONFLICT (lock_name) DO UPDATE SET
			holder_id = EXCLUDED.holder_id,
			expires_at = EXCLUDED.expires_at,
			acquired_at = NOW()
		WHERE distributed_locks.holder_id = EXCLUDED.holder_id OR distributed_locks.expires_at < NOW()
		RETURNING lock_name
	',
		[lock_name, holder_id, expires_at.format_rfc3339()])!

	db.commit()!

	return rows.len > 0
}

pub fn (mut p PostgresClusterMetadataPort) release_lock(lock_name string, holder_id string) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.exec_param_many('
		DELETE FROM distributed_locks WHERE lock_name = \$1 AND holder_id = \$2
	',
		[lock_name, holder_id])!
}

pub fn (mut p PostgresClusterMetadataPort) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	now := time.now()
	expires_at := now.add(time.Duration(ttl_ms) * time.millisecond)

	rows := db.exec_param_many('
		UPDATE distributed_locks SET expires_at = \$1
		WHERE lock_name = \$2 AND holder_id = \$3
		RETURNING lock_name
	',
		[expires_at.format_rfc3339(), lock_name, holder_id])!

	return rows.len > 0
}

// ============================================================
// Health Monitoring
// ============================================================

pub fn (mut p PostgresClusterMetadataPort) mark_broker_dead(broker_id i32) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.exec_param("UPDATE brokers SET status = 'dead' WHERE broker_id = \$1", broker_id.str())!
}

// ============================================================
// Capability
// ============================================================

pub fn (p &PostgresClusterMetadataPort) get_capability() domain.StorageCapability {
	return postgres_capability
}

// ============================================================
// Helper Functions
// ============================================================

fn broker_status_to_string(status domain.BrokerStatus) string {
	return match status {
		.starting { 'starting' }
		.active { 'active' }
		.draining { 'draining' }
		.shutdown { 'shutdown' }
		.dead { 'dead' }
	}
}

fn string_to_broker_status(s string) domain.BrokerStatus {
	return match s {
		'active' { domain.BrokerStatus.active }
		'draining' { domain.BrokerStatus.draining }
		'shutdown' { domain.BrokerStatus.shutdown }
		'dead' { domain.BrokerStatus.dead }
		else { domain.BrokerStatus.starting }
	}
}
