// Infra Layer - PostgreSQL 클러스터 메타데이터 구현
// 멀티 브로커 조정을 위한 ClusterMetadataPort 구현
module postgres

import db.pg
import domain
import time
import sync

/// PostgresClusterMetadataPort는 port.ClusterMetadataPort를 구현합니다.
/// PostgreSQL을 사용하여 브로커 등록, 파티션 할당, 분산 락을 관리합니다.
pub struct PostgresClusterMetadataPort {
mut:
	pool       &pg.ConnectionPool
	cluster_id string
	lock       sync.RwMutex
}

/// new_cluster_metadata_port는 새로운 PostgreSQL 클러스터 메타데이터 포트를 생성합니다.
pub fn new_cluster_metadata_port(pool &pg.ConnectionPool, cluster_id string) !&PostgresClusterMetadataPort {
	mut cmp := &PostgresClusterMetadataPort{
		pool:       pool
		cluster_id: cluster_id
	}

	cmp.init_cluster_schema()!
	return cmp
}

/// init_cluster_schema는 클러스터 관련 테이블들을 초기화합니다.
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
// 브로커 등록
// ============================================================

/// register_broker는 브로커를 클러스터에 등록합니다.
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

/// deregister_broker는 브로커를 클러스터에서 등록 해제합니다.
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

/// update_broker_heartbeat는 브로커의 하트비트를 업데이트합니다.
pub fn (mut p PostgresClusterMetadataPort) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	status := if heartbeat.wants_shutdown { 'draining' } else { 'active' }

	db.exec_param_many('
		UPDATE brokers SET last_heartbeat = \$1, status = \$2 WHERE broker_id = \$3
	',
		[heartbeat.timestamp.str(), status, heartbeat.broker_id.str()])!
}

/// get_broker는 특정 브로커의 정보를 조회합니다.
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

/// list_brokers는 등록된 모든 브로커 목록을 조회합니다.
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

/// list_active_brokers는 활성 상태인 브로커 목록만 조회합니다.
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
// 클러스터 메타데이터
// ============================================================

/// get_cluster_metadata는 현재 클러스터 메타데이터를 조회합니다.
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

/// update_cluster_metadata는 클러스터 메타데이터를 업데이트합니다.
/// Optimistic locking을 사용하여 동시 수정을 감지합니다.
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
// 파티션 할당
// ============================================================

/// get_partition_assignment는 특정 토픽-파티션의 할당 정보를 조회합니다.
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
		topic_id:         parse_pg_bytea(get_row_str(&row, 1, ''))
		partition:        i32(get_row_int(&row, 2, 0))
		preferred_broker: i32(get_row_int(&row, 3, -1))
		replica_brokers:  parse_pg_int_array(get_row_str(&row, 4, '{}'))
		isr_brokers:      parse_pg_int_array(get_row_str(&row, 5, '{}'))
		partition_epoch:  i32(get_row_int(&row, 6, 0))
	}
}

/// list_partition_assignments는 특정 토픽의 모든 파티션 할당을 조회합니다.
pub fn (mut p PostgresClusterMetadataPort) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	rows := db.exec_param('
		SELECT topic_name, topic_id, partition_id, preferred_broker, replica_brokers, isr_brokers, partition_epoch
		FROM partition_assignments WHERE topic_name = \$1 ORDER BY partition_id
	',
		topic_name)!

	mut assignments := []domain.PartitionAssignment{}
	for row in rows {
		assignments << domain.PartitionAssignment{
			topic_name:       get_row_str(&row, 0, '')
			topic_id:         parse_pg_bytea(get_row_str(&row, 1, ''))
			partition:        i32(get_row_int(&row, 2, 0))
			preferred_broker: i32(get_row_int(&row, 3, -1))
			replica_brokers:  parse_pg_int_array(get_row_str(&row, 4, '{}'))
			isr_brokers:      parse_pg_int_array(get_row_str(&row, 5, '{}'))
			partition_epoch:  i32(get_row_int(&row, 6, 0))
		}
	}

	return assignments
}

/// update_partition_assignment는 파티션 할당을 업데이트합니다.
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
// 분산 락 (Distributed Locking)
// ============================================================

/// try_acquire_lock은 분산 락 획득을 시도합니다.
/// TTL이 만료되면 락은 자동으로 해제됩니다.
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

/// release_lock은 분산 락을 해제합니다.
pub fn (mut p PostgresClusterMetadataPort) release_lock(lock_name string, holder_id string) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.exec_param_many('
		DELETE FROM distributed_locks WHERE lock_name = \$1 AND holder_id = \$2
	',
		[lock_name, holder_id])!
}

/// refresh_lock은 분산 락의 TTL을 갱신합니다.
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
// 상태 모니터링 (Health Monitoring)
// ============================================================

/// mark_broker_dead는 브로커를 dead 상태로 표시합니다.
pub fn (mut p PostgresClusterMetadataPort) mark_broker_dead(broker_id i32) ! {
	mut db := p.pool.acquire()!
	defer { p.pool.release(db) }

	db.exec_param("UPDATE brokers SET status = 'dead' WHERE broker_id = \$1", broker_id.str())!
}

// ============================================================
// 스토리지 기능 (Capability)
// ============================================================

/// get_capability는 PostgreSQL 스토리지 기능 정보를 반환합니다.
pub fn (p &PostgresClusterMetadataPort) get_capability() domain.StorageCapability {
	return postgres_capability
}

// ============================================================
// 헬퍼 함수 (Helper Functions)
// ============================================================

/// broker_status_to_string은 BrokerStatus를 문자열로 변환합니다.
fn broker_status_to_string(status domain.BrokerStatus) string {
	return match status {
		.starting { 'starting' }
		.active { 'active' }
		.draining { 'draining' }
		.shutdown { 'shutdown' }
		.dead { 'dead' }
	}
}

/// string_to_broker_status는 문자열을 BrokerStatus로 변환합니다.
fn string_to_broker_status(s string) domain.BrokerStatus {
	return match s {
		'active' { domain.BrokerStatus.active }
		'draining' { domain.BrokerStatus.draining }
		'shutdown' { domain.BrokerStatus.shutdown }
		'dead' { domain.BrokerStatus.dead }
		else { domain.BrokerStatus.starting }
	}
}

/// parse_pg_int_array는 PostgreSQL int[] 배열 문자열을 []i32로 파싱합니다.
/// 형식: "{1,2,3}" 또는 빈 배열 "{}"
fn parse_pg_int_array(s string) []i32 {
	if s.len == 0 || s == '{}' || s == 'NULL' {
		return []i32{}
	}

	// 중괄호 제거
	trimmed := s.trim('{}')
	if trimmed.len == 0 {
		return []i32{}
	}

	mut result := []i32{}
	parts := trimmed.split(',')
	for part in parts {
		cleaned := part.trim_space()
		if cleaned.len > 0 {
			// V의 string.int()는 직접 int를 반환하므로 바로 사용
			val := cleaned.int()
			if val != 0 || cleaned == '0' {
				result << i32(val)
			}
		}
	}
	return result
}

/// parse_pg_bytea는 PostgreSQL bytea 문자열을 []u8로 파싱합니다.
/// 지원 형식:
/// - hex 형식: "\\x0102030405..." (PostgreSQL 기본 출력)
/// - escape 형식: "\\001\\002..." (레거시)
fn parse_pg_bytea(s string) []u8 {
	if s.len == 0 || s == 'NULL' {
		return []u8{}
	}

	// hex 형식 확인: \x 또는 \\x로 시작
	if s.starts_with('\\x') || s.starts_with(r'\x') {
		hex_str := if s.starts_with('\\x') {
			s[2..]
		} else {
			s[2..]
		}
		return parse_hex_string(hex_str)
	}

	// escape 형식 (레거시): \001\002...
	mut result := []u8{}
	mut i := 0
	for i < s.len {
		if s[i] == `\\` && i + 3 < s.len {
			// octal escape: \NNN
			octal := s[i + 1..i + 4]
			if is_octal(octal) {
				val := parse_octal(octal)
				result << u8(val)
				i += 4
				continue
			}
		}
		result << s[i]
		i += 1
	}
	return result
}

/// parse_hex_string은 16진수 문자열을 바이트 배열로 변환합니다.
fn parse_hex_string(s string) []u8 {
	if s.len == 0 || s.len % 2 != 0 {
		return []u8{}
	}

	mut result := []u8{cap: s.len / 2}
	mut i := 0
	for i < s.len {
		high := hex_char_to_nibble(s[i])
		low := hex_char_to_nibble(s[i + 1])
		if high >= 0 && low >= 0 {
			result << u8((high << 4) | low)
		}
		i += 2
	}
	return result
}

/// hex_char_to_nibble은 16진수 문자를 4비트 값으로 변환합니다.
fn hex_char_to_nibble(c u8) int {
	if c >= `0` && c <= `9` {
		return int(c - `0`)
	} else if c >= `a` && c <= `f` {
		return int(c - `a` + 10)
	} else if c >= `A` && c <= `F` {
		return int(c - `A` + 10)
	}
	return -1
}

/// is_octal은 문자열이 3자리 8진수인지 확인합니다.
fn is_octal(s string) bool {
	if s.len != 3 {
		return false
	}
	for c in s {
		if c < `0` || c > `7` {
			return false
		}
	}
	return true
}

/// parse_octal은 3자리 8진수 문자열을 정수로 변환합니다.
fn parse_octal(s string) int {
	mut result := 0
	for c in s {
		result = result * 8 + int(c - `0`)
	}
	return result
}
