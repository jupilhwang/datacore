module domain

/// Topic은 Kafka 토픽을 나타냅니다.
/// name: 토픽 이름
/// partition_count: 파티션 수
/// replication_factor: 복제 팩터 (Stateless 모드에서는 항상 1)
/// config: 토픽 설정
/// is_internal: 내부 토픽 여부 (__consumer_offsets 등)
pub struct Topic {
pub:
	name               string
	partition_count    int
	replication_factor int = 1
	config             TopicConfig
	is_internal        bool
}

/// TopicConfig는 토픽 설정을 나타냅니다.
/// retention_ms: 메시지 보관 기간 (밀리초, 기본 7일)
/// retention_bytes: 최대 보관 크기 (-1 = 무제한)
/// segment_bytes: 세그먼트 파일 크기 (기본 1GB)
/// cleanup_policy: 정리 정책 ('delete' 또는 'compact')
/// min_insync_replicas: 최소 동기화 레플리카 수
/// max_message_bytes: 최대 메시지 크기
/// compression_type: 압축 타입 ('none', 'gzip', 'snappy', 'lz4', 'zstd')
pub struct TopicConfig {
pub:
	retention_ms        i64    = 604800000
	retention_bytes     i64    = -1
	segment_bytes       i64    = 1073741824
	cleanup_policy      string = 'delete'
	min_insync_replicas int    = 1
	max_message_bytes   int    = 1048576
	compression_type    string = 'none'
}

/// TopicMetadata는 API 응답을 위한 토픽 메타데이터를 나타냅니다.
/// name: 토픽 이름
/// topic_id: 토픽 UUID (16바이트)
/// partition_count: 파티션 수
/// config: 설정 맵
/// is_internal: 내부 토픽 여부
pub struct TopicMetadata {
pub:
	name            string
	topic_id        []u8 // UUID, 16바이트
	partition_count int
	config          map[string]string
	is_internal     bool
}
