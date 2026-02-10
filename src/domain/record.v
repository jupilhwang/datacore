// 도메인 레이어 - 레코드 도메인 모델
// Kafka 메시지의 핵심 데이터 구조를 정의합니다.
module domain

import time

/// Record는 Kafka의 단일 메시지를 나타냅니다.
/// key: 메시지 키 (파티셔닝에 사용)
/// value: 메시지 본문
/// headers: 메시지 헤더 (메타데이터)
/// timestamp: 메시지 타임스탬프
pub struct Record {
pub:
	key       []u8
	value     []u8
	headers   map[string][]u8
	timestamp time.Time
	// 원본 압축 타입 (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
	// 크로스 브로커 fetch 시 원본 압축 정보를 보존하기 위해 사용
	compression_type u8
	// 트랜잭션 제어 레코드 메타데이터
	// is_control_record가 true이면 이 레코드는 트랜잭션 마커(commit/abort)입니다.
	is_control_record bool
	// 제어 레코드의 경우: true = COMMIT, false = ABORT
	control_type ControlRecordType = .none
	// 트랜잭션 레코드를 위한 Producer ID
	producer_id i64 = -1
	// 트랜잭션 레코드를 위한 Producer Epoch
	producer_epoch i16 = -1
}

/// ControlRecordType은 제어 레코드의 유형을 나타냅니다.
/// none: 제어 레코드가 아님
/// abort: 트랜잭션 롤백 마커
/// commit: 트랜잭션 커밋 마커
pub enum ControlRecordType {
	none   = 0 // 제어 레코드 아님
	abort  = 1 // 트랜잭션 롤백 마커
	commit = 2 // 트랜잭션 커밋 마커
}

/// RecordBatch는 레코드의 배치를 나타냅니다.
/// 참고: DataCore Stateless 아키텍처
/// - partition_leader_epoch: 항상 0 (리더 선출 없음)
/// - producer_id/producer_epoch: 멱등성을 위해 사용, 스토리지 레벨에서 처리
pub struct RecordBatch {
pub:
	base_offset            i64
	partition_leader_epoch i32 // Stateless: 항상 0
	magic                  i8 = 2 // v2 형식
	crc                    u32
	attributes             i16
	last_offset_delta      i32
	first_timestamp        i64
	max_timestamp          i64
	producer_id            i64 = -1 // 멱등성은 스토리지에서 처리
	producer_epoch         i16 = -1 // 멱등성은 스토리지에서 처리
	base_sequence          i32 = -1
	records                []Record
}

/// AppendResult는 레코드 추가 결과를 나타냅니다.
/// base_offset: 첫 번째 레코드의 오프셋
/// log_append_time: 로그 추가 시간
/// log_start_offset: 로그 시작 오프셋
/// record_count: 추가된 레코드 수
pub struct AppendResult {
pub:
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
	record_count     int
}

/// FetchResult는 레코드 조회 결과를 나타냅니다.
/// records: 조회된 레코드 목록
/// first_offset: 반환된 첫 번째 레코드의 실제 오프셋 (v0.41.1)
/// high_watermark: 하이 워터마크 오프셋
/// last_stable_offset: 마지막 안정 오프셋 (트랜잭션용)
/// log_start_offset: 로그 시작 오프셋
pub struct FetchResult {
pub:
	records            []Record
	first_offset       i64 // 반환된 첫 번째 레코드의 실제 오프셋
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
}
