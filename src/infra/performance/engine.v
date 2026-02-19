module performance

import infra.performance.core

/// 하위 호환성을 위해 core에서 설정 타입을 재내보냅니다.
pub type PerformanceConfig = core.PerformanceConfig
pub type PerformanceStats = core.PerformanceStats

/// PerformanceEngine은 모든 성능 최적화 플러그인의 표준 인터페이스를 정의합니다.
pub interface PerformanceEngine {
	/// name은 엔진 이름을 반환합니다.
	name() string
mut:
	/// init은 설정으로 엔진을 초기화합니다.
	init(config core.PerformanceConfig) !

	// 버퍼 관리
	/// get_buffer는 지정된 크기의 버퍼를 획득합니다.
	get_buffer(size int) &core.Buffer
	/// put_buffer는 버퍼를 풀에 반환합니다.
	put_buffer(buf &core.Buffer)

	// 객체 풀링
	/// get_record는 풀링된 레코드를 획득합니다.
	get_record() &core.PooledRecord
	/// put_record는 레코드를 풀에 반환합니다.
	put_record(r &core.PooledRecord)
	/// get_batch는 풀링된 배치를 획득합니다.
	get_batch() &core.PooledRecordBatch
	/// put_batch는 배치를 풀에 반환합니다.
	put_batch(b &core.PooledRecordBatch)
	/// get_request는 풀링된 요청을 획득합니다.
	get_request() &core.PooledRequest
	/// put_request는 요청을 풀에 반환합니다.
	put_request(r &core.PooledRequest)

	// I/O 작업
	/// read_file_at은 파일의 지정된 오프셋에서 데이터를 읽습니다.
	read_file_at(path string, offset i64, size int) ![]u8
	/// write_file_at은 파일의 지정된 오프셋에 데이터를 씁니다.
	write_file_at(path string, offset i64, data []u8) !

	/// get_stats는 성능 통계를 반환합니다.
	get_stats() core.PerformanceStats
}
