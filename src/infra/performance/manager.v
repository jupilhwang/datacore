module performance

import infra.performance.core
import infra.performance.engines

// 성능 관리자 - 전략 파사드

/// PerformanceManager는 성능 최적화를 위한 중앙 관리자입니다.
@[heap]
pub struct PerformanceManager {
pub mut:
	engine  PerformanceEngine
	enabled bool
}

/// new_performance_manager는 사용 가능한 최적의 엔진으로 성능 관리자를 생성합니다.
pub fn new_performance_manager(config core.PerformanceConfig) &PerformanceManager {
	mut engine := PerformanceEngine(engines.GenericPerformanceEngine{})

	$if linux {
		if config.enable_linux_optimizations {
			// TODO: LinuxPerformanceEngine이 완전히 구현되면 초기화
			// 현재는 Generic을 사용하지만 사용 가능하면 Linux로 로깅
			// mut linux_engine := engines.LinuxPerformanceEngine{}
			// engine = linux_engine
		}
	}

	engine.init(config) or {
		// 오류 시 최소/일반 엔진으로 폴백
		println('Failed to init optimized engine, falling back to Generic')
	}

	return &PerformanceManager{
		engine:  engine
		enabled: true
	}
}

// 엔진에 프록시하는 편의 메서드

/// get_buffer는 지정된 크기의 버퍼를 획득합니다.
pub fn (mut m PerformanceManager) get_buffer(size int) &core.Buffer {
	return m.engine.get_buffer(size)
}

/// put_buffer는 버퍼를 풀에 반환합니다.
pub fn (mut m PerformanceManager) put_buffer(buf &core.Buffer) {
	m.engine.put_buffer(buf)
}

/// get_record는 풀링된 레코드를 획득합니다.
pub fn (mut m PerformanceManager) get_record() &core.PooledRecord {
	return m.engine.get_record()
}

/// put_record는 레코드를 풀에 반환합니다.
pub fn (mut m PerformanceManager) put_record(r &core.PooledRecord) {
	m.engine.put_record(r)
}

/// get_batch는 풀링된 배치를 획득합니다.
pub fn (mut m PerformanceManager) get_batch() &core.PooledRecordBatch {
	return m.engine.get_batch()
}

/// put_batch는 배치를 풀에 반환합니다.
pub fn (mut m PerformanceManager) put_batch(b &core.PooledRecordBatch) {
	m.engine.put_batch(b)
}

/// get_request는 풀링된 요청을 획득합니다.
pub fn (mut m PerformanceManager) get_request() &core.PooledRequest {
	return m.engine.get_request()
}

/// put_request는 요청을 풀에 반환합니다.
pub fn (mut m PerformanceManager) put_request(r &core.PooledRequest) {
	m.engine.put_request(r)
}

// 헬퍼 함수 (원래 manager.v에서 이동)

/// with_buffer는 풀링된 버퍼로 함수를 실행합니다.
pub fn (mut m PerformanceManager) with_buffer(min_size int, f fn (mut core.Buffer)) {
	mut buf := m.get_buffer(min_size)
	defer { m.put_buffer(buf) }
	f(mut buf)
}

/// compute_partition은 키에 대한 Kafka 파티션을 계산합니다.
pub fn (m &PerformanceManager) compute_partition(key []u8, num_partitions int) int {
	return core.kafka_partition(key, num_partitions)
}

/// compute_checksum은 CRC32 체크섬을 계산합니다.
pub fn (m &PerformanceManager) compute_checksum(data []u8) u32 {
	return core.crc32_ieee(data)
}

// 전역 성능 관리자 (싱글톤 패턴)

// 싱글톤 인스턴스를 보유하는 구조체
struct GlobalPerformanceHolder {
mut:
	instance &PerformanceManager = unsafe { nil }
	is_init  bool
}

// 모듈 레벨 싱글톤 홀더
const global_holder = &GlobalPerformanceHolder{}

/// init_global_performance는 전역 성능 관리자를 초기화합니다.
pub fn init_global_performance(config core.PerformanceConfig) {
	mut holder := unsafe { global_holder }
	if !holder.is_init {
		unsafe {
			holder.instance = new_performance_manager(config)
			holder.is_init = true
		}
	}
}

/// get_global_performance는 전역 성능 관리자를 반환합니다.
pub fn get_global_performance() &PerformanceManager {
	holder := unsafe { global_holder }
	if !holder.is_init {
		// 초기화되지 않은 경우 안전한 기본값
		init_global_performance(core.PerformanceConfig{})
	}
	return unsafe { holder.instance }
}

/// get_stats는 성능 통계를 반환합니다.
pub fn (mut m PerformanceManager) get_stats() core.PerformanceStats {
	return m.engine.get_stats()
}
