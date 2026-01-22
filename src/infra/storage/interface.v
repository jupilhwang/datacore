// 어댑터 레이어 - 스토리지 인터페이스
module storage

import service.port

/// PluginInfo는 스토리지 플러그인 메타데이터를 담습니다.
pub struct PluginInfo {
pub:
	name        string // 플러그인 이름
	version     string // 플러그인 버전
	description string // 플러그인 설명
	author      string // 플러그인 작성자
}

/// StoragePlugin은 스토리지 백엔드를 위한 인터페이스입니다.
pub interface StoragePlugin {
	// 메타데이터
	info() PluginInfo

	// 라이프사이클
	init(config map[string]string) !
	shutdown() !
	health_check() !port.HealthStatus

	// 스토리지 어댑터 조회
	get_adapter() !&StorageAdapter
}

/// StorageAdapter는 port.StoragePort를 구현합니다.
/// 이 구조체는 마커 구조체이며, 실제 구현은 plugins/ 디렉토리에 있습니다.
pub struct StorageAdapter {
}
