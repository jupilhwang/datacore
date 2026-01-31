// 어댑터 레이어 - 스토리지 인터페이스
module storage

import service.port

/// StorageAdapter는 port.StoragePort를 구현합니다.
/// 이 구조체는 마커 구조체이며, 실제 구현은 plugins/ 디렉토리에 있습니다.
pub struct StorageAdapter {
}
