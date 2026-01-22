/// CRC32-C (Castagnoli) 체크섬 모듈
///
/// 이 모듈은 Kafka RecordBatch 검증에 사용되는 CRC32-C 체크섬을 계산합니다.
/// Slicing-by-8 알고리즘을 사용하여 최적화된 성능을 제공합니다.
///
/// 다항식: 0x1EDC6F41 (iSCSI/Castagnoli)
/// 역순 다항식: 0x82F63B78
///
/// 성능 특성:
/// - Slicing-by-8: 한 번에 8바이트 처리, 기본 테이블 대비 ~3-5배 빠름
/// - 작은 데이터(<8바이트): 바이트 단위 폴백
///
/// 향후 개선 계획:
/// - SSE4.2 하드웨어 가속 (Intel/AMD)
/// - ARM CRC32 명령어 지원
module crc32c

// ============================================================================
// CRC32-C 테이블 (Castagnoli 다항식)
// ============================================================================

/// 기본 CRC32-C 다항식 테이블 (256 엔트리)
const crc32c_table = [
	u32(0x00000000),
	0xF26B8303,
	0xE13B70F7,
	0x1350F3F4,
	0xC79A971F,
	0x35F1141C,
	0x26A1E7E8,
	0xD4CA64EB,
	0x8AD958CF,
	0x78B2DBCC,
	0x6BE22838,
	0x9989AB3B,
	0x4D43CFD0,
	0xBF284CD3,
	0xAC78BF27,
	0x5E133C24,
	0x105EC76F,
	0xE235446C,
	0xF165B798,
	0x030E349B,
	0xD7C45070,
	0x25AFD373,
	0x36FF2087,
	0xC494A384,
	0x9A879FA0,
	0x68EC1CA3,
	0x7BBCEF57,
	0x89D76C54,
	0x5D1D08BF,
	0xAF768BBC,
	0xBC267848,
	0x4E4DFB4B,
	0x20BD8EDE,
	0xD2D60DDD,
	0xC186FE29,
	0x33ED7D2A,
	0xE72719C1,
	0x154C9AC2,
	0x061C6936,
	0xF477EA35,
	0xAA64D611,
	0x580F5512,
	0x4B5FA6E6,
	0xB93425E5,
	0x6DFE410E,
	0x9F95C20D,
	0x8CC531F9,
	0x7EAEB2FA,
	0x30E349B1,
	0xC288CAB2,
	0xD1D83946,
	0x23B3BA45,
	0xF779DEAE,
	0x05125DAD,
	0x1642AE59,
	0xE4292D5A,
	0xBA3A117E,
	0x4851927D,
	0x5B016189,
	0xA96AE28A,
	0x7DA08661,
	0x8FCB0562,
	0x9C9BF696,
	0x6EF07595,
	0x417B1DBC,
	0xB3109EBF,
	0xA0406D4B,
	0x522BEE48,
	0x86E18AA3,
	0x748A09A0,
	0x67DAFA54,
	0x95B17957,
	0xCBA24573,
	0x39C9C670,
	0x2A993584,
	0xD8F2B687,
	0x0C38D26C,
	0xFE53516F,
	0xED03A29B,
	0x1F682198,
	0x5125DAD3,
	0xA34E59D0,
	0xB01EAA24,
	0x42752927,
	0x96BF4DCC,
	0x64D4CECF,
	0x77843D3B,
	0x85EFBE38,
	0xDBFC821C,
	0x2997011F,
	0x3AC7F2EB,
	0xC8AC71E8,
	0x1C661503,
	0xEE0D9600,
	0xFD5D65F4,
	0x0F36E6F7,
	0x61C69362,
	0x93AD1061,
	0x80FDE395,
	0x72966096,
	0xA65C047D,
	0x5437877E,
	0x4767748A,
	0xB50CF789,
	0xEB1FCBAD,
	0x197448AE,
	0x0A24BB5A,
	0xF84F3859,
	0x2C855CB2,
	0xDEEEDFB1,
	0xCDBE2C45,
	0x3FD5AF46,
	0x7198540D,
	0x83F3D70E,
	0x90A324FA,
	0x62C8A7F9,
	0xB602C312,
	0x44694011,
	0x5739B3E5,
	0xA55230E6,
	0xFB410CC2,
	0x092A8FC1,
	0x1A7A7C35,
	0xE811FF36,
	0x3CDB9BDD,
	0xCEB018DE,
	0xDDE0EB2A,
	0x2F8B6829,
	0x82F63B78,
	0x709DB87B,
	0x63CD4B8F,
	0x91A6C88C,
	0x456CAC67,
	0xB7072F64,
	0xA457DC90,
	0x563C5F93,
	0x082F63B7,
	0xFA44E0B4,
	0xE9141340,
	0x1B7F9043,
	0xCFB5F4A8,
	0x3DDE77AB,
	0x2E8E845F,
	0xDCE5075C,
	0x92A8FC17,
	0x60C37F14,
	0x73938CE0,
	0x81F80FE3,
	0x55326B08,
	0xA759E80B,
	0xB4091BFF,
	0x466298FC,
	0x1871A4D8,
	0xEA1A27DB,
	0xF94AD42F,
	0x0B21572C,
	0xDFEB33C7,
	0x2D80B0C4,
	0x3ED04330,
	0xCCBBC033,
	0xA24BB5A6,
	0x502036A5,
	0x4370C551,
	0xB11B4652,
	0x65D122B9,
	0x97BAA1BA,
	0x84EA524E,
	0x7681D14D,
	0x2892ED69,
	0xDAF96E6A,
	0xC9A99D9E,
	0x3BC21E9D,
	0xEF087A76,
	0x1D63F975,
	0x0E330A81,
	0xFC588982,
	0xB21572C9,
	0x407EF1CA,
	0x532E023E,
	0xA145813D,
	0x758FE5D6,
	0x87E466D5,
	0x94B49521,
	0x66DF1622,
	0x38CC2A06,
	0xCAA7A905,
	0xD9F75AF1,
	0x2B9CD9F2,
	0xFF56BD19,
	0x0D3D3E1A,
	0x1E6DCDEE,
	0xEC064EED,
	0xC38D26C4,
	0x31E6A5C7,
	0x22B65633,
	0xD0DDD530,
	0x0417B1DB,
	0xF67C32D8,
	0xE52CC12C,
	0x1747422F,
	0x49547E0B,
	0xBB3FFD08,
	0xA86F0EFC,
	0x5A048DFF,
	0x8ECEE914,
	0x7CA56A17,
	0x6FF599E3,
	0x9D9E1AE0,
	0xD3D3E1AB,
	0x21B862A8,
	0x32E8915C,
	0xC083125F,
	0x144976B4,
	0xE622F5B7,
	0xF5720643,
	0x07198540,
	0x590AB964,
	0xAB613A67,
	0xB831C993,
	0x4A5A4A90,
	0x9E902E7B,
	0x6CFBAD78,
	0x7FAB5E8C,
	0x8DC0DD8F,
	0xE330A81A,
	0x115B2B19,
	0x020BD8ED,
	0xF0605BEE,
	0x24AA3F05,
	0xD6C1BC06,
	0xC5914FF2,
	0x37FACCF1,
	0x69E9F0D5,
	0x9B8273D6,
	0x88D28022,
	0x7AB90321,
	0xAE7367CA,
	0x5C18E4C9,
	0x4F48173D,
	0xBD23943E,
	0xF36E6F75,
	0x0105EC76,
	0x12551F82,
	0xE03E9C81,
	0x34F4F86A,
	0xC69F7B69,
	0xD5CF889D,
	0x27A40B9E,
	0x79B737BA,
	0x8BDCB4B9,
	0x988C474D,
	0x6AE7C44E,
	0xBE2DA0A5,
	0x4C4623A6,
	0x5F16D052,
	0xAD7D5351,
]

// ============================================================================
// CPU 기능 감지
// ============================================================================

/// 하드웨어 CRC32-C 가속이 사용 가능한지 확인합니다.
/// 현재는 항상 false를 반환합니다 (순수 V 구현 사용).
/// 향후 C 인터롭을 통해 실제 하드웨어 가속을 추가할 수 있습니다.
pub fn cpu_supports_hw_crc32c() bool {
	// TODO: C 인터롭을 통한 하드웨어 가속 지원 시 true 반환
	return false
}

// 호환성을 위한 별칭
/// cpu_supports_sse42는 SSE4.2 CRC32-C 지원 여부를 반환합니다.
/// cpu_supports_hw_crc32c의 별칭입니다.
pub fn cpu_supports_sse42() bool {
	return cpu_supports_hw_crc32c()
}

// ============================================================================
// CRC32-C 구현
// ============================================================================

/// crc32c_sw는 소프트웨어 기반 CRC32-C 계산 (테이블 룩업)을 수행합니다.
/// 바이트 단위 처리로 정확하고 안정적인 구현을 제공합니다.
pub fn crc32c_sw(data []u8) u32 {
	mut crc := u32(0xFFFFFFFF)
	for b in data {
		index := (crc ^ u32(b)) & 0xFF
		crc = (crc >> 8) ^ crc32c_table[index]
	}
	return crc ^ 0xFFFFFFFF
}

/// Slicing-by-8 최적화 CRC32-C 계산
/// 8바이트 단위로 처리하여 성능 향상 (기본 대비 ~3-5배)
fn crc32c_slicing8(data []u8) u32 {
	mut crc := u32(0xFFFFFFFF)
	mut i := 0
	len := data.len

	// 8바이트 단위로 처리
	for i + 8 <= len {
		// 첫 번째 4바이트 읽기 (리틀 엔디안) + CRC XOR
		lo := (u32(data[i]) | (u32(data[i + 1]) << 8) | (u32(data[i + 2]) << 16) | (u32(data[i + 3]) << 24)) ^ crc
		// 두 번째 4바이트 읽기
		hi := u32(data[i + 4]) | (u32(data[i + 5]) << 8) | (u32(data[i + 6]) << 16) | (u32(data[i +
			7]) << 24)

		// 8개 테이블에서 룩업하여 결합
		crc = crc32c_tables[7][lo & 0xFF] ^ crc32c_tables[6][(lo >> 8) & 0xFF] ^ crc32c_tables[5][(lo >> 16) & 0xFF] ^ crc32c_tables[4][(lo >> 24) & 0xFF] ^ crc32c_tables[3][hi & 0xFF] ^ crc32c_tables[2][(hi >> 8) & 0xFF] ^ crc32c_tables[1][(hi >> 16) & 0xFF] ^ crc32c_tables[0][(hi >> 24) & 0xFF]

		i += 8
	}

	// 나머지 바이트 처리 (0-7 바이트)
	for i < len {
		index := (crc ^ u32(data[i])) & 0xFF
		crc = (crc >> 8) ^ crc32c_tables[0][index]
		i += 1
	}

	return crc ^ 0xFFFFFFFF
}

// ============================================================================
// 공개 API
// ============================================================================

/// CRC32-C 체크섬을 계산합니다.
/// 8바이트 이상 데이터에는 Slicing-by-8 알고리즘을 사용합니다.
///
/// 예제:
/// ```v
/// data := [u8(0x01), 0x02, 0x03, 0x04]
/// checksum := crc32c.calculate(data)
/// ```
pub fn calculate(data []u8) u32 {
	if data.len == 0 {
		return 0
	}
	// 8바이트 이상이면 Slicing-by-8 사용
	if data.len >= 8 {
		return crc32c_slicing8(data)
	}
	// 작은 데이터는 기본 테이블 룩업 사용
	return crc32c_sw(data)
}

/// init은 증분 CRC32-C 계산을 위한 초기값을 반환합니다.
pub fn init() u32 {
	return 0xFFFFFFFF
}

/// update는 기존 CRC 값에 데이터를 추가하여 업데이트합니다.
/// 스트리밍 데이터의 체크섬 계산에 유용합니다.
pub fn update(crc u32, data []u8) u32 {
	if data.len == 0 {
		return crc
	}

	mut c := crc
	for b in data {
		index := (c ^ u32(b)) & 0xFF
		c = (c >> 8) ^ crc32c_table[index]
	}
	return c
}

/// finalize는 증분 CRC 계산을 완료하고 최종 체크섬을 반환합니다.
pub fn finalize(crc u32) u32 {
	return crc ^ 0xFFFFFFFF
}

/// is_hardware_accelerated는 하드웨어 가속이 사용 가능한지 확인합니다.
pub fn is_hardware_accelerated() bool {
	return cpu_supports_hw_crc32c()
}
