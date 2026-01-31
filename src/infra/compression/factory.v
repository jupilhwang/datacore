/// 인프라 레이어 - Compressor 팩토리
/// CompressionType에 따라 적절한 Compressor 인스턴스를 생성하는 팩토리
module compression

/// new_compressor는 지정된 CompressionType에 맞는 Compressor를 생성합니다.
/// C 라이브러리를 사용한 고성능 구현을 반환합니다.
pub fn new_compressor(compression_type CompressionType) !Compressor {
	match compression_type {
		.none {
			return new_noop_compressor()
		}
		.gzip {
			return new_gzip_compressor()
		}
		.snappy {
			// C 라이브러리 사용
			return new_snappy_compressor_c()
		}
		.lz4 {
			// C 라이브러리 사용
			return new_lz4_compressor_c()
		}
		.zstd {
			// C 라이브러리 사용
			return new_zstd_compressor_c()
		}
	}
	return error('unsupported compression type: ${compression_type}')
}

/// new_compressor_with_level은 지정된 CompressionType과 레벨로 Compressor를 생성합니다.
/// Gzip과 ZSTD만 레벨을 지원합니다.
pub fn new_compressor_with_level(compression_type CompressionType, level int) !Compressor {
	match compression_type {
		.none {
			return new_noop_compressor()
		}
		.gzip {
			return new_gzip_compressor_with_level(level)
		}
		.snappy {
			return new_snappy_compressor()
		}
		.lz4 {
			return new_lz4_compressor()
		}
		.zstd {
			return new_zstd_compressor_with_level(level)
		}
	}
	return error('unsupported compression type: ${compression_type}')
}

/// 사용 가능한 모든 압축 타입 반환.
pub fn list_available_compressors() []CompressionType {
	return [
		CompressionType.none,
		CompressionType.gzip,
		CompressionType.snappy,
		CompressionType.lz4,
		CompressionType.zstd,
	]
}
