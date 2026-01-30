/// 인프라 레이어 - 압축 인터페이스
/// Kafka 프로토콜과 호환되는 압축 타입 및 Compressor 인터페이스 정의
module compression

/// CompressionType은 Kafka 프로토콜과 호환되는 압축 알고리즘 타입입니다.
pub enum CompressionType {
	none   = 0
	gzip   = 1
	snappy = 2
	lz4    = 3
	zstd   = 4
}

/// CompressionType을 문자열로 변환합니다.
pub fn (ct CompressionType) str() string {
	return match ct {
		.none { 'none' }
		.gzip { 'gzip' }
		.snappy { 'snappy' }
		.lz4 { 'lz4' }
		.zstd { 'zstd' }
	}
}

/// 문자열에서 CompressionType을 파싱합니다.
pub fn compression_type_from_string(s string) !CompressionType {
	return match s.to_lower() {
		'none', 'noop', '' { CompressionType.none }
		'gzip', 'gz' { CompressionType.gzip }
		'snappy' { CompressionType.snappy }
		'lz4' { CompressionType.lz4 }
		'zstd', 'zstandard' { CompressionType.zstd }
		else { return error('unknown compression type: ${s}') }
	}
}

/// Compressor는 압축/해제 기능을 제공하는 인터페이스입니다.
pub interface Compressor {
	compress(data []u8) ![]u8
	decompress(data []u8) ![]u8
	compression_type() CompressionType
}

/// CompressorError는 압축 관련 에러를 나타냅니다.
pub struct CompressorError {
	message string
	typ     string
}

/// new_compressor_error는 새 CompressorError를 생성합니다.
pub fn new_compressor_error(message string, typ string) CompressorError {
	return CompressorError{
		message: message
		typ:     typ
	}
}

/// msg returns the error message.
pub fn (e CompressorError) msg() string {
	return '${e.typ}: ${e.message}'
}

/// code returns the error code.
pub fn (e CompressorError) code() int {
	return match e.typ {
		'invalid_input' { 1 }
		'compression_failed' { 2 }
		'decompression_failed' { 3 }
		'unsupported_type' { 4 }
		else { 0 }
	}
}
