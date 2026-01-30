/// 인프라 레이어 - Gzip 압축
/// V 표준 라이브러리 compress.gzip을 사용한 Gzip Compressor 구현
module compression

import compress.gzip

/// Gzip 압축 알고리즘 구현.
pub struct GzipCompressor {
	level int
}

/// new_gzip_compressor는 새 GzipCompressor를 생성합니다.
/// 기본 압축 레벨은 6 (균형)입니다.
pub fn new_gzip_compressor() &GzipCompressor {
	return &GzipCompressor{
		level: 6
	}
}

/// new_gzip_compressor_with_level은 지정된 압축 레벨로 GzipCompressor를 생성합니다.
/// 레벨: 1-9 (1=최고속도, 9=최고압축)
pub fn new_gzip_compressor_with_level(level int) &GzipCompressor {
	mut lvl := level
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 9 {
		lvl = 9
	}
	return &GzipCompressor{
		level: lvl
	}
}

/// compress는 데이터를 Gzip으로 압축합니다.
pub fn (c &GzipCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	compressed := gzip.compress(data) or { return error('gzip compression failed: ${err}') }

	return compressed
}

/// decompress는 Gzip으로 압축된 데이터를 해제합니다.
pub fn (c &GzipCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	decompressed := gzip.decompress(data) or { return error('gzip decompression failed: ${err}') }

	return decompressed
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &GzipCompressor) compression_type() CompressionType {
	return CompressionType.gzip
}
