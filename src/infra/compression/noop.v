/// 인프라 레이어 - Noop 압축 (통과)
/// 압축 없이 데이터를 그대로 통과시키는 Compressor 구현
module compression

/// NoopCompressor는 압축 없이 데이터를 그대로 통과시킵니다.
pub struct NoopCompressor {
}

/// new_noop_compressor는 새 NoopCompressor를 생성합니다.
pub fn new_noop_compressor() &NoopCompressor {
	return &NoopCompressor{}
}

/// compress는 데이터를 그대로 반환합니다 (압축 없음).
pub fn (c &NoopCompressor) compress(data []u8) ![]u8 {
	return data.clone()
}

/// decompress는 데이터를 그대로 반환합니다 (해제 없음).
pub fn (c &NoopCompressor) decompress(data []u8) ![]u8 {
	return data.clone()
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &NoopCompressor) compression_type() CompressionType {
	return CompressionType.none
}
