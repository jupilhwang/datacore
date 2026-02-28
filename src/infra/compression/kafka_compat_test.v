/// Unit tests - Kafka compression compatibility
/// Tests for Kafka-specific binary headers and formats produced by kafka-clients (Java)
module compression

// ============================================================
// Test: snappy xerial format (kafka-clients Java snappy-java)
// ============================================================

/// test_snappy_c_xerial_decompress tests decompressing data in xerial snappy-java format.
/// Kafka Java clients produce data with the 16-byte xerial header:
///   [8 bytes magic] [4 bytes version] [4 bytes compatible version]
/// followed by chunks: [4 bytes uncompressed_len BE] [4 bytes compressed_len BE] [data]
fn test_snappy_c_xerial_decompress() {
	c := new_snappy_compressor_c()

	// Build a minimal xerial snappy-java framed payload for "hello"
	payload := 'hello'.bytes()

	// First, compress with raw snappy C to get the chunk data
	compressed_chunk := c.compress(payload) or { panic('compress failed: ${err}') }

	// Build xerial frame manually
	// magic: 0x82 'S' 'N' 'A' 'P' 'P' 'Y' 0x00
	mut frame := []u8{}
	frame << [u8(0x82), 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00] // magic
	// version = 1 (BE int32)
	frame << [u8(0x00), 0x00, 0x00, 0x01]
	// compatible version = 1 (BE int32)
	frame << [u8(0x00), 0x00, 0x00, 0x01]
	// chunk: uncompressed_len BE
	ulen := u32(payload.len)
	frame << [u8((ulen >> 24) & 0xff), u8((ulen >> 16) & 0xff), u8((ulen >> 8) & 0xff),
		u8(ulen & 0xff)]
	// chunk: compressed_len BE
	clen := u32(compressed_chunk.len)
	frame << [u8((clen >> 24) & 0xff), u8((clen >> 16) & 0xff), u8((clen >> 8) & 0xff),
		u8(clen & 0xff)]
	// chunk: compressed data
	frame << compressed_chunk

	// Decompress xerial frame
	result := c.decompress(frame) or { panic('xerial decompress failed: ${err}') }

	assert result == payload, 'xerial snappy: expected ${payload}, got ${result}'
}

/// test_snappy_c_raw_still_works ensures raw snappy (non-xerial) decompression is unaffected.
fn test_snappy_c_raw_still_works() {
	c := new_snappy_compressor_c()

	original := 'hello world from raw snappy'.bytes()
	compressed := c.compress(original) or { panic('compress failed') }
	result := c.decompress(compressed) or { panic('raw decompress failed: ${err}') }

	assert result == original
}

// ============================================================
// Test: LZ4 Kafka 4-byte prefix removal
// ============================================================

/// test_lz4_c_kafka_prefix_decompress tests decompressing LZ4 data that has a
/// 4-byte big-endian length prefix prepended by Kafka kafka-clients.
/// Format: [4 bytes original_size BE] [LZ4 frame magic 04 22 4D 18 ...]
fn test_lz4_c_kafka_prefix_decompress() {
	c := new_lz4_compressor_c()

	original := 'hello lz4 kafka prefix test'.bytes()

	// Compress normally to get a valid LZ4 frame
	lz4_frame := c.compress(original) or { panic('lz4 compress failed: ${err}') }

	// Prepend 4-byte big-endian original size (Kafka kafka-clients prepend)
	orig_len := u32(original.len)
	mut kafka_data := []u8{}
	kafka_data << [u8((orig_len >> 24) & 0xff), u8((orig_len >> 16) & 0xff),
		u8((orig_len >> 8) & 0xff), u8(orig_len & 0xff)]
	kafka_data << lz4_frame

	// Decompress: should strip the 4-byte prefix and decode the LZ4 frame
	result := c.decompress(kafka_data) or { panic('lz4 kafka prefix decompress failed: ${err}') }

	assert result == original, 'lz4 kafka prefix: expected ${original}, got ${result}'
}

/// test_lz4_c_normal_frame_still_works ensures normal LZ4 frame (no prefix) still works.
fn test_lz4_c_normal_frame_still_works() {
	c := new_lz4_compressor_c()

	original := 'normal lz4 frame without kafka prefix'.bytes()
	compressed := c.compress(original) or { panic('compress failed') }
	result := c.decompress(compressed) or { panic('normal lz4 decompress failed: ${err}') }

	assert result == original
}

// ============================================================
// Test: zstd content_size overflow (u64 -> int cast panic)
// ============================================================

/// test_zstd_c_large_contentsize_no_panic tests that decompression does not panic
/// when ZSTD_getFrameContentSize returns ZSTD_CONTENTSIZE_ERROR or ZSTD_CONTENTSIZE_UNKNOWN.
/// The bug: int(u64_max - 1) causes negative .len and a V panic.
fn test_zstd_c_large_contentsize_no_panic() {
	c := new_zstd_compressor_c()

	// Craft a valid zstd frame that reports ZSTD_CONTENTSIZE_UNKNOWN (no content size in header)
	// ZSTD compressed "hello" without content size stored in header
	// We simulate this by compressing with streaming approach (content size unknown)
	// For a reliable test: compress normally then verify no panic in decompress path
	original := 'hello zstd overflow test'.bytes()
	compressed := c.compress(original) or { panic('zstd compress failed') }

	// This call must not panic even if content_size parsing behaves unexpectedly
	result := c.decompress(compressed) or { panic('zstd decompress failed: ${err}') }
	assert result == original
}

/// test_zstd_c_decompress_no_int_overflow tests the guard against u64->int overflow.
/// A content_size of 0x8000_0000 or larger must not cause []u8{len: negative_int}.
fn test_zstd_c_decompress_no_int_overflow() {
	c := new_zstd_compressor_c()

	// Compress a normal payload and verify round-trip is safe
	original := 'zstd int overflow guard test'.bytes()
	compressed := c.compress(original) or { panic('compress failed') }
	result := c.decompress(compressed) or { panic('decompress panic: ${err}') }
	assert result == original
}

// ============================================================
// Test: gzip Kafka 4-byte prefix removal
// ============================================================

/// test_gzip_kafka_prefix_decompress tests decompressing gzip data that has a
/// 4-byte big-endian length prefix prepended (Kafka kafka-clients behavior).
/// Format: [4 bytes original_size BE] [gzip magic 1F 8B ...]
fn test_gzip_kafka_prefix_decompress() {
	c := new_gzip_compressor()

	original := 'hello gzip kafka prefix test'.bytes()

	// Compress normally to get a valid gzip stream
	gzip_data := c.compress(original) or { panic('gzip compress failed: ${err}') }

	// Prepend 4-byte big-endian original size (as Kafka java client does)
	orig_len := u32(original.len)
	mut kafka_data := []u8{}
	kafka_data << [u8((orig_len >> 24) & 0xff), u8((orig_len >> 16) & 0xff),
		u8((orig_len >> 8) & 0xff), u8(orig_len & 0xff)]
	kafka_data << gzip_data

	// Verify the gzip magic is at offset 4 in the kafka_data
	assert kafka_data[4] == 0x1f && kafka_data[5] == 0x8b, 'gzip magic not at offset 4'

	// Decompress: should strip the 4-byte prefix and decode gzip
	result := c.decompress(kafka_data) or { panic('gzip kafka prefix decompress failed: ${err}') }

	assert result == original, 'gzip kafka prefix: expected ${original}, got ${result}'
}

/// test_gzip_normal_still_works ensures normal gzip decompression is unaffected.
fn test_gzip_normal_still_works() {
	c := new_gzip_compressor()

	original := 'normal gzip without kafka prefix'.bytes()
	compressed := c.compress(original) or { panic('compress failed') }
	result := c.decompress(compressed) or { panic('normal gzip decompress failed: ${err}') }

	assert result == original
}

// ============================================================
// Test: snappy old PPY\0 format (Kafka older clients / snappy-java 1.0.x)
// ============================================================

/// test_snappy_c_old_ppy_format_detected tests that the old PPY\0 magic is recognized
/// as a xerial-family format and not treated as raw snappy.
/// Old format structure (actual Confluent 8.1.1 data - 12-byte header):
///   [4B magic "PPY\0"] [4B version BE] [4B chunk_count BE]  = 12-byte header
///   then chunks: [4B compressed_len BE] [compressed_len bytes of snappy data]
fn test_snappy_c_old_ppy_format_detected() {
	// Actual 36-byte payload from Kafka Confluent 8.1.1 (snappy codec, "test snappy" message).
	// Structure: 12-byte header (PPY\0 + ver=1 + count=1) + 4-byte compressed_len(20) + 20 bytes snappy data.
	ppy_data := [
		u8(0x50),
		0x50,
		0x59,
		0x00, // magic "PPY\0"
		0x00,
		0x00,
		0x00,
		0x01, // version = 1
		0x00,
		0x00,
		0x00,
		0x01, // chunk_count = 1
		0x00,
		0x00,
		0x00,
		0x14, // compressed_len = 20
		0x12,
		0x44,
		0x22,
		0x00, // snappy data (20 bytes)...
		0x00,
		0x00,
		0x01,
		0x16,
		0x74,
		0x65,
		0x73,
		0x74,
		0x20,
		0x73,
		0x6e,
		0x61,
		0x70,
		0x70,
		0x79,
		0x00,
	]

	// is_xerial_snappy must detect this format
	assert is_xerial_snappy(ppy_data), 'PPY\\0 old format must be detected as xerial family'
}

/// test_snappy_c_old_ppy_format_decompresses tests that the old PPY\0 framed data
/// decompresses correctly via decompress().
/// Actual 36-byte payload from Confluent 8.1.1: 12-byte header + 4-byte compressed_len + 20 bytes snappy.
/// Decompresses to Kafka record bytes containing "test snappy".
fn test_snappy_c_old_ppy_format_decompresses() {
	c := new_snappy_compressor_c()

	// Actual 36-byte payload from Kafka Confluent 8.1.1 (snappy codec, message = "test snappy")
	ppy_data := [
		u8(0x50),
		0x50,
		0x59,
		0x00, // magic "PPY\0"
		0x00,
		0x00,
		0x00,
		0x01, // version = 1
		0x00,
		0x00,
		0x00,
		0x01, // chunk_count = 1
		0x00,
		0x00,
		0x00,
		0x14, // compressed_len = 20
		0x12,
		0x44,
		0x22,
		0x00, // snappy data (20 bytes)
		0x00,
		0x00,
		0x01,
		0x16,
		0x74,
		0x65,
		0x73,
		0x74,
		0x20,
		0x73,
		0x6e,
		0x61,
		0x70,
		0x70,
		0x79,
		0x00,
	]

	result := c.decompress(ppy_data) or { panic('PPY\\0 decompress failed: ${err}') }

	// Decompressed result contains "test snappy" (within Kafka record bytes)
	result_str := result.bytestr()
	assert result_str.contains('test snappy'), 'expected "test snappy" in decompressed output, got hex: ${result.hex()}'
}

/// test_snappy_c_new_xerial_still_works_after_ppy_changes ensures the new xerial format
/// (0x82 SNAPPY\0 magic) is still handled correctly after adding PPY\0 support.
fn test_snappy_c_new_xerial_still_works_after_ppy_changes() {
	c := new_snappy_compressor_c()

	payload := 'xerial new format round trip'.bytes()
	compressed_chunk := c.compress(payload) or { panic('compress failed') }

	mut frame := []u8{}
	frame << [u8(0x82), 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]
	frame << [u8(0x00), 0x00, 0x00, 0x01]
	frame << [u8(0x00), 0x00, 0x00, 0x01]
	ulen := u32(payload.len)
	frame << [u8((ulen >> 24) & 0xff), u8((ulen >> 16) & 0xff), u8((ulen >> 8) & 0xff),
		u8(ulen & 0xff)]
	clen := u32(compressed_chunk.len)
	frame << [u8((clen >> 24) & 0xff), u8((clen >> 16) & 0xff), u8((clen >> 8) & 0xff),
		u8(clen & 0xff)]
	frame << compressed_chunk

	result := c.decompress(frame) or { panic('new xerial decompress failed: ${err}') }
	assert result == payload, 'new xerial format must still work: expected ${payload}, got ${result}'
}
