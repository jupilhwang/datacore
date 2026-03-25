// Compression interface following DIP.
// Abstracts data compression/decompression from the infrastructure layer.
// Uses i16 for compression type to avoid coupling with infra enum.
module port

/// CompressionPort abstracts data compression and decompression.
/// Implemented by infra/compression CompressionService via an adapter.
pub interface CompressionPort {
mut:
	/// Compresses data using the specified compression type.
	/// compression_type: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
	compress(data []u8, compression_type i16) ![]u8

	/// Decompresses data using the specified compression type.
	decompress(data []u8, compression_type i16) ![]u8
}
