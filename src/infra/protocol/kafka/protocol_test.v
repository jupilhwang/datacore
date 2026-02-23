module kafka_test

import infra.protocol.kafka

// test_request_header_parsing tests parsing of the common Kafka request header
fn test_request_header_parsing() {
	// API_KEY=3 (Metadata), API_VERSION=8, CORRELATION_ID=1234, CLIENT_ID="test-client"
	// Note: parse_request expects data WITHOUT size field (TCP layer strips it)

	mut data := [
		// API Key (Metadata - 3)
		u8(0x00),
		u8(0x03),
		// API Version (8) - Non-Flexible Header
		u8(0x00),
		u8(0x08),
		// Correlation ID (1234)
		u8(0x00),
		u8(0x00),
		u8(0x04),
		u8(0xD2),
		// Client ID Length (11, i16) - NULLABLE_STRING
		u8(0x00),
		u8(0x0B),
		// Client ID ("test-client")
		u8(`t`),
		u8(`e`),
		u8(`s`),
		u8(`t`),
		u8(`-`),
		u8(`c`),
		u8(`l`),
		u8(`i`),
		u8(`e`),
		u8(`n`),
		u8(`t`),
		// Request Body (Empty for header test)
		u8(0x00),
	]

	request := kafka.parse_request(data)!
	header := request.header

	assert header.api_key == 3
	assert header.api_version == 8
	assert header.correlation_id == 1234
	assert header.client_id == 'test-client'
}

// TODO: test_metadata_request_parsing
// TODO: test_produce_request_parsing
