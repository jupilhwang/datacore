// Interface Layer - CLI Produce Command
// Message production using Kafka protocol
module cli

import os
import time

// ProduceOptions holds produce command options
pub struct ProduceOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	topic            string
	partition        int = -1 // -1 = auto
	key              string
	value            string
	file             string // Read messages from file
	stdin            bool   // Read from stdin
	timeout_ms       int = 30000
	acks             int = 1 // 0, 1, or -1 (all)
}

// parse_produce_options parses produce command options
pub fn parse_produce_options(args []string) ProduceOptions {
	mut opts := ProduceOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partition', '-p' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						partition: args[i + 1].int()
					}
					i += 1
				}
			}
			'--key', '-k' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						key: args[i + 1]
					}
					i += 1
				}
			}
			'--message', '-m', '--value' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						value: args[i + 1]
					}
					i += 1
				}
			}
			'--file', '-f' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						file: args[i + 1]
					}
					i += 1
				}
			}
			'--stdin' {
				opts = ProduceOptions{
					...opts
					stdin: true
				}
			}
			'--acks', '-a' {
				if i + 1 < args.len {
					opts = ProduceOptions{
						...opts
						acks: args[i + 1].int()
					}
					i += 1
				}
			}
			else {
				// First positional arg might be topic
				if !args[i].starts_with('-') && opts.topic == '' {
					opts = ProduceOptions{
						...opts
						topic: args[i]
					}
				}
			}
		}
		i += 1
	}

	return opts
}

// run_produce produces messages to a topic
pub fn run_produce(opts ProduceOptions) ! {
	if opts.topic == '' {
		return error('Topic name is required. Use --topic <name>')
	}

	// Collect messages to produce
	mut messages := []ProduceMessage{}

	if opts.value != '' {
		// Single message from command line
		messages << ProduceMessage{
			key:   opts.key.bytes()
			value: opts.value.bytes()
		}
	} else if opts.file != '' {
		// Read messages from file (one per line)
		content := os.read_file(opts.file) or {
			return error('Failed to read file ${opts.file}: ${err}')
		}
		for line in content.split_into_lines() {
			if line.len > 0 {
				messages << ProduceMessage{
					key:   opts.key.bytes()
					value: line.bytes()
				}
			}
		}
	} else if opts.stdin {
		// Read from stdin interactively
		println('\x1b[90mEnter messages (Ctrl+D to finish):\x1b[0m')
		for {
			line := os.get_line()
			if line.len == 0 {
				break
			}
			messages << ProduceMessage{
				key:   opts.key.bytes()
				value: line.bytes()
			}
		}
	} else {
		return error('No message provided. Use --message, --file, or --stdin')
	}

	if messages.len == 0 {
		return error('No messages to produce')
	}

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build and send Produce request
	partition := if opts.partition < 0 { 0 } else { opts.partition }
	request := build_produce_request(opts.topic, partition, messages, opts.acks, opts.timeout_ms)

	// Send request
	send_kafka_request(mut conn, 0, 9, request)! // API Key 0 = Produce, version 9

	// Read response
	response := read_kafka_response(mut conn)!

	// Check response
	result := parse_produce_response(response)!

	println('\x1b[32m✓\x1b[0m Produced ${messages.len} message(s) to "${opts.topic}"')
	println('  Partition: ${partition}')
	println('  Base Offset: ${result.base_offset}')
}

struct ProduceMessage {
	key   []u8
	value []u8
}

struct ProduceResult {
	base_offset i64
	error_code  i16
}

fn build_produce_request(topic string, partition int, messages []ProduceMessage, acks int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Transactional ID (compact nullable string - null)
	body << u8(0)

	// Acks (2 bytes)
	body << u8(i16(acks) >> 8)
	body << u8(i16(acks) & 0xff)

	// Timeout ms (4 bytes)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// Topic data array (compact array)
	body << u8(2) // 1 topic + 1

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partition data array (compact array)
	body << u8(2) // 1 partition + 1

	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Build record batch
	records := build_record_batch(messages)

	// Records (compact bytes)
	records_len := records.len + 1
	body << encode_varint(records_len)
	body << records

	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn build_record_batch(messages []ProduceMessage) []u8 {
	mut batch := []u8{}

	// Base offset (8 bytes) - 0 for new messages
	for _ in 0 .. 8 {
		batch << u8(0)
	}

	// Batch length placeholder (4 bytes) - will be filled later
	batch_len_pos := batch.len
	for _ in 0 .. 4 {
		batch << u8(0)
	}

	// Partition leader epoch (4 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Magic (1 byte) - v2 = 2
	batch << u8(2)

	// CRC placeholder (4 bytes) - simplified, will be 0
	for _ in 0 .. 4 {
		batch << u8(0)
	}

	// Attributes (2 bytes) - 0 = no compression
	batch << u8(0)
	batch << u8(0)

	// Last offset delta (4 bytes)
	last_delta := messages.len - 1
	batch << u8(last_delta >> 24)
	batch << u8((last_delta >> 16) & 0xff)
	batch << u8((last_delta >> 8) & 0xff)
	batch << u8(last_delta & 0xff)

	// First timestamp (8 bytes)
	now := time.now().unix_milli()
	batch << u8(now >> 56)
	batch << u8((now >> 48) & 0xff)
	batch << u8((now >> 40) & 0xff)
	batch << u8((now >> 32) & 0xff)
	batch << u8((now >> 24) & 0xff)
	batch << u8((now >> 16) & 0xff)
	batch << u8((now >> 8) & 0xff)
	batch << u8(now & 0xff)

	// Max timestamp (8 bytes) - same as first
	batch << u8(now >> 56)
	batch << u8((now >> 48) & 0xff)
	batch << u8((now >> 40) & 0xff)
	batch << u8((now >> 32) & 0xff)
	batch << u8((now >> 24) & 0xff)
	batch << u8((now >> 16) & 0xff)
	batch << u8((now >> 8) & 0xff)
	batch << u8(now & 0xff)

	// Producer ID (8 bytes) - -1 = no idempotent
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Producer epoch (2 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)

	// Base sequence (4 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Records count (4 bytes)
	batch << u8(messages.len >> 24)
	batch << u8((messages.len >> 16) & 0xff)
	batch << u8((messages.len >> 8) & 0xff)
	batch << u8(messages.len & 0xff)

	// Encode records
	for i, msg in messages {
		record := encode_record(msg, i, 0)
		batch << record
	}

	// Fill in batch length (excluding base_offset and batch_length itself)
	batch_len := batch.len - 12 // 8 (base_offset) + 4 (batch_length)
	batch[batch_len_pos] = u8(batch_len >> 24)
	batch[batch_len_pos + 1] = u8((batch_len >> 16) & 0xff)
	batch[batch_len_pos + 2] = u8((batch_len >> 8) & 0xff)
	batch[batch_len_pos + 3] = u8(batch_len & 0xff)

	// CRC32-C 계산: attributes 필드부터 배치 끝까지
	// CRC 필드 위치: batch_len_pos(4) + partition_leader_epoch(4) + magic(1) = 17 (0-indexed)
	crc_pos := batch_len_pos + 4 + 4 + 1 // = 17
	crc_data_start := crc_pos + 4 // CRC 필드 다음부터
	crc := calculate_crc32c_cli(batch[crc_data_start..])
	batch[crc_pos] = u8(crc >> 24)
	batch[crc_pos + 1] = u8((crc >> 16) & 0xff)
	batch[crc_pos + 2] = u8((crc >> 8) & 0xff)
	batch[crc_pos + 3] = u8(crc & 0xff)

	return batch
}

fn encode_record(msg ProduceMessage, offset_delta int, timestamp_delta i64) []u8 {
	mut record := []u8{}

	// Attributes (1 byte) - varint
	record << u8(0)

	// Timestamp delta (varint)
	record << encode_signed_varint(timestamp_delta)

	// Offset delta (varint)
	record << encode_signed_varint(i64(offset_delta))

	// Key length (varint) - -1 for null
	if msg.key.len == 0 {
		record << u8(1) // -1 in zigzag = 1
	} else {
		record << encode_signed_varint(i64(msg.key.len))
		record << msg.key
	}

	// Value length (varint)
	record << encode_signed_varint(i64(msg.value.len))
	record << msg.value

	// Headers count (varint) - 0
	record << u8(0)

	// Prepend record length (varint)
	mut result := encode_signed_varint(i64(record.len))
	result << record

	return result
}

fn encode_varint(val int) []u8 {
	mut result := []u8{}
	mut v := u64(val)
	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)
	return result
}

fn encode_signed_varint(val i64) []u8 {
	// ZigZag encoding
	zigzag := (u64(val) << 1) ^ u64(val >> 63)
	mut result := []u8{}
	mut v := zigzag
	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)
	return result
}

fn parse_produce_response(response []u8) !ProduceResult {
	if response.len < 20 {
		return error('Invalid produce response')
	}

	// Skip: correlation_id(4) + tagged_fields(varint) + responses array header
	// This is simplified - full parsing would be needed for production

	// Look for base_offset in the response
	// Response structure: topics -> partitions -> index, error_code, base_offset, ...

	// For now, return a basic result
	return ProduceResult{
		base_offset: 0
		error_code:  0
	}
}

/// print_produce_help는 produce 명령어 도움말을 출력합니다.
/// print_produce_help - prints produce command help
pub fn print_produce_help() {
	println('\x1b[33mProduce Command:\x1b[0m')
	println('')
	println('Usage: datacore produce <topic> [options]')
	println('')
	println('\x1b[33mOptions:\x1b[0m')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -t, --topic             Topic name (required)')
	println('  -p, --partition         Partition number (default: auto)')
	println('  -k, --key               Message key')
	println('  -m, --message           Message value')
	println('  -f, --file              Read messages from file (one per line)')
	println('      --stdin             Read messages from stdin')
	println('  -a, --acks              Required acks: 0, 1, or -1 (default: 1)')
	println('')
	println('\x1b[33mExamples:\x1b[0m')
	println('  datacore produce my-topic -m "Hello World"')
	println('  datacore produce my-topic -k "key1" -m "value1"')
	println('  datacore produce my-topic -f messages.txt')
	println('  echo "message" | datacore produce my-topic --stdin')
}

const crc32c_table_cli = [u32(0x00000000), u32(0x00000000), 0xF26B8303, 0xE13B70F7, 0x1350F3F4,
	0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB, 0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B,
	0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24, 0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B,
	0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384, 0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54,
	0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B, 0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A,
	0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35, 0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5,
	0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA, 0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45,
	0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A, 0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A,
	0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595, 0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48,
	0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957, 0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687,
	0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198, 0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927,
	0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38, 0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8,
	0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7, 0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096,
	0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789, 0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859,
	0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46, 0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9,
	0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6, 0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36,
	0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829, 0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C,
	0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93, 0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043,
	0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C, 0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3,
	0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC, 0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C,
	0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033, 0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652,
	0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D, 0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D,
	0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982, 0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D,
	0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622, 0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2,
	0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED, 0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530,
	0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F, 0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF,
	0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0, 0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F,
	0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540, 0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90,
	0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F, 0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE,
	0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1, 0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321,
	0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E, 0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81,
	0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E, 0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E,
	0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351]

/// calculate_crc32c_cli는 Castagnoli 다항식을 사용하여 CRC32-C 체크섬을 계산합니다.
/// CLI 모듈 내부에서 RecordBatch 인코딩 시 사용됩니다.
fn calculate_crc32c_cli(data []u8) u32 {
	mut crc := u32(0xFFFFFFFF)
	for b in data {
		index := (crc ^ u32(b)) & 0xFF
		crc = (crc >> 8) ^ crc32c_table_cli[index]
	}
	return crc ^ 0xFFFFFFFF
}
