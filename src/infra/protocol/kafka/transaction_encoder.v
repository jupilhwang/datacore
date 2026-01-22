// Kafka 프로토콜 - 트랜잭션 인코더
// 트랜잭션 관련 응답의 인코딩 메서드
module kafka

/// encode는 InitProducerIdResponse를 인코딩합니다 (API Key 22).
pub fn (r InitProducerIdResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms: INT32 (v0+) - 스로틀링 시간
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 (v0+) - 에러 코드
	writer.write_i16(r.error_code)

	// producer_id: INT64 (v0+) - 프로듀서 ID
	writer.write_i64(r.producer_id)

	// producer_epoch: INT16 (v0+) - 프로듀서 에포크
	writer.write_i16(r.producer_epoch)

	// flexible 버전의 태그된 필드
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 AddPartitionsToTxnResponse를 인코딩합니다 (API Key 24).
pub fn (r AddPartitionsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		if is_flexible {
			writer.write_compact_string(res.name)
			writer.write_compact_array_len(res.partitions.len)
		} else {
			writer.write_string(res.name)
			writer.write_array_len(res.partitions.len)
		}

		for p in res.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 AddOffsetsToTxnResponse를 인코딩합니다 (API Key 25).
pub fn (r AddOffsetsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 EndTxnResponse를 인코딩합니다 (API Key 26).
pub fn (r EndTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 WriteTxnMarkersResponse를 인코딩합니다 (API Key 27).
pub fn (r WriteTxnMarkersResponse) encode(version i16) []u8 {
	// v1은 항상 flexible
	is_flexible := version >= 1
	mut writer := new_writer()

	if is_flexible {
		writer.write_compact_array_len(r.markers.len)
	} else {
		writer.write_array_len(r.markers.len)
	}

	for marker in r.markers {
		writer.write_i64(marker.producer_id)

		if is_flexible {
			writer.write_compact_array_len(marker.topics.len)
		} else {
			writer.write_array_len(marker.topics.len)
		}

		for t in marker.topics {
			if is_flexible {
				writer.write_compact_string(t.name)
				writer.write_compact_array_len(t.partitions.len)
			} else {
				writer.write_string(t.name)
				writer.write_array_len(t.partitions.len)
			}

			for p in t.partitions {
				writer.write_i32(p.partition_index)
				writer.write_i16(p.error_code)

				if is_flexible {
					writer.write_tagged_fields()
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 TxnOffsetCommitResponse를 인코딩합니다 (API Key 28).
pub fn (r TxnOffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
