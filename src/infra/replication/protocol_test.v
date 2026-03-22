module replication

import domain
import time
import encoding.binary

// --- Protocol encode/decode 라운드트립 테스트 ---

// create_test_message는 테스트용 ReplicationMessage를 생성한다.
fn create_test_message(msg_type domain.ReplicationType) domain.ReplicationMessage {
	return domain.ReplicationMessage{
		msg_type:       msg_type
		correlation_id: 'test-corr-001'
		sender_id:      'broker-1'
		timestamp:      time.now().unix_milli()
		topic:          'test-topic'
		partition:      0
		offset:         100
		records_data:   'test-data'.bytes()
		success:        true
		error_msg:      ''
	}
}

// test_encode_decode_replicate: replicate 타입 메시지의 encode/decode 라운드트립
fn test_encode_decode_replicate() {
	p := Protocol.new()
	msg := create_test_message(.replicate)

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	assert encoded.len > 4, 'encoded 데이터는 4바이트 길이 헤더보다 커야 함'

	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.msg_type == .replicate
	assert decoded.correlation_id == msg.correlation_id
	assert decoded.sender_id == msg.sender_id
	assert decoded.topic == msg.topic
	assert decoded.partition == msg.partition
	assert decoded.offset == msg.offset
	assert decoded.success == msg.success
}

// test_encode_decode_replicate_ack: replicate_ack 타입의 라운드트립
fn test_encode_decode_replicate_ack() {
	p := Protocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate_ack
		correlation_id: 'ack-corr-002'
		sender_id:      'broker-2'
		timestamp:      time.now().unix_milli()
		topic:          'ack-topic'
		partition:      1
		offset:         200
		success:        true
		error_msg:      ''
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.msg_type == .replicate_ack
	assert decoded.correlation_id == 'ack-corr-002'
	assert decoded.sender_id == 'broker-2'
	assert decoded.offset == 200
	assert decoded.success == true
}

// test_encode_decode_flush_ack: flush_ack 타입의 라운드트립
fn test_encode_decode_flush_ack() {
	p := Protocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .flush_ack
		correlation_id: 'flush-corr-003'
		sender_id:      'broker-3'
		timestamp:      time.now().unix_milli()
		topic:          'flush-topic'
		partition:      2
		offset:         300
		success:        true
		error_msg:      ''
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.msg_type == .flush_ack
	assert decoded.correlation_id == 'flush-corr-003'
	assert decoded.topic == 'flush-topic'
	assert decoded.partition == 2
	assert decoded.offset == 300
}

// test_encode_decode_heartbeat: heartbeat 타입의 라운드트립
fn test_encode_decode_heartbeat() {
	p := Protocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'hb-corr-004'
		sender_id:      'broker-4'
		timestamp:      time.now().unix_milli()
		topic:          ''
		partition:      0
		offset:         0
		success:        false
		error_msg:      ''
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.msg_type == .heartbeat
	assert decoded.sender_id == 'broker-4'
	assert decoded.topic == ''
}

// test_encode_length_prefix: 인코딩된 데이터의 처음 4바이트가 올바른 길이 프리픽스인지 확인
fn test_encode_length_prefix() {
	p := Protocol.new()
	msg := create_test_message(.replicate)

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}

	// 처음 4바이트에서 페이로드 길이를 읽는다
	payload_len := int(binary.big_endian_u32(encoded[0..4]))

	// 페이로드 길이 + 4바이트 헤더 == 전체 인코딩 길이
	assert payload_len + 4 == encoded.len, '길이 프리픽스(${payload_len})와 실제 페이로드 크기(${encoded.len - 4})가 다름'
}

// test_decode_too_short: 4바이트 미만의 데이터로 decode 시 에러 반환
fn test_decode_too_short() {
	p := Protocol.new()

	// 빈 데이터
	p.decode([]u8{}) or {
		assert err.msg().contains('too short')
		return
	}
	assert false, '빈 데이터에서 에러가 발생해야 함'
}

// test_decode_truncated_payload: 헤더는 있지만 페이로드가 불완전한 경우
fn test_decode_truncated_payload() {
	p := Protocol.new()
	msg := create_test_message(.replicate)

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}

	// 페이로드를 절반으로 잘라낸다
	truncated := encoded[..encoded.len / 2]

	p.decode(truncated) or {
		assert err.msg().contains('expected')
		return
	}
	assert false, '잘린 데이터에서 에러가 발생해야 함'
}

// test_decode_3_bytes: 정확히 3바이트(헤더 미완성)로 decode 시 에러 반환
fn test_decode_3_bytes() {
	p := Protocol.new()

	p.decode([u8(0), 0, 1]) or {
		assert err.msg().contains('too short')
		return
	}
	assert false, '3바이트 데이터에서 에러가 발생해야 함'
}

// test_encode_decode_with_error_message: error_msg가 포함된 메시지의 라운드트립
fn test_encode_decode_with_error_message() {
	p := Protocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate_ack
		correlation_id: 'err-corr-005'
		sender_id:      'broker-5'
		timestamp:      time.now().unix_milli()
		topic:          'error-topic'
		partition:      0
		offset:         0
		success:        false
		error_msg:      'connection timeout to replica'
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.success == false
	assert decoded.error_msg == 'connection timeout to replica'
}

// test_decode_unknown_msg_type: 알 수 없는 msg_type 값이면 에러 반환
fn test_decode_unknown_msg_type() {
	p := Protocol.new()

	// msg_type을 "invalid"로 설정한 JSON을 직접 인코딩
	json_str := '{"msg_type":"invalid","correlation_id":"x","sender_id":"b","timestamp":"0","topic":"","partition":"0","offset":"0","success":"false","error_msg":""}'
	json_bytes := json_str.bytes()

	mut buf := []u8{len: 4 + json_bytes.len}
	binary.big_endian_put_u32(mut buf[0..4], u32(json_bytes.len))
	copy(mut buf[4..], json_bytes)

	p.decode(buf) or {
		assert err.msg().contains('unknown msg_type')
		return
	}
	assert false, '알 수 없는 msg_type에서 에러가 발생해야 함'
}

// test_encode_decode_with_large_payload: 큰 페이로드 메시지의 라운드트립
fn test_encode_decode_with_large_payload() {
	p := Protocol.new()

	// 큰 상관관계 ID와 큰 토픽으로 메시지 크기를 늘린다
	long_topic := 'a'.repeat(1000)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'large-corr-' + 'x'.repeat(200)
		sender_id:      'broker-large'
		timestamp:      time.now().unix_milli()
		topic:          long_topic
		partition:      99
		offset:         999999
		success:        true
		error_msg:      ''
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.topic == long_topic
	assert decoded.partition == 99
	assert decoded.offset == 999999
}

// test_encode_decode_empty_fields: 모든 필드가 빈 상태인 메시지의 라운드트립
fn test_encode_decode_empty_fields() {
	p := Protocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: ''
		sender_id:      ''
		timestamp:      0
		topic:          ''
		partition:      0
		offset:         0
		success:        false
		error_msg:      ''
	}

	encoded := p.encode(msg) or {
		assert false, 'encode 실패: ${err}'
		return
	}
	decoded := p.decode(encoded) or {
		assert false, 'decode 실패: ${err}'
		return
	}

	assert decoded.msg_type == .heartbeat
	assert decoded.correlation_id == ''
	assert decoded.sender_id == ''
	assert decoded.timestamp == 0
}

// test_multiple_sequential_encode_decode: 여러 메시지를 순차적으로 encode/decode
fn test_multiple_sequential_encode_decode() {
	p := Protocol.new()
	types := [domain.ReplicationType.replicate, .replicate_ack, .flush_ack, .heartbeat]

	for i, t in types {
		msg := domain.ReplicationMessage{
			msg_type:       t
			correlation_id: 'seq-${i}'
			sender_id:      'broker-${i}'
			timestamp:      time.now().unix_milli()
			topic:          'topic-${i}'
			partition:      i32(i)
			offset:         i64(i * 100)
			success:        i % 2 == 0
			error_msg:      ''
		}

		encoded := p.encode(msg) or {
			assert false, 'encode 실패 (index ${i}): ${err}'
			return
		}
		decoded := p.decode(encoded) or {
			assert false, 'decode 실패 (index ${i}): ${err}'
			return
		}

		assert decoded.msg_type == t
		assert decoded.correlation_id == 'seq-${i}'
		assert decoded.partition == i32(i)
		assert decoded.offset == i64(i * 100)
	}
}
