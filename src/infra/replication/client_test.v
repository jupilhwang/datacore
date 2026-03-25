module replication

import domain
import time

// --- Client 단위 테스트 ---

// create_test_client는 테스트용 Client를 생성한다.
fn create_test_client(timeout_ms int) &Client {
	return Client.new(timeout_ms)
}

// test_client_new: Client 생성 시 필드 초기화 확인
fn test_client_new() {
	client := create_test_client(5000)

	assert client.timeout_ms == 5000
}

// test_client_new_default_timeout: 기본 타임아웃 값으로 Client 생성
fn test_client_new_default_timeout() {
	client := create_test_client(1000)

	assert client.timeout_ms == 1000
}

// test_client_close: close 호출이 패닉 없이 완료되는지 확인
fn test_client_close() {
	mut client := create_test_client(5000)
	client.close()
	// close()가 패닉 없이 완료되면 통과
	assert true
}

// test_send_to_unreachable_address: 연결 불가능한 주소로 send 시 에러 반환
fn test_send_to_unreachable_address() {
	mut client := create_test_client(500)
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'test-hb-001'
		sender_id:      'test-broker'
		timestamp:      time.now().unix_milli()
	}

	// 연결 불가능한 주소 사용 (존재하지 않는 포트)
	client.send('127.0.0.1:59999', msg) or {
		assert err.msg().contains('failed to connect')
		return
	}
	assert false, '연결 불가능한 주소에서 에러가 발생해야 함'
}

// test_send_with_retry_unreachable: 연결 불가능한 주소로 retry 시 모든 시도 실패 후 에러 반환
fn test_send_with_retry_unreachable() {
	mut client := create_test_client(200)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'retry-001'
		sender_id:      'test-broker'
		timestamp:      time.now().unix_milli()
		topic:          'retry-topic'
		partition:      0
		offset:         0
	}

	start := time.now()
	client.send_with_retry('127.0.0.1:59998', msg, 2) or {
		elapsed := time.since(start)
		// 2회 시도 후 에러 반환 확인
		assert err.msg().contains('all 2 attempts failed')
		// 최소 1회 백오프(100ms)가 있었는지 확인
		assert elapsed >= time.Duration(50 * time.millisecond), '백오프가 적용되어야 함'
		return
	}
	assert false, '모든 재시도 실패 후 에러가 발생해야 함'
}

// test_send_with_retry_zero_retries: max_retries=0이면 시도 없이 즉시 에러
fn test_send_with_retry_zero_retries() {
	mut client := create_test_client(200)
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'zero-retry-001'
		sender_id:      'test-broker'
		timestamp:      time.now().unix_milli()
	}

	client.send_with_retry('127.0.0.1:59997', msg, 0) or {
		assert err.msg().contains('all 0 attempts failed')
		return
	}
	assert false, '0회 재시도에서 에러가 발생해야 함'
}

// test_send_with_retry_single_attempt: max_retries=1이면 단 한 번만 시도
fn test_send_with_retry_single_attempt() {
	mut client := create_test_client(200)
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'single-retry-001'
		sender_id:      'test-broker'
		timestamp:      time.now().unix_milli()
	}

	client.send_with_retry('127.0.0.1:59996', msg, 1) or {
		assert err.msg().contains('all 1 attempts failed')
		return
	}
	assert false, '1회 재시도 실패 후 에러가 발생해야 함'
}

// test_send_async_does_not_block: send_async가 블로킹 없이 즉시 반환
fn test_send_async_does_not_block() {
	mut client := create_test_client(200)
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'async-001'
		sender_id:      'test-broker'
		timestamp:      time.now().unix_milli()
	}

	start := time.now()
	client.send_async('127.0.0.1:59995', msg)
	elapsed := time.since(start)

	// send_async는 spawn으로 백그라운드에서 실행하므로 즉시 반환해야 함
	assert elapsed < time.Duration(500 * time.millisecond), 'send_async는 500ms 이내에 반환해야 함'

	// 백그라운드 고루틴이 완료될 때까지 대기
	time.sleep(time.Duration(300 * time.millisecond))
}

// test_multiple_clients_independent: 여러 Client 인스턴스가 독립적으로 동작
fn test_multiple_clients_independent() {
	client1 := create_test_client(1000)
	client2 := create_test_client(2000)
	client3 := create_test_client(3000)

	assert client1.timeout_ms == 1000
	assert client2.timeout_ms == 2000
	assert client3.timeout_ms == 3000
}
