module replication

import domain
import time
import net
import log

// --- Server 단위 테스트 ---

// test_server_new: Server 생성 시 초기 상태 확인
fn test_server_new() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	server := Server.new(19200, handler)

	assert server.port == 19200
	assert server.running == false
}

// test_server_is_running_initial: 초기 상태에서 is_running()이 false 반환
fn test_server_is_running_initial() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	server := Server.new(19201, handler)

	assert server.is_running() == false
}

// test_server_start_stop_lifecycle: start/stop 라이프사이클이 정상 동작
fn test_server_start_stop_lifecycle() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut server := Server.new(19202, handler)

	server.start() or {
		assert false, 'server start 실패: ${err}'
		return
	}

	// 서버가 시작 상태인지 확인
	assert server.is_running() == true

	// 잠시 대기 (accept_loop이 시작될 시간)
	time.sleep(time.Duration(50 * time.millisecond))

	server.stop() or {
		assert false, 'server stop 실패: ${err}'
		return
	}

	assert server.is_running() == false
}

// test_server_double_start_returns_error: 이미 실행 중인 서버를 다시 시작하면 에러 반환
fn test_server_double_start_returns_error() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut server := Server.new(19203, handler)

	server.start() or {
		assert false, 'first start 실패: ${err}'
		return
	}

	time.sleep(time.Duration(50 * time.millisecond))

	// 두 번째 start는 에러를 반환해야 함
	server.start() or {
		assert err.msg().contains('already running')
		server.stop() or {}
		return
	}

	server.stop() or {}
	assert false, '이미 실행 중인 서버의 두 번째 start에서 에러가 발생해야 함'
}

// test_server_stop_when_not_running: 실행 중이 아닌 서버를 stop하면 조용히 반환
fn test_server_stop_when_not_running() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut server := Server.new(19204, handler)

	// 시작하지 않은 서버를 stop해도 에러가 발생하지 않아야 함
	server.stop() or {
		assert false, '미실행 서버의 stop에서 에러가 발생하면 안 됨: ${err}'
		return
	}
	assert server.is_running() == false
}

// test_server_accepts_tcp_connection: 서버가 TCP 연결을 수락하는지 확인
fn test_server_accepts_tcp_connection() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'test-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(19205, handler)

	server.start() or {
		assert false, 'server start 실패: ${err}'
		return
	}

	time.sleep(time.Duration(100 * time.millisecond))

	// TCP 연결 시도
	mut conn := net.dial_tcp('127.0.0.1:19205') or {
		server.stop() or {}
		assert false, '서버 연결 실패: ${err}'
		return
	}
	conn.close() or {}

	server.stop() or {}
	assert true
}

// test_server_message_handler_echo: 서버가 핸들러를 통해 메시지를 처리하고 응답 반환
fn test_server_message_handler_echo() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'echo-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(19206, handler)

	server.start() or {
		assert false, 'server start 실패: ${err}'
		return
	}

	time.sleep(time.Duration(100 * time.millisecond))

	// Client를 통해 메시지를 보내고 응답을 받는다
	mut client := Client.new(3000)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'echo-test-001'
		sender_id:      'test-client'
		timestamp:      time.now().unix_milli()
		topic:          'echo-topic'
		partition:      0
		offset:         42
	}

	response := client.send('127.0.0.1:19206', msg) or {
		server.stop() or {}
		assert false, 'send 실패: ${err}'
		return
	}

	assert response.msg_type == .replicate_ack
	assert response.correlation_id == 'echo-test-001'
	assert response.sender_id == 'echo-server'
	assert response.topic == 'echo-topic'
	assert response.partition == 0
	assert response.offset == 42
	assert response.success == true

	server.stop() or {}
}

// test_server_handler_error_response: 핸들러가 에러를 반환하면 에러 응답 전송
fn test_server_handler_error_response() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return error('handler processing failed')
	}
	mut server := Server.new(19207, handler)

	server.start() or {
		assert false, 'server start 실패: ${err}'
		return
	}

	time.sleep(time.Duration(100 * time.millisecond))

	mut client := Client.new(3000)
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'error-test-001'
		sender_id:      'test-client'
		timestamp:      time.now().unix_milli()
		topic:          'error-topic'
		partition:      0
		offset:         0
	}

	response := client.send('127.0.0.1:19207', msg) or {
		server.stop() or {}
		assert false, 'send 실패: ${err}'
		return
	}

	// 핸들러 에러 시 서버는 success=false 응답을 보내야 함
	assert response.success == false
	assert response.error_msg == 'handler processing failed'

	server.stop() or {}
}

// test_server_multiple_messages: 하나의 연결에서 여러 메시지를 순차 처리
fn test_server_multiple_messages() {
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return domain.ReplicationMessage{
			msg_type:       .replicate_ack
			correlation_id: msg.correlation_id
			sender_id:      'multi-server'
			timestamp:      time.now().unix_milli()
			topic:          msg.topic
			partition:      msg.partition
			offset:         msg.offset
			success:        true
		}
	}
	mut server := Server.new(19208, handler)

	server.start() or {
		assert false, 'server start 실패: ${err}'
		return
	}

	time.sleep(time.Duration(100 * time.millisecond))

	// Protocol을 직접 사용하여 하나의 연결에서 여러 메시지 전송
	p := Protocol.new()
	mut conn := net.dial_tcp('127.0.0.1:19208') or {
		server.stop() or {}
		assert false, '연결 실패: ${err}'
		return
	}

	conn.set_read_timeout(time.Duration(3000 * time.millisecond))
	conn.set_write_timeout(time.Duration(3000 * time.millisecond))

	for i in 0 .. 3 {
		msg := domain.ReplicationMessage{
			msg_type:       .replicate
			correlation_id: 'multi-${i}'
			sender_id:      'test-client'
			timestamp:      time.now().unix_milli()
			topic:          'multi-topic'
			partition:      i32(i)
			offset:         i64(i * 10)
		}

		p.write_message(mut conn, msg) or {
			conn.close() or {}
			server.stop() or {}
			assert false, 'write_message 실패 (${i}): ${err}'
			return
		}

		response := p.read_message(mut conn) or {
			conn.close() or {}
			server.stop() or {}
			assert false, 'read_message 실패 (${i}): ${err}'
			return
		}

		assert response.correlation_id == 'multi-${i}'
		assert response.partition == i32(i)
		assert response.offset == i64(i * 10)
	}

	conn.close() or {}
	server.stop() or {}
}
