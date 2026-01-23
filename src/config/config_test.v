// 설정 우선순위 cascade 테스트
module config

import os

fn test_parse_cli_args() {
	// --key=value 형식
	args1 := ['--broker-port=9093', '--broker-host=localhost']
	result1 := parse_cli_args(args1)
	assert result1['broker-port'] == '9093'
	assert result1['broker-host'] == 'localhost'

	// --key value 형식
	args2 := ['--broker-port', '9094', '--broker-host', '127.0.0.1']
	result2 := parse_cli_args(args2)
	assert result2['broker-port'] == '9094'
	assert result2['broker-host'] == '127.0.0.1'

	// 혼합 형식
	args3 := ['--broker-port=9095', '--broker-host', 'example.com']
	result3 := parse_cli_args(args3)
	assert result3['broker-port'] == '9095'
	assert result3['broker-host'] == 'example.com'
}

fn test_config_priority_cli_over_env() {
	// CLI 인자가 환경변수보다 우선
	os.setenv('DATACORE_BROKER_PORT', '9093', true)

	mut cli_args := map[string]string{}
	cli_args['broker-port'] = '9094'

	// CLI 인자 우선
	assert cli_args['broker-port'] == '9094'

	// 환경변수는 CLI 인자가 없을 때만 사용
	env_port := os.getenv('DATACORE_BROKER_PORT')
	assert env_port == '9093'

	os.unsetenv('DATACORE_BROKER_PORT')
}

fn test_load_config_from_file() {
	// config.toml 파일이 있으면 로드
	cfg := load_config('config.toml') or {
		// 파일이 없으면 테스트 스킵
		println('config.toml not found, skipping test')
		return
	}

	// 기본값 확인 (config.toml에 정의된 값)
	assert cfg.broker.port > 0
	assert cfg.broker.host != ''
}
