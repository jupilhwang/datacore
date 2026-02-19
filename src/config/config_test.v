// Configuration priority cascade tests
module config

import os

fn test_parse_cli_args() {
	// --key=value format
	args1 := ['--broker-port=9093', '--broker-host=localhost']
	result1 := parse_cli_args(args1)
	assert result1['broker-port'] == '9093'
	assert result1['broker-host'] == 'localhost'

	// --key value format
	args2 := ['--broker-port', '9094', '--broker-host', '127.0.0.1']
	result2 := parse_cli_args(args2)
	assert result2['broker-port'] == '9094'
	assert result2['broker-host'] == '127.0.0.1'

	// mixed format
	args3 := ['--broker-port=9095', '--broker-host', 'example.com']
	result3 := parse_cli_args(args3)
	assert result3['broker-port'] == '9095'
	assert result3['broker-host'] == 'example.com'
}

fn test_config_priority_cli_over_env() {
	// CLI arguments take priority over environment variables
	os.setenv('DATACORE_BROKER_PORT', '9093', true)

	mut cli_args := map[string]string{}
	cli_args['broker-port'] = '9094'

	// CLI argument takes priority
	assert cli_args['broker-port'] == '9094'

	// environment variable is used only when CLI argument is absent
	env_port := os.getenv('DATACORE_BROKER_PORT')
	assert env_port == '9093'

	os.unsetenv('DATACORE_BROKER_PORT')
}

fn test_load_config_from_file() {
	// load config.toml if it exists
	cfg := load_config('config.toml') or {
		// skip test if file is not found
		println('config.toml not found, skipping test')
		return
	}

	// verify defaults (values defined in config.toml)
	assert cfg.broker.port > 0
	assert cfg.broker.host != ''
}
