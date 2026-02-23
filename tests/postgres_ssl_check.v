// PostgreSQL SSL 연결 테스트 프로그램
// 환경 변수 설정을 확인하고 테스트 가이드를 제공합니다.
module main

import os

fn main() {
	println('=================================')
	println('PostgreSQL SSL Connection Test')
	println('=================================')
	println('')

	// 환경 변수 확인
	host := os.getenv_opt('DATACORE_PG_HOST') or {
		println('❌ Environment variables not configured')
		println('')
		println('To test PostgreSQL SSL connections, set:')
		println('')
		println('  export DATACORE_PG_HOST=localhost')
		println('  export DATACORE_PG_PORT=5432')
		println('  export DATACORE_PG_USER=your_user')
		println('  export DATACORE_PG_PASSWORD=your_password')
		println('  export DATACORE_PG_DATABASE=datacore_test')
		println('  export DATACORE_PG_SSLMODE=disable  # or require, prefer, etc.')
		println('')
		println('Then run the actual tests:')
		println('  v test src/infra/storage/plugins/postgres/adapter_test.v')
		println('')
		exit(0)
	}

	port_str := os.getenv_opt('DATACORE_PG_PORT') or { '5432' }
	user := os.getenv_opt('DATACORE_PG_USER') or { 'not_set' }
	database := os.getenv_opt('DATACORE_PG_DATABASE') or { 'datacore_test' }
	sslmode := os.getenv_opt('DATACORE_PG_SSLMODE') or { 'disable' }

	println('✅ Environment variables configured:')
	println('')
	println('  DATACORE_PG_HOST:     ${host}')
	println('  DATACORE_PG_PORT:     ${port_str}')
	println('  DATACORE_PG_USER:     ${user}')
	println('  DATACORE_PG_DATABASE: ${database}')
	println('  DATACORE_PG_SSLMODE:  ${sslmode}')
	println('')

	println('📋 SSL Mode: ${sslmode}')
	match sslmode {
		'disable' {
			println('  ℹ️  SSL is disabled - connection is not encrypted')
		}
		'allow' {
			println('  ℹ️  SSL is allowed - server decides encryption')
		}
		'prefer' {
			println('  ℹ️  SSL is preferred - encrypted if server supports')
		}
		'require' {
			println('  ✅ SSL is required - connection is encrypted')
		}
		'verify-ca' {
			println('  ✅ SSL with CA verification - connection is encrypted and verified')
		}
		'verify-full' {
			println('  ✅ SSL with full verification - connection is fully verified')
		}
		else {
			println('  ⚠️  Unknown SSL mode: ${sslmode}')
		}
	}
	println('')

	println('🧪 To run the actual PostgreSQL tests:')
	println('')
	println('  # Run all PostgreSQL tests')
	println('  v test src/infra/storage/plugins/postgres/adapter_test.v')
	println('')
	println('  # Run specific SSL tests')
	println('  v test src/infra/storage/plugins/postgres/adapter_test.v -run test_ssl')
	println('')
	println('  # Or use the test script')
	println('  ./tests/postgres_ssl_test.sh')
	println('')
}
