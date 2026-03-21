// Iceberg Config/Runtime 연결 및 FormatVersion 일관성 테스트.
// Problem 1: is_iceberg_enabled()가 config의 enabled 필드를 무시하는 문제.
// Problem 2: writer가 format_version을 하드코딩(2)하지 않고 config에서 읽어야 함.
module s3

import os

// --- is_iceberg_enabled 테스트 ---

// config.enabled=false, env var 없음 -> false 반환 검증
fn test_is_iceberg_enabled_false_when_config_disabled_no_env() {
	// 환경 변수가 설정되지 않았음을 보장
	os.unsetenv('DATACORE_ICEBERG_ENABLED')

	config := IcebergConfig{
		enabled: false
	}
	result := is_iceberg_enabled_with_config(config)
	assert result == false, 'config.enabled=false, env var 없음이면 false여야 함'
}

// config.enabled=true, env var 없음 -> true 반환 검증
fn test_is_iceberg_enabled_true_when_config_enabled() {
	os.unsetenv('DATACORE_ICEBERG_ENABLED')

	config := IcebergConfig{
		enabled: true
	}
	result := is_iceberg_enabled_with_config(config)
	assert result == true, 'config.enabled=true이면 true여야 함'
}

// env var=true, config.enabled=false -> env var가 우선하여 true 반환 검증
fn test_is_iceberg_enabled_env_var_overrides_config() {
	os.setenv('DATACORE_ICEBERG_ENABLED', 'true', true)
	defer {
		os.unsetenv('DATACORE_ICEBERG_ENABLED')
	}

	config := IcebergConfig{
		enabled: false
	}
	result := is_iceberg_enabled_with_config(config)
	assert result == true, 'env var=true이면 config.enabled=false를 오버라이드하여 true여야 함'
}

// env var=false, config.enabled=true -> env var가 우선하여 false 반환 검증
fn test_is_iceberg_enabled_env_var_false_overrides_config_true() {
	os.setenv('DATACORE_ICEBERG_ENABLED', 'false', true)
	defer {
		os.unsetenv('DATACORE_ICEBERG_ENABLED')
	}

	config := IcebergConfig{
		enabled: true
	}
	result := is_iceberg_enabled_with_config(config)
	assert result == false, 'env var=false이면 config.enabled=true를 오버라이드하여 false여야 함'
}

// env var=1 (숫자 형식) -> true 반환 검증
fn test_is_iceberg_enabled_env_var_numeric_one() {
	os.setenv('DATACORE_ICEBERG_ENABLED', '1', true)
	defer {
		os.unsetenv('DATACORE_ICEBERG_ENABLED')
	}

	config := IcebergConfig{
		enabled: false
	}
	result := is_iceberg_enabled_with_config(config)
	assert result == true, "env var='1'이면 true여야 함"
}

// --- IcebergMetadata 기본 format_version 테스트 ---

// format_version 미지정 시 0이 되므로, create_writer_metadata에서 2로 보정되는지 검증
fn test_iceberg_metadata_default_format_version_via_writer() {
	config := IcebergConfig{
		enabled:           true
		format:            'parquet'
		compression:       'zstd'
		write_mode:        'append'
		max_rows_per_file: 1000000
		max_file_size_mb:  128
	}
	writer := new_iceberg_writer(&S3StorageAdapter{}, config, 's3://test/table') or {
		panic('writer creation failed: ${err}')
	}
	assert writer.table_metadata.format_version == 2, 'format_version must be 2 when not explicitly set'
}

// --- new_iceberg_writer format_version 테스트 ---

// new_iceberg_writer가 config의 format_version을 사용하는지 검증
fn test_new_iceberg_writer_uses_config_format_version() {
	config := IcebergConfig{
		enabled:           true
		format:            'parquet'
		compression:       'zstd'
		write_mode:        'append'
		partition_by:      ['timestamp', 'topic']
		max_rows_per_file: 1000000
		max_file_size_mb:  128
		schema_evolution:  true
		format_version:    3
	}

	writer := new_iceberg_writer(&S3StorageAdapter{}, config, 's3://test/table') or {
		panic('writer 생성 실패: ${err}')
	}
	assert writer.table_metadata.format_version == 3, 'writer는 config의 format_version=3을 사용해야 함'
}

// format_version을 지정하지 않으면 기본값 2가 사용되는지 검증
fn test_new_iceberg_writer_default_format_version_is_2() {
	config := IcebergConfig{
		enabled:           true
		format:            'parquet'
		compression:       'zstd'
		write_mode:        'append'
		max_rows_per_file: 1000000
		max_file_size_mb:  128
	}

	writer := new_iceberg_writer(&S3StorageAdapter{}, config, 's3://test/table') or {
		panic('writer 생성 실패: ${err}')
	}
	assert writer.table_metadata.format_version == 2, 'format_version 미지정 시 기본값 2여야 함'
}
