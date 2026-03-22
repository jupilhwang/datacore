// S3 Client 단위 테스트
// URL 생성, 호스트 헤더 생성, 에러 감지 로직 등을 테스트
// 실제 S3 호출 없이 요청 빌딩 로직만 검증
module s3

// --- get_endpoint() 테스트 ---

// test_get_endpoint_custom: 사용자 정의 엔드포인트가 있으면 그대로 반환
fn test_get_endpoint_custom() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:    'http://localhost:9000'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}

	result := adapter.get_endpoint()
	assert result == 'http://localhost:9000'
}

// test_get_endpoint_aws_path_style: AWS path-style URL 생성
fn test_get_endpoint_aws_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'my-bucket'
			region:         'ap-northeast-2'
			use_path_style: true
		}
	}

	result := adapter.get_endpoint()
	assert result == 'https://s3.ap-northeast-2.amazonaws.com'
}

// test_get_endpoint_aws_virtual_hosted: AWS virtual-hosted-style URL 생성
fn test_get_endpoint_aws_virtual_hosted() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'my-bucket'
			region:         'us-west-2'
			use_path_style: false
		}
	}

	result := adapter.get_endpoint()
	assert result == 'https://my-bucket.s3.us-west-2.amazonaws.com'
}

// --- get_host() 테스트 ---

// test_get_host_custom_endpoint_path_style: 사용자 정의 엔드포인트 + path-style 호스트
fn test_get_host_custom_endpoint_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://minio.local:9000'
			bucket_name:    'test-bucket'
			region:         'us-east-1'
			use_path_style: true
		}
	}

	result := adapter.get_host()
	assert result == 'minio.local:9000'
}

// test_get_host_custom_endpoint_virtual_hosted: 사용자 정의 엔드포인트 + virtual-hosted 호스트
fn test_get_host_custom_endpoint_virtual_hosted() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'http://s3.local:9000'
			bucket_name:    'test-bucket'
			region:         'us-east-1'
			use_path_style: false
		}
	}

	result := adapter.get_host()
	assert result == 'test-bucket.s3.local:9000'
}

// test_get_host_aws_path_style: AWS path-style 호스트
fn test_get_host_aws_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'my-bucket'
			region:         'eu-west-1'
			use_path_style: true
		}
	}

	result := adapter.get_host()
	assert result == 's3.eu-west-1.amazonaws.com'
}

// test_get_host_aws_virtual_hosted: AWS virtual-hosted-style 호스트
fn test_get_host_aws_virtual_hosted() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'my-bucket'
			region:         'eu-west-1'
			use_path_style: false
		}
	}

	result := adapter.get_host()
	assert result == 'my-bucket.s3.eu-west-1.amazonaws.com'
}

// test_get_host_custom_https_endpoint: HTTPS 프로토콜이 올바르게 제거되는지 확인
fn test_get_host_custom_https_endpoint() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       'https://s3.custom.io:443'
			bucket_name:    'secure-bucket'
			region:         'us-east-1'
			use_path_style: true
		}
	}

	result := adapter.get_host()
	assert result == 's3.custom.io:443'
}

// --- is_network_error() 테스트 ---

// test_is_network_error_socket_error: 소켓 에러 감지
fn test_is_network_error_socket_error() {
	err := error('socket error: connection failed')
	assert is_network_error(err) == true
}

// test_is_network_error_resolve: DNS 해석 실패 감지
fn test_is_network_error_resolve() {
	err := error('failed to resolve hostname')
	assert is_network_error(err) == true
}

// test_is_network_error_connection_refused: 연결 거부 감지
fn test_is_network_error_connection_refused() {
	err := error('connection refused')
	assert is_network_error(err) == true
}

// test_is_network_error_timeout: 타임아웃 감지
fn test_is_network_error_timeout() {
	err := error('request timed out')
	assert is_network_error(err) == true
}

// test_is_network_error_econnrefused: ECONNREFUSED 에러코드 감지
fn test_is_network_error_econnrefused() {
	err := error('ECONNREFUSED')
	assert is_network_error(err) == true
}

// test_is_network_error_econnreset: ECONNRESET 에러코드 감지
fn test_is_network_error_econnreset() {
	err := error('ECONNRESET')
	assert is_network_error(err) == true
}

// test_is_network_error_read_timeout: read timeout 감지
fn test_is_network_error_read_timeout() {
	err := error('read timeout occurred')
	assert is_network_error(err) == true
}

// test_is_network_error_write_timeout: write timeout 감지
fn test_is_network_error_write_timeout() {
	err := error('write timeout occurred')
	assert is_network_error(err) == true
}

// test_is_network_error_not_network: 네트워크가 아닌 일반 에러는 false
fn test_is_network_error_not_network() {
	err := error('invalid JSON format')
	assert is_network_error(err) == false
}

// test_is_network_error_s3_network_error_type: S3NetworkError 타입 감지
fn test_is_network_error_s3_network_error_type() {
	err := IError(S3NetworkError{
		detail: 'DNS lookup failed'
	})
	assert is_network_error(err) == true
}

// test_is_network_error_unreachable: 네트워크 도달 불가 감지
fn test_is_network_error_unreachable() {
	err := error('network is unreachable')
	assert is_network_error(err) == true
}

// test_is_network_error_deadline_exceeded: deadline exceeded 감지
fn test_is_network_error_deadline_exceeded() {
	err := error('context deadline exceeded')
	assert is_network_error(err) == true
}

// --- S3NetworkError 타입 테스트 ---

// test_s3_network_error_message: S3NetworkError 메시지 형식 확인
fn test_s3_network_error_message() {
	err := S3NetworkError{
		detail: 'DNS resolution failed for s3.amazonaws.com'
	}
	assert err.msg() == 'S3 network error: DNS resolution failed for s3.amazonaws.com'
}

// --- S3ETagMismatchError 타입 테스트 ---

// test_s3_etag_mismatch_error_message: ETag 불일치 에러 메시지 확인
fn test_s3_etag_mismatch_error_message() {
	err := S3ETagMismatchError{}
	assert err.msg() == 'etag_mismatch'
}

// --- parse_list_objects_response() 테스트 ---

// test_parse_list_objects_empty_body: 빈 XML 응답 파싱
fn test_parse_list_objects_empty_body() {
	result := parse_list_objects_response('')
	assert result.len == 0
}

// test_parse_list_objects_invalid_xml: 유효하지 않은 XML 파싱 (크래시 없이 빈 결과)
fn test_parse_list_objects_invalid_xml() {
	result := parse_list_objects_response('not xml at all')
	assert result.len == 0
}

// test_parse_list_objects_single_object: 단일 객체 파싱
fn test_parse_list_objects_single_object() {
	body := '<ListBucketResult><Contents><Key>data/test.parquet</Key><Size>1024</Size><ETag>"abc123"</ETag></Contents></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 1
	assert result[0].key == 'data/test.parquet'
	assert result[0].size == 1024
	assert result[0].etag == 'abc123'
}

// test_parse_list_objects_multiple_objects: 여러 객체 파싱
fn test_parse_list_objects_multiple_objects() {
	body := '<ListBucketResult><Contents><Key>a.txt</Key><Size>10</Size></Contents><Contents><Key>b.txt</Key><Size>20</Size></Contents><Contents><Key>c.txt</Key><Size>30</Size></Contents></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 3
	assert result[0].key == 'a.txt'
	assert result[1].key == 'b.txt'
	assert result[2].key == 'c.txt'
	assert result[0].size == 10
	assert result[2].size == 30
}

// test_parse_list_objects_no_contents: Contents가 없는 응답
fn test_parse_list_objects_no_contents() {
	body := '<ListBucketResult><Name>my-bucket</Name><Prefix>data/</Prefix></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 0
}

// --- parse_list_objects_page() 테스트 ---

// test_parse_list_objects_page_with_token: 페이지네이션 토큰 추출 확인
fn test_parse_list_objects_page_with_token() {
	body := '<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next-token-xyz</NextContinuationToken><Contents><Key>page1.dat</Key><Size>100</Size></Contents></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.is_truncated == true
	assert page.next_continuation_token == 'next-token-xyz'
	assert page.objects.len == 1
	assert page.objects[0].key == 'page1.dat'
}

// test_parse_list_objects_page_last_page: 마지막 페이지 (IsTruncated=false)
fn test_parse_list_objects_page_last_page() {
	body := '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>last.dat</Key></Contents></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.is_truncated == false
	assert page.next_continuation_token == ''
	assert page.objects.len == 1
}

// test_parse_list_objects_page_empty: 빈 페이지 응답
fn test_parse_list_objects_page_empty() {
	body := '<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.is_truncated == false
	assert page.objects.len == 0
}

// test_parse_list_objects_page_invalid: 유효하지 않은 XML
fn test_parse_list_objects_page_invalid() {
	page := parse_list_objects_page('garbage')
	assert page.objects.len == 0
	assert page.is_truncated == false
}

// --- 엔드포인트/호스트 조합 테스트 ---

// test_endpoint_and_host_consistency_path_style: path-style에서 엔드포인트와 호스트 일관성
fn test_endpoint_and_host_consistency_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'consistency-bucket'
			region:         'us-east-1'
			use_path_style: true
		}
	}

	endpoint := adapter.get_endpoint()
	host := adapter.get_host()

	// path-style에서 endpoint는 https://s3.region.amazonaws.com
	// host도 s3.region.amazonaws.com
	assert endpoint.contains(host)
}

// test_endpoint_and_host_consistency_virtual: virtual-hosted에서 엔드포인트와 호스트 일관성
fn test_endpoint_and_host_consistency_virtual() {
	adapter := S3StorageAdapter{
		config: S3Config{
			endpoint:       ''
			bucket_name:    'vhost-bucket'
			region:         'us-east-1'
			use_path_style: false
		}
	}

	endpoint := adapter.get_endpoint()
	host := adapter.get_host()

	// virtual-hosted에서 endpoint와 host 모두 bucket 이름을 포함해야 함
	assert endpoint.contains('vhost-bucket')
	assert host.contains('vhost-bucket')
}
