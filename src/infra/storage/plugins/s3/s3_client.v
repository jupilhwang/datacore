// Infra Layer - S3 클라이언트
// S3 스토리지를 위한 저수준 HTTP 작업
// AWS SigV4 서명 요청 및 S3 API 작업 제공
module s3

import crypto.sha256
import crypto.hmac
import net.http
import time

// S3 HTTP 요청 재시도 설정 상수
const max_retries = 3
const initial_backoff_ms = 100
const max_backoff_jitter_ms = 50

/// S3Object는 S3 목록 조회 결과의 객체를 나타냅니다.
/// ListObjectsV2 API 응답에서 파싱된 개별 객체 정보를 담습니다.
pub struct S3Object {
pub:
	key           string    // 객체 키 (S3 버킷 내 경로)
	size          i64       // 객체 크기 (바이트)
	last_modified time.Time // 마지막 수정 시간 (UTC)
	etag          string    // ETag (객체 무결성 검증용 해시)
}

// ============================================================
// S3 HTTP 작업 (S3 HTTP Operations)
// ============================================================

/// get_object는 S3에서 객체를 조회합니다.
/// start와 end 파라미터로 Range 요청을 지원합니다.
/// 반환값: (객체 데이터, ETag)
/// start가 음수이면 전체 객체를 조회합니다.
fn (mut a S3StorageAdapter) get_object(key string, start i64, end i64) !([]u8, string) {
	endpoint := a.get_endpoint()
	// Use global config to work around V struct copy issues
	url := if g_s3_config.use_path_style {
		'${endpoint}/${g_s3_config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	// 헤더 준비
	mut headers := a.sign_request('GET', key, '', []u8{})

	// start가 음수가 아니면 Range 헤더 추가
	if start >= 0 {
		range_val := if end > start { 'bytes=${start}-${end}' } else { 'bytes=${start}-' }
		headers.add_custom('Range', range_val) or {}
	}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .get
		header: headers
	}) or { return error('S3 GET failed: ${err}') }

	if resp.status_code == 404 {
		return error('Object not found: ${key}')
	}
	// Range 요청의 경우 206 Partial Content도 성공
	if resp.status_code != 200 && resp.status_code != 206 {
		return error('S3 GET failed with status ${resp.status_code}')
	}

	etag := resp.header.get(.etag) or { '' }
	return resp.body.bytes(), etag
}

/// put_object는 S3에 객체를 씁니다.
/// 내부적으로 재시도 로직이 포함된 put_object_with_retry를 호출합니다.
fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	a.put_object_with_retry(key, data, 3)!
}

/// put_object_with_retry는 지수 백오프 재시도로 객체 쓰기를 시도합니다.
/// 500, 503 에러 발생 시 지수 백오프(100ms, 200ms, 400ms...)로 재시도합니다.
/// 지터(jitter)를 추가하여 동시 재시도로 인한 충돌을 방지합니다.
fn (mut a S3StorageAdapter) put_object_with_retry(key string, data []u8, max_retries int) ! {
	endpoint := a.get_endpoint()
	// Use global config to work around V struct copy issues
	url := if g_s3_config.use_path_style {
		'${endpoint}/${g_s3_config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut last_err := ''
	for attempt in 0 .. max_retries {
		headers := a.sign_request('PUT', key, '', data)

		resp := http.fetch(http.FetchConfig{
			url:    url
			method: .put
			header: headers
			data:   data.bytestr()
		}) or {
			last_err = 'S3 PUT failed: ${err}'
			if attempt < max_retries - 1 {
				// 지수 백오프: 100ms, 200ms, 400ms...
				backoff_ms := initial_backoff_ms * (1 << attempt)
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}
			return error(last_err)
		}

		if resp.status_code in [200, 201, 204] {
			return
		}

		// 503 (Service Unavailable / 스로틀링) 및 500 (Server Error)에서 재시도
		if resp.status_code in [500, 503] && attempt < max_retries - 1 {
			last_err = 'S3 PUT failed with status ${resp.status_code}'
			// 지터가 있는 지수 백오프
			backoff_ms := initial_backoff_ms * (1 << attempt) +
				int(time.now().unix_milli() % max_backoff_jitter_ms)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		return error('S3 PUT failed with status ${resp.status_code}')
	}
	return error(last_err)
}

/// put_object_if_not_exists는 객체가 존재하지 않을 때만 씁니다 (조건부 PUT).
/// If-None-Match: * 헤더를 사용하여 동시 생성을 방지합니다.
/// 객체가 이미 존재하면 412 Precondition Failed 에러를 반환합니다.
fn (mut a S3StorageAdapter) put_object_if_not_exists(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	// Use global config to work around V struct copy issues
	url := if g_s3_config.use_path_style {
		'${endpoint}/${g_s3_config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut headers := a.sign_request('PUT', key, '', data)
	headers.add_custom('If-None-Match', '*') or {}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .put
		header: headers
		data:   data.bytestr()
	}) or { return error('S3 PUT failed: ${err}') }

	if resp.status_code == 412 {
		return error('Object already exists (precondition failed)')
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

/// delete_object는 S3에서 객체를 삭제합니다.
/// 삭제 성공 시 200 또는 204 상태 코드를 반환합니다.
fn (mut a S3StorageAdapter) delete_object(key string) ! {
	endpoint := a.get_endpoint()
	// Use global config to work around V struct copy issues
	url := '${endpoint}/${g_s3_config.bucket_name}/${key}'

	headers := a.sign_request('DELETE', key, '', []u8{})

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .delete
		header: headers
	}) or { return error('S3 DELETE failed: ${err}') }

	if resp.status_code !in [200, 204] {
		return error('S3 DELETE failed with status ${resp.status_code}')
	}
}

/// delete_objects_with_prefix는 지정된 접두사를 가진 모든 객체를 삭제합니다.
/// 먼저 접두사로 객체 목록을 조회한 후 각 객체를 개별 삭제합니다.
fn (mut a S3StorageAdapter) delete_objects_with_prefix(prefix string) ! {
	objects := a.list_objects(prefix)!
	for obj in objects {
		a.delete_object(obj.key) or {}
	}
}

/// list_objects는 지정된 접두사로 S3 객체 목록을 조회합니다.
/// ListObjectsV2 API를 사용하여 객체 목록을 가져옵니다.
/// OpenSSL 에러 등의 네트워크 문제에 대비하여 재시도 로직을 포함합니다.
fn (mut a S3StorageAdapter) list_objects(prefix string) ![]S3Object {
	endpoint := a.get_endpoint()
	query := 'prefix=${prefix}&list-type=2'
	// Use global config to work around V struct copy issues
	url := '${endpoint}/${g_s3_config.bucket_name}?${query}'

	mut last_err := ''

	for attempt in 0 .. max_retries {
		if attempt > 0 {
			eprintln('[S3] LIST retry ${attempt}/${max_retries} for prefix="${prefix}"')
		}

		headers := a.sign_request('GET', '', query, []u8{})

		resp := http.fetch(http.FetchConfig{
			url:    url
			method: .get
			header: headers
		}) or {
			last_err = 'S3 LIST failed: ${err}'
			eprintln('[S3] LIST error (attempt ${attempt + 1}): ${err}')

			if attempt < max_retries - 1 {
				// 지수 백오프: 100ms, 200ms, 400ms
				backoff_ms := initial_backoff_ms * (1 << attempt)
				eprintln('[S3] Waiting ${backoff_ms}ms before retry...')
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}
			return error(last_err)
		}

		if resp.status_code == 200 {
			// 성공 - XML 응답 파싱
			return parse_list_objects_response(resp.body)
		}

		// 503 (Service Unavailable) 및 500 (Server Error)에서 재시도
		if resp.status_code in [500, 503] && attempt < max_retries - 1 {
			last_err = 'S3 LIST failed with status ${resp.status_code}'
			eprintln('[S3] LIST status ${resp.status_code}, retrying...')

			// 지터가 있는 지수 백오프
			backoff_ms := initial_backoff_ms * (1 << attempt) +
				int(time.now().unix_milli() % max_backoff_jitter_ms)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		return error('S3 LIST failed with status ${resp.status_code}')
	}

	return error(last_err)
}

// ============================================================
// AWS SigV4 서명 (AWS SigV4 Signing)
// ============================================================

/// sign_request는 AWS Signature V4를 사용하여 HTTP 요청에 서명합니다.
/// AWS S3 API 호출에 필요한 인증 헤더를 생성합니다.
/// 서명 과정: Canonical Request -> String to Sign -> Signing Key -> Signature
fn (a &S3StorageAdapter) sign_request(method string, key string, query string, body []u8) http.Header {
	mut h := http.Header{}
	now := time.now().as_utc()

	// UTC 시간 보장을 위한 수동 포맷팅
	date_day := now.custom_format('YYYYMMDD')
	hours := now.hour
	minutes := now.minute
	seconds := now.second
	date_str := '${date_day}T${hours:02}${minutes:02}${seconds:02}Z'

	h.add_custom('x-amz-date', date_str) or {}
	host := a.get_host()
	h.add_custom('Host', host) or {}

	payload_hash := sha256.sum(body).hex()
	h.add_custom('x-amz-content-sha256', payload_hash) or {}

	if body.len > 0 {
		h.add_custom('Content-Length', body.len.str()) or {}
	}

	// Use global config to work around V struct copy issues
	if g_s3_config.access_key.len == 0 || g_s3_config.secret_key.len == 0 {
		return h
	}

	// Canonical Request
	canonical_uri := if key == '' {
		'/${g_s3_config.bucket_name}'
	} else if key.starts_with('/') {
		'/${g_s3_config.bucket_name}${key}'
	} else {
		'/${g_s3_config.bucket_name}/${key}'
	}
	canonical_querystring := a.canonicalize_query(query)

	canonical_headers := 'host:${host}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${date_str}\n'
	signed_headers := 'host;x-amz-content-sha256;x-amz-date'

	canonical_request := '${method}\n${canonical_uri}\n${canonical_querystring}\n${canonical_headers}\n${signed_headers}\n${payload_hash}'

	// String to Sign
	algorithm := 'AWS4-HMAC-SHA256'
	credential_scope := '${date_day}/${g_s3_config.region}/s3/aws4_request'
	canonical_request_hash := sha256.sum(canonical_request.bytes()).hex()

	string_to_sign := '${algorithm}\n${date_str}\n${credential_scope}\n${canonical_request_hash}'

	// Signing Key
	k_date := hmac.new(('AWS4' + g_s3_config.secret_key).bytes(), date_day.bytes(), sha256.sum,
		64)
	k_region := hmac.new(k_date, g_s3_config.region.bytes(), sha256.sum, 64)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, 64)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, 64)

	// Signature
	signature := hmac.new(k_signing, string_to_sign.bytes(), sha256.sum, 64).hex()

	auth_header := '${algorithm} Credential=${g_s3_config.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
	h.add_custom('Authorization', auth_header) or {}

	return h
}

/// get_endpoint는 S3 엔드포인트 URL을 반환합니다.
/// 사용자 정의 엔드포인트(MinIO/LocalStack)가 설정되면 해당 값을 사용합니다.
/// 그렇지 않으면 AWS S3 엔드포인트를 경로 스타일 또는 가상 호스트 스타일로 반환합니다.
fn (a &S3StorageAdapter) get_endpoint() string {
	// Use global config to work around V struct copy issues
	if g_s3_config.endpoint.len > 0 {
		return g_s3_config.endpoint
	}
	if g_s3_config.use_path_style {
		return 'https://s3.${g_s3_config.region}.amazonaws.com'
	} else {
		return 'https://${g_s3_config.bucket_name}.s3.${g_s3_config.region}.amazonaws.com'
	}
}

/// get_host는 S3 요청의 Host 헤더 값을 반환합니다.
/// SigV4 서명에서 Host 헤더는 필수 서명 헤더입니다.
fn (a &S3StorageAdapter) get_host() string {
	// Use global config to work around V struct copy issues
	if g_s3_config.endpoint.len > 0 {
		return g_s3_config.endpoint.replace('http://', '').replace('https://', '').split('/')[0]
	}
	return 's3.${g_s3_config.region}.amazonaws.com'
}

/// canonicalize_query는 AWS SigV4를 위해 쿼리 파라미터를 정렬하고 인코딩합니다.
/// 쿼리 파라미터를 알파벳 순으로 정렬하고 URL 인코딩을 적용합니다.
fn (a &S3StorageAdapter) canonicalize_query(query string) string {
	if query == '' {
		return ''
	}

	// 쿼리 문자열을 맵으로 파싱
	mut params := map[string]string{}
	for pair in query.split('&') {
		parts := pair.split_nth('=', 2)
		if parts.len == 2 {
			// AWS SigV4를 위해 키와 값을 URL 디코딩 후 재인코딩
			key := url_decode(parts[0])
			value := url_decode(parts[1])
			params[key] = value
		}
	}

	// 키를 알파벳 순으로 정렬
	mut keys := []string{}
	for k in params.keys() {
		keys << k
	}
	keys.sort()

	// canonical 쿼리 문자열 생성
	mut result := []string{}
	for key in keys {
		// AWS SigV4는 특정 인코딩 필요
		encoded_key := url_encode_for_sigv4(key)
		encoded_value := url_encode_for_sigv4(params[key])
		result << '${encoded_key}=${encoded_value}'
	}

	return result.join('&')
}

// ============================================================
// URL 인코딩 유틸리티 (URL Encoding Utilities)
// ============================================================

/// url_decode는 퍼센트 인코딩된 문자열을 디코딩합니다.
/// 예: %20 -> 공백, %2F -> /
fn url_decode(s string) string {
	mut result := s
	mut i := 0
	for i < result.len {
		if result[i] == u8(`%`) && i + 2 < result.len {
			hex_str := result[i + 1..i + 3]
			if is_hex_char(hex_str[0]) && is_hex_char(hex_str[1]) {
				c := hex_to_u8(hex_str)
				result = result[0..i] + c.ascii_str() + result[i + 3..]
			} else {
				i++
			}
		} else {
			i++
		}
	}
	return result
}

/// is_hex_char는 문자가 유효한 16진수 숫자인지 확인합니다.
/// 0-9, A-F, a-f 범위의 문자를 허용합니다.
fn is_hex_char(c u8) bool {
	return (c >= `0` && c <= `9`) || (c >= `A` && c <= `F`) || (c >= `a` && c <= `f`)
}

/// hex_to_u8는 두 문자 16진수 문자열을 바이트로 변환합니다.
/// 예: "4A" -> 74
fn hex_to_u8(s string) u8 {
	mut result := u8(0)
	for c in s {
		result <<= 4
		if c >= `0` && c <= `9` {
			result += c - `0`
		} else if c >= `A` && c <= `F` {
			result += c - `A` + 10
		} else if c >= `a` && c <= `f` {
			result += c - `a` + 10
		}
	}
	return result
}

/// u8_to_hex는 바이트를 두 문자 대문자 16진수 문자열로 변환합니다.
/// 예: 74 -> "4A"
fn u8_to_hex(c u8) string {
	high := (c >> 4) & 0x0F
	low := c & 0x0F
	mut high_hex := '0'
	mut low_hex := '0'

	if high < 10 {
		high_hex = (u8(`0`) + high).ascii_str()
	} else {
		high_hex = (u8(`A`) + high - 10).ascii_str()
	}

	if low < 10 {
		low_hex = (u8(`0`) + low).ascii_str()
	} else {
		low_hex = (u8(`A`) + low - 10).ascii_str()
	}

	return high_hex + low_hex
}

/// url_encode_for_sigv4는 AWS SigV4 요구사항에 따라 문자열을 인코딩합니다.
/// A-Z, a-z, 0-9, -, ., _, ~ 문자는 인코딩하지 않습니다.
/// 그 외 문자는 %XX 형식으로 퍼센트 인코딩합니다.
fn url_encode_for_sigv4(s string) string {
	mut result := []u8{}
	for c in s {
		match c {
			`A`...`Z`, `a`...`z`, `0`...`9`, `-`, `.`, `_`, `~` {
				result << c
			}
			else {
				result << u8(`%`)
				hex := u8_to_hex(c)
				result << hex[0]
				result << hex[1]
			}
		}
	}
	return result.bytestr()
}

// ============================================================
// XML 응답 파싱 (XML Response Parsing)
// ============================================================

/// parse_list_objects_response는 S3 ListObjectsV2 XML 응답을 파싱합니다.
/// 단순화된 XML 파싱으로 <Key> 태그만 추출합니다.
/// 프로덕션 환경에서는 적절한 XML 파서 사용을 권장합니다.
fn parse_list_objects_response(body string) []S3Object {
	// 단순화된 XML 파싱 - 프로덕션에서는 적절한 XML 파서 사용
	mut objects := []S3Object{}

	mut remaining := body
	for {
		key_start := remaining.index('<Key>') or { break }
		key_end := remaining.index('</Key>') or { break }

		key := remaining[key_start + 5..key_end]
		objects << S3Object{
			key:  key
			size: 0
		}

		remaining = remaining[key_end + 6..]
	}

	return objects
}
