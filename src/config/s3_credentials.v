/// S3 자격 증명 로더 모듈
/// AWS 자격 증명 파일에서 S3 접근 키를 로드합니다.
module config

import os

/// load_s3_credentials_from_file은 ~/.aws/credentials 파일에서 AWS 자격 증명을 로드합니다.
/// AWS_PROFILE 환경 변수가 설정되어 있으면 해당 프로파일을 사용하고,
/// 그렇지 않으면 'default' 프로파일을 사용합니다.
/// 반환값: (access_key, secret_key) 튜플
fn load_s3_credentials_from_file() (string, string) {
	home_dir := os.home_dir()
	cred_path := os.join_path(home_dir, '.aws', 'credentials')

	// 자격 증명 파일이 없으면 빈 값 반환
	if !os.exists(cred_path) {
		return '', ''
	}

	// 파일 읽기
	lines := os.read_lines(cred_path) or { return '', '' }

	// 현재 파싱 중인 프로파일
	mut cur_prof := ''

	// 대상 프로파일 결정 (환경 변수 또는 기본값)
	mut target_prof := os.getenv('AWS_PROFILE')
	if target_prof == '' {
		target_prof = 'default'
	}

	mut access_key := ''
	mut secret_key := ''

	// 파일 라인별 파싱
	for line in lines {
		l := line.trim_space()

		// 빈 줄 또는 주석 건너뛰기
		if l.len == 0 || l.starts_with('#') || l.starts_with(';') {
			continue
		}

		// 프로파일 섹션 헤더 처리 (예: [default])
		if l.starts_with('[') && l.ends_with(']') {
			cur_prof = l[1..l.len - 1].trim_space()
			continue
		}

		// 대상 프로파일의 키-값 쌍 파싱
		if cur_prof == target_prof {
			p := l.split('=')
			if p.len == 2 {
				k := p[0].trim_space().to_lower()
				v := p[1].trim_space()

				// 액세스 키 추출
				if k == 'aws_access_key_id' && access_key == '' {
					access_key = v
				} else if k == 'aws_secret_access_key' && secret_key == '' {
					// 시크릿 키 추출
					secret_key = v
				}
			}
		}
	}

	return access_key, secret_key
}
