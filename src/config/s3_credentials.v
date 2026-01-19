module config

import os

// load_s3_credentials_from_file attempts to load AWS credentials from ~/.aws/credentials
fn load_s3_credentials_from_file() (string, string) {
	home_dir := os.home_dir()
	cred_path := os.join_path(home_dir, '.aws', 'credentials')
	if !os.exists(cred_path) {
		return '', ''
	}

	lines := os.read_lines(cred_path) or { return '', '' }
	mut cur_prof := ''
	mut target_prof := os.getenv('AWS_PROFILE')
	if target_prof == '' {
		target_prof = 'default'
	}

	mut access_key := ''
	mut secret_key := ''

	for line in lines {
		l := line.trim_space()
		if l.len == 0 || l.starts_with('#') || l.starts_with(';') {
			continue
		}
		if l.starts_with('[') && l.ends_with(']') {
			cur_prof = l[1..l.len - 1].trim_space()
			continue
		}
		if cur_prof == target_prof {
			p := l.split('=')
			if p.len == 2 {
				k := p[0].trim_space().to_lower()
				v := p[1].trim_space()
				if k == 'aws_access_key_id' && access_key == '' {
					access_key = v
				} else if k == 'aws_secret_access_key' && secret_key == '' {
					secret_key = v
				}
			}
		}
	}
	return access_key, secret_key
}
