/// S3 credentials loader module
/// Loads S3 access keys from the AWS credentials file.
module config

import os

/// load_s3_credentials_from_file loads AWS credentials from ~/.aws/credentials.
/// If the AWS_PROFILE environment variable is set, that profile is used;
/// otherwise, the 'default' profile is used.
/// returns: (access_key, secret_key) tuple
fn load_s3_credentials_from_file() (string, string) {
	home_dir := os.home_dir()
	cred_path := os.join_path(home_dir, '.aws', 'credentials')

	// return empty values if credentials file does not exist
	if !os.exists(cred_path) {
		return '', ''
	}

	// read the file
	lines := os.read_lines(cred_path) or { return '', '' }

	// currently parsed profile
	mut cur_prof := ''

	// determine target profile (from env var or default)
	mut target_prof := os.getenv('AWS_PROFILE')
	if target_prof == '' {
		target_prof = 'default'
	}

	mut access_key := ''
	mut secret_key := ''

	// parse file line by line
	for line in lines {
		l := line.trim_space()

		// skip empty lines or comments
		if l.len == 0 || l.starts_with('#') || l.starts_with(';') {
			continue
		}

		// handle profile section header (e.g. [default])
		if l.starts_with('[') && l.ends_with(']') {
			cur_prof = l[1..l.len - 1].trim_space()
			continue
		}

		// parse key-value pairs for the target profile
		if cur_prof == target_prof {
			p := l.split_nth('=', 2)
			if p.len == 2 {
				k := p[0].trim_space().to_lower()
				v := p[1].trim_space()

				// extract access key
				if k == 'aws_access_key_id' && access_key == '' {
					access_key = v
				} else if k == 'aws_secret_access_key' && secret_key == '' {
					// extract secret key
					secret_key = v
				}
			}
		}
	}

	return access_key, secret_key
}
