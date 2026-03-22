/// S3 credentials loader module
/// Loads and resolves S3 access keys from multiple sources.
module config

import os
import toml

/// resolve_s3_credentials resolves S3 access/secret keys using 4-tier priority.
/// Priority: CLI args > env vars > ~/.aws/credentials > config.toml
fn resolve_s3_credentials(mut s3 S3StorageConfig, cli_args map[string]string, doc toml.Doc) {
	// priority 1: CLI arguments
	if cli_access_key := cli_args['s3-access-key'] {
		s3.access_key = cli_access_key
	}
	if cli_secret_key := cli_args['s3-secret-key'] {
		s3.secret_key = cli_secret_key
	}

	// priority 2: environment variables
	if s3.access_key == '' {
		s3.access_key = os.getenv('AWS_ACCESS_KEY_ID')
	}
	if s3.secret_key == '' {
		s3.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
	}

	// priority 3: ~/.aws/credentials file
	if s3.access_key == '' || s3.secret_key == '' {
		file_access, file_secret := load_s3_credentials_from_file()
		if s3.access_key == '' {
			s3.access_key = file_access
		}
		if s3.secret_key == '' {
			s3.secret_key = file_secret
		}
	}

	// priority 4: config.toml
	if s3.access_key == '' {
		s3.access_key = get_string(doc, 'storage.s3.access_key', '')
	}
	if s3.secret_key == '' {
		s3.secret_key = get_string(doc, 'storage.s3.secret_key', '')
	}
}

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
