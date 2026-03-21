module s3

// Tests for validate_identifier - S3 key path traversal prevention.

fn test_validate_identifier_accepts_valid_names() {
	// Simple alphanumeric
	validate_identifier('my-group', 'group_id') or { assert false, 'should accept: ${err.msg()}' }
	validate_identifier('topic_1', 'topic') or { assert false, 'should accept: ${err.msg()}' }
	validate_identifier('consumer.group.v2', 'group_id') or {
		assert false, 'should accept: ${err.msg()}'
	}
	validate_identifier('a', 'field') or { assert false, 'should accept single char: ${err.msg()}' }
	validate_identifier('test-group-123_v2.0', 'group_id') or {
		assert false, 'should accept mixed: ${err.msg()}'
	}
}

fn test_validate_identifier_rejects_empty_string() {
	validate_identifier('', 'group_id') or {
		assert err.msg().contains('cannot be empty')
		return
	}
	assert false, 'should have rejected empty string'
}

fn test_validate_identifier_rejects_path_traversal() {
	// The primary attack vector this validation prevents
	validate_identifier('../../topics/admin-topic/metadata', 'group_id') or {
		assert err.msg().contains('invalid character') || err.msg().contains('path traversal')
		return
	}
	assert false, 'should have rejected path traversal'
}

fn test_validate_identifier_rejects_double_dot() {
	validate_identifier('foo..bar', 'group_id') or {
		assert err.msg().contains('path traversal')
		return
	}
	assert false, 'should have rejected double dot'
}

fn test_validate_identifier_rejects_slash() {
	validate_identifier('group/subpath', 'group_id') or {
		assert err.msg().contains('invalid character')
		return
	}
	assert false, 'should have rejected forward slash'
}

fn test_validate_identifier_rejects_backslash() {
	validate_identifier('group\\subpath', 'group_id') or {
		assert err.msg().contains('invalid character')
		return
	}
	assert false, 'should have rejected backslash'
}

fn test_validate_identifier_rejects_null_byte() {
	validate_identifier('group\x00id', 'group_id') or {
		assert err.msg().contains('null byte')
		return
	}
	assert false, 'should have rejected null byte'
}

fn test_validate_identifier_rejects_non_printable() {
	validate_identifier('group\x01id', 'group_id') or {
		assert err.msg().contains('non-printable')
		return
	}
	assert false, 'should have rejected non-printable character'
}

fn test_validate_identifier_rejects_too_long() {
	long_name := 'a'.repeat(256)
	validate_identifier(long_name, 'group_id') or {
		assert err.msg().contains('too long')
		return
	}
	assert false, 'should have rejected string longer than 255'
}

fn test_validate_identifier_accepts_max_length() {
	max_name := 'a'.repeat(255)
	validate_identifier(max_name, 'group_id') or {
		assert false, 'should accept 255 char string: ${err.msg()}'
	}
}

fn test_validate_identifier_rejects_space() {
	validate_identifier('group id', 'group_id') or {
		assert err.msg().contains('invalid character')
		return
	}
	assert false, 'should have rejected space'
}

fn test_validate_identifier_single_dot_allowed() {
	// Single dot is valid (consistent with topic name validation)
	validate_identifier('my.group', 'group_id') or {
		assert false, 'should accept single dot: ${err.msg()}'
	}
}
