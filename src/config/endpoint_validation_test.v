module config

// Tests for validate_s3_endpoint - SSRF prevention for S3 endpoint configuration.
// Moved from infra/storage/plugins/s3/validation_test.v alongside the implementation.

fn test_validate_s3_endpoint_accepts_empty_string() {
	// Empty endpoint means "use default AWS endpoint" - should pass
	validate_s3_endpoint('') or { assert false, 'should accept empty string: ${err.msg()}' }
}

fn test_validate_s3_endpoint_accepts_valid_aws_endpoints() {
	validate_s3_endpoint('https://s3.amazonaws.com') or {
		assert false, 'should accept s3.amazonaws.com: ${err.msg()}'
	}
	validate_s3_endpoint('https://minio.example.com:9000') or {
		assert false, 'should accept minio with port: ${err.msg()}'
	}
	validate_s3_endpoint('http://s3.us-east-1.amazonaws.com') or {
		assert false, 'should accept regional endpoint: ${err.msg()}'
	}
}

fn test_validate_s3_endpoint_rejects_aws_metadata_ssrf() {
	// AWS metadata endpoint - primary SSRF target
	validate_s3_endpoint('http://169.254.169.254') or {
		assert err.msg().contains('private') || err.msg().contains('link-local')
		return
	}
	assert false, 'should have rejected AWS metadata endpoint 169.254.169.254'
}

fn test_validate_s3_endpoint_rejects_loopback() {
	validate_s3_endpoint('http://127.0.0.1:8080') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected loopback 127.0.0.1'
}

fn test_validate_s3_endpoint_rejects_private_10() {
	validate_s3_endpoint('http://10.0.0.1') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected private IP 10.0.0.1'
}

fn test_validate_s3_endpoint_rejects_private_192_168() {
	validate_s3_endpoint('http://192.168.1.1') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected private IP 192.168.1.1'
}

fn test_validate_s3_endpoint_rejects_private_172_16() {
	validate_s3_endpoint('http://172.16.0.1') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected private IP 172.16.0.1'
}

fn test_validate_s3_endpoint_rejects_localhost() {
	validate_s3_endpoint('http://localhost') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected localhost'
}

fn test_validate_s3_endpoint_rejects_zero_address() {
	validate_s3_endpoint('http://0.0.0.0') or {
		assert err.msg().contains('non-routable')
		return
	}
	assert false, 'should have rejected 0.0.0.0'
}

fn test_validate_s3_endpoint_rejects_missing_scheme() {
	validate_s3_endpoint('s3.amazonaws.com') or {
		assert err.msg().contains('scheme')
		return
	}
	assert false, 'should have rejected URL without scheme'
}

fn test_validate_s3_endpoint_rejects_loopback_127_x() {
	// Any IP in 127.0.0.0/8 range is loopback
	validate_s3_endpoint('http://127.0.0.2') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected loopback 127.0.0.2'
}

fn test_validate_s3_endpoint_rejects_link_local_169_254() {
	// 169.254.x.x is link-local, includes AWS metadata
	validate_s3_endpoint('http://169.254.1.1') or {
		assert err.msg().contains('link-local')
		return
	}
	assert false, 'should have rejected link-local 169.254.1.1'
}

fn test_validate_s3_endpoint_rejects_private_172_31() {
	// 172.16.0.0/12 covers 172.16.x.x through 172.31.x.x
	validate_s3_endpoint('http://172.31.255.255') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected private IP 172.31.255.255'
}

fn test_validate_s3_endpoint_accepts_public_172_32() {
	// 172.32.x.x is NOT in the private range
	validate_s3_endpoint('http://172.32.0.1') or {
		assert false, 'should accept public IP 172.32.0.1: ${err.msg()}'
	}
}

fn test_validate_s3_endpoint_rejects_ipv6_loopback() {
	validate_s3_endpoint('http://[::1]') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected IPv6 loopback ::1'
}

fn test_validate_s3_endpoint_rejects_ipv4_mapped_ipv6() {
	// ::ffff:127.0.0.1 is IPv4-mapped IPv6 for loopback
	validate_s3_endpoint('http://[::ffff:127.0.0.1]:9000') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected IPv4-mapped IPv6 loopback ::ffff:127.0.0.1'
}

fn test_validate_s3_endpoint_rejects_ipv4_mapped_ipv6_private() {
	// ::ffff:10.0.0.1 is IPv4-mapped IPv6 for private range
	validate_s3_endpoint('http://[::ffff:10.0.0.1]:9000') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected IPv4-mapped IPv6 private ::ffff:10.0.0.1'
}

fn test_validate_s3_endpoint_rejects_ipv6_loopback_expanded() {
	// 0:0:0:0:0:0:0:1 is the expanded form of ::1
	validate_s3_endpoint('http://[0:0:0:0:0:0:0:1]:9000') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected expanded IPv6 loopback 0:0:0:0:0:0:0:1'
}

fn test_validate_s3_endpoint_rejects_ipv6_ula() {
	// fc00::/7 is IPv6 Unique Local Address (private equivalent)
	validate_s3_endpoint('http://[fc00::1]:9000') or {
		assert err.msg().contains('private') || err.msg().contains('unique local')
		return
	}
	assert false, 'should have rejected IPv6 ULA fc00::1'
}

fn test_validate_s3_endpoint_rejects_ipv6_ula_fd() {
	// fd00::/8 is also ULA
	validate_s3_endpoint('http://[fd00::1]:9000') or {
		assert err.msg().contains('private') || err.msg().contains('unique local')
		return
	}
	assert false, 'should have rejected IPv6 ULA fd00::1'
}

fn test_validate_s3_endpoint_rejects_ipv6_link_local() {
	// fe80::/10 is IPv6 link-local
	validate_s3_endpoint('http://[fe80::1]:9000') or {
		assert err.msg().contains('link-local')
		return
	}
	assert false, 'should have rejected IPv6 link-local fe80::1'
}

fn test_validate_s3_endpoint_allows_public_ipv6() {
	// 2001:db8::1 is a documentation prefix but should pass validation
	// (not private/loopback/link-local)
	validate_s3_endpoint('http://[2001:db8::1]:9000') or {
		assert false, 'should accept public IPv6 2001:db8::1: ${err.msg()}'
	}
}

fn test_validate_s3_endpoint_rejects_ipv6_loopback_leading_zeros() {
	// 0000:0000:0000:0000:0000:0000:0000:0001 is ::1 with leading zeros
	validate_s3_endpoint('http://[0000:0000:0000:0000:0000:0000:0000:0001]:9000') or {
		assert err.msg().contains('loopback')
		return
	}
	assert false, 'should have rejected IPv6 loopback with leading zeros'
}

fn test_validate_s3_endpoint_rejects_ipv4_mapped_ipv6_192_168() {
	// ::ffff:192.168.1.1 is IPv4-mapped IPv6 for private range
	validate_s3_endpoint('http://[::ffff:192.168.1.1]:9000') or {
		assert err.msg().contains('private')
		return
	}
	assert false, 'should have rejected IPv4-mapped IPv6 private ::ffff:192.168.1.1'
}

fn test_normalize_ipv6_collapses_zeros() {
	assert normalize_ipv6('0:0:0:0:0:0:0:1') == '::1'
	assert normalize_ipv6('0000:0000:0000:0000:0000:0000:0000:0001') == '::1'
	assert normalize_ipv6('fe80:0000:0000:0000:0000:0000:0000:0001') == 'fe80::1'
	assert normalize_ipv6('2001:0db8:0000:0000:0000:0000:0000:0001') == '2001:db8::1'
}

fn test_normalize_ipv6_already_collapsed() {
	assert normalize_ipv6('::1') == '::1'
	assert normalize_ipv6('fe80::1') == 'fe80::1'
	assert normalize_ipv6('::ffff:127.0.0.1') == '::ffff:127.0.0.1'
}
