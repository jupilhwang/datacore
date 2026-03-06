module s3

// Unit tests for S3 Multi-Object Delete (batch delete) functionality.
// Tests cover: XML body generation, key chunking at 1000-key boundary,
// empty list handling, and response error parsing.

fn test_build_delete_objects_xml_single_key() {
	xml := build_delete_objects_xml(['key1'])
	assert xml.contains('<Delete>')
	assert xml.contains('<Object><Key>key1</Key></Object>')
	assert xml.contains('</Delete>')
}

fn test_build_delete_objects_xml_multiple_keys() {
	keys := ['a/b/c.bin', 'd/e/f.bin', 'g/h/i.bin']
	xml := build_delete_objects_xml(keys)

	for key in keys {
		assert xml.contains('<Object><Key>${key}</Key></Object>')
	}
}

fn test_build_delete_objects_xml_preserves_order() {
	keys := ['z-key', 'a-key', 'm-key']
	xml := build_delete_objects_xml(keys)

	z_pos := xml.index('<Key>z-key</Key>') or { -1 }
	a_pos := xml.index('<Key>a-key</Key>') or { -1 }
	m_pos := xml.index('<Key>m-key</Key>') or { -1 }

	assert z_pos >= 0
	assert a_pos >= 0
	assert m_pos >= 0
	assert z_pos < a_pos
	assert a_pos < m_pos
}

fn test_chunk_keys_under_limit() {
	keys := ['k1', 'k2', 'k3']
	chunks := chunk_keys(keys, 1000)

	assert chunks.len == 1
	assert chunks[0].len == 3
}

fn test_chunk_keys_exact_limit() {
	mut keys := []string{cap: 1000}
	for i in 0 .. 1000 {
		keys << 'key-${i}'
	}
	chunks := chunk_keys(keys, 1000)

	assert chunks.len == 1
	assert chunks[0].len == 1000
}

fn test_chunk_keys_over_limit() {
	mut keys := []string{cap: 1001}
	for i in 0 .. 1001 {
		keys << 'key-${i}'
	}
	chunks := chunk_keys(keys, 1000)

	assert chunks.len == 2
	assert chunks[0].len == 1000
	assert chunks[1].len == 1
}

fn test_chunk_keys_large_set() {
	mut keys := []string{cap: 2500}
	for i in 0 .. 2500 {
		keys << 'key-${i}'
	}
	chunks := chunk_keys(keys, 1000)

	assert chunks.len == 3
	assert chunks[0].len == 1000
	assert chunks[1].len == 1000
	assert chunks[2].len == 500
}

fn test_chunk_keys_empty() {
	chunks := chunk_keys([]string{}, 1000)
	assert chunks.len == 0
}

fn test_parse_delete_objects_errors_no_errors() {
	body := '<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted><Key>key1</Key></Deleted>
  <Deleted><Key>key2</Key></Deleted>
</DeleteResult>'

	errors := parse_delete_objects_errors(body)
	assert errors.len == 0
}

fn test_build_delete_objects_xml_escapes_special_chars() {
	keys := ['key&value', 'path<tag>', 'file"name', "it's", 'a&b<c>d"e\'f']
	xml := build_delete_objects_xml(keys)

	assert xml.contains('<Key>key&amp;value</Key>')
	assert xml.contains('<Key>path&lt;tag&gt;</Key>')
	assert xml.contains('<Key>file&quot;name</Key>')
	assert xml.contains('<Key>it&apos;s</Key>')
	assert xml.contains('<Key>a&amp;b&lt;c&gt;d&quot;e&apos;f</Key>')
	// Must NOT contain unescaped special chars in key values
	assert !xml.contains('<Key>key&value</Key>')
}

fn test_parse_delete_objects_errors_with_errors() {
	body := '<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted><Key>key1</Key></Deleted>
  <Error>
    <Key>key2</Key>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
  </Error>
  <Error>
    <Key>key3</Key>
    <Code>InternalError</Code>
    <Message>Internal Server Error</Message>
  </Error>
</DeleteResult>'

	errors := parse_delete_objects_errors(body)
	assert errors.len == 2
	assert errors[0].contains('key2')
	assert errors[1].contains('key3')
}
