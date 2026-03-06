// Unit tests for XML utility functions (xml_escape)
module s3

fn test_xml_escape_no_special_chars() {
	result := xml_escape('simple-key/path.bin')
	assert result == 'simple-key/path.bin'
}

fn test_xml_escape_ampersand() {
	result := xml_escape('key&value')
	assert result == 'key&amp;value'
}

fn test_xml_escape_less_than() {
	result := xml_escape('key<value')
	assert result == 'key&lt;value'
}

fn test_xml_escape_greater_than() {
	result := xml_escape('key>value')
	assert result == 'key&gt;value'
}

fn test_xml_escape_double_quote() {
	result := xml_escape('key"value')
	assert result == 'key&quot;value'
}

fn test_xml_escape_single_quote() {
	result := xml_escape("key'value")
	assert result == 'key&apos;value'
}

fn test_xml_escape_all_special_chars() {
	result := xml_escape('a&b<c>d"e\'f')
	assert result == 'a&amp;b&lt;c&gt;d&quot;e&apos;f'
}

fn test_xml_escape_empty_string() {
	result := xml_escape('')
	assert result == ''
}

fn test_xml_escape_ampersand_processed_first() {
	// Ensure & is escaped first, so &lt; is not double-escaped
	result := xml_escape('&lt;')
	assert result == '&amp;lt;'
}

fn test_xml_escape_multiple_ampersands() {
	result := xml_escape('a&&b')
	assert result == 'a&amp;&amp;b'
}
