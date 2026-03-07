// Infra Layer - S3 XML utility functions
// Provides shared XML encoding helpers for S3 API request body construction.
module s3

/// xml_escape replaces XML special characters with their entity references.
/// Ampersand (&) must be replaced first to avoid double-escaping.
fn xml_escape(s string) string {
	if s == '' {
		return s
	}
	// & must be first to prevent double-escaping of subsequent replacements
	mut result := s.replace('&', '&amp;')
	result = result.replace('<', '&lt;')
	result = result.replace('>', '&gt;')
	result = result.replace('"', '&quot;')
	result = result.replace("'", '&apos;')
	return result
}
