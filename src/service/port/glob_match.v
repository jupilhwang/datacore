// Glob pattern matching for subscription topic/key filtering.
module port

/// glob_match checks whether text matches the given glob pattern.
/// Supports: * (any sequence), ? (single char), [abc] (char class),
/// [a-z] (range), [!abc]/[^abc] (negation).
fn glob_match(pattern string, text string) bool {
	return glob_match_at(pattern, 0, text, 0)
}

/// glob_match_at recursively matches pattern against text from given positions.
fn glob_match_at(pattern string, pi int, text string, ti int) bool {
	mut p := pi
	mut t := ti
	for p < pattern.len {
		if pattern[p] == `*` {
			return match_star(pattern, p + 1, text, t)
		}
		if t >= text.len {
			return false
		}
		if pattern[p] == `?` {
			p++
			t++
			continue
		}
		if pattern[p] == `[` {
			end := find_bracket_end(pattern, p)
			if end < 0 {
				// unclosed bracket: treat as literal
				if text[t] != pattern[p] {
					return false
				}
				p++
				t++
				continue
			}
			if !match_char_class(pattern, p + 1, end, text[t]) {
				return false
			}
			p = end + 1
			t++
			continue
		}
		if text[t] != pattern[p] {
			return false
		}
		p++
		t++
	}
	return t == text.len
}

/// match_star handles `*` by trying every possible expansion.
fn match_star(pattern string, pi int, text string, ti int) bool {
	for t := ti; t <= text.len; t++ {
		if glob_match_at(pattern, pi, text, t) {
			return true
		}
	}
	return false
}

/// find_bracket_end locates the closing `]` for a character class.
/// Returns -1 if no closing bracket is found.
fn find_bracket_end(pattern string, open int) int {
	mut i := open + 1
	// skip leading `!` or `^` for negation
	if i < pattern.len && (pattern[i] == `!` || pattern[i] == `^`) {
		i++
	}
	// skip leading `]` (literal `]` in class)
	if i < pattern.len && pattern[i] == `]` {
		i++
	}
	for i < pattern.len {
		if pattern[i] == `]` {
			return i
		}
		i++
	}
	return -1
}

/// match_char_class checks whether ch matches the character class
/// defined between pattern[start..end] (exclusive of brackets).
fn match_char_class(pattern string, start int, end int, ch u8) bool {
	negated := start < end && (pattern[start] == `!` || pattern[start] == `^`)
	mut i := if negated { start + 1 } else { start }
	mut found := false
	for i < end {
		if i + 2 < end && pattern[i + 1] == `-` {
			lo := pattern[i]
			hi := pattern[i + 2]
			if ch >= lo && ch <= hi {
				found = true
			}
			i += 3
		} else {
			if pattern[i] == ch {
				found = true
			}
			i++
		}
	}
	return if negated { !found } else { found }
}
