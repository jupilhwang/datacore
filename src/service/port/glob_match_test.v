// Tests for glob pattern matching used in subscription filtering.
module port

// -- exact match --

fn test_glob_match_exact_match() {
	assert glob_match('hello', 'hello')
	assert !glob_match('hello', 'world')
	assert !glob_match('hello', 'hell')
	assert !glob_match('hello', 'helloo')
}

fn test_glob_match_empty_strings() {
	assert glob_match('', '')
	assert !glob_match('', 'a')
	assert !glob_match('a', '')
}

// -- star wildcard (backward compat) --

fn test_glob_match_star_matches_everything() {
	assert glob_match('*', '')
	assert glob_match('*', 'anything')
	assert glob_match('*', 'a.b.c')
}

fn test_glob_match_star_prefix() {
	assert glob_match('*.log', 'app.log')
	assert glob_match('*.log', 'error.log')
	assert !glob_match('*.log', 'app.txt')
	assert !glob_match('*.log', 'log')
}

fn test_glob_match_star_suffix() {
	assert glob_match('app.*', 'app.log')
	assert glob_match('app.*', 'app.txt')
	assert !glob_match('app.*', 'server.log')
}

fn test_glob_match_star_middle() {
	assert glob_match('a*c', 'abc')
	assert glob_match('a*c', 'ac')
	assert glob_match('a*c', 'aXYZc')
	assert !glob_match('a*c', 'aXYZd')
}

fn test_glob_match_multiple_stars() {
	assert glob_match('*.*', 'file.txt')
	assert glob_match('*.*', '.hidden')
	assert !glob_match('*.*', 'noext')
	assert glob_match('a*b*c', 'abc')
	assert glob_match('a*b*c', 'aXbYc')
	assert !glob_match('a*b*c', 'aXYc')
}

// -- question mark wildcard --

fn test_glob_match_question_single_char() {
	assert glob_match('?', 'a')
	assert glob_match('?', 'z')
	assert !glob_match('?', '')
	assert !glob_match('?', 'ab')
}

fn test_glob_match_question_in_pattern() {
	assert glob_match('h?llo', 'hello')
	assert glob_match('h?llo', 'hallo')
	assert !glob_match('h?llo', 'hllo')
	assert !glob_match('h?llo', 'heello')
}

fn test_glob_match_question_combined_with_star() {
	assert glob_match('?*', 'a')
	assert glob_match('?*', 'abc')
	assert !glob_match('?*', '')
	assert glob_match('*?', 'a')
	assert glob_match('*?', 'abc')
	assert !glob_match('*?', '')
}

// -- character class [abc] --

fn test_glob_match_char_class_set() {
	assert glob_match('[abc]', 'a')
	assert glob_match('[abc]', 'b')
	assert glob_match('[abc]', 'c')
	assert !glob_match('[abc]', 'd')
	assert !glob_match('[abc]', '')
	assert !glob_match('[abc]', 'ab')
}

fn test_glob_match_char_class_in_pattern() {
	assert glob_match('topic-[abc]', 'topic-a')
	assert glob_match('topic-[abc]', 'topic-b')
	assert !glob_match('topic-[abc]', 'topic-d')
}

// -- character range [a-z] --

fn test_glob_match_char_range() {
	assert glob_match('[a-z]', 'a')
	assert glob_match('[a-z]', 'm')
	assert glob_match('[a-z]', 'z')
	assert !glob_match('[a-z]', 'A')
	assert !glob_match('[a-z]', '0')
}

fn test_glob_match_digit_range() {
	assert glob_match('[0-9]', '0')
	assert glob_match('[0-9]', '5')
	assert glob_match('[0-9]', '9')
	assert !glob_match('[0-9]', 'a')
}

fn test_glob_match_combined_range_and_set() {
	assert glob_match('[a-z0-9]', 'a')
	assert glob_match('[a-z0-9]', '5')
	assert !glob_match('[a-z0-9]', 'A')
}

// -- negated character class [!abc] [^abc] --

fn test_glob_match_negated_class_excl() {
	assert glob_match('[!abc]', 'd')
	assert glob_match('[!abc]', 'z')
	assert !glob_match('[!abc]', 'a')
	assert !glob_match('[!abc]', 'b')
	assert !glob_match('[!abc]', '')
}

fn test_glob_match_negated_class_caret() {
	assert glob_match('[^abc]', 'd')
	assert !glob_match('[^abc]', 'a')
}

fn test_glob_match_negated_range() {
	assert glob_match('[!0-9]', 'a')
	assert !glob_match('[!0-9]', '5')
}

// -- complex patterns --

fn test_glob_match_topic_patterns() {
	assert glob_match('orders-*', 'orders-created')
	assert glob_match('orders-*', 'orders-updated')
	assert !glob_match('orders-*', 'users-created')
}

fn test_glob_match_kafka_topic_style() {
	assert glob_match('*.events.*', 'user.events.created')
	assert glob_match('*.events.*', 'order.events.updated')
	assert !glob_match('*.events.*', 'user.commands.create')
}

fn test_glob_match_complex_combined() {
	assert glob_match('log-[0-9][0-9][0-9][0-9]', 'log-2024')
	assert !glob_match('log-[0-9][0-9][0-9][0-9]', 'log-abcd')
	assert glob_match('user-?-*', 'user-a-profile')
	assert !glob_match('user-?-*', 'user--profile')
}

// -- backward compat with simple_match --

fn test_glob_match_backward_compat_star_only() {
	assert glob_match('*', 'anything')
}

fn test_glob_match_backward_compat_star_prefix_suffix() {
	assert glob_match('*needle*', 'haystackneedlehaystack')
	assert !glob_match('*needle*', 'haystackhaystack')
}

fn test_glob_match_backward_compat_starts_with() {
	assert glob_match('prefix*', 'prefix-value')
	assert !glob_match('prefix*', 'other-value')
}

fn test_glob_match_backward_compat_ends_with() {
	assert glob_match('*suffix', 'my-suffix')
	assert !glob_match('*suffix', 'my-other')
}

fn test_glob_match_backward_compat_exact() {
	assert glob_match('exact', 'exact')
	assert !glob_match('exact', 'not-exact')
}

// -- literal bracket escape edge case --

fn test_glob_match_unclosed_bracket_literal() {
	assert glob_match('[abc', '[abc')
	assert !glob_match('[abc', 'a')
}
