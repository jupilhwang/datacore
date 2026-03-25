module port

import time

/// LogField represents a key-value pair for structured logging.
/// Defined in the port layer so that both service and infrastructure
/// layers can use the same type without circular dependencies.
pub struct LogField {
pub:
	key   string
	value string
}

/// field_string creates a string log field.
@[inline]
pub fn field_string(key string, value string) LogField {
	return LogField{
		key:   key
		value: value
	}
}

/// field_int creates an integer log field.
@[inline]
pub fn field_int(key string, value i64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

/// field_bool creates a boolean log field.
@[inline]
pub fn field_bool(key string, value bool) LogField {
	return LogField{
		key:   key
		value: if value { 'true' } else { 'false' }
	}
}

/// field_err_str creates an error message log field.
@[inline]
pub fn field_err_str(err_msg string) LogField {
	return LogField{
		key:   'error'
		value: err_msg
	}
}

/// field_float creates a float log field.
@[inline]
pub fn field_float(key string, value f64) LogField {
	return LogField{
		key:   key
		value: '${value:.6}'
	}
}

/// field_bytes creates a byte-size log field.
@[inline]
pub fn field_bytes(key string, size i64) LogField {
	return LogField{
		key:   '${key}_bytes'
		value: '${size}'
	}
}

/// field_duration creates a duration log field.
@[inline]
pub fn field_duration(key string, d time.Duration) LogField {
	ms := f64(d) / f64(time.millisecond)
	return LogField{
		key:   '${key}_ms'
		value: '${ms:.3}'
	}
}
