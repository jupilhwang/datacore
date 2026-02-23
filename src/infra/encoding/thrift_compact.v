// Thrift Compact Protocol (TCompactProtocol) encoder.
// Used for encoding Parquet file metadata (footer).
// Spec: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
module encoding

// Compact protocol field type IDs.
const thrift_type_boolean_true = u8(0x01)
const thrift_type_boolean_false = u8(0x02)
const thrift_type_byte = u8(0x03)
const thrift_type_i16 = u8(0x04)
const thrift_type_i32 = u8(0x05)
const thrift_type_i64 = u8(0x06)
const thrift_type_double = u8(0x07)
const thrift_type_binary = u8(0x08)
const thrift_type_list = u8(0x09)
const thrift_type_set = u8(0x0A)
const thrift_type_map = u8(0x0B)
const thrift_type_struct = u8(0x0C)
const thrift_stop = u8(0x00)

// ThriftWriter encodes data using the Thrift Compact Protocol.
pub struct ThriftWriter {
pub mut:
	buf           []u8
	last_field_id i16
	field_stack   []i16
}

// new_thrift_writer creates a new ThriftWriter.
pub fn new_thrift_writer() ThriftWriter {
	return ThriftWriter{
		// Pre-allocate a reasonable default capacity to avoid early reallocations.
		buf:           []u8{cap: 256}
		last_field_id: 0
		field_stack:   []i16{cap: 8}
	}
}

// bytes returns the encoded bytes.
// Returns a reference to the internal buffer — callers must not modify the ThriftWriter after calling bytes().
pub fn (w &ThriftWriter) bytes() []u8 {
	return w.buf
}

// write_varint32 encodes an unsigned 32-bit integer as a variable-length integer.
// Each byte stores 7 bits; the MSB indicates whether more bytes follow.
fn (mut w ThriftWriter) write_varint32(n u32) {
	mut val := n
	for val >= 0x80 {
		w.buf << u8((val & 0x7F) | 0x80)
		val >>= 7
	}
	w.buf << u8(val)
}

// write_varint64 encodes an unsigned 64-bit integer as a variable-length integer.
fn (mut w ThriftWriter) write_varint64(n u64) {
	mut val := n
	for val >= 0x80 {
		w.buf << u8((val & 0x7F) | 0x80)
		val >>= 7
	}
	w.buf << u8(val)
}

// zigzag32 converts a signed int32 to an unsigned zigzag-encoded value.
// Zigzag maps negative numbers to positive: 0->0, -1->1, 1->2, -2->3, ...
fn zigzag32(n i32) u32 {
	return (u32(n) << 1) ^ u32(n >> 31)
}

// zigzag64 converts a signed int64 to an unsigned zigzag-encoded value.
fn zigzag64(n i64) u64 {
	return (u64(n) << 1) ^ u64(n >> 63)
}

// write_struct_begin starts a struct (saves current last_field_id to stack).
pub fn (mut w ThriftWriter) write_struct_begin() {
	w.field_stack << w.last_field_id
	w.last_field_id = 0
}

// write_struct_end ends a struct (writes stop byte, restores last_field_id).
pub fn (mut w ThriftWriter) write_struct_end() {
	w.buf << thrift_stop
	if w.field_stack.len > 0 {
		w.last_field_id = w.field_stack[w.field_stack.len - 1]
		w.field_stack.delete(w.field_stack.len - 1)
	}
}

// write_field_header writes a field header.
// If delta (current - last) is 1..15, it is packed into the upper nibble of the type byte.
// Otherwise, a separate i16 follows.
fn (mut w ThriftWriter) write_field_header(field_type u8, field_id i16) {
	delta := field_id - w.last_field_id
	if delta > 0 && delta <= 15 {
		// Pack delta into upper nibble
		w.buf << u8((u8(delta) << 4) | field_type)
	} else {
		// Write type byte then field id as zigzag varint
		w.buf << field_type
		w.write_varint32(zigzag32(i32(field_id)))
	}
	w.last_field_id = field_id
}

// write_bool writes a boolean field.
pub fn (mut w ThriftWriter) write_bool(field_id i16, val bool) {
	if val {
		w.write_field_header(thrift_type_boolean_true, field_id)
	} else {
		w.write_field_header(thrift_type_boolean_false, field_id)
	}
}

// write_i8 writes an i8 field.
pub fn (mut w ThriftWriter) write_i8(field_id i16, val i8) {
	w.write_field_header(thrift_type_byte, field_id)
	w.buf << u8(val)
}

// write_i16 writes an i16 field as a zigzag varint.
pub fn (mut w ThriftWriter) write_i16(field_id i16, val i16) {
	w.write_field_header(thrift_type_i16, field_id)
	w.write_varint32(zigzag32(i32(val)))
}

// write_i32 writes an i32 field as a zigzag varint.
pub fn (mut w ThriftWriter) write_i32(field_id i16, val i32) {
	w.write_field_header(thrift_type_i32, field_id)
	w.write_varint32(zigzag32(val))
}

// write_i64 writes an i64 field as a zigzag varint.
pub fn (mut w ThriftWriter) write_i64(field_id i16, val i64) {
	w.write_field_header(thrift_type_i64, field_id)
	w.write_varint64(zigzag64(val))
}

// write_binary writes a binary/string field (length-prefixed byte array).
pub fn (mut w ThriftWriter) write_binary(field_id i16, data []u8) {
	w.write_field_header(thrift_type_binary, field_id)
	w.write_varint32(u32(data.len))
	w.buf << data
}

// write_string writes a UTF-8 string field.
// Writes directly from the string's internal byte representation to avoid an intermediate []u8 allocation.
pub fn (mut w ThriftWriter) write_string(field_id i16, val string) {
	w.write_field_header(thrift_type_binary, field_id)
	w.write_varint32(u32(val.len))
	for i in 0 .. val.len {
		w.buf << val[i]
	}
}

// write_raw_binary writes a raw binary value (length-prefixed) without field header.
pub fn (mut w ThriftWriter) write_raw_binary(data []u8) {
	w.write_varint32(u32(data.len))
	w.buf << data
}

// write_raw_string writes a raw string value without field header.
// Writes directly from the string's internal byte representation to avoid an intermediate []u8 allocation.
pub fn (mut w ThriftWriter) write_raw_string(val string) {
	w.write_varint32(u32(val.len))
	for i in 0 .. val.len {
		w.buf << val[i]
	}
}

// write_list_begin writes a list header (element type and count).
// If count fits in 4 bits (0..14), packs count into upper nibble.
// If count >= 15, writes 0xF0 | type, then count as varint.
pub fn (mut w ThriftWriter) write_list_begin(field_id i16, elem_type u8, count int) {
	w.write_field_header(thrift_type_list, field_id)
	if count < 15 {
		w.buf << u8((u8(count) << 4) | elem_type)
	} else {
		w.buf << u8(0xF0 | elem_type)
		w.write_varint32(u32(count))
	}
}

// write_raw_i32 writes an i32 value directly as zigzag varint (no field header).
// Used inside list elements.
pub fn (mut w ThriftWriter) write_raw_i32(val i32) {
	w.write_varint32(zigzag32(val))
}

// write_raw_i64 writes an i64 value directly as zigzag varint (no field header).
pub fn (mut w ThriftWriter) write_raw_i64(val i64) {
	w.write_varint64(zigzag64(val))
}

// write_raw_struct_begin starts an inline struct (within a list, etc.) - saves field context.
pub fn (mut w ThriftWriter) write_raw_struct_begin() {
	w.field_stack << w.last_field_id
	w.last_field_id = 0
}

// write_raw_struct_end ends an inline struct - writes stop byte and restores field context.
pub fn (mut w ThriftWriter) write_raw_struct_end() {
	w.buf << thrift_stop
	if w.field_stack.len > 0 {
		w.last_field_id = w.field_stack[w.field_stack.len - 1]
		w.field_stack.delete(w.field_stack.len - 1)
	}
}
