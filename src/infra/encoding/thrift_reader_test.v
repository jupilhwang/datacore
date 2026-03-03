// Tests for ThriftReader — Thrift Compact Protocol decoder.
module encoding

fn test_thrift_reader_varint32_single_byte() {
	mut r := new_thrift_reader([u8(0x00)])
	val := r.read_varint32() or { panic(err) }
	assert val == u32(0)
}

fn test_thrift_reader_varint32_multi_byte() {
	mut r := new_thrift_reader([u8(0x80), u8(0x01)])
	val := r.read_varint32() or { panic(err) }
	assert val == u32(128)
}

fn test_thrift_reader_varint32_300() {
	mut r := new_thrift_reader([u8(0xAC), u8(0x02)])
	val := r.read_varint32() or { panic(err) }
	assert val == u32(300)
}

fn test_thrift_reader_zigzag32_roundtrip() {
	assert unzigzag32(u32(0)) == i32(0)
	assert unzigzag32(u32(1)) == i32(-1)
	assert unzigzag32(u32(2)) == i32(1)
	assert unzigzag32(u32(3)) == i32(-2)
}

fn test_thrift_reader_zigzag64_roundtrip() {
	assert unzigzag64(u64(0)) == i64(0)
	assert unzigzag64(u64(1)) == i64(-1)
	assert unzigzag64(u64(2)) == i64(1)
}

fn test_thrift_reader_i32_roundtrip() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_i32(1, i32(42))
	w.write_struct_end()

	mut r := new_thrift_reader(w.bytes())
	r.read_struct_begin()
	field_type, field_id := r.read_field_header() or { panic(err) }
	assert field_type == thrift_type_i32
	assert field_id == i16(1)
	val := r.read_i32() or { panic(err) }
	assert val == i32(42)
	r.read_struct_end() or { panic(err) }
}

fn test_thrift_reader_string_roundtrip() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_string(1, 'hello')
	w.write_struct_end()

	mut r := new_thrift_reader(w.bytes())
	r.read_struct_begin()
	_, _ := r.read_field_header() or { panic(err) }
	s := r.read_string() or { panic(err) }
	assert s == 'hello'
	r.read_struct_end() or { panic(err) }
}

fn test_thrift_reader_i64_roundtrip() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_i64(1, i64(1700000000000))
	w.write_struct_end()

	mut r := new_thrift_reader(w.bytes())
	r.read_struct_begin()
	_, _ := r.read_field_header() or { panic(err) }
	val := r.read_i64() or { panic(err) }
	assert val == i64(1700000000000)
	r.read_struct_end() or { panic(err) }
}

fn test_thrift_reader_list_begin() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_list_begin(1, thrift_type_i32, 3)
	w.write_raw_i32(i32(10))
	w.write_raw_i32(i32(20))
	w.write_raw_i32(i32(30))
	w.write_struct_end()

	mut r := new_thrift_reader(w.bytes())
	r.read_struct_begin()
	_, _ := r.read_field_header() or { panic(err) }
	elem_type, count := r.read_list_begin() or { panic(err) }
	assert elem_type == thrift_type_i32
	assert count == 3
	v0 := r.read_raw_i32() or { panic(err) }
	v1 := r.read_raw_i32() or { panic(err) }
	v2 := r.read_raw_i32() or { panic(err) }
	assert v0 == i32(10)
	assert v1 == i32(20)
	assert v2 == i32(30)
	r.read_struct_end() or { panic(err) }
}
