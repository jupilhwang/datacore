// Unit Tests - Domain Layer: Record
module domain_test

import domain as d
import time

fn test_record_create() {
    record := d.Record{
        key: 'test-key'.bytes()
        value: 'test-value'.bytes()
        headers: map[string][]u8{}
        timestamp: time.now()
    }
    
    assert record.key.bytestr() == 'test-key'
    assert record.value.bytestr() == 'test-value'
}

fn test_append_result() {
    result := d.AppendResult{
        base_offset: 100
        log_append_time: time.now().unix()
        log_start_offset: 0
        record_count: 5
    }
    
    assert result.base_offset == 100
    assert result.record_count == 5
}
