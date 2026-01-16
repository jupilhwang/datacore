// Unit tests for Request Pipeline
module server

import time

fn test_pipeline_creation() {
    mut pipeline := new_pipeline(10)
    
    assert pipeline.pending_count() == 0
    assert !pipeline.is_full()
    
    stats := pipeline.get_stats()
    assert stats.pending_count == 0
    assert stats.max_pending == 10
    assert stats.total_enqueued == 0
    assert stats.total_completed == 0
}

fn test_pipeline_enqueue() {
    mut pipeline := new_pipeline(10)
    
    // Enqueue a request
    pipeline.enqueue(1, 0, 0, [u8(1), 2, 3]) or {
        assert false, 'enqueue should not fail'
    }
    
    assert pipeline.pending_count() == 1
    
    stats := pipeline.get_stats()
    assert stats.total_enqueued == 1
}

fn test_pipeline_enqueue_multiple() {
    mut pipeline := new_pipeline(10)
    
    // Enqueue multiple requests
    for i in 1 .. 6 {
        pipeline.enqueue(i32(i), 0, 0, [u8(i)]) or {
            assert false, 'enqueue should not fail'
        }
    }
    
    assert pipeline.pending_count() == 5
    assert !pipeline.is_full()
}

fn test_pipeline_full() {
    mut pipeline := new_pipeline(3)
    
    // Fill the pipeline
    for i in 1 .. 4 {
        pipeline.enqueue(i32(i), 0, 0, [u8(i)]) or {
            assert false, 'enqueue should not fail'
        }
    }
    
    assert pipeline.is_full()
    
    // Try to enqueue when full - should fail
    pipeline.enqueue(4, 0, 0, [u8(4)]) or {
        assert err.str().contains('pipeline full')
        return
    }
    assert false, 'should have failed due to full pipeline'
}

fn test_pipeline_complete_and_get_ready() {
    mut pipeline := new_pipeline(10)
    
    // Enqueue requests
    pipeline.enqueue(1, 0, 0, [u8(1)]) or { assert false }
    pipeline.enqueue(2, 0, 0, [u8(2)]) or { assert false }
    pipeline.enqueue(3, 0, 0, [u8(3)]) or { assert false }
    
    // Complete first request
    pipeline.complete(1, [u8(10), 11]) or { assert false }
    
    // Get ready responses - should return first only
    ready := pipeline.get_ready_responses()
    assert ready.len == 1
    assert ready[0].correlation_id == 1
    assert ready[0].response_data == [u8(10), 11]
    
    assert pipeline.pending_count() == 2
}

fn test_pipeline_order_preservation() {
    mut pipeline := new_pipeline(10)
    
    // Enqueue requests in order
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    pipeline.enqueue(2, 0, 0, []) or { assert false }
    pipeline.enqueue(3, 0, 0, []) or { assert false }
    
    // Complete out of order
    pipeline.complete(3, [u8(30)]) or { assert false }
    pipeline.complete(2, [u8(20)]) or { assert false }
    
    // Get ready - should return none since first not complete
    ready1 := pipeline.get_ready_responses()
    assert ready1.len == 0
    
    // Complete first
    pipeline.complete(1, [u8(10)]) or { assert false }
    
    // Now should get all three in order
    ready2 := pipeline.get_ready_responses()
    assert ready2.len == 3
    assert ready2[0].correlation_id == 1
    assert ready2[1].correlation_id == 2
    assert ready2[2].correlation_id == 3
}

fn test_pipeline_complete_with_error() {
    mut pipeline := new_pipeline(10)
    
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    
    pipeline.complete_with_error(1, 'test error') or { assert false }
    
    ready := pipeline.get_ready_responses()
    assert ready.len == 1
    assert ready[0].error_msg == 'test error'
}

fn test_pipeline_complete_not_found() {
    mut pipeline := new_pipeline(10)
    
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    
    // Try to complete non-existent request
    pipeline.complete(999, []) or {
        assert err.str().contains('not found')
        return
    }
    assert false, 'should have failed for non-existent correlation_id'
}

fn test_pipeline_peek_first() {
    mut pipeline := new_pipeline(10)
    
    // Empty pipeline
    if _ := pipeline.peek_first() {
        assert false, 'should return none for empty pipeline'
    }
    
    pipeline.enqueue(42, 3, 2, [u8(1), 2, 3]) or { assert false }
    
    if req := pipeline.peek_first() {
        assert req.correlation_id == 42
        assert req.api_key == 3
        assert req.api_version == 2
    } else {
        assert false, 'should return first request'
    }
    
    // peek should not remove
    assert pipeline.pending_count() == 1
}

fn test_pipeline_clear() {
    mut pipeline := new_pipeline(10)
    
    for i in 1 .. 6 {
        pipeline.enqueue(i32(i), 0, 0, []) or { assert false }
    }
    
    assert pipeline.pending_count() == 5
    
    pipeline.clear()
    
    assert pipeline.pending_count() == 0
    assert !pipeline.is_full()
}

fn test_pipeline_oldest_pending_age() {
    mut pipeline := new_pipeline(10)
    
    // Empty pipeline
    assert pipeline.oldest_pending_age() == 0
    
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    
    // Small delay to ensure age > 0
    time.sleep(10 * time.millisecond)
    
    age := pipeline.oldest_pending_age()
    assert age >= 10  // At least 10ms
}

fn test_pipeline_has_timed_out() {
    mut pipeline := new_pipeline(10)
    
    // Empty pipeline - no timeout
    assert !pipeline.has_timed_out(100)
    
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    
    // Should not have timed out yet
    assert !pipeline.has_timed_out(10000)
    
    // Wait and check with short timeout
    time.sleep(50 * time.millisecond)
    assert pipeline.has_timed_out(10)  // 10ms timeout, waited 50ms
}

fn test_pipeline_stats() {
    mut pipeline := new_pipeline(5)
    
    // Enqueue and complete some requests
    pipeline.enqueue(1, 0, 0, []) or { assert false }
    pipeline.enqueue(2, 0, 0, []) or { assert false }
    
    pipeline.complete(1, []) or { assert false }
    pipeline.complete(2, []) or { assert false }
    
    _ := pipeline.get_ready_responses()
    
    stats := pipeline.get_stats()
    assert stats.max_pending == 5
    assert stats.total_enqueued == 2
    assert stats.total_completed == 2
    assert stats.pending_count == 0
}

fn test_pending_request_struct() {
    now := time.now()
    req := PendingRequest{
        correlation_id: 123
        api_key: 1
        api_version: 10
        received_at: now
        request_data: [u8(1), 2, 3]
        response_data: [u8(4), 5, 6]
        completed: true
        error_msg: ''
    }
    
    assert req.correlation_id == 123
    assert req.api_key == 1
    assert req.api_version == 10
    assert req.request_data.len == 3
    assert req.response_data.len == 3
    assert req.completed == true
    assert req.error_msg == ''
}
