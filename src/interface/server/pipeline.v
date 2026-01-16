// Interface Layer - Request Pipelining
// Kafka protocol supports request pipelining: multiple requests can be sent
// before receiving responses, but responses must be returned in order.
module server

import sync
import time

// PendingRequest represents a request waiting for response
pub struct PendingRequest {
pub:
    correlation_id  i32
    api_key         i16
    api_version     i16
    received_at     time.Time
    request_data    []u8
pub mut:
    response_data   []u8
    completed       bool
    error_msg       string
}

// RequestPipeline manages pipelined requests for a connection
// Ensures responses are sent in the order requests were received
pub struct RequestPipeline {
mut:
    pending         []PendingRequest
    max_pending     int
    lock            sync.Mutex
    total_enqueued  u64
    total_completed u64
}

// new_pipeline creates a new request pipeline
pub fn new_pipeline(max_pending int) &RequestPipeline {
    return &RequestPipeline{
        max_pending: max_pending
        pending: []PendingRequest{cap: max_pending}
    }
}

// enqueue adds a new request to the pipeline
// Returns error if pipeline is full
pub fn (mut p RequestPipeline) enqueue(correlation_id i32, api_key i16, api_version i16, data []u8) ! {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    if p.pending.len >= p.max_pending {
        return error('pipeline full: ${p.pending.len}/${p.max_pending} pending requests')
    }
    
    p.pending << PendingRequest{
        correlation_id: correlation_id
        api_key: api_key
        api_version: api_version
        received_at: time.now()
        request_data: data
    }
    p.total_enqueued += 1
}

// complete marks a request as completed with its response
pub fn (mut p RequestPipeline) complete(correlation_id i32, response []u8) ! {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    for mut req in p.pending {
        if req.correlation_id == correlation_id {
            req.response_data = response
            req.completed = true
            p.total_completed += 1
            return
        }
    }
    
    return error('correlation_id ${correlation_id} not found in pipeline')
}

// complete_with_error marks a request as completed with an error
pub fn (mut p RequestPipeline) complete_with_error(correlation_id i32, err_msg string) ! {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    for mut req in p.pending {
        if req.correlation_id == correlation_id {
            req.completed = true
            req.error_msg = err_msg
            p.total_completed += 1
            return
        }
    }
    
    return error('correlation_id ${correlation_id} not found in pipeline')
}

// get_ready_responses returns responses ready to be sent (in order)
// Only returns consecutive completed requests from the front
pub fn (mut p RequestPipeline) get_ready_responses() []PendingRequest {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    mut ready := []PendingRequest{}
    
    for p.pending.len > 0 && p.pending[0].completed {
        ready << p.pending[0]
        p.pending.delete(0)
    }
    
    return ready
}

// peek_first returns the first pending request without removing it
pub fn (mut p RequestPipeline) peek_first() ?PendingRequest {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    if p.pending.len > 0 {
        return p.pending[0]
    }
    return none
}

// pending_count returns the number of pending requests
pub fn (mut p RequestPipeline) pending_count() int {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    return p.pending.len
}

// is_full returns true if pipeline cannot accept more requests
pub fn (mut p RequestPipeline) is_full() bool {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    return p.pending.len >= p.max_pending
}

// clear removes all pending requests
pub fn (mut p RequestPipeline) clear() {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    p.pending.clear()
}

// get_stats returns pipeline statistics
pub fn (mut p RequestPipeline) get_stats() PipelineStats {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    return PipelineStats{
        pending_count: p.pending.len
        max_pending: p.max_pending
        total_enqueued: p.total_enqueued
        total_completed: p.total_completed
    }
}

// PipelineStats holds pipeline statistics
pub struct PipelineStats {
pub:
    pending_count   int
    max_pending     int
    total_enqueued  u64
    total_completed u64
}

// oldest_pending_age returns the age of the oldest pending request in milliseconds
pub fn (mut p RequestPipeline) oldest_pending_age() i64 {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    if p.pending.len == 0 {
        return 0
    }
    
    return (time.now() - p.pending[0].received_at).milliseconds()
}

// has_timed_out checks if any pending request has exceeded the timeout
pub fn (mut p RequestPipeline) has_timed_out(timeout_ms i64) bool {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    now := time.now()
    for req in p.pending {
        if (now - req.received_at).milliseconds() > timeout_ms {
            return true
        }
    }
    return false
}
