// Benchmark Runner - Standalone executable
// Run with: v -o bin/benchmark cmd/benchmark && ./bin/benchmark
module main

import time

fn main() {
    println('╔══════════════════════════════════════════════════════════════╗')
    println('║           DataCore Performance Benchmark Suite               ║')
    println('╚══════════════════════════════════════════════════════════════╝')
    println('')
    
    // Buffer allocation benchmark
    println('▶ Running Buffer Allocation Benchmark...')
    benchmark_buffer_allocation()
    
    // Object creation benchmark
    println('▶ Running Object Creation Benchmark...')
    benchmark_object_creation()
    
    println('')
    println('▶ Benchmark complete!')
}

fn benchmark_buffer_allocation() {
    iterations := u64(10000)
    sizes := [64, 256, 1024, 4096, 16384]
    
    println('┌────────────────────┬────────────┬────────────┬──────────────┐')
    println('│ Buffer Size        │ Iterations │ Avg (ns)   │ Ops/sec      │')
    println('├────────────────────┼────────────┼────────────┼──────────────┤')
    
    for size in sizes {
        start := time.sys_mono_now()
        for _ in 0 .. iterations {
            _ := []u8{len: size}
        }
        elapsed := time.sys_mono_now() - start
        avg_ns := elapsed / iterations
        ops_per_sec := if avg_ns > 0 { f64(1_000_000_000) / f64(avg_ns) } else { 0.0 }
        
        println('│ ${size:18} │ ${iterations:10} │ ${avg_ns:10} │ ${ops_per_sec:12.0} │')
    }
    
    println('└────────────────────┴────────────┴────────────┴──────────────┘')
}

fn benchmark_object_creation() {
    iterations := u64(10000)
    
    println('┌────────────────────┬────────────┬────────────┬──────────────┐')
    println('│ Operation          │ Iterations │ Avg (ns)   │ Ops/sec      │')
    println('├────────────────────┼────────────┼────────────┼──────────────┤')
    
    // Struct creation
    start := time.sys_mono_now()
    for _ in 0 .. iterations {
        _ := TestRecord{
            key: 'test-key'
            value: 'test-value'
            timestamp: time.now().unix()
        }
    }
    elapsed := time.sys_mono_now() - start
    avg_ns := elapsed / iterations
    ops_per_sec := if avg_ns > 0 { f64(1_000_000_000) / f64(avg_ns) } else { 0.0 }
    println('│ ${'Struct Creation':18} │ ${iterations:10} │ ${avg_ns:10} │ ${ops_per_sec:12.0} │')
    
    // Array creation
    start2 := time.sys_mono_now()
    for _ in 0 .. iterations {
        _ := []TestRecord{cap: 100}
    }
    elapsed2 := time.sys_mono_now() - start2
    avg_ns2 := elapsed2 / iterations
    ops_per_sec2 := if avg_ns2 > 0 { f64(1_000_000_000) / f64(avg_ns2) } else { 0.0 }
    println('│ ${'Array Creation':18} │ ${iterations:10} │ ${avg_ns2:10} │ ${ops_per_sec2:12.0} │')
    
    println('└────────────────────┴────────────┴────────────┴──────────────┘')
}

struct TestRecord {
    key       string
    value     string
    timestamp i64
}
