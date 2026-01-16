// Infra Layer - Metrics Collection
// Simple metrics implementation compatible with Prometheus format
module observability

import sync
import time

// MetricType represents the type of metric
pub enum MetricType {
    counter
    gauge
    histogram
}

// Metric represents a single metric
@[heap]
pub struct Metric {
pub:
    name        string
    help        string
    metric_type MetricType
    labels      map[string]string
pub mut:
    value       f64
    count       u64      // For histogram
    sum         f64      // For histogram
    buckets     []Bucket // For histogram
}

// Bucket represents a histogram bucket
pub struct Bucket {
pub:
    upper_bound f64
pub mut:
    count       u64
}

// MetricsRegistry holds all registered metrics
pub struct MetricsRegistry {
mut:
    metrics map[string]&Metric
    lock    sync.RwMutex
}

// new_registry creates a new metrics registry
pub fn new_registry() &MetricsRegistry {
    return &MetricsRegistry{
        metrics: map[string]&Metric{}
    }
}

// Singleton registry holder
struct RegistryHolder {
mut:
    registry &MetricsRegistry = unsafe { nil }
    lock     sync.Mutex
}

fn get_registry_holder() &RegistryHolder {
    return &RegistryHolder{}
}

// get_registry returns the global metrics registry
pub fn get_registry() &MetricsRegistry {
    mut holder := get_registry_holder()
    holder.lock.@lock()
    defer { holder.lock.unlock() }
    
    if holder.registry == unsafe { nil } {
        holder.registry = new_registry()
    }
    return holder.registry
}

// register registers a new metric
pub fn (mut r MetricsRegistry) register(name string, help string, metric_type MetricType) &Metric {
    r.lock.@lock()
    defer { r.lock.unlock() }
    
    if name in r.metrics {
        return r.metrics[name] or { &Metric{} }
    }
    
    mut metric := &Metric{
        name: name
        help: help
        metric_type: metric_type
        labels: map[string]string{}
        value: 0
    }
    
    // Initialize histogram buckets if needed
    if metric_type == .histogram {
        metric.buckets = [
            Bucket{0.005, 0},
            Bucket{0.01, 0},
            Bucket{0.025, 0},
            Bucket{0.05, 0},
            Bucket{0.1, 0},
            Bucket{0.25, 0},
            Bucket{0.5, 0},
            Bucket{1.0, 0},
            Bucket{2.5, 0},
            Bucket{5.0, 0},
            Bucket{10.0, 0},
        ]
    }
    
    r.metrics[name] = metric
    return metric
}

// get returns a metric by name
pub fn (mut r MetricsRegistry) get(name string) ?&Metric {
    r.lock.rlock()
    defer { r.lock.runlock() }
    return r.metrics[name] or { return none }
}

// Counter functions
pub fn (mut m Metric) inc() {
    if m.metric_type == .counter || m.metric_type == .gauge {
        m.value += 1
    }
}

pub fn (mut m Metric) inc_by(v f64) {
    if m.metric_type == .counter || m.metric_type == .gauge {
        m.value += v
    }
}

pub fn (mut m Metric) dec() {
    if m.metric_type == .gauge {
        m.value -= 1
    }
}

pub fn (mut m Metric) dec_by(v f64) {
    if m.metric_type == .gauge {
        m.value -= v
    }
}

pub fn (mut m Metric) set(v f64) {
    if m.metric_type == .gauge {
        m.value = v
    }
}

// Histogram functions
pub fn (mut m Metric) observe(v f64) {
    if m.metric_type == .histogram {
        m.sum += v
        m.count += 1
        for mut b in m.buckets {
            if v <= b.upper_bound {
                b.count += 1
            }
        }
    }
}

// Export to Prometheus format
pub fn (r &MetricsRegistry) export_prometheus() string {
    mut output := ''
    
    for name, metric in r.metrics {
        // Help line
        output += '# HELP ${name} ${metric.help}\n'
        
        // Type line
        type_str := match metric.metric_type {
            .counter { 'counter' }
            .gauge { 'gauge' }
            .histogram { 'histogram' }
        }
        output += '# TYPE ${name} ${type_str}\n'
        
        // Value lines
        match metric.metric_type {
            .counter, .gauge {
                output += '${name} ${metric.value}\n'
            }
            .histogram {
                for b in metric.buckets {
                    output += '${name}_bucket{le="${b.upper_bound}"} ${b.count}\n'
                }
                output += '${name}_bucket{le="+Inf"} ${metric.count}\n'
                output += '${name}_sum ${metric.sum}\n'
                output += '${name}_count ${metric.count}\n'
            }
        }
        output += '\n'
    }
    
    return output
}

// Pre-defined DataCore metrics
pub struct DataCoreMetrics {
pub mut:
    messages_produced_total     &Metric
    messages_consumed_total     &Metric
    bytes_produced_total        &Metric
    bytes_consumed_total        &Metric
    active_connections          &Metric
    request_latency_seconds     &Metric
    topic_count                 &Metric
    partition_count             &Metric
    consumer_group_count        &Metric
    produce_requests_total      &Metric
    fetch_requests_total        &Metric
    metadata_requests_total     &Metric
    errors_total                &Metric
}

// new_datacore_metrics creates and registers all DataCore metrics
pub fn new_datacore_metrics() DataCoreMetrics {
    mut reg := get_registry()
    
    return DataCoreMetrics{
        messages_produced_total: reg.register('datacore_messages_produced_total', 'Total number of messages produced', .counter)
        messages_consumed_total: reg.register('datacore_messages_consumed_total', 'Total number of messages consumed', .counter)
        bytes_produced_total: reg.register('datacore_bytes_produced_total', 'Total bytes produced', .counter)
        bytes_consumed_total: reg.register('datacore_bytes_consumed_total', 'Total bytes consumed', .counter)
        active_connections: reg.register('datacore_active_connections', 'Number of active client connections', .gauge)
        request_latency_seconds: reg.register('datacore_request_latency_seconds', 'Request latency in seconds', .histogram)
        topic_count: reg.register('datacore_topics_total', 'Total number of topics', .gauge)
        partition_count: reg.register('datacore_partitions_total', 'Total number of partitions', .gauge)
        consumer_group_count: reg.register('datacore_consumer_groups_total', 'Total number of consumer groups', .gauge)
        produce_requests_total: reg.register('datacore_produce_requests_total', 'Total produce requests', .counter)
        fetch_requests_total: reg.register('datacore_fetch_requests_total', 'Total fetch requests', .counter)
        metadata_requests_total: reg.register('datacore_metadata_requests_total', 'Total metadata requests', .counter)
        errors_total: reg.register('datacore_errors_total', 'Total errors', .counter)
    }
}

// Timer for measuring latency
pub struct Timer {
    start_time time.Time
    metric     &Metric
}

// start_timer starts a new timer for a histogram metric
pub fn (m &Metric) start_timer() Timer {
    return Timer{
        start_time: time.now()
        metric: unsafe { m }
    }
}

// observe_duration records the elapsed time
pub fn (mut t Timer) observe_duration() {
    elapsed := time.since(t.start_time)
    seconds := f64(elapsed) / f64(time.second)
    unsafe {
        mut metric := t.metric
        metric.observe(seconds)
    }
}
