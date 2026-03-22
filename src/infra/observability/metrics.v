/// Infrastructure layer - Metrics collection
/// Simple metrics implementation compatible with Prometheus format
module observability

import strings
import sync
import time

/// MetricType represents the type of a metric.
pub enum MetricType {
	counter
	gauge
	histogram
}

/// Metric represents a single metric.
@[heap]
pub struct Metric {
pub:
	name        string
	help        string
	metric_type MetricType
	labels      map[string]string
pub mut:
	value   f64
	count   u64
	sum     f64
	buckets []Bucket
mut:
	mu sync.Mutex
}

/// Bucket represents a histogram bucket.
pub struct Bucket {
pub:
	upper_bound f64
pub mut:
	count u64
}

/// MetricsRegistry holds all registered metrics.
pub struct MetricsRegistry {
mut:
	metrics map[string]&Metric
	lock    sync.RwMutex
}

/// new_registry creates a new metrics registry.
pub fn new_registry() &MetricsRegistry {
	return &MetricsRegistry{
		metrics: map[string]&Metric{}
	}
}

/// Singleton registry holder
struct RegistryHolder {
mut:
	registry &MetricsRegistry = unsafe { nil }
	lock     sync.Mutex
}

// 모듈 수준 싱글톤 홀더 (const holder 패턴)
const g_registry_const_holder = &RegistryHolder{
	registry: unsafe { nil }
}

fn get_registry_holder() &RegistryHolder {
	return unsafe { g_registry_const_holder }
}

/// get_registry returns the global metrics registry.
pub fn get_registry() &MetricsRegistry {
	mut holder := get_registry_holder()
	holder.lock.@lock()
	defer { holder.lock.unlock() }

	if holder.registry == unsafe { nil } {
		holder.registry = new_registry()
	}
	return holder.registry
}

/// register registers a new metric.
pub fn (mut r MetricsRegistry) register(name string, help string, metric_type MetricType) &Metric {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if name in r.metrics {
		return r.metrics[name] or { &Metric{} }
	}

	mut metric := &Metric{
		name:        name
		help:        help
		metric_type: metric_type
		labels:      map[string]string{}
		value:       0
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

/// get returns a metric by name.
pub fn (mut r MetricsRegistry) get(name string) ?&Metric {
	r.lock.rlock()
	defer { r.lock.runlock() }
	return r.metrics[name] or { return none }
}

/// Counter functions
pub fn (mut m Metric) inc() {
	m.mu.@lock()
	defer { m.mu.unlock() }
	if m.metric_type == .counter || m.metric_type == .gauge {
		m.value += 1
	}
}

/// inc_by increments the metric by the specified value.
pub fn (mut m Metric) inc_by(v f64) {
	m.mu.@lock()
	defer { m.mu.unlock() }
	if m.metric_type == .counter || m.metric_type == .gauge {
		m.value += v
	}
}

/// dec decrements the gauge by 1.
pub fn (mut m Metric) dec() {
	m.mu.@lock()
	defer { m.mu.unlock() }
	if m.metric_type == .gauge {
		m.value -= 1
	}
}

/// dec_by decrements the gauge by the specified value.
pub fn (mut m Metric) dec_by(v f64) {
	m.mu.@lock()
	defer { m.mu.unlock() }
	if m.metric_type == .gauge {
		m.value -= v
	}
}

/// set sets the gauge to the specified value.
pub fn (mut m Metric) set(v f64) {
	m.mu.@lock()
	defer { m.mu.unlock() }
	if m.metric_type == .gauge {
		m.value = v
	}
}

/// get_value returns the current metric value safely.
pub fn (mut m Metric) get_value() f64 {
	m.mu.@lock()
	defer { m.mu.unlock() }
	return m.value
}

/// MetricSnapshot holds a point-in-time copy of metric data for safe reading.
pub struct MetricSnapshot {
pub:
	value   f64
	count   u64
	sum     f64
	buckets []Bucket
}

/// snapshot returns a thread-safe copy of the metric data.
/// Uses unsafe cast to obtain a mutable reference from an immutable pointer,
/// which is safe here because the mutex ensures exclusive access during the copy.
pub fn (m &Metric) snapshot() MetricSnapshot {
	unsafe {
		mut mu := &m.mu
		mu.@lock()
		snap := MetricSnapshot{
			value:   m.value
			count:   m.count
			sum:     m.sum
			buckets: m.buckets.clone()
		}
		mu.unlock()
		return snap
	}
}

/// Histogram functions
pub fn (mut m Metric) observe(v f64) {
	m.mu.@lock()
	defer { m.mu.unlock() }
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

/// Export in Prometheus format
pub fn (r &MetricsRegistry) export_prometheus() string {
	mut sb := strings.new_builder(4096)

	for name, metric in r.metrics {
		// Take a thread-safe snapshot of values before formatting
		snap := metric.snapshot()

		// Help line
		sb.write_string('# HELP ${name} ${metric.help}\n')

		// Type line
		type_str := match metric.metric_type {
			.counter { 'counter' }
			.gauge { 'gauge' }
			.histogram { 'histogram' }
		}
		sb.write_string('# TYPE ${name} ${type_str}\n')

		// Value line
		match metric.metric_type {
			.counter, .gauge {
				sb.write_string('${name} ${snap.value}\n')
			}
			.histogram {
				for b in snap.buckets {
					sb.write_string('${name}_bucket{le="${b.upper_bound}"} ${b.count}\n')
				}
				sb.write_string('${name}_bucket{le="+Inf"} ${snap.count}\n')
				sb.write_string('${name}_sum ${snap.sum}\n')
				sb.write_string('${name}_count ${snap.count}\n')
			}
		}
		sb.write_string('\n')
	}

	return sb.str()
}

/// Timer is a timer for measuring latency.
pub struct Timer {
	start_time time.Time
	metric     &Metric
}

/// start_timer starts a new timer for a histogram metric.
pub fn (m &Metric) start_timer() Timer {
	return Timer{
		start_time: time.now()
		metric:     unsafe { m }
	}
}

/// observe_duration records the elapsed time.
pub fn (mut t Timer) observe_duration() {
	elapsed := time.since(t.start_time)
	seconds := f64(elapsed) / f64(time.second)
	unsafe {
		mut metric := t.metric
		metric.observe(seconds)
	}
}
