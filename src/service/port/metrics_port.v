module port

/// CounterMetric abstracts a counter metric for the service layer.
pub interface CounterMetric {
mut:
	inc()
	inc_by(v f64)
}

/// HistogramMetric abstracts a histogram metric for the service layer.
pub interface HistogramMetric {
mut:
	observe(v f64)
}

/// NoopCounter is a silent counter for use when no metric is provided.
pub struct NoopCounter {}

pub fn new_noop_counter() &NoopCounter {
	return &NoopCounter{}
}

pub fn (mut c NoopCounter) inc() {}

pub fn (mut c NoopCounter) inc_by(v f64) {}

/// NoopHistogram is a silent histogram for use when no metric is provided.
pub struct NoopHistogram {}

pub fn new_noop_histogram() &NoopHistogram {
	return &NoopHistogram{}
}

pub fn (mut h NoopHistogram) observe(v f64) {}
