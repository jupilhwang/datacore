/// Infrastructure layer - Atomic metrics
/// High-performance metric collection to minimize lock contention
/// Uses sharding-based counters
module observability

import sync

// ShardedCounter - sharded counter (distributes lock contention)

const shard_count = 16

/// ShardedCounter is a counter that minimizes lock contention through sharding.
pub struct ShardedCounter {
pub mut:
	shards [16]Shard
}

/// Shard is the counter and lock for an individual shard.
pub struct Shard {
pub mut:
	value i64
	lock  sync.Mutex
}

/// new_sharded_counter creates a new ShardedCounter.
fn new_sharded_counter() ShardedCounter {
	mut sc := ShardedCounter{}
	return sc
}

/// get_shard_index calculates the shard index based on the current thread ID.
fn get_shard_index() int {
	return int(C.pthread_self()) % shard_count
}

/// inc increments the counter by 1.
fn (mut sc ShardedCounter) inc() {
	idx := get_shard_index()
	mut shard := &sc.shards[idx]
	shard.lock.@lock()
	shard.value += 1
	shard.lock.unlock()
}

/// inc_by increments the counter by the specified value.
fn (mut sc ShardedCounter) inc_by(delta i64) {
	if delta <= 0 {
		return
	}
	idx := get_shard_index()
	mut shard := &sc.shards[idx]
	shard.lock.@lock()
	shard.value += delta
	shard.lock.unlock()
}

/// dec decrements the counter by 1.
fn (mut sc ShardedCounter) dec() {
	idx := get_shard_index()
	mut shard := &sc.shards[idx]
	shard.lock.@lock()
	shard.value -= 1
	shard.lock.unlock()
}

/// dec_by decrements the counter by the specified value.
fn (mut sc ShardedCounter) dec_by(delta i64) {
	if delta <= 0 {
		return
	}
	idx := get_shard_index()
	mut shard := &sc.shards[idx]
	shard.lock.@lock()
	shard.value -= delta
	shard.lock.unlock()
}

/// get returns the sum of all shards.
fn (sc &ShardedCounter) get() i64 {
	mut total := i64(0)
	for i := 0; i < shard_count; i++ {
		mut shard := &sc.shards[i]
		shard.lock.@lock()
		total += shard.value
		shard.lock.unlock()
	}
	return total
}

/// set sets all shards to the specified value (distributed).
fn (mut sc ShardedCounter) set(val i64) {
	per_shard := val / shard_count
	remainder := val % shard_count
	for i := 0; i < shard_count; i++ {
		mut shard := &sc.shards[i]
		shard.lock.@lock()
		shard.value = per_shard
		if i64(i) < remainder {
			shard.value += 1
		}
		shard.lock.unlock()
	}
}

/// reset resets all shards to 0.
fn (mut sc ShardedCounter) reset() {
	for i := 0; i < shard_count; i++ {
		mut shard := &sc.shards[i]
		shard.lock.@lock()
		shard.value = 0
		shard.lock.unlock()
	}
}

// ShardedMetrics - sharded metrics collection

/// ShardedMetrics is a metric set that minimizes lock contention.
pub struct ShardedMetrics {
pub mut:
	topic_create_count   ShardedCounter
	topic_delete_count   ShardedCounter
	topic_lookup_count   ShardedCounter
	append_count         ShardedCounter
	append_record_count  ShardedCounter
	append_bytes         ShardedCounter
	fetch_count          ShardedCounter
	fetch_record_count   ShardedCounter
	fetch_bytes          ShardedCounter
	delete_records_count ShardedCounter
	offset_commit_count  ShardedCounter
	offset_fetch_count   ShardedCounter
	group_save_count     ShardedCounter
	group_load_count     ShardedCounter
	group_delete_count   ShardedCounter
	error_count          ShardedCounter
	append_latency_us    ShardedCounter
	fetch_latency_us     ShardedCounter
}

/// new_sharded_metrics creates a new ShardedMetrics.
fn new_sharded_metrics() ShardedMetrics {
	return ShardedMetrics{
		topic_create_count:   new_sharded_counter()
		topic_delete_count:   new_sharded_counter()
		topic_lookup_count:   new_sharded_counter()
		append_count:         new_sharded_counter()
		append_record_count:  new_sharded_counter()
		append_bytes:         new_sharded_counter()
		fetch_count:          new_sharded_counter()
		fetch_record_count:   new_sharded_counter()
		fetch_bytes:          new_sharded_counter()
		delete_records_count: new_sharded_counter()
		offset_commit_count:  new_sharded_counter()
		offset_fetch_count:   new_sharded_counter()
		group_save_count:     new_sharded_counter()
		group_load_count:     new_sharded_counter()
		group_delete_count:   new_sharded_counter()
		error_count:          new_sharded_counter()
		append_latency_us:    new_sharded_counter()
		fetch_latency_us:     new_sharded_counter()
	}
}

/// record_append records an append operation.
fn (mut m ShardedMetrics) record_append(record_count int, bytes i64, latency_us i64) {
	m.append_count.inc()
	m.append_record_count.inc_by(i64(record_count))
	m.append_bytes.inc_by(bytes)
	m.append_latency_us.inc_by(latency_us)
}

/// record_fetch records a fetch operation.
fn (mut m ShardedMetrics) record_fetch(record_count int, bytes i64, latency_us i64) {
	m.fetch_count.inc()
	m.fetch_record_count.inc_by(i64(record_count))
	m.fetch_bytes.inc_by(bytes)
	m.fetch_latency_us.inc_by(latency_us)
}

/// record_topic_create records a topic creation.
fn (mut m ShardedMetrics) record_topic_create() {
	m.topic_create_count.inc()
}

/// record_topic_delete records a topic deletion.
fn (mut m ShardedMetrics) record_topic_delete() {
	m.topic_delete_count.inc()
}

/// record_topic_lookup records a topic lookup.
fn (mut m ShardedMetrics) record_topic_lookup() {
	m.topic_lookup_count.inc()
}

/// record_offset_commit records an offset commit.
fn (mut m ShardedMetrics) record_offset_commit(count int) {
	m.offset_commit_count.inc_by(i64(count))
}

/// record_offset_fetch records an offset fetch.
fn (mut m ShardedMetrics) record_offset_fetch(count int) {
	m.offset_fetch_count.inc_by(i64(count))
}

/// record_group_save records a group save.
fn (mut m ShardedMetrics) record_group_save() {
	m.group_save_count.inc()
}

/// record_group_load records a group load.
fn (mut m ShardedMetrics) record_group_load() {
	m.group_load_count.inc()
}

/// record_group_delete records a group deletion.
fn (mut m ShardedMetrics) record_group_delete() {
	m.group_delete_count.inc()
}

/// record_error records an error.
fn (mut m ShardedMetrics) record_error() {
	m.error_count.inc()
}

/// record_delete_records records a record deletion.
fn (mut m ShardedMetrics) record_delete_records() {
	m.delete_records_count.inc()
}

/// get_summary returns a metrics summary.
fn (m &ShardedMetrics) get_summary() ShardedMetricsSnapshot {
	return ShardedMetricsSnapshot{
		topic_create_count:   m.topic_create_count.get()
		topic_delete_count:   m.topic_delete_count.get()
		topic_lookup_count:   m.topic_lookup_count.get()
		append_count:         m.append_count.get()
		append_record_count:  m.append_record_count.get()
		append_bytes:         m.append_bytes.get()
		fetch_count:          m.fetch_count.get()
		fetch_record_count:   m.fetch_record_count.get()
		fetch_bytes:          m.fetch_bytes.get()
		delete_records_count: m.delete_records_count.get()
		offset_commit_count:  m.offset_commit_count.get()
		offset_fetch_count:   m.offset_fetch_count.get()
		group_save_count:     m.group_save_count.get()
		group_load_count:     m.group_load_count.get()
		group_delete_count:   m.group_delete_count.get()
		error_count:          m.error_count.get()
		append_latency_us:    m.append_latency_us.get()
		fetch_latency_us:     m.fetch_latency_us.get()
	}
}

/// reset resets all metrics to 0.
fn (mut m ShardedMetrics) reset() {
	m.topic_create_count.reset()
	m.topic_delete_count.reset()
	m.topic_lookup_count.reset()
	m.append_count.reset()
	m.append_record_count.reset()
	m.append_bytes.reset()
	m.fetch_count.reset()
	m.fetch_record_count.reset()
	m.fetch_bytes.reset()
	m.delete_records_count.reset()
	m.offset_commit_count.reset()
	m.offset_fetch_count.reset()
	m.group_save_count.reset()
	m.group_load_count.reset()
	m.group_delete_count.reset()
	m.error_count.reset()
	m.append_latency_us.reset()
	m.fetch_latency_us.reset()
}

// ShardedMetricsSnapshot - metrics snapshot

/// ShardedMetricsSnapshot stores metric values at a specific point in time.
pub struct ShardedMetricsSnapshot {
pub:
	topic_create_count   i64
	topic_delete_count   i64
	topic_lookup_count   i64
	append_count         i64
	append_record_count  i64
	append_bytes         i64
	fetch_count          i64
	fetch_record_count   i64
	fetch_bytes          i64
	delete_records_count i64
	offset_commit_count  i64
	offset_fetch_count   i64
	group_save_count     i64
	group_load_count     i64
	group_delete_count   i64
	error_count          i64
	append_latency_us    i64
	fetch_latency_us     i64
}

/// get_append_latency_avg_ms returns the average append latency in milliseconds.
fn (s &ShardedMetricsSnapshot) get_append_latency_avg_ms() f64 {
	if s.append_count == 0 {
		return 0.0
	}
	return f64(s.append_latency_us) / f64(s.append_count) / 1000.0
}

/// get_fetch_latency_avg_ms returns the average fetch latency in milliseconds.
fn (s &ShardedMetricsSnapshot) get_fetch_latency_avg_ms() f64 {
	if s.fetch_count == 0 {
		return 0.0
	}
	return f64(s.fetch_latency_us) / f64(s.fetch_count) / 1000.0
}

/// to_string returns a string representation of the metrics summary.
fn (s &ShardedMetricsSnapshot) to_string() string {
	return '[ShardedMetrics]
  Topics: create=${s.topic_create_count}, delete=${s.topic_delete_count}, lookup=${s.topic_lookup_count}
  Records: append=${s.append_count} (${s.append_record_count} records, ${s.append_bytes} bytes, avg_latency=${s.get_append_latency_avg_ms():.2f}ms), 
           fetch=${s.fetch_count} (${s.fetch_record_count} records, ${s.fetch_bytes} bytes, avg_latency=${s.get_fetch_latency_avg_ms():.2f}ms)
  Offsets: commit=${s.offset_commit_count}, fetch=${s.offset_fetch_count}
  Groups: save=${s.group_save_count}, load=${s.group_load_count}, delete=${s.group_delete_count}
  Errors: ${s.error_count}'
}

// Compatibility aliases

/// AtomicCounter type.
pub type AtomicCounter = ShardedCounter

/// AtomicMetrics type.
pub type AtomicMetrics = ShardedMetrics

/// AtomicMetricsSnapshot type.
pub type AtomicMetricsSnapshot = ShardedMetricsSnapshot

/// new_atomic_counter creates a new atomic counter.
fn new_atomic_counter() ShardedCounter {
	return new_sharded_counter()
}

/// new_atomic_metrics creates new atomic metrics.
fn new_atomic_metrics() ShardedMetrics {
	return new_sharded_metrics()
}
