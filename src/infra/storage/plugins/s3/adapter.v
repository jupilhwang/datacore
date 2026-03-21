// S3 storage adapter with ETag-based concurrency control
module s3

import domain
import config as app_config
import service.port
import time
import crypto.md5
import sync
import sync.stdatomic

// NOTE: S3 client functions in s3_client.v; index types in partition_index.v;
// buffer types in buffer_manager.v; compaction in compaction.v
// NOTE: Topic CRUD in adapter_topic.v; record ops in adapter_record.v;
// consumer group/offset ops in adapter_group.v

// Configuration Constants

const max_topic_name_length = 255
const max_partition_count = 10000
const topic_cache_ttl = 3 * time.minute
const group_cache_ttl = 30 * time.second
const iceberg_cache_ttl = 5 * time.minute
const iceberg_cache_max_entries = 100
const record_overhead_bytes = 30
const fetch_size_multiplier = 2
const fetch_offset_estimate_divisor = 100
const max_offset_commit_buffer = 100
const max_offset_commit_concurrent = 50
const max_fetch_concurrent = 16

/// s3_capability defines the storage capabilities of the S3 adapter.
pub const s3_capability = domain.StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// S3Config holds the S3 storage configuration.
pub struct S3Config {
pub mut:
	bucket_name    string = 'datacore-storage'
	region         string = 'us-east-1'
	endpoint       string
	access_key     string
	secret_key     string
	prefix         string = 'datacore/'
	max_retries    int    = 3
	retry_delay_ms int    = 100
	use_path_style bool   = true
	timezone       string = 'UTC'
	// Batch settings
	batch_timeout_ms int
	batch_max_bytes  i64
	// Flush threshold settings: skip flush when buffer < min_flush_bytes
	min_flush_bytes      int = 65536
	max_flush_skip_count int = 80
	// Compaction settings
	compaction_interval_ms int
	target_segment_bytes   i64
	index_cache_ttl_ms     int
	// Broker settings
	broker_id i32
	// Offset batch settings
	offset_batch_enabled         bool = true
	offset_flush_interval_ms     int  = 100
	offset_flush_threshold_count int  = 50
	// Index batch settings: accumulate N segments before writing index to S3
	index_batch_size        int = 5
	index_flush_interval_ms int = 500
	// Sync linger: batch acks=1/-1 produce requests within a short window
	// 0 = disabled (immediate per-request write); >0 = linger window in ms
	sync_linger_ms int
	// Server-side copy: use S3 Multipart Copy for compaction to avoid data transfer
	use_server_side_copy bool = true
}

/// S3StorageAdapter implements the StoragePort for S3 storage.
pub struct S3StorageAdapter {
pub mut:
	config S3Config
	// Local cache with TTL
	topic_cache            map[string]CachedTopic
	topic_id_cache         map[string]string
	topic_id_reverse_cache map[string]string
	group_cache            map[string]CachedGroup
	offset_cache           map[string]map[string]i64
	topic_index_cache      map[string]CachedPartitionIndex
	// Locks for thread safety
	topic_lock  sync.RwMutex
	group_lock  sync.RwMutex
	offset_lock sync.RwMutex
	// Flush buffers for batched S3 writes
	topic_partition_buffers map[string]TopicPartitionBuffer
	buffer_lock             sync.RwMutex
	index_update_lock       sync.Mutex
	is_flushing_flag        i64
	// Flush skip counters per partition (for min_flush_bytes threshold)
	flush_skip_counts map[string]int
	// Compaction settings
	min_segment_count_to_compact int = 5
	is_running_flag              i64
	// WaitGroup for graceful worker shutdown
	worker_wg sync.WaitGroup
	// Metrics
	metrics      S3Metrics
	metrics_lock sync.Mutex
	// Offset batch buffers
	offset_buffers     map[string]OffsetGroupBuffer
	offset_buffer_lock sync.Mutex
	// Pending index batch updates: accumulate segments before writing index to S3
	pending_index_updates map[string][]LogSegment
	index_flush_counter   map[string]int
	index_flush_lock      sync.Mutex
	last_index_flush_at   i64
	// Iceberg table writers (when Iceberg is enabled)
	iceberg_writers map[string]&IcebergWriter
	iceberg_lock    sync.RwMutex
	// Iceberg 런타임 설정 (config 패키지와의 연결)
	iceberg_config IcebergConfig
	// Sync linger buffers for acks=1/-1 batching
	sync_linger_buffers map[string]SyncLingerBuffer
	sync_linger_lock    sync.Mutex
	// Per-partition append locks for offset reservation atomicity.
	// Prevents concurrent appends to the same partition from reading
	// the same high_watermark and creating overlapping offsets.
	partition_append_locks map[string]&sync.Mutex
	partition_append_mu    sync.Mutex
	// SigV4 signing key cache (valid per UTC day, avoids 4 HMAC-SHA256 ops per request)
	signing_key_cache CachedSigningKey
	signing_key_lock  sync.Mutex
}

/// CachedTopic holds cached topic information.
struct CachedTopic {
	meta      domain.TopicMetadata
	etag      string
	cached_at time.Time
}

/// CachedGroup holds cached group information.
struct CachedGroup {
	group     domain.ConsumerGroup
	etag      string
	cached_at time.Time
}

/// S3Metrics tracks metrics for S3 storage operations.
struct S3Metrics {
mut:
	// Flush metrics
	flush_count         i64
	flush_success_count i64
	flush_error_count   i64
	flush_total_ms      i64
	// Compaction metrics
	compaction_count         i64
	compaction_success_count i64
	compaction_error_count   i64
	compaction_total_ms      i64
	compaction_bytes_merged  i64
	// S3 API metrics
	s3_get_count    i64
	s3_put_count    i64
	s3_delete_count i64
	s3_list_count   i64
	s3_error_count  i64
	// Cache metrics
	cache_hit_count  i64
	cache_miss_count i64
	// Offset commit metrics
	offset_commit_count         i64
	offset_commit_success_count i64
	offset_commit_error_count   i64
	// Offset flush metrics
	offset_flush_count         i64
	offset_flush_success_count i64
	offset_flush_error_count   i64
	// Sync append metrics (acks != 0)
	sync_append_count         i64
	sync_append_success_count i64
	sync_append_error_count   i64
	sync_append_total_ms      i64
}

/// reset_metrics resets all metrics to zero.
fn (mut m S3Metrics) reset() {
	m.flush_count = 0
	m.flush_success_count = 0
	m.flush_error_count = 0
	m.flush_total_ms = 0
	m.compaction_count = 0
	m.compaction_success_count = 0
	m.compaction_error_count = 0
	m.compaction_total_ms = 0
	m.compaction_bytes_merged = 0
	m.s3_get_count = 0
	m.s3_put_count = 0
	m.s3_delete_count = 0
	m.s3_list_count = 0
	m.s3_error_count = 0
	m.cache_hit_count = 0
	m.cache_miss_count = 0
	m.offset_commit_count = 0
	m.offset_commit_success_count = 0
	m.offset_commit_error_count = 0
	m.sync_append_count = 0
	m.sync_append_success_count = 0
	m.sync_append_error_count = 0
	m.sync_append_total_ms = 0
}

/// get_summary returns a string summary of the metrics.
fn (m &S3Metrics) get_summary() string {
	flush_success_rate := if m.flush_count > 0 {
		f64(m.flush_success_count) / f64(m.flush_count) * 100.0
	} else {
		0.0
	}
	compaction_success_rate := if m.compaction_count > 0 {
		f64(m.compaction_success_count) / f64(m.compaction_count) * 100.0
	} else {
		0.0
	}
	cache_hit_rate := if m.cache_hit_count + m.cache_miss_count > 0 {
		f64(m.cache_hit_count) / f64(m.cache_hit_count + m.cache_miss_count) * 100.0
	} else {
		0.0
	}
	offset_commit_success_rate := if m.offset_commit_count > 0 {
		f64(m.offset_commit_success_count) / f64(m.offset_commit_count) * 100.0
	} else {
		0.0
	}
	sync_append_success_rate := if m.sync_append_count > 0 {
		f64(m.sync_append_success_count) / f64(m.sync_append_count) * 100.0
	} else {
		0.0
	}

	return '[S3 Metrics]
  Flush: ${m.flush_count} total, ${m.flush_success_count} success (${flush_success_rate:.1f}%), ${m.flush_total_ms}ms
  Compaction: ${m.compaction_count} total, ${m.compaction_success_count} success (${compaction_success_rate:.1f}%), ${m.compaction_bytes_merged} bytes merged, ${m.compaction_total_ms}ms
  S3 API: GET=${m.s3_get_count}, PUT=${m.s3_put_count}, DELETE=${m.s3_delete_count}, LIST=${m.s3_list_count}, errors=${m.s3_error_count}
  Cache: ${m.cache_hit_count} hits, ${m.cache_miss_count} misses (${cache_hit_rate:.1f}% hit rate)
  Offset Commit: ${m.offset_commit_count} total, ${m.offset_commit_success_count} success (${offset_commit_success_rate:.1f}%)
  Sync Append: ${m.sync_append_count} total, ${m.sync_append_success_count} success (${sync_append_success_rate:.1f}%), ${m.sync_append_total_ms}ms'
}

// Note: CachedPartitionIndex is defined in partition_index.v.

// S3 object key structure:
// {prefix}/topics/{topic_name}/metadata.json
// {prefix}/topics/{topic_name}/partitions/{partition}/log-{start_offset}-{end_offset}.bin
// {prefix}/topics/{topic_name}/partitions/{partition}/index.json
// {prefix}/groups/{group_id}/state.json
// {prefix}/offsets/{group_id}/{topic}:{partition}.json

/// new_s3_adapter creates a new S3 storage adapter.
pub fn new_s3_adapter(config S3Config) !&S3StorageAdapter {
	return &S3StorageAdapter{
		config:                  config
		topic_cache:             map[string]CachedTopic{}
		topic_id_reverse_cache:  map[string]string{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{}
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
		flush_skip_counts:       map[string]int{}
		offset_buffers:          map[string]OffsetGroupBuffer{}
		pending_index_updates:   map[string][]LogSegment{}
		index_flush_counter:     map[string]int{}
		iceberg_writers:         map[string]&IcebergWriter{}
		sync_linger_buffers:     map[string]SyncLingerBuffer{}
	}
}

/// from_storage_config creates an S3Config from the application-level S3StorageConfig.
/// Centralizes the mapping to reduce drift risk between the two config structs.
/// Fields not present in S3StorageConfig (max_retries, retry_delay_ms, use_path_style,
/// broker_id) retain their S3Config defaults and must be set separately if needed.
fn from_storage_config(cfg app_config.S3StorageConfig) S3Config {
	return S3Config{
		bucket_name:                  cfg.bucket
		region:                       cfg.region
		endpoint:                     cfg.endpoint
		access_key:                   cfg.access_key
		secret_key:                   cfg.secret_key
		prefix:                       cfg.prefix
		timezone:                     cfg.timezone
		batch_timeout_ms:             cfg.batch_timeout_ms
		batch_max_bytes:              cfg.batch_max_bytes
		min_flush_bytes:              cfg.min_flush_bytes
		max_flush_skip_count:         cfg.max_flush_skip_count
		compaction_interval_ms:       cfg.compaction_interval_ms
		target_segment_bytes:         cfg.target_segment_bytes
		index_cache_ttl_ms:           cfg.index_cache_ttl_ms
		offset_batch_enabled:         cfg.offset_batch_enabled
		offset_flush_interval_ms:     cfg.offset_flush_interval_ms
		offset_flush_threshold_count: cfg.offset_flush_threshold_count
		index_batch_size:             cfg.index_batch_size
		index_flush_interval_ms:      cfg.index_flush_interval_ms
		sync_linger_ms:               cfg.sync_linger_ms
		use_server_side_copy:         cfg.use_server_side_copy
	}
}

/// new_s3_adapter_from_storage_config creates a new S3 storage adapter
/// from the application-level S3StorageConfig. Uses from_storage_config
/// to centralize the field mapping. broker_id is passed separately because
/// it originates from BrokerConfig, not S3StorageConfig.
pub fn new_s3_adapter_from_storage_config(s3_cfg app_config.S3StorageConfig, broker_id i32) !&S3StorageAdapter {
	mut s3_config := from_storage_config(s3_cfg)
	s3_config.broker_id = broker_id
	return new_s3_adapter(s3_config)
}

/// health_check checks the storage health status.
pub fn (mut a S3StorageAdapter) health_check() !port.HealthStatus {
	// Attempt to list a small number of objects
	_ := a.list_objects(a.config.prefix) or { return .unhealthy }
	return .healthy
}

/// get_storage_capability returns storage capability information.
pub fn (a &S3StorageAdapter) get_storage_capability() domain.StorageCapability {
	return s3_capability
}

/// get_cluster_metadata_port returns the cluster metadata interface.
/// S3 supports multi-broker mode.
pub fn (mut a S3StorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	// Pass original adapter directly to use config copy + original pointer
	return new_s3_cluster_metadata_adapter(&a)
}

/// topic_metadata_key returns the S3 key for topic metadata.
fn (a &S3StorageAdapter) topic_metadata_key(name string) string {
	return '${a.config.prefix}topics/${name}/metadata.json'
}

/// partition_index_key returns the S3 key for a partition index.
fn (a &S3StorageAdapter) partition_index_key(topic string, partition int) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/index.json'
}

/// log_segment_key returns the S3 key for a log segment.
fn (a &S3StorageAdapter) log_segment_key(topic string, partition int, start i64, end i64) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/log-${start:016}-${end:016}.bin'
}

/// group_key returns the S3 key for a consumer group.
fn (a &S3StorageAdapter) group_key(group_id string) string {
	return '${a.config.prefix}groups/${group_id}/state.json'
}

/// offset_key returns the S3 key for an offset.
fn (a &S3StorageAdapter) offset_key(group_id string, topic string, partition int) string {
	return '${a.config.prefix}offsets/${group_id}/${topic}:${partition}.json'
}

// Moved to s3_client.v.

// Note: PartitionIndex, LogSegment, get_partition_index have been moved to partition_index.v.

// Note: StoredRecord and TopicPartitionBuffer are defined in s3_record_codec.v and buffer_manager.v.

// Note: async_flush_partition, flush_worker, flush_buffer_to_s3 have been moved to buffer_manager.v.

/// start_workers starts flush, compaction, and sync linger workers.
pub fn (mut a S3StorageAdapter) start_workers() {
	// Atomic check-then-set to prevent double-start race
	if stdatomic.load_i64(&a.is_running_flag) == 1 {
		return
	}
	stdatomic.store_i64(&a.is_running_flag, 1)

	mut worker_count := 2
	if a.config.sync_linger_ms > 0 {
		worker_count = 3
	}
	a.worker_wg.add(worker_count)

	go a.flush_worker()
	go a.compaction_worker()
	if a.config.sync_linger_ms > 0 {
		go a.sync_linger_worker()
	}
}

/// stop_workers signals all background workers to stop.
/// Workers drain pending buffers before exiting (graceful shutdown).
/// Blocks until all workers have completed via WaitGroup.
pub fn (mut a S3StorageAdapter) stop_workers() {
	stdatomic.store_i64(&a.is_running_flag, 0)
	a.worker_wg.wait()
}

/// get_metrics returns the current metrics snapshot.
pub fn (mut a S3StorageAdapter) get_metrics() S3Metrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary returns the metrics summary string.
pub fn (mut a S3StorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics resets all metrics to zero.
pub fn (mut a S3StorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}

// Note: compaction_worker, compact_all_partitions, compact_partition, merge_segments have been moved to compaction.v.

// Note: encode_stored_records and decode_stored_records have been moved to s3_record_codec.v.

/// generate_topic_id generates a topic_id from the topic name.
fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}

/// record_cache_hit increments the cache hit counter.
fn (mut a S3StorageAdapter) record_cache_hit() {
	stdatomic.add_i64(&a.metrics.cache_hit_count, 1)
}

/// record_cache_miss increments the cache miss and S3 GET counters.
fn (mut a S3StorageAdapter) record_cache_miss() {
	stdatomic.add_i64(&a.metrics.cache_miss_count, 1)
	stdatomic.add_i64(&a.metrics.s3_get_count, 1)
}

/// record_sync_append_error increments the sync append error and S3 error counters.
fn (mut a S3StorageAdapter) record_sync_append_error() {
	stdatomic.add_i64(&a.metrics.sync_append_error_count, 1)
	stdatomic.add_i64(&a.metrics.s3_error_count, 1)
}

/// record_sync_append_put increments the S3 PUT and sync append counters.
fn (mut a S3StorageAdapter) record_sync_append_put() {
	stdatomic.add_i64(&a.metrics.s3_put_count, 1)
	stdatomic.add_i64(&a.metrics.sync_append_count, 1)
}

/// record_sync_append_index_error increments only the sync append error counter.
fn (mut a S3StorageAdapter) record_sync_append_index_error() {
	stdatomic.add_i64(&a.metrics.sync_append_error_count, 1)
}

/// record_sync_append_success increments the success counter and accumulates elapsed time.
// NOTE: stdatomic.add_i64 delta parameter is int (i32), not i64.
// Individual elapsed_ms values are typically small (< 2^31 ms = ~24 days per call),
// so truncation of a single sample is acceptable. The i64 accumulator field
// (sync_append_total_ms) handles the cumulative sum without overflow.
fn (mut a S3StorageAdapter) record_sync_append_success(elapsed_ms i64) {
	stdatomic.add_i64(&a.metrics.sync_append_success_count, 1)
	stdatomic.add_i64(&a.metrics.sync_append_total_ms, int(elapsed_ms))
}

// TODO(jira#XXX): Implement S3 shared partition state persistence
/// save_share_partition_state saves a SharePartition state (not yet implemented for S3).
pub fn (mut a S3StorageAdapter) save_share_partition_state(state domain.SharePartitionState) ! {
	return error('share partition state persistence not yet implemented for S3')
}

/// load_share_partition_state loads a SharePartition state (not yet implemented for S3).
pub fn (mut a S3StorageAdapter) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

/// delete_share_partition_state deletes a SharePartition state (not yet implemented for S3).
pub fn (mut a S3StorageAdapter) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {
	return error('share partition state persistence not yet implemented for S3')
}

/// load_all_share_partition_states loads all SharePartition states for a group (not yet implemented for S3).
pub fn (mut a S3StorageAdapter) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
