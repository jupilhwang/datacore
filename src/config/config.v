// Configuration Management
module config

import os
import toml

pub struct Config {
pub:
    broker          BrokerConfig
    storage         StorageConfig
    schema_registry SchemaRegistryConfig
    observability   ObservabilityConfig
}

pub struct BrokerConfig {
pub:
    host                string = '0.0.0.0'
    port                int    = 9092
    broker_id           int    = 1
    cluster_id          string = 'datacore-cluster'
    max_connections     int    = 10000
    max_request_size    int    = 104857600
    request_timeout_ms  int    = 30000
    idle_timeout_ms     int    = 600000
}

pub struct StorageConfig {
pub:
    engine      string = 'memory'
    memory      MemoryStorageConfig
    s3          S3StorageConfig
    sqlite      SqliteStorageConfig
    postgres    PostgresStorageConfig
}

pub struct MemoryStorageConfig {
pub:
    max_memory_mb       int = 1024
    segment_size_bytes  int = 1073741824
}

pub struct S3StorageConfig {
pub:
    endpoint        string
    bucket          string
    access_key      string
    secret_key      string
    region          string = 'us-east-1'
    prefix          string
}

pub struct SqliteStorageConfig {
pub:
    path            string = 'datacore.db'
    journal_mode    string = 'WAL'
}

pub struct PostgresStorageConfig {
pub:
    host            string = 'localhost'
    port            int    = 5432
    database        string = 'datacore'
    user            string
    password        string
    pool_size       int    = 10
}

pub struct SchemaRegistryConfig {
pub:
    enabled     bool   = true
    topic       string = '__schemas'
}

pub struct ObservabilityConfig {
pub:
    metrics     MetricsConfig
    logging     LoggingConfig
    tracing     TracingConfig
}

pub struct MetricsConfig {
pub:
    enabled     bool   = true
    endpoint    string = '/metrics'
    port        int    = 9093
}

pub struct LoggingConfig {
pub:
    enabled     bool   = true
    level       string = 'info'
    format      string = 'json'
}

pub struct TracingConfig {
pub:
    enabled         bool   = false
    endpoint        string = 'http://localhost:4317'
    service_name    string = 'datacore'
    sample_rate     f64    = 1.0
}

// Load configuration from TOML file
pub fn load_config(path string) !Config {
    // Check if file exists
    if !os.exists(path) {
        // Return default config if no file
        return Config{}
    }
    
    content := os.read_file(path) or {
        return error('Failed to read config file: ${err}')
    }
    
    doc := toml.parse_text(content) or {
        return error('Failed to parse config file: ${err}')
    }
    
    // Parse broker config
    broker_host := get_string(doc, 'broker.host', '0.0.0.0')
    broker_port := get_int(doc, 'broker.port', 9092)
    broker_id := get_int(doc, 'broker.broker_id', 1)
    cluster_id := get_string(doc, 'broker.cluster_id', 'datacore-cluster')
    max_connections := get_int(doc, 'broker.max_connections', 10000)
    max_request_size := get_int(doc, 'broker.max_request_size', 104857600)
    request_timeout_ms := get_int(doc, 'broker.request_timeout_ms', 30000)
    idle_timeout_ms := get_int(doc, 'broker.idle_timeout_ms', 600000)
    
    broker := BrokerConfig{
        host: broker_host
        port: broker_port
        broker_id: broker_id
        cluster_id: cluster_id
        max_connections: max_connections
        max_request_size: max_request_size
        request_timeout_ms: request_timeout_ms
        idle_timeout_ms: idle_timeout_ms
    }
    
    // Parse storage config
    storage_engine := get_string(doc, 'storage.engine', 'memory')
    max_memory_mb := get_int(doc, 'storage.memory.max_memory_mb', 1024)
    
    storage := StorageConfig{
        engine: storage_engine
        memory: MemoryStorageConfig{
            max_memory_mb: max_memory_mb
        }
    }
    
    // Parse schema registry config
    schema_enabled := get_bool(doc, 'schema_registry.enabled', true)
    schema_topic := get_string(doc, 'schema_registry.topic', '__schemas')
    
    schema_registry := SchemaRegistryConfig{
        enabled: schema_enabled
        topic: schema_topic
    }
    
    // Parse observability config
    metrics_enabled := get_bool(doc, 'observability.metrics.enabled', true)
    metrics_port := get_int(doc, 'observability.metrics.port', 9093)
    logging_level := get_string(doc, 'observability.logging.level', 'info')
    logging_format := get_string(doc, 'observability.logging.format', 'json')
    
    observability := ObservabilityConfig{
        metrics: MetricsConfig{
            enabled: metrics_enabled
            port: metrics_port
        }
        logging: LoggingConfig{
            level: logging_level
            format: logging_format
        }
    }
    
    return Config{
        broker: broker
        storage: storage
        schema_registry: schema_registry
        observability: observability
    }
}

// Helper function to get string value from TOML
fn get_string(doc toml.Doc, key string, default_val string) string {
    val := doc.value_opt(key) or { return default_val }
    return val.string()
}

// Helper function to get int value from TOML
fn get_int(doc toml.Doc, key string, default_val int) int {
    val := doc.value_opt(key) or { return default_val }
    return val.int()
}

// Helper function to get bool value from TOML
fn get_bool(doc toml.Doc, key string, default_val bool) bool {
    val := doc.value_opt(key) or { return default_val }
    return val.bool()
}

// Save configuration to TOML file
pub fn (c Config) save(path string) ! {
    mut content := '# DataCore Configuration\n\n'
    
    content += '[broker]\n'
    content += 'host = "${c.broker.host}"\n'
    content += 'port = ${c.broker.port}\n'
    content += 'broker_id = ${c.broker.broker_id}\n'
    content += 'cluster_id = "${c.broker.cluster_id}"\n'
    content += 'max_connections = ${c.broker.max_connections}\n'
    content += 'max_request_size = ${c.broker.max_request_size}\n'
    content += '\n'
    
    content += '[storage]\n'
    content += 'engine = "${c.storage.engine}"\n'
    content += '\n'
    
    content += '[storage.memory]\n'
    content += 'max_memory_mb = ${c.storage.memory.max_memory_mb}\n'
    content += '\n'
    
    content += '[schema_registry]\n'
    content += 'enabled = ${c.schema_registry.enabled}\n'
    content += 'topic = "${c.schema_registry.topic}"\n'
    content += '\n'
    
    content += '[observability.metrics]\n'
    content += 'enabled = ${c.observability.metrics.enabled}\n'
    content += 'port = ${c.observability.metrics.port}\n'
    content += '\n'
    
    content += '[observability.logging]\n'
    content += 'level = "${c.observability.logging.level}"\n'
    content += 'format = "${c.observability.logging.format}"\n'
    
    os.write_file(path, content)!
}
