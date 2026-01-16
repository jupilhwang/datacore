// Unit tests for Connection Manager
module server

import time

fn test_extract_ip_ipv4() {
    // Test IPv4 address extraction
    assert extract_ip('127.0.0.1:9092') == '127.0.0.1'
    assert extract_ip('192.168.1.100:8080') == '192.168.1.100'
    assert extract_ip('10.0.0.1:443') == '10.0.0.1'
}

fn test_extract_ip_ipv6() {
    // Test IPv6 address extraction
    assert extract_ip('[::1]:9092') == '::1'
    assert extract_ip('[fe80::1]:8080') == 'fe80::1'
    assert extract_ip('[2001:db8::1]:443') == '2001:db8::1'
}

fn test_connection_manager_creation() {
    config := ServerConfig{
        max_connections: 100
        max_connections_per_ip: 10
        idle_timeout_ms: 60000
    }
    
    mut cm := new_connection_manager(config)
    
    assert cm.active_count() == 0
    
    metrics := cm.get_metrics()
    assert metrics.active_connections == 0
    assert metrics.total_connections == 0
    assert metrics.rejected_connections == 0
}

fn test_connection_metrics() {
    config := ServerConfig{
        max_connections: 100
        max_connections_per_ip: 10
    }
    
    mut cm := new_connection_manager(config)
    
    // Initial metrics
    metrics := cm.get_metrics()
    assert metrics.active_connections == 0
    assert metrics.total_connections == 0
    assert metrics.total_bytes_received == 0
    assert metrics.total_bytes_sent == 0
    assert metrics.total_requests == 0
}

fn test_client_connection_struct() {
    now := time.now()
    client := ClientConnection{
        fd: 10
        remote_addr: '127.0.0.1:12345'
        connected_at: now
        last_active_at: now
        request_count: 5
        bytes_received: 1000
        bytes_sent: 2000
        client_id: 'test-client'
        api_version: 2
        client_sw_name: 'test-app'
        client_sw_ver: '1.0.0'
    }
    
    assert client.fd == 10
    assert client.remote_addr == '127.0.0.1:12345'
    assert client.request_count == 5
    assert client.bytes_received == 1000
    assert client.bytes_sent == 2000
    assert client.client_id == 'test-client'
}

fn test_server_config_defaults() {
    config := ServerConfig{}
    
    assert config.host == '0.0.0.0'
    assert config.port == 9092
    assert config.broker_id == 1
    assert config.cluster_id == 'datacore-cluster'
    assert config.max_connections == 10000
    assert config.max_connections_per_ip == 100
    assert config.idle_timeout_ms == 600000
    assert config.request_timeout_ms == 30000
    assert config.max_request_size == 104857600
}

fn test_get_all_connections_empty() {
    config := ServerConfig{
        max_connections: 100
        max_connections_per_ip: 10
    }
    
    mut cm := new_connection_manager(config)
    
    connections := cm.get_all_connections()
    assert connections.len == 0
}

fn test_close_all_empty() {
    config := ServerConfig{
        max_connections: 100
        max_connections_per_ip: 10
    }
    
    mut cm := new_connection_manager(config)
    
    closed := cm.close_all()
    assert closed == 0
}

fn test_cleanup_idle_empty() {
    config := ServerConfig{
        max_connections: 100
        max_connections_per_ip: 10
        idle_timeout_ms: 1000
    }
    
    mut cm := new_connection_manager(config)
    
    closed := cm.cleanup_idle()
    assert closed == 0
}
