// Unit tests for the gRPC interface server
module grpc

import domain
import infra.storage.plugins.memory
import service.port

fn test_new_grpc_server_creates_instance() {
	storage := port.StoragePort(memory.new_memory_adapter())
	config := GrpcServerConfig{
		host:       '127.0.0.1'
		port:       19094
		broker_id:  1
		cluster_id: 'test-cluster'
	}
	grpc_config := domain.default_grpc_config()

	server := new_grpc_server(config, storage, grpc_config)

	assert server.config.host == '127.0.0.1'
	assert server.config.port == 19094
	assert server.config.broker_id == 1
	assert server.config.cluster_id == 'test-cluster'
	assert server.running == false
}

fn test_grpc_server_config_defaults() {
	config := GrpcServerConfig{}

	assert config.host == '0.0.0.0'
	assert config.port == 9093
	assert config.broker_id == 1
	assert config.cluster_id == 'datacore-cluster'
}
