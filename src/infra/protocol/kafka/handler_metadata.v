// Metadata handlers - ApiVersions, Metadata, FindCoordinator, DescribeCluster
module kafka

import domain

// ApiVersions handler
fn (h Handler) handle_api_versions(version i16) []u8 {
	// Support up to v3 (flexible)
	// If client requests > v3, downgrade to v3
	safe_version := if version > 3 { i16(3) } else { version }

	resp := new_api_versions_response()
	return resp.encode(safe_version)
}

// Metadata handler
fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata,
		version))!

	mut resp_topics := []MetadataResponseTopic{}

	// If specific topics requested, return only those
	if req.topics.len > 0 {
		for req_topic in req.topics {
			// Extract topic name - skip if not provided
			if topic_name := req_topic.name {
				// Try to get topic from storage
				mut topic := h.storage.get_topic(topic_name) or {
					// Topic not found - check if auto-create is enabled
					if req.allow_auto_topic_creation {
						// Auto-create topic with default config (1 partition)
						h.storage.create_topic(topic_name, 1, domain.TopicConfig{}) or {
							// Failed to create - return error response
							resp_topics << MetadataResponseTopic{
								error_code:           3 // UNKNOWN_TOPIC_OR_PARTITION
								name:                 topic_name
								topic_id:             []u8{len: 16}
								is_internal:          false
								partitions:           []
								topic_authorized_ops: -2147483648
							}
							continue
						}
						// Get the newly created topic
						h.storage.get_topic(topic_name) or {
							resp_topics << MetadataResponseTopic{
								error_code:           3
								name:                 topic_name
								topic_id:             []u8{len: 16}
								is_internal:          false
								partitions:           []
								topic_authorized_ops: -2147483648
							}
							continue
						}
					} else {
						// Auto-create disabled - return error response
						resp_topics << MetadataResponseTopic{
							error_code:           3 // UNKNOWN_TOPIC_OR_PARTITION
							name:                 topic_name
							topic_id:             []u8{len: 16}
							is_internal:          false
							partitions:           []
							topic_authorized_ops: -2147483648
						}
						continue
					}
				}

				mut partitions := []MetadataResponsePartition{}
				for p in 0 .. topic.partition_count {
					partitions << MetadataResponsePartition{
						error_code:       0
						partition_index:  p
						leader_id:        h.broker_id
						leader_epoch:     0
						replica_nodes:    [h.broker_id]
						isr_nodes:        [h.broker_id]
						offline_replicas: []
					}
				}

				resp_topics << MetadataResponseTopic{
					error_code:           0
					name:                 topic.name
					topic_id:             topic.topic_id
					is_internal:          topic.is_internal
					partitions:           partitions
					topic_authorized_ops: -2147483648
				}
			}
		}
	} else {
		// No specific topics requested - return all topics
		topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }

		for topic in topic_list {
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  p
					leader_id:        h.broker_id
					leader_epoch:     0
					replica_nodes:    [h.broker_id]
					isr_nodes:        [h.broker_id]
					offline_replicas: []
				}
			}

			resp_topics << MetadataResponseTopic{
				error_code:           0
				name:                 topic.name
				topic_id:             topic.topic_id
				is_internal:          topic.is_internal
				partitions:           partitions
				topic_authorized_ops: -2147483648
			}
		}
	}

	// Build metadata response
	resp := MetadataResponse{
		throttle_time_ms:       0
		brokers:                [
			MetadataResponseBroker{
				node_id: h.broker_id
				host:    h.host
				port:    h.broker_port
				rack:    none
			},
		]
		cluster_id:             h.cluster_id
		controller_id:          h.broker_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648 // Unknown
	}

	eprintln('[DEBUG] handle_metadata: Building response with broker node_id=${h.broker_id} host="${h.host}" (len=${h.host.len}) port=${h.broker_port}')
	return resp.encode(version)
}

// FindCoordinator handler
fn (h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator,
		version))!

	mut resp := FindCoordinatorResponse{}
	if version >= 4 {
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       h.broker_id
				host:          h.host
				port:          h.broker_port
				error_code:    0
				error_message: none
			}
		}

		resp = FindCoordinatorResponse{
			throttle_time_ms: 0
			coordinators:     coordinators
		}
	} else {
		resp = FindCoordinatorResponse{
			throttle_time_ms: 0
			error_code:       0
			error_message:    none
			node_id:          h.broker_id
			host:             h.host
			port:             h.broker_port
			coordinators:     []FindCoordinatorResponseNode{}
		}
	}

	return resp.encode(version)
}

// DescribeCluster handler
fn (mut h Handler) handle_describe_cluster(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_cluster_request(mut reader, version, is_flexible_version(.describe_cluster,
		version))!

	resp := h.process_describe_cluster(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_describe_cluster(req DescribeClusterRequest, version i16) !DescribeClusterResponse {
	// Return cluster info
	// Note: We currently don't support ACLs or detailed cluster operations, so we return authorized operations = INT32_MIN (unknown)

	// Create broker list with ONE broker (this one)
	brokers := [
		DescribeClusterBroker{
			broker_id: h.broker_id
			host:      h.host        // This is the ADVERTISED host
			port:      h.broker_port // This is the ADVERTISED port
			rack:      none
		},
	]

	return DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    h.cluster_id
		controller_id:                 h.broker_id // We act as our own controller
		brokers:                       brokers
		cluster_authorized_operations: -2147483648 // INT32_MIN = Unknown/Not Supported
	}
}

// Process Metadata request (Frame-based)
// NOTE: DataCore Stateless Architecture
// - leader_id: always this broker (all brokers are equivalent)
// - leader_epoch: always 0 (no leader election)
// - replica_nodes/isr_nodes: always [broker_id] (no replication, shared storage)
fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
	mut resp_topics := []MetadataResponseTopic{}

	// If client requested specific topics, return only those
	// If no topics specified (null or empty), return all topics
	if req.topics.len > 0 {
		// Client requested specific topics
		for req_topic in req.topics {
			topic_name := req_topic.name or { '' }
			if topic_name.len == 0 {
				continue
			}

			// Try to get the topic from storage
			topic := h.storage.get_topic(topic_name) or {
				// Topic not found - return error response for this topic
				resp_topics << MetadataResponseTopic{
					error_code:  i16(ErrorCode.unknown_topic_or_partition)
					name:        topic_name
					topic_id:    []u8{len: 16}
					is_internal: false
					partitions:  []
				}
				continue
			}

			// Topic found - build partition list
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        h.broker_id
					leader_epoch:     0
					replica_nodes:    [h.broker_id]
					isr_nodes:        [h.broker_id]
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:  0
				name:        topic.name
				topic_id:    topic.topic_id
				is_internal: topic.is_internal
				partitions:  partitions
			}
		}
	} else {
		// No specific topics requested - return all topics
		topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }

		for topic in topic_list {
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        h.broker_id
					leader_epoch:     0
					replica_nodes:    [h.broker_id]
					isr_nodes:        [h.broker_id]
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:  0
				name:        topic.name
				topic_id:    topic.topic_id
				is_internal: topic.is_internal
				partitions:  partitions
			}
		}
	}

	return MetadataResponse{
		throttle_time_ms:       0
		brokers:                [
			MetadataResponseBroker{
				node_id: h.broker_id
				host:    h.host
				port:    h.broker_port
				rack:    ''
			},
		]
		cluster_id:             h.cluster_id
		controller_id:          h.broker_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648 // Unknown
	}
}

// Process FindCoordinator request (Frame-based)
fn (mut h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
	if version >= 4 {
		// v4+: return coordinators array
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       h.broker_id
				host:          h.host
				port:          h.broker_port
				error_code:    0
				error_message: none
			}
		}

		return FindCoordinatorResponse{
			throttle_time_ms: 0
			coordinators:     coordinators
		}
	}

	// v0-v3: single coordinator fields
	return FindCoordinatorResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    none
		node_id:          h.broker_id
		host:             h.host
		port:             h.broker_port
		coordinators:     []FindCoordinatorResponseNode{}
	}
}
