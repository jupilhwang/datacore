// Topic REST API handlers
//
// Handles CRUD operations for topics:
// - GET /v1/topics: List topics
// - POST /v1/topics: Create topic
// - GET /v1/topics/{name}: Get topic info
// - DELETE /v1/topics/{name}: Delete topic
module rest

import domain
import json
import net
import regex

// TopicResponse is the response struct representing a single topic.
struct TopicResponse {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// TopicsListResponse is the response struct for listing topics.
struct TopicsListResponse {
	topics []TopicResponse @[json: 'topics']
}

// CreateTopicRequest is the request struct for POST /v1/topics.
struct CreateTopicRequest {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// handle_topics_api handles topic REST API requests.
fn (mut s RestServer) handle_topics_api(method string, path string, query map[string]string, headers map[string]string, body string, mut conn net.TcpConn) {
	parts := path.trim_left('/').split('/')

	// GET /v1/topics - list topics
	// POST /v1/topics - create topic
	if parts.len == 2 && parts[1] == 'topics' {
		match method {
			'GET' {
				s.list_topics(mut conn)
			}
			'POST' {
				s.create_topic(body, mut conn)
			}
			else {
				s.send_error(mut conn, 405, 40501, 'Method Not Allowed')
				conn.close() or {}
			}
		}
		return
	}

	// GET /v1/topics/{topic} - topic info
	// DELETE /v1/topics/{topic} - delete topic
	if parts.len == 3 && parts[1] == 'topics' {
		match method {
			'GET' {
				s.get_topic(parts[2], mut conn)
			}
			'DELETE' {
				s.delete_topic(parts[2], mut conn)
			}
			else {
				s.send_error(mut conn, 405, 40501, 'Method Not Allowed')
				conn.close() or {}
			}
		}
		return
	}

	s.send_error(mut conn, 404, 40401, 'Not Found')
	conn.close() or {}
}

// list_topics returns a list of all topics.
fn (mut s RestServer) list_topics(mut conn net.TcpConn) {
	topics := s.storage.list_topics() or {
		s.send_error(mut conn, 500, 50001, 'Failed to list topics')
		conn.close() or {}
		return
	}

	mut topic_list := []TopicResponse{}
	for topic in topics {
		topic_list << TopicResponse{
			name:       topic.name
			partitions: topic.partition_count
		}
	}

	resp := TopicsListResponse{
		topics: topic_list
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// get_topic returns topic information.
fn (mut s RestServer) get_topic(name string, mut conn net.TcpConn) {
	topic := s.storage.get_topic(name) or {
		s.send_error(mut conn, 404, 40401, 'Topic not found')
		conn.close() or {}
		return
	}

	resp := TopicResponse{
		name:       topic.name
		partitions: topic.partition_count
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// create_topic creates a new topic.
// body is pre-read by handle_connection using a buffered reader.
fn (mut s RestServer) create_topic(body string, mut conn net.TcpConn) {
	if body == '' {
		s.send_error(mut conn, 400, 40001, 'Request body is required')
		conn.close() or {}
		return
	}

	req := json.decode(CreateTopicRequest, body) or {
		s.send_error(mut conn, 400, 40001, 'Invalid request body: ${err}')
		conn.close() or {}
		return
	}

	if req.name == '' {
		s.send_error(mut conn, 400, 40001, 'Topic name is required')
		conn.close() or {}
		return
	}

	// Validate topic name: ^[a-zA-Z0-9._-]{1,249}$
	if req.name.len > 249 || !is_valid_topic_name(req.name) {
		s.send_error(mut conn, 400, 40002, 'Invalid topic name. Use alphanumeric, dots, underscores, hyphens (max 249 chars)')
		conn.close() or {}
		return
	}

	partitions := if req.partitions > 0 { req.partitions } else { 1 }

	topic_meta := s.storage.create_topic(req.name, partitions, domain.TopicConfig{}) or {
		// Return 409 Conflict if topic already exists
		if err.msg().contains('already exists') {
			s.send_error(mut conn, 409, 40901, 'Topic already exists: ${req.name}')
		} else {
			s.send_error(mut conn, 500, 50001, 'Failed to create topic: ${err}')
		}
		conn.close() or {}
		return
	}

	resp := TopicResponse{
		name:       topic_meta.name
		partitions: topic_meta.partition_count
	}
	s.send_json(mut conn, 201, json.encode(resp))
	conn.close() or {}
}

// delete_topic deletes a topic.
fn (mut s RestServer) delete_topic(name string, mut conn net.TcpConn) {
	s.storage.delete_topic(name) or {
		if err.msg().contains('not found') || err.msg().contains('does not exist') {
			s.send_error(mut conn, 404, 40401, 'Topic not found: ${name}')
		} else {
			s.send_error(mut conn, 500, 50001, 'Failed to delete topic: ${err}')
		}
		conn.close() or {}
		return
	}

	// 204 No Content - successfully deleted
	s.send_json(mut conn, 204, '')
	conn.close() or {}
}

// is_valid_topic_name validates that a topic name matches the pattern ^[a-zA-Z0-9._-]{1,249}$.
fn is_valid_topic_name(name string) bool {
	if name == '' || name.len > 249 {
		return false
	}
	mut re := regex.regex_opt(r'^[a-zA-Z0-9._\-]+$') or { return false }
	return re.matches_string(name)
}
