// Health check handlers for REST API server
//
// Handles Kubernetes-compatible health check endpoints:
// - /health, /healthz: Full health status
// - /ready, /readyz: Readiness probe
// - /live, /livez: Liveness probe
module rest

import json
import net
import time

// Application version matching src/v.mod. Update on release.
const app_version = '0.50.2'

// HealthResponse is the health check response struct.
struct HealthResponse {
	status         string @[json: 'status']
	storage        string @[json: 'storage']
	uptime_seconds i64    @[json: 'uptime_seconds']
	version        string @[json: 'version']
}

// ReadyResponse is the response struct for the /ready endpoint.
struct ReadyResponse {
	ready  bool   @[json: 'ready']
	reason string @[json: 'reason'; omitempty]
}

// LiveResponse is the response struct for the /live endpoint.
struct LiveResponse {
	alive bool @[json: 'alive']
}

// handle_health handles the /health endpoint.
// Returns full health status including storage state.
fn (mut s RestServer) handle_health(mut conn net.TcpConn) {
	storage_status := s.storage.health_check() or {
		resp := HealthResponse{
			status:  'unhealthy'
			storage: 'error'
		}
		s.send_json(mut conn, 503, json.encode(resp))
		return
	}

	status := match storage_status {
		.healthy { 'healthy' }
		.degraded { 'degraded' }
		.unhealthy { 'unhealthy' }
	}

	http_status := match storage_status {
		.healthy { 200 }
		.degraded { 200 }
		.unhealthy { 503 }
	}

	uptime_seconds := time.since(s.start_time) / time.second

	resp := HealthResponse{
		status:         status
		storage:        status
		uptime_seconds: uptime_seconds
		version:        app_version
	}
	s.send_json(mut conn, http_status, json.encode(resp))
}

// handle_ready handles the /ready endpoint.
// Returns whether the server is ready to receive traffic.
fn (mut s RestServer) handle_ready(mut conn net.TcpConn) {
	if s.ready {
		storage_status := s.storage.health_check() or {
			resp := ReadyResponse{
				ready:  false
				reason: 'storage_unavailable'
			}
			s.send_json(mut conn, 503, json.encode(resp))
			return
		}

		if storage_status == .unhealthy {
			resp := ReadyResponse{
				ready:  false
				reason: 'storage_unhealthy'
			}
			s.send_json(mut conn, 503, json.encode(resp))
			return
		}

		resp := ReadyResponse{
			ready: true
		}
		s.send_json(mut conn, 200, json.encode(resp))
	} else {
		resp := ReadyResponse{
			ready:  false
			reason: 'server_not_ready'
		}
		s.send_json(mut conn, 503, json.encode(resp))
	}
}

// handle_live handles the /live endpoint.
// Returns whether the server process is alive.
fn (mut s RestServer) handle_live(mut conn net.TcpConn) {
	if s.running {
		s.send_json(mut conn, 200, json.encode(LiveResponse{ alive: true }))
	} else {
		s.send_json(mut conn, 503, json.encode(LiveResponse{ alive: false }))
	}
}
