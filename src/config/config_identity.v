// Broker identity generation
// Deterministic broker ID based on server identity (MAC, IP, hostname).
module config

import os
import rand

// Deterministic Broker ID generation
// Generates the same broker_id on the same server every time,
// based on unique server identifiers (MAC address, IP address, hostname).
// Fallback order: MAC address -> IP address -> hostname -> random

/// generate_deterministic_broker_id generates a deterministic broker_id based on server identity.
/// Always returns the same value when run on the same server.
fn generate_deterministic_broker_id() int {
	// priority 1: MAC address
	mac := get_mac_address()
	if mac.len > 0 {
		return string_to_broker_id(mac)
	}

	// priority 2: IP address
	ip := get_primary_ip()
	if ip.len > 0 {
		return string_to_broker_id(ip)
	}

	// priority 3: hostname
	hostname := os.hostname() or { '' }
	if hostname.len > 0 {
		return string_to_broker_id(hostname)
	}

	// last resort: random fallback
	return rand.int_in_range(1, 99999999) or { 1 }
}

/// get_mac_address returns the MAC address of the first physical network interface.
/// Selects the first valid MAC that is not loopback (00:00:00:00:00:00).
fn get_mac_address() string {
	// Linux: read from /sys/class/net/ directory
	if os.exists('/sys/class/net') {
		result := os.execute('for iface in /sys/class/net/*; do cat "\${iface}/address" 2>/dev/null; done')
		if result.exit_code == 0 && result.output.len > 0 {
			lines := result.output.split('\n')
			for line in lines {
				mac := line.trim_space()
				if mac.len > 0 && mac != '00:00:00:00:00:00' {
					return mac
				}
			}
		}
	}

	// macOS: read from ifconfig
	result := os.execute('ifconfig 2>/dev/null | grep ether | head -1')
	if result.exit_code == 0 && result.output.len > 0 {
		parts := result.output.trim_space().split(' ')
		for i, part in parts {
			if part == 'ether' && i + 1 < parts.len {
				mac := parts[i + 1].trim_space()
				if mac.len > 0 && mac != '00:00:00:00:00:00' {
					return mac
				}
			}
		}
	}

	return ''
}

/// get_primary_ip returns the primary IP address used for external connections.
fn get_primary_ip() string {
	// Linux: get source IP via ip route
	result_ip := os.execute("ip route get 1.1.1.1 2>/dev/null | grep -oP 'src \\K[^ ]+'")
	if result_ip.exit_code == 0 && result_ip.output.len > 0 {
		ip := result_ip.output.trim_space()
		if ip.len > 0 && ip != '127.0.0.1' {
			return ip
		}
	}

	// macOS: get default interface then extract IP
	result_mac := os.execute('ipconfig getifaddr en0 2>/dev/null')
	if result_mac.exit_code == 0 && result_mac.output.len > 0 {
		ip := result_mac.output.trim_space()
		if ip.len > 0 && ip != '127.0.0.1' {
			return ip
		}
	}

	return ''
}

/// string_to_broker_id hashes a string using FNV-1a and maps it to a broker_id in the range 1~99999999.
fn string_to_broker_id(s string) int {
	// FNV-1a 32-bit hash
	mut hash := u64(0x811c9dc5)
	for b in s.bytes() {
		hash = hash ^ u64(b)
		hash = (hash * u64(0x01000193)) & u64(0xFFFFFFFF)
	}
	// map to range 1 ~ 99999999
	return int(hash % 99999998) + 1
}
