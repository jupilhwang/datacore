// Broker identity generation
// Deterministic broker ID based on server identity (MAC, IP, hostname).
// Uses direct file reads (/sys/class/net, /proc/net/fib_trie) instead of
// shell commands for Docker compatibility (Alpine, scratch, distroless).
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
/// Reads directly from /sys/class/net/ on Linux, falls back to ifconfig on macOS.
fn get_mac_address() string {
	$if linux {
		return read_mac_from_sysfs()
	} $else $if macos {
		return read_mac_from_ifconfig()
	}
	return ''
}

/// get_primary_ip returns the primary IP address used for external connections.
/// Parses /proc/net/fib_trie on Linux, falls back to ipconfig on macOS.
fn get_primary_ip() string {
	$if linux {
		return read_ip_from_proc()
	} $else $if macos {
		return read_ip_from_ipconfig()
	}
	return ''
}

/// read_mac_from_sysfs reads MAC from /sys/class/net/*/address.
/// Skips loopback and virtual interfaces (docker, veth, br-).
fn read_mac_from_sysfs() string {
	entries := os.ls('/sys/class/net') or { return '' }
	for iface in entries {
		if is_virtual_interface(iface) {
			continue
		}
		content := os.read_file('/sys/class/net/${iface}/address') or { '' }
		mac := content.trim_space()
		if mac.len > 0 && mac != '00:00:00:00:00:00' {
			return mac
		}
	}
	return ''
}

/// is_virtual_interface returns true for loopback and container-managed interfaces.
fn is_virtual_interface(name string) bool {
	if name == 'lo' {
		return true
	}
	for prefix in ['docker', 'veth', 'br-'] {
		if name.starts_with(prefix) {
			return true
		}
	}
	return false
}

/// read_mac_from_ifconfig reads MAC from ifconfig output on macOS.
fn read_mac_from_ifconfig() string {
	result := os.execute('ifconfig 2>/dev/null | grep ether | head -1')
	if result.exit_code != 0 || result.output.len == 0 {
		return ''
	}
	parts := result.output.trim_space().split(' ')
	for i, part in parts {
		if part == 'ether' && i + 1 < parts.len {
			mac := parts[i + 1].trim_space()
			if mac.len > 0 && mac != '00:00:00:00:00:00' {
				return mac
			}
		}
	}
	return ''
}

/// read_ip_from_proc reads primary IP by parsing /proc/net/fib_trie.
/// Finds LOCAL addresses excluding 127.x.x.x loopback range.
fn read_ip_from_proc() string {
	content := os.read_file('/proc/net/fib_trie') or { return '' }
	return parse_local_ip_from_fib_trie(content)
}

/// parse_local_ip_from_fib_trie extracts the first non-loopback LOCAL IP
/// from fib_trie content. Looks for IP lines followed by "/32 host LOCAL".
fn parse_local_ip_from_fib_trie(content string) string {
	lines := content.split('\n')
	mut prev_ip := ''
	for line in lines {
		trimmed := line.trim_space()
		if trimmed.starts_with('|-- ') || trimmed.starts_with('+-- ') {
			candidate := trimmed[4..].split(' ')[0]
			if !candidate.contains('/') {
				prev_ip = candidate
			}
		} else if trimmed == '/32 host LOCAL' && prev_ip.len > 0 {
			if !prev_ip.starts_with('127.') {
				return prev_ip
			}
		}
	}
	return ''
}

/// read_ip_from_ipconfig reads primary IP from ipconfig on macOS.
fn read_ip_from_ipconfig() string {
	result := os.execute('ipconfig getifaddr en0 2>/dev/null')
	if result.exit_code != 0 || result.output.len == 0 {
		return ''
	}
	ip := result.output.trim_space()
	if ip.len > 0 && ip != '127.0.0.1' {
		return ip
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
