// Identity detection unit tests
// Tests for pure helper functions extracted from config_identity.v
module config

fn test_is_virtual_interface_rejects_loopback() {
	assert is_virtual_interface('lo') == true
}

fn test_is_virtual_interface_rejects_docker() {
	assert is_virtual_interface('docker0') == true
	assert is_virtual_interface('docker_gwbridge') == true
}

fn test_is_virtual_interface_rejects_veth() {
	assert is_virtual_interface('veth1234abc') == true
}

fn test_is_virtual_interface_rejects_bridge() {
	assert is_virtual_interface('br-abcdef123456') == true
}

fn test_is_virtual_interface_accepts_physical() {
	assert is_virtual_interface('eth0') == false
	assert is_virtual_interface('ens33') == false
	assert is_virtual_interface('enp0s3') == false
	assert is_virtual_interface('wlan0') == false
	assert is_virtual_interface('en0') == false
}

fn test_parse_fib_trie_extracts_local_ip() {
	content := '
Main:
  +-- 0.0.0.0/0 3 0 5
     +-- 0.0.0.0
        /0 universe UNICAST
     +-- 10.0.2.0/24 2 0 2
        +-- 10.0.2.0
           /32 link BROADCAST
           /24 link UNICAST
        |-- 10.0.2.15
           /32 host LOCAL
        +-- 10.0.2.255
           /32 link BROADCAST
     +-- 127.0.0.0/8 2 0 2
        +-- 127.0.0.0
           /32 link BROADCAST
           /8 host LOCAL
        |-- 127.0.0.1
           /32 host LOCAL
        |-- 127.255.255.255
           /32 link BROADCAST
'
	ip := parse_local_ip_from_fib_trie(content)
	assert ip == '10.0.2.15'
}

fn test_parse_fib_trie_skips_loopback() {
	// Only loopback entries
	content := '
Main:
  +-- 127.0.0.0/8 2 0 2
     +-- 127.0.0.0
        /32 link BROADCAST
        /8 host LOCAL
     |-- 127.0.0.1
        /32 host LOCAL
     |-- 127.255.255.255
        /32 link BROADCAST
'
	ip := parse_local_ip_from_fib_trie(content)
	assert ip == ''
}

fn test_parse_fib_trie_empty_content() {
	assert parse_local_ip_from_fib_trie('') == ''
}

fn test_parse_fib_trie_multiple_interfaces_returns_first() {
	content := '
Main:
  +-- 172.17.0.0/16 2 0 2
     +-- 172.17.0.0
        /32 link BROADCAST
     |-- 172.17.0.2
        /32 host LOCAL
     +-- 172.17.255.255
        /32 link BROADCAST
  +-- 192.168.1.0/24 2 0 2
     |-- 192.168.1.100
        /32 host LOCAL
  +-- 127.0.0.0/8 2 0 2
     |-- 127.0.0.1
        /32 host LOCAL
'
	ip := parse_local_ip_from_fib_trie(content)
	assert ip == '172.17.0.2'
}

fn test_string_to_broker_id_range() {
	id := string_to_broker_id('test-mac-address')
	assert id >= 1
	assert id <= 99999999
}

fn test_string_to_broker_id_deterministic() {
	id1 := string_to_broker_id('aa:bb:cc:dd:ee:ff')
	id2 := string_to_broker_id('aa:bb:cc:dd:ee:ff')
	assert id1 == id2
}

fn test_string_to_broker_id_different_inputs() {
	id1 := string_to_broker_id('aa:bb:cc:dd:ee:ff')
	id2 := string_to_broker_id('11:22:33:44:55:66')
	assert id1 != id2
}
