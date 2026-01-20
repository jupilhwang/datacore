// Adapter Layer - Storage Interface
module storage

import service.port

// PluginInfo contains storage plugin metadata
pub struct PluginInfo {
pub:
	name        string
	version     string
	description string
	author      string
}

// StoragePlugin interface for storage backends
pub interface StoragePlugin {
	// Metadata
	info() PluginInfo

	// Lifecycle
	init(config map[string]string) !
	shutdown() !
	health_check() !port.HealthStatus

	// Get storage adapter
	get_adapter() !&StorageAdapter
}

// StorageAdapter implements port.StoragePort
pub struct StorageAdapter {
	// This is a marker struct - concrete implementations are in plugins/
}
