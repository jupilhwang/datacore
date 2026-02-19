module main
import config as cfg

fn main() {
	conf := cfg.load_config('config-broker1-memory.toml') or {
		eprintln('Failed to load config: ${err}')
		return
	}
	println('Storage engine: ${conf.storage.engine}')
	println('Broker ID: ${conf.broker.broker_id}')
	println('Replication enabled: ${conf.is_replication_enabled()}')
	println('Replication port: ${conf.broker.replication.port}')
}
