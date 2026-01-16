// Interface Layer - CLI Commands
// DataCore CLI for broker management and operations
module cli

import os
import time

// App is the CLI application
pub struct App {
pub:
    name        string = 'datacore'
    version     string = '0.1.0'
    description string = 'Kafka-compatible message broker written in V'
}

// new_app creates a new CLI application
pub fn new_app() &App {
    return &App{}
}

// print_help prints help information
pub fn (app &App) print_help() {
    println('\x1b[36m${app.name}\x1b[0m - ${app.description}')
    println('')
    println('\x1b[33mUsage:\x1b[0m')
    println('  ${app.name} <command> [options]')
    println('')
    println('\x1b[33mCommands:\x1b[0m')
    println('  \x1b[32mbroker start\x1b[0m     Start the broker server')
    println('  \x1b[32mbroker stop\x1b[0m      Stop the broker server')
    println('  \x1b[32mbroker status\x1b[0m    Show broker status')
    println('  \x1b[32mtopic create\x1b[0m     Create a topic')
    println('  \x1b[32mtopic list\x1b[0m       List topics')
    println('  \x1b[32mtopic delete\x1b[0m     Delete a topic')
    println('  \x1b[32mtopic describe\x1b[0m   Describe a topic')
    println('  \x1b[32mproduce\x1b[0m          Produce messages')
    println('  \x1b[32mconsume\x1b[0m          Consume messages')
    println('  \x1b[32mgroup list\x1b[0m       List consumer groups')
    println('  \x1b[32mgroup describe\x1b[0m   Describe a consumer group')
    println('  \x1b[32mversion\x1b[0m          Show version information')
    println('  \x1b[32mhelp\x1b[0m             Show this help')
    println('')
    println('\x1b[33mGlobal Options:\x1b[0m')
    println('  -c, --config   Configuration file path (default: config.toml)')
    println('  -h, --help     Show help for a command')
    println('  -v, --verbose  Enable verbose output')
    println('  -d, --debug    Enable debug mode')
    println('')
    println('\x1b[33mExamples:\x1b[0m')
    println('  ${app.name} broker start')
    println('  ${app.name} broker start -c /etc/datacore/config.toml')
    println('  ${app.name} topic create my-topic --partitions 3 --replication 1')
    println('  ${app.name} produce my-topic --message "Hello World"')
    println('  ${app.name} consume my-topic --group my-group')
}

// print_broker_help prints broker command help
pub fn (app &App) print_broker_help() {
    println('\x1b[33mBroker Commands:\x1b[0m')
    println('')
    println('\x1b[32mstart\x1b[0m  Start the broker server')
    println('  Options:')
    println('    -c, --config    Config file path (default: config.toml)')
    println('    -d, --daemon    Run as daemon (background)')
    println('    -p, --pid       PID file path (default: /tmp/datacore.pid)')
    println('')
    println('\x1b[32mstop\x1b[0m   Stop the broker server')
    println('  Options:')
    println('    -p, --pid       PID file path')
    println('    -f, --force     Force stop (SIGKILL)')
    println('')
    println('\x1b[32mstatus\x1b[0m Show broker status')
    println('  Options:')
    println('    -p, --pid       PID file path')
    println('')
    println('\x1b[33mExamples:\x1b[0m')
    println('  ${app.name} broker start')
    println('  ${app.name} broker start -c /etc/datacore/config.toml -d')
    println('  ${app.name} broker stop')
    println('  ${app.name} broker status')
}

// print_version prints version information
pub fn (app &App) print_version() {
    println('\x1b[36m${app.name}\x1b[0m version \x1b[32mv${app.version}\x1b[0m')
    println('')
    println('Build Info:')
    println('  Kafka Protocol: 0.8 - 4.1')
    println('  API Versions:   0 - 18 (Admin), 0 - 9 (Produce), 0 - 13 (Fetch)')
    println('  Language:       V (vlang.io)')
    println('  Platform:       ${os.user_os()}/${os.uname().machine}')
}

// CliOptions holds CLI options
pub struct CliOptions {
pub:
    config_path string = 'config.toml'
    pid_path    string = '/tmp/datacore.pid'
    verbose     bool
    debug       bool
    daemon      bool
    force       bool
}

// get_config_path gets the config path from arguments
pub fn get_config_path(args []string) string {
    for i, arg in args {
        if arg == '-c' || arg == '--config' {
            if i + 1 < args.len {
                return args[i + 1]
            }
        }
    }
    return 'config.toml'
}

// parse_options parses CLI options
pub fn parse_options(args []string) CliOptions {
    mut opts := CliOptions{}
    
    mut i := 0
    for i < args.len {
        match args[i] {
            '-c', '--config' {
                if i + 1 < args.len {
                    opts = CliOptions{
                        ...opts
                        config_path: args[i + 1]
                    }
                    i += 1
                }
            }
            '-p', '--pid' {
                if i + 1 < args.len {
                    opts = CliOptions{
                        ...opts
                        pid_path: args[i + 1]
                    }
                    i += 1
                }
            }
            '-v', '--verbose' {
                opts = CliOptions{
                    ...opts
                    verbose: true
                }
            }
            '-d', '--debug', '--daemon' {
                opts = CliOptions{
                    ...opts
                    debug: args[i] == '--debug' || args[i] == '-d'
                    daemon: args[i] == '--daemon'
                }
            }
            '-f', '--force' {
                opts = CliOptions{
                    ...opts
                    force: true
                }
            }
            else {}
        }
        i += 1
    }
    
    return opts
}

// ============================================================
// PID File Management
// ============================================================

// write_pid writes the current process PID to file
pub fn write_pid(path string) ! {
    pid := os.getpid()
    os.write_file(path, '${pid}')!
}

// read_pid reads PID from file
pub fn read_pid(path string) !int {
    if !os.exists(path) {
        return error('PID file not found: ${path}')
    }
    content := os.read_file(path) or {
        return error('Failed to read PID file: ${err}')
    }
    return content.trim_space().int()
}

// remove_pid removes the PID file
pub fn remove_pid(path string) {
    if os.exists(path) {
        os.rm(path) or {}
    }
}

// check_pid_running checks if process with PID is running
pub fn check_pid_running(pid int) bool {
    // Check if process exists by sending signal 0
    $if linux || macos {
        result := os.execute('kill -0 ${pid} 2>/dev/null')
        return result.exit_code == 0
    }
    return false
}

// ============================================================
// Banner and UI
// ============================================================

// print_banner prints the startup banner
pub fn print_banner(app &App) {
    println('')
    println('\x1b[36m ____        _         ____')
    println('|  _ \\  __ _| |_ __ _ / ___|___  _ __ ___')
    println('| | | |/ _` | __/ _` | |   / _ \\| \'__/ _ \\')
    println('| |_| | (_| | || (_| | |__| (_) | | |  __/')
    println('|____/ \\__,_|\\__\\__,_|\\____\\___/|_|  \\___|')
    println('                                          \x1b[0m')
    println('')
    println('\x1b[90mKafka-compatible message broker v${app.version}\x1b[0m')
    println('')
}

// print_startup_info prints startup information
pub fn print_startup_info(host string, port int, broker_id int, cluster_id string) {
    println('\x1b[32m✓\x1b[0m Broker Configuration:')
    println('  • Broker ID:  ${broker_id}')
    println('  • Cluster ID: ${cluster_id}')
    println('  • Listen:     ${host}:${port}')
    println('')
}

// print_startup_complete prints startup complete message
pub fn print_startup_complete(host string, port int) {
    println('\x1b[32m✓\x1b[0m Broker started successfully!')
    println('')
    println('\x1b[33mEndpoints:\x1b[0m')
    println('  • Kafka:    ${host}:${port}')
    println('  • Metrics:  ${host}:9093/metrics')
    println('')
    println('\x1b[90mPress Ctrl+C to stop\x1b[0m')
    println('')
}

// print_shutdown prints shutdown message
pub fn print_shutdown() {
    println('')
    println('\x1b[33m⚡\x1b[0m Shutting down broker...')
}

// print_shutdown_complete prints shutdown complete message
pub fn print_shutdown_complete() {
    println('\x1b[32m✓\x1b[0m Broker stopped.')
}

// ============================================================
// Status Display
// ============================================================

// BrokerStatus holds broker status information
pub struct BrokerStatus {
pub:
    running     bool
    pid         int
    uptime      time.Duration
    host        string
    port        int
    broker_id   int
    cluster_id  string
    topics      int
    partitions  int
    connections int
}

// print_status prints broker status
pub fn print_status(status BrokerStatus) {
    if status.running {
        println('\x1b[32m●\x1b[0m Broker is \x1b[32mrunning\x1b[0m')
        println('')
        println('  PID:         ${status.pid}')
        println('  Uptime:      ${format_duration(status.uptime)}')
        println('  Listen:      ${status.host}:${status.port}')
        println('  Broker ID:   ${status.broker_id}')
        println('  Cluster ID:  ${status.cluster_id}')
        println('')
        println('  Topics:      ${status.topics}')
        println('  Partitions:  ${status.partitions}')
        println('  Connections: ${status.connections}')
    } else {
        println('\x1b[31m●\x1b[0m Broker is \x1b[31mnot running\x1b[0m')
    }
}

fn format_duration(d time.Duration) string {
    total_secs := d / time.second
    days := total_secs / 86400
    hours := (total_secs % 86400) / 3600
    mins := (total_secs % 3600) / 60
    secs := total_secs % 60
    
    if days > 0 {
        return '${days}d ${hours}h ${mins}m'
    } else if hours > 0 {
        return '${hours}h ${mins}m ${secs}s'
    } else if mins > 0 {
        return '${mins}m ${secs}s'
    }
    return '${secs}s'
}

// ============================================================
// Progress and Spinner
// ============================================================

// print_progress prints a progress indicator
pub fn print_progress(message string) {
    print('\x1b[90m⏳ ${message}...\x1b[0m')
}

// print_done prints done message
pub fn print_done() {
    println(' \x1b[32m✓\x1b[0m')
}

// print_failed prints failed message
pub fn print_failed(err string) {
    println(' \x1b[31m✗\x1b[0m')
    println('\x1b[31mError:\x1b[0m ${err}')
}

// ============================================================
// Confirmation Prompts
// ============================================================

// confirm asks for user confirmation
pub fn confirm(message string) bool {
    print('${message} [y/N]: ')
    response := os.get_line()
    return response.to_lower().starts_with('y')
}
