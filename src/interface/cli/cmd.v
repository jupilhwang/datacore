// API Layer - CLI Commands
module cli

// App is the CLI application
pub struct App {
    name        string = 'datacore'
    version     string = '0.1.0'
    description string = 'Kafka-compatible message broker'
}

// new_app creates a new CLI application
pub fn new_app() &App {
    return &App{}
}

// print_help prints help information
pub fn (app &App) print_help() {
    println('${app.name} - ${app.description}')
    println('')
    println('Usage: ${app.name} <command> [options]')
    println('')
    println('Commands:')
    println('  broker start   Start the broker server')
    println('  broker stop    Stop the broker server')
    println('  topic create   Create a topic')
    println('  topic list     List topics')
    println('  topic delete   Delete a topic')
    println('  group list     List consumer groups')
    println('  group describe Describe a consumer group')
    println('  version        Show version information')
    println('  help           Show this help')
    println('')
    println('Options:')
    println('  -c, --config   Configuration file path (default: config.toml)')
    println('  -h, --help     Show help')
    println('  -v, --version  Show version')
}

// print_version prints version information
pub fn (app &App) print_version() {
    println('${app.name} v${app.version}')
    println('Kafka Protocol: 0.8 - 4.1')
    println('API Version: v1')
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

// CliOptions holds CLI options
pub struct CliOptions {
pub:
    config_path string = 'config.toml'
    verbose     bool
    debug       bool
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
            '-v', '--verbose' {
                opts = CliOptions{
                    ...opts
                    verbose: true
                }
            }
            '-d', '--debug' {
                opts = CliOptions{
                    ...opts
                    debug: true
                }
            }
            else {}
        }
        i += 1
    }
    
    return opts
}
