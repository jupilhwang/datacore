// Interface Layer - CLI Commands
// 인터페이스 레이어 - CLI 명령어
//
// DataCore 브로커 관리 및 운영을 위한 CLI 애플리케이션입니다.
// 브로커 시작/중지, 토픽 관리, 메시지 생산/소비 등의
// 명령어를 제공합니다.
//
// 주요 기능:
// - 브로커 시작, 중지, 상태 확인
// - 토픽 생성, 삭제, 조회
// - 메시지 생산 및 소비
// - 컨슈머 그룹 관리
module cli

import os
import time

/// App은 CLI 애플리케이션을 나타내는 구조체입니다.
pub struct App {
pub:
	name        string = 'datacore'
	version     string = '0.38.0'
	description string = 'Kafka-compatible message broker written in V'
}

/// new_app은 새로운 CLI 애플리케이션을 생성합니다.
pub fn new_app() &App {
	return &App{}
}

/// print_help는 도움말 정보를 출력합니다.
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

/// print_broker_help는 브로커 명령어 도움말을 출력합니다.
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

/// print_version은 버전 정보를 출력합니다.
pub fn (app &App) print_version() {
	println('\x1b[36m${app.name}\x1b[0m version \x1b[32mv${app.version}\x1b[0m')
	println('')
	println('Build Info:')
	println('  Kafka Protocol: 0.8 - 4.1')
	println('  API Versions:   0 - 18 (Admin), 0 - 9 (Produce), 0 - 13 (Fetch)')
	println('  Language:       V (vlang.io)')
	println('  Platform:       ${os.user_os()}/${os.uname().machine}')
}

/// CliOptions는 CLI 옵션을 담는 구조체입니다.
pub struct CliOptions {
pub:
	config_path string = 'config.toml'       // 설정 파일 경로
	pid_path    string = '/tmp/datacore.pid' // PID 파일 경로
	verbose     bool // 상세 출력 활성화
	debug       bool // 디버그 모드 활성화
	daemon      bool // 데몬 모드로 실행
	force       bool // 강제 실행
}

/// get_config_path는 인자에서 설정 파일 경로를 가져옵니다.
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

/// parse_options는 CLI 옵션을 파싱합니다.
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
					debug:  args[i] == '--debug' || args[i] == '-d'
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

/// write_pid는 현재 프로세스 PID를 파일에 씁니다.
pub fn write_pid(path string) ! {
	pid := os.getpid()
	os.write_file(path, '${pid}')!
}

/// read_pid는 파일에서 PID를 읽습니다.
pub fn read_pid(path string) !int {
	if !os.exists(path) {
		return error('PID file not found: ${path}')
	}
	content := os.read_file(path) or { return error('Failed to read PID file: ${err}') }
	return content.trim_space().int()
}

/// remove_pid는 PID 파일을 삭제합니다.
pub fn remove_pid(path string) {
	if os.exists(path) {
		os.rm(path) or {}
	}
}

/// check_pid_running은 해당 PID의 프로세스가 실행 중인지 확인합니다.
pub fn check_pid_running(pid int) bool {
	// 시그널 0을 보내 프로세스 존재 여부 확인
	$if linux || macos {
		result := os.execute('kill -0 ${pid} 2>/dev/null')
		return result.exit_code == 0
	}
	return false
}

/// print_banner는 시작 배너를 출력합니다.
pub fn print_banner(app &App) {
	println('')
	println('\x1b[36m ____        _         ____')
	println('|  _ \\  __ _| |_ __ _ / ___|___  _ __ ___')
	println("| | | |/ _` | __/ _` | |   / _ \\| '__/ _ \\")
	println('| |_| | (_| | || (_| | |__| (_) | | |  __/')
	println('|____/ \\__,_|\\__\\__,_|\\____\\___/|_|  \\___|')
	println('                                          \x1b[0m')
	println('')
	println('\x1b[90mKafka-compatible message broker v${app.version}\x1b[0m')
	println('')
}

/// print_startup_info는 시작 정보를 출력합니다.
pub fn print_startup_info(host string, port int, broker_id int, cluster_id string) {
	println('\x1b[32m✓\x1b[0m Broker Configuration:')
	println('  • Broker ID:  ${broker_id}')
	println('  • Cluster ID: ${cluster_id}')
	println('  • Listen:     ${host}:${port}')
	println('')
}

/// print_shutdown은 종료 메시지를 출력합니다.
pub fn print_shutdown() {
	println('')
	println('\x1b[33m⚡\x1b[0m Shutting down broker...')
}

/// print_shutdown_complete는 종료 완료 메시지를 출력합니다.
pub fn print_shutdown_complete() {
	println('\x1b[32m✓\x1b[0m Broker stopped.')
}

/// BrokerStatus는 브로커 상태 정보를 담는 구조체입니다.
pub struct BrokerStatus {
pub:
	running     bool          // 실행 중 여부
	pid         int           // 프로세스 ID
	uptime      time.Duration // 가동 시간
	host        string        // 호스트
	port        int           // 포트
	broker_id   int           // 브로커 ID
	cluster_id  string        // 클러스터 ID
	topics      int           // 토픽 수
	partitions  int           // 파티션 수
	connections int           // 연결 수
}

/// print_status는 브로커 상태를 출력합니다.
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

// format_duration은 Duration을 읽기 쉬운 형식으로 변환합니다.
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

/// print_progress는 진행 상태 표시기를 출력합니다.
pub fn print_progress(message string) {
	print('\x1b[90m⏳ ${message}...\x1b[0m')
}

/// print_done은 완료 메시지를 출력합니다.
pub fn print_done() {
	println(' \x1b[32m✓\x1b[0m')
}

/// print_failed는 실패 메시지를 출력합니다.
pub fn print_failed(err string) {
	println(' \x1b[31m✗\x1b[0m')
	println('\x1b[31mError:\x1b[0m ${err}')
}

/// confirm은 사용자 확인을 요청합니다.
pub fn confirm(message string) bool {
	print('${message} [y/N]: ')
	response := os.get_line()
	return response.to_lower().starts_with('y')
}
