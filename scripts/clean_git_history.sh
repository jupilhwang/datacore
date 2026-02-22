#!/usr/bin/env bash
# =============================================================================
# Git History Cleanup Script
# .gitignore에 등록된 패턴의 파일들을 Git 히스토리에서 완전히 삭제합니다.
#
# 사용법:
#   ./scripts/clean_git_history.sh [OPTIONS]
#
# 옵션:
#   -h, --help        도움말 출력
#   -n, --dry-run     실제 변경 없이 삭제 대상만 미리보기
#   -y, --yes         확인 프롬프트 없이 자동 실행
#   --no-backup       백업 생성 건너뜀 (권장하지 않음)
#   --gitignore FILE  사용할 .gitignore 파일 경로 지정 (기본값: .gitignore)
#
# 요구사항:
#   - git-filter-repo: pip install git-filter-repo  또는  brew install git-filter-repo
#
# 주의사항:
#   - 실행 후 팀원들은 저장소를 새로 clone 해야 합니다.
#   - force push 이후 기존 fork/clone은 히스토리가 달라집니다.
#   - 협업 중인 저장소는 팀원들과 사전 협의 후 실행하세요.
# =============================================================================

set -Eeuo pipefail
# inherit_errexit: Bash 4.4+에서만 지원 (macOS 기본 Bash 3.2 제외)
if (( BASH_VERSINFO[0] >= 4 && BASH_VERSINFO[1] >= 4 )); then
    shopt -s inherit_errexit
fi

# --- 상수 -------------------------------------------------------------------
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
readonly SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"

# --- 색상 코드 ---------------------------------------------------------------
readonly RED='\033[0;31m'
readonly YELLOW='\033[1;33m'
readonly GREEN='\033[0;32m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m'

# --- 전역 상태 ---------------------------------------------------------------
DRY_RUN=false
AUTO_YES=false
NO_BACKUP=false
GITIGNORE_FILE=".gitignore"
BACKUP_DIR=""
PATHS_FILE=""

# --- 유틸리티 함수 ------------------------------------------------------------

log_info()    { printf "${GREEN}[INFO]${NC}  %s\n" "$1"; }
log_warn()    { printf "${YELLOW}[WARN]${NC}  %s\n" "$1" >&2; }
log_error()   { printf "${RED}[ERROR]${NC} %s\n" "$1" >&2; }
log_step()    { printf "\n${BOLD}${CYAN}>>> %s${NC}\n" "$1"; }
log_detail()  { printf "    %s\n" "$1"; }

cleanup() {
    local exit_code=$?
    if [[ -n "$PATHS_FILE" && -f "$PATHS_FILE" ]]; then
        rm -f -- "$PATHS_FILE"
    fi
    if (( exit_code != 0 )); then
        log_warn "스크립트가 오류로 종료되었습니다 (exit code: $exit_code)."
        if [[ -n "$BACKUP_DIR" && -d "$BACKUP_DIR" ]]; then
            log_info "백업 위치: $BACKUP_DIR"
            log_info "롤백: cd $REPO_ROOT && git fetch ../$BACKUP_DIR '+refs/*:refs/*' --force"
        fi
    fi
}
trap cleanup EXIT

usage() {
    cat <<EOF
사용법: $SCRIPT_NAME [OPTIONS]

.gitignore에 등록된 패턴의 파일들을 Git 히스토리에서 완전히 삭제합니다.

옵션:
  -h, --help          이 도움말을 출력합니다
  -n, --dry-run       실제 변경 없이 삭제 대상만 미리봅니다
  -y, --yes           확인 프롬프트 없이 자동으로 실행합니다
      --no-backup     백업 생성을 건너뜁니다 (권장하지 않음)
      --gitignore F   .gitignore 파일 경로를 지정합니다 (기본값: .gitignore)

예시:
  $SCRIPT_NAME --dry-run                     # 삭제 대상만 확인
  $SCRIPT_NAME                               # 대화형 실행
  $SCRIPT_NAME --yes --no-backup             # 자동 실행 (CI 환경)
  $SCRIPT_NAME --gitignore /path/.gitignore  # 파일 경로 지정

실행 후 원격 저장소 반영:
  git push --force --all origin
  git push --force --tags origin
EOF
    exit 0
}

# --- 인수 파싱 ----------------------------------------------------------------
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h | --help)      usage ;;
            -n | --dry-run)   DRY_RUN=true; shift ;;
            -y | --yes)       AUTO_YES=true; shift ;;
            --no-backup)      NO_BACKUP=true; shift ;;
            --gitignore)
                [[ -z "${2-}" ]] && { log_error "--gitignore 옵션에 파일 경로가 필요합니다."; exit 1; }
                GITIGNORE_FILE="$2"; shift 2 ;;
            --gitignore=*)    GITIGNORE_FILE="${1#*=}"; shift ;;
            *)
                log_error "알 수 없는 옵션: $1"
                printf "  사용법: %s --help\n" "$SCRIPT_NAME" >&2
                exit 1 ;;
        esac
    done
}

# --- 사전 점검 ----------------------------------------------------------------
check_prerequisites() {
    log_step "사전 점검"

    # git-filter-repo 확인
    if ! command -v git-filter-repo &>/dev/null; then
        log_error "git-filter-repo가 설치되어 있지 않습니다."
        log_detail "설치 방법 (하나를 선택):"
        log_detail "  pip install git-filter-repo"
        log_detail "  brew install git-filter-repo"
        exit 1
    fi
    log_info "git-filter-repo: $(git-filter-repo --version 2>/dev/null | head -1 || echo '설치됨')"

    # Git 저장소 확인
    if ! git -C "$REPO_ROOT" rev-parse --git-dir &>/dev/null; then
        log_error "Git 저장소가 아닙니다: $REPO_ROOT"
        exit 1
    fi
    log_info "저장소 경로: $REPO_ROOT"

    # .gitignore 파일 확인
    local gitignore_path
    gitignore_path="$REPO_ROOT/$GITIGNORE_FILE"
    if [[ ! -f "$gitignore_path" ]]; then
        log_error ".gitignore 파일이 없습니다: $gitignore_path"
        exit 1
    fi
    log_info ".gitignore 파일: $gitignore_path"

    # 비커밋 변경사항 확인
    if ! git -C "$REPO_ROOT" diff --quiet HEAD 2>/dev/null ||
       ! git -C "$REPO_ROOT" diff --cached --quiet 2>/dev/null; then
        log_warn "커밋되지 않은 변경사항이 있습니다."
        log_warn "계속 진행하면 스테이징되지 않은 변경사항이 손실될 수 있습니다."
        if [[ "$AUTO_YES" == false ]]; then
            prompt_confirm "커밋되지 않은 변경사항이 있어도 계속하시겠습니까?"
        fi
    fi
}

# --- 삭제 대상 수집 -----------------------------------------------------------
collect_target_paths() {
    local gitignore_path="$REPO_ROOT/$GITIGNORE_FILE"

    PATHS_FILE="$(mktemp)"

    # 현재 HEAD에서 .gitignore 패턴에 해당하는 추적 중인 파일 수집
    # git ls-files -c -i : 추적 중(cached)이면서 gitignore에 매칭되는 파일
    git -C "$REPO_ROOT" ls-files -c -i \
        --exclude-from="$gitignore_path" \
        >"$PATHS_FILE" 2>/dev/null || true

    # 히스토리에만 존재하고 현재는 삭제된 파일도 포함
    # git log --all --diff-filter=D --name-only 로 삭제된 파일 이력 수집
    git -C "$REPO_ROOT" log --all --diff-filter=D --name-only --format='' 2>/dev/null \
        | sort -u \
        | while IFS= read -r deleted_file; do
              [[ -z "$deleted_file" ]] && continue
              # gitignore 패턴과 매칭되는지 검사
              if git -C "$REPO_ROOT" check-ignore -q -- "$deleted_file" 2>/dev/null; then
                  printf '%s\n' "$deleted_file"
              fi
          done >>"$PATHS_FILE" || true

    # 중복 제거 및 정렬
    local sorted_file
    sorted_file="$(mktemp)"
    sort -u "$PATHS_FILE" >"$sorted_file"
    mv -- "$sorted_file" "$PATHS_FILE"
}

# --- 삭제 대상 표시 -----------------------------------------------------------
show_target_paths() {
    local count
    count="$(wc -l <"$PATHS_FILE" | tr -d ' ')"

    log_step "삭제 대상 파일"

    if [[ "$count" -eq 0 ]]; then
        log_info "히스토리에서 삭제할 대상 파일이 없습니다."
        log_info ".gitignore 패턴에 해당하는 추적 파일이 존재하지 않습니다."
        return 1  # 처리 불필요 신호
    fi

    printf "  발견된 파일 수: ${BOLD}%s${NC}\n\n" "$count"

    local display_limit=30
    local displayed=0
    while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        printf "  ${RED}삭제${NC}  %s\n" "$path"
        (( displayed++ ))
        if (( displayed >= display_limit && count > display_limit )); then
            printf "  ... 외 %d개 파일 (전체 목록: %s)\n" \
                "$(( count - display_limit ))" "$PATHS_FILE"
            break
        fi
    done <"$PATHS_FILE"

    return 0
}

# --- 사용자 확인 프롬프트 ------------------------------------------------------
prompt_confirm() {
    local message="${1:-계속 진행하시겠습니까?}"
    local response

    printf "\n${YELLOW}%s${NC} (yes/no): " "$message"
    read -r response </dev/tty

    if [[ "$response" != "yes" ]]; then
        log_info "취소되었습니다."
        exit 0
    fi
}

# --- 백업 생성 ----------------------------------------------------------------
create_backup() {
    if [[ "$NO_BACKUP" == true ]]; then
        log_warn "백업을 건너뜁니다 (--no-backup 옵션)."
        return
    fi

    log_step "백업 생성"

    local timestamp
    timestamp="$(date +%Y%m%d_%H%M%S)"
    BACKUP_DIR="$(dirname -- "$REPO_ROOT")/git_backup_${timestamp}"

    log_info "백업 생성 중: $BACKUP_DIR"
    if git clone --mirror -- "$REPO_ROOT" "$BACKUP_DIR" 2>&1 | sed 's/^/  /'; then
        log_info "백업 완료: $BACKUP_DIR"
        log_detail "롤백 방법:"
        log_detail "  cd $REPO_ROOT"
        log_detail "  git fetch $BACKUP_DIR '+refs/*:refs/*' --force"
    else
        log_warn "백업 생성에 실패했습니다."
        if [[ "$AUTO_YES" == false ]]; then
            prompt_confirm "백업 없이 계속 진행하시겠습니까?"
        fi
    fi
}

# --- git filter-repo 실행 ----------------------------------------------------
run_filter_repo() {
    log_step "Git 히스토리 정리"

    local current_branch
    current_branch="$(git -C "$REPO_ROOT" branch --show-current 2>/dev/null || echo 'detached')"
    log_info "현재 브랜치: $current_branch"
    log_info "삭제 대상 파일 수: $(wc -l <"$PATHS_FILE" | tr -d ' ')개"

    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY-RUN] 실제 변경 없이 종료합니다."
        log_info "실제 실행 명령:"
        log_detail "git filter-repo --invert-paths --paths-from-file \"$PATHS_FILE\" --force"
        return
    fi

    log_info "git filter-repo 실행 중... (저장소 크기에 따라 수 분이 걸릴 수 있습니다)"

    git -C "$REPO_ROOT" filter-repo \
        --invert-paths \
        --paths-from-file "$PATHS_FILE" \
        --force \
        2>&1 | sed 's/^/  /'

    log_info "히스토리 정리 완료."
}

# --- 완료 안내 출력 -----------------------------------------------------------
print_summary() {
    log_step "완료"

    if [[ "$DRY_RUN" == true ]]; then
        printf "${YELLOW}[DRY-RUN] 실제 변경은 수행되지 않았습니다.${NC}\n"
        printf "  실행: %s (--dry-run 없이)\n\n" "$SCRIPT_NAME"
        return
    fi

    printf "${GREEN}히스토리에서 파일 삭제가 완료되었습니다.${NC}\n\n"

    printf "${BOLD}다음 단계 — 원격 저장소에 반영:${NC}\n"
    printf "  ${CYAN}git push --force --all origin${NC}\n"
    printf "  ${CYAN}git push --force --tags origin${NC}\n\n"

    printf "${BOLD}팀원들에게 안내할 사항:${NC}\n"
    printf "  기존 clone은 히스토리가 달라졌으므로 새로 clone 해야 합니다.\n"
    printf "  ${CYAN}git clone <repo-url>${NC}\n\n"

    if [[ -n "$BACKUP_DIR" && -d "$BACKUP_DIR" ]]; then
        printf "${BOLD}백업 위치:${NC}\n"
        printf "  %s\n\n" "$BACKUP_DIR"
        printf "${BOLD}롤백 방법 (필요 시):${NC}\n"
        printf "  ${CYAN}cd %s${NC}\n" "$REPO_ROOT"
        printf "  ${CYAN}git fetch %s '+refs/*:refs/*' --force${NC}\n\n" "$BACKUP_DIR"
    fi
}

# --- 경고 배너 ----------------------------------------------------------------
print_warning_banner() {
    cat <<EOF

${RED}${BOLD}========================================================
  경고: 이 작업은 되돌리기 어렵습니다 (백업 권장)
========================================================${NC}

  - Git 히스토리가 영구적으로 재작성됩니다.
  - 원격 저장소에 force push가 필요합니다.
  - 협업 중이라면 팀원들과 사전에 협의하세요.
  - 팀원들은 이후 저장소를 새로 clone 해야 합니다.

EOF
}

# --- 메인 -------------------------------------------------------------------
main() {
    parse_args "$@"

    printf "${BOLD}=== Git History Cleanup Script ===${NC}\n"
    printf "  저장소: %s\n" "$REPO_ROOT"
    printf "  .gitignore: %s\n" "$GITIGNORE_FILE"
    [[ "$DRY_RUN"   == true ]] && printf "  모드: ${CYAN}DRY-RUN${NC} (변경 없음)\n"
    [[ "$NO_BACKUP" == true ]] && printf "  백업: ${YELLOW}건너뜀${NC}\n"

    check_prerequisites
    collect_target_paths

    if ! show_target_paths; then
        exit 0
    fi

    if [[ "$DRY_RUN" == false ]]; then
        print_warning_banner
        if [[ "$AUTO_YES" == false ]]; then
            prompt_confirm "위 파일들을 Git 히스토리에서 영구 삭제하시겠습니까?"
        fi
        create_backup
    fi

    run_filter_repo
    print_summary
}

main "$@"
