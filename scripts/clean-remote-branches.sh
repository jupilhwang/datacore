#!/bin/bash
#
# Remote Branch Cleaner Script
# 로컬에서 머지된 원격 브랜치를 정리하는 스크립트
#
# 사용법:
#   ./scripts/clean-remote-branches.sh [--dry-run]
#
# 옵션:
#   --dry-run    삭제할 브랜치만 표시 (실제 삭제 안함)
#   --force      확인 없이 바로 삭제
#
# 주의: 이 스크립트는 GitHub CLI (gh) 또는 git push를 사용합니다.
#

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 옵션 파싱
DRY_RUN=false
FORCE=false
for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=true ;;
        --force) FORCE=true ;;
    esac
done

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  Remote Branch Cleaner${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# Remote 확인
REMOTE=$(git remote | head -1)
REMOTE_URL=$(git remote get-url ${REMOTE})
PUSH_URL=$(git remote get-url --push ${REMOTE} 2>/dev/null || echo "${REMOTE_URL}")

echo -e "Remote: ${GREEN}${REMOTE}${NC}"
echo -e "Remote URL: ${GREEN}${REMOTE_URL}${NC}"
echo -e "Push URL: ${GREEN}${PUSH_URL}${NC}"
echo ""

# GitHub CLI 확인
if command -v gh &> /dev/null; then
    echo -e "${GREEN}GitHub CLI (gh) 사용 가능${NC}"
    USE_GH=true
else
    echo -e "${YELLOW}GitHub CLI (gh) 미설치 - git push 사용${NC}"
    USE_GH=false
fi
echo ""

# 삭제할 브랜치 목록 가져오기
echo -e "${YELLOW}로컬에서 머지된 원격 브랜치 검색 중...${NC}"
echo ""

# Remote 브랜치 목록
REMOTE_BRANCHES=$(git branch -r | sed 's/^[[:space:]]*//' | grep -v HEAD)

# 머지된 브랜치 찾기
MERGED_BRANCHES=()
for branch in $REMOTE_BRANCHES; do
    # origin/ 제거
    branch_name=${branch#origin/}
    
    # main/master 브랜치는 건너뜀
    if [[ "$branch_name" == "main" || "$branch_name" == "master" ]]; then
        continue
    fi
    
    # Dependabot 브랜치는 별도 표시
    if [[ "$branch_name" == dependabot/* ]]; then
        continue
    fi
    
    # 로컬에 해당 브랜치가 없으면 이미 삭제된 것
    if ! git show-ref --verify --quiet "refs/heads/${branch_name}"; then
        # 이미 로컬에서 삭제된 브랜치는 remote에서도 삭제 대상
        if git branch -r --contains "origin/${branch_name}" 2>/dev/null | grep -q "origin/main\|origin/master"; then
            MERGED_BRANCHES+=("$branch_name")
        fi
    fi
done

# 로컬에서 이미 삭제된 브랜치 확인
echo -e "${BLUE}로컬에서 삭제된 원격 브랜치:${NC}"
DELETED_LOCAL_BRANCHES=()
for branch in $REMOTE_BRANCHES; do
    branch_name=${branch#origin/}
    if [[ "$branch_name" == "main" || "$branch_name" == "master" ]]; then
        continue
    fi
    if ! git show-ref --verify --quiet "refs/heads/${branch_name}"; then
        DELETED_LOCAL_BRANCHES+=("$branch_name")
        echo -e "  ${YELLOW}${branch_name}${NC}"
    fi
done
echo ""

# Dependabot 브랜치
echo -e "${BLUE}Dependabot 브랜치:${NC}"
DEPENDABOT_BRANCHES=()
for branch in $REMOTE_BRANCHES; do
    branch_name=${branch#origin/}
    if [[ "$branch_name" == dependabot/* ]]; then
        DEPENDABOT_BRANCHES+=("$branch_name")
        echo -e "  ${YELLOW}${branch_name}${NC}"
    fi
done
echo ""

# 삭제 대상 요약
TOTAL_DELETE=${#DELETED_LOCAL_BRANCHES[@]}

if [ $TOTAL_DELETE -eq 0 ]; then
    echo -e "${GREEN}삭제할 브랜치가 없습니다.${NC}"
    exit 0
fi

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  삭제 대상 브랜치 (${TOTAL_DELETE}개)${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

for branch in "${DELETED_LOCAL_BRANCHES[@]}"; do
    echo -e "  ${RED}${branch}${NC}"
done
echo ""

# Dry run 모드
if [ "$DRY_RUN" = true ]; then
    echo -e "${BLUE}[DRY RUN] 삭제할 브랜치 목록만 표시합니다.${NC}"
    exit 0
fi

# 확인
if [ "$FORCE" = false ]; then
    echo -e "${YELLOW}위 브랜치를 원격에서 삭제하시겠습니까? (y/N)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo -e "${RED}취소되었습니다.${NC}"
        exit 0
    fi
fi

# 브랜치 삭제 실행
echo ""
echo -e "${YELLOW}브랜치 삭제 중...${NC}"
echo ""

DELETED_COUNT=0
FAILED_COUNT=0

for branch in "${DELETED_LOCAL_BRANCHES[@]}"; do
    echo -e "${YELLOW}삭제 중: ${branch}${NC}"
    
    if [ "$USE_GH" = true ]; then
        # GitHub CLI 사용
        REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "")
        if [ -n "$REPO" ]; then
            if gh api "repos/${REPO}/git/refs/heads/${branch}" -X DELETE 2>/dev/null; then
                echo -e "  ${GREEN}삭제 성공 (gh API)${NC}"
                ((DELETED_COUNT++))
            else
                echo -e "  ${RED}삭제 실패 (gh API)${NC}"
                ((FAILED_COUNT++))
            fi
        else
            # gh repo view 실패 시 git push로 시도
            if git push ${REMOTE} --delete ${branch} 2>&1; then
                echo -e "  ${GREEN}삭제 성공 (git push)${NC}"
                ((DELETED_COUNT++))
            else
                echo -e "  ${RED}삭제 실패 (git push)${NC}"
                ((FAILED_COUNT++))
            fi
        fi
    else
        # git push 사용
        if git push ${REMOTE} --delete ${branch} 2>&1; then
            echo -e "  ${GREEN}삭제 성공${NC}"
            ((DELETED_COUNT++))
        else
            echo -e "  ${RED}삭제 실패${NC}"
            ((FAILED_COUNT++))
        fi
    fi
done

echo ""
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  완료${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""
echo -e "삭제 성공: ${GREEN}${DELETED_COUNT}${NC}"
echo -e "삭제 실패: ${RED}${FAILED_COUNT}${NC}"
echo ""

# 실패가 있는 경우 안내
if [ $FAILED_COUNT -gt 0 ]; then
    echo -e "${YELLOW}실패한 브랜치는 권한 문제일 수 있습니다.${NC}"
    echo -e "${YELLOW}GitHub 웹에서 직접 삭제하거나, 관리자에게 문의하세요.${NC}"
    echo ""
    echo -e "GitHub 웹에서 삭제: ${REMOTE_URL}/branches"
fi
