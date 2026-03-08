#!/bin/bash
#
# Git History Cleaner Script
# config.toml.bak 파일을 Git 히스토리에서 완전히 제거하는 스크립트
#
# 사용법:
#   ./scripts/clean-git-history.sh [파일패턴]
#
# 예시:
#   ./scripts/clean-git-history.sh config.toml.bak
#   ./scripts/clean-git-history.sh "*.bak"
#   ./scripts/clean-git-history.sh "config.toml.bak secrets.env"
#
# 주의: 이 스크립트는 Git 히스토리를 재작성하므로 force push가 필요합니다.
#

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 파일 패턴 (기본값: config.toml.bak)
FILES_TO_REMOVE="${1:-config.toml.bak}"

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  Git History Cleaner${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""
echo -e "제거할 파일 패턴: ${GREEN}${FILES_TO_REMOVE}${NC}"
echo ""

# 현재 브랜치 확인
CURRENT_BRANCH=$(git branch --show-current)
echo -e "현재 브랜치: ${GREEN}${CURRENT_BRANCH}${NC}"

# Remote 확인
REMOTE=$(git remote | head -1)
echo -e "Remote: ${GREEN}${REMOTE}${NC}"

# Remote URL 확인
REMOTE_URL=$(git remote get-url ${REMOTE})
PUSH_URL=$(git remote get-url --push ${REMOTE} 2>/dev/null || echo "${REMOTE_URL}")
echo -e "Remote URL: ${GREEN}${REMOTE_URL}${NC}"
echo -e "Push URL: ${GREEN}${PUSH_URL}${NC}"
echo ""

# SSH URL 확인 및 설정
if [[ "${PUSH_URL}" == *"https://"* ]]; then
    echo -e "${YELLOW}Warning: Push URL이 HTTPS입니다. SSH로 변경합니다.${NC}"
    REPO_PATH=$(echo "${PUSH_URL}" | sed 's|.*github.com[:/]\(.*\)\.git|\1|')
    SSH_URL="git@github.com:${REPO_PATH}.git"
    echo -e "새 Push URL: ${GREEN}${SSH_URL}${NC}"
    git remote set-url --push ${REMOTE} "${SSH_URL}"
fi

# 백업 생성
BACKUP_DIR=".git-backup-$(date +%Y%m%d_%H%M%S)"
echo -e "${YELLOW}Git 백업 생성 중...${NC}"
cp -r .git "${BACKUP_DIR}"
echo -e "백업 위치: ${GREEN}${BACKUP_DIR}${NC}"
echo ""

# 파일 존재 확인
echo -e "${YELLOW}히스토리에서 파일 검색 중...${NC}"
FILE_COUNT=$(git log --all --oneline --name-only | grep -E "^${FILES_TO_REMOVE}$" | wc -l | tr -d ' ')
if [ "${FILE_COUNT}" -eq 0 ]; then
    echo -e "${GREEN}이미 로컬 히스토리에서 제거되었습니다.${NC}"
else
    echo -e "발견된 파일 수: ${GREEN}${FILE_COUNT}${NC}"
    
    # filter-branch 실행
    echo -e "${YELLOW}Git filter-branch 실행 중... (시간이 걸릴 수 있습니다)${NC}"
    FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch --force --index-filter \
        "git rm --cached --ignore-unmatch ${FILES_TO_REMOVE}" \
        --prune-empty --tag-name-filter cat -- --all
    echo -e "${GREEN}filter-branch 완료${NC}"
fi
echo ""

# 정리 작업
echo -e "${YELLOW}참조 정리 중...${NC}"
rm -rf .git/refs/original/
git for-each-ref --format="%(refname)" refs/replace/ 2>/dev/null | xargs -r -n1 git update-ref -d 2>/dev/null || true
git reflog expire --expire=now --all
git gc --prune=now --aggressive
echo -e "${GREEN}정리 완료${NC}"
echo ""

# 검증
echo -e "${YELLOW}검증 중...${NC}"
REMAINING=$(git log --all --oneline --name-only | grep -E "^${FILES_TO_REMOVE}$" | wc -l | tr -d ' ')
if [ "${REMAINING}" -eq 0 ]; then
    echo -e "${GREEN}성공: 로컬 히스토리에서 모든 '${FILES_TO_REMOVE}' 파일이 제거되었습니다.${NC}"
else
    echo -e "${RED}실패: 여전히 ${REMAINING}개의 파일이 히스토리에 남아있습니다.${NC}"
    exit 1
fi
echo ""

# Remote push 안내
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  다음 단계${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""
echo -e "Remote에 변경사항을 적용하려면 force push가 필요합니다:"
echo ""
echo -e "  ${GREEN}# 모든 브랜치 force push${NC}"
echo -e "  git push ${REMOTE} --force --all"
echo ""
echo -e "  ${GREEN}# 현재 브랜치만 force push${NC}"
echo -e "  git push ${REMOTE} --force ${CURRENT_BRANCH}"
echo ""
echo -e "  ${GREEN}# 태그도 업데이트${NC}"
echo -e "  git push ${REMOTE} --force --tags"
echo ""
echo -e "${RED}경고: Force push는 히스토리를 재작성합니다.${NC}"
echo -e "${RED}다른 팀원들과 협업 중인 경우 사전에 알려야 합니다.${NC}"
echo ""
echo -e "백업 복원 방법:"
echo -e "  rm -rf .git && mv ${BACKUP_DIR} .git"
echo ""
