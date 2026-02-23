#!/usr/bin/env python3
"""
V 언어 소스 파일에 pub 요소에 대한 doc comment (///) 를 추가하는 스크립트.
기존 doc comment가 있으면 유지하고, 없는 경우에만 추가합니다.
"""

import os
import re
import sys
from pathlib import Path

# 파일별 처리 통계
stats = {
    "files_modified": 0,
    "comments_added": 0,
    "files_skipped": 0,
}

def to_korean_name(name: str) -> str:
    """PascalCase 또는 snake_case 이름을 한국어 설명용으로 변환"""
    return name

def generate_doc_comment(kind: str, name: str, line_content: str) -> str:
    """pub 요소 종류와 이름에 따라 적절한 doc comment 생성"""
    clean_name = name.strip()

    if kind == "struct":
        return f"/// {clean_name}는 관련 데이터를 담는 구조체입니다."
    elif kind == "fn":
        # 반환 타입 힌트 확인
        if "!" in line_content:
            return f"/// {clean_name}를 수행합니다."
        elif ") ?" in line_content or ") string" in line_content or ") int" in line_content or ") bool" in line_content:
            return f"/// {clean_name}를 반환합니다."
        else:
            return f"/// {clean_name}를 수행합니다."
    elif kind == "interface":
        return f"/// {clean_name} 인터페이스입니다."
    elif kind == "const":
        return f"/// {clean_name} 상수입니다."
    elif kind == "type":
        return f"/// {clean_name} 타입입니다."
    elif kind == "enum":
        return f"/// {clean_name}는 관련 값을 정의하는 열거형입니다."
    else:
        return f"/// {clean_name}."


def extract_name_from_line(kind: str, line: str) -> str:
    """라인에서 이름 추출"""
    line = line.strip()

    if kind == "fn":
        # pub fn (mut s Server) start() ! -> start
        # pub fn new_server(...) -> new_server
        m = re.search(r'pub fn\s+(?:\([^)]+\)\s+)?(\w+)', line)
        if m:
            return m.group(1)
    elif kind == "struct":
        m = re.search(r'pub struct\s+(\w+)', line)
        if m:
            return m.group(1)
    elif kind == "interface":
        m = re.search(r'pub interface\s+(\w+)', line)
        if m:
            return m.group(1)
    elif kind == "const":
        # pub const (  or  pub const name = ...
        m = re.search(r'pub const\s+(\w+)', line)
        if m:
            return m.group(1)
        return "const"
    elif kind == "type":
        m = re.search(r'pub type\s+(\w+)', line)
        if m:
            return m.group(1)
    elif kind == "enum":
        m = re.search(r'pub enum\s+(\w+)', line)
        if m:
            return m.group(1)

    return ""


def get_pub_kind(line: str) -> str | None:
    """라인이 pub 선언이면 종류 반환"""
    stripped = line.strip()
    if stripped.startswith("pub struct "):
        return "struct"
    elif stripped.startswith("pub fn "):
        return "fn"
    elif stripped.startswith("pub interface "):
        return "interface"
    elif stripped.startswith("pub const "):
        return "const"
    elif stripped.startswith("pub type "):
        return "type"
    elif stripped.startswith("pub enum "):
        return "enum"
    return None


def process_file(filepath: str) -> int:
    """파일을 처리하여 doc comment 추가. 추가된 comment 수 반환"""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    added = 0

    while i < len(lines):
        line = lines[i]
        kind = get_pub_kind(line)

        if kind is not None:
            # 이전 줄들 확인: 이미 /// doc comment가 있는지 확인
            # 바로 앞 줄 (공백/주석 제외)
            prev_idx = i - 1
            # 빈 줄이나 attribute (@[...]) 은 건너뜀
            while prev_idx >= 0 and (lines[prev_idx].strip() == "" or lines[prev_idx].strip().startswith("@[")):
                prev_idx -= 1

            has_doc = False
            if prev_idx >= 0:
                prev_line = lines[prev_idx].strip()
                if prev_line.startswith("///"):
                    has_doc = True

            # 새로 추가된 줄들도 확인 (new_lines의 마지막)
            if not has_doc and new_lines:
                # new_lines 맨 끝에서 역방향으로 /// 찾기
                j = len(new_lines) - 1
                while j >= 0 and (new_lines[j].strip() == "" or new_lines[j].strip().startswith("@[")):
                    j -= 1
                if j >= 0 and new_lines[j].strip().startswith("///"):
                    has_doc = True

            if not has_doc:
                name = extract_name_from_line(kind, line)
                if name:
                    # 현재 줄의 들여쓰기 계산 (보통 pub 요소는 최상위)
                    indent = ""
                    comment = generate_doc_comment(kind, name, line)
                    new_lines.append(f"{indent}{comment}\n")
                    added += 1

        new_lines.append(line)
        i += 1

    if added > 0:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

    return added


def find_v_files(src_dir: str) -> list[str]:
    """src 디렉토리에서 test 파일을 제외한 .v 파일 목록 반환"""
    v_files = []
    for root, dirs, files in os.walk(src_dir):
        for fname in files:
            if fname.endswith(".v") and not fname.endswith("_test.v"):
                v_files.append(os.path.join(root, fname))
    return sorted(v_files)


def main():
    project_root = "/Users/jhwang/works/test/datacore"
    src_dir = os.path.join(project_root, "src")

    v_files = find_v_files(src_dir)
    print(f"처리할 파일 수: {len(v_files)}")

    modified_files = []

    for fpath in v_files:
        try:
            added = process_file(fpath)
            if added > 0:
                rel_path = os.path.relpath(fpath, project_root)
                modified_files.append((rel_path, added))
                stats["files_modified"] += 1
                stats["comments_added"] += added
            else:
                stats["files_skipped"] += 1
        except Exception as e:
            print(f"오류 ({fpath}): {e}", file=sys.stderr)

    print(f"\n=== 결과 ===")
    print(f"수정된 파일 수: {stats['files_modified']}")
    print(f"추가된 doc comment 수: {stats['comments_added']}")
    print(f"변경 없는 파일 수: {stats['files_skipped']}")
    print(f"\n수정된 파일 목록:")
    for fpath, cnt in modified_files:
        print(f"  {fpath}: +{cnt}")


if __name__ == "__main__":
    main()
