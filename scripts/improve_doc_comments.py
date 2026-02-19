#!/usr/bin/env python3
"""
기존 '/// XXX는 관련 데이터를 담는 구조체입니다.' 처럼 일반적인 comment를
위에 있는 '//' 주석을 참조하여 더 구체적으로 개선하는 스크립트.
"""

import os
import re
import sys

stats = {"files_modified": 0, "improved": 0}


def get_pub_kind(line: str) -> str | None:
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


def extract_name_from_line(kind: str, line: str) -> str:
    line = line.strip()
    if kind == "fn":
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


GENERIC_PATTERNS = [
    "관련 데이터를 담는 구조체입니다.",
    "관련 값을 정의하는 열거형입니다.",
    "을 수행합니다.",
    "를 수행합니다.",
    "는 수행합니다.",
    "은 수행합니다.",
]


def is_generic_comment(comment: str) -> bool:
    """일반적인(개선 필요한) comment인지 확인"""
    for pattern in GENERIC_PATTERNS:
        if pattern in comment:
            return True
    return False


def get_regular_comment_before(lines: list, doc_comment_idx: int) -> str:
    """doc comment 바로 위에 있는 // 주석 텍스트를 반환 (있으면)"""
    idx = doc_comment_idx - 1
    while idx >= 0:
        stripped = lines[idx].strip()
        if stripped.startswith("// ") or stripped == "//":
            # // 주석 블록 수집
            comments = []
            j = idx
            while j >= 0 and (lines[j].strip().startswith("// ") or lines[j].strip() == "//"):
                comments.insert(0, lines[j].strip().lstrip("/ ").strip())
                j -= 1
            # 마지막 // 주석만 반환
            return " ".join(c for c in comments if c)
        elif stripped == "" or stripped.startswith("@["):
            idx -= 1
        else:
            break
    return ""


def make_doc_from_comment(kind: str, name: str, comment_text: str) -> str:
    """// 주석 텍스트를 기반으로 /// doc comment 생성"""
    if not comment_text:
        return ""
    
    # 첫 문장만 사용 (너무 길면 잘라냄)
    # 여러 줄이면 첫 줄만
    first_line = comment_text.split(".")[0].strip()
    if len(first_line) > 80:
        first_line = first_line[:80].rsplit(" ", 1)[0]
    
    # 이미 한국어 형식이면 그대로 사용
    if not first_line:
        return ""
    
    # 영어 주석 -> 그냥 활용
    return f"/// {name}은 {first_line}."


def process_file(filepath: str) -> int:
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    improved = 0
    new_lines = list(lines)

    for i, line in enumerate(lines):
        kind = get_pub_kind(line)
        if kind is None:
            continue

        # doc comment가 바로 위에 있는지 확인
        prev_idx = i - 1
        while prev_idx >= 0 and (lines[prev_idx].strip() == "" or lines[prev_idx].strip().startswith("@[")):
            prev_idx -= 1

        if prev_idx < 0:
            continue

        prev_line = lines[prev_idx].strip()
        if not prev_line.startswith("///"):
            continue

        # generic comment인지 확인
        if not is_generic_comment(prev_line):
            continue

        # 위에 있는 // 주석 확인
        comment_above = get_regular_comment_before(lines, prev_idx)
        if not comment_above:
            continue

        name = extract_name_from_line(kind, line)
        if not name:
            continue

        new_doc = make_doc_from_comment(kind, name, comment_above)
        if new_doc and new_doc != prev_line:
            # new_lines에서 해당 줄 교체
            new_lines[prev_idx] = new_doc + "\n"
            improved += 1

    if improved > 0:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

    return improved


def find_v_files(src_dir: str) -> list[str]:
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

    for fpath in v_files:
        try:
            improved = process_file(fpath)
            if improved > 0:
                rel_path = os.path.relpath(fpath, project_root)
                stats["files_modified"] += 1
                stats["improved"] += improved
                print(f"  {rel_path}: {improved}개 개선")
        except Exception as e:
            print(f"오류 ({fpath}): {e}", file=sys.stderr)

    print(f"\n개선된 comment: {stats['improved']}개 ({stats['files_modified']}개 파일)")


if __name__ == "__main__":
    main()
