#!/usr/bin/env python3
"""
V 언어 소스 파일에 pub 요소에 대한 doc comment (///) 를 추가하는 스크립트 v2.
이름 패턴을 분석하여 더 구체적인 한국어 설명을 생성합니다.
"""

import os
import re
import sys

stats = {
    "files_modified": 0,
    "comments_added": 0,
    "files_skipped": 0,
}

# 접미사 -> 설명 매핑 (struct)
STRUCT_SUFFIX_MAP = {
    "Config": "설정 정보를 담는 구조체입니다.",
    "Options": "옵션 정보를 담는 구조체입니다.",
    "Request": "요청 데이터를 담는 구조체입니다.",
    "Response": "응답 데이터를 담는 구조체입니다.",
    "Metadata": "메타데이터를 담는 구조체입니다.",
    "Manager": "관리 기능을 제공하는 구조체입니다.",
    "Handler": "요청 처리 기능을 제공하는 구조체입니다.",
    "Server": "서버 기능을 제공하는 구조체입니다.",
    "Client": "클라이언트 기능을 제공하는 구조체입니다.",
    "Pool": "풀 관리 기능을 제공하는 구조체입니다.",
    "Store": "저장소 기능을 제공하는 구조체입니다.",
    "Registry": "레지스트리 기능을 제공하는 구조체입니다.",
    "Encoder": "인코딩 기능을 제공하는 구조체입니다.",
    "Decoder": "디코딩 기능을 제공하는 구조체입니다.",
    "Coordinator": "코디네이터 기능을 제공하는 구조체입니다.",
    "Adapter": "어댑터 기능을 제공하는 구조체입니다.",
    "Metrics": "지표 데이터를 담는 구조체입니다.",
    "Stats": "통계 데이터를 담는 구조체입니다.",
    "Info": "정보를 담는 구조체입니다.",
    "State": "상태 정보를 담는 구조체입니다.",
    "Result": "결과 데이터를 담는 구조체입니다.",
    "Error": "오류 정보를 담는 구조체입니다.",
    "Connection": "연결 정보를 담는 구조체입니다.",
    "Session": "세션 정보를 담는 구조체입니다.",
    "Record": "레코드 데이터를 담는 구조체입니다.",
    "Batch": "배치 데이터를 담는 구조체입니다.",
    "Buffer": "버퍼 기능을 제공하는 구조체입니다.",
    "API": "API 기능을 제공하는 구조체입니다.",
    "Worker": "작업자 기능을 제공하는 구조체입니다.",
    "Guard": "가드 기능을 제공하는 구조체입니다.",
    "Writer": "쓰기 기능을 제공하는 구조체입니다.",
    "Reader": "읽기 기능을 제공하는 구조체입니다.",
    "Exporter": "내보내기 기능을 제공하는 구조체입니다.",
    "Compressor": "압축 기능을 제공하는 구조체입니다.",
    "Service": "서비스 기능을 제공하는 구조체입니다.",
    "Partition": "파티션 정보를 담는 구조체입니다.",
    "Topic": "토픽 정보를 담는 구조체입니다.",
    "Schema": "스키마 정보를 담는 구조체입니다.",
    "Catalog": "카탈로그 기능을 제공하는 구조체입니다.",
    "Snapshot": "스냅샷 정보를 담는 구조체입니다.",
    "Index": "인덱스 정보를 담는 구조체입니다.",
    "Header": "헤더 정보를 담는 구조체입니다.",
    "Frame": "프레임 데이터를 담는 구조체입니다.",
    "Pipeline": "파이프라인 기능을 제공하는 구조체입니다.",
    "Type": "타입 정보를 담는 구조체입니다.",
    "Field": "필드 정보를 담는 구조체입니다.",
    "Column": "컬럼 정보를 담는 구조체입니다.",
    "Row": "행 데이터를 담는 구조체입니다.",
    "Group": "그룹 정보를 담는 구조체입니다.",
    "Assignment": "할당 정보를 담는 구조체입니다.",
    "Binding": "바인딩 정보를 담는 구조체입니다.",
    "Filter": "필터 조건을 담는 구조체입니다.",
    "Pattern": "패턴 정보를 담는 구조체입니다.",
    "Principal": "주체 정보를 담는 구조체입니다.",
    "Credentials": "인증 정보를 담는 구조체입니다.",
    "Election": "선출 정보를 담는 구조체입니다.",
    "Tracer": "추적 기능을 제공하는 구조체입니다.",
    "Span": "스팬 정보를 담는 구조체입니다.",
    "Logger": "로깅 기능을 제공하는 구조체입니다.",
    "App": "애플리케이션 정보를 담는 구조체입니다.",
}

# 접두사 -> 설명 매핑 (fn)
FN_PREFIX_MAP = {
    "new_": "새 인스턴스를 생성합니다.",
    "create_": "새 항목을 생성합니다.",
    "get_": "값을 반환합니다.",
    "set_": "값을 설정합니다.",
    "add_": "항목을 추가합니다.",
    "remove_": "항목을 제거합니다.",
    "delete_": "항목을 삭제합니다.",
    "update_": "항목을 갱신합니다.",
    "start_": "시작합니다.",
    "stop_": "중지합니다.",
    "run_": "실행합니다.",
    "handle_": "처리합니다.",
    "parse_": "파싱합니다.",
    "encode_": "인코딩합니다.",
    "decode_": "디코딩합니다.",
    "compress_": "압축합니다.",
    "decompress_": "압축 해제합니다.",
    "read_": "읽기를 수행합니다.",
    "write_": "쓰기를 수행합니다.",
    "fetch_": "데이터를 가져옵니다.",
    "save_": "저장합니다.",
    "load_": "로드합니다.",
    "init_": "초기화합니다.",
    "reset_": "초기화합니다.",
    "close_": "종료합니다.",
    "open_": "엽니다.",
    "connect_": "연결합니다.",
    "disconnect_": "연결을 종료합니다.",
    "send_": "전송합니다.",
    "receive_": "수신합니다.",
    "check_": "검사합니다.",
    "validate_": "유효성을 검사합니다.",
    "verify_": "검증합니다.",
    "list_": "목록을 반환합니다.",
    "find_": "항목을 검색합니다.",
    "search_": "검색합니다.",
    "filter_": "필터링합니다.",
    "sort_": "정렬합니다.",
    "merge_": "병합합니다.",
    "split_": "분리합니다.",
    "format_": "포맷팅합니다.",
    "print_": "출력합니다.",
    "log_": "로그를 기록합니다.",
    "build_": "빌드합니다.",
    "calculate_": "계산합니다.",
    "compute_": "계산합니다.",
    "process_": "처리합니다.",
    "register_": "등록합니다.",
    "unregister_": "등록 해제합니다.",
    "subscribe_": "구독합니다.",
    "unsubscribe_": "구독을 취소합니다.",
    "publish_": "발행합니다.",
    "apply_": "적용합니다.",
    "describe_": "정보를 반환합니다.",
    "is_": "여부를 반환합니다.",
    "has_": "포함 여부를 반환합니다.",
    "can_": "가능 여부를 반환합니다.",
    "assign_": "할당합니다.",
    "elect_": "선출합니다.",
    "replicate_": "복제합니다.",
    "commit_": "커밋합니다.",
    "abort_": "중단합니다.",
    "flush_": "플러시합니다.",
    "cleanup_": "정리합니다.",
    "shutdown_": "셧다운합니다.",
    "authorize_": "권한을 확인합니다.",
    "authenticate_": "인증합니다.",
    "extract_": "추출합니다.",
    "convert_": "변환합니다.",
    "transform_": "변환합니다.",
    "append_": "추가합니다.",
    "insert_": "삽입합니다.",
    "select_": "선택합니다.",
    "peek_": "첫 번째 항목을 조회합니다.",
    "notify_": "알림을 전송합니다.",
    "broadcast_": "브로드캐스트합니다.",
    "acquire_": "획득합니다.",
    "release_": "반환합니다.",
    "lock_": "잠금을 수행합니다.",
    "unlock_": "잠금을 해제합니다.",
    "bind_": "바인딩합니다.",
    "default_": "기본값을 반환합니다.",
}

# 인터페이스 접미사
INTERFACE_SUFFIX_MAP = {
    "Port": "포트 인터페이스입니다.",
    "Handler": "핸들러 인터페이스입니다.",
    "Storage": "스토리지 인터페이스입니다.",
    "Service": "서비스 인터페이스입니다.",
    "Manager": "관리자 인터페이스입니다.",
    "Store": "저장소 인터페이스입니다.",
    "Client": "클라이언트 인터페이스입니다.",
    "Compressor": "압축기 인터페이스입니다.",
    "Encoder": "인코더 인터페이스입니다.",
    "Decoder": "디코더 인터페이스입니다.",
    "Authenticator": "인증기 인터페이스입니다.",
}

# enum 접미사
ENUM_SUFFIX_MAP = {
    "State": "상태를 나타내는 열거형입니다.",
    "Status": "상태 코드를 나타내는 열거형입니다.",
    "Type": "타입을 나타내는 열거형입니다.",
    "Mode": "모드를 나타내는 열거형입니다.",
    "Operation": "연산을 나타내는 열거형입니다.",
    "Level": "레벨을 나타내는 열거형입니다.",
    "Compression": "압축 유형을 나타내는 열거형입니다.",
    "Algorithm": "알고리즘을 나타내는 열거형입니다.",
    "Mechanism": "메커니즘을 나타내는 열거형입니다.",
    "Error": "오류 코드를 나타내는 열거형입니다.",
    "Protocol": "프로토콜을 나타내는 열거형입니다.",
    "Format": "포맷을 나타내는 열거형입니다.",
}


def generate_smart_comment(kind: str, name: str, line: str) -> str:
    """이름 패턴을 분석하여 구체적인 doc comment 생성"""
    if kind == "struct":
        for suffix, desc in STRUCT_SUFFIX_MAP.items():
            if name.endswith(suffix):
                return f"/// {name}는 {desc}"
        return f"/// {name}는 관련 데이터를 담는 구조체입니다."

    elif kind == "fn":
        for prefix, desc in FN_PREFIX_MAP.items():
            if name.startswith(prefix):
                return f"/// {name}은 {desc}"
        # 접미사 확인
        if name.endswith("_count") or name.endswith("_size") or name.endswith("_len"):
            return f"/// {name}은 개수를 반환합니다."
        if name.endswith("_str") or name == "str":
            return f"/// {name}은 문자열 표현을 반환합니다."
        return f"/// {name}을 수행합니다."

    elif kind == "interface":
        for suffix, desc in INTERFACE_SUFFIX_MAP.items():
            if name.endswith(suffix):
                return f"/// {name}는 {desc}"
        return f"/// {name} 인터페이스입니다."

    elif kind == "const":
        return f"/// {name} 상수입니다."

    elif kind == "type":
        return f"/// {name} 타입입니다."

    elif kind == "enum":
        for suffix, desc in ENUM_SUFFIX_MAP.items():
            if name.endswith(suffix):
                return f"/// {name}는 {desc}"
        return f"/// {name}는 관련 값을 정의하는 열거형입니다."

    return f"/// {name}."


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


def process_file(filepath: str) -> int:
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    added = 0

    while i < len(lines):
        line = lines[i]
        kind = get_pub_kind(line)

        if kind is not None:
            # 이미 doc comment가 있는지 확인
            prev_idx = i - 1
            while prev_idx >= 0 and (lines[prev_idx].strip() == "" or lines[prev_idx].strip().startswith("@[")):
                prev_idx -= 1

            has_doc = False
            if prev_idx >= 0 and lines[prev_idx].strip().startswith("///"):
                has_doc = True

            if not has_doc and new_lines:
                j = len(new_lines) - 1
                while j >= 0 and (new_lines[j].strip() == "" or new_lines[j].strip().startswith("@[")):
                    j -= 1
                if j >= 0 and new_lines[j].strip().startswith("///"):
                    has_doc = True

            if not has_doc:
                name = extract_name_from_line(kind, line)
                if name:
                    comment = generate_smart_comment(kind, name, line)
                    new_lines.append(f"{comment}\n")
                    added += 1

        new_lines.append(line)
        i += 1

    if added > 0:
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

    return added


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
