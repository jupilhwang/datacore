#!/usr/bin/env python3
"""
Translate Korean comments in V source files to English.
Uses Claude API for accurate translation.
"""

import os
import re
import sys
import subprocess
import json

def has_korean(text):
    """Check if text contains Korean characters."""
    return bool(re.search(r'[가-힣]', text))

def extract_comment_lines(content):
    """Extract lines that have Korean comments."""
    lines = content.split('\n')
    korean_lines = []
    for i, line in enumerate(lines):
        if has_korean(line):
            korean_lines.append((i, line))
    return korean_lines

def translate_with_claude(text_batch):
    """Use Claude to translate Korean comments to English."""
    import anthropic
    
    client = anthropic.Anthropic()
    
    prompt = f"""You are a technical translator. Translate the Korean comments in these V (vlang) source code lines to English.

Rules:
1. Only translate the Korean text in comments (lines starting with //, ///, or containing // after code)
2. Keep all code, symbols, and non-Korean text exactly as-is
3. Use clear, concise technical English
4. Preserve the comment style (// or ///)
5. Return ONLY the translated lines in the exact same format, one per line
6. Do not add any explanation or extra text
7. Keep line numbers and structure identical

Lines to translate (format: LINE_NUM|ORIGINAL_LINE):
{text_batch}

Return in exact same format: LINE_NUM|TRANSLATED_LINE"""

    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return message.content[0].text

def process_file(filepath):
    """Process a single V file, translating all Korean comments."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if not has_korean(content):
        return 0
    
    lines = content.split('\n')
    korean_line_indices = []
    
    for i, line in enumerate(lines):
        if has_korean(line):
            korean_line_indices.append(i)
    
    if not korean_line_indices:
        return 0
    
    # Process in batches of 50 lines
    batch_size = 50
    total_translated = 0
    
    for batch_start in range(0, len(korean_line_indices), batch_size):
        batch_indices = korean_line_indices[batch_start:batch_start + batch_size]
        
        # Build batch text
        batch_lines = []
        for idx in batch_indices:
            batch_lines.append(f"{idx}|{lines[idx]}")
        batch_text = '\n'.join(batch_lines)
        
        # Translate
        result = translate_with_claude(batch_text)
        
        # Parse results
        for result_line in result.strip().split('\n'):
            if '|' in result_line:
                parts = result_line.split('|', 1)
                if len(parts) == 2:
                    try:
                        line_idx = int(parts[0].strip())
                        translated = parts[1]
                        if line_idx < len(lines):
                            lines[line_idx] = translated
                            total_translated += 1
                    except ValueError:
                        pass
    
    # Write back
    new_content = '\n'.join(lines)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    return total_translated

def main():
    """Main function to process all V files."""
    src_dir = '/Users/jhwang/works/test/datacore/src'
    
    # Get all V files with Korean comments
    result = subprocess.run(
        ['grep', '-rla', '[가-힣]', src_dir, '--include=*.v'],
        capture_output=True, text=True
    )
    
    files = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
    
    # Process by directory order
    order_prefixes = [
        'src/domain/',
        'src/infra/',
        'src/service/',
        'src/interface/',
        'src/config/',
    ]
    
    ordered_files = []
    for prefix in order_prefixes:
        prefix_full = os.path.join('/Users/jhwang/works/test/datacore', prefix)
        matching = [f for f in files if f.startswith(prefix_full) or prefix in f]
        ordered_files.extend(matching)
    
    # Add any remaining files
    for f in files:
        if f not in ordered_files:
            ordered_files.append(f)
    
    total_files = 0
    total_comments = 0
    
    for filepath in ordered_files:
        if not os.path.exists(filepath):
            continue
        print(f"Processing: {filepath.replace('/Users/jhwang/works/test/datacore/', '')}")
        count = process_file(filepath)
        if count > 0:
            total_files += 1
            total_comments += count
            print(f"  -> Translated {count} comments")
    
    print(f"\nSummary:")
    print(f"  Files modified: {total_files}")
    print(f"  Comments translated: {total_comments}")

if __name__ == '__main__':
    main()
