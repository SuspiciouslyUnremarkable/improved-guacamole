import re
import os
import gc

def extract_placeholders(sql: str):
    patterns = [
        (r"({{.*?}})", "JINJA"),
        (r"({%-?.*?-%})", "JINJA"),
        (r"({#.*?#})", "JINJA_COMMENT"),
        (r"--[^\n]*", "SQL_COMMENT"),
        (r"/\*.*?\*/", "SQL_BLOCK_COMMENT")
    ]

    replacements = {}
    counter = 1

    for pattern, label in patterns:
        matches = re.findall(pattern, sql, re.DOTALL)
        for match in matches:
            key = f"__PLACEHOLDER_{label}_{counter:04d}__"
            replacements[key] = match
            sql = sql.replace(match, key)
            counter += 1

    return sql, replacements

def flatten_sql_whitespace(sql: str) -> str:
    sql = sql.replace("\n", " ")
    sql = re.sub(r"\s+", " ", sql)
    return sql.strip()

def format_sql_keywords_pass1(sql: str) -> str:
    keywords = [
        "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "FULL JOIN",
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
        "JOIN", "UNION", "LIMIT", "ON", "AND", "OR", "WITH", "WHEN", "THEN", "ELSE", "END"
    ]

    major_clauses = {
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
        "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "FULL JOIN", "JOIN"
    }

    keywords.sort(key=lambda k: (-k.count(" "), -len(k)))
    pattern = r"(?<!\w)(?P<kw>{})(?!\w)".format("|".join(re.escape(k) for k in keywords))

    def insert_newline(match):
        kw = match.group("kw").upper()
        prefix = "\n\n" if kw in major_clauses else "\n"
        return prefix + match.group("kw")

    return re.sub(pattern, insert_newline, sql, flags=re.IGNORECASE)

def format_sql_commas_pass1(sql: str) -> str:
    stack = []
    select_blocks = []
    sql = "(" + sql + ")"

    for i, char in enumerate(sql):
        if char == '(': stack.append(i)
        elif char == ')' and stack:
            start = stack.pop()
            block = sql[start + 1:i].lower()
            if 'select' in block:
                select_blocks.append((start, i))

    def in_select_block(index):
        return any(start < index < end for start, end in select_blocks)

    result = []
    i = 0
    buffer = ""
    while i < len(sql):
        char = sql[i]
        if sql[i:i+6].lower() == 'select':
            buffer += sql[i:i+6]; i += 6; continue
        if char == ',':
            if in_select_block(i):
                result.append(buffer.strip()); result.append("\n, "); buffer = ""
            else: buffer += char
        elif any(i == start for start, _ in select_blocks): buffer += "("
        elif any(i == end for _, end in select_blocks):
            if buffer.strip(): result.append(buffer.strip())
            result.append("\n\n)\n\n"); buffer = ""
        else: buffer += char
        i += 1
    if buffer.strip(): result.append(buffer.strip())
    final_sql = "".join(result).strip()
    if final_sql.startswith('(') and final_sql.endswith(')'):
        final_sql = final_sql[1:-1]
    final_sql = re.sub(r'\n{3,}', '\n\n', final_sql)
    return final_sql

def indent_sql_by_structure_pass1(sql: str, indent: str = "    ") -> str:
    lines = sql.splitlines()
    result = []
    depth = 0
    major_clauses = (
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
        "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "FULL JOIN"
    )

    for line in lines:
        stripped = line.strip()
        if not stripped:
            result.append("")
            continue

        if stripped == ')':
            depth = max(depth - 2, 0)
            result.append((indent * depth) + stripped)
            continue

        if stripped.endswith("("):
            result.append((indent * depth) + stripped)
            depth += 2
            continue
        
        if any(stripped.upper().startswith(k) for k in major_clauses):
            depth = max(depth - 1, 0)
            result.append((indent * depth) + stripped)
            depth += 1
            continue

        if stripped.upper().startswith("THEN") or stripped.upper().startswith("ELSE"):
            depth += 1
            result.append((indent * depth) + stripped)
            depth = max(depth - 1, 0)
            continue

        if stripped.upper().endswith("CASE"):
            result.append((indent * depth) + stripped)
            depth += 1
            continue

        if stripped.upper().startswith("END"):
            result.append((indent * depth) + stripped)
            depth = max(depth - 1, 0)
            continue
        
        result.append((indent * depth) + stripped)

    
    return "\n".join(result)

def restore_placeholders_pass1(sql: str, replacements: dict) -> str:
    for placeholder, original in replacements.items():
        sql = sql.replace(placeholder, original.strip())
    return sql

def preprocess_and_format_sql_pass1(raw_sql: str, filename: str = None, debug: bool = False, mirror_audit: bool = True) -> str:
    original_flat = flatten_sql_whitespace(raw_sql)
    flattened_sql, replacements = extract_placeholders(raw_sql)
    if filename and debug:
        with open(filename.replace('.sql', '_1_flattened.sql'), 'w', encoding='utf-8') as f:
            f.write(flattened_sql)

    whitespace_flattened_sql = flatten_sql_whitespace(flattened_sql)
    if filename and debug:
        with open(filename.replace('.sql', '_2_whitespace_flattened.sql'), 'w', encoding='utf-8') as f:
            f.write(whitespace_flattened_sql)

    keyword_formatted_sql = format_sql_keywords_pass1(whitespace_flattened_sql)
    if filename and debug:
        with open(filename.replace('.sql', '_3_keywords.sql'), 'w', encoding='utf-8') as f:
            f.write(keyword_formatted_sql)

    comma_formatted_sql = format_sql_commas_pass1(keyword_formatted_sql)
    if filename and debug:
        with open(filename.replace('.sql', '_4_commas.sql'), 'w', encoding='utf-8') as f:
            f.write(comma_formatted_sql)

    indented_sql = indent_sql_by_structure_pass1(comma_formatted_sql)
    if filename and debug:
        with open(filename.replace('.sql', '_5_indents.sql'), 'w', encoding='utf-8') as f:
            f.write(indented_sql)

    restored_sql = restore_placeholders_pass1(indented_sql, replacements)
    if filename and debug:
        with open(filename.replace('.sql', '_6_restored.sql'), 'w', encoding='utf-8') as f:
            f.write(restored_sql)

    gc.collect()

    if filename:
        rel_path = os.path.splitext(os.path.relpath(filename))[0]
        rel_dir, rel_file = os.path.split(rel_path)
        rel_file_base = os.path.splitext(rel_file)[0]
        if mirror_audit:
            audit_base = os.path.join("sql_format_tool", "audit_folder", rel_dir, rel_file_base)
        else:
            audit_base = os.path.join("sql_format_tool", "audit_folder")

        os.makedirs(audit_base, exist_ok=True)

        with open(os.path.join(audit_base, f"{rel_file_base}_pass1_01_pre_format.sql"), "w", encoding="utf-8") as f:
            f.write(raw_sql)

        flattened_pre = flatten_sql_whitespace(raw_sql)
        flattened_post = flatten_sql_whitespace(restored_sql)

        if flattened_pre != flattened_post:
            with open(os.path.join(audit_base, f"{rel_file_base}_pass1_02_diff.txt"), "w", encoding="utf-8") as f:
                f.write(flattened_pre + "\n")
                f.write(flattened_post + "\n")

        with open(os.path.join(audit_base, f"{rel_file_base}_pass1_03_post_format.sql"), "w", encoding="utf-8") as f:
            f.write(restored_sql)


    if filename:
        audit_file = os.path.join("audit", os.path.basename(filename).replace(".sql", "_audit.txt"))
        audit_flattened_comparison(raw_sql, restored_sql, audit_file)
    return restored_sql

def process_file_or_directory(path: str, debug: bool = False, mirror_audit: bool = True):
    if os.path.isfile(path):
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        _ = preprocess_and_format_sql_pass1(content, filename=path, debug=debug, mirror_audit=mirror_audit)
        print(f"Processed file: {path}")

    elif os.path.isdir(path):
        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith(".sql"):
                    full_path = os.path.join(root, file)
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    _ = preprocess_and_format_sql_pass1(content, filename=full_path, debug=debug, mirror_audit=mirror_audit)
                    print(f"Processed file: {full_path}")
    else:
        print(f"Path does not exist: {path}")


import argparse


def audit_flattened_comparison(pre_sql: str, post_sql: str, audit_path: str):
    flattened_pre = flatten_sql_whitespace(pre_sql)
    flattened_post = flatten_sql_whitespace(post_sql)

    if flattened_pre != flattened_post:
        os.makedirs(os.path.dirname(audit_path), exist_ok=True)
        with open(audit_path, 'w', encoding='utf-8') as f:
            f.write(flattened_pre + "\n")
            f.write(flattened_post + "\n")

def main():
    parser = argparse.ArgumentParser(description="Format SQL files with structured indentation.")
    parser.add_argument("path", help="Path to a .sql file or directory containing .sql files")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode with intermediate file writes")
    parser.add_argument("--no-mirror-audit", action="store_true", help="Disable folder hierarchy mirroring for audit output")
    args = parser.parse_args()
    process_file_or_directory(args.path, debug=args.debug, mirror_audit=not args.no_mirror_audit)

if __name__ == "__main__":
    main()
