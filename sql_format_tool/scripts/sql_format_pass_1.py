#!/usr/bin/env python3
"""
Unified SQL Formatting Script (Pass 1)
Includes:
- Placeholder protection (Jinja, comments, strings)
- Newline handling for non-function parentheses
- Keyword and comma formatting
- Indentation rules
- Audit file generation
"""

import re
import argparse
from pathlib import Path

PASS1_VERSION = 1
PASS1_COMMENT = f"-- sqlfluff-pass1-version: {PASS1_VERSION}"
AUDIT_ROOT = Path("sql_format_tool/audit_folder")
DBT_PROJECT_DIR = Path("../dbt").resolve()

SNOWFLAKE_FUNCTIONS = {
    "ARRAY_AGG","AVG","CAST","COALESCE","COUNT","DATEADD","DATEDIFF","FIRST_VALUE","LAST_VALUE","LISTAGG","MAX","MIN","ROW_NUMBER",
    "SUM","TO_DATE","TO_TIMESTAMP","NVL","IFF","CASE","DECODE","LEAD","LAG","RANK","DENSE_RANK","NTILE","ABS","CEIL","CEILING","FLOOR",
    "ROUND","TRUNC","EXP","LN","LOG","LOG10","MOD","POWER","SQRT","SIGN","SIN","COS","TAN","ASIN","ACOS","ATAN","ATAN2","COSH","SINH",
    "TANH","GREATEST","LEAST","NULLIF","REGEXP_REPLACE","REGEXP_SUBSTR","SPLIT_PART","SUBSTR","SUBSTRING","TRIM","LTRIM","RTRIM","UPPER",
    "LOWER","INITCAP","REPLACE","REVERSE","CONCAT","CONCAT_WS","LPAD","RPAD","LEFT","RIGHT","POSITION","CHARINDEX","ASCII","CHR","TO_CHAR",
    "TO_NUMBER","TO_VARCHAR","TO_DECIMAL","TO_DOUBLE","TO_BOOLEAN","TO_VARIANT","TO_OBJECT","TO_ARRAY","TRY_CAST","TRY_TO_DATE","TRY_TO_TIMESTAMP"
}

def has_pass1_comment(sql: str) -> bool:
    match = re.search(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*(\d+)", sql)
    return bool(match and int(match.group(1)) >= PASS1_VERSION)

def insert_pass1_comment(sql: str) -> str:
    sql = re.sub(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*\d+\s*", "", sql, count=1)
    return PASS1_COMMENT + "\n" + sql.lstrip()

def is_function_call(sql: str, idx: int) -> bool:
    match = re.search(r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*$", sql[:idx])
    return bool(match and match.group(1).split(".")[-1].upper() in SNOWFLAKE_FUNCTIONS)

def newline_after_non_function_parentheses(sql: str) -> str:
    result = []
    for i, ch in enumerate(sql):
        result.append(ch)
        if ch == '(' and not is_function_call(sql, i):
            result.append("\n")
    return "".join(result)

def newline_around_non_function_closing_parentheses(sql: str) -> str:
    result = []
    for i, ch in enumerate(sql):
        if ch == ')' and not is_function_call(sql, i):
            result.append("\n")
            result.append(ch)
            result.append("\n")
        else:
            result.append(ch)
    return "".join(result)

def extract_placeholders(sql: str):
    patterns = [
        (r"({{.*?}})", "JINJA"),
        (r"({%-?.*?-%})", "JINJA"),
        (r"({#.*?#})", "JINJA_COMMENT"),
        (r"--[^\n]*", "SQL_COMMENT"),
        (r"/\\*.*?\\*/", "SQL_BLOCK_COMMENT"),
        (r"'(?:''|[^'])*'", "SINGLE_QUOTED_STRING"),
        (r'\"(?:[^\"]|\"\")*\"', "DOUBLE_QUOTED_STRING")
    ]

    replacements = {}
    counter = 1
    for pattern, label in patterns:
        for match in re.findall(pattern, sql, re.DOTALL):
            key = f"__PLACEHOLDER_{label}_{counter:04d}__"
            replacements[key] = match
            sql = sql.replace(match, key)
            counter += 1
    return sql, replacements

def restore_placeholders(sql: str, replacements: dict) -> str:
    for placeholder, original in replacements.items():
        sql = sql.replace(placeholder, original)
    return sql

def flatten_sql_whitespace(sql: str, remove_all_spaces=False) -> str:
    sql = re.sub(r"\\s+", " ", sql.replace("\n", " ")).strip()
    return sql.replace(" ", "") if remove_all_spaces else sql

def format_sql_keywords(sql: str) -> str:
    keywords = ["LEFT JOIN","RIGHT JOIN","INNER JOIN","OUTER JOIN","FULL JOIN","SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING","JOIN","UNION","LIMIT","ON","AND","OR","WITH","WHEN","THEN","ELSE","END"]
    major_clauses = {"SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING","LEFT JOIN","RIGHT JOIN","INNER JOIN","OUTER JOIN","FULL JOIN","JOIN"}
    # Avoid matching inside identifiers (underscores allowed in identifiers)
    pattern = r"(?<![A-Za-z0-9_])(?P<kw>{})(?![A-Za-z0-9_])".format("|".join(re.escape(k) for k in keywords))

    def insert_newline(match):
        kw = match.group("kw").upper()
        return ("\n\n" if kw in major_clauses else "\n") + kw
    return re.sub(pattern, insert_newline, sql, flags=re.IGNORECASE)

def format_sql_commas(sql: str) -> str:
    result, stack, select_blocks = [], [], []
    sql = "(" + sql + ")"
    for i, ch in enumerate(sql):
        if ch == '(' and not is_function_call(sql, i):
            stack.append(i)
        elif ch == ')' and stack:
            start = stack.pop()
            if 'select' in sql[start + 1:i].lower():
                select_blocks.append((start, i))
    def in_select_block(idx):
        return any(start < idx < end for start, end in select_blocks)
    buffer, i = "", 0
    while i < len(sql):
        if sql[i:i + 6].lower() == 'select':
            buffer += sql[i:i + 6]
            i += 6
            continue
        ch = sql[i]
        if ch == ',' and in_select_block(i):
            stripped_buffer = buffer.rstrip()
            if not stripped_buffer.endswith('\n'):
                result.append(stripped_buffer)
                result.append("\n, ")
            else:
                result.append(stripped_buffer + ", ")
            buffer = ""
        else:
            buffer += ch
        i += 1
    if buffer.strip():
        result.append(buffer.strip())
    formatted = "".join(result).strip()
    return re.sub(r'\\n{3,}', '\\n\\n', formatted[1:-1] if formatted.startswith('(') else formatted)

def indent_sql(sql: str, indent: str = "    ") -> str:
    lines, depth = sql.splitlines(), 0
    major_clauses = ("SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING","JOIN","LEFT JOIN","RIGHT JOIN","INNER JOIN","OUTER JOIN","FULL JOIN")
    result = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            result.append("")
            continue
        if stripped == ')':
            depth = max(depth - 1, 0)
            result.append(indent * depth + stripped)
            continue
        if stripped == '(':
            result.append(indent * depth + stripped)
            depth += 1
            continue
        if any(stripped.upper().startswith(k) for k in major_clauses):
            depth = max(depth - 1, 0)
            result.append(indent * depth + stripped)
            depth += 1
            continue
        if stripped.upper().startswith(('THEN','ELSE')):
            depth += 1
            result.append(indent * depth + stripped)
            depth = max(depth - 1, 0)
            continue
        if stripped.upper().endswith('CASE'):
            result.append(indent * depth + stripped)
            depth += 1
            continue
        if stripped.upper().startswith('END'):
            result.append(indent * depth + stripped)
            depth = max(depth - 1, 0)
            continue
        result.append(indent * depth + stripped)
    return "\n".join(result)

def write_audit_files(filename: Path, pre_sql: str, post_sql: str, diff_detected: bool, mirror_audit: bool):
    rel_path = filename.relative_to(DBT_PROJECT_DIR) if mirror_audit else filename.name
    audit_base = AUDIT_ROOT / (rel_path.parent if mirror_audit else Path()) / filename.stem
    audit_base.mkdir(parents=True, exist_ok=True)
    (audit_base / f"{filename.stem}_pass1_01_pre_format.sql").write_text(pre_sql, encoding="utf-8")
    (audit_base / f"{filename.stem}_pass1_03_post_format.sql").write_text(post_sql, encoding="utf-8")
    if diff_detected:
        diff_path = audit_base / f"{filename.stem}_pass1_02_diff.txt"
        diff_path.write_text(flatten_sql_whitespace(pre_sql, True) + "\n" + flatten_sql_whitespace(post_sql, True), encoding="utf-8")
        print(f"❌ Structural/textual change detected in {filename}, diff saved to {diff_path}")

def process_sql_file(filename: Path, mirror_audit: bool) -> str:
    raw_sql = filename.read_text(encoding="utf-8")
    if has_pass1_comment(raw_sql):
        print(f"ℹ️ {filename} already formatted, skipping.")
        return "already_formatted"
    flattened_sql, placeholders = extract_placeholders(raw_sql)
    formatted = newline_after_non_function_parentheses(flatten_sql_whitespace(flattened_sql))
    formatted = newline_around_non_function_closing_parentheses(formatted)
    formatted = format_sql_keywords(formatted)
    formatted = format_sql_commas(formatted)
    formatted = indent_sql(formatted)
    restored = restore_placeholders(formatted, placeholders)
    pre_flat = flatten_sql_whitespace(raw_sql, True)
    post_flat = flatten_sql_whitespace(restored, True)
    diff_detected = pre_flat != post_flat
    restored_with_comment = insert_pass1_comment(restored)
    write_audit_files(filename, raw_sql, restored, diff_detected, mirror_audit)
    if diff_detected:
        return "diff_detected"
    filename.write_text(restored_with_comment, encoding="utf-8")
    print(f"✅ Updated {filename} with formatted SQL.")
    return "formatted"

def process_path(path: Path, mirror_audit: bool):
    processed, skipped, diffs = [], [], []
    if path.is_file() and path.suffix == ".sql":
        status = process_sql_file(path, mirror_audit)
        (processed if status == "formatted" else skipped if status == "already_formatted" else diffs).append(path)
    else:
        for file in path.rglob("*.sql"):
            status = process_sql_file(file, mirror_audit)
            (processed if status == "formatted" else skipped if status == "already_formatted" else diffs).append(file)
    print("\n=== Pass 1 Summary ===")
    print(f"Formatted files: {len(processed)}")
    print(f"Skipped files (already formatted): {len(skipped)}")
    print(f"Files with structural diffs: {len(diffs)}")
    summary_path = AUDIT_ROOT / "pass1_summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("=== Pass 1 Summary ===\n")
        f.write(f"Formatted files: {len(processed)}\n")
        for file in processed: f.write(f"  - {file}\n")
        f.write(f"Skipped files: {len(skipped)}\n")
        for file in skipped: f.write(f"  - {file}\n")
        f.write(f"Files with diffs: {len(diffs)}\n")
        for file in diffs: f.write(f"  - {file}\n")

def main():
    parser = argparse.ArgumentParser(description="Format SQL files for Pass 1")
    parser.add_argument("path", help="Path to .sql file or directory")
    parser.add_argument("--no-mirror-audit", action="store_true", help="Disable folder hierarchy mirroring in audit")
    args = parser.parse_args()
    path = Path(args.path).resolve()
    mirror_audit = not args.no_mirror_audit
    if not path.exists():
        print(f"Path does not exist: {path}")
        return
    process_path(path, mirror_audit)
if __name__ == "__main__":
    main()
