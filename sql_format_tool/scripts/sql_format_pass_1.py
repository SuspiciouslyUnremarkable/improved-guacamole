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
import gc
from pathlib import Path
from typing import Tuple, Dict

PASS1_VERSION = 1
PASS1_COMMENT = f"-- sqlfluff-pass1-version: {PASS1_VERSION}"
AUDIT_ROOT = Path("sql_format_tool/audit_folder")
DBT_PROJECT_DIR = Path("dbt").resolve()

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

def pad_commas_spacing(sql: str) -> str:
    """Ensure all commas have exactly one space after them (and no extra spaces before)."""
    # Remove spaces before commas
    sql = re.sub(r'\s*,', ' ,', sql)
    # Ensure one space after commas (unless end of line)
    sql = re.sub(r',\s*', ', ', sql)
    return sql

def normalize_commas_spacing(sql: str) -> str:
    """Ensure all commas have exactly one space after them (and no extra spaces before)."""
    # Ensure one space after commas (unless end of line)
    sql = re.sub(r',\s*', ', ', sql)
    return sql

def find_sql_blocks(sql: str):
    """Identify function call blocks and SELECT blocks in SQL."""
    stack = []
    function_blocks = []
    select_blocks = []

    for i, ch in enumerate(sql):
        if ch == '(':
            if is_function_call(sql, i):
                stack.append((i, True))
            else:
                stack.append((i, False))
        elif ch == ')' and stack:
            start, is_function = stack.pop()
            if is_function:
                function_blocks.append((start, i))
            elif 'select' in sql[start + 1:i].lower():
                select_blocks.append((start, i))

    return function_blocks, select_blocks

def in_block(idx: int, blocks: list[tuple[int, int]]) -> bool:
    """Return True if index is inside or at the boundaries of any block."""
    return any(start <= idx <= end for start, end in blocks)

def is_function_call(sql: str, idx: int) -> bool:
    match = re.search(r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*$", sql[:idx])
    return bool(match and match.group(1).split(".")[-1].upper() in SNOWFLAKE_FUNCTIONS)

def newline_after_non_function_parentheses(sql: str) -> str:
    """Add a newline after '(' unless it is part of a function call."""
    function_blocks, _ = find_sql_blocks(sql)
    result = []
    for i, ch in enumerate(sql):
        result.append(ch)
        if ch == '(' and not in_block(i, function_blocks):
            result.append("\n")
    return "".join(result)

def ensure_comment_newlines(sql: str) -> str:
    """Ensure each '--' comment starts on a new line, even when multiple comments are adjacent."""
    # Replace any occurrence of '--' that is not already at the start of a line with a newline before it
    # Also handle multiple consecutive comments by ensuring only one newline is added
    sql = re.sub(r'(?<!\n)--', '\n--', sql)
    return sql


def newline_around_non_function_closing_parentheses(sql: str) -> str:
    """Add newlines around ')' unless it's inside a function call."""
    function_blocks, _ = find_sql_blocks(sql)
    result = []
    for i, ch in enumerate(sql):
        if ch == ')' and not in_block(i, function_blocks):
            # Ensure newline before and after
            if result and result[-1] != '\n':
                result.append('\n')
            result.append(')')
            result.append('\n')
        else:
            result.append(ch)
    return "".join(result)



def newline_around_cte_closing_parentheses(sql: str) -> str:
    """Ensure there is an extra newline before and after the closing parenthesis of a CTE."""
    result = []
    tokens = sql.splitlines(keepends=True)
    depth = 0
    in_cte = False

    for line in tokens:
        stripped = line.strip().upper()
        # Detect CTE start: WITH name AS (  or  , name AS (
        if re.match(r'^(WITH|,)\s+\w+\s+AS\s*\($', stripped, re.IGNORECASE):
            in_cte = True
            depth = 1
            result.append(line)
            continue
        if in_cte:
            depth += stripped.count('(')
            depth -= stripped.count(')')
            if depth == 0 and ')' in stripped:
                # Insert extra newline before and after closing parenthesis of CTE
                if not result[-1].endswith('\n'):
                    result[-1] = result[-1] + '\n'
                result.append('\n)\n\n')
                in_cte = False
                continue
            result.append(line)
        else:
            result.append(line)
    return ''.join(result)


def normalize_extra_newlines(sql: str) -> str:
    """Replace any sequence of 3 or more newlines with exactly 2 newlines."""
    return re.sub(r'\n{3,}', '\n\n', sql)


def extract_placeholders(sql: str):
    replacements = {}
    placeholder_counter = 1

    # Comment patterns first
    PLACEHOLDER_PATTERNS = [
        (r"--[^\n]*", "SQL_COMMENT"),
        (r"/\*.*?\*/", "SQL_BLOCK_COMMENT"),
        (r"{{.*?}}", "JINJA"),
        (r"{%-?.*?-%}", "JINJA"),
        (r"{#.*?#}", "JINJA_COMMENT"),
        (r"'(?:''|[^'])*'", "SINGLE_QUOTED_STRING"),
        (r'"(?:[^"]|"")*"', "DOUBLE_QUOTED_STRING"),
    ]

    combined_pattern = "|".join(f"({p})" for p, _ in PLACEHOLDER_PATTERNS)
    regex = re.compile(combined_pattern, re.DOTALL)

    def replace_match(match):
        nonlocal placeholder_counter
        idx = match.lastindex - 1
        label = PLACEHOLDER_PATTERNS[idx][1]
        key = f"__PLACEHOLDER_{label}_{placeholder_counter:04d}__"
        replacements[key] = match.group(0)
        placeholder_counter += 1
        return key

    sql_with_placeholders = regex.sub(replace_match, sql)
    return sql_with_placeholders, replacements



def restore_placeholders(sql: str, replacements: Dict[str, str]) -> str:
    """Restore placeholders back to their original content."""
    for key, original in replacements.items():
        sql = sql.replace(key, original)
    return sql

def flatten_sql_whitespace(sql: str, remove_all_spaces=False) -> str:
    sql = re.sub(r"\\s+", " ", sql)
    sql = sql.replace("\n", " ").strip()
    sql = re.sub(r" {2,}", " ", sql)  # Replace two or more spaces with a single space
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
    """Move commas in SELECT column lists to new lines and normalize spacing,
    skipping commas inside function calls."""
    function_blocks, select_blocks = find_sql_blocks(sql)
    result = []
    buffer = ""
    i = 0

    while i < len(sql):
        if sql[i:i + 6].lower() == 'select':
            buffer += sql[i:i + 6]
            i += 6
            continue

        ch = sql[i]
        if ch == ',':
            next_char = sql[i + 1] if i + 1 < len(sql) else ''
            if in_block(i, select_blocks) and not in_block(i, function_blocks):
                stripped_buffer = buffer.rstrip()
                result.append(stripped_buffer)
                result.append("\n, ")
                buffer = ""
            else:
                stripped_buffer = buffer.rstrip()
                result.append(stripped_buffer + ",")
                if next_char != ' ':
                    result.append(" ")
                buffer = ""
        else:
            buffer += ch
        i += 1

    if buffer.strip():
        result.append(buffer.strip())

    formatted = "".join(result).strip()
    return re.sub(r'\n{3,}', '\n\n', formatted)



def indent_sql(sql: str, indent: str = "    ") -> str:
    """Indent SQL based on structure. 
    - `)` must be on its own line.
    - Lines ending with `(` increase depth.
    - If a keyword has been seen in the current block, 
      indent other lines one level deeper.
    """
    lines = sql.splitlines()
    depth = 0
    keyword_count = 0

    # Define major clauses that should reset depth
    major_clauses = (
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY",
        "HAVING", "JOIN", "LEFT JOIN", "RIGHT JOIN",
        "INNER JOIN", "OUTER JOIN", "FULL JOIN"
    )

    result = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            result.append("")
            continue

        # Closing parenthesis on its own line decreases depth
        if stripped == ")":
            depth = max(depth - 1, 0)
            result.append(indent * depth + ")")
            keyword_count = 0  # reset for new block

        # Opening parenthesis when it ends the line increases depth
        elif stripped.endswith("("):
            result.append(indent * depth + stripped)
            depth += 1
            keyword_count = 0  # reset keywords when entering new block

        # Begins with SELECT (start of a block)
        elif stripped.upper().startswith("SELECT"):
            result.append(indent * depth + stripped)
            keyword_count += 1

        # Major clauses at start of line
        elif any(stripped.upper().startswith(k) for k in major_clauses):
            result.append(indent * depth + stripped)
            keyword_count += 1

        # CASE / WHEN / THEN / ELSE / END blocks
        elif stripped.upper().endswith("CASE"):
            depth += 1
            result.append(indent * depth + stripped)
        elif stripped.upper().startswith(("THEN", "ELSE")):
            depth += 2
            result.append(indent * depth + stripped)
            depth = max(depth - 2, 0)
        elif stripped.upper().startswith("END"):
            depth += 1
            result.append(indent * depth + stripped)
            depth = max(depth - 2, 0)

        # Default indentation (uses extra indent if keywords seen)
        else:
            extra_indent = 1 if keyword_count > 0 else 0
            result.append(indent * (depth + extra_indent) + stripped)

    return "\n".join(result)

def write_audit_files(filename: Path, pre_sql: str, post_sql: str, post_sql_with_comment: str,
                      diff_detected: bool, mirror_audit: bool, last_stage_num: int = 1):
    """Write pre/post/diff audit files with numbering aligned to debug output.
       - Diff uses raw pre_sql vs restored (no comment)
       - Post-format uses restored_with_comment
    """
    rel_path = filename.relative_to(DBT_PROJECT_DIR) if mirror_audit else Path(filename.name)
    audit_base = AUDIT_ROOT / (rel_path.parent if mirror_audit else Path()) / filename.stem
    audit_base.mkdir(parents=True, exist_ok=True)

    # 00 = pre-format
    (audit_base / f"{filename.stem}_00_pre_format.sql").write_text(pre_sql, encoding="utf-8")

    stage = last_stage_num

    # Diff (only if difference found) → raw vs restored (no comment)
    if diff_detected:
        diff_path = audit_base / f"{filename.stem}_{stage:02d}_diff.txt"
        diff_path.write_text(
            flatten_sql_whitespace(pre_sql, True) + "\n" +
            flatten_sql_whitespace(post_sql, True),
            encoding="utf-8"
        )
        print(f"❌ Structural/textual change detected in {filename}, diff saved to {diff_path}")
        stage += 1

    # Post-format = restored_with_comment
    (audit_base / f"{filename.stem}_{stage:02d}_post_format.sql").write_text(
        post_sql_with_comment, encoding="utf-8"
    )

def process_sql_file(filename: Path, mirror_audit: bool, debug: bool = False) -> str:
    raw_sql = filename.read_text(encoding="utf-8")
    if has_pass1_comment(raw_sql):
        print(f"ℹ️ {filename} already formatted, skipping.")
        return "already_formatted"

    # Build mirrored path
    rel_path = filename.relative_to(DBT_PROJECT_DIR) if mirror_audit else Path(filename.name)
    audit_base = AUDIT_ROOT / (rel_path.parent if mirror_audit else Path()) / filename.stem
    audit_base.mkdir(parents=True, exist_ok=True)

    stage_counter = 1

    def debug_write(stage_name: str, content: str):
        nonlocal stage_counter
        if debug:
            stage_path = audit_base / f"{filename.stem}_{stage_counter:02d}_{stage_name}.sql"
            stage_path.write_text(content, encoding="utf-8")
            stage_counter += 1

    # Formatting stages
    flattened_sql, placeholders = extract_placeholders(raw_sql)
    debug_write("placeholders", flattened_sql)

    formatted = flatten_sql_whitespace(flattened_sql)
    debug_write("flatten", formatted)

    formatted = pad_commas_spacing(formatted)
    debug_write("commas_padded", formatted)

    formatted = format_sql_keywords(formatted)
    debug_write("keywords", formatted)

    formatted = format_sql_commas(formatted)
    debug_write("commas", formatted)

    formatted = newline_after_non_function_parentheses(formatted)
    debug_write("after_open_paren", formatted)

    formatted = newline_around_non_function_closing_parentheses(formatted)
    debug_write("around_close_paren", formatted)

    formatted = newline_around_cte_closing_parentheses(formatted)
    debug_write("cte_close_paren", formatted)

    formatted = normalize_extra_newlines(formatted)
    debug_write("normalized_newlines", formatted)

    formatted = indent_sql(formatted)
    debug_write("indentation", formatted)

    formatted = normalize_commas_spacing(formatted)
    debug_write("commas_normalized", formatted)

    formatted = restore_placeholders(formatted, placeholders)
    debug_write("placeholders_restored", formatted)
    
    restored = ensure_comment_newlines(formatted)
    debug_write("comment_newlines", restored)

    restored_with_comment = insert_pass1_comment(restored)
    debug_write("with_version_comment", restored_with_comment)

    # Diff check
    pre_flat = flatten_sql_whitespace(raw_sql, True).lower()
    post_flat = flatten_sql_whitespace(restored, True).lower()
    diff_detected = pre_flat != post_flat

    # Always write main audit files, passing stage count
    write_audit_files(
    filename,
    raw_sql,                # pre_sql
    restored,               # post_sql (no comment) for diff
    restored_with_comment,  # post_sql_with_comment for final audit output
    diff_detected,
    mirror_audit,
    last_stage_num=stage_counter
)
    if not debug:
        filename.write_text(restored_with_comment, encoding="utf-8")
        print(f"✅ Updated {filename} with formatted SQL.")
    else:
        print(f"🔍 Debug mode: {filename} not replaced, see audit folder for steps.")

    return "diff_detected" if diff_detected else "formatted"
    

def process_path(path: Path, mirror_audit: bool, debug: bool = False):
    processed, skipped, diffs = [], [], []
    if path.is_file() and path.suffix == ".sql":
        status = process_sql_file(path, mirror_audit, debug)
        (processed if status == "formatted" else skipped if status == "already_formatted" else diffs).append(path)
    else:
        for file in path.rglob("*.sql"):
            status = process_sql_file(file, mirror_audit, debug)
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
    parser.add_argument("--debug", action="store_true", help="Output intermediate audit files for each formatting step")
    args = parser.parse_args()

    path = Path(args.path).resolve()
    mirror_audit = not args.no_mirror_audit
    debug = args.debug

    if not path.exists():
        print(f"Path does not exist: {path}")
        return
    if path.is_file() and path.suffix == ".sql":
        process_sql_file(path, mirror_audit, debug)
    else:
        for file in path.rglob("*.sql"):
            process_sql_file(file, mirror_audit, debug)

    gc.collect()  # Clean up memory after processing


if __name__ == "__main__":
    main()
