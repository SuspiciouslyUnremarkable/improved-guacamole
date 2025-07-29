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


INDENT = "    "  # one indentation level


SNOWFLAKE_FUNCTIONS = {
    "ARRAY_AGG","AVG","CAST","COALESCE","COUNT","DATEADD","DATEDIFF","FIRST_VALUE","LAST_VALUE","LISTAGG","MAX","MIN","ROW_NUMBER",
    "SUM","TO_DATE","TO_TIMESTAMP","NVL","IFF","CASE","DECODE","LEAD","LAG","RANK","DENSE_RANK","NTILE","ABS","CEIL","CEILING","FLOOR",
    "ROUND","TRUNC","EXP","LN","LOG","LOG10","MOD","POWER","SQRT","SIGN","SIN","COS","TAN","ASIN","ACOS","ATAN","ATAN2","COSH","SINH",
    "TANH","GREATEST","LEAST","NULLIF","REGEXP_REPLACE","REGEXP_SUBSTR","SPLIT_PART","SUBSTR","SUBSTRING","TRIM","LTRIM","RTRIM","UPPER",
    "LOWER","INITCAP","REPLACE","REVERSE","CONCAT","CONCAT_WS","LPAD","RPAD","LEFT","RIGHT","POSITION","CHARINDEX","ASCII","CHR","TO_CHAR",
    "TO_NUMBER","TO_VARCHAR","TO_DECIMAL","TO_DOUBLE","TO_BOOLEAN","TO_VARIANT","TO_OBJECT","TO_ARRAY","TRY_CAST","TRY_TO_DATE","TRY_TO_TIMESTAMP"
}


# Major SQL clauses that define boundaries
MAJOR_CLAUSES = (
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
    "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN",
    "FULL JOIN", "UNION", "EXCEPT", "INTERSECT", "LIMIT", "UPDATE"
)


KEYWORDS = [
        "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "FULL JOIN",
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
        "JOIN", "UNION", "LIMIT", "ON", "AND", "OR",
        "WITH", "WHEN", "THEN", "ELSE", "END", "OVER", "VALUES"
]


def has_pass1_comment(sql: str) -> bool:
    match = re.search(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*(\d+)", sql)
    return bool(match and int(match.group(1)) >= PASS1_VERSION)


def insert_pass1_comment(sql: str) -> str:
    sql = re.sub(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*\d+\s*", "", sql, count=1)
    return PASS1_COMMENT + "\n" + sql.lstrip()


# === Placeholder extraction and restoration ===
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


def find_sql_blocks(sql: str):
    """Identify function call blocks and SELECT blocks in SQL."""
    stack = []
    function_blocks = []

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

    return function_blocks


def in_block(idx: int, blocks: list[tuple[int, int]]) -> bool:
    """Return True if index is inside or at the boundaries of any block."""
    return any(start <= idx <= end for start, end in blocks)


def is_function_call(sql: str, idx: int) -> bool:
    match = re.search(r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*$", sql[:idx])
    return bool(match and match.group(1).split(".")[-1].upper() in SNOWFLAKE_FUNCTIONS)


def normalize_operators(sql: str) -> str:
    """
    Normalize spacing around SQL operators and special characters.
    - Adds exactly one space on either side of operators (= + / % < > <= >= <> != ||)
    - Handles semicolons separately (moves to own line)
    - Handles '*' and '-' with custom logic to avoid breaking wildcards and negative numbers
    """

    # --- Multi-character operators first ---
    multi_ops = [r'<>', r'!=', r'<=', r'>=', r'\|\|']
    for op in multi_ops:
        pattern = re.compile(rf'\s*({op})\s*')
        sql = pattern.sub(r' \1 ', sql)

    # --- Single-character operators except * and - ---
    # Protect characters used in multi-ops: <>, !=, <=, >=, ||
    single_ops = r'(?<![<>!])=(?![=])|(?<!<)<(?!>)|(?<!>)>(?!<)|[+/%]'
    sql = re.sub(rf'\s*({single_ops})\s*', r' \1 ', sql)

    # --- Handle '*' separately (avoid breaking table.*) ---
    # Step 1: Normal star spacing (but not table.*)
    sql = re.sub(r'(?<!\.)\s*\*\s*', r' * ', sql)

    # Step 2: Cleanup function-parameter stars back to count(*)
    sql = re.sub(r'\(\s*\*\s*\)', r'(*)', sql)

    # --- Handle '-' separately (avoid negative numbers) ---
    # Only add spacing if '-' is between two non-operator tokens
    # e.g., "a-1" -> "a - 1", but "-1" stays as is
    sql = re.sub(r'(?<=\w)\s*-\s*(?=\w)', r' - ', sql)

    # --- Handle semicolons ---
    sql = re.sub(r'\s*;\s*', r'\n\n;\n\n', sql)

    # --- Collapse triple newlines ---
    sql = re.sub(r'\n{3,}', '\n\n', sql)

    return sql


def format_parentheses(sql: str, debug: bool = False) -> str:
    """
    Adds newlines around parentheses:
    - CTE parentheses -> \n\n prefix/suffix
    - Other non-function parentheses -> \n prefix/suffix
    - Function parentheses -> no additional newlines
    """

    def tokenize(sql_text):
        # Preserve whitespace and symbols separately
        return re.findall(r"\s+|[A-Za-z0-9_]+|\(|\)|.", sql_text)

    def is_cte_context(tokens, index):
        """
        Determine if '(' at tokens[index] is a CTE open:
        Looks for pattern: "<alias> AS ("
        and ensures a WITH exists before in same statement.
        """
        # Find previous non-whitespace token
        j = index - 1
        while j >= 0 and tokens[j].isspace():
            j -= 1
        if j < 1 or tokens[j].upper() != "AS":
            return False
        # Ensure WITH exists earlier in this statement
        return any(tok.upper() == "WITH" for tok in tokens[:j])

    tokens = tokenize(sql)
    output = []
    paren_stack = []

    for i, tok in enumerate(tokens):
        if tok == "(":
            # Look back to detect function calls
            j = i - 1
            while j >= 0 and tokens[j].isspace():
                j -= 1
            prev_token = tokens[j].upper() if j >= 0 else ""

            if prev_token in SNOWFLAKE_FUNCTIONS:
                paren_stack.append("function")
                output.append("(")
                if debug:
                    print(f"[FUNC OPEN] {prev_token}(")
            elif is_cte_context(tokens, i):
                paren_stack.append("cte")
                output.append("(\n\n")
                if debug:
                    print("[CTE OPEN] ( after alias AS")
            else:
                paren_stack.append("group")
                output.append("(\n")
                if debug:
                    print(f"[GROUP OPEN] ( after {prev_token}")
        elif tok == ")":
            context = paren_stack.pop() if paren_stack else "group"
            if context == "function":
                output.append(")")
                if debug:
                    print("[FUNC CLOSE] )")
            elif context == "cte":
                output.append("\n\n)\n\n")
                if debug:
                    print("[CTE CLOSE] )")
            else:
                output.append("\n)")
                if debug:
                    print("[GROUP CLOSE] )")
        else:
            output.append(tok)

    formatted = "".join(output)
    # Normalize extra newlines (3 or more -> 2)
    formatted = re.sub(r"\n{3,}", "\n\n", formatted)
    return formatted


def ensure_comment_newlines(sql: str) -> str:
    """Ensure each '--' comment starts on a new line, even when multiple comments are adjacent."""
    # Replace any occurrence of '--' that is not already at the start of a line with a newline before it
    # Also handle multiple consecutive comments by ensuring only one newline is added
    sql = re.sub(r'(?<!\n)--', '\n--', sql)
    return sql


def flatten_sql_whitespace(sql: str, remove_all_spaces=False) -> str:
    sql = re.sub(r"\\s+", " ", sql)
    sql = sql.replace("\n", " ").strip()
    sql = re.sub(r" {2,}", " ", sql)  # Replace two or more spaces with a single space
    return sql.replace(" ", "") if remove_all_spaces else sql


def format_sql_keywords(sql: str) -> str:
    # Regex pattern to match keywords
    pattern = r"(?<![A-Za-z0-9_])(?P<kw>{})(?![A-Za-z0-9_])".format("|".join(re.escape(k) for k in KEYWORDS))

    def insert_newline(match):
        kw = match.group("kw").upper()
        start_index = match.start()
        # Check for existing newline directly before keyword
        if start_index > 0 and sql[start_index - 1] == "\n":
            return kw
        # Use \r as a placeholder for blank lines if it's a major clause
        if kw in MAJOR_CLAUSES:
            return "\n\n" + kw  # Marker for blank line
        return "\n" + kw

    return re.sub(pattern, insert_newline, sql, flags=re.IGNORECASE)


def finalize_newlines(sql: str) -> str:
    """
    Normalize whitespace and newlines:
    - Replace sequences of 3 or more newlines with exactly 2 newlines.
    - Remove leading and trailing whitespace from each line.
    """
    # Collapse 3+ newlines to 2
    sql = re.sub(r'\n{3,}', '\n\n', sql)

    return sql


def format_sql_commas(sql: str) -> str:
    """
    Ensure commas are formatted correctly:
    - Commas inside function calls remain inline
    - Commas outside function calls are moved to a new line
    """
    function_blocks = find_sql_blocks(sql)
    pattern = re.compile(r'[\s]*,[\s]*')

    def replacement(match):
        comma_pos = match.start() + match.group().find(',')  # actual comma position
        if in_block(comma_pos, function_blocks):
            return ", "  # inside function: keep inline
        else:
            return "\n, "  # outside function: break line
    return pattern.sub(replacement, sql)


def starts_with_major_clause(line: str) -> bool:
    """Detect if a line starts with a major SQL clause keyword."""
    return any(line.upper().startswith(clause) for clause in MAJOR_CLAUSES)


def compute_depth_up_to(lines, index: int) -> int:
    """Count parentheses depth up to and including a given index."""
    depth = 0
    for j in range(index + 1):
        depth += lines[j].count("(")
        depth -= lines[j].count(")")
    return depth


def process_case_block(lines, indents, start_index, debug, depth=0, max_depth=10):
    if depth > max_depth:
        debug(f"[CASE CHILD] Max recursion depth exceeded at {start_index}, skipping CASE.")
        return start_index + 1

    base_indent = indents[start_index]
    debug(f"[CASE CHILD]({depth}) Starting at line {start_index}: {lines[start_index]} (base={base_indent})")

    i = start_index + 1
    while i < len(lines):
        stripped = lines[i].strip()
        upper = stripped.upper()
        if not stripped:
            i += 1
            continue

        # Indentation rules
        if upper.startswith("WHEN"):
            indents[i] = base_indent + 1
            debug(f"  Depth {depth}: WHEN at {i}: -> {base_indent + 1}")
        elif upper.startswith("THEN"):
            indents[i] = base_indent + 2
            debug(f"  Depth {depth}: THEN at {i}: -> {base_indent + 2}")
        elif upper.startswith("ELSE"):
            indents[i] = base_indent + 2
            debug(f"  Depth {depth}: ELSE at {i}: -> {base_indent + 2}")
        elif upper.startswith("END"):
            indents[i] = base_indent + 1
            debug(f"[CASE CHILD] Depth {depth}: END at {i}: -> {base_indent + 1} (returning)")
            return i + 1
        else:
            indents[i] = max(indents[i], base_indent + 2)
            debug(f"  Depth {depth}: Content at {i}: -> {indents[i]}")

        # Detect nested CASE
        if re.match(r"(^CASE$|.*\bCASE\b$)", upper) and not upper.startswith("END"):
            debug(f"  Depth {depth}: Nested CASE at {i} -> recursion")
            i = process_case_block(lines, indents, i, debug, depth + 1, max_depth)
            continue

        i += 1

    debug(f"[CASE CHILD] Depth {depth}: Warning: reached EOF without END for CASE starting at {start_index}")
    return i


def process_parentheses_block(lines, indents, start_index: int, debug):
    """Indent content inside parentheses by +1 until closing paren."""
    depth = 1
    i = start_index + 1
    debug(f"[PARENS CHILD] Starting at line {start_index}: {lines[start_index]}")
    while i < len(lines) and depth > 0:
        stripped = lines[i].strip()
        depth += stripped.count("(")
        depth -= stripped.count(")")
        if depth <= 0:
            debug(f"  Stop: closing parenthesis at line {i}")
            break
        indents[i] += 1
        debug(f"  Line {i}: '{stripped}' inside parens -> indent +1")
        i += 1
    debug(f"[PARENS CHILD] Completed at line {i}")
    return i


def process_major_clause(lines, indents, start_index, debug):
    """
    Indent all lines inside a major clause until:
    - Another top-level major clause at the same depth
    - A semicolon
    - A closing parenthesis that reduces depth below the starting level

    Special handling for SELECT:
    - Non-comma lines inside SELECT are indented by +1.
    (CASE WHEN blocks are handled separately by process_case_block)
    """
    clause_type = lines[start_index].strip().upper().split()[0]
    clause_base_depth = compute_depth_up_to(lines, start_index - 1)

    debug(f"[MAJOR CLAUSE CHILD] Starting at line {start_index}: {lines[start_index]} "
          f"(baseline depth={clause_base_depth})")

    i = start_index + 1
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            continue

        # Depth before and after current line
        depth_before = compute_depth_up_to(lines, i - 1)
        depth_after = depth_before + lines[i].count("(") - lines[i].count(")")
        relative_before = depth_before - clause_base_depth
        relative_after = depth_after - clause_base_depth

        # Stop conditions
        if stripped.startswith(";"):
            debug(f"  Stop: semicolon at line {i}")
            break
        if starts_with_major_clause(stripped) and relative_before == 0:
            debug(f"  Stop: another major clause at same level at line {i}")
            break
        if relative_after < 0:
            debug(f"  Stop: depth decreased beyond baseline at line {i}")
            break

        # --- Indentation rules ---
        extra_select_indent = 0
        if clause_type == "SELECT":
            # Only add +1 if it's not a comma-prefixed column
            if not stripped.startswith(","):
                extra_select_indent = 1

        # Apply final indentation
        indents[i] += 1 + extra_select_indent
        debug(f"  Line {i}: '{stripped}' "
              f"depth_before={depth_before} relative={relative_before} -> indent +{1 + extra_select_indent}")
        i += 1

    debug(f"[MAJOR CLAUSE CHILD] Completed at line {i}")
    return i


def indent_sql_with_children(sql: str, debug_verbose: bool = False) -> str:
    """
    Apply indentation:
      - Parentheses content +1
      - Major clause content +1 (SELECT gets special handling)
      - Nested CASE WHEN rules
    """
    lines = sql.splitlines()
    indents = [0] * len(lines)
    debug = (lambda msg: print(msg)) if debug_verbose else (lambda msg: None)

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        upper = stripped.upper()
        if not stripped:
            i += 1
            continue

        paren_trigger = stripped.endswith("(")
        major_trigger = starts_with_major_clause(stripped)
        case_trigger = upper.endswith("CASE")

        # CASE WHEN child
        if case_trigger:
            debug(f"[MAIN] Line {i}: '{stripped}' -> CASE detected (depth=0) → launching child")
            i = process_case_block(lines, indents, i, debug, depth=0)
            continue
        
        # Parentheses child
        if paren_trigger:
            debug(f"[MAIN] Line {i}: '{stripped}' -> Parentheses detected → launching child")
            process_parentheses_block(lines, indents, i, debug)

        # Major clause child
        if major_trigger:
            debug(f"[MAIN] Line {i}: '{stripped}' -> Major clause detected → launching child")
            process_major_clause(lines, indents, i, debug)

        i += 1

    output = []
    for idx, (line, level) in enumerate(zip(lines, indents)):
        stripped = line.strip()
        if stripped:
            output.append(("    " * level) + stripped)
            debug(f"[OUTPUT] Line {idx}: level={level} text='{stripped}'")
        else:
            output.append("")
            debug(f"[OUTPUT] Line {idx}: blank line")

    return "\n".join(output)


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


def process_sql_file(filename: Path, mirror_audit: bool, debug: bool = False, debug_verbose: bool = False) -> str:
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

    formatted = normalize_operators(formatted)
    debug_write("format_operators", formatted)

    formatted = format_sql_keywords(formatted)
    debug_write("newline_keywords", formatted)

    formatted = format_sql_commas(formatted)
    debug_write("newline_commas", formatted)

    formatted = format_parentheses(formatted)
    debug_write("format_parentheses", formatted)

    formatted = indent_sql_with_children(formatted, debug_verbose)
    debug_write("indentation", formatted)

    formatted = restore_placeholders(formatted, placeholders)
    debug_write("placeholders_restored", formatted)
    
    restored = ensure_comment_newlines(formatted)
    debug_write("newline_comments", restored)

    finalized = finalize_newlines(restored)
    debug_write("finalize_newlines", finalized)

    restored_with_comment = insert_pass1_comment(finalized)
    debug_write("with_version_comment", restored_with_comment)

    # Diff check
    pre_flat = flatten_sql_whitespace(raw_sql, True).lower()
    post_flat = flatten_sql_whitespace(finalized, True).lower()
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
    parser.add_argument("--debug_verbose", action="store_true", help="Enable verbose debug output for indentation and processing steps")
    args = parser.parse_args()

    path = Path(args.path).resolve()
    mirror_audit = not args.no_mirror_audit
    debug = args.debug
    debug_verbose = args.debug_verbose

    if not path.exists():
        print(f"Path does not exist: {path}")
        return
    if path.is_file() and path.suffix == ".sql":
        process_sql_file(path, mirror_audit, debug, debug_verbose)
    else:
        for file in path.rglob("*.sql"):
            process_sql_file(file, mirror_audit, debug, debug_verbose)

    gc.collect()  # Clean up memory after processing


if __name__ == "__main__":
    main()
