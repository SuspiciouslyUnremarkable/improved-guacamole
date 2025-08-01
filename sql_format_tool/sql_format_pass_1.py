import argparse
import json
import re
import os
import subprocess
import shutil
import tempfile
import sqlparse
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, DML
from pathlib import Path
from sqlfluff.core import Linter
from typing import Tuple, Dict, Any, Optional, List

PASS1_VERSION = 1
PASS1_COMMENT = f"-- sqlfluff-pass1-version: {PASS1_VERSION}"
AUDIT_FOLDER = Path("sql_format_tool/audit_folder")


DBT_PROJECT_DIR = Path("dbt").resolve()
SQLFLUFF_CONFIG_PATH = Path("sql_format_tool/resources/.sqlfluff").resolve()



# === PASS 1 TEMPORARY IGNORE RULES FOR DEBUGGING ===
PASS_ONE_DEBUG_EXCLUDE_RULES = [
    # Layout-only rules that might be safe to include after testing
    # "layout.spacing",
    # "layout.long_lines",
    # "convention.select_trailing_comma",
    # "references.special_chars",
    # "capitalisation.keywords",
    # "capitalisation.functions",
    # "layout.indent",
    # "layout.newlines",
    # "layout.end_of_file",
    # "layout.functions",
    # "layout.cte_bracket",
    # "layout.set_operators",
    # "layout.keyword_newline",
]
# === END PASS 1 TEMPORARY IGNORE RULES FOR DEBUGGING ===

# === PASS 1 STRUCTURAL RULE EXCLUSIONS ===
PASS_ONE_EXCLUDE_RULES = [
    # "ambiguous.column_count", "ambiguous.distinct", "ambiguous.join", "ambiguous.column_references", "ambiguous.set_columns", "ambiguous.join_condition",
    # "aliasing.length", "aliasing.unique.column", "aliasing.self_alias.column", "aliasing.table",
    # "convention.not_equal", "convention.coalesce", "convention.is_null", "convention.statement_brackets", "convention.left_join", "convention.casting_style", "convention.join_condition",
    # "references.from", "references.qualification", "references.keywords", "references.consistent",
    # "structure.simple_case", "structure.unused_cte", "structure.nested_case", "structure.subquery", "structure.using", "structure.distinct", "structure.join_condition_order", "structure.constant_expression", "structure.unused_join", "structure.column_order",
] + PASS_ONE_DEBUG_EXCLUDE_RULES
# === END PASS 1 STRUCTURAL RULE EXCLUSIONS ===


CLEANUP_RULES = ["LT01", "LT02", "LT03", "LT05", "CP01", "CP02"]

NEWLINE_RULES = [
    "layout.cte_newline",
    "layout.cte_bracket",
    "layout.keyword_newline",
    "layout.set_operators",
    "layout.newlines"
]


# Keywords typically formatted to start a new line in SQL formatting

STRUCTURAL_KEYWORDS = [
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "HAVING",
    "ORDER BY",
    "QUALIFY",
    "LIMIT",
    "WITH",
    "UNION",
    "UNION ALL",
    "EXCEPT",
    "MINUS",
    "INTERSECT",
]

JOIN_KEYWORDS = [
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "LEFT OUTER JOIN",
    "RIGHT JOIN",
    "RIGHT OUTER JOIN",
    "FULL JOIN",
    "FULL OUTER JOIN",
    "CROSS JOIN",
    "ON",
    "USING",
]

CASE_KEYWORDS = [
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
]

DML_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "VALUES",
]

WINDOW_KEYWORDS = [
    "OVER",
    "PARTITION BY",
]

# Combined list of all keywords
NEWLINE_KEYWORDS = (
    STRUCTURAL_KEYWORDS
    + JOIN_KEYWORDS
    + CASE_KEYWORDS
    + DML_KEYWORDS
    + WINDOW_KEYWORDS
)


# Sort so multi-word keywords (longer ones) come first
NEWLINE_KEYWORDS.sort(key=lambda k: (-len(k.split()), k))


# List of major clauses to add an extra newline before
MAJOR_CLAUSES = [
    "FROM", "WHERE", "GROUP BY", "ORDER BY",
    "HAVING", "LIMIT", "UNION",
    "JOIN", "INNER JOIN",
    "LEFT JOIN", "LEFT OUTER JOIN",
    "RIGHT JOIN", "RIGHT OUTER JOIN",
    "FULL JOIN", "FULL OUTER JOIN",
    "CROSS JOIN"
]


def add_blank_line_before_major_clauses(sql: str, debug_verbose: bool = False) -> str:
    """
    Ensures exactly one blank line (two newlines) before any major clause
    that begins a line. Preserves indentation before the clause.
    """
    # Sort to handle multi-word clauses like "LEFT JOIN" before "JOIN"
    clauses_sorted = sorted(MAJOR_CLAUSES, key=len, reverse=True)

    # Pattern:
    #   ^(\s*) captures any leading indentation
    #   (clause) matches one of our clauses (case-insensitive)
    pattern = re.compile(
        r'(\n+)(\s*)(' + '|'.join(re.escape(c) for c in clauses_sorted) + r')\b',
        flags=re.IGNORECASE
    )

    def replacer(match):
        newlines = match.group(1)
        indent = match.group(2)
        clause = match.group(3)
        # Enforce exactly two newlines, preserve indentation & clause
        if debug_verbose:
            print(f"[DEBUG] Found clause: '{clause}' with indent '{indent}'")
        return "\n\n" + indent + clause

    return pattern.sub(replacer, sql)



def insert_newlines_before_keywords(sql: str) -> str:
    # Parse SQL into statement tokens
    statements = sqlparse.parse(sql)
    updated_sql = []

    for stmt in statements:
        tokens = list(stmt.flatten())  # flatten handles nested groups
        result = []
        for i, token in enumerate(tokens):
            text = token.value.upper()
            # Check if this token (or token + next) is in NEWLINE_KEYWORDS
            if text in NEWLINE_KEYWORDS:
                # Add newline unless previous token already had one
                if result and not result[-1].endswith("\n"):
                    result.append("\n")
                result.append(token.value)
            elif i < len(tokens)-1 and f"{text} {tokens[i+1].value.upper()}" in NEWLINE_KEYWORDS:
                # Handle multiword (LEFT JOIN)
                if result and not result[-1].endswith("\n"):
                    result.append("\n")
                result.append(token.value + " " + tokens[i+1].value)
                # Skip next token as it's part of the keyword
                tokens[i+1] = sqlparse.sql.Token(Keyword, "")
            else:
                result.append(token.value)
        updated_sql.append("".join(result))

    return "\n".join(updated_sql)



def remove_extra_spaces(sql: str) -> str:
    """
    Replace multiple spaces with a single space, without affecting newlines.
    """
    # Replace sequences of 2+ spaces (not including newlines) with a single space
    return re.sub(r' {2,}', ' ', sql)



# ------------------ Pass1 version comment handling ------------------
def has_pass1_comment(sql: str) -> bool:
    match = re.search(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*(\d+)", sql)
    return bool(match and int(match.group(1)) >= PASS1_VERSION)



def remove_pass1_comment(sql: str) -> str:
    match = re.search(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*\d+\s*\n?", sql)
    if match:
        sql = sql[:match.start()] + sql[match.end():]
    return sql.lstrip()



def insert_pass1_comment(sql: str) -> str:
    sql = re.sub(r"(?im)^\s*--\s*sqlfluff-pass1-version:\s*\d+\s*", "", sql, count=1)
    return PASS1_COMMENT + "\n" + sql.lstrip()



# ------------------ Comment Handling ------------------
def convert_single_to_block_comments(sql: str) -> str:
    """
    Convert `-- comment` to `/* comment */`.
    """
    return re.sub(r"--(.*)$", lambda m: f"/*{m.group(1).strip()}*/", sql, flags=re.M)



def move_inline_block_comments(sql: str) -> str:
    """
    Move inline block comments (/* ... */) to the line before their original position.
    e.g.:
        SELECT col1 /* comment */ FROM table;
    becomes:
        /* comment */
        SELECT col1 FROM table;
    """
    lines = sql.splitlines()
    output = []

    for line in lines:
        match = re.search(r"(.*?)(/\*.*?\*/)(.*)", line)
        if match:
            before, comment, after = match.groups()
            before, after = before.rstrip(), after.strip()

            # Case 1: comment at end of line (common case)
            if before and not after:
                output.append(comment)   # move comment up
                output.append(before)
            # Case 2: comment in middle of the line
            elif before and after:
                output.append(comment)   # move comment up
                output.append(f"{before} {after}")
            # Case 3: comment already at start
            elif not before and after:
                output.append(comment)
                output.append(after)
            else:
                output.append(comment)
        else:
            output.append(line)

    return "\n".join(output)



# ------------------ Placeholder extraction and restoration------------------
def extract_placeholders(sql: str) -> Tuple[str, Dict[str, str]]:
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



# ------------------ sqlfluff ------------------
def validate_sqlfluff_config(sqlfluff_config_path: Path, sample_file: Path) -> None:
    print(f"\n=== Checking sqlfluff configuration from: {sqlfluff_config_path} ===")

    # Show version
    version_proc = subprocess.run(
        ["sqlfluff", "version"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    print(f"[INFO] sqlfluff version: {version_proc.stdout.strip()}")

    # List rules
    rules_proc = subprocess.run(
        ["sqlfluff", "rules"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    print("--- Available Rules ---")
    print(rules_proc.stdout or rules_proc.stderr)

    # Run lint in verbose mode to confirm config file in use
    if sample_file.exists():
        lint_proc = subprocess.run(
            [
                "sqlfluff", "lint", str(sample_file),
                "--dialect", "snowflake",
                "--verbose",
                "--config", str(sqlfluff_config_path)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print("--- Verbose Lint Output (look for 'Using config file') ---")
        print(lint_proc.stdout or lint_proc.stderr)
    else:
        print("[WARNING] No sample file provided, skipping lint check.")


def run_sqlfluff_lint(sql_file_path: Path, rules: Optional[List[str]] = None, debug_verbose = False) -> Tuple[int, dict]:
    """
    Run sqlfluff lint on a SQL file.
    Returns: (returncode, lint_results_dict)
    """
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file does not exist: {sql_file_path}")
    
    sql_file_path_posix = sql_file_path.resolve().as_posix()
    sqlfluff_config_path_posix = SQLFLUFF_CONFIG_PATH.resolve().as_posix()

    # Base command
    cmd = ["sqlfluff", "lint", sql_file_path_posix, "--format", "json"]

    if rules is None:
        cmd += ["--config", sqlfluff_config_path_posix,
                "--exclude-rules", ",".join(PASS_ONE_EXCLUDE_RULES)]
    else:
        cmd += ["--rules", ",".join(rules),
                "--dialect", "snowflake",
                "--config", sqlfluff_config_path_posix]


    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode not in (0, 1, 65):
        raise RuntimeError(f"sqlfluff lint failed ({result.returncode}): {result.stderr}")

    try:
        lint_results = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse sqlfluff lint output: {e}\nOutput:\n{result.stdout}")

    if rules and debug_verbose:
        print(result.stdout)
        print(result.stderr)

    return result.returncode, lint_results


def run_sqlfluff_fix(sql_file_path: Path, rules: Optional[List[str]] = None, debug_verbose: bool = False) -> Tuple[int, str]:
    """
    Run sqlfluff fix on a SQL file.
    Returns: (returncode, fixed_sql_string)
    """
    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file does not exist: {sql_file_path}")

    sql_file_path_posix = sql_file_path.resolve().as_posix()
    sqlfluff_config_path_posix = SQLFLUFF_CONFIG_PATH.resolve().as_posix()

    # Base command
    cmd = ["sqlfluff", "fix", sql_file_path_posix]

    if rules is None:
        cmd += ["--config", sqlfluff_config_path_posix,
                "--exclude-rules", ",".join(PASS_ONE_EXCLUDE_RULES)]
    else:
        cmd += ["--rules", ",".join(rules),
                "--dialect", "snowflake",
                "--config", sqlfluff_config_path_posix]


    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode not in (0, 1):
        raise RuntimeError(f"sqlfluff fix failed ({result.returncode}): {result.stderr}")

    # Read fixed SQL back from file
    with open(sql_file_path, "r", encoding="utf-8") as f:
        fixed_sql = f.read()

    if rules and debug_verbose:
        print(result.stdout)
        print(result.stderr)

    return result.returncode, fixed_sql



def extract_unfixable_violations(lint_results: list) -> list[dict]:
    """
    Extract violations from SQLFluff lint JSON where no auto-fixes are available.
    
    Parameters:
        lint_results (list): Parsed JSON list from sqlfluff lint.
    
    Returns:
        list[dict]: A list of dicts with start_line_no, code, description.
    """
    unfixable = []

    for file_result in lint_results:
        violations = file_result.get("violations", [])
        for v in violations:
            # Check if fixes is empty (i.e., no automatic fix available)
            if not v.get("fixes"):
                unfixable.append({
                    "start_line_no": v.get("start_line_no"),
                    "code": v.get("code"),
                    "description": v.get("description")
                })

    return unfixable



def add_noqa_comments(sql: str, unfixable: list[dict]) -> str:
    """
    Append noqa comments to lines with unfixable lint violations.
    
    Parameters:
        sql (str): The SQL content.
        unfixable (list): List of dicts with 'start_line_no', 'code', 'description'.
    
    Returns:
        str: Modified SQL with noqa comments added.
    """
    lines = sql.splitlines()

    for violation in unfixable:
        line_no = violation["start_line_no"] - 1  # SQLFluff line numbers are 1-based
        code = violation["code"]
        description = violation["description"]

        if 0 <= line_no < len(lines):
            # Add noqa comment only if not already present
            if f"-- noqa: {code}" not in lines[line_no]:
                lines[line_no] += f"  -- noqa: {code} -- {description}"

    return "\n".join(lines)



# ------------------ Write JSON ------------------
def write_json_pretty(data, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)



def process_sql_file(filepath: Path, debug: bool = False, debug_verbose: bool = False):
    if not filepath.exists() or filepath.suffix.lower() != ".sql":
        print(f"Skipping invalid file: {filepath}")
        return

    temp_files_to_delete = []

    audit_path = AUDIT_FOLDER / filepath.stem
    if audit_path.exists():
        shutil.rmtree(audit_path)
    audit_path.mkdir(parents=True, exist_ok=True)

    stage = 1  # <-- stage counter for automated audit filenames


    # --- Stage 1: Pre-processing ---
    original_sql = filepath.read_text(encoding="utf-8")


    # Converts '--' to '/**/' comments to prevent newline changes causing improper commenting
    sql = convert_single_to_block_comments(original_sql)
    if debug:
        (audit_path / f"{stage:02d}_convert_comments.sql").write_text(sql, encoding="utf-8")
        stage += 1
    

    # Moves any comments from in the within a line to a new line above
    sql = move_inline_block_comments(sql)
    if debug:
        (audit_path / f"{stage:02d}_preprocess.sql").write_text(sql, encoding="utf-8")
        stage += 1
    
    

    # --- Stage 2: Processing (Lint → noqa → Fix) ---

    # Generate file for lint to be run on (save to target dir, not audit dir)
    pre_lint_file = filepath.parent / f"{filepath.stem}_pre_lint.sql"
    pre_lint_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(pre_lint_file)
    stage += 1


    # Lints the file using CLEANUP_RULES ruleset
    lint_code, lint_results = run_sqlfluff_lint(pre_lint_file, rules=CLEANUP_RULES + NEWLINE_RULES, debug_verbose=debug_verbose)
    if debug:
        (audit_path / f"{stage:02d}_pre_lint.json").write_text(json.dumps(lint_results, indent=4), encoding="utf-8")
        stage += 1


    # List unfixable violations
    unfixable = extract_unfixable_violations(lint_results)


    # Add NOQA comments to prevent unfixable violations from breaking sqlfluff fix
    # NOTE: There should be no unfixable violations with this CLEANUP_RULES ruleset
    sql = add_noqa_comments(sql, unfixable)
    if debug:
        (audit_path / f"{stage:02d}_pre_noqa.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Generate file for fix to be run on
    pre_fix_file = filepath.parent / f"{filepath.stem}_pre_fix.sql"
    pre_fix_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(pre_fix_file)


    # Runs sqlfluff fix with limited ruleset to fix case and spacing
    # This is to make it easier to apply manual fixes later, like inserting newlines
    fix_code, sql = run_sqlfluff_fix(pre_fix_file, CLEANUP_RULES + NEWLINE_RULES, debug_verbose=debug_verbose)
    if debug:
        (audit_path / f"{stage:02d}_pre_fix.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Extract any Jinja, comments, or strings and replace with placeholders
    # To prevent certain alterations accidentily altering them 
    sql, placeholders = extract_placeholders(sql)
    if debug:
        (audit_path / f"{stage:02d}_extract_placeholders.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Add newlines before keywords to enable sqlfluff fix to indent correctly
    sql = insert_newlines_before_keywords(sql)
    if debug:
        (audit_path / f"{stage:02d}_newline_keywords.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Reinsert string and comments
    sql = restore_placeholders(sql, placeholders)
    if debug:
        (audit_path / f"{stage:02d}_restore_placeholders.sql").write_text(sql, encoding="utf-8")
        stage += 1

    
    # Generate file for lint to be run on (save to target dir, not audit dir)
    lint_file = filepath.parent / f"{filepath.stem}_lint.sql"
    lint_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(lint_file)
    stage += 1


    # --- Stage 2: Processing (Lint → noqa → Fix) ---
    lint_code, lint_results = run_sqlfluff_lint(lint_file, debug_verbose=debug_verbose)
    if debug:
        (audit_path / f"{stage:02d}_lint.json").write_text(json.dumps(lint_results, indent=4), encoding="utf-8")
        stage += 1


    # List unfixable violations
    unfixable = extract_unfixable_violations(lint_results)


    # adds NOQA comments to prevent unfixable violations breaking sqlfluff fix
    sql = add_noqa_comments(sql, unfixable)
    if debug:
        (audit_path / f"{stage:02d}_noqa.sql").write_text(sql, encoding="utf-8")
        stage += 1

    
    # Generate file for fix to be run on
    fix_file = filepath.parent / f"{filepath.stem}_fix.sql"
    fix_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(fix_file)


    # Runs main sqlfluff fix
    fix_code, sql = run_sqlfluff_fix(fix_file, debug_verbose=debug_verbose)
    if debug:
        (audit_path / f"{stage:02d}_fix.sql").write_text(sql, encoding="utf-8")
        stage += 1
    

    # --- Stage 3: Post-processing ---

    # Add newlines before major clauses to make them more readable
    sql = add_blank_line_before_major_clauses(sql, debug_verbose)
    if debug:
        (audit_path / f"{stage:02d}_newline_major_clauses.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # add newlines around semicolons
    sql = re.sub(r'\s*;\s*', r'\n\n;\n\n', sql)
    if debug:
        (audit_path / f"{stage:02d}_postprocess.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Convert '--' into '/**/' comments
    sql = convert_single_to_block_comments(sql)
    if debug:
        (audit_path / f"{stage:02d}_convert_comments.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # Move comments to their own line
    sql = move_inline_block_comments(sql)
    if debug:
        (audit_path / f"{stage:02d}_move_comments.sql").write_text(sql, encoding="utf-8")
        stage += 1


    # --- Finalize ---
    if debug:
        (audit_path / f"{stage:02d}_final.sql").write_text(sql, encoding="utf-8")
        print(f"DEBUG MODE: Final output saved to {audit_path}")
    else:
        filepath.write_text(sql, encoding="utf-8")
        print(f"File {filepath} formatted and replaced successfully.")
    

    # Remove all the temp files created in the target dir
    for file in temp_files_to_delete:
        if file.exists():
            try:
                os.remove(file)
            except Exception as e:
                print(f"Warning: Could not delete file {file}: {e}")



# ------------------ Main entrypoint ------------------
def main():

    parser = argparse.ArgumentParser(description="SQL Format & Lint Tool")
    parser.add_argument("path", help="Path to SQL file or folder")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--debug_verbose", action="store_true", help="Enable verbose debug logging")
    args = parser.parse_args()

    path = Path(args.path).resolve()

    if not path.exists():
        print(f"ERROR: Path does not exist: {path}")
        return
    
    # validate_sqlfluff_config(SQLFLUFF_CONFIG_PATH, path)

    sql_files = []
    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files = [path]
    elif path.is_dir():
        sql_files = list(path.rglob("*.sql"))

    if not sql_files:
        print("No .sql files found to process.")
        return

    for sql_file in sql_files:
        process_sql_file(sql_file, args.debug, args.debug_verbose)


if __name__ == "__main__":
    main()

