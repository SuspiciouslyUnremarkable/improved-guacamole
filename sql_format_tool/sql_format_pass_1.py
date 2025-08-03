import argparse
import json
import re
import os
import subprocess
import shutil
import tempfile
import sqlparse
import gc
from sqlparse.sql import TokenList
from sqlparse.sql import Parenthesis
from sqlparse.tokens import Keyword, DML, Punctuation, Whitespace
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


PROTECTED_KEYWORDS = {"OVER (", "WITHIN GROUP ("}

def flatten_whitespace(sql: str) -> str:
    """
    Replace all whitespace (spaces, tabs, newlines) with a single space,
    effectively flattening the SQL into one line.

    Args:
        sql (str): SQL string to flatten.

    Returns:
        str: Single-line SQL with normalized whitespace.
    """
    # Replace any run of whitespace with a single space
    flattened = re.sub(r'\s+', ' ', sql)
    return flattened.strip()



def write_stage(audit_path: Path, stage: int, name: str, content: str, debug: bool) -> int:
    if debug:
        (audit_path / f"{stage:02d}_{name}.sql").write_text(content, encoding="utf-8")
    return stage + 1


def insert_newlines_before_keywords(sql: str) -> str:
    """
    Insert newline before major SQL keywords (from NEWLINE_KEYWORDS)
    only if there is not already a newline separating it from the previous
    non-whitespace token.
    """
    statements = sqlparse.parse(sql)
    updated_sql = []

    for stmt in statements:
        tokens = list(stmt.flatten())
        result = []
        i = 0

        while i < len(tokens):
            token = tokens[i]
            text_upper = token.value.upper()

            # Build combined keyword (e.g., "ORDER BY")
            combined = None
            if i < len(tokens) - 1:
                next_upper = tokens[i + 1].value.upper()
                combined = f"{text_upper} {next_upper}"

            # Check if current token is a keyword that needs a newline
            if combined in NEWLINE_KEYWORDS or text_upper in NEWLINE_KEYWORDS:
                # Find previous non-whitespace token
                j = len(result) - 1
                while j >= 0 and result[j].strip() == "":
                    j -= 1

                # Check if we already have a newline before this keyword
                already_has_newline = (j >= 0 and result[j].endswith("\n"))

                if not already_has_newline:
                    result.append("\n")

                # Handle combined keyword (ORDER BY, GROUP BY, etc.)
                if combined in NEWLINE_KEYWORDS:
                    result.append(f"{token.value} {tokens[i + 1].value}")
                    i += 2
                    continue
                else:
                    result.append(token.value)
            else:
                result.append(token.value)

            i += 1

        updated_sql.append("".join(result))

    return "\n".join(updated_sql)


# def normalize_newlines_and_keywords(sql: str, keyword_list: list[str], verbose: bool = False) -> str:
#     """
#     1. Trims each line's whitespace.
#     2. Collapses multiple blank lines.
#     3. Adds a newline before keywords (from keyword_list).
#     4. Replaces any newline directly after a comma with a space.
#     """
#     # --- 1: Trim leading/trailing whitespace on each line ---
#     lines = [line.strip() for line in sql.splitlines()]
#     sql = "\n".join(lines)

#     # --- 2: Collapse multiple blank lines ---
#     sql = re.sub(r"\n{2,}", "\n", sql)

#     # --- 3: Add newline before keywords ---
#     output = []
#     for line in sql.splitlines():
#         stripped = line.strip()
#         upper = stripped.upper()
#         if any(upper.startswith(k) for k in keyword_list):
#             if output and output[-1].strip() != "":
#                 output.append("")
#                 if verbose:
#                     print(f"[DEBUG] Added blank line before: {stripped}")
#         output.append(line)
#     sql = "\n".join(output)

#     # --- 4: Remove newline immediately after commas ---
#     # Replace a comma followed by optional spaces/newline and indentation with just a comma + space
#     sql = re.sub(r",\s*\n\s*", ", ", sql)

#     return sql



def add_blank_line_before_major_clauses(sql: str, verbose: bool = False) -> str:
    lines = sql.splitlines()
    result = []
    in_protected_region = False
    paren_depth = 0

    for line in lines:
        raw_line = line
        stripped = line.strip()
        upper_line = stripped.upper()

        # Protected region detection (OVER(, WITHIN GROUP()
        if not in_protected_region and any(upper_line.endswith(pk) for pk in PROTECTED_KEYWORDS):
            in_protected_region = True
            paren_depth = 1
            result.append(raw_line)
            if verbose:
                print(f"[DEBUG] Entering protected region at line: {raw_line}")
            continue

        if in_protected_region:
            paren_depth += raw_line.count("(")
            paren_depth -= raw_line.count(")")
            result.append(raw_line)
            if paren_depth <= 0:
                in_protected_region = False
                if verbose:
                    print(f"[DEBUG] Leaving protected region at line: {raw_line}")
            continue

        if any(upper_line.startswith(clause) for clause in MAJOR_CLAUSES):
            if result and result[-1].strip() != "":
                result.append("")
                if verbose:
                    print(f"[DEBUG] Added blank line before: {raw_line.strip()}")
            result.append(raw_line)
        else:
            result.append(raw_line)

    return "\n".join(result)



def remove_extra_spaces(sql: str) -> str:
    """
    Replace multiple spaces with a single space, without affecting newlines.
    """
    # Replace sequences of 2+ spaces (not including newlines) with a single space
    return re.sub(r' {2,}', ' ', sql)



def insert_newlines_for_parens_and_commas(sql: str, verbose: bool = False) -> str:
    """
    Adds newlines to:
      1) Parentheses blocks that are unbalanced or complex (not simple balanced inline calls).
         - Newline before unmatched closing `)` (forward pass)
         - Newline after unmatched opening `(` (backward pass)
      2) Commas that are outside parentheses or inside already complex parentheses.

    Args:
        sql (str): SQL string with newlines already present.
        verbose (bool): If True, prints debug info.

    Returns:
        str: SQL with newlines added where appropriate.
    """

    def is_complex_parentheses(paren_only: str) -> bool:
        # Remove paired "()" repeatedly to detect complexity
        temp = paren_only
        for _ in range(temp.count("(")):
            temp = temp.replace("()", "")
        return len(temp) > 0  # If any unpaired or non-nested remains → complex

    lines = sql.splitlines()
    result = []

    for line in lines:
        stripped = line.rstrip()

        # --- Detect if parentheses are complex ---
        paren_only = "".join(ch for ch in stripped if ch in "()")
        has_complex_parens = bool(paren_only) and is_complex_parentheses(paren_only)

        # === Pass 1: Parentheses handling ===
        # Forward pass for unmatched closing ")"
        depth = 0
        forward_result = ""
        for ch in stripped:
            if ch == "(":
                depth += 1
                forward_result += ch
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    forward_result += "\n" + ch
                    depth = 0
                    if verbose:
                        print(f"[DEBUG] Added newline before unmatched ')' in: {line}")
                else:
                    forward_result += ch
            else:
                forward_result += ch

        # Backward pass for unmatched opening "("
        depth = 0
        backward_chars = []
        for ch in reversed(forward_result):
            if ch == ")":
                depth += 1
                backward_chars.append(ch)
            elif ch == "(":
                depth -= 1
                if depth < 0:
                    backward_chars.append(ch + "\n")
                    depth = 0
                    if verbose:
                        print(f"[DEBUG] Added newline after unmatched '(' in: {line}")
                else:
                    backward_chars.append(ch)
            else:
                backward_chars.append(ch)
        paren_fixed = "".join(reversed(backward_chars))

        # === Pass 2: Comma handling ===
        paren_depth = 0
        buffer = ""
        segments = []
        for i, ch in enumerate(paren_fixed):
            if ch == "(":
                paren_depth += 1
                buffer += ch
            elif ch == ")":
                paren_depth = max(paren_depth - 1, 0)
                buffer += ch
            elif ch == ",":
                # Break comma if outside parentheses or inside already complex parentheses
                if paren_depth == 0 or has_complex_parens:
                    if buffer.strip():  # Avoid double-breaking existing formatted lines
                        segments.append(buffer)
                        buffer = ch
                        if verbose:
                            print(f"[DEBUG] Added newline before comma in: {line}")
                    else:
                        buffer += ch
                else:
                    buffer += ch
            else:
                buffer += ch
        segments.append(buffer)
        final_line = "\n".join(segments)

        result.append(final_line)

    return "\n".join(result)



def convert_sql_to_uppercase(sql: str, verbose: bool = False) -> str:
    """
    Converts all SQL characters to uppercase.
    Assumes placeholders and string literals have been extracted or protected already.

    Args:
        sql (str): Input SQL.
        verbose (bool): Print debug info.

    Returns:
        str: SQL fully uppercased.
    """
    upper_sql = sql.upper()
    if verbose:
        print("[DEBUG] Converted SQL to uppercase.")
    return upper_sql




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
def run_sqlfluff_lint(sql_file_path: Path, rules: Optional[List[str]] = None, verbose = False) -> Tuple[int, dict]:
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

    if rules and verbose:
        print(result.stdout)
        print(result.stderr)

    return result.returncode, lint_results


def run_sqlfluff_fix(sql_file_path: Path, rules: Optional[List[str]] = None, verbose: bool = False) -> Tuple[int, str]:
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

    if rules and verbose:
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
                lines[line_no] += f"  -- noqa: {code} :: {description}"

    return "\n".join(lines)



def add_newline_after_cte_closing(sql: str, verbose: bool = False) -> str:
    """
    Adds an extra newline after each CTE closing parenthesis in a WITH clause.
    Example:
        WITH a AS (
            SELECT 1
        ), b AS (
            SELECT 2
        )
        SELECT 3
    becomes:
        WITH a AS (
            SELECT 1
        ),

        b AS (
            SELECT 2
        )

        SELECT 3
    """
    lines = sql.splitlines()
    output = []

    inside_with = False
    paren_depth = 0
    cte_depth_trigger = None  # depth when a CTE started

    for line in lines:
        stripped = line.strip().upper()

        # Detect entering WITH
        if stripped.startswith("WITH "):
            inside_with = True
            if verbose:
                print("[DEBUG] Entering WITH clause")

        # Track parentheses depth
        open_count = line.count("(")
        close_count = line.count(")")
        prev_depth = paren_depth
        paren_depth += open_count - close_count

        # Detect start of a CTE block: "<name> AS ("
        if re.match(r".+\bAS\s*\($", line.strip(), re.IGNORECASE):
            cte_depth_trigger = prev_depth + 1
            if verbose:
                print(f"[DEBUG] Starting CTE block, depth trigger={cte_depth_trigger}")

        output.append(line)

        # Detect CTE closing
        if inside_with and cte_depth_trigger is not None and paren_depth == cte_depth_trigger - 1:
            output.append("")  # extra newline
            if verbose:
                print(f"[DEBUG] Added newline after CTE closing at depth {paren_depth}")
            cte_depth_trigger = None  # reset

        # Detect leaving WITH (final SELECT marks end of CTEs)
        if inside_with and stripped.startswith("SELECT") and cte_depth_trigger is None and paren_depth == 0:
            inside_with = False
            if verbose:
                print("[DEBUG] Leaving WITH clause")

    return "\n".join(output)



def strip_indentation(sql: str) -> str:
    """
    Remove all leading whitespace from each line.
    Useful for resetting indentation before re-indenting.
    """
    lines = sql.splitlines()
    return "\n".join(line.lstrip() for line in lines)


def reindent_sql(sql: str, indent: str = "    ", verbose: bool = False) -> str:
    """
    Reindent SQL from scratch:
        - Major clauses define base indentation.
        - CASE WHEN blocks:
            CASE = clause + 1
            WHEN = CASE + 1
            THEN/ELSE = CASE + 2
            END = CASE + 1
            AND/OR inside CASE align with WHEN (CASE + 1)
        - OVER() and WITHIN GROUP() blocks get +1 indentation for block and contents.
        - Parentheses add +1 indentation level.
        - Reset context after ';'
        - First line after SELECT indented an extra 2 spaces
        - Comments (/*...*/ and -- ...) are preserved as-is.
    """

    MAJOR_CLAUSES = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
        "UNION", "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
        "INNER JOIN", "OUTER JOIN", "CROSS JOIN",
        "LEFT OUTER JOIN", "RIGHT OUTER JOIN",
        "EXCEPT", "INTERSECT", "MINUS", "QUALIFY", "USING"
    ]

    def is_major_clause(line: str) -> bool:
        upper = line.upper()
        return any(upper == c or upper.startswith(f"{c} ") for c in MAJOR_CLAUSES)

    clause_stack = []
    result = []
    after_select = False

    def current_indent_offset():
        return sum(entry["inherited_indent"] for entry in clause_stack)

    lines = sql.splitlines()
    for raw_line in lines:
        stripped = raw_line.strip()
        upper = stripped.upper()

        if verbose:
            print(f"\n--- Processing line: '{stripped}' ---")
            print(f"[STACK BEFORE] {clause_stack}")

        # --- Comments ---
        if stripped.startswith("/*") or stripped.startswith("--"):
            result.append(stripped)
            continue

        # --- Reset on semicolon ---
        if stripped == ";":
            clause_stack.clear()
            after_select = False
            result.append(";")
            continue

        # --- Pops ---
        if upper.startswith("WHEN"):
            while clause_stack and clause_stack[-1]["type"] in ("WHEN", "THEN/ELSE"):
                clause_stack.pop()
        elif upper.startswith("THEN") or upper.startswith("ELSE"):
            while clause_stack and clause_stack[-1]["type"] == "THEN/ELSE":
                clause_stack.pop()

        # Closing parenthesis handling (OVER/WITHIN GROUP or normal paren)
        if stripped.startswith(")") and clause_stack:
            if clause_stack[-1]["type"] == "PAREN":
                clause_stack.pop()
            if clause_stack[-1]["type"] in ("OVER", "WITHIN_GROUP"):
                indent_level = current_indent_offset()
                result.append((indent * indent_level) + stripped)
                clause_stack.pop()
                continue
            else:
                while clause_stack and clause_stack[-1]["keyword"] != "(":
                    clause_stack.pop()
                if clause_stack and clause_stack[-1]["keyword"] == "(":
                    clause_stack.pop()

        # Pop previous major clause
        if is_major_clause(stripped):
            if clause_stack and clause_stack[-1]["type"] == "CLAUSE":
                clause_stack.pop()

        # --- Special END handling ---
        if upper.startswith("END"):
            while clause_stack and clause_stack[-1]["type"] in ("THEN/ELSE", "WHEN"):
                clause_stack.pop()
            indent_level = current_indent_offset()
            extra_spaces = "  " if after_select else ""
            after_select = False
            result.append((indent * indent_level) + extra_spaces + stripped)
            if clause_stack and clause_stack[-1]["type"] == "CASE":
                clause_stack.pop()
            continue

        # --- NEW: OVER and WITHIN GROUP blocks ---
        if "OVER (" in upper:
            clause_stack.append({"type": "OVER", "keyword": "OVER", "inherited_indent": 1})
        if "WITHIN GROUP (" in upper:
            clause_stack.append({"type": "WITHIN_GROUP", "keyword": "WITHIN GROUP", "inherited_indent": 1})

        # --- Indentation for current line ---
        indent_level = current_indent_offset()
        extra_spaces = ""
        if after_select and not is_major_clause(stripped) and upper != "SELECT":
            extra_spaces = "  "
            after_select = False
        result.append((indent * indent_level) + extra_spaces + stripped)



        if is_major_clause(stripped):
            # If inside an OVER or WITHIN_GROUP, skip ORDER BY
            if upper.startswith("ORDER BY") and any(e["type"] in ("OVER", "WITHIN_GROUP") for e in clause_stack):
                if verbose:
                    print("[SKIP] ORDER BY inside OVER/WITHIN GROUP (not a top-level clause)")
            else:
                clause_stack.append({"type": "CLAUSE", "keyword": upper.split()[0], "inherited_indent": 1})
                if upper.startswith("SELECT"):
                    after_select = True
                else:
                    after_select = False

        elif upper.endswith("CASE"):
            clause_stack.append({"type": "CASE", "keyword": "CASE", "inherited_indent": 1})
        elif upper.startswith("WHEN"):
            clause_stack.append({"type": "WHEN", "keyword": "WHEN", "inherited_indent": 1})
        elif upper.startswith("THEN") or upper.startswith("ELSE"):
            clause_stack.append({"type": "THEN/ELSE", "keyword": upper.split()[0], "inherited_indent": 2})
        elif upper.startswith("AND") or upper.startswith("OR"):
            in_case = any(entry["type"] == "CASE" for entry in clause_stack)
            inherited_indent = 1 if in_case else 0
            clause_stack.append({"type": "AND/OR", "keyword": upper.split()[0], "inherited_indent": inherited_indent})

        

        if stripped.endswith("("):
            clause_stack.append({"type": "PAREN", "keyword": "(", "inherited_indent": 1})

    return "\n".join(result)






# ------------------ Write JSON ------------------
def write_json_pretty(data, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)



def process_sql_file(filepath: Path, debug: bool = False, verbose: bool = False):
    
    # Checks that filepath exists and is for a sql file
    if not filepath.exists() or filepath.suffix.lower() != ".sql":
        print(f"Skipping invalid file: {filepath}")
        return

    # List of files created in the target dir that will need to be deleted later
    temp_files_to_delete = []

    # Set audit path
    audit_path = AUDIT_FOLDER / filepath.stem

    # Clears audit path
    if audit_path.exists():
        shutil.rmtree(audit_path)
    audit_path.mkdir(parents=True, exist_ok=True)

    # stage counter for automated audit filenames
    stage = 1

    # Loads sql from sql file
    sql = filepath.read_text(encoding="utf-8")
    stage = write_stage(audit_path, stage, "original", sql, debug)


    # Remove all newlines and indentation
    # This is because some files contain newlines in weird places, and sqlfluff does not identiy/purge these correctly
    sql = flatten_whitespace(sql)
    stage = write_stage(audit_path, stage, "flatten_whitespace", sql, debug)

    # Extract any Jinja, comments, or strings and replace with placeholders
    # To prevent certain alterations accidentily altering them 
    sql, placeholders = extract_placeholders(sql)
    stage = write_stage(audit_path, stage, "extract_placeholders", sql, debug)

    # Converts everything to uppercase
    sql = convert_sql_to_uppercase(sql)
    stage = write_stage(audit_path, stage, "uppercase", sql, debug)

    # Insert newlines before keywords
    sql = insert_newlines_before_keywords(sql)
    stage = write_stage(audit_path, stage, "newlines_keywords", sql, debug)

    # add newlines around semicolons
    sql = re.sub(r'\s*;\s*', r'\n\n;\n\n', sql)
    stage = write_stage(audit_path, stage, "newline_semicolon", sql, debug)


    # Insert newlines for multi-line parentheses blocks
    sql = insert_newlines_for_parens_and_commas(sql)
    stage = write_stage(audit_path, stage, "newlines_parentheses", sql, debug)


    # Reinsert string and comments
    sql = restore_placeholders(sql, placeholders)
    stage = write_stage(audit_path, stage, "restore_placeholders", sql, debug)

    # Generate file for lint to be run on (save to target dir, not audit dir)
    lint_file = filepath.parent / f"{filepath.stem}_lint.sql"
    lint_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(lint_file)
    stage += 1

    # --- Stage 2: Processing (Lint → noqa → Fix) ---
    lint_code, lint_results = run_sqlfluff_lint(lint_file, verbose=verbose)
    if debug:
        (audit_path / f"{stage:02d}_lint.json").write_text(json.dumps(lint_results, indent=4), encoding="utf-8")
        stage += 1

    # List unfixable violations
    unfixable = extract_unfixable_violations(lint_results)

    # adds NOQA comments to prevent unfixable violations breaking sqlfluff fix
    if unfixable:
        sql = add_noqa_comments(sql, unfixable)
        stage = write_stage(audit_path, stage, "noqa_comments", sql, debug)

    # Generate file for fix to be run on
    fix_file = filepath.parent / f"{filepath.stem}_fix.sql"
    fix_file.write_text(sql, encoding="utf-8")
    temp_files_to_delete.append(fix_file)

    # Runs main sqlfluff fix
    fix_code, sql = run_sqlfluff_fix(fix_file)
    stage = write_stage(audit_path, stage, "fix", sql, debug)

    # # Add newlines before major clauses to make them more readable
    # sql = add_blank_line_before_major_clauses(sql)
    # stage = write_stage(audit_path, stage, "newline_major_clauses", sql, debug)


    # Newline between CTEs
    sql = add_newline_after_cte_closing(sql)
    stage = write_stage(audit_path, stage, "newline_after_cte", sql, debug)

    # Convert '--' into '/**/' comments
    sql = convert_single_to_block_comments(sql)
    stage = write_stage(audit_path, stage, "convert_comments", sql, debug)

    # Move comments to their own line
    sql = move_inline_block_comments(sql)
    stage = write_stage(audit_path, stage, "move_comments", sql, debug)

    # add newlines around semicolons
    sql = re.sub(r'\s*;\s*', r'\n\n;\n\n', sql)
    stage = write_stage(audit_path, stage, "newline_semicolon", sql, debug)

    # Strip existing indentation
    sql = strip_indentation(sql)
    stage = write_stage(audit_path, stage, "strip_indentation", sql, debug)

    # Apply indentation
    sql = reindent_sql(sql, verbose=verbose)
    stage = write_stage(audit_path, stage, "indentation", sql, debug)

    

    # --- Finalize ---
    if debug:
        stage = write_stage(audit_path, stage, "final", sql, debug)
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
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    args = parser.parse_args()

    path = Path(args.path).resolve()

    if not path.exists():
        print(f"ERROR: Path does not exist: {path}")
        return

    sql_files = []
    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files = [path]
    elif path.is_dir():
        sql_files = list(path.rglob("*.sql"))

    if not sql_files:
        print("No .sql files found to process.")
        return

    for sql_file in sql_files:
        process_sql_file(sql_file, args.debug, args.verbose)

    gc.collect()


if __name__ == "__main__":
    main()

