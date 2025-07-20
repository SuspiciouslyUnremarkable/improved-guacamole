"""

SQL Formatting Tool - Pass 1

 

This script applies positional and case formatting to SQL files using sqlfluff.

It is intended to be run before structural changes (Pass 2) for easier code review.

 

Usage:

- Run interactively and enter the path to a SQL file or directory.

- The script will process all .sql files found, saving audit copies at each stage.

- Review output and commit changes before running Pass 2.

 

Main Functions:

- pass1_format_sql_file: Handles formatting and audit logging for each file.

- run_sqlfluff_fix: Runs sqlfluff with positional/case rules only, logs errors and unfixable violations.

- find_sql_files: Recursively finds .sql files in a directory or single file.

 

Error Handling:

- If sqlfluff fails, unfixable violations are printed and formatting stops for that file.

- A diff file is generated if unexpected structural changes are detected.

 

Summary:

- At the end, a summary of processed and skipped files is printed.

"""

 

# === USER-CONFIGURABLE VARIABLES ===

PASS1_VERSION = 1

SQL_DIALECT = "snowflake"

SQLFLUFF_CONFIG = "sql_format_tool/scripts/.sqlfluff"

AUDIT_ROOT = "sql_format_tool/audit_folder"  # Root for audit folders (should be gitignored)

DEBUG = False  # Set to True to enable debug prints

DBT_PROJECT_DIR = "../dbt"  # Path to dbt project directory, update as needed

import argparse

import subprocess

from pathlib import Path

import re

import shutil

import json

import difflib

import sys

 

# Exclude rules for first pass (positional/case only)

# === PASS 1 TEMPORARY IGNORE RULES FOR DEBUGGING ===

PASS_ONE_DEBUG_EXCLUDE_RULES = [

    # "capitalisation.keywords",  # CP01

    # "capitalisation.functions", # CP03

    # "layout.indent",           # LT02

    # "layout.newlines",         # LT15

    # "layout.end_of_file"       # LT12

    # "layout.functions",         # LT06

    # "layout.cte_bracket",       # LT07

    # "layout.set_operators",     # LT11

    # "layout.keyword_newline",   # LT14

]

# === END PASS 1 TEMPORARY IGNORE RULES FOR DEBUGGING ===

 

PASS_ONE_EXCLUDE_RULES = [

    "ambiguous.column_count", "ambiguous.distinct", "ambiguous.join", "ambiguous.column_references", "ambiguous.set_columns", "ambiguous.join_condition",

    "aliasing.length", "aliasing.unique.column", "aliasing.self_alias.column", "aliasing.table",

    "convention.not_equal", "convention.coalesce", "convention.select_trailing_comma", "convention.is_null", "convention.statement_brackets", "convention.left_join", "convention.casting_style", "convention.join_condition",

    "references.from", "references.qualification", "references.keywords", "references.special_chars", "references.consistent",

    "structure.simple_case", "structure.unused_cte", "structure.nested_case", "structure.subquery", "structure.using", "structure.distinct", "structure.join_condition_order", "structure.constant_expression", "structure.unused_join", "structure.column_order",

    "layout.spacing", "layout.long_lines"

] + PASS_ONE_DEBUG_EXCLUDE_RULES

# === END USER-CONFIGURABLE VARIABLES ===

 

PASS1_COMMENT = f"-- sqlfluff-pass1-version: {PASS1_VERSION}"

 

def debug_print(*args):

    if DEBUG:

        print(*args)

 

def has_pass1_comment(sql: str) -> bool:

    match = re.match(r"^-- sqlfluff-pass1-version: (\d+)", sql)

    if match:

        version = int(match.group(1))

        return version >= PASS1_VERSION

    return False

 

def insert_pass1_comment(sql: str) -> str:

    # Remove any existing pass1 comment

    sql = re.sub(r"^-- sqlfluff-pass1-version: \d+\s*", "", sql, count=1)

    return PASS1_COMMENT + "\n" + sql

 

def flatten_sql(sql: str) -> str:

    # Remove all whitespace (spaces, tabs, newlines) and make lowercase for comparison

    sql = re.sub(r"\s+", "", sql)  # Remove all whitespace

    return sql.lower().strip()

 

def add_newlines_before_keywords(sql: str) -> str:

    # List of common Snowflake SQL keywords (add more as needed)

    keywords = [

        'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'INNER JOIN', 'OUTER JOIN',

        'ON', 'AND', 'OR', 'UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT', 'WHEN', 'THEN', 'ELSE', 'END',

        'WITH', 'LIMIT', 'OFFSET', 'OVER', 'PARTITION BY', 'QUALIFY'

    ]

    # Sort by length descending to avoid partial matches (e.g., 'IN' in 'INNER JOIN')

    keywords = sorted(keywords, key=len, reverse=True)

    for kw in keywords:

        # Add newline before keyword if not already at line start

        # Use word boundaries and ignore case

        pattern = r'(?i)(?<!\n)(?<!^)\b' + re.escape(kw) + r'\b'

        sql = re.sub(pattern, f'\n{kw.upper()}', sql)

    debug_print("✔️ Inserted newlines before all major SQL keywords")

    return sql

 

def get_audit_dir_for_sql(sql_path: Path, input_root: Path) -> Path:

    """Return the audit directory for a given SQL file, mirroring the hierarchy and using the SQL filename as a folder name."""

    rel_path = sql_path.relative_to(input_root)

    audit_dir = Path(AUDIT_ROOT) / rel_path.parent / rel_path.stem

    audit_dir.mkdir(parents=True, exist_ok=True)

    return audit_dir

 

def save_lint_json_to_audit(lint_json, src_path: Path, input_root: Path, pass_num: int = 1):

    """Save lint JSON to the audit folder for the SQL file, as passX_02_lint.json."""

    audit_dir = get_audit_dir_for_sql(src_path, input_root)

    dest = audit_dir / f"pass{pass_num}_02_lint.json"

    with open(dest, 'w', encoding='utf-8') as f:

        json.dump(lint_json, f, indent=2)

    print(f"Lint JSON saved to {dest}")

    return dest

 

def copy_to_audit(src: Path, audit_stage: str, input_root: Path):

    """Copy a file to the audit folder for the SQL file, using the new naming convention."""

    audit_dir = get_audit_dir_for_sql(src, input_root)

    if audit_stage == "pre_pass1":

        dest = audit_dir / "pass1_01_pre_format.sql"

    elif audit_stage == "post_pass1":

        dest = audit_dir / "pass1_03_post_format.sql"

    elif audit_stage == "pre_pass2":

        dest = audit_dir / "pass2_01_pre_format.sql"

    elif audit_stage == "post_pass2":

        dest = audit_dir / "pass2_03_post_format.sql"

    else:

        dest = audit_dir / f"{audit_stage}.sql"

    dest.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(src, dest)

    return dest

 

def run_sqlfluff_fix(filepath: Path, exclude_rules=None, add_noqa_on_fail=False, input_root: Path = None, pass_num: int = 1):

    cmd = [

        "sqlfluff", "fix", str(filepath),

        "--dialect", SQL_DIALECT,

        "--config", SQLFLUFF_CONFIG

    ]

    if exclude_rules:

        cmd += ["--exclude-rules", ",".join(exclude_rules)]

    debug_print(f"Running: {' '.join(cmd)}")

    noqa_comments = []  # Track (line_no, noqa_comment) for later removal

    try:

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)

        debug_print(result.stdout)

        debug_print(result.stderr)

        return noqa_comments  # No comments added

    except subprocess.CalledProcessError as e:

        print(f"⚠️ sqlfluff exited with error code {e.returncode} for {filepath.name}.")

        if e.stdout:

            print(e.stdout)

        if e.stderr:

            debug_print(e.stderr)

        if add_noqa_on_fail and e.returncode == 1:

            # Run sqlfluff lint --format json to extract violations

            print("\n=== LINTING FOR UNRESOLVED VIOLATIONS ===")

            lint_cmd = [

                "sqlfluff", "lint", str(filepath),

                "--dialect", SQL_DIALECT,

                "--config", SQLFLUFF_CONFIG,

                "--format", "json"

            ]

            if exclude_rules:

                lint_cmd += ["--exclude-rules", ",".join(exclude_rules)]

            lint_result = subprocess.run(lint_cmd, capture_output=True, text=True)

            # Save lint JSON to audit folder for this SQL file

            if input_root is not None:

                save_lint_json_to_audit(lint_result, filepath, input_root, pass_num=pass_num)

            else:

                lint_json_path = filepath.with_suffix('.lint.json')

                with open(lint_json_path, 'w', encoding='utf-8') as f:

                    json.dump(lint_json, f, indent=2)

                print(f"Lint JSON saved to {lint_json_path}")

            lint_json = None

            try:

                lint_json = json.loads(lint_result.stdout)

                with open(lint_json_path, 'w', encoding='utf-8') as f:

                    json.dump(lint_json, f, indent=2)

                print(f"Lint JSON saved to {lint_json_path}")

            except Exception as ex:

                print("Could not parse sqlfluff lint JSON output:", ex)

                print(lint_result.stdout)

                return noqa_comments

            # Find all unresolved violations (fixes == [])

            violations = []

            for file_result in lint_json:

                for v in file_result.get("violations", []):

                    if isinstance(v.get("fixes", None), list) and len(v["fixes"]) == 0:

                        # Use start_line_no if present, else fallback to line_no if it exists

                        line_no = v.get("start_line_no") or v.get("line_no")

                        if line_no is not None:

                            violations.append({

                                "line_no": line_no,

                                "code": v["code"]

                            })

            if not violations:

                print("No unfixable violations found.")

                return noqa_comments

            # Add noqa comments to the relevant lines

            sql_lines = filepath.read_text(encoding='utf-8').splitlines()

            for v in violations:

                idx = v["line_no"] - 1

                noqa_comment = f"-- noqa: {v['code']}"

                if idx < len(sql_lines):

                    if noqa_comment not in sql_lines[idx]:

                        sql_lines[idx] = sql_lines[idx].rstrip() + f"  {noqa_comment}"

                        noqa_comments.append((idx, noqa_comment))

            filepath.write_text("\n".join(sql_lines) + "\n", encoding='utf-8')

            print(f"Added noqa comments for violations: {violations}")

            # Retry fix ONCE

            try:

                result = subprocess.run(cmd, check=True, capture_output=True, text=True)

                debug_print(result.stdout)

                debug_print(result.stderr)

                return noqa_comments

            except subprocess.CalledProcessError as e2:

                print(f"❌ sqlfluff fix still failed after adding noqa comments. See {lint_json_path} for details.")

                if e2.stdout:

                    print(e2.stdout)

                if e2.stderr:

                    debug_print(e2.stderr)

                sys.exit(1)

        else:

            # Run sqlfluff lint --format json to extract unfixable violations (legacy path)

            print("\n=== UNFIXABLE VIOLATIONS (from sqlfluff lint) ===")

            lint_cmd = [

                "sqlfluff", "lint", str(filepath),

                "--dialect", SQL_DIALECT,

                "--config", SQLFLUFF_CONFIG,

                "--format", "json"

            ]

            if exclude_rules:

                lint_cmd += ["--exclude-rules", ",".join(exclude_rules)]

            lint_result = subprocess.run(lint_cmd, capture_output=True, text=True)

            # Save lint JSON to audit folder for this SQL file

            if input_root is not None:

                save_lint_json_to_audit(lint_result, filepath, input_root, pass_num=pass_num)

            else:

                lint_json_path = filepath.with_suffix('.lint.json')

                with open(lint_json_path, 'w', encoding='utf-8') as f:

                    json.dump(lint_json, f, indent=2)

                print(f"Lint JSON saved to {lint_json_path}")

            try:

                lint_json = json.loads(lint_result.stdout)

                with open(lint_json_path, 'w', encoding='utf-8') as f:

                    json.dump(lint_json, f, indent=2)

                found_unfixable = False

                for file_result in lint_json:

                    for v in file_result.get("violations", []):

                        if isinstance(v.get("fixes", None), list) and len(v["fixes"]) == 0:

                            line_no = v.get("start_line_no") or v.get("line_no")

                            if line_no is not None:

                                print(f"UNFIXABLE: L:{line_no} | P:{v.get('start_line_pos', '?')} | {v['code']} | {v['description']}")

                                found_unfixable = True

                if not found_unfixable:

                    print("No unfixable violations found.")

            except Exception as ex:

                print("Could not parse sqlfluff lint JSON output:", ex)

            print("=== END UNFIXABLE VIOLATIONS ===\n")

            print(f"❌ sqlfluff fix failed and there are unresolved violations. See {lint_json_path} for details. Formatting will stop for this file.")

            sys.exit(1)

 

def add_noqa_for_unresolvable(filepath: Path, exclude_rules=None, input_root: Path = None, pass_num: int = 1):

    """Run sqlfluff lint, add noqa comments for unresolvable violations, and return list of added comments."""

    cmd = [

        "sqlfluff", "lint", str(filepath),

        "--dialect", SQL_DIALECT,

        "--config", SQLFLUFF_CONFIG,

        "--format", "json"

    ]

    if exclude_rules:

        cmd += ["--exclude-rules", ",".join(exclude_rules)]

    noqa_comments = []

    try:

        result = subprocess.run(cmd, check=True, capture_output=True, text=True)

        lint_json = json.loads(result.stdout)

        # Save lint JSON to audit folder mirroring SQL file's relative path

        if input_root is not None:

            save_lint_json_to_audit(lint_json, filepath, input_root, pass_num=pass_num)

    except Exception as e:

        print(f"⚠️ sqlfluff lint failed for {filepath.name}: {e}")

        if hasattr(e, 'stdout') and e.stdout:

            try:

                lint_json = json.loads(e.stdout)

                if input_root is not None:

                    save_lint_json_to_audit(lint_json, filepath, input_root, pass_num=pass_num)

            except Exception as parse_ex:

                print(f"Could not parse partial lint output: {parse_ex}")

                return []

        else:

            return []

    # Find all unresolved violations (fixes == [])

    violations = []

    for file_result in lint_json:

        for v in file_result.get("violations", []):

            if isinstance(v.get("fixes", None), list) and len(v["fixes"]) == 0:

                line_no = v.get("start_line_no") or v.get("line_no")

                if line_no is not None:

                    violations.append({

                        "line_no": line_no,

                        "code": v["code"]

                    })

    if not violations:

        return []

    # Add noqa comments to the relevant lines

    sql_lines = filepath.read_text(encoding='utf-8').splitlines()

    for v in violations:

        idx = v["line_no"] - 1

        noqa_comment = f"-- noqa: {v['code']}"

        if idx < len(sql_lines):

            if noqa_comment not in sql_lines[idx]:

                sql_lines[idx] = sql_lines[idx].rstrip() + f"  {noqa_comment}"

                noqa_comments.append((idx, noqa_comment))

    filepath.write_text("\n".join(sql_lines) + "\n", encoding='utf-8')

    print(f"Added noqa comments for violations: {violations}")

    return noqa_comments

 

def remove_noqa_comments(sql: str) -> str:

    # Remove any -- noqa: <CODE> comments from the end of lines

    return re.sub(r"\s*-- noqa: [A-Z0-9]+", "", sql)

 

def pass1_format_sql_file(filepath: Path, input_root: Path):

    print(f"\n--- Pass 1: Processing file: {filepath} ---")

    original_sql = filepath.read_text(encoding='utf-8')

    copy_to_audit(filepath, "pre_pass1", input_root)

    if has_pass1_comment(original_sql):

        print(f"ℹ️ {filepath.name} already at pass 1 version {PASS1_VERSION}. Skipping.")

        return

    # Remove any old pass1 comment

    sql = re.sub(r"^-- sqlfluff-pass1-version: \d+\s*", "", original_sql, count=1)

    # Break logical operators and join ON

    sql = add_newlines_before_keywords(sql)

    # Write to temp file

    temp_path = filepath.with_suffix('.pass1.tmp.sql')

    temp_path.write_text(sql, encoding='utf-8')

    # Lint first, add noqa for unresolvable violations

    add_noqa_for_unresolvable(temp_path, exclude_rules=PASS_ONE_EXCLUDE_RULES, input_root=input_root, pass_num=1)

    # Now run sqlfluff fix (positional/case only)

    noqa_comments = run_sqlfluff_fix(temp_path, exclude_rules=PASS_ONE_EXCLUDE_RULES, add_noqa_on_fail=False, input_root=input_root, pass_num=1)

    # Read result, flatten and compare

    first_pass_sql = temp_path.read_text(encoding='utf-8')

    # Remove any -- noqa: <CODE> comments before flattening for comparison

    flat_original = flatten_sql(remove_noqa_comments(original_sql))

    flat_first = flatten_sql(remove_noqa_comments(first_pass_sql))

    if flat_original != flat_first:

        print(f"❌ First pass made text/structural changes! Not updating {filepath.name}.")

        # Show unified diff of flattened files and write to a .diff.txt file

        diff = difflib.unified_diff(

            flat_original.splitlines(),

            flat_first.splitlines(),

            fromfile='original_flattened',

            tofile='first_pass_flattened',

            lineterm=''  # Don't add extra newlines

        )

        diff_path = filepath.with_suffix('.pass1.diff.txt')

        with open(diff_path, 'w', encoding='utf-8') as f:

            for line in diff:

                f.write(line + '\n')

        print(f"--- Flattened SQL Diff written to: {diff_path} ---\n")

        temp_path.unlink()

        return

    # Insert pass1 version comment

    first_pass_sql = insert_pass1_comment(first_pass_sql)

    # Write to original file (in-place update)

    filepath.write_text(first_pass_sql, encoding='utf-8')

    # Save to audit folder

    copy_to_audit(filepath, "post_pass1", input_root)

    print(f"✅ Pass 1 complete: {filepath}")

    temp_path.unlink()

 

def find_sql_files(input_path: Path):

    if input_path.is_file() and input_path.suffix == ".sql":

        yield input_path

    elif input_path.is_dir():

        for p in input_path.rglob("*.sql"):

            yield p

 

def main():

    parser = argparse.ArgumentParser(description="Run sqlfluff pass 1 on a file or directory.")

    input_path_str = input("Enter the SQL file or directory to process: ").strip()

    input_path = Path(input_path_str).resolve()

    input_root = input_path if input_path.is_dir() else input_path.parent

    processed = []

    skipped = []

    for sql_file in find_sql_files(input_path):

        try:

            result = pass1_format_sql_file(sql_file, input_root)

            processed.append(sql_file)

        except Exception as e:

            print(f"Error processing {sql_file}: {e}")

            skipped.append(sql_file)

    print("\n=== Pass 1 Summary ===")

    print(f"Processed files: {len(processed)}")

    for f in processed:

        print(f"  - {f}")

    if skipped:

        print(f"Skipped files: {len(skipped)}")

        for f in skipped:

            print(f"  - {f}")

    print("======================\n")

 

if __name__ == "__main__":

    main()