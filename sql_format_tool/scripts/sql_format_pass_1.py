import json
import subprocess
from pathlib import Path
import re
import difflib
import shutil
import argparse
from datetime import datetime
from collections import defaultdict

# ========== CONFIG ==========
DEBUG = False
PASS1_VERSION = "1.0"
PASS1_MARKER_PREFIX = "-- sqlfluff-pass1-version: "
AUDIT_ROOT = Path("sql_format_tool/audit_folder")
AUDIT_COUNTER = {"count": 1}  # global counter for audit file ordering
SUMMARY_REPORT = []
SKIPPED_FILES = []
SQLFLUFF_CONFIG = Path("sql_format_tool/scripts/.sqlfluff")

# ========== HELPER FUNCTIONS ==========

def debug_print(*args):
    if DEBUG:
        print("[DEBUG]", *args)

def flatten_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip()).replace('"', '').replace("'", "")

def add_newlines_for_keywords(sql: str) -> str:
    keywords = [
        "SELECT", "FROM", "WHERE", "JOIN", "AND", "OR", "ON", "GROUP BY", "ORDER BY",
        "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN", "FULL JOIN", "LIMIT",
    ]
    for kw in sorted(keywords, key=len, reverse=True):
        sql = re.sub(rf"(?<!\n)\b({kw})\b", r"\n\1", sql, flags=re.IGNORECASE)
    return sql

def add_newlines_before_commas(sql: str) -> str:
    return re.sub(r"\s*,", r"\n,", sql)

def make_audit_path(sql_path: Path, mirror: bool = True) -> Path:
    if not mirror:
        return AUDIT_ROOT
    rel_parts = sql_path.with_suffix("").parts
    return AUDIT_ROOT.joinpath(*rel_parts)

def run_sqlfluff_lint(filepath: Path, audit_path: Path) -> dict:
    result = subprocess.run([
        "sqlfluff", "lint", str(filepath), "--format", "json",
        "--dialect", "snowflake", "--config", str(SQLFLUFF_CONFIG)
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        audit_path.mkdir(parents=True, exist_ok=True)
        lint_output_path = audit_path / f"{filepath.stem}.pass1_{AUDIT_COUNTER['count']:02d}_lint.json"
        lint_output_path.write_text(result.stdout, encoding='utf-8')
        AUDIT_COUNTER["count"] += 1
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        debug_print("Failed to parse lint JSON.")
        return {}

def run_sqlfluff_fix(filepath: Path):
    subprocess.run([
        "sqlfluff", "fix", str(filepath), "--dialect", "snowflake", "--config", str(SQLFLUFF_CONFIG)
    ])

def get_existing_pass1_version(sql: str) -> str:
    match = re.search(rf"{re.escape(PASS1_MARKER_PREFIX)}(\d+(?:\.\d+)*)", sql)
    return match.group(1) if match else None

def version_to_tuple(version: str):
    return tuple(map(int, version.split('.')))

def should_skip_formatting(sql: str, filepath: Path) -> bool:
    existing_version = get_existing_pass1_version(sql)
    if not existing_version:
        return False
    skip = version_to_tuple(existing_version) >= version_to_tuple(PASS1_VERSION)
    if skip:
        SKIPPED_FILES.append(str(filepath))
    return skip

def audit_copy(file_path: Path, stage: str, audit_path: Path):
    audit_path.mkdir(parents=True, exist_ok=True)
    count = AUDIT_COUNTER["count"]
    new_name = f"{file_path.stem}.pass1_{count:02d}_{stage}{file_path.suffix}"
    shutil.copy(file_path, audit_path / new_name)
    AUDIT_COUNTER["count"] += 1

def diff_summary(before: str, after: str):
    diff = difflib.unified_diff(
        before.splitlines(), after.splitlines(), lineterm=""
    )
    return "\n".join(diff)

def add_noqa_comments(sql: str, lint_results: dict, filepath: Path) -> str:
    lines = sql.splitlines()
    file_summary = defaultdict(list)
    for file_result in lint_results:
        for violation in file_result.get("violations", []):
            if isinstance(violation.get("fixes", None), list) and len(violation["fixes"]) == 0:
                line = violation.get("start_line_no") or violation.get("line_no")
                code = violation["code"]
                if line is not None and line - 1 < len(lines):
                    match = re.search(r"-- noqa: (.+)", lines[line - 1])
                    if match:
                        existing_codes = set(code.strip() for code in match.group(1).split(","))
                        existing_codes.add(code)
                        new_comment = "-- noqa: " + ", ".join(sorted(existing_codes))
                        lines[line - 1] = re.sub(r"-- noqa: .+", new_comment, lines[line - 1])
                    else:
                        lines[line - 1] += f"  -- noqa: {code}"
                    file_summary[line].append(code)
    if file_summary:
        SUMMARY_REPORT.append({"file": str(filepath), "unfixables": dict(file_summary)})
    return "\n".join(lines)

def remove_noqa_comments(sql: str) -> str:
    return re.sub(r"\s*-- noqa: .+", "", sql)

def write_summary_report():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = AUDIT_ROOT / f"pass1_summary_{timestamp}.txt"
    with report_path.open("w", encoding="utf-8") as f:
        if SUMMARY_REPORT:
            for entry in SUMMARY_REPORT:
                f.write(f"File: {entry['file']}\n")
                for line_no, codes in entry["unfixables"].items():
                    code_list = ", ".join(codes)
                    f.write(f"  Line {line_no}: {code_list}\n")
                f.write("\n")
        if SKIPPED_FILES:
            f.write("Skipped Files (already formatted):\n")
            for file in SKIPPED_FILES:
                f.write(f"  {file}\n")
            f.write("\n")
    print(f"📝 Summary report written to: {report_path}")

# ========== MAIN FUNCTION ==========

def pass1_format_sql_file(filepath: Path, mirror: bool):
    print(f"\n--- Processing file: {filepath} ---")

    original_sql = filepath.read_text(encoding='utf-8')
    if should_skip_formatting(original_sql, filepath):
        print("⏭️  Already formatted with current or newer Pass1 version. Skipping.")
        return

    audit_path = make_audit_path(filepath, mirror)

    staged_sql = add_newlines_for_keywords(original_sql)
    staged_sql = add_newlines_before_commas(staged_sql)

    temp_path = filepath.with_suffix('.tmp.sql')
    temp_path.write_text(staged_sql, encoding='utf-8')
    audit_copy(temp_path, "pre_format", audit_path)

    lint_data = run_sqlfluff_lint(temp_path, audit_path)
    if lint_data:
        staged_sql = add_noqa_comments(staged_sql, lint_data, filepath)
        temp_path.write_text(staged_sql, encoding='utf-8')

    run_sqlfluff_fix(temp_path)

    formatted_sql = temp_path.read_text(encoding='utf-8')
    audit_copy(temp_path, "post_format", audit_path)
    temp_path.unlink()

    if flatten_sql(original_sql) != flatten_sql(remove_noqa_comments(formatted_sql)):
        print("❌ Unexpected structural changes. Review required.")
        diff = diff_summary(original_sql, formatted_sql)
        diff_file = audit_path / f"{filepath.stem}.pass1_diff.txt"
        diff_file.write_text(diff, encoding='utf-8')

        # NEW: Write side-by-side flattened pre and post lines
        flat_compare_file = audit_path / f"{filepath.stem}.pass1_flat_compare.txt"
        flat_before = flatten_sql(original_sql)
        flat_after = flatten_sql(remove_noqa_comments(formatted_sql))
        flat_compare_file.write_text(f"{flat_before}\n{flat_after}\n", encoding='utf-8')

        print(f"🔍 Diff written to: {diff_file.name}")
        print(f"🔍 Flattened comparison written to: {flat_compare_file.name}")
        return

    final_output = f"{PASS1_MARKER_PREFIX}{PASS1_VERSION}\n\n{formatted_sql}"
    filepath.write_text(final_output, encoding='utf-8')
    print("✅ Formatted successfully.")

# ========== ENTRY POINT ==========

def main():
    parser = argparse.ArgumentParser(description="SQLFluff Pass 1 Formatter")
    parser.add_argument("--path", type=str, required=True, help="Path to SQL file or directory")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--flat", action="store_true", help="Do not mirror folder structure in audit output")
    args = parser.parse_args()

    global DEBUG
    DEBUG = args.debug

    path = Path(args.path)
    mirror = not args.flat

    sql_files = []
    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files = [path]
    elif path.is_dir():
        sql_files = list(path.rglob("*.sql"))
    else:
        print("❌ Invalid path. Must be .sql file or directory.")
        return

    for file in sql_files:
        pass1_format_sql_file(file, mirror)

    write_summary_report()

if __name__ == "__main__":
    main()
