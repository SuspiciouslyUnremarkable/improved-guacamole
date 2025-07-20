
import os
import json
import shutil
import subprocess
import argparse

from pathlib import Path

def run_sqlfluff_lint(file_path, config_path):
    try:
        result = subprocess.run(
            ["sqlfluff", "lint", file_path, "--format", "json", "--config", config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False
        )
        if result.stderr:
            print(f"[ERROR] sqlfluff lint error for {file_path}: {result.stderr.strip()}")
        return json.loads(result.stdout) if result.stdout.strip().startswith("[") else []
    except Exception as e:
        print(f"[EXCEPTION] Failed to lint {file_path}: {str(e)}")
        return []

def run_sqlfluff_fix(file_path, config_path, dry_run=False):
    command = ["sqlfluff", "fix", file_path, "--force", "--config", config_path]
    if dry_run:
        command.append("--dry-run")
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.stderr:
            print(f"[ERROR] sqlfluff fix error for {file_path}: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"[EXCEPTION] Failed to fix {file_path}: {str(e)}")
        return False

def add_noqa_comments(file_path, violations):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for violation in violations:
            line_no = violation.get("line_no")
            code = violation.get("code")
            if not line_no or not code:
                continue
            idx = line_no - 1
            if idx < len(lines):
                if "-- noqa:" in lines[idx]:
                    lines[idx] = lines[idx].strip() + f",{code}\n"
                else:
                    lines[idx] = lines[idx].rstrip("\n") + f" -- noqa: {code}\n"

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        return True
    except Exception as e:
        print(f"[EXCEPTION] Failed to insert noqa comments in {file_path}: {str(e)}")
        return False

def flatten_sql(sql_text):
    return " ".join(sql_text.split())

def save_audit_files(base_dir, file_path, pre_sql, post_sql, lint_json):
    rel_path = Path(file_path).with_suffix("")
    subdir = Path("sql_format_tool/audit_folder") / rel_path
    os.makedirs(subdir, exist_ok=True)

    file_stem = Path(file_path).stem

    with open(subdir / f"{file_stem}_pass2_01_pre_format.sql", "w", encoding="utf-8") as f:
        f.write(pre_sql)

    with open(subdir / f"{file_stem}_pass2_02_lint.json", "w", encoding="utf-8") as f:
        json.dump(lint_json, f, indent=2)

    with open(subdir / f"{file_stem}_pass2_03_post_format.sql", "w", encoding="utf-8") as f:
        f.write(post_sql)

def process_file(file_path, config_path, dry_run=False):
    summary_entry = {"file": file_path, "status": "skipped", "noqa_added": []}

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            original_sql = f.read()

        lint_result = run_sqlfluff_lint(file_path, config_path)
        if not lint_result or "violations" not in lint_result[0]:
            print(f"[SKIPPED] {file_path} - no lint result or failed lint.")
            return summary_entry

        violations = lint_result[0]["violations"]
        unfixables = [v for v in violations if not v.get("fixes")]

        if unfixables:
            summary_entry["status"] = "noqa-added"
            summary_entry["noqa_added"] = [f"L{v['line_no']}: {v['code']}" for v in unfixables]
            add_noqa_comments(file_path, unfixables)

        run_sqlfluff_fix(file_path, config_path, dry_run=dry_run)

        with open(file_path, "r", encoding="utf-8") as f:
            final_sql = f.read()

        save_audit_files("sql_format_tool/audit_folder", file_path, original_sql, final_sql, lint_result)
        summary_entry["status"] = "processed"
    except Exception as e:
        print(f"[ERROR] Failed to process {file_path}: {str(e)}")

    return summary_entry

def process_directory(path, config_path, dry_run=False):
    summary = []

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                print(f"[PROCESSING] {file_path}")
                result = process_file(file_path, config_path, dry_run=dry_run)
                summary.append(result)
            else:
                print(f"[SKIPPED] {file} (not .sql)")

    summary_path = Path("sql_format_tool/audit_folder/summary_pass2.txt")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        for entry in summary:
            f.write(f"{entry['file']} - {entry['status']}\n")
            for note in entry.get("noqa_added", []):
                f.write(f"    {note}\n")

def main():
    parser = argparse.ArgumentParser(description="SQL Format Pass 2 using sqlfluff")
    parser.add_argument("path", help="Path to a file or directory to process")
    parser.add_argument("--config", default="sql_format_tool/scripts/.sqlfluff", help="Path to .sqlfluff config")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no actual fixes applied)")
    args = parser.parse_args()

    if os.path.isfile(args.path):
        result = process_file(args.path, args.config, dry_run=args.dry_run)
        print(f"{result['file']} - {result['status']}")
    elif os.path.isdir(args.path):
        process_directory(args.path, args.config, dry_run=args.dry_run)
    else:
        print(f"Invalid path: {args.path}")

if __name__ == "__main__":
    main()
