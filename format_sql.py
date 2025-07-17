import json
import subprocess
from pathlib import Path
import re

debug = True  # Set to True to enable debug prints

def get_rf02_violations(filepath: Path):
    print(f"\nRunning sqlfluff lint on: {filepath}")
    result = subprocess.run(
        ["sqlfluff", "lint", str(filepath), "--format", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if debug:
        print("=== STDOUT ===")
        print(result.stdout[:1000])
        print("=== STDERR ===")
        print(result.stderr.strip())

    if result.returncode != 0 and not result.stdout.strip():
        print("SQLFluff exited with error and no output.")
        return []

    try:
        result_json = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("ERROR: Could not parse JSON from SQLFluff output.")
        return []

    if not result_json:
        print("No files returned by lint.")
        return []

    violations_list = result_json[0].get("violations", [])
    if not violations_list:
        print("No violations found.")
        return []

    fields = set()
    for v in violations_list:
        if debug:
            print(f"Violation found: Code={v['code']}, Description={v['description']}")
        if v["code"] == "RF02":
            match = re.search(r"'([^']+)'", v["description"])
            if match:
                field_name = match.group(1)
                if debug:
                    print(f"  ↳ Extracted unqualified field: {field_name}")
                fields.add(field_name)
            else:
                print(f"  ⚠️ WARNING: Could not extract field from: {v['description']}")

    return sorted(fields)

def qualify_fields_in_sql(sql: str, fields: list) -> str:
    for field in fields:
        pattern = rf'\b(?<!\.)({re.escape(field)})\b'
        replacement = r'requires_table_reference.\1'
        sql, count = re.subn(pattern, replacement, sql)
        if count:
            print(f"✔️ Replaced {count} occurrence(s) of '{field}' with 'requires_table_reference.{field}'")
    return sql

def break_logical_operators(sql: str) -> str:
    # Add newline before AND/OR if not already at line start
    updated_sql = re.sub(r'\s+(AND|OR)\b', r'\n\1', sql, flags=re.IGNORECASE)
    print("✔️ Inserted newlines before logical operators (AND/OR)")
    return updated_sql

def break_join_on(sql: str) -> str:
    # Add newline before AND/OR if not already at line start
    updated_sql = re.sub(r'\s+(ON)\b', r'\n\1', sql, flags=re.IGNORECASE)
    print("✔️ Inserted newlines before join conditions (ON)")
    return updated_sql

def format_sql_file(filepath: Path):
    print(f"\n--- Processing file: {filepath} ---")
    original_sql = filepath.read_text(encoding='utf-8')

    if "requires_table_reference." in original_sql:
        print("ℹ️ Skipping RF02 qualification — placeholder already present.")
        qualified_sql = original_sql
    else:
        unqualified_fields = get_rf02_violations(filepath)
        if not unqualified_fields:
            print("❌ No unqualified fields found or unable to parse SQL.")
            return
        print(f"Fields to qualify: {unqualified_fields}")
        qualified_sql = qualify_fields_in_sql(original_sql, unqualified_fields)

    # 🧠 Insert newline before AND/OR
    sql_with_line_breaks = break_logical_operators(qualified_sql)

    # 🧠 Insert newline before ON in JOIN condition
    sql_with_line_breaks = break_join_on(sql_with_line_breaks)

    temp_path = filepath.with_suffix('.tmp.sql')
    temp_path.write_text(sql_with_line_breaks, encoding='utf-8')

    print(f"\n🧼 Running sqlfluff fix on temp file: {temp_path}")
    subprocess.run([
        "sqlfluff", "fix", str(temp_path),
        "--dialect", "snowflake",
        "--config", ".sqlfluff"
    ])

    formatted_sql = temp_path.read_text(encoding='utf-8')
    filepath.write_text(formatted_sql, encoding='utf-8')
    temp_path.unlink()

    print(f"✅ Updated and formatted: {filepath}\n")

# Run for all .sql files in the target directory
sql_dir = Path("sql_requiring_formatting")
for sql_file in sql_dir.glob("*.sql"):
    format_sql_file(sql_file)
