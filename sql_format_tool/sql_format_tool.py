import sys
import argparse
import gc
import re
import json
from typing import List, Union, Tuple
from pathlib import Path


audit_folder = Path("sql_format_tool/audit_folder").resolve()
dbt_project_dir = Path("dbt").resolve()
sqlfluff_config_path = Path("sql_format_tool/resources/.sqlfluff").resolve()



def flatten_whitespace(sql: str) -> str:
    """
    Replace all whitespace characters (spaces, tabs, newlines) with a single space.
    effectively flattens the SQL string into a single line.
    
    Args:
        sql (str): The input SQL string.
        
    Returns:
        str: The SQL string with flattened whitespace.
    """

    # Replace all whitespace characters with a single space
    flattened = re.sub(r'\s+', ' ', sql)
    
    # Remove spaces before commas and semicolons
    flattened = re.sub(r'\s+([,;])', r'\1', flattened)

    # Add space after commas if not present
    flattened = re.sub(r',(?=\S)', r', ', flattened)

    return flattened.strip()


def write_stage(audit_path: Path, stage: int, name: str, content: str) -> int:
    (audit_path / f"stage_{stage:02d}_{name}.sql").write_text(content, encoding="utf-8")
    return stage + 1


def convert_single_to_block_comments(sql: str) -> str:
    """
    Convert single-line comments (--) to block comments (/* */) in the SQL string.
    
    Args:
        sql (str): The input SQL string.

    Returns:
        str: The SQL string with single-line comments converted to block comments.
    """

    return re.sub(r'--(.*?)(\n|$)', r'/*\1 */\2', sql)

def move_inline_block_comments(sql: str) -> str:
    """
    Move inline block comments (/* ... */) to their own line.
    If a block comment is found on the same line as SQL code, it is moved to the line above.

    Args:
        sql (str): The input SQL string.
        
    Returns:
        str: The SQL string with inline block comments moved above the statements.
    """

    def replacer(match):
        before = match.group(1).rstrip()
        comment = match.group(2)
        after = match.group(3).lstrip()
        if before and after:
            return f"{before}\n{comment}\n{after}"
        else:
            return match.group(0)

    pattern = re.compile(r'([^\n]*?)(/\*.*?\*/)([^\n]*?)', re.DOTALL)
    return pattern.sub(replacer, sql)


def extract_non_jinja_placeholders(sql: str, block_comments_only: bool = False) -> Tuple[str, dict[str, str]]:
    """
    Extract non-Jinja placeholders from the SQL string and replace them with unique tokens.
    Placeholders can be in the form of :placeholder or /* placeholder */.

    Args:
        sql (str): The input SQL string.
        block_comments_only (bool): If True, only extract placeholders in block comments.   

    Returns:
        Tuple[str, dict]: A tuple containing the modified SQL string and a dictionary mapping tokens to original placeholders.
    """

    replacements = {}
    placeholder_counter = 0
    if block_comments_only:
        patterns = [
            (r"/\*.*?\*/", "SQL_BLOCK_COMMENT"),
        ]
    else:
        patterns = [
            (r"--[^\n]*", "SQL_COMMENT"),
            (r"/\*.*?\*/", "SQL_BLOCK_COMMENT"),
            (r"'(?:''|[^'])*'", "SINGLE_QUOTE_STRING"),
            (r'"(?:[^"]|"")*"', "DOUBLE_QUOTE_STRING"),
        ]

    combined_pattern = "|".join(f"({p[0]})" for p in patterns)
    regex = re.compile(combined_pattern, re.DOTALL)
    def replacement_function(match):
        nonlocal placeholder_counter
        idx = match.lastindex - 1
        label = patterns[idx][1]
        full = match.group(0)
        key = f"__{label}_{placeholder_counter:04d}__"
        placeholder_counter += 1

        # For quoted strings, keep the quotes in the replacement
        if label == "SINGLE_QUOTE_STRING" and len(full) >= 2 and full[0] == "'" and full[-1] == "'":
            replacements[key] = full[1:-1]
            return f"'{key}'"
        elif label == "DOUBLE_QUOTE_STRING" and len(full) >= 2 and full[0] == '"' and full[-1] == '"':
            replacements[key] = full[1:-1]
            return f'"{key}"'
        # For comments, keep the border markers
        elif label == "SQL_COMMENT" and full.startswith("--"):
            replacements[key] = full[2:].strip()
            return f"--{key}"
        elif label == "SQL_BLOCK_COMMENT" and full.startswith("/*") and full.endswith("*/"):
            replacements[key] = full[2:-2].strip()
            return f"/*{key}*/"
        else:
            replacements[key] = full
            return key
    modified_sql = regex.sub(replacement_function, sql)
    return modified_sql, replacements


def extract_jinja_placeholders(sql: str) -> Tuple[str, dict[str, str]]:
    """
    Extract Jinja placeholders from the SQL string and replace them with unique tokens.
    Placeholders can be in the form of {{ ... }} or {% ... %}.

    Args:
        sql (str): The input SQL string.

    Returns:
        Tuple[str, dict]: A tuple containing the modified SQL string and a dictionary mapping tokens to original placeholders.
    """

    replacements = {}
    placeholder_counter = 0
    patterns = [
        (r"\{\{.*?\}\}", "JINJA_EXPRESSION"),
        (r"\{%.*?%\}", "JINJA_STATEMENT"),
        (r"\{#.*?#\}", "JINJA_COMMENT"),
    ]

    combined_pattern = "|".join(f"({p[0]})" for p in patterns)
    regex = re.compile(combined_pattern, re.DOTALL)
    def replacement_function(match):
        nonlocal placeholder_counter
        idx = match.lastindex - 1
        label = patterns[idx][1]
        full = match.group(0)
        key = f"__{label}_{placeholder_counter:04d}__"
        placeholder_counter += 1
        replacements[key] = full
        return key
    modified_sql = regex.sub(replacement_function, sql)
    return modified_sql, replacements


def restore_placeholders(sql: str, replacements: dict[str, str]) -> str:
    """
    Restore placeholders in the SQL string from the replacements dictionary.

    Args:
        sql (str): The input SQL string with tokens.
        replacements (dict): A dictionary mapping tokens to original placeholders.

    Returns:
        str: The SQL string with placeholders restored.
    """

    for key, value in replacements.items():
        sql = sql.replace(key, value)
    return sql


def run_sqlfluff_lint(sql_file_path: Path, config_path: Path) -> str:
    """
    Run sqlfluff linting on the given SQL file using the specified configuration.

    Args:
        sql_file_path (Path): The path to the SQL file to lint.
        config_path (Path): The path to the sqlfluff configuration file.

    Returns:
        str: The output from the sqlfluff lint command.
    """

    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    
    sql_file_path_posix = sql_file_path.resolve().as_posix()
    sqlfluff_config_path_posix = config_path.resolve().as_posix()

    stem = sql_file_path.stem
    if stem.endswith("_lint"):
        stem = stem[:-5]

    audit_path = audit_folder / stem

    # Base command
    cmd = ["sqlfluff", "lint", sql_file_path_posix, "--config", sqlfluff_config_path_posix, "--format", "json"]


    
    import subprocess

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode not in (0, 1, 65):  # 0: no issues, 65: linting issues found
        # Write stdout and stderr to files for debugging
        (audit_path / "sqlfluff_stdout.txt").write_text(result.stdout, encoding="utf-8")
        (audit_path / "sqlfluff_stderr.txt").write_text(result.stderr, encoding="utf-8")
        raise RuntimeError(f"sqlfluff linting failed: {result.stderr}")

    try:
        lint_result = json.loads(result.stdout)
    except json.JSONDecodeError:
        # Write stdout and stderr to files for debugging
        (audit_path / "sqlfluff_stdout.txt").write_text(result.stdout, encoding="utf-8")
        (audit_path / "sqlfluff_stderr.txt").write_text(result.stderr, encoding="utf-8")
        raise

    return result.returncode, lint_result


def format_using_lint(lint_json: Union[dict, list], sql: str) -> Tuple[str, List[Tuple[int, int, str, str]]]:
    """
    Format the SQL string based on the sqlfluff linting results.

    Args:
        lint_json (dict or list): The JSON output from sqlfluff linting.
        sql (str): The input SQL string.

    Returns:
        Tuple[str, List[Tuple[int, int, str, str]]]: A tuple containing the formatted SQL string and a list of violations.
    """

    violations = []
    if isinstance(lint_json, list):
        for file_result in lint_json:
            violations.extend(file_result.get("violations", []))
    elif isinstance(lint_json, dict):
        violation = lint_json.get("violations", [])


    applied_fixes = []
    unfixed_violations = []

    for violation in file_result.get("violations", []):
        start_file_pos = violation.get("start_file_pos", {})
        end_file_pos = violation.get("end_file_pos")
        code = violation.get("code")

        if code in ["RF03"]:
            unfixed_violations.append(start_file_pos)
            continue
            
        if code in ["CP01", "CP03"]:
            for fix in violation.get("fixes", []):
                fix_type = fix.get("fix_type")
                if fix_type == "replace":
                    edit = fix.get("edit", '')
                    content = fix.get("content", "")
                    applied_fixes.append((start_file_pos, end_file_pos, edit, code))
                    sql_content = sql_content[:start_file_pos] + edit + sql_content[end_file_pos:]

    
    # Make unfixable violations distinct
    unfixed_violations_distinct = list(set(unfixed_violations))
    # Insert 'MISSING_REFERENCE' flags to highlight unfixable violations
    for pos in sorted(unfixed_violations_distinct, reverse=true):
        sql_content = sql_content[:pos] + "MISSING_REFERENCE." + sql_content[pos:]


    return sql_content, applied_fixes


def newline_keywords(sql: str, keywords: List[str]) -> str:
    """
    Add newlines before specified SQL keywords.

    Args:
        sql (str): The input SQL string.
        keywords (List[str]): A list of SQL keywords to add newlines before.

    Returns:
        str: The SQL string with newlines added before specified keywords.
    """

    exclude_list = [
        'AS', 'IS', 'IN', 'BETWEEN', 'ASC', 'DESC'
        , 'LIKE', 'ILIKE', 'RLIKE', 'NOT'
        , 'NS', 'NANOSECOND', 'NANOSECONDS'
        , 'US', 'MICROSECOND', 'MICROSECONDS'
        , 'MS', 'MILLISECOND', 'MILLISECONDS'
        , 'S', 'SS', 'SEC', 'SECOND', 'SECONDS'
        , 'MI', 'MIN', 'MINUTE', 'MINUTES'
        , 'H', 'HR', 'HOUR', 'HOURS'
        , 'D', 'DAY', 'DAYS'
        , 'W', 'WK', 'WEEK', 'WEEKS'
        , 'MON', 'MONTH', 'MONTHS'
        , 'Q', 'QUARTER', 'QUARTERS'
        , 'Y', 'YR', 'YEAR', 'YEARS'
        , 'EPOCH', 'DOY', 'DOW', 'CENTURY', 'DECADE'
    ]
    include_combos = [("CASE", "WHEN")]

    pos = 0
    prev_keyword = None
    for _, _, edit, code in keywords:
        if code == "CP01":
            idx = sql.find(edit, pos)
            if idx == -1:
                # Find the word immediately before the current keyword
                before = sql[:idx].rstrip()
                # Get the last word before the keyword
                import re
                match = re.search(r'(\b\w+\b)\s*$', before)
                last_word = match.group(1).upper() if match else None
                # Only add newline if last word is not excluded and either (last_word != prev_keyword) or (last_word, edit) in include_combos
                allow_newline = False
                if edit not in exclude_list:
                    if last_word != prev_keyword:
                        allow_newline = True
                    elif prev_keyword is not None and (last_word, edit) in include_combos:
                        allow_newline = True

                if allow_newline:
                    if idx > 0 and sql[idx - 1] != '\n':
                        sql = sql[:idx] + '\n' + sql[idx:]
                        pos = idx + len(edit) + 1

                pos = idx + len(edit)
                prev_keyword = edit
            else:
                continue
    return sql


def newline_after_keywords(sql: str) -> str:
    """
    Add newlines after certain SQL keywords.

    Args:
        sql (str): The input SQL string.

    Returns:
        str: The SQL string with newlines added after certain keywords.
    """

    include_list = ['WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'QUALIFY', 'LIMIT', 'PARTITION BY']

    for keyword in include_list:
        pattern = re.compile(r'(?i)\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
        sql = pattern.sub(lambda m: m.group(0) + '\n', sql)


    lines = sql.splitlines()
    output = []
    i = 0
    


def process_sql_file(file_path, debug=False):
    pass

def main():
    
    parser = argparse.ArgumentParser(description="SQL Format Tool")
    parser.add_argument("path,", nargs="?", help="Path to the SQL file or folder")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # If no arguments are provided, prompt the user for input
    if len(sys.argv) == 1:
        path = input("Enter the path to the SQL file or folder: ").strip()
        debug = input("Enable debug logging? (y/n): ").strip().lower() == 'y'
        args = parser.parse_args([path] + (["--debug"] if debug else []))
    else:
        args = parser.parse_args()

    if not path.exists():
        print(f"Error: The path '{path}' does not exist.")
        return
    
    sql_files = []
    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files.append(path)
    elif path.is_dir():
        sql_files.extend(path.rglob("*.sql"))

    if not sql_files:
        print("No SQL files found to process.")
        return
    
    for sql_file in sql_files:
        process_sql_file(sql_file, debug=args.debug)

    gc.collect()


if __name__ == "__main__":
    main()