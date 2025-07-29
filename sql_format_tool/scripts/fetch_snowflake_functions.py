import requests
import json
from bs4 import BeautifulSoup

URL = "https://docs.snowflake.com/en/sql-reference/functions-all"

output_path = r"sql_format_tool\resources\snowflake_functions.json"

def scrape_snowflake_functions():
    resp = requests.get(URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find functions table in the page")

    rows = table.find_all("tr")
    funcs = []
    headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

    name_idx = headers.index("Function Name")
    summary_idx = headers.index("Summary")
    category_idx = headers.index("Category")

    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        name = tds[name_idx].get_text(strip=True)
        summary = tds[summary_idx].get_text(strip=True)
        category = tds[category_idx].get_text(strip=True)
        # Skip rows that have name only like alphabet headers
        if not summary and not category:
            continue
        funcs.append({
            "name": name.upper(),
            "summary": summary,
            "category": category
        })

    return funcs

def main():
    funcs = scrape_snowflake_functions()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(funcs, f, indent=2)
    print(f"Fetched {len(funcs)} functions. Output saved to snowflake_functions.json")

if __name__ == "__main__":
    main()
