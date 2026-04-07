"""
Search global news coverage via the GDELT Doc API (direct HTTP calls).
Uses Boolean query syntax for flexible matching — far less restrictive than
the gdeltdoc library's exact-phrase-only approach.
No API key required. Free.
Docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
"""

import time
import requests
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────

START_DATE = "2026-03-28"  # format: YYYY-MM-DD
END_DATE   = "2026-04-02"

# Boolean query: Haiti must appear, plus at least one fuel-related term.
# Parentheses and AND/OR are supported by the GDELT API directly.
QUERY = 'Haiti AND (fuel OR carburant OR gaz OR "prix energie" OR "gas prices")'

MAX_RECORDS = 250  # max per call allowed by GDELT

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"

# ── Search ────────────────────────────────────────────────────────────────────

def gdelt_search(query: str, start: str, end: str, max_records: int) -> pd.DataFrame:
    """
    Call the GDELT Doc API directly and return results as a DataFrame.
    Dates must be in YYYYMMDDHHMMSS format for the API.
    """
    params = {
        "query":         query,
        "startdatetime": start.replace("-", "") + "000000",
        "enddatetime":   end.replace("-", "") + "000000",
        "maxrecords":    max_records,
        "mode":          "artlist",
        "format":        "json",
    }

    for attempt in range(3):
        try:
            response = requests.get(GDELT_API, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            articles = data.get("articles", [])
            return pd.DataFrame(articles)
        except Exception as e:
            wait = 15 * (attempt + 1)
            print(f"  Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    print("  All attempts failed. Returning empty DataFrame.")
    return pd.DataFrame()


print(f"Searching GDELT: {QUERY}")
articles = gdelt_search(QUERY, START_DATE, END_DATE, MAX_RECORDS)

if articles.empty:
    print("No articles found for this query/date range.")
    exit()

articles = articles.drop_duplicates(subset="url").reset_index(drop=True)
print(f"\nTotal unique articles: {len(articles)}")

# ── Filter and preview ────────────────────────────────────────────────────────

# Flag French/Haitian Creole articles — higher signal for local coverage
local = articles[articles["language"].isin(["French", "Haitian Creole"])]
print(f"Local-language articles (French/Haitian Creole): {len(local)}")

print("\nAll articles:")
print(articles[["title", "seendate", "language", "sourcecountry", "url"]].to_string())
