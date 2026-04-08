"""Download all BRH quarterly statistical reports from the BRH website.

Source page: https://www.brh.ht/supervision-bancaire/rapports-statistiques-2/
Output:      data/raw/brh_trim{Q}_{YYYY}.xlsx  (or .xls for older files)

Files already present are skipped. Run again after a new report is published
to pick up the latest quarter.

Usage:
    python scripts/download_brh_reports.py
"""

import re
import time
import urllib.request
import urllib.error
from pathlib import Path

# ── Output directory ──────────────────────────────────────────────────────────

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# ── Full list of URLs scraped from the BRH website ───────────────────────────
# Source: https://www.brh.ht/supervision-bancaire/rapports-statistiques-2/
# Last scraped: April 2026.  Add new entries at the top when new reports appear.

URLS = [
    # 2026
    "https://www.brh.ht/wp-content/uploads/rap-stats-trim-1-26_-V2.xlsx",
    # 2025
    "https://www.brh.ht/wp-content/uploads/rap-stats-trim-1-25.xlsx",
    "https://www.brh.ht/wp-content/uploads/rap-stats-trim-2-25-provisoire.xlsx",
    "https://www.brh.ht/wp-content/uploads/Rap-stats-trim-3-25-PROVISOIRE.xlsx",
    "https://www.brh.ht/wp-content/uploads/rapstat-trim-4-2025.xlsx",
    # 2024
    "https://www.brh.ht/wp-content/uploads/Rapport-Statistiques-1Tri-2024.xlsx",
    "https://www.brh.ht/wp-content/uploads/Rap-Stat-Trim-2-2024.xlsx",
    "https://www.brh.ht/wp-content/uploads/rap-stats-trim-3-24.xlsx",
    "https://www.brh.ht/wp-content/uploads/rap-stats-trim-4.-24xlsx.xlsx",
    # 2023
    "https://www.brh.ht/wp-content/uploads/Trim-1-2023.xlsx",
    "https://www.brh.ht/wp-content/uploads/Rapport2tri2023.xlsx",
    "https://www.brh.ht/wp-content/uploads/rapstat-trim-3-2023.xlsx",
    "https://www.brh.ht/wp-content/uploads/Rapstat_Trim-4_2023.xlsx",
    # 2022
    "https://www.brh.ht/wp-content/uploads/trim_1_2022.xlsx",
    "https://www.brh.ht/wp-content/uploads/Trim_2_2022.xlsx",
    "https://www.brh.ht/wp-content/uploads/Trim-3-2022.xlsx",
    "https://www.brh.ht/wp-content/uploads/Trim-4-2022.xlsx",
    # 2021
    "https://www.brh.ht/wp-content/uploads/trim_1_2021.xlsx",
    "https://www.brh.ht/wp-content/uploads/trim_2_2021.xlsx",
    "https://www.brh.ht/wp-content/uploads/trim_3_2021.xlsx",
    "https://www.brh.ht/wp-content/uploads/trim_4_2021.xlsx",
    # 2020
    "https://www.brh.ht/wp-content/uploads/trim_1_2020.xls",
    "https://www.brh.ht/wp-content/uploads/trim_2_2020.xlsx",
    "https://www.brh.ht/wp-content/uploads/trim_3_2020.xlsx",
    "https://www.brh.ht/wp-content/uploads/trim_4_2020.xlsx",
    # 2019
    "https://www.brh.ht/wp-content/uploads/trim_1_2019.xls",
    "https://www.brh.ht/wp-content/uploads/trim_2_2019.xls",
    "https://www.brh.ht/wp-content/uploads/trim_3_2019.xls",
    "https://www.brh.ht/wp-content/uploads/trim_4_2019.xlsx",
    # 2018
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2018.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2018.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2018.xls",
    "https://www.brh.ht/wp-content/uploads/trim_4_2018.xls",
    # 2017
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2017.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2017.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2017.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2017.xls",
    # 2016
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2016.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2016.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2016.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2016.xls",
    # 2015
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2015.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2015.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2015.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2015.xls",
    # 2014
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2014.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2014.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2014.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2014.xls",
    # 2013
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2013.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2013.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2013.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2013.xls",
    # 2012
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2012.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2012.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2012.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2012.xls",
    # 2011
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2011.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2011.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2011.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2011.xls",
    # 2010
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2010.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2010.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2010.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2010.xls",
    # 2009
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2009.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2009.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2009.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2009.xls",
    # 2008
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2008.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2008.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2008.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2008.xls",
    # 2007
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2007.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2007.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2007.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2007.xls",
    # 2006
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2006.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2006.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2006.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2006.xls",
    # 2005
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2005.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2005.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2005.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2005.xls",
    # 2004
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2004.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2004.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2004.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2004.xls",
    # 2003
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2003.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2003.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2003.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2003.xls",
    # 2002
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2002.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2002.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2002.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2002.xls",
    # 2001
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2001.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2001.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2001.xls",
    # 2000
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_1_2000.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_2_2000.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_3_2000.xls",
    "https://www.brh.ht/wp-content/uploads/2018/08/trim_4_2000.xls",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_quarter_year(url: str) -> tuple[int, int] | None:
    """Extract (quarter, year) from a BRH report URL.

    Handles patterns like:
      trim_1_2019, trim-1-25, rap-stats-trim-1-25,
      Rapport-Statistiques-1Tri-2024, Rap-Stat-Trim-2-2024
    """
    name = url.split("/")[-1].lower()

    # Pattern A: trim followed by Q and year  (e.g. trim_1_2019, trim-1-25, trim-4-2025)
    m = re.search(r"trim[-_. ]?(\d)[-_. ]+(\d{2,4})", name)
    if m:
        q = int(m.group(1))
        yr = int(m.group(2))
        if yr < 100:
            yr += 2000
        return q, yr

    # Pattern B: Qtri-YYYY or QtriYYYY  (e.g. 1tri-2024, 2tri2023)
    m = re.search(r"(\d)tri[-_]?(\d{4})", name)
    if m:
        return int(m.group(1)), int(m.group(2))

    return None


def local_filename(url: str) -> str:
    """Return a clean, normalised local filename like brh_trim1_2025.xlsx."""
    ext = ".xls" if url.endswith(".xls") else ".xlsx"
    qy = parse_quarter_year(url)
    if qy:
        q, yr = qy
        return f"brh_trim{q}_{yr}{ext}"
    # Fallback: use the original filename from the URL
    return url.split("/")[-1]


def download(url: str, dest: Path, timeout: int = 60) -> str:
    """Download url to dest. Returns 'downloaded', 'skipped', or 'error: ...'."""
    if dest.exists():
        return "skipped"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; BRH-downloader/1.0)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            dest.write_bytes(resp.read())
        return "downloaded"
    except urllib.error.HTTPError as e:
        return f"error: HTTP {e.code}"
    except urllib.error.URLError as e:
        return f"error: {e.reason}"
    except Exception as e:
        return f"error: {e}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    total      = len(URLS)
    downloaded = 0
    skipped    = 0
    errors     = []

    print(f"Downloading {total} BRH quarterly reports -> {RAW_DIR}\n")

    for i, url in enumerate(URLS, 1):
        filename = local_filename(url)
        dest     = RAW_DIR / filename
        status   = download(url, dest)

        tag = {
            "downloaded": "OK  ",
            "skipped":    "SKIP",
        }.get(status, "ERR ")

        print(f"  [{i:>3}/{total}] {tag} {filename:<35} {status if tag == 'ERR ' else ''}")

        if status == "downloaded":
            downloaded += 1
            time.sleep(0.5)   # be polite to the server
        elif status == "skipped":
            skipped += 1
        else:
            errors.append((filename, status))

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped, {len(errors)} errors.")

    if errors:
        print("\nFailed files:")
        for fname, err in errors:
            print(f"  {fname}: {err}")


if __name__ == "__main__":
    main()
