"""
Reads the xAI search log Excel and regenerates haiti_tracker.html with
the latest data injected into the DATA LAYER section.

Run without arguments: auto-locates output/x_search_log.xlsx next to this script.
Run with argument:     python haiti_dashboard_update.py path/to/x_search_log.xlsx

Optional, for live WTI prices:
    pip install fredapi
    Set env var FRED_API_KEY (free key at https://fred.stlouisfed.org/docs/api/api_key.html)

"""

import sys
import json 
import re
import os
import unicodedata
import pandas as pd
from datetime import datetime
from pathlib import Path


# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_EXCEL = Path(__file__).parent / "output" / "x_search_log.xlsx"
TEMPLATE_PATH = Path(__file__).parent / "index.html"
OUTPUT_PATH   = Path(__file__).parent.parent / "docs" / "index.html"  # served by GitHub Pages

MAX_TWEETS      = 12   # max X/Twitter quotes in the tweet widget
MAX_NEWS_QUOTES = 10   # max press quotes in the news widget
MAX_TIMELINE_DAYS = 4     # max days shown in the event timeline
MAX_TIMELINE_PER_DAY = 6   # max entries shown per day
MAX_NEWS_PER_DAY = 2       # max news-source entries per day (rest filled by X quotes)
MAX_DELTA       = 5    # max items in the "new since last run" card


# ── Helpers ───────────────────────────────────────────────────────────────────

def html_escape(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))


def strip_bullet(s: str) -> str:
    """Remove leading bullet chars that the model sometimes adds."""
    return re.sub(r"^[\u2022\-\*]\s*", "", s.strip())


def fmt_date(d) -> str:
    """Cross-platform date formatter — no zero-padded day."""
    if isinstance(d, str):
        d = datetime.fromisoformat(d)
    return d.strftime("%b ") + str(d.day) + d.strftime(", %Y")


# ── Load Excel ────────────────────────────────────────────────────────────────

def load_excel(path: str) -> dict:
    return pd.read_excel(path, sheet_name=None)


# ── Extract X/Twitter quotes ──────────────────────────────────────────────────

def extract_tweets(quotes_df: pd.DataFrame, max_n: int = MAX_TWEETS) -> list[dict]:
    """Parse the Quotes sheet into tweet dicts for the carousel widget."""
    if quotes_df.empty:
        return []

    df = quotes_df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp", ascending=False)

    tweets = []
    seen_bodies = set()

    for _, row in df.iterrows():
        raw = str(row["Quote"]).strip()
        ts  = row["Timestamp"]

        # Standard format: "@handle: 'text'" or "• @handle: \"text\""
        m = re.search(r"@(\w+)[^:]*:\s*[\"'\u2018\u2019\u201c\u201d](.+)[\"'\u2018\u2019\u201c\u201d]", raw, re.S)
        if m:
            handle = "@" + m.group(1)
            body   = m.group(2).strip()
        else:
            # Fallback: grab any @handle anywhere in the text
            h = re.search(r"@(\w+)", raw)
            handle = "@" + h.group(1) if h else "X user"
            body   = re.sub(r"^[\u2022\-\*\d\.]\s*", "", raw)[:300]

        key = body[:60].lower()
        if key in seen_bodies or len(body) < 20:
            continue
        seen_bodies.add(key)

        tweets.append({
            "handle": handle,
            "date":   fmt_date(ts),
            "body":   body,
        })

        if len(tweets) >= max_n:
            break

    return tweets


# ── Extract news quotes ───────────────────────────────────────────────────────

def extract_news_quotes(news_quotes_df: pd.DataFrame, max_n: int = MAX_NEWS_QUOTES) -> list[dict]:
    """Parse the News Quotes sheet into dicts for the press quotes carousel."""
    if news_quotes_df.empty:
        return []

    df = news_quotes_df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp", ascending=False)

    items = []
    seen_bodies = set()

    for _, row in df.iterrows():
        raw = str(row["Quote"]).strip()
        ts  = row["Timestamp"]

        # Standard format from prompt: "Outlet: 'quote text'" or "Outlet — 'quote'"
        m = re.match(
            r"^([^:'\"—\u2014]{2,50})[:\u2014—]\s*[\"'\u2018\u2019\u201c\u201d]?(.+?)[\"'\u2018\u2019\u201c\u201d]?\s*$",
            raw, re.S
        )
        if m:
            outlet = m.group(1).strip()
            body   = m.group(2).strip()
        else:
            outlet = "Press"
            body   = re.sub(r"^[\u2022\-\*\d\.]\s*", "", raw)[:300]

        key = body[:60].lower()
        if key in seen_bodies or len(body) < 20:
            continue
        seen_bodies.add(key)

        items.append({
            "outlet": outlet,
            "date":   fmt_date(ts),
            "body":   body,
        })

        if len(items) >= max_n:
            break

    return items


# ── Build delta list ──────────────────────────────────────────────────────────

def build_delta(runs_df: pd.DataFrame, max_n: int = MAX_DELTA) -> list[dict]:
    """
    Shows highlights from the latest X/Twitter run.
    Marks each as 'new' if it doesn't appear (first 6 words) in the previous run.
    """
    if runs_df.empty or "Timestamp" not in runs_df.columns:
        return []

    df = runs_df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df = df.sort_values("Timestamp", ascending=False)

    def parse_highlights(raw: str) -> list[str]:
        # Grok sometimes uses \n\n between bullets, sometimes \n — handle both
        lines = []
        for line in re.split(r"\n+", raw):
            line = strip_bullet(line)
            if len(line) >= 15:
                lines.append(line)
        return lines

    latest_lines = parse_highlights(str(df.iloc[0].get("Highlights", "")))

    # Build a reference string from the previous run to detect what's genuinely new
    prev_text = ""
    if len(df) > 1:
        prev_text = str(df.iloc[1].get("Highlights", "")).lower()

    items = []
    for line in latest_lines:
        words = line.lower().split()
        if prev_text and len(words) >= 6:
            # "New" if the first 6 words don't appear in the previous highlights
            is_new = " ".join(words[:6]) not in prev_text
        else:
            is_new = line.lower() not in prev_text if prev_text else True
        items.append({"text": line, "is_new": is_new})
        if len(items) >= max_n:
            break

    return items


# ── Build consensus text ──────────────────────────────────────────────────────

def build_consensus(runs_df: pd.DataFrame) -> str:
    """X/Twitter consensus from the most recent run."""
    if runs_df.empty or "Timestamp" not in runs_df.columns:
        return ""
    df = runs_df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    latest = df.sort_values("Timestamp", ascending=False).iloc[0]
    return str(latest.get("Consensus", "")).strip()


def build_news_consensus(news_runs_df: pd.DataFrame) -> str:
    """News/press consensus from the most recent news run."""
    if news_runs_df.empty:
        return ""
    df = news_runs_df.copy()
    _NEWS_RUNS_COLS = ["Timestamp", "Topic", "From Date", "To Date", "Model",
                       "Summary", "Highlights", "Consensus"]
    # If the header row was never written, pandas uses the first data row as
    # column names — detect this and assign the correct names.
    if "Consensus" not in df.columns and df.shape[1] == len(_NEWS_RUNS_COLS):
        df.columns = _NEWS_RUNS_COLS
    if "Timestamp" not in df.columns:
        return ""
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"])
    if df.empty:
        return ""
    latest = df.sort_values("Timestamp", ascending=False).iloc[0]
    return str(latest.get("Consensus", "")).strip()


# ── Build situation signals ──────────────────────────────────────────────────

_SIGNALS_COLS = [
    "Timestamp", "Topic",
    "protest_level", "protest_status", "protest_trend",
    "security_level", "security_status", "security_trend",
    "supply_level",   "supply_status",   "supply_trend",
    "media_level",    "media_status",    "media_trend",  # kept for backwards-compat with existing sheets
]

def build_signals(signals_df: pd.DataFrame) -> dict | None:
    """
    Return signal data for the 3 active panels (protest, security, supply).
    Each entry has: dots (list of 7 ints, oldest→newest), status, trend.
    Returns None if the sheet is missing or empty.
    """
    if signals_df.empty:
        return None
    df = signals_df.copy()
    # Handle missing header row (same issue as News Runs sheet)
    if "Timestamp" not in df.columns and df.shape[1] == len(_SIGNALS_COLS):
        df.columns = _SIGNALS_COLS
    if "Timestamp" not in df.columns:
        return None
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"]).sort_values("Timestamp", ascending=False)
    if df.empty:
        return None

    result = {}
    for sig in ["protest", "security", "supply"]:
        # Last 7 runs, oldest first → that's the dot order (oldest left)
        levels = df[f"{sig}_level"].tolist()[:7][::-1]
        while len(levels) < 7:
            levels = [0] + levels  # pad left with 0 if fewer than 7 runs
        latest = df.iloc[0]
        result[sig] = {
            "dots":   [int(lvl) for lvl in levels],
            "status": str(latest.get(f"{sig}_status", "")).strip(),
            "trend":  str(latest.get(f"{sig}_trend",  "")).strip(),
        }
    return result


# ── Build event timeline ─────────────────────────────────────────────────────

# Maps Grok tag → (CSS class suffix, display label)
_TAG_CSS = {
    "Violence":    ("violence",    "Violence"),
    "Complaints":  ("complaints",  "Complaints"),
    "Protest":     ("protest",     "Protest"),
    "Government":  ("govt",        "Government"),
    "Economy":     ("economy",     "Economy"),
    "Media":       ("media",       "Media"),
    "Disruptions": ("disruptions", "Disruptions"),
    "Misc":        ("misc",        "Misc"),
}


def build_timeline(sheets: dict) -> str:
    """
    Build the full #timeline HTML from Quotes and News Quotes sheets,
    joined to their Runs sheets to get the to_date coverage date per entry.
    Entries are grouped by date, newest first.
    """

    def get_ts_to_date(runs: pd.DataFrame) -> pd.Series:
        """Return a Series mapping run Timestamp → To Date."""
        if runs.empty or "Timestamp" not in runs.columns or "To Date" not in runs.columns:
            return pd.Series(dtype="datetime64[ns]")
        r = runs.copy()
        r["Timestamp"] = pd.to_datetime(r["Timestamp"])
        r["To Date"]   = pd.to_datetime(r["To Date"])
        return r.drop_duplicates("Timestamp").set_index("Timestamp")["To Date"]

    def parse_source_body(raw: str, source_type: str) -> tuple[str, str]:
        """Split a raw quote string into (source_label, body_text)."""
        if source_type == "x":
            # Expected: "@handle: 'text'" or similar
            m = re.search(
                r"@(\w+)[^:]*:\s*[\"'\u2018\u2019\u201c\u201d](.+)[\"'\u2018\u2019\u201c\u201d]",
                raw, re.S,
            )
            if m:
                return "@" + m.group(1) + " · X/Twitter", m.group(2).strip()
            return "X/Twitter", raw
        else:
            # Expected: "Outlet: 'text'"
            m = re.match(
                r"^([^:'\"—\u2014]{2,50})[:\u2014—]\s*[\"'\u2018\u2019\u201c\u201d]?(.+?)[\"'\u2018\u2019\u201c\u201d]?\s*$",
                raw, re.S,
            )
            if m:
                return m.group(1).strip(), m.group(2).strip()
            return "Press", raw

    def render_group(date: pd.Timestamp, entries: list[dict], is_latest: bool) -> str:
        day_str = date.strftime("%A, %b ") + str(date.day) + date.strftime(", %Y")
        badge   = '\n            <span class="new-badge">latest</span>' if is_latest else ""
        lines   = [f'\n        <div class="tl-group">',
                   f'          <div class="tl-date-header">\n            {day_str}{badge}\n          </div>']
        for e in entries:
            css_cls, label = _TAG_CSS.get(e["tag"], ("misc", "Misc"))
            lines.append(
                f'          <div class="tl-event" data-tags="{css_cls}">\n'
                f'            <span class="ev-tag tag-{css_cls}">{label}</span>\n'
                f'            <div class="ev-body">\n'
                f'              <div class="ev-source">{html_escape(e["source"])}</div>\n'
                f'              <div class="ev-quote">{html_escape(e["body"])}</div>\n'
                f'            </div>\n'
                f'          </div>'
            )
        lines.append(f'        </div>')
        return "\n".join(lines)

    # Collect all entries from both sources
    all_entries: list[dict] = []
    for sheet_q, sheet_r, stype in [
        ("Quotes",      "Runs",      "x"),
        ("News Quotes", "News Runs", "news"),
    ]:
        df   = sheets.get(sheet_q, pd.DataFrame())
        runs = sheets.get(sheet_r, pd.DataFrame())
        if df.empty or "Quote" not in df.columns:
            continue
        ts_map = get_ts_to_date(runs)
        df = df.copy()
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        df["to_date"]   = pd.to_datetime(df["Timestamp"].map(ts_map)).dt.normalize()
        # Fall back: if Runs join fails (timestamp precision mismatch), use the
        # quote's own run timestamp date rather than silently dropping rows.
        unmatched = df["to_date"].isna()
        if unmatched.any():
            df.loc[unmatched, "to_date"] = df.loc[unmatched, "Timestamp"].dt.normalize()
        df = df.dropna(subset=["to_date"])
        # Case-insensitive column lookup (handles "Tag", "tag", "TAG", etc.)
        tag_col = next((c for c in df.columns if c.lower() == "tag"), None)
        for _, row in df.iterrows():
            raw = str(row["Quote"]).strip()
            if not raw or raw.lower() == "nan":
                continue
            tag_raw = str(row[tag_col]).strip() if tag_col else "Misc"
            # Normalize: handle NaN and case differences (Grok may return lowercase)
            if tag_raw.lower() in ("nan", "", "none"):
                tag = "Misc"
            else:
                tag_normalized = tag_raw.title()  # "violence" → "Violence"
                tag = tag_normalized if tag_normalized in _TAG_CSS else "Misc"
            source, body = parse_source_body(raw, stype)
            all_entries.append({"to_date": row["to_date"], "tag": tag,
                                 "source": source, "body": body, "stype": stype})

    if not all_entries:
        return ""

    df_all = pd.DataFrame(all_entries)
    dates  = sorted(df_all["to_date"].unique(), reverse=True)[:MAX_TIMELINE_DAYS]

    groups = []
    for i, d in enumerate(dates):
        day = df_all[df_all["to_date"] == d].to_dict("records")
        # Cap news entries, then fill remaining slots with X quotes
        news_rows = [r for r in day if r["stype"] == "news"][:MAX_NEWS_PER_DAY]
        x_rows    = [r for r in day if r["stype"] == "x"][:MAX_TIMELINE_PER_DAY - len(news_rows)]
        day_rows  = x_rows + news_rows  # X first, news appended at bottom
        groups.append(render_group(pd.Timestamp(d), day_rows, is_latest=(i == 0)))

    return "\n".join(groups)


# ── Build header metadata ─────────────────────────────────────────────────────

def build_header(runs_df: pd.DataFrame, news_runs_df: pd.DataFrame) -> dict:
    """Compute last-run timestamp and combined search window across both sources."""
    all_timestamps = []
    earliest_fr, latest_to = None, None

    for df in [runs_df, news_runs_df]:
        if df.empty or "Timestamp" not in df.columns:
            continue
        df = df.copy()
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        all_timestamps.extend(df["Timestamp"].tolist())

        fr = pd.to_datetime(df["From Date"]).min()
        to = pd.to_datetime(df["To Date"]).max()
        earliest_fr = fr if earliest_fr is None else min(earliest_fr, fr)
        latest_to   = to if latest_to   is None else max(latest_to, to)

    if not all_timestamps:
        return {}

    latest_ts = max(all_timestamps)
    return {
        "last_run":  latest_ts.strftime("%Y-%m-%d %H:%M"),
        "window":    fmt_date(earliest_fr) + " – " + fmt_date(latest_to),
        "latest_to": latest_to,  # used to label the "latest" group in the event timeline
    }


# ── Fetch WTI from CEIC (optional) ───────────────────────────────────────────

def fetch_wti_fred(days: int = 60) -> dict | None:
    """
    Fetch WTI spot price (Crude Oil: Spot Price: West Texas Intermediate Cushing)
    from CEIC API. Series ID 42651401, SR code SR89421787.
    More up-to-date than FRED, which publishes with a lag.
    """
    import requests

    try:
        from credentials import CEIC_TOKEN as token
    except ImportError:
        token = os.environ.get("CEIC_TOKEN", "")
    if not token:
        return None

    try:
        url = (
            "https://api.ceicdata.com/v2/series/"
            f"42651401_SR89421787?lang=en&format=json&token={token}"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        time_points = resp.json()["data"][0]["timePoints"]
        s = pd.Series(
            [tp["value"] for tp in time_points],
            index=pd.to_datetime([tp["date"] for tp in time_points]),
        ).sort_index()

        # Keep only the last `days` calendar days
        cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
        s = s[s.index >= cutoff]
        s = s.dropna()

        if s.empty:
            return None

        labels = [str(d.day) + " " + d.strftime("%b") for d in s.index]
        prices = [round(float(v), 2) for v in s]
        latest = prices[-1]
        first  = prices[0]
        pct    = round((latest - first) / first * 100, 1)
        sign   = "+" if pct > 0 else ""
        s.index = s.index.normalize()  # strip time component for date-alignment
        return {
            "labels":    labels,
            "prices":    prices,
            "price_val": f"${latest:.2f}",
            "change":    f"{sign}{pct}% in {days} days",
            "note":      fmt_date(s.index[-1]) + " · USD/barrel",
            "series":    s,   # kept for date-alignment with futures; not written to HTML
        }
    except Exception as e:
        print(f"[WTI] CEIC fetch failed: {e}")
        return None


# ── Haiti location data ───────────────────────────────────────────────────────

HAITI_LOCATIONS: dict[str, tuple[float, float]] = {
    # Departments
    "Ouest":               (18.5444, -72.3388),
    "Nord":                (19.7578, -72.2063),
    "Nord-Est":            (19.5597, -71.8564),
    "Nord-Ouest":          (19.8374, -72.8310),
    "Artibonite":          (19.0744, -72.5366),
    "Centre":              (19.0000, -71.9500),
    "Sud":                 (18.2040, -73.3500),
    "Sud-Est":             (18.2300, -72.3300),
    "Nippes":              (18.3900, -73.4200),
    "Grande-Anse":         (18.4432, -74.1198),
    # Major cities / communes
    "Port-au-Prince":      (18.5944, -72.3074),
    "Cap-Haïtien":         (19.7578, -72.2063),
    "Cap Haitien":         (19.7578, -72.2063),
    "Gonaïves":            (19.4530, -72.6890),
    "Gonaives":            (19.4530, -72.6890),
    "Saint-Marc":          (19.1150, -72.7020),
    "Jacmel":              (18.2341, -72.5354),
    "Les Cayes":           (18.2004, -73.7500),
    "Jérémie":             (18.6500, -74.1167),
    "Jeremie":             (18.6500, -74.1167),
    "Hinche":              (19.1478, -71.9878),
    "Fort-Liberté":        (19.6670, -71.8380),
    "Miragoâne":           (18.4450, -73.0880),
    "Miragoane":           (18.4450, -73.0880),
    "Léogâne":             (18.5124, -72.6330),
    "Leogane":             (18.5124, -72.6330),
    "Petit-Goâve":         (18.4330, -72.8670),
    "Petit-Goave":         (18.4330, -72.8670),
    "Croix-des-Bouquets":  (18.6060, -72.2280),
    # Port-au-Prince metro
    "Delmas":              (18.5444, -72.3072),
    "Delmas 33":           (18.5430, -72.3100),
    "Delmas 32":           (18.5435, -72.3090),
    "Delmas 75":           (18.5510, -72.2980),
    "Delmas 83":           (18.5520, -72.2960),
    "Pétion-Ville":        (18.5124, -72.2852),
    "Petion-Ville":        (18.5124, -72.2852),
    "Tabarre":             (18.5880, -72.2700),
    "Cité Soleil":         (18.5700, -72.3300),
    "Cite Soleil":         (18.5700, -72.3300),
    "Martissant":          (18.5230, -72.3530),
    "Carrefour":           (18.5400, -72.4020),
    "Kenscoff":            (18.4600, -72.2800),
    "Fontamara":           (18.5270, -72.3610),
    "Turgeau":             (18.5480, -72.3260),
    "Bel-Air":             (18.5560, -72.3400),
    "Bel Air":             (18.5560, -72.3400),
    "Bourdon":             (18.5350, -72.3200),
    "Lalue":               (18.5480, -72.3350),
    "La Plaine":           (18.5950, -72.2380),
    "Thomassin":           (18.4900, -72.2700),
    "Vivy Mitchell":       (18.5200, -72.2950),
    "Route de Frères":     (18.5300, -72.2900),
    "Frères":              (18.5300, -72.2900),
    "Juvenat":             (18.5050, -72.2830),
    "Morne Calvaire":      (18.5000, -72.2950),
    "Pacot":               (18.5490, -72.3310),
    "Canapé-Vert":         (18.5410, -72.3230),
    "Canape-Vert":         (18.5410, -72.3230),
    "Ruelle Vaillant":     (18.5460, -72.3280),
    "Solino":              (18.5570, -72.3200),
    "Nazon":               (18.5530, -72.3150),
    "Christ-Roi":          (18.5520, -72.3290),
    "Bas Delmas":          (18.5400, -72.3200),
    # Northern communes
    "Limonade":            (19.7060, -72.1120),
    "Quartier Morin":      (19.7200, -72.1670),
    "Plaisance":           (19.5980, -72.4650),
    "Milot":               (19.6080, -72.2200),
    "Acul du Nord":        (19.7330, -72.3000),
    # Artibonite
    "Dessalines":          (19.2850, -72.5050),
    "Marchand Dessalines": (19.2850, -72.5050),
    "Verrettes":           (19.0580, -72.4680),
    "L'Estère":            (19.2120, -72.6500),
    # South / Grand-Anse
    "Aquin":               (18.2760, -73.1170),
    "Cayes":               (18.2004, -73.7500),
    "Tiburon":             (18.3500, -74.3830),
    "Dame Marie":          (18.5670, -74.4170),
    "Anse-à-Veau":         (18.4980, -73.3520),
}


def _normalize(text: str) -> str:
    """Lowercase + strip accents for fuzzy location matching."""
    return unicodedata.normalize("NFD", text.lower()).encode("ascii", "ignore").decode()


# Pre-compute normalized keys once at import time
_LOCATIONS_NORM: dict[str, tuple[str, tuple[float, float]]] = {
    _normalize(k): (k, v) for k, v in HAITI_LOCATIONS.items()
}


def build_map_data(sheets: dict) -> list[dict]:
    """
    Count how many tweets/news items mention each known Haiti location.
    Each quote contributes at most 1 to a location's count (deduped per row).
    Also collects up to 5 short snippets per location for click-popup display.
    Returns a list of {name, lat, lon, count, snippets} dicts for locations with ≥1 mention.
    """
    from collections import Counter, defaultdict
    mentions: Counter = Counter()
    snippets: dict[str, list[str]] = defaultdict(list)

    for sheet_name in ["Quotes", "News Quotes"]:
        df = sheets.get(sheet_name, pd.DataFrame())
        if df.empty or "Quote" not in df.columns:
            continue
        for raw in df["Quote"].dropna():
            text = str(raw)
            text_norm = _normalize(text)
            seen: set[str] = set()
            for norm_key, (original_name, _) in _LOCATIONS_NORM.items():
                if norm_key in text_norm and original_name not in seen:
                    mentions[original_name] += 1
                    seen.add(original_name)
                    # collect a short snippet (first 110 chars of cleaned text)
                    snippet = text.strip().replace("\n", " ")
                    if len(snippet) > 110:
                        snippet = snippet[:110].rstrip() + "…"
                    snippets[original_name].append(snippet)

    return [
        {
            "name": name,
            "lat": HAITI_LOCATIONS[name][0],
            "lon": HAITI_LOCATIONS[name][1],
            "count": count,
            "snippets": snippets[name][:5],  # cap at 5 to keep popup compact
        }
        for name, count in mentions.items()
        if count > 0
    ]


# ── Fetch front-month WTI futures from Yahoo Finance ─────────────────────────

def fetch_wti_futures(days: int = 60) -> "pd.Series | None":
    """Fetch CL=F (front-month WTI futures) closing prices from Yahoo Finance.
    Returns a date-normalized pd.Series, or None on failure."""
    try:
        import yfinance as yf
    except ImportError:
        print("[WTI futures] yfinance not installed — run: python -m pip install yfinance")
        return None
    try:
        df = yf.Ticker("CL=F").history(period="3mo")[["Close"]]
        df.index = df.index.tz_convert(None).normalize()  # strip tz + time component
        cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
        s = df["Close"][df.index >= cutoff].dropna()
        return s if not s.empty else None
    except Exception as e:
        print(f"[WTI futures] Yahoo fetch failed: {e}")
        return None


# ── Inject into HTML ──────────────────────────────────────────────────────────

def inject(html: str, tweets: list, news_quotes: list, delta: list,
           x_consensus: str, news_consensus: str,
           header: dict, wti: dict | None, timeline: str = "",
           signals: dict | None = None,
           map_data: list | None = None) -> str:

    # ── Event timeline (full replacement)
    if timeline:
        html = re.sub(
            r'(<div id="timeline">).*?(</div><!-- #timeline -->)',
            lambda m: m.group(1) + "\n" + timeline + "\n\n      " + m.group(2),
            html, flags=re.S,
        )

    # ── Header metadata
    if header.get("last_run"):
        html = re.sub(r'id="h-last-run">[^<]*',
                      f'id="h-last-run">{html_escape(header["last_run"])}', html)
    if header.get("window"):
        html = re.sub(r'id="h-window">[^<]*',
                      f'id="h-window">{html_escape(header["window"])}', html)

    # ── WTI oil chart data
    if wti:
        oil_js = (
            f'const OIL_DATA = {{\n'
            f'  labels:  {json.dumps(wti["labels"])},\n'
            f'  prices:  {json.dumps(wti["prices"])},\n'
            f'  futures: {json.dumps(wti.get("futures"))}\n'
            f'}};'
        )
        html = re.sub(r'const OIL_DATA = \{.*?\};', oil_js, html, flags=re.S)
        html = re.sub(r'id="oil-price-val">[^<]*',
                      f'id="oil-price-val">{html_escape(wti["price_val"])}', html)
        html = re.sub(r'id="oil-change-val">[^<]*',
                      f'id="oil-change-val">{html_escape(wti["change"])}', html)
        html = re.sub(r'id="oil-note-val">[^<]*',
                      f'id="oil-note-val">{html_escape(wti["note"])}', html)

    # ── X/Twitter tweet widget
    if tweets:
        tweets_js = "const TWEETS = " + json.dumps(tweets, ensure_ascii=False, indent=2) + ";"
        html = re.sub(r'const TWEETS = \[.*?\];', tweets_js, html, flags=re.S)
        t0 = tweets[0]
        html = re.sub(r'id="tw-handle">[^<]*',
                      f'id="tw-handle">{html_escape(t0["handle"])}', html)
        html = re.sub(r'id="tw-body">[^<]*',
                      f'id="tw-body">{html_escape(t0["body"])}', html)
        html = re.sub(r'id="tw-date">[^<]*',
                      f'id="tw-date">{html_escape(t0["date"])}', html)
        html = re.sub(r'id="tw-counter">[^<]*',
                      f'id="tw-counter">1 / {len(tweets)}', html)

    # ── Press quotes widget
    if news_quotes:
        nq_js = "const NEWS_QUOTES = " + json.dumps(news_quotes, ensure_ascii=False, indent=2) + ";"
        html = re.sub(r'const NEWS_QUOTES = \[.*?\];', nq_js, html, flags=re.S)
        nq0 = news_quotes[0]
        html = re.sub(r'id="nq-outlet">[^<]*',
                      f'id="nq-outlet">{html_escape(nq0["outlet"])}', html)
        html = re.sub(r'id="nq-body">[^<]*',
                      f'id="nq-body">{html_escape(nq0["body"])}', html)
        html = re.sub(r'id="nq-date">[^<]*',
                      f'id="nq-date">{html_escape(nq0["date"])}', html)
        html = re.sub(r'id="nq-counter">[^<]*',
                      f'id="nq-counter">1 / {len(news_quotes)}', html)

    # ── Delta list
    if delta:
        items_html = "\n".join(
            f'          <div class="delta-item{" is-new" if d["is_new"] else ""}">'
            f'{html_escape(d["text"])}</div>'
            for d in delta
        )
        html = re.sub(
            r'(<div id="delta-list">)[\s\S]*?(</div><!-- #delta-list -->)',
            lambda m: m.group(1) + "\n" + items_html + "\n        " + m.group(2),
            html, flags=re.S,
        )

    # ── X/Twitter consensus
    if x_consensus:
        html = re.sub(
            r'(<div class="consensus-text" id="consensus-text">).*?(</div>)',
            f'\\1\n          {html_escape(x_consensus)}\n        \\2',
            html, flags=re.S
        )

    # ── News consensus
    if news_consensus:
        html = re.sub(
            r'(<div class="consensus-text" id="news-consensus-text">).*?(</div>)',
            f'\\1\n          {html_escape(news_consensus)}\n        \\2',
            html, flags=re.S
        )

    # ── Situation signals
    if signals:
        for sig, data in signals.items():
            dots_html = "\n".join(
                f'        <div class="dot" data-level="{lvl}"></div>'
                for lvl in data["dots"]
            )
            # Use comment markers (same pattern as timeline/delta-list) to avoid
            # matching the first inner </div> instead of the dot-row closing tag.
            html = re.sub(
                rf'(<div class="dot-row" id="sig-{sig}-dots">)[\s\S]*?(</div><!-- #sig-{sig}-dots -->)',
                lambda m, d=dots_html: m.group(1) + "\n" + d + "\n      " + m.group(2),
                html,
            )
            html = re.sub(
                rf'id="sig-{sig}-status">[^<]*',
                f'id="sig-{sig}-status">{html_escape(data["status"])}',
                html,
            )
            html = re.sub(
                rf'id="sig-{sig}-trend">[^<]*',
                f'id="sig-{sig}-trend">{html_escape(data["trend"])}',
                html,
            )

    # ── Location map
    if map_data is not None:
        map_js = f'const MAP_DATA = {json.dumps(map_data, ensure_ascii=False)};  /* HAITI_MAP_DATA */'
        html = re.sub(
            r'const MAP_DATA = \[.*?\];\s*/\* HAITI_MAP_DATA \*/',
            lambda _: map_js, html, flags=re.S,
        )

    return html


# ── Main ──────────────────────────────────────────────────────────────────────

def main(excel_path: str):
    print(f"[1/6] Loading {excel_path} ...")
    sheets = load_excel(excel_path)

    runs_df        = sheets.get("Runs",        pd.DataFrame())
    quotes_df      = sheets.get("Quotes",      pd.DataFrame())
    news_runs_df   = sheets.get("News Runs",   pd.DataFrame())
    news_quotes_df = sheets.get("News Quotes", pd.DataFrame())
    signals_df     = sheets.get("Signals",     pd.DataFrame())

    print("[2/6] Extracting X/Twitter quotes ...")
    tweets = extract_tweets(quotes_df)
    print(f"      → {len(tweets)} tweets")

    print("[3/6] Extracting press quotes ...")
    news_quotes = extract_news_quotes(news_quotes_df)
    print(f"      → {len(news_quotes)} news quotes")

    print("[4/6] Building delta and consensus ...")
    delta         = build_delta(runs_df)
    print(f"      → {len(delta)} delta items (is_new: {sum(d['is_new'] for d in delta)})")
    if not delta:
        print(f"      [WARN] Runs sheet rows: {len(runs_df)}, "
              f"Highlights sample: {str(runs_df.iloc[0].get('Highlights','<empty>') if not runs_df.empty else '<no rows>')[:80]}")
    x_consensus   = build_consensus(runs_df)
    news_consensus = build_news_consensus(news_runs_df)
    header        = build_header(runs_df, news_runs_df)

    signals = build_signals(signals_df)
    print(f"      → signals: {'loaded' if signals else 'none (run x_search.py first)'}")

    print("[5/6] Building event timeline ...")
    timeline = build_timeline(sheets)
    print(f"      → {len(pd.DataFrame(sheets.get('Quotes', pd.DataFrame())).dropna()) + len(pd.DataFrame(sheets.get('News Quotes', pd.DataFrame())).dropna())} entries across {timeline.count('tl-group')} date groups")

    print("[6/7] Fetching WTI oil prices ...")
    wti = fetch_wti_fred()
    if wti:
        print(f"      → CEIC spot: {wti['price_val']}")
    else:
        print("      → CEIC fetch failed; keeping placeholder chart data")

    wti_fut = fetch_wti_futures()
    if wti_fut is not None:
        print(f"      → Yahoo futures: {len(wti_fut)} days, latest ${wti_fut.iloc[-1]:.2f}")
    else:
        print("      → Yahoo futures fetch failed; chart will show spot line only")

    # Merge CEIC spot + Yahoo futures onto a unified date axis
    if wti and wti_fut is not None:
        s_ceic    = wti.pop("series")
        all_dates = sorted(set(s_ceic.index) | set(wti_fut.index))
        wti["labels"]  = [str(d.day) + " " + d.strftime("%b") for d in all_dates]
        wti["prices"]  = [round(float(s_ceic[d]),  2) if d in s_ceic.index  else None for d in all_dates]
        wti["futures"] = [round(float(wti_fut[d]), 2) if d in wti_fut.index else None for d in all_dates]
    elif wti:
        wti.pop("series", None)
        wti["futures"] = None

    print("[7/8] Building location map data ...")
    map_data = build_map_data(sheets)
    print(f"      → {len(map_data)} locations mentioned: "
          + ", ".join(f"{d['name']} ({d['count']})" for d in
                      sorted(map_data, key=lambda x: -x['count'])[:5]))

    print("[8/8] Injecting into HTML ...")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    updated  = inject(template, tweets, news_quotes, delta,
                      x_consensus, news_consensus, header, wti, timeline, signals,
                      map_data)
    OUTPUT_PATH.write_text(updated, encoding="utf-8")

    print(f"\nDone. Open: {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_EXCEL)
    if not Path(path).exists():
        print(f"Excel file not found: {path}")
        print(f"Default location: {DEFAULT_EXCEL}")
        sys.exit(1)
    main(path)
