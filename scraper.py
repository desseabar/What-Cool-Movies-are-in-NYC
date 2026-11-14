#!/usr/bin/env python3
"""
NYC indie/foreign movie theater scraper.

Usage:
    python scraper.py           # terminal table
    python scraper.py --html    # also save movies.html
    python scraper.py --site    # update docs/movies.json for GitHub Pages

To add a new theater:
    1. Write scrape_mytheatre() and optionally scrape_mytheatre_coming_soon()
    2. Add it to the THEATERS list near the bottom of this file.
"""

import html as _html
import json
import re
import sys
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup, Comment, NavigableString

try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Movie:
    title: str
    theater: str
    url: str
    status: str = "Now Playing"   # "Now Playing" or "Coming Soon"
    opens: str = ""               # e.g. "Opens Thu May 1"
    booking_url: str = ""
    year: str = ""
    director: str = ""
    cast: str = ""
    country: str = ""
    # Structured screenings: [{"date": "YYYY-MM-DD", "time": "6:30 PM", "url": "...", "label": "35mm"}]
    showtimes: list = field(default_factory=list)
    show_dates: list = field(default_factory=list)  # ["YYYY-MM-DD", ...]
    date_start: str = ""   # ISO date — run/engagement start
    date_end:   str = ""   # ISO date — run/engagement end
    description: str = ""
    notes: list = field(default_factory=list)       # event info: Q&As, intros, print formats
    poster: str = ""
    letterboxd_url: str = ""
    letterboxd_rating: str = ""


# ---------------------------------------------------------------------------
# Common utilities
# ---------------------------------------------------------------------------

_MONTH_FULL = ["", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]


# ── Text cleanup ────────────────────────────────────────────────────────────

def clean_text(s: str) -> str:
    """Unescape HTML entities, strip tags, and collapse whitespace to one line."""
    if not s:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = _html.unescape(_html.unescape(s))       # some feeds double-escape (&amp;rsquo;)
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace(" ", " ").replace("\xa0", " ").replace("​", "")
    return re.sub(r"\s+", " ", s).strip()


def html_paragraphs(s: str) -> list:
    """Split an HTML (or plain) blob into cleaned paragraphs."""
    if not s:
        return []
    parts = re.split(r"</p\s*>|<br\s*/?>\s*(?:&nbsp;|\s)*<br\s*/?>|\r?\n\s*\r?\n", s, flags=re.I)
    return [p for p in (clean_text(x) for x in parts) if p]


# Paragraphs that describe the *event* (guests, formats, ticketing) rather than the film
_EVENT_RE = re.compile(
    r"^\W*(?:q\s*&\s*a\b|q&as?\b|introduc|intro\b|ext\. intro|preceded by|followed by|"
    r"post-screening|pre-screening|pre-show|in[ -]person|join us|co-presented|hosted by|"
    r"(?:a )?special (?:presentation|introduction)|.{0,40}will precede|"
    r"presented (?:by|with|in partnership|in [a-z]+ with english subtitles)|"
    r"screening (?:in|on) \d|screening in 3d|this (?:screening|event|show)|tickets?\b|note:|please note|"
    r"doors\b|special (?:event|screening|guest|pre-show)|sneak preview|live (?:score|performance|music)|"
    r"book signing|(?:new york|world|north american|u\.?s\.?|east coast|nyc|international) premiere|"
    r"\d{2}mm print|(?:16|35|70)mm\b|4k (?:dcp|restoration)|opens? (?:mon|tue|wed|thu|fri|sat|sun)|"
    r"showtimes to be announced|with (?:an? )?(?:introduction|q&a|conversation|discussion)|"
    r"(?:conversation|discussion) with|part of\b|distributor:|run ?time:|accessibility|"
    r"(?:mon|tues?|wed(?:nes)?|thu(?:rs)?|fri|sat(?:ur)?|sun)(?:day)?,? (?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w* \d{1,2}\b"
    r"(?:.{0,40}(?:\d{1,2}(?::\d{2})? ?[ap]\.?m|\|)|[^.]{0,30}:\s))",
    re.IGNORECASE,
)
# Boilerplate that is neither synopsis nor useful event info
_DROP_RE = re.compile(
    r"flashing lights|photosensitiv|strobe effect|epilep|does not generally provide advisories|"
    r"guild members|complimentary entry|all sales are final|make an additional \$|select the .{0,40} ticket",
    re.IGNORECASE,
)


def split_description(paragraphs) -> tuple:
    """Separate synopsis paragraphs from event notes. Returns (description, notes)."""
    if isinstance(paragraphs, str):
        paragraphs = html_paragraphs(paragraphs)
    desc, notes = [], []
    for p in paragraphs:
        p = clean_text(p)
        if not p or _DROP_RE.search(p):
            continue
        if _EVENT_RE.search(p):
            # "35mm print courtesy of X. Blimp pilot Michael Lander…" → note + synopsis
            lead = re.match(r"^(.{5,160}?[.!])\s+(?=[A-Z“\"])(.{80,})$", p)
            if lead and not _EVENT_RE.search(lead.group(2)):
                notes.append(lead.group(1))
                desc.append(lead.group(2))
            elif len(p) < 450:
                notes.append(p)
            else:
                desc.append(p)
        elif len(p.split()) >= 4 or re.search(r"[.!?]$", p):
            desc.append(p)          # fragments like "$26" are neither synopsis nor a note
    return "\n\n".join(desc), notes


# ── Titles ──────────────────────────────────────────────────────────────────

_SMALL_WORDS = {
    "a", "an", "and", "as", "at", "but", "by", "for", "from", "in", "into", "nor", "of",
    "on", "or", "per", "the", "to", "vs", "vs.", "via", "with", "de", "du", "des", "la",
    "le", "les", "et", "el", "y", "di", "da", "del", "von", "van", "und", "en",
}
_KEEP_UPPER = {"DCP", "4K", "3D", "2D", "TV", "USA", "US", "UK", "NYC", "DJ", "VHS", "MTV",
               "BAM", "IFC", "AI", "OK", "LSD", "FBI", "CIA", "NASA", "IMAX", "Q&A", "SF"}
_ROMAN_RE = re.compile(r"^(?=[IVX]+$)X{0,3}(IX|IV|V?I{0,3})$")


def _title_word(word: str, force_cap: bool) -> str:
    m = re.match(r"^([^\w]*)(.*?)([^\w]*)$", word, re.UNICODE)
    lead, core, trail = m.groups() if m else ("", word, "")
    if not core:
        return word
    if any(c.islower() for c in core):          # already deliberately cased (e.g. "threeASFOUR")
        return word
    if core.upper() in _KEEP_UPPER or _ROMAN_RE.match(core) or re.match(r"^[A-Z](\.[A-Z])+\.?$", core):
        return word
    if re.match(r"^\d+(MM|TH|ST|ND|RD|S)$", core):  # 35MM → 35mm, 25TH → 25th
        return lead + core.lower() + trail
    if not force_cap and core.lower() in _SMALL_WORDS:
        return lead + core.lower() + trail

    def cap(part: str) -> str:
        # Apostrophes: MARY'S → Mary's, L'INNOCENTE → L'Innocente, O'BRIEN → O'Brien
        pieces = re.split(r"(['’])", part)
        out = pieces[0][:1].upper() + pieces[0][1:].lower()
        for i in range(1, len(pieces), 2):
            nxt = pieces[i + 1] if i + 1 < len(pieces) else ""
            nxt = nxt.lower() if len(nxt) <= 2 else nxt[:1].upper() + nxt[1:].lower()
            out += pieces[i] + nxt
        return out

    return lead + "-".join(cap(p) for p in core.split("-")) + trail


def smart_title(s: str) -> str:
    """Title-case strings that arrive in ALL CAPS; leave mixed-case strings alone."""
    s = (s or "").strip()
    letters = [c for c in s if c.isalpha()]
    if not letters or sum(c.isupper() for c in letters) / len(letters) < 0.6:
        return s
    tokens = re.split(r"(\s+)", s)
    out, force = [], True
    words = [t for t in tokens if t and not t.isspace()]
    last = words[-1] if words else None
    for t in tokens:
        if not t or t.isspace():
            out.append(t)
            continue
        out.append(_title_word(t, force or t is last))
        force = bool(re.search(r"[:–—.!?/+]$|^[-–—]$", t))
    return "".join(out)


_FORMAT_LABELS = [
    (re.compile(r"\s*[\[(]\s*(\d{2})\s*mm\s*(?:print)?\s*[\])]\s*$", re.I), lambda m: f"{m.group(1)}mm"),
    (re.compile(r"\s+(?:in|on)\s+(\d{2})\s*mm\s*$", re.I), lambda m: f"{m.group(1)}mm"),
    (re.compile(r"\s*[-–:|]\s*(\d{2})\s*mm(?:\s+print)?\s*$", re.I), lambda m: f"{m.group(1)}mm"),
    (re.compile(r"\s*[\[(]\s*4k(?:\s+(?:dcp|restoration|remaster))?\s*[\])]\s*$", re.I), lambda m: "4K"),
    (re.compile(r"\s+(?:in\s+)?4k\s+(?:remaster|restoration)(?:ed)?\s*$", re.I), lambda m: "4K restoration"),
    (re.compile(r"\s*[\[(]\s*(?:dcp|digital)\s*[\])]\s*$", re.I), lambda m: ""),
    (re.compile(r"\s*[\[(]\s*(?:open captioning|open captions?|oc)\s*[\])]\s*$", re.I), lambda m: "Open captions"),
    (re.compile(r"\s*[\[(]\s*(3d|imax)\s*[\])]\s*$", re.I), lambda m: m.group(1).upper()),
    (re.compile(r"\s*\(\s*\d{1,2}/\d{1,2}(?:/\d{2,4})?\s*\)\s*$"), lambda m: ""),
    (re.compile(r"\s*[\[(]\s*(dubbed|subtitled|english dub|sub)\s*[\])]\s*$", re.I), lambda m: m.group(1).capitalize()),
    (re.compile(r"\s*[-–:]\s*(\d+(?:st|nd|rd|th)\s+anniversary)(?:\s+(?:screening|edition|re-?release))?\s*$", re.I),
     lambda m: m.group(1).lower()),
]
_YEAR_SUFFIX = re.compile(r"\s*\((1[89]\d\d|20\d\d)\)\s*$")
_LEADING_GAUGE = re.compile(r"^(\d{2})\s*mm\s+(?=\S)", re.I)
_PRESENTER = re.compile(r"^((?:[\w&.'’-]+\s+){0,4}[\w&.'’-]+?)\s+presents?:?\s+(?=\S)", re.I)


def split_title(title: str) -> tuple:
    """Return (clean_title, [format labels], year, [notes]) from a raw listing title."""
    t = clean_text(title)
    labels, notes, year = [], [], ""
    changed = True
    while changed:
        changed = False
        for rx, fn in _FORMAT_LABELS:
            m = rx.search(t)
            if m and m.start() > 0:
                lab = fn(m)
                if lab and lab not in labels:
                    labels.append(lab)
                t = t[: m.start()].rstrip(" -–:|")
                changed = True
        m = _YEAR_SUFFIX.search(t)
        if m and m.start() > 0:
            year = year or m.group(1)
            t = t[: m.start()]
            changed = True
    m = _LEADING_GAUGE.match(t)
    if m:
        labels.append(f"{m.group(1)}mm")
        t = t[m.end():]
    m = _PRESENTER.match(t)
    if m and not re.match(r"^(?:the|a|an)$", m.group(1), re.I):
        notes.append(f"Presented by {m.group(1)}")
        t = t[m.end():]
    return smart_title(t.strip()), labels, year, notes


def clean_title(title: str) -> str:
    """Strip display-only suffixes so titles match across sources."""
    return split_title(title)[0]


def title_key(title: str) -> str:
    """Accent/punctuation-insensitive key for matching the same film across sources."""
    t = unicodedata.normalize("NFKD", (title or "").lower())
    t = "".join(c for c in t if not unicodedata.combining(c)).replace("&", "and")
    t = re.sub(r"^(the|a|an)\s+", "", t)
    return re.sub(r"[^a-z0-9]", "", t)


# ── Showtimes ───────────────────────────────────────────────────────────────

def fmt_time(hour: int, minute: int) -> str:
    ampm = "AM" if hour < 12 else "PM"
    h12 = hour % 12 or 12
    return f"{h12}:{minute:02d} {ampm}"


def norm_time(s: str, bare_hours_pm: bool = False) -> str:
    """'6:30pm' / '6:30 p.m.' / '18:30' / '7PM' → '6:30 PM'.  Returns '' if no time found.
    With bare_hours_pm, times lacking am/pm are treated as PM except 10/11 (Film Forum style)."""
    if not s:
        return ""
    s = s.replace(" ", " ").replace("\xa0", " ")
    m = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\b\.?", s, re.I)
    if m:
        h, mi = int(m.group(1)) % 12, int(m.group(2) or 0)
        return fmt_time(h + (12 if m.group(3).lower() == "p" else 0), mi)
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", s)
    if not m:
        return ""
    h, mi = int(m.group(1)), int(m.group(2))
    if h >= 13 or h == 0 or not bare_hours_pm:
        return fmt_time(h, mi)
    if h == 12:
        return fmt_time(12, mi)
    return fmt_time(h if h in (10, 11) else h + 12, mi)


def showtime(date_iso: str, time_str: str = "", url: str = "", label: str = "") -> dict:
    return {"date": date_iso, "time": time_str, "url": url or "", "label": label or ""}


def _time_sort_key(t: str) -> int:
    m = re.match(r"(\d{1,2}):(\d{2}) (AM|PM)", t or "")
    if not m:
        return -1
    h = int(m.group(1)) % 12 + (12 if m.group(3) == "PM" else 0)
    # Treat post-midnight shows (before 5 AM) as the end of the business day
    return (h + 24 if h < 5 else h) * 60 + int(m.group(2))


def sort_showtimes(sts: list) -> list:
    seen, out = set(), []
    for st in sorted(sts, key=lambda s: (s.get("date", ""), _time_sort_key(s.get("time", "")))):
        key = (st.get("date"), st.get("time"), st.get("label", ""))
        if key in seen or not st.get("date"):
            continue
        seen.add(key)
        out.append(st)
    return out


def fmt_showtime(st: dict) -> str:
    try:
        d = date.fromisoformat(st["date"]).strftime("%a %b %-d")
    except (ValueError, KeyError):
        d = st.get("date", "")
    s = f"{d} {st.get('time', '')}".strip()
    return f"{s} ({st['label']})" if st.get("label") else s


def _normalize_opens(text: str) -> str:
    """Return 'Month Day' (e.g. 'April 20') from any date-like string, or '' if unparseable."""
    if not text:
        return ""
    iso_m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if iso_m:
        return f"{_MONTH_FULL[int(iso_m.group(2))]} {int(iso_m.group(3))}"
    mo_m = re.search(
        r'\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?'
        r'|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'\w*\s+(\d{1,2})\b',
        text, re.IGNORECASE,
    )
    if mo_m:
        try:
            d = datetime.strptime(f"{mo_m.group(1)[:3].capitalize()} {mo_m.group(2)}", "%b %d")
            return f"{_MONTH_FULL[d.month]} {d.day}"
        except ValueError:
            pass
    return ""


def _infer_year(month: int, day: int) -> int:
    """Pick the nearest year for a bare month/day, rolling forward if the date passed >30 days ago."""
    today = date.today()
    try:
        candidate = date(today.year, month, day)
    except ValueError:
        return today.year
    return today.year + 1 if candidate < today - timedelta(days=30) else today.year


def _parse_show_date(text: str) -> str:
    """Parse a showtime date string to ISO YYYY-MM-DD, or '' if unparseable."""
    text = text.strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    # Match "Apr 20", "Apr20", "April 20" — allow zero or more spaces between month and day
    m = re.search(r"\b([A-Z][a-z]{2,8})\s*(\d{1,2})\b", text)
    if m:
        try:
            month = datetime.strptime(m.group(1)[:3], "%b").month
            day = int(m.group(2))
            return f"{_infer_year(month, day)}-{month:02d}-{day:02d}"
        except ValueError:
            pass
    return ""


def _fetch(url: str) -> Optional[BeautifulSoup]:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    for attempt in range(2):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 429:
                time.sleep(3)
                continue
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            if attempt == 0:
                continue
            print(f"[warn] {url}: {e}", file=sys.stderr)
    return None


def _apply_details(movies: list, detail_fn: Callable, has_booking: bool = False) -> None:
    """Fetch detail pages concurrently and merge results into movies in place."""
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(detail_fn, m.url): m for m in movies}
        for future in as_completed(futures):
            try:
                details = future.result() or {}
            except Exception as e:
                print(f"[warn] detail page {futures[future].url}: {e}", file=sys.stderr)
                continue
            movie = futures[future]
            for f in ("year", "director", "cast", "country", "poster"):
                val = details.get(f, "")
                if val and not getattr(movie, f):
                    setattr(movie, f, val)
            # Detail-page synopsis is the full text; listing pages often carry a teaser or event blurb
            if len(details.get("description", "")) > len(movie.description):
                movie.description = details["description"]
            for n in details.get("notes", []):
                if n not in movie.notes:
                    movie.notes.append(n)
            # Prefer detail-page schedules (more complete) over listing-page ones
            if details.get("showtimes") and len(details["showtimes"]) >= len(movie.showtimes):
                movie.showtimes = details["showtimes"]
            if details.get("show_dates") and len(details["show_dates"]) >= len(movie.show_dates):
                movie.show_dates = details["show_dates"]
            for f in ("date_start", "date_end"):
                if details.get(f) and not getattr(movie, f):
                    setattr(movie, f, details[f])
            if has_booking and not movie.booking_url:
                movie.booking_url = details.get("booking_url", "")


def _abs(url: str, base: str) -> str:
    """Make a relative URL absolute."""
    return url if url.startswith("http") else base.rstrip("/") + "/" + url.lstrip("/")


# ---------------------------------------------------------------------------
# Detail-page fetchers
# ---------------------------------------------------------------------------

def _nitehawk_date(val: str) -> str:
    """Nitehawk data-date is an ISO date or a Unix timestamp for midnight *UTC*."""
    val = (val or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        return val
    try:
        return datetime.utcfromtimestamp(int(val)).strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return ""


def _nitehawk_showtimes(root) -> list:
    sts = []
    for li in root.select("li[data-date]"):
        d = _nitehawk_date(li.get("data-date", ""))
        el = li.select_one(".showtime")
        if not d or not el:
            continue
        raw = el.get_text(" ", strip=True)
        t = norm_time(raw)
        if not t:
            continue
        label = "Sold out" if re.search(r"sold.?out", raw + " ".join(el.get("class") or []), re.I) else ""
        sts.append(showtime(d, t, el.get("href", ""), label))
    return sort_showtimes(sts)


def _nitehawk_details(url: str) -> dict:
    soup = _fetch(url.split("?")[0])
    if not soup:
        return {}
    result: dict = {}
    notes = []
    specs_p = soup.select_one("p.show-specs")
    if specs_p:
        for outer in specs_p.select("span"):
            label_el = outer.select_one("span.show-spec-label")
            if not label_el:
                continue
            label = label_el.get_text(strip=True).rstrip(":")
            value = clean_text(outer.get_text(" ", strip=True).replace(label_el.get_text(strip=True), "", 1))
            if label == "Release Year":
                result["year"] = value
            elif label == "Director":
                result["director"] = value
            elif label == "Country":
                result["country"] = value
            elif label == "Format" and re.search(r"\d+\s*mm", value, re.I):
                notes.append(f"Presented in {value}")
    paras = []
    for p in soup.select("div.show-content p"):
        text = p.get_text(" ", strip=True)
        strong = p.select_one("strong, b")
        if strong and strong.get_text(strip=True).startswith("Starring"):
            result["cast"] = clean_text(text.replace(strong.get_text(" ", strip=True), "", 1)).lstrip(": ")
            continue
        paras.append(text)
    desc, more_notes = split_description(paras)
    result["description"] = desc
    result["notes"] = notes + more_notes

    sts = _nitehawk_showtimes(soup)
    if sts:
        result["showtimes"] = sts
        result["show_dates"] = sorted({s["date"] for s in sts})
    else:
        show_dates = sorted(filter(None, {_nitehawk_date(el["data-date"]) for el in soup.select("[data-date]")}))
        if show_dates:
            result["show_dates"] = show_dates
        else:
            # No sessions yet — parse "Opens on July 1" from date-selector placeholder
            empty_sel = soup.select_one("div.date-selector.empty")
            if empty_sel:
                iso = _parse_show_date(empty_sel.get_text(strip=True))
                if iso:
                    result["date_start"] = result["date_end"] = iso
    return result


def _short_label(note: str) -> str:
    n = note.lower()
    gauge = re.search(r"\b(16|35|70)\s*mm\b", n)
    if gauge and not re.search(r"q\s*&\s*a|intro", n):
        return f"{gauge.group(1)}mm"
    if "q&a" in n or "q & a" in n:
        return "Q&A"
    if "intro" in n:
        return "Intro"
    if "sneak" in n or "preview" in n:
        return "Preview"
    return "Event"


def _ifc_details(url: str) -> dict:
    soup = _fetch(url)
    if not soup:
        return {}
    result: dict = {}
    notes = []
    for li in soup.select("ul.film-details li"):
        strong = li.select_one("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True)
        value = clean_text(li.get_text(" ", strip=True).replace(label, "", 1))
        if label == "Year":
            result["year"] = value
        elif label == "Director":
            result["director"] = value
        elif label == "Cast":
            result["cast"] = value
        elif label == "Country":
            result["country"] = value
        elif label == "Format" and re.search(r"\d+\s*mm", value, re.I):
            notes.append(f"Presented in {value}")
    ticket = soup.select_one("a.ifc-button[href*='tickets']") or \
             soup.select_one("a[href*='tickets.ifccenter.com']")
    if ticket:
        result["booking_url"] = ticket["href"]

    col = soup.select_one("div.ifc-col-2") or soup
    paras = []
    for p in col.find_all("p", recursive=False):
        if "date-time" in (p.get("class") or []):
            continue
        text = p.get_text(" ", strip=True)
        if text and not text.startswith("IFC Center does not generally"):
            paras.append(text)
    desc, more_notes = split_description(paras)
    result["description"] = desc
    notes += more_notes

    # Schedules: "Special Events" list (guest notes) and "Showtimes" list (date + times)
    events: dict = {}
    sts = []
    for ul in col.select("ul.schedule-list"):
        h2 = ul.find_previous_sibling("h2")
        is_event = bool(h2 and "special" in h2.get_text().lower())
        for li in ul.find_all("li", recursive=False):
            details = li.select_one("div.details")
            date_el = details.select_one("strong") if details else None
            d = _parse_show_date(date_el.get_text(" ", strip=True)) if date_el else ""
            if not d:
                continue
            if is_event:
                p = details.select_one("p")
                note = clean_text(p.get_text(" ", strip=True)) if p else ""
                if note:
                    notes.append(f"{date.fromisoformat(d).strftime('%a %b %-d')}: {note}")
                    a = li.select_one("a.ifc-button")
                    events[(d, norm_time(note, bare_hours_pm=True))] = (_short_label(note), a["href"] if a else "")
                continue
            for tli in details.select("ul.times li"):
                span = tli.select_one("span")
                tm = norm_time((span or tli).get_text(" ", strip=True))
                a = tli.select_one("a")
                if tm:
                    sts.append(showtime(d, tm, a["href"] if a else ""))
    for st in sts:
        ev = events.pop((st["date"], st["time"]), None)
        if ev:
            st["label"] = ev[0]
    for (d, tm), (label, href) in events.items():
        sts.append(showtime(d, tm, href, label))
    if sts:
        result["showtimes"] = sort_showtimes(sts)
        result["show_dates"] = sorted({s["date"] for s in sts})
    result["notes"] = notes
    return result


def _filmforum_link_title(link) -> str:
    """Extract film title from <a>, skipping presenter attribution before <br/>."""
    parts = [p.strip() for p in link.get_text(separator="\n", strip=True).split("\n") if p.strip()]
    return smart_title(parts[-1]) if parts else ""


def _p_segments(p) -> list:
    """Split a <p> at <br> tags into (text, has_plain_text) segments. Film Forum packs
    awards (bold), synopsis (plain), credits (italic) and metadata (bold) into one <p>."""
    segs, cur, plain = [], [], False
    for node in list(p.children) + [None]:
        if node is None or getattr(node, "name", None) == "br":
            text = clean_text(" ".join(cur))
            if text:
                segs.append((text, plain))
            cur, plain = [], False
            continue
        if isinstance(node, NavigableString):
            if isinstance(node, Comment):
                continue
            plain = plain or bool(node.strip())
            cur.append(str(node))
        else:
            if node.name not in ("strong", "b", "em", "i") and node.get_text(strip=True):
                plain = True
            cur.append(node.get_text(" "))
    return segs


_FF_STOP = r"(?:\s+(?:STARRING|WRITTEN|CONCEIVED|PRODUCED|FEATURING|ADAPTED|BASED|PHOTOGRAPHED|SCREENPLAY|MUSIC|NARRATED)\b|\s+CO-DIRECTED\b|\s*[.;]|$)"


def _filmforum_details(url: str) -> dict:
    soup = _fetch(url)
    if not soup:
        return {}
    result: dict = {}

    copy = soup.select_one("div.copy")
    if copy:
        # Metadata line: the bold block that looks like "YEAR  RUNTIME MIN.  COUNTRY" or "Directed by"
        meta = ""
        for strong in copy.select("strong"):
            text = re.sub(r"[\xa0\s]+", " ", strong.get_text(" ")).strip()
            if re.match(r"^\d{4}\s+\d+\s+MIN\.", text, re.I) or re.search(r"[Dd]irected by|\bApprox\.", text):
                meta = text
                break
        # Format 1 (all-caps): "YEAR RUNTIME MIN. COUNTRY [IN LANGUAGE] [DISTRIBUTOR]"
        if re.match(r"^\d{4}\s+\d+\s+MIN\.", meta, re.IGNORECASE):
            result["year"] = meta[:4]
            cm = re.search(r"MIN\.?\s+([A-Z][A-Z/ ]*?)(?:\s+IN\b|\s+WITH\b|\s+SILENT\b)", meta)
            if cm:
                result["country"] = smart_title(cm.group(1).strip().rstrip(" ,/"))
            else:
                cm2 = re.search(r"MIN\.?\s+([A-Z]{2,}(?: ?/ ?[A-Z]{2,})*)", meta)
                if cm2:
                    result["country"] = smart_title(cm2.group(1))
        # Format 2 (mixed case): "Country, Year Directed by Director Starring Cast Approx. N min."
        elif meta:
            ym = re.search(r"\b(19\d{2}|20\d{2})\b", meta)
            if ym:
                result["year"] = ym.group(1)
            cm = re.match(r"^([A-Za-z][A-Za-z./]*(?:/[A-Za-z.]+)?),", meta)
            if cm:
                result["country"] = cm.group(1).strip().rstrip(".")
            dm = re.search(r"[Dd]irected by\s+(.+?)(?:\s+[Ss]tarring|\s+Approx|\s*$)", meta)
            if dm:
                result["director"] = dm.group(1).strip()
            sm = re.search(r"[Ss]tarring\s+(.+?)(?:\s+Approx|\s+Produced|\s*$)", meta)
            if sm:
                result["cast"] = sm.group(1).strip()

        # Synopsis: plain-text segments of the paragraphs before the first sub-heading (Trailer/Reviews)
        paras = []
        for node in copy.children:
            if getattr(node, "name", None) in ("h2", "h3", "h4", "div"):
                break
            if getattr(node, "name", None) == "p":
                paras += [t for t, plain in _p_segments(node) if plain and len(t) > 40]
        desc, notes = split_description(paras)
        result["description"] = desc
        result["notes"] = notes

    # Director/cast banner: "DIRECTED BY X STARRING Y" / "DIRECTED BY X CO-DIRECTED BY Z"
    urgent = soup.select_one("div.urgent")
    if urgent:
        text = clean_text(urgent.get_text(" ", strip=True))
        if not result.get("director"):
            names = re.findall(r"(?:^|\s)(?:CO-)?DIRECTED BY\s+(.+?)" + _FF_STOP, text, re.IGNORECASE)
            if names:
                result["director"] = ", ".join(smart_title(n.strip()) for n in names)
        if not result.get("cast"):
            m = re.search(r"STARRING\s+(.+?)" + _FF_STOP.replace("STARRING|", ""), text)
            if m:
                result["cast"] = smart_title(re.sub(r"\s+AND\s+", ", ", m.group(1).strip()))

    # Dates for films outside the weekly grid (e.g. "Sunday, June 28 11:00")
    detail_dates: set = set()
    sts = []
    for p in soup.select("div.details p"):
        txt = p.get_text(" ", strip=True)
        iso = _parse_show_date(txt)
        if iso and not re.match(r"\s*opens", txt, re.I):
            detail_dates.add(iso)
            for tm in re.findall(r"\b\d{1,2}:\d{2}\b", txt):
                sts.append(showtime(iso, norm_time(tm, bare_hours_pm=True), url))
    if detail_dates:
        result["show_dates"] = sorted(detail_dates)
    if sts:
        result["showtimes"] = sort_showtimes(sts)
    return result


# ---------------------------------------------------------------------------
# Nitehawk Cinema — Prospect Park
# ---------------------------------------------------------------------------

def scrape_nitehawk() -> list:
    movies = []
    soup = _fetch("https://nitehawkcinema.com/prospectpark/")
    if not soup:
        return movies
    for card in soup.select("li.show-container.thumbnail"):
        title_el = card.select_one("div.show-title")
        if not title_el:
            continue
        link = card.select_one("a.overlay-link")
        url = link["href"] if link else "https://nitehawkcinema.com/prospectpark/"
        desc = card.select_one("div.short-description")
        first_st = card.select_one("a.showtime")
        sts = _nitehawk_showtimes(card)
        movies.append(Movie(
            title=smart_title(title_el.get_text(strip=True)),
            theater="Nitehawk (Prospect Park)",
            url=url,
            booking_url=first_st["href"] if first_st else "",
            showtimes=sts,
            show_dates=sorted({s["date"] for s in sts}),
            description=clean_text(desc.get_text(" ", strip=True)) if desc else "",
        ))
    print(f"  Fetching detail pages for {len(movies)} Nitehawk now-playing films…")
    _apply_details(movies, _nitehawk_details)
    return movies


def scrape_nitehawk_coming_soon() -> list:
    movies = []
    soup = _fetch("https://nitehawkcinema.com/prospectpark/coming-soon-2/")
    if not soup:
        return movies
    seen_urls: set = set()
    for card in soup.select("div.show-details"):
        title_el = card.select_one("h1.show-title a.title")
        if not title_el:
            continue
        url = title_el.get("href", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        date_el = card.select_one("select.datelist option") or \
                  card.select_one("div.selected-date span") or \
                  card.select_one("div.no-showtimes")
        opens = _normalize_opens(date_el.get_text(strip=True)) if date_el else ""
        movies.append(Movie(
            title=smart_title(title_el.get_text(strip=True)),
            theater="Nitehawk (Prospect Park)",
            url=url,
            status="Coming Soon",
            opens=opens,
        ))
    print(f"  Fetching detail pages for {len(movies)} Nitehawk coming-soon films…")
    _apply_details(movies, _nitehawk_details)
    return movies


# ---------------------------------------------------------------------------
# IFC Center
# ---------------------------------------------------------------------------

def _ifc_week_schedule(soup) -> dict:
    """Homepage 7-day schedule → {film_url: [title, [showtimes]]}. Covers repertory
    titles that never appear in the 'now playing' grid."""
    out: dict = {}
    for day in soup.select("div.daily-schedule"):
        h3 = day.find("h3")
        d = _parse_show_date(h3.get_text(" ", strip=True)) if h3 else ""
        if not d:
            continue
        for li in day.select(":scope > ul > li"):
            a = li.select_one("div.details h3 a")
            if not a:
                continue
            entry = out.setdefault(a["href"], [a.get_text(strip=True), []])
            for t in li.select("ul.times a"):
                tm = norm_time(t.get_text(" ", strip=True))
                if tm:
                    entry[1].append(showtime(d, tm, t.get("href", "").strip()))
    return out


def scrape_ifc() -> list:
    soup = _fetch("https://www.ifccenter.com/")
    if not soup:
        return []
    by_url: dict = {}
    for card in soup.select("div.ifc-grid-item"):
        title_el = card.select_one("div.ifc-grid-info h2")
        link_el = card.select_one("a[href*='/films/']")
        if not title_el or not link_el or link_el["href"] in by_url:
            continue
        by_url[link_el["href"]] = Movie(title=title_el.get_text(strip=True), theater="IFC Center", url=link_el["href"])
    for url, (title, sts) in _ifc_week_schedule(soup).items():
        m = by_url.setdefault(url, Movie(title=title, theater="IFC Center", url=url))
        m.showtimes = sort_showtimes(m.showtimes + sts)
        m.show_dates = sorted({s["date"] for s in m.showtimes})
    movies = list(by_url.values())
    print(f"  Fetching detail pages for {len(movies)} IFC now-playing films…")
    _apply_details(movies, _ifc_details, has_booking=True)
    return movies


def scrape_ifc_coming_soon() -> list:
    movies = []
    soup = _fetch("https://www.ifccenter.com/coming-soon/")
    if not soup:
        return movies
    seen_urls: set = set()
    for card in soup.select("div.ifc-grid-item"):
        title_el = card.select_one("div.ifc-grid-info h2")
        if not title_el:
            continue
        link_el = card.select_one("a[href*='/films/']")
        url = link_el["href"] if link_el else "https://www.ifccenter.com/"
        if url in seen_urls:
            continue
        seen_urls.add(url)
        opens_el = card.select_one("div.ifc-grid-info p")
        opens = _normalize_opens(opens_el.get_text(strip=True)) if opens_el else ""
        movies.append(Movie(
            title=title_el.get_text(strip=True),
            theater="IFC Center",
            url=url,
            status="Coming Soon",
            opens=opens,
        ))
    print(f"  Fetching detail pages for {len(movies)} IFC coming-soon films…")
    _apply_details(movies, _ifc_details, has_booking=True)
    return movies


# ---------------------------------------------------------------------------
# Film Forum
# ---------------------------------------------------------------------------

def _filmforum_week_schedule(soup) -> dict:
    """Weekly tab grid on /now_playing → {film_url: [title, [showtimes]]}.
    Tab i is today + i; each tab also carries an HTML comment with its day of month."""
    out: dict = {}
    container = soup.select_one("div.showtimes-container")
    if not container:
        return out
    today_obj = date.today()
    for i, tab in enumerate(container.select("div[id^=tabs-]")):
        d = today_obj + timedelta(days=i)
        com = tab.find(string=lambda s: isinstance(s, Comment))
        if com and com.strip().isdigit():
            for off in (0, -1, 1):
                cand = today_obj + timedelta(days=i + off)
                if cand.day == int(com.strip()):
                    d = cand
                    break
        for p in tab.select("p"):
            link = p.select_one("strong a[href*='/film/']")
            if not link:
                continue
            url = _abs(link["href"], "https://filmforum.org")
            entry = out.setdefault(url, [_filmforum_link_title(link), []])
            for s in p.select("span"):
                if "alert" in (s.get("class") or []):
                    continue
                raw = s.get_text(" ", strip=True)
                tm = norm_time(raw, bare_hours_pm=True)
                if tm:
                    entry[1].append(showtime(d.isoformat(), tm, url, "Open captions" if "(OC)" in raw.upper() else ""))
    return out


def scrape_filmforum() -> list:
    soup = _fetch("https://filmforum.org/now_playing")
    if not soup:
        return []
    movies = []
    for url, (title, sts) in _filmforum_week_schedule(soup).items():
        sts = sort_showtimes(sts)
        movies.append(Movie(
            title=title, theater="Film Forum", url=url,
            showtimes=sts, show_dates=sorted({s["date"] for s in sts}),
        ))
    print(f"  Fetching detail pages for {len(movies)} Film Forum now-playing films…")
    _apply_details(movies, _filmforum_details)
    return movies


def scrape_filmforum_coming_soon() -> list:
    movies = []
    soup = _fetch("https://filmforum.org/coming_soon")
    if not soup:
        return movies
    seen_urls: set = set()
    for card in soup.select("div.film-details"):
        link = card.select_one("h3 a[href*='/film/'], h3 a[href*='filmforum.org']")
        if not link:
            continue
        url = _abs(link["href"], "https://filmforum.org")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        opens_el = card.select_one("div.details p")
        opens = _normalize_opens(opens_el.get_text(strip=True)) if opens_el else ""
        movies.append(Movie(
            title=_filmforum_link_title(link),
            theater="Film Forum",
            url=url,
            status="Coming Soon",
            opens=opens,
        ))
    print(f"  Fetching detail pages for {len(movies)} Film Forum coming-soon films…")
    _apply_details(movies, _filmforum_details)
    return movies


# ---------------------------------------------------------------------------
# Metrograph
# ---------------------------------------------------------------------------

_JUNK_TITLE = re.compile(r"private event|closed for|theater (?:&|and) commissary|gift card|^membership", re.I)


def _metrograph_details(url: str) -> dict:
    """The listing's blurb is often just an event note ("Introduction by…"); the film
    page has the real synopsis."""
    soup = _fetch(url)
    if not soup:
        return {}
    info = soup.select_one("div.movie-info")
    if not info:
        return {}
    paras = []
    for p in info.find_all("p"):
        if p.find("p"):                          # wrapper <p> around the real paragraphs
            continue
        text = p.get_text(" ", strip=True)
        if text and not re.match(r"(Distributor:|Part of |Back to films)", text):
            paras.append(text)
    desc, notes = split_description(paras)
    return {"description": desc, "notes": notes}


def scrape_metrograph() -> list:
    movies = []
    soup = _fetch("https://metrograph.com/film/")
    if not soup:
        return movies
    seen_urls: set = set()
    for card in soup.select("div.homepage-in-theater-movie"):
        title_el = card.select_one("h3.movie_title a")
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        url = _abs(title_el["href"], "https://metrograph.com")
        if url in seen_urls or _JUNK_TITLE.search(title):
            continue
        seen_urls.add(url)

        director, year, fmt = "", "", ""
        for h5 in card.select("h5"):
            text = clean_text(h5.get_text(" ", strip=True))
            if text.lower().startswith("director:"):
                director = re.sub(r"\s+\.$", "", text.split(":", 1)[1].strip())
            elif re.match(r"\d{4}\s*/", text):
                parts = [x.strip() for x in text.split("/")]
                year = parts[0]
                fmt = next((x for x in parts[1:] if re.match(r"\d{2}mm$", x, re.I)), "")

        sts = []
        # Metrograph uses h6 for single-date films, h5 for multi-date films
        for date_el in card.select("div.showtimes h5, div.showtimes h6"):
            iso_date = _parse_show_date(date_el.get_text(" ", strip=True))
            day_div = date_el.find_next_sibling("div")
            if not iso_date or not day_div:
                continue
            for a in day_div.select("a[href*='t.metrograph.com']"):
                tm = norm_time(a.get_text(strip=True))
                if tm:
                    sold = "sold" in (a.get("class") or []) or "sold out" in (a.get("title") or "").lower()
                    sts.append(showtime(iso_date, tm, a["href"], "Sold out" if sold else fmt.lower()))
        sts = sort_showtimes(sts)

        synopsis_el = card.select_one("p.synopsis")
        desc, notes = split_description([synopsis_el.get_text(" ", strip=True)] if synopsis_el else [])
        if fmt:
            notes.append(f"Presented in {fmt.lower()}")

        movies.append(Movie(
            title=title,
            theater="Metrograph",
            url=url,
            booking_url=sts[0]["url"] if sts else "",
            year=year,
            director=director,
            description=desc,
            notes=notes,
            showtimes=sts,
            show_dates=sorted({s["date"] for s in sts}),
        ))
    print(f"  Fetching detail pages for {len(movies)} Metrograph films…")
    _apply_details(movies, _metrograph_details)
    return movies


# ---------------------------------------------------------------------------
# Film Noir Cinema
# ---------------------------------------------------------------------------

def scrape_filmnoircinema() -> list:
    """All upcoming screenings are on /program. Deduplicated by title."""
    soup = _fetch("https://www.filmnoircinema.com/program")
    if not soup:
        return []

    by_title: dict = defaultdict(list)
    for article in soup.select("article.eventlist-event--upcoming"):
        title_el = article.select_one("h1.eventlist-title a, h2.eventlist-title a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        url = _abs(title_el.get("href", ""), "https://www.filmnoircinema.com")

        month_el = article.select_one(".eventlist-datetag-startdate--month")
        day_el   = article.select_one(".eventlist-datetag-startdate--day")
        time_el  = article.select_one("time.event-time-12hr-start")
        show_date = ""
        if month_el and day_el:
            try:
                m_num = datetime.strptime(month_el.get_text(strip=True)[:3], "%b").month
                d_num = int(day_el.get_text(strip=True))
                show_date = f"{_infer_year(m_num, d_num)}-{m_num:02d}-{d_num:02d}"
            except (ValueError, TypeError):
                pass
        st = showtime(show_date, norm_time(time_el.get_text(" ", strip=True)) if time_el else "", url) \
            if show_date else None

        year, country, description, notes = "", "", "", []
        desc_div = article.select_one("div.eventlist-description")
        if desc_div:
            paras = [p.get_text(" ", strip=True) for p in desc_div.select("p") if p.get_text(strip=True)]
            if paras:
                meta_m = re.match(r"^([A-Za-z/ ]+?)[.,]?\s*(\d{4})\b", paras[0])
                if meta_m and len(paras[0]) < 120:
                    country = meta_m.group(1).strip()
                    year = meta_m.group(2)
                    paras = paras[1:]
                description, notes = split_description(paras)

        by_title[title.lower()].append({
            "title": title, "url": url, "showtime": st,
            "year": year, "country": country, "description": description, "notes": notes,
        })

    movies = []
    for entries in by_title.values():
        first = entries[0]
        sts = sort_showtimes([e["showtime"] for e in entries if e["showtime"]])
        movies.append(Movie(
            title=smart_title(first["title"]),
            theater="Film Noir Cinema",
            url=first["url"],
            booking_url=first["url"],
            year=first["year"],
            country=first["country"],
            description=first["description"],
            notes=first["notes"],
            showtimes=sts,
            show_dates=sorted({s["date"] for s in sts}),
        ))
    return movies


# ---------------------------------------------------------------------------
# Nitehawk Cinema — Williamsburg
# ---------------------------------------------------------------------------

def scrape_nitehawk_williamsburg() -> list:
    movies = []
    soup = _fetch("https://nitehawkcinema.com/williamsburg/")
    if not soup:
        return movies
    seen_urls: set = set()
    base = "https://nitehawkcinema.com"
    for item in soup.select("div.promo-item"):
        link = item.select_one("a[href*='/williamsburg/movies/']")
        url = _abs(link["href"], base) if link else base + "/williamsburg/"
        if url in seen_urls:
            continue
        seen_urls.add(url)
        raw = item.get_text(" ", strip=True)
        # Text is "Apr 18-19 FILM TITLE" — parse date range, strip from title
        date_start = date_end = ""
        m = re.match(r"^([A-Za-z]{3})\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\s+(.*)", raw)
        if m:
            try:
                month_num = datetime.strptime(m.group(1), "%b").month
                d1 = int(m.group(2))
                yr = _infer_year(month_num, d1)
                date_start = f"{yr}-{month_num:02d}-{d1:02d}"
                d2 = int(m.group(3)) if m.group(3) else d1
                date_end = f"{yr}-{month_num:02d}-{d2:02d}"
            except (ValueError, TypeError):
                pass
            title = m.group(4).strip()
        else:
            m2 = re.match(r"^[A-Za-z]{3}[\s\d\-–]+\s+(.*)", raw)
            title = (m2.group(1) if m2 else raw).strip()
        title = smart_title(title)
        if not title:
            continue
        # The promo grid sometimes links the wrong film page; fall back to the title's slug
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower().replace("&", " ")).strip("-")
        if not any(w in url for w in slug.split("-") if len(w) > 3):
            url = f"{base}/williamsburg/movies/{slug}/"
        movies.append(Movie(
            title=title, theater="Nitehawk (Williamsburg)", url=url,
            date_start=date_start, date_end=date_end,
        ))
    print(f"  Fetching detail pages for {len(movies)} Nitehawk Williamsburg films…")
    _apply_details(movies, _nitehawk_details)
    return movies


# ---------------------------------------------------------------------------
# BAM (Brooklyn Academy of Music)
# ---------------------------------------------------------------------------

_BAM_BASE = "https://www.bam.org"


def _bam_parse_date(date_text: str) -> tuple:
    """Return (status, opens, date_start, date_end) from a BAM date string."""
    dt = date_text.strip()
    if not dt or dt.lower() == "now playing":
        return "Now Playing", "", "", ""
    if re.match(r"Opens\s+", dt, re.IGNORECASE):
        return "Coming Soon", _normalize_opens(dt), "", ""
    today = date.today()
    yr_m = re.search(r"\b(20\d\d)\b", dt)
    year = int(yr_m.group(1)) if yr_m else today.year
    dates_found = []
    for m in re.finditer(r"\b([A-Z][a-z]{2})\s+(\d{1,2})\b", dt):
        try:
            dates_found.append(
                datetime.strptime(f"{m.group(1)} {m.group(2)} {year}", "%b %d %Y").date()
            )
        except ValueError:
            pass
    if not dates_found:
        return "Now Playing", "", "", ""
    start, end = min(dates_found), max(dates_found)
    if end < today:
        return "Now Playing", "", start.isoformat(), end.isoformat()
    if start > today:
        return "Coming Soon", _normalize_opens(dt), start.isoformat(), end.isoformat()
    return "Now Playing", "", start.isoformat(), end.isoformat()


def _bam_extract_details(soup) -> dict:
    """Extract director/year/description from a parsed BAM film page."""
    result = {}

    # Director + year live in the wider page body
    page_text = soup.get_text(" ", strip=True)
    dm = re.search(r"Directed by\s+(.+?)\s*\((\d{4})\)", page_text)
    if dm:
        result["director"] = dm.group(1).strip()
        result["year"] = dm.group(2)

    # Description is in div.description (distinct from the sponsor/body noise)
    desc_el = soup.select_one("div.description")
    if desc_el:
        paras = [p.get_text(" ", strip=True) for p in desc_el.find_all("p")] or [desc_el.get_text(" ", strip=True)]
        result["description"], result["notes"] = split_description(paras)

    # Individual screenings (with times + booking links) from the JSON-LD Event graph
    sts = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            ld = json.loads(script.string or "")
        except ValueError:
            continue
        for e in (ld.get("graph", [ld]) if isinstance(ld, dict) else []):
            start = e.get("startDate", "") if e.get("@type") == "Event" else ""
            if len(start) >= 16:
                tm = fmt_time(int(start[11:13]), int(start[14:16]))
                sts.append(showtime(start[:10], tm, (e.get("offers") or {}).get("url", "")))
    if sts:
        result["showtimes"] = sort_showtimes(sts)
        result["show_dates"] = sorted({s["date"] for s in sts})

    # Date range lives in div.bam-block-hero-date on detail pages
    hero_date = soup.select_one("div.bam-block-hero-date")
    if hero_date:
        date_text = hero_date.get_text(strip=True)
        _, _, date_start, date_end = _bam_parse_date(date_text)
        if date_start:
            result["date_start"] = date_start
        if date_end:
            result["date_end"] = date_end

    return result


def _bam_details(url: str) -> dict:
    soup = _fetch(url)
    return _bam_extract_details(soup) if soup else {}


def scrape_bam() -> list:
    listing_soup = _fetch(f"{_BAM_BASE}/film")
    if not listing_soup:
        return []

    # Collect entries — only /film/ paths (skip external /link/ redirects)
    listing_entries = []
    seen_listing: set = set()
    for block in listing_soup.select("div.productionblock"):
        h = block.select_one("h2,h3")
        a = block.select_one("a[href^='/film/']")
        p = block.select_one("p")
        if not (h and a):
            continue
        url = _abs(a["href"], _BAM_BASE)
        if url in seen_listing:
            continue
        seen_listing.add(url)
        listing_entries.append({
            "title":     h.get_text(strip=True),
            "url":       url,
            "date_text": p.get_text(strip=True) if p else "",
        })

    print(f"  Classifying {len(listing_entries)} BAM pages…")

    def process_entry(entry: dict):
        """
        Fetch one BAM page. Returns ('series', [child dicts]) if it's a
        series/retrospective, or ('film', Movie) if it's a single film.
        """
        soup = _fetch(entry["url"])
        if not soup:
            return None

        own_path = entry["url"].replace(_BAM_BASE, "")

        # Series detection: productionblocks with /film/ links distinct from
        # this page's own URL indicate individual films within a series.
        children = []
        for block in soup.select("div.productionblock"):
            child_a = block.select_one("a[href^='/film/']")
            if not child_a or child_a["href"] == own_path:
                continue
            child_h = block.select_one("h2,h3,h4")
            child_p = block.select_one("p")
            if child_h:
                children.append({
                    "title":     child_h.get_text(strip=True),
                    "url":       _abs(child_a["href"], _BAM_BASE),
                    "date_text": child_p.get_text(strip=True) if child_p else "",
                })

        # Single-film pages have a 3-film "Now Playing" sidebar that looks like
        # children. True series pages have 4+ children AND no "Directed by" credit.
        page_text = soup.get_text(" ", strip=True)
        has_director = bool(re.search(r"Directed by", page_text, re.IGNORECASE))
        if len(children) >= 4 and not has_director:
            return ("series", children)

        details = _bam_extract_details(soup)
        status, opens, date_start, date_end = _bam_parse_date(entry["date_text"])
        # Fall back to hero-date on detail page when listing has no usable dates
        if not date_start and details.get("date_start"):
            date_start = details["date_start"]
            date_end = details.get("date_end", date_start)
        return ("film", Movie(
            title=smart_title(entry["title"]),
            theater="BAM",
            url=entry["url"],
            status=status,
            opens=opens,
            date_start=date_start,
            date_end=date_end,
            showtimes=details.get("showtimes", []),
            show_dates=details.get("show_dates", []),
            director=details.get("director", ""),
            year=details.get("year", ""),
            description=details.get("description", ""),
            notes=details.get("notes", []),
        ))

    # Run concurrently; collect results in main thread (no shared mutation)
    regular_movies: list[Movie] = []
    series_children: list[dict] = []
    seen_urls: set = set()

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(process_entry, e): e for e in listing_entries}
        for f in as_completed(futures):
            try:
                result = f.result()
                if result is None:
                    continue
                kind, payload = result
                if kind == "series":
                    for c in payload:
                        if c["url"] not in seen_urls:
                            seen_urls.add(c["url"])
                            series_children.append(c)
                else:
                    if payload.url not in seen_urls:
                        seen_urls.add(payload.url)
                        regular_movies.append(payload)
            except Exception as e:
                print(f"[warn] BAM: {e}", file=sys.stderr)

    # Build Movie objects for series children, then fetch their detail pages
    child_movies: list[Movie] = []
    today_iso = date.today().isoformat()
    for c in series_children:
        status, opens, date_start, date_end = _bam_parse_date(c["date_text"])
        if date_end and date_end < today_iso:
            continue        # past screening in a series archive — skip the detail fetch
        child_movies.append(Movie(
            title=smart_title(c["title"]),
            theater="BAM",
            url=c["url"],
            status=status,
            opens=opens,
            date_start=date_start,
            date_end=date_end,
        ))

    if child_movies:
        print(f"  Fetching details for {len(child_movies)} BAM series films…")
        _apply_details(child_movies, _bam_details)

    return regular_movies + child_movies


# ---------------------------------------------------------------------------
# Paris Theater  (Next.js — film records embedded as JSON in RSC inline scripts)
# ---------------------------------------------------------------------------

_PARIS_BASE = "https://www.paristheaternyc.com"


def _paris_film_records(page_html: str) -> dict:
    """Decode every {"id":…,"attributes":{"FilmName":…}} record in the RSC payload → {slug: attributes}.
    Each film's own record carries its synopsis/poster; the page also embeds the featured
    film everywhere, so field-name regexes over the whole page return the wrong synopsis."""
    content = ""
    for s in re.findall(r"<script>self\.__next_f\.push\((\[.*?\])\)</script>", page_html, re.DOTALL):
        try:
            data = json.loads(s)
            if len(data) >= 2 and isinstance(data[1], str):
                content += data[1]
        except ValueError:
            pass
    films: dict = {}
    dec = json.JSONDecoder()
    for m in re.finditer(r'\{"id":\d+,"attributes":\{"FilmName"', content):
        try:
            obj, _ = dec.raw_decode(content, m.start())
        except ValueError:
            continue
        a = obj.get("attributes") or {}
        slug = a.get("Slug")
        if not slug:
            continue
        # The same film appears several times; keep the most complete copy
        if slug not in films or len(json.dumps(a)) > len(json.dumps(films[slug])):
            films[slug] = a
    return films


def _paris_image(field) -> str:
    try:
        attrs = field["data"]["attributes"]
    except (TypeError, KeyError):
        return ""
    fmts = attrs.get("formats") or {}
    return (fmts.get("medium") or fmts.get("small") or {}).get("url") or attrs.get("url", "")


def scrape_paris() -> list:
    try:
        r = requests.get(_PARIS_BASE + "/", headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}, timeout=15)
    except requests.RequestException as e:
        print(f"[warn] Paris Theater: {e}", file=sys.stderr)
        return []
    if r.status_code != 200:
        print(f"[warn] Paris Theater: HTTP {r.status_code}", file=sys.stderr)
        return []

    today = date.today()
    movies = []
    for slug, a in _paris_film_records(r.text).items():
        try:
            open_date = date.fromisoformat(a["OpeningDate"]) if a.get("OpeningDate") else None
            close_date = date.fromisoformat(a["ClosingDate"]) if a.get("ClosingDate") else None
        except ValueError:
            open_date = close_date = None
        if not open_date or (close_date and close_date < today):
            continue
        status = "Coming Soon" if open_date > today else "Now Playing"
        fmt = (a.get("FilmFormat") or "").strip()
        label = fmt.lower() if re.match(r"\d{2}mm$", fmt, re.I) else ""

        desc, notes = split_description(a.get("SynopsisWYSIWYG") or a.get("Synopsis") or "")
        if label:
            notes.insert(0, f"Presented in {label}")

        # Special events carry their own date + time + ticket link
        sts = []
        for ev in ((a.get("events") or {}).get("data") or []):
            ea = ev.get("attributes") or {}
            if ea.get("EventDate") and ea["EventDate"] >= today.isoformat():
                sts.append(showtime(ea["EventDate"], norm_time(ea.get("EventTime") or ""),
                                    ea.get("TicketLink") or "", _short_label(ea.get("EventName") or "")))
                if ea.get("EventName"):
                    notes.append(clean_text(ea["EventName"]))

        movies.append(Movie(
            title=clean_text(a.get("FilmName", "")),
            theater="Paris Theater",
            url=f"{_PARIS_BASE}/film/{slug}",
            status=status,
            opens=f"{_MONTH_FULL[open_date.month]} {open_date.day}" if status == "Coming Soon" else "",
            year=a.get("Year") or "",
            director=clean_text(a.get("Director") or ""),
            cast=", ".join(x.strip() for x in (a.get("Cast") or "").split(",") if x.strip()),
            description=desc or clean_text(a.get("Synopsis") or ""),
            notes=notes,
            poster=_paris_image(a.get("Poster")),
            showtimes=sort_showtimes(sts),
            # Daily times live behind the (bot-blocked) ticketing site, so list every run date
            show_dates=[(open_date + timedelta(days=i)).isoformat()
                        for i in range(min(((close_date or open_date) - open_date).days + 1, 60))],
            date_start=open_date.isoformat(),
            date_end=(close_date or open_date).isoformat(),
        ))
    return movies


# ---------------------------------------------------------------------------
# Film at Lincoln Center  (server-rendered listing page)
# ---------------------------------------------------------------------------

def scrape_filmlinc() -> list:
    movies = []
    soup = _fetch("https://www.filmlinc.org/")
    if not soup:
        return movies
    seen_urls: set = set()
    # Each film card is a div.py-8 (Tailwind) containing an /films/ link
    for card in soup.select("div.py-8, div.py-6"):
        link = card.select_one("a[href^='/films/']")
        if not link:
            continue
        title = link.get_text(strip=True)
        if not title or title in ("Get Tickets", "Learn More"):
            continue
        url = _abs(link["href"], "https://www.filmlinc.org")
        if url in seen_urls:
            continue
        seen_urls.add(url)

        card_text = card.get_text(" ", strip=True)
        opens_m = re.search(r"Opens\s+(.+?)(?:\s+with|\s+at|\.|$)", card_text, re.IGNORECASE)
        premiere_m = re.search(r"(World Premiere[^\.]*)", card_text, re.IGNORECASE)
        if opens_m or premiere_m:
            raw_opens = (opens_m.group(1) if opens_m else premiere_m.group(1)).strip()
            status = "Coming Soon"
            opens  = _normalize_opens(raw_opens)
        else:
            status = "Now Playing"
            opens  = ""

        # FLC listing shows today's showtimes
        today_iso = date.today().isoformat()
        sts = [showtime(today_iso, norm_time(t), url)
               for t in re.findall(r"\d{1,2}:\d{2}\s*(?:AM|PM)", card_text, re.IGNORECASE)] \
            if status == "Now Playing" else []
        movies.append(Movie(
            title=smart_title(title),
            theater="Film at Lincoln Center",
            url=url,
            status=status,
            opens=opens,
            showtimes=sort_showtimes(sts),
            show_dates=[today_iso] if sts else [],
        ))
    return movies


# ---------------------------------------------------------------------------
# Angelika Film Center  (Reading Cinemas API — no Playwright needed)
# ---------------------------------------------------------------------------

_ANGELIKA_API   = "https://production-api.readingcinemas.com"
_ANGELIKA_CNTRY = "6"
_ANGELIKA_ID    = "0000000005"
_ANGELIKA_BASE  = "https://angelikafilmcenter.com/nyc"


def _angelika_token() -> str:
    r = requests.get(
        f"{_ANGELIKA_API}/settings/{_ANGELIKA_CNTRY}",
        headers={"Origin": _ANGELIKA_BASE},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["data"]["settings"]["token"]


def _angelika_films(status: str, token: str) -> list:
    # nowShowing uses cinemaId; comingSoon uses flag for cinema filtering
    param_key = "cinemaId" if status == "nowShowing" else "flag"
    r = requests.get(
        f"{_ANGELIKA_API}/films",
        params={"countryId": _ANGELIKA_CNTRY, param_key: _ANGELIKA_ID, "status": status},
        headers={"Authorization": f"Bearer {token}", "Origin": _ANGELIKA_BASE},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("data", [])


def _parse_angelika_films(raw: list, status: str) -> list:
    movies = []
    for f in raw:
        title = clean_text(f.get("name") or f.get("movieName") or "")
        if not title:
            continue
        slug = f.get("movieSlug") or ""
        url = f"{_ANGELIKA_BASE}/movies/{slug}" if slug else _ANGELIKA_BASE

        release = f.get("release_date", "")
        opens = ""
        if release and release != "Invalid date":
            try:
                d = datetime.strptime(release, "%Y-%m-%d")
                opens = f"{_MONTH_FULL[d.month]} {d.day}"
            except ValueError:
                pass

        sts = []
        showdates = f.get("showdates") or []
        if isinstance(showdates, list):
            for sd in showdates:
                for st_type in sd.get("showtypes", []):
                    kind = (st_type.get("type") or "").strip()
                    label = kind if kind and kind.lower() not in ("standard", "2d", "digital", "regular") else ""
                    for st in st_type.get("showtimes", []):
                        try:
                            # Normalize short UTC offset "-04" → "-04:00" for Python 3.9
                            dt = datetime.fromisoformat(re.sub(r"([+-]\d{2})$", r"\1:00", st.get("date_time", "")))
                        except ValueError:
                            continue
                        lab = "Sold out" if st.get("soldout") else label
                        sts.append(showtime(dt.strftime("%Y-%m-%d"), fmt_time(dt.hour, dt.minute), url, lab))
        sts = sort_showtimes(sts)

        desc, notes = split_description(f.get("synopsis") or "")
        movies.append(Movie(
            title=title,
            theater="Angelika Film Center",
            url=url,
            booking_url=url,
            status=status,
            opens=opens,
            director=clean_text(f.get("director") or ""),
            cast=clean_text(f.get("cast") or "").rstrip(","),
            description=desc,
            notes=notes,
            poster=f.get("poster_image") or "",
            showtimes=sts,
            show_dates=sorted({s["date"] for s in sts}),
        ))
    return movies


def scrape_angelika() -> list:
    try:
        token = _angelika_token()
        raw = _angelika_films("nowShowing", token)
        return _parse_angelika_films(raw, "Now Playing")
    except Exception as e:
        print(f"[warn] Angelika Film Center: {e}", file=sys.stderr)
        return []


def scrape_angelika_coming_soon() -> list:
    try:
        token = _angelika_token()
        raw = _angelika_films("comingSoon", token)
        return _parse_angelika_films(raw, "Coming Soon")
    except Exception as e:
        print(f"[warn] Angelika Film Center (coming soon): {e}", file=sys.stderr)
        return []


# ---------------------------------------------------------------------------
# Alamo Drafthouse NYC
# ---------------------------------------------------------------------------

_ALAMO_UA      = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_ALAMO_MARKET  = "https://drafthouse.com/s/mother/v2/schedule/market/nyc"
_ALAMO_PRES    = "https://drafthouse.com/s/mother/v2/schedule/presentation/{slug}"
_ALAMO_SHOW    = "https://drafthouse.com/nyc/show/{slug}"

# Slugs with these prefixes are format variants or special-event wrappers — skip them
_ALAMO_SKIP_PREFIXES = ("special-event-", "hdr-by-barco-", "advance-screening-")


def _alamo_get(url: str) -> dict:
    r = requests.get(url, headers={"User-Agent": _ALAMO_UA}, timeout=15)
    r.raise_for_status()
    return r.json()


def _alamo_fetch_detail(slug: str) -> dict:
    """Return show data + session dates from the presentation detail endpoint."""
    try:
        d = _alamo_get(_ALAMO_PRES.format(slug=slug))
        pres = d["data"]["presentation"]
        sessions = pres.get("sessions") or []
        show_dates = sorted({
            s["dateTime"][:10]
            for s in sessions
            if s.get("dateTime")
        })
        return {
            "show": pres["show"],
            "openingDateClt": pres.get("openingDateClt"),
            "show_dates": show_dates,
        }
    except Exception:
        return {}


def scrape_alamo() -> list:
    try:
        data = _alamo_get(_ALAMO_MARKET)["data"]
    except Exception as e:
        print(f"[warn] Alamo Drafthouse: {e}", file=sys.stderr)
        return []

    today = date.today().isoformat()
    cinemas = {c["id"]: c.get("name", "") for c in (data.get("market") or [{}])[0].get("cinemas", [])} \
        if isinstance(data.get("market"), list) else {}
    pres_by_slug = {p.get("slug", ""): p for p in data.get("presentations", [])}

    # Sessions per presentation, labelled with the venue (NYC has several Alamos)
    used_cinemas = {s.get("cinemaId") for s in data.get("sessions", [])}
    multi_venue = len(used_cinemas) > 1
    slug_sessions: dict = defaultdict(list)
    for s in data.get("sessions", []):
        slug, when = s.get("presentationSlug", ""), s.get("showTimeClt", "")
        if not slug or len(when) < 16 or s.get("isHidden"):
            continue
        pres = pres_by_slug.get(slug, {})
        bits = []
        if slug.startswith("hdr-by-barco-"):
            bits.append("HDR")
        sup = pres.get("superTitle")
        sup = sup.get("superTitle") if isinstance(sup, dict) else sup
        if isinstance(sup, str) and sup.strip():
            bits.append(clean_text(sup))
        if multi_venue and cinemas.get(s.get("cinemaId")):
            bits.append(cinemas[s["cinemaId"]])
        if s.get("status") == "SOLDOUT":
            bits.append("Sold out")
        slug_sessions[slug].append(showtime(
            when[:10], fmt_time(int(when[11:13]), int(when[14:16])),
            _ALAMO_SHOW.format(slug=slug), " · ".join(bits)))

    # One film per title: the same movie is often listed under several presentations
    # (its own page plus series like "Terror Tuesday"), each with its own sessions
    groups: dict = defaultdict(list)
    for slug, p in pres_by_slug.items():
        title = (p.get("show") or {}).get("title", "").strip()
        if title and not slug.startswith(("special-event-", "advance-screening-")) or \
                (title and slug_sessions.get(slug)):
            groups[title.lower()].append(p)

    def _build(group: list) -> Optional[Movie]:
        # Prefer the film's own presentation (slug == show slug) for the write-up
        group.sort(key=lambda p: (p.get("slug") != (p.get("show") or {}).get("slug"), p.get("slug", "")))
        main = group[0]
        detail = _alamo_fetch_detail(main["slug"])
        show = detail.get("show") or main.get("show") or {}
        desc, notes = split_description(show.get("description") or "")
        release = show.get("nationalReleaseDateUtc") or ""
        directors = show.get("directors") or []
        openings = sorted(p["openingDateClt"] for p in group if p.get("openingDateClt"))
        opening = openings[0] if openings else ""
        sts = sort_showtimes([st for p in group for st in slug_sessions.get(p["slug"], [])])
        status = "Coming Soon" if (opening and opening[:10] > today and not any(
            st["date"] <= today for st in sts)) else "Now Playing"
        posters = (show.get("posterImages") or [{}])
        url = _ALAMO_SHOW.format(slug=main["slug"])
        return Movie(
            title=clean_text(show.get("title") or main["show"].get("title", "")),
            theater="Alamo Drafthouse NYC",
            url=url,
            booking_url=url,
            status=status,
            opens=_normalize_opens(opening) if status == "Coming Soon" else "",
            director=", ".join(d.get("name", "") if isinstance(d, dict) else d for d in directors),
            year=release[:4],
            description=desc,
            notes=notes,
            poster=(posters[0] or {}).get("uri", ""),
            showtimes=sts,
            show_dates=sorted({st["date"] for st in sts} | set(detail.get("show_dates", []))),
            date_start=opening[:10],
        )

    movies = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_build, g) for g in groups.values()]
        for fut in as_completed(futures):
            try:
                m = fut.result()
                if m:
                    movies.append(m)
            except Exception as e:
                print(f"[warn] Alamo Drafthouse: {e}", file=sys.stderr)
    return movies


# ---------------------------------------------------------------------------
# HK Cinemas (Cobble Hill · Williamsburg · Kew Gardens · Mamaroneck)
# ---------------------------------------------------------------------------

_HK_API     = "https://api-v3.mobilemoviegoing.cloud/include/app/get_films.php"
_HK_EID     = "9c37778e-4da1-436b-96c2-e0221eca51ff"
_HK_HEADERS = {
    "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin":       "https://www.hk-cinemas.com",
    "Referer":      "https://www.hk-cinemas.com/",
    "Content-Type": "application/json",
}
_HK_LOCATIONS = {
    "Cobble Hill Cinemas":  "00001-00001-00001",
    "Williamsburg Cinemas": "00001-00001-00002",
    "Kew Gardens Cinema":   "00001-00001-00003",
    "Mamaroneck Cinemas":   "00001-00001-00004",
}


def _scrape_hk(theater: str, location_id: str, mode: str) -> list:
    status = "Now Playing" if mode == "now" else "Coming Soon"
    try:
        r = requests.post(
            _HK_API,
            headers=_HK_HEADERS,
            json={"mode": mode, "eid": _HK_EID, "location": location_id, "pos_route": 1},
            timeout=15,
        )
        r.raise_for_status()
        raw = r.json().get("films") or []
    except Exception as e:
        print(f"[warn] HK Cinemas ({theater}): {e}", file=sys.stderr)
        return []

    movies = []
    for f in raw:
        if not f:
            continue
        title = (f.get("dispName") or "").strip()
        if not title:
            continue
        try:
            dirs = json.loads(f.get("Director") or "[]")
        except (ValueError, TypeError):
            dirs = []
        description, notes = split_description(f.get("Synopsis") or "")
        opens = _normalize_opens(f.get("ReleaseDate") or "")
        url = "https://www.hk-cinemas.com"
        # schedDates format: ["202604200000", "202604210000", ...] (YYYYMMDDHHNN)
        show_dates: list = []
        raw_sched = f.get("schedDates") or "[]"
        try:
            sched_list = json.loads(raw_sched) if isinstance(raw_sched, str) else raw_sched
            for dt_str in sched_list:
                s = str(dt_str)
                if len(s) >= 8:
                    show_dates.append(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        except (ValueError, TypeError):
            pass
        movies.append(Movie(
            title=title,
            theater=theater,
            url=url,
            booking_url=url,
            status=status,
            opens=opens if status == "Coming Soon" else "",
            director=", ".join(dirs),
            description=description,
            notes=notes,
            show_dates=sorted(set(show_dates)),
        ))
    return movies


def scrape_hk_cobblehill()       -> list: return _scrape_hk("Cobble Hill Cinemas",  "00001-00001-00001", "now")
def scrape_hk_cobblehill_soon()  -> list: return _scrape_hk("Cobble Hill Cinemas",  "00001-00001-00001", "advance")
def scrape_hk_williamsburg()     -> list: return _scrape_hk("Williamsburg Cinemas", "00001-00001-00002", "now")
def scrape_hk_williamsburg_soon()-> list: return _scrape_hk("Williamsburg Cinemas", "00001-00001-00002", "advance")
def scrape_hk_kewgardens()       -> list: return _scrape_hk("Kew Gardens Cinema",   "00001-00001-00003", "now")
def scrape_hk_kewgardens_soon()  -> list: return _scrape_hk("Kew Gardens Cinema",   "00001-00001-00003", "advance")
def scrape_hk_mamaroneck()       -> list: return _scrape_hk("Mamaroneck Cinemas",   "00001-00001-00004", "now")
def scrape_hk_mamaroneck_soon()  -> list: return _scrape_hk("Mamaroneck Cinemas",   "00001-00001-00004", "advance")


# ---------------------------------------------------------------------------
# Low Cinema (Ridgewood, Queens)
# ---------------------------------------------------------------------------

_LOW_BASE = "https://lowcinema.com"


def scrape_lowcinema() -> list:
    soup = _fetch(f"{_LOW_BASE}/tickets/")
    if not soup:
        return []

    cards = soup.select("div.movie-card")
    movies = []

    def _parse_card(card) -> Optional[Movie]:
        title_tag = card.select_one("h2.movie-title a")
        if not title_tag:
            return None
        title = title_tag.get_text(strip=True)
        rel_url = title_tag.get("href", "")
        film_url = f"{_LOW_BASE}{rel_url}"

        # Earliest booking link as the canonical ticket URL
        first_link = card.select_one("a.showtime-link")
        booking_url = f"{_LOW_BASE}{first_link['href']}" if first_link else film_url
        # Show dates from the date-grid squares (data-date="YYYY-MM-DD"); each date has a
        # matching panel (id="panel-<movie>-<date>") listing that day's showtime links
        show_dates_set: set = {
            el["data-date"]
            for el in card.select("span.date-square[data-date]")
            if el.get("data-date")
        }
        sts = []
        for panel in card.select("div.showtimes-panel"):
            dm = re.search(r"(\d{4}-\d{2}-\d{2})$", panel.get("id", ""))
            if not dm:
                continue
            for a in panel.select("a.showtime-link"):
                tm = norm_time(a.get_text(" ", strip=True))
                if tm:
                    sts.append(showtime(dm.group(1), tm, _abs(a.get("href", ""), _LOW_BASE)))
        poster_el = card.select_one("div.movie-poster img")

        # Fetch film detail page for director/year/country/description
        detail = _fetch(film_url)
        director = year = country = description = ""
        notes = []
        if detail:
            meta = detail.find("meta", {"name": "description"})
            if meta and meta.get("content"):
                # Format: "Dir. X, YEAR, COUNTRY, ..., MIN. <description>"
                content = meta["content"]
                dir_m = re.match(r"Dir\.\s*([^,]+),\s*(\d{4}),\s*([^,]+),", content)
                if dir_m:
                    director = dir_m.group(1).strip()
                    year     = dir_m.group(2).strip()
                    country  = dir_m.group(3).strip()

            desc_tag = detail.select_one("div.movie-description:not(.mobile-description)")
            if desc_tag:
                # First <p> is the "Dir. X, YEAR..." metadata — skip it; take remaining paragraphs
                paras = desc_tag.find_all("p")
                body_paras = [p.get_text(" ", strip=True) for p in paras[1:] if p.get_text(strip=True)]
                description, notes = split_description(body_paras)

        return Movie(
            title=title,
            theater="Low Cinema",
            url=film_url,
            booking_url=booking_url,
            status="Now Playing",
            director=director,
            year=year,
            country=country,
            description=description,
            notes=notes,
            poster=poster_el.get("src", "") if poster_el else "",
            showtimes=sort_showtimes(sts),
            show_dates=sorted(show_dates_set | {st["date"] for st in sts}),
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_parse_card, card): card for card in cards}
        for fut in as_completed(futures):
            try:
                m = fut.result()
                if m:
                    movies.append(m)
            except Exception as e:
                print(f"[warn] Low Cinema: {e}", file=sys.stderr)

    return movies


# ---------------------------------------------------------------------------
# Theater registry  ← add new theaters here
# ---------------------------------------------------------------------------
#
# Each entry: (display_name, now_playing_fn, coming_soon_fn or None)
# coming_soon_fn is called separately; return [] if not supported.

THEATERS: list[tuple] = [
    ("Nitehawk (Prospect Park)",  scrape_nitehawk,               scrape_nitehawk_coming_soon),
    ("Nitehawk (Williamsburg)",   scrape_nitehawk_williamsburg,  None),
    ("IFC Center",                scrape_ifc,                    scrape_ifc_coming_soon),
    ("Film Forum",                scrape_filmforum,              scrape_filmforum_coming_soon),
    ("Metrograph",                scrape_metrograph,             None),
    ("Film Noir Cinema",          scrape_filmnoircinema,         None),
    ("BAM",                       scrape_bam,                    None),
    ("Paris Theater",             scrape_paris,                  None),
    ("Film at Lincoln Center",    scrape_filmlinc,               None),
    ("Angelika Film Center",      scrape_angelika,               scrape_angelika_coming_soon),
    ("Alamo Drafthouse NYC",      scrape_alamo,                  None),
    ("Low Cinema",                 scrape_lowcinema,              None),
    ("Cobble Hill Cinemas",       scrape_hk_cobblehill,          scrape_hk_cobblehill_soon),
    ("Williamsburg Cinemas",      scrape_hk_williamsburg,        scrape_hk_williamsburg_soon),
    ("Kew Gardens Cinema",        scrape_hk_kewgardens,          scrape_hk_kewgardens_soon),
    ("Mamaroneck Cinemas",        scrape_hk_mamaroneck,          scrape_hk_mamaroneck_soon),
]

JS_THEATERS: list[tuple] = []  # all theaters now use direct HTTP scraping


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def _upcoming(movie: Movie) -> list:
    today = date.today().isoformat()
    return [st for st in movie.showtimes if st.get("date", "") >= today]


def _fmt_showtimes(movie: Movie, limit: int = 5) -> str:
    upcoming = _upcoming(movie)
    if upcoming:
        lines = [fmt_showtime(st) for st in upcoming[:limit]]
        if len(upcoming) > limit:
            lines.append(f"+{len(upcoming) - limit} more")
        return "\n".join(lines)
    return movie.opens or "—"


def display_rich(movies: list) -> None:
    console = Console()
    table = Table(
        title=f"NYC Indie Movie Listings — {datetime.now().strftime('%b %d, %Y')}",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Title", style="bold cyan", min_width=22)
    table.add_column("Status", min_width=11)
    table.add_column("Year", style="yellow", min_width=5)
    table.add_column("Director", min_width=14)
    table.add_column("Cast", min_width=16)
    table.add_column("Country", min_width=8)
    table.add_column("Theater", style="green", min_width=14)
    table.add_column("Showtimes / Opens", min_width=18)

    for m in sorted(movies, key=lambda x: (x.status, x.title.lower())):
        status_str = f"[yellow]{m.status}[/yellow]" if m.status == "Coming Soon" else f"[green]{m.status}[/green]"
        table.add_row(
            m.title, status_str, m.year or "—", m.director or "—",
            m.cast or "—", m.country or "—", m.theater, _fmt_showtimes(m),
        )

    now_p  = sum(1 for m in movies if m.status == "Now Playing")
    soon_p = sum(1 for m in movies if m.status == "Coming Soon")
    theaters = len({m.theater for m in movies})
    console.print(table)
    console.print(f"\n[dim]{now_p} now playing · {soon_p} coming soon · {theaters} theaters[/dim]")


def display_plain(movies: list) -> None:
    for m in sorted(movies, key=lambda x: (x.status, x.title.lower())):
        print(f"\n{'─' * 60}")
        print(f"TITLE:    {m.title}")
        print(f"STATUS:   {m.status}{(' — ' + m.opens) if m.opens else ''}")
        print(f"YEAR:     {m.year or '—'}    COUNTRY: {m.country or '—'}")
        print(f"DIRECTOR: {m.director or '—'}")
        print(f"CAST:     {m.cast or '—'}")
        print(f"THEATER:  {m.theater}")
        if _upcoming(m):
            print(f"TIMES:    {', '.join(fmt_showtime(st) for st in _upcoming(m)[:5])}")
        if m.letterboxd_url:
            print(f"LBXD:     {m.letterboxd_url}")
        if m.booking_url:
            print(f"BOOK:     {m.booking_url}")
    print(f"\n{len(movies)} films found.")


def save_json(movies: list, path: str = "movies.json") -> None:
    data = {
        "fetched_at": datetime.now().isoformat(),
        "movies": [asdict(m) for m in sorted(movies, key=lambda x: (x.status, x.title.lower()))],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def save_html(movies: list, path: str = "movies.html") -> None:
    theater_options = "".join(
        f'<option value="{t}">{t}</option>'
        for t in sorted({m.theater for m in movies})
    )
    now_p  = sum(1 for m in movies if m.status == "Now Playing")
    soon_p = sum(1 for m in movies if m.status == "Coming Soon")

    rows = ""
    for m in sorted(movies, key=lambda x: (x.status, x.title.lower())):
        if _upcoming(m):
            times_html = "<br>".join(_html.escape(fmt_showtime(st)) for st in _upcoming(m)[:8])
        elif m.opens:
            times_html = f'<span class="opens-date">{m.opens}</span>'
        else:
            times_html = "—"
        safe_desc = m.description[:220].replace("<", "&lt;").replace(">", "&gt;")
        book_btn  = f'<a class="book-btn" href="{m.booking_url}" target="_blank">Book</a>' \
                    if m.booking_url else "—"
        status_cls = "badge-now" if m.status == "Now Playing" else "badge-soon"
        rows += f"""
        <tr data-status="{m.status}">
          <td><a href="{m.url}" target="_blank">{_html.escape(m.title)}</a>{f' <a href="{m.letterboxd_url}" target="_blank" title="Letterboxd">↗</a>' if m.letterboxd_url else ""}</td>
          <td class="status-cell"><span class="badge {status_cls}">{m.status}</span></td>
          <td class="year">{m.year or "—"}</td>
          <td class="director">{m.director or "—"}</td>
          <td class="cast">{m.cast or "—"}</td>
          <td class="country">{m.country or "—"}</td>
          <td class="theater">{m.theater}</td>
          <td class="times">{times_html}</td>
          <td class="booking">{book_btn}</td>
          <td class="desc">{safe_desc}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NYC Indie Movies — {datetime.now().strftime('%b %d, %Y')}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 1500px; margin: 2rem auto; padding: 0 1.5rem; color: #1a1a1a; background: #f5f5f5; }}
  h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: .3rem; }}
  .meta {{ color: #666; font-size: .85rem; margin-bottom: 1.25rem; }}
  .filter-bar {{ display: flex; gap: .6rem; flex-wrap: wrap; align-items: center; margin-bottom: 1rem; }}
  input[type=search], select {{ padding: 7px 11px; border: 1px solid #ccc; border-radius: 6px; font-size: .88rem; background: #fff; }}
  input[type=search] {{ width: 240px; }}
  .count {{ color: #888; font-size: .82rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.12); overflow: hidden; }}
  thead th {{ background: #111; color: #fff; padding: 10px 13px; text-align: left; white-space: nowrap; cursor: pointer; user-select: none; font-size: .88rem; font-weight: 600; }}
  thead th:hover {{ background: #333; }}
  thead th.sorted-asc::after  {{ content: " ▲"; font-size: .7em; opacity: .8; }}
  thead th.sorted-desc::after {{ content: " ▼"; font-size: .7em; opacity: .8; }}
  thead th.no-sort {{ cursor: default; }}
  td {{ padding: 9px 13px; border-bottom: 1px solid #eee; vertical-align: top; font-size: .88rem; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f0f5ff; }}
  a {{ color: #1a56db; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: .75rem; font-weight: 600; white-space: nowrap; }}
  .badge-now  {{ background: #d1fae5; color: #065f46; }}
  .badge-soon {{ background: #fef3c7; color: #92400e; }}
  .status-cell {{ white-space: nowrap; }}
  .year     {{ white-space: nowrap; color: #555; font-weight: 600; }}
  .director {{ color: #444; }}
  .cast     {{ color: #444; font-size: .82rem; max-width: 180px; }}
  .country  {{ white-space: nowrap; color: #555; font-size: .82rem; }}
  .theater  {{ white-space: nowrap; color: #555; font-size: .82rem; }}
  .times    {{ font-size: .78rem; color: #555; white-space: nowrap; line-height: 1.6; }}
  .opens-date {{ font-style: italic; color: #92400e; }}
  .desc     {{ color: #555; font-size: .82rem; max-width: 220px; line-height: 1.5; }}
  .booking  {{ white-space: nowrap; }}
  .book-btn {{
    display: inline-block; padding: 4px 11px; background: #111; color: #fff !important;
    border-radius: 4px; font-size: .78rem; font-weight: 600; letter-spacing: .02em;
    text-decoration: none !important; transition: background .15s;
  }}
  .book-btn:hover {{ background: #444; }}
</style>
</head>
<body>
<h1>NYC Indie Movie Listings</h1>
<p class="meta">
  Fetched {datetime.now().strftime('%B %d, %Y at %-I:%M %p')} &mdash;
  {now_p} now playing · {soon_p} coming soon · {len({m.theater for m in movies})} theaters
</p>

<div class="filter-bar">
  <input type="search" id="search" placeholder="Search title, director, cast, country…" oninput="filterTable()">
  <select id="theater-filter" onchange="filterTable()">
    <option value="">All theaters</option>
    {theater_options}
  </select>
  <select id="status-filter" onchange="filterTable()">
    <option value="">All screenings</option>
    <option value="Now Playing">Now Playing</option>
    <option value="Coming Soon">Coming Soon</option>
  </select>
  <span class="count" id="count">{len(movies)} films</span>
</div>

<table id="movies">
  <thead>
    <tr>
      <th onclick="sortTable(0)">Title</th>
      <th onclick="sortTable(1)">Status</th>
      <th onclick="sortTable(2)">Year</th>
      <th onclick="sortTable(3)">Director</th>
      <th onclick="sortTable(4)">Cast</th>
      <th onclick="sortTable(5)">Country</th>
      <th onclick="sortTable(6)">Theater</th>
      <th class="no-sort">Showtimes / Opens</th>
      <th class="no-sort">Book</th>
      <th onclick="sortTable(9)">Description</th>
    </tr>
  </thead>
  <tbody>{rows}
  </tbody>
</table>

<script>
let sortCol = -1, sortDir = 1;

function sortTable(col) {{
  const ths = document.querySelectorAll('#movies thead th');
  ths.forEach(th => th.className = th.classList.contains('no-sort') ? 'no-sort' : '');
  if (sortCol === col) {{ sortDir *= -1; }} else {{ sortDir = 1; sortCol = col; }}
  ths[col].className = sortDir === 1 ? 'sorted-asc' : 'sorted-desc';
  const tbody = document.querySelector('#movies tbody');
  const rows = Array.from(tbody.rows).filter(r => r.style.display !== 'none');
  rows.sort((a, b) => a.cells[col].textContent.localeCompare(b.cells[col].textContent) * sortDir);
  rows.forEach(r => tbody.appendChild(r));
}}

function filterTable() {{
  const q       = document.getElementById('search').value.toLowerCase();
  const theater = document.getElementById('theater-filter').value.toLowerCase();
  const status  = document.getElementById('status-filter').value;
  let count = 0;
  document.querySelectorAll('#movies tbody tr').forEach(row => {{
    const title    = row.cells[0].textContent.toLowerCase();
    const rowStat  = row.dataset.status;
    const director = row.cells[3].textContent.toLowerCase();
    const cast     = row.cells[4].textContent.toLowerCase();
    const country  = row.cells[5].textContent.toLowerCase();
    const th       = row.cells[6].textContent.toLowerCase();
    const desc     = row.cells[9].textContent.toLowerCase();
    const match =
      (!q || title.includes(q) || director.includes(q) || cast.includes(q) || country.includes(q) || desc.includes(q)) &&
      (!theater || th.includes(theater)) &&
      (!status  || rowStat === status);
    row.style.display = match ? '' : 'none';
    if (match) count++;
  }});
  document.getElementById('count').textContent = count + ' films';
}}
</script>
</body>
</html>"""

    with open(path, "w") as f:
        f.write(html)


# ---------------------------------------------------------------------------
# Post-processing: clean, merge, enrich
# ---------------------------------------------------------------------------

def _years_compatible(a: str, b: str) -> bool:
    if not a or not b:
        return True
    try:
        return abs(int(a[:4]) - int(b[:4])) <= 1
    except ValueError:
        return a == b


def _merge_into(base: Movie, other: Movie) -> None:
    """Fold a second listing of the same film at the same theater into base."""
    base.showtimes = sort_showtimes(base.showtimes + other.showtimes)
    base.show_dates = sorted(set(base.show_dates) | set(other.show_dates))
    for f in ("year", "director", "cast", "country", "poster", "booking_url"):
        if not getattr(base, f) and getattr(other, f):
            setattr(base, f, getattr(other, f))
    if len(other.description) > len(base.description):
        base.description = other.description
    base.notes += [n for n in other.notes if n not in base.notes]
    if other.status == "Now Playing":
        base.status, base.opens = "Now Playing", ""
    if other.date_start and (not base.date_start or other.date_start < base.date_start):
        base.date_start = other.date_start
    if other.date_end and other.date_end > base.date_end:
        base.date_end = other.date_end


def _clean_listing(m: Movie) -> None:
    title, labels, year, notes = split_title(m.title)
    m.title = title or m.title
    if year and not m.year:
        m.year = year
    m.notes += [n for n in notes if n not in m.notes]
    if labels:
        lab = ", ".join(labels)
        for st in m.showtimes:
            if not st.get("label"):
                st["label"] = lab
            elif lab.lower() not in st["label"].lower():
                st["label"] = f"{lab} · {st['label']}"
        note = f"Presented in {lab}"
        if not m.showtimes and note not in m.notes:
            m.notes.append(note)

    m.director = re.sub(r"\s+\.$", "", re.sub(r"\s+", " ", clean_text(m.director)))
    m.cast = re.sub(r"\s*,\s*", ", ", clean_text(m.cast)).strip(", ")
    m.country = clean_text(m.country)
    # "Michael Mann's Manhunter" / "Nancy Meyers’ The Parent Trap" → drop the possessive credit
    for d in re.split(r",|&|\band\b", m.director):
        d = d.strip()
        pm = re.match(re.escape(d) + r"[’']s?\s*(?=\S)", m.title, re.I) if len(d) > 3 else None
        if pm:
            m.title = m.title[pm.end():]
            break
    m.description = "\n\n".join(p for p in (clean_text(x) for x in (m.description or "").split("\n\n")) if p)
    m.notes = list(dict.fromkeys(clean_text(n) for n in m.notes if clean_text(n)))


def postprocess(movies: list, use_letterboxd: bool = True) -> list:
    today = date.today().isoformat()

    for m in movies:
        _clean_listing(m)
    movies = [m for m in movies if m.title and not _JUNK_TITLE.search(m.title)]

    # Same film listed twice at one theater (e.g. "Paper Tiger" and "PAPER TIGER in 35MM")
    by_key: dict = defaultdict(list)
    for m in movies:
        bucket = by_key[(m.theater, title_key(m.title))]
        twin = next((b for b in bucket if _years_compatible(b.year, m.year)), None)
        if twin:
            _merge_into(twin, m)
        else:
            bucket.append(m)
    movies = [m for bucket in by_key.values() for m in bucket]

    for m in movies:
        m.showtimes = sort_showtimes(m.showtimes)
        m.show_dates = sorted(set(m.show_dates) | {st["date"] for st in m.showtimes})
        if m.status == "Coming Soon" and not m.date_start and m.opens:
            iso = _parse_show_date(m.opens)
            if iso:
                m.date_start = m.date_end = iso
        # Expand date ranges into individual show_dates (e.g. BAM "Apr 17—Apr 23")
        if not m.show_dates and m.date_start:
            try:
                cur = date.fromisoformat(m.date_start)
                end = date.fromisoformat(m.date_end) if m.date_end else cur
                while cur <= end and len(m.show_dates) < 60:
                    m.show_dates.append(cur.isoformat())
                    cur += timedelta(days=1)
            except ValueError:
                pass

    # Drop runs that have already ended
    def _ended(m: Movie) -> bool:
        last = m.show_dates[-1] if m.show_dates else m.date_end
        return bool(last) and last < today
    movies = [m for m in movies if not _ended(m)]

    # Cross-theater enrichment: fill blanks from other listings of the same film (same era)
    groups: dict = defaultdict(list)
    for m in movies:
        groups[title_key(m.title)].append(m)
    for group in groups.values():
        for m in group:
            for f in ("year", "director", "cast", "country", "description", "poster"):
                if getattr(m, f):
                    continue
                donor = next((o for o in group if o is not m and getattr(o, f)
                              and _years_compatible(m.year, o.year)), None)
                if donor:
                    setattr(m, f, getattr(donor, f))

    if use_letterboxd:
        import letterboxd
        films, seen = [], set()
        for m in movies:
            k = letterboxd.cache_key(m.title, m.year)
            if k not in seen:
                seen.add(k)
                films.append((m.title, m.year, m.director))
        try:
            results = letterboxd.lookup_many(films)
        except Exception as e:
            print(f"[warn] Letterboxd enrichment failed: {e}", file=sys.stderr)
            results = {}
        hits = 0
        for m in movies:
            r = results.get(letterboxd.cache_key(m.title, m.year)) or {}
            if not r or r.get("miss"):
                continue
            hits += 1
            m.letterboxd_url = r.get("letterboxd_url", "")
            m.letterboxd_rating = r.get("rating", "")
            m.poster = r.get("poster") or m.poster
            if not m.cast:
                m.cast = r.get("cast", "")
            if not m.director:
                m.director = r.get("directors", "")
            # Theater years are occasionally wrong (re-release dates, typos); the match was
            # director-verified, so trust Letterboxd when they disagree by more than a year
            if r.get("year") and not _years_compatible(m.year, r["year"]) or not m.year:
                m.year = r.get("year", "") or m.year
            if not m.description:
                m.description = r.get("description", "")
        print(f"  Letterboxd/IMDb matched {hits}/{len(movies)} listings")
    return movies


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="NYC indie movie theater scraper")
    parser.add_argument("--html", action="store_true", help="Save movies.html")
    parser.add_argument("--json", action="store_true", help="Save movies.json")
    parser.add_argument("--site", action="store_true", help="Save docs/movies.json for GitHub Pages")
    parser.add_argument("--no-letterboxd", action="store_true",
                        help="Skip poster/cast/Letterboxd lookups (faster)")
    parser.add_argument("--only", metavar="NAME", help="Only scrape theaters whose name contains NAME")
    args = parser.parse_args()

    active = [t for t in THEATERS if not args.only or args.only.lower() in t[0].lower()]

    raw: list = []
    for name, fn_now, fn_soon in active:
        print(f"Fetching {name}…")
        found = fn_now()
        print(f"  → {len(found)} now playing")
        raw.extend(found)
        if fn_soon:
            cs = fn_soon()
            print(f"  → {len(cs)} coming soon")
            raw.extend(cs)

    # Deduplicate: same (normalised URL, theater, title) — Now Playing beats Coming Soon.
    # Strip non-identifying query params (e.g. ?date=today) but preserve identity params
    # like Metrograph's ?vista_film_id=...  Title is part of the key because some theaters
    # (HK Cinemas) share one URL across every film.
    _STRIP_PARAMS = {"date", "utm_source", "utm_medium", "utm_campaign"}

    def _url_key(url: str) -> str:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        p = urlparse(url)
        qs = {k: v for k, v in parse_qs(p.query).items() if k not in _STRIP_PARAMS}
        return urlunparse(p._replace(query=urlencode(qs, doseq=True), fragment="")).rstrip("/")

    seen: dict = {}
    for m in sorted(raw, key=lambda x: 0 if x.status == "Now Playing" else 1):
        key = (_url_key(m.url), m.theater, title_key(m.title))
        if key not in seen:
            seen[key] = m
    movies = list(seen.values())
    removed = len(raw) - len(movies)
    if removed:
        print(f"\n  (deduplicated {removed} duplicate entries)")

    movies = postprocess(movies, use_letterboxd=not args.no_letterboxd)

    print()
    if HAS_RICH:
        display_rich(movies)
    else:
        display_plain(movies)

    saved = []
    if args.html:
        save_html(movies)
        saved.append("movies.html")
    if args.json:
        save_json(movies)
        saved.append("movies.json")
    if args.site:
        import os
        os.makedirs("docs", exist_ok=True)
        save_json(movies, "docs/movies.json")
        saved.append("docs/movies.json")
    if saved:
        print(f"Saved → {'  '.join(saved)}")


if __name__ == "__main__":
    main()
