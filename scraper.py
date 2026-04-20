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

import json
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup

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
    showtimes: list = field(default_factory=list)
    show_dates: list = field(default_factory=list)  # ["YYYY-MM-DD", ...]
    date_start: str = ""   # ISO date — run/engagement start
    date_end:   str = ""   # ISO date — run/engagement end
    description: str = ""


# ---------------------------------------------------------------------------
# Common utilities
# ---------------------------------------------------------------------------

_MONTH_FULL = ["", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]


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
            details = future.result()
            movie = futures[future]
            for f in ("year", "director", "cast", "country", "description"):
                val = details.get(f, "")
                if val and not getattr(movie, f):
                    setattr(movie, f, val)
            # Prefer detail-page dates (more complete) over listing-page dates
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

def _nitehawk_details(url: str) -> dict:
    soup = _fetch(url.split("?")[0])
    if not soup:
        return {}
    result = {}
    specs_p = soup.select_one("p.show-specs")
    if specs_p:
        for outer in specs_p.select("span"):
            label_el = outer.select_one("span.show-spec-label")
            if not label_el:
                continue
            label = label_el.get_text(strip=True).rstrip(":")
            value = outer.get_text(separator=" ", strip=True).replace(label_el.get_text(strip=True), "").strip()
            if label == "Release Year":
                result["year"] = value
            elif label == "Director":
                result["director"] = value
            elif label == "Country":
                result["country"] = value
    for p in soup.select("div.show-content p"):
        strong = p.select_one("strong")
        if strong and "Starring" in strong.get_text():
            result["cast"] = p.get_text(separator=" ", strip=True).replace(strong.get_text(strip=True), "").strip()
            break
    # Full show schedule: datelist options (multi-date) or individual showtime buttons (single-date)
    show_dates = sorted({
        datetime.fromtimestamp(int(el["data-date"])).strftime("%Y-%m-%d")
        for el in soup.select("[data-date]")
        if el.get("data-date")
    })
    if show_dates:
        result["show_dates"] = show_dates
    else:
        # No sessions yet — parse "Opens on July 1" from date-selector placeholder
        empty_sel = soup.select_one("div.date-selector.empty")
        if empty_sel:
            iso = _parse_show_date(empty_sel.get_text(strip=True))
            if iso:
                result["date_start"] = iso
                result["date_end"] = iso
    return result


def _ifc_details(url: str) -> dict:
    soup = _fetch(url)
    if not soup:
        return {}
    result = {}
    for li in soup.select("ul.film-details li"):
        strong = li.select_one("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True)
        value = li.get_text(separator=" ", strip=True).replace(label, "", 1).strip()
        if label == "Year":
            result["year"] = value
        elif label == "Director":
            result["director"] = value
        elif label == "Cast":
            result["cast"] = value
        elif label == "Country":
            result["country"] = value
    ticket = soup.select_one("a.ifc-button[href*='tickets']") or \
             soup.select_one("a[href*='tickets.ifccenter.com']")
    if ticket:
        result["booking_url"] = ticket["href"]
    # Show dates from per-film schedule list ("Wed Apr 22 9:30 pm Buy Tickets")
    show_dates = []
    for li in soup.select("ul.schedule-list li"):
        div = li.select_one("div")
        if div:
            iso = _parse_show_date(div.get_text(" ", strip=True))
            if iso:
                show_dates.append(iso)
    if show_dates:
        result["show_dates"] = sorted(set(show_dates))
    return result


def _filmforum_link_title(link) -> str:
    """Extract film title from <a>, skipping presenter attribution before <br/>."""
    parts = [p.strip() for p in link.get_text(separator="\n", strip=True).split("\n") if p.strip()]
    return parts[-1].title() if parts else ""


def _filmforum_details(url: str) -> dict:
    soup = _fetch(url)
    if not soup:
        return {}
    result = {}

    copy = soup.select_one("div.copy")
    if copy:
        strong = copy.select_one("strong")
        if strong:
            meta = re.sub(r"[\xa0\s]+", " ", strong.get_text()).strip()

            # Format 1 (all-caps): "YEAR RUNTIME MIN. COUNTRY [IN LANGUAGE] [DISTRIBUTOR]"
            if re.match(r"^\d{4}\s+\d+\s+MIN\.", meta, re.IGNORECASE):
                m = re.match(r"^(\d{4})", meta)
                if m:
                    result["year"] = m.group(1)
                # Country: stop at language marker; fallback to first token only
                cm = re.search(r"MIN\.?\s+([A-Z][A-Z/ ]*?)(?:\s+IN\b|\s+WITH\b)", meta, re.IGNORECASE)
                if cm:
                    result["country"] = cm.group(1).strip().rstrip(" ,/")
                else:
                    # No language marker — take first slash-joined uppercase word, e.g. "CANADA" from "CANADA ICARUS FILMS"
                    cm2 = re.search(r"MIN\.?\s+([A-Z]{2,}(?:/[A-Z]{2,})*)", meta)
                    if cm2:
                        result["country"] = cm2.group(1)

            # Format 2 (mixed case): "Country, Year Directed by Director Starring Cast Approx. N min."
            else:
                ym = re.search(r"\b(19\d{2}|20\d{2})\b", meta)
                if ym:
                    result["year"] = ym.group(1)
                cm = re.match(r"^([A-Za-z][A-Za-z./]*(?:/[A-Za-z.]+)?),", meta)
                if cm:
                    result["country"] = cm.group(1).strip().rstrip(".")
                dm = re.search(r"[Dd]irected by\s+(.+?)(?:\s+Starring|\s+Approx|\s*$)", meta)
                if dm:
                    result["director"] = dm.group(1).strip()
                sm = re.search(r"[Ss]tarring\s+(.+?)(?:\s+Approx|\s+Produced|\s*$)", meta)
                if sm:
                    result["cast"] = sm.group(1).strip()

        # Description: text in .copy p before the <strong> metadata line
        for p in copy.select("p"):
            text = p.get_text(" ", strip=True)
            if len(text) > 80:
                strong_el = p.select_one("strong")
                if strong_el:
                    text = text[: text.find(strong_el.get_text(" ", strip=True)[:30])].strip()
                if len(text) > 80:
                    result["description"] = text[:500]
                    break

    # Format 1: director from .urgent div (all-caps films don't embed director in metadata)
    urgent = soup.select_one("div.urgent")
    if urgent and not result.get("director"):
        text = urgent.get_text(" ", strip=True)
        m = re.search(r"(?:WRITTEN[^,]*?AND\s+)?DIRECTED BY\s+(.+?)(?:\s*\.|$)", text, re.IGNORECASE)
        if m:
            result["director"] = m.group(1).strip().title()

    # Show dates from weekly tab grid — tab i corresponds to today + i days
    film_slug = url.rstrip("/").split("/")[-1]
    container = soup.select_one("div.showtimes-container")
    if container and film_slug:
        today_obj = date.today()
        show_dates: set = set()
        for i, tab in enumerate(container.select("[id^=tabs-]")[:7]):
            if tab.select_one(f'a[href*="{film_slug}"]'):
                show_dates.add((today_obj + timedelta(days=i)).isoformat())
        if show_dates:
            result["show_dates"] = sorted(show_dates)

    # Fallback: parse dates from div.details p text (e.g. "Sunday, June 28 11:00")
    # Catches films whose run is outside the 7-day tab window
    if not result.get("show_dates"):
        detail_dates: set = set()
        for p in soup.select("div.details p"):
            iso = _parse_show_date(p.get_text(" ", strip=True))
            if iso:
                detail_dates.add(iso)
        if detail_dates:
            result["show_dates"] = sorted(detail_dates)

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
        showtimes = []
        show_dates_set: set = set()
        for li in card.select("ul.showtime-button-row li"):
            st = li.select_one("a.showtime")
            if not st:
                continue
            raw = st.get_text(" ", strip=True)
            m = re.search(r"\d+:\d+\s*(?:am|pm)", raw, re.IGNORECASE)
            time_str = m.group() if m else ""
            if not time_str:
                continue
            ts = li.get("data-date", "")
            try:
                dt_obj = datetime.fromtimestamp(int(ts))
                date_str = dt_obj.strftime("%a %b %-d")
                show_dates_set.add(dt_obj.strftime("%Y-%m-%d"))
                showtimes.append(f"{date_str} {time_str}")
            except (ValueError, OSError):
                showtimes.append(time_str)
        movies.append(Movie(
            title=title_el.get_text(strip=True),
            theater="Nitehawk (Prospect Park)",
            url=url,
            booking_url=first_st["href"] if first_st else "",
            showtimes=showtimes,
            show_dates=sorted(show_dates_set),
            description=desc.get_text(strip=True) if desc else "",
        ))
    # Supplement with /movies/ page to capture films not playing today
    seen_urls = {m.url for m in movies}
    movies_page = _fetch("https://nitehawkcinema.com/prospectpark/movies/")
    if movies_page:
        for art in movies_page.select("article"):
            link = art.select_one("a[href*='/prospectpark/movies/']")
            if not link or link["href"] in seen_urls:
                continue
            title_el = art.select_one("h2 a, h1 a")
            if not title_el:
                continue
            seen_urls.add(link["href"])
            movies.append(Movie(
                title=title_el.get_text(strip=True),
                theater="Nitehawk (Prospect Park)",
                url=link["href"],
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
            title=title_el.get_text(strip=True),
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

def scrape_ifc() -> list:
    movies = []
    soup = _fetch("https://www.ifccenter.com/")
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
        movies.append(Movie(title=title_el.get_text(strip=True), theater="IFC Center", url=url))
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

def scrape_filmforum() -> list:
    movies = []
    soup = _fetch("https://filmforum.org/now_playing")
    if not soup:
        return movies
    seen_urls: set = set()
    for link in soup.find_all("a", href=re.compile(r"filmforum\.org/film/|^/film/")):
        strong_parent = link.find_parent("strong")
        if not strong_parent:
            continue
        p_parent = strong_parent.find_parent("p")
        if not p_parent:
            continue
        url = _abs(link["href"], "https://filmforum.org")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        title = _filmforum_link_title(link)
        showtimes = []
        show_dates_set: set = set()
        for s in p_parent.select("span"):
            text = s.get_text(strip=True)
            if not text:
                continue
            showtimes.append(text)
            iso = _parse_show_date(text)
            if iso:
                show_dates_set.add(iso)
        movies.append(Movie(
            title=title, theater="Film Forum", url=url,
            showtimes=showtimes, show_dates=sorted(show_dates_set),
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
        title = _filmforum_link_title(link)
        opens_el = card.select_one("div.details p")
        opens = _normalize_opens(opens_el.get_text(strip=True)) if opens_el else ""
        movies.append(Movie(
            title=title,
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

def scrape_metrograph() -> list:
    """Metrograph listing page already includes director, year, synopsis inline."""
    movies = []
    soup = _fetch("https://metrograph.com/film/")
    if not soup:
        return movies
    seen_urls: set = set()
    for card in soup.select("div.homepage-in-theater-movie"):
        title_el = card.select_one("h3.movie_title a")
        if not title_el:
            continue
        url = _abs(title_el["href"], "https://metrograph.com")
        if url in seen_urls:
            continue
        seen_urls.add(url)

        director, year = "", ""
        h5s = card.select("h5")
        for h5 in h5s:
            text = h5.get_text(strip=True)
            if text.lower().startswith("director:"):
                director = text.split(":", 1)[1].strip()
            elif re.match(r"\d{4}\s*/", text):
                year = text.split("/")[0].strip()

        showtimes, booking_url = [], ""
        show_dates_set: set = set()
        # Metrograph uses h6 for single-date films, h5 for multi-date films
        for date_el in card.select("div.showtimes h5, div.showtimes h6"):
            date_str = date_el.get_text(strip=True)
            iso_date = _parse_show_date(date_str)
            if iso_date:
                show_dates_set.add(iso_date)
            day_div = date_el.find_next_sibling("div")
            if day_div:
                for a in day_div.select("a[href*='t.metrograph.com']"):
                    time_str = a.get_text(strip=True)
                    if time_str:
                        showtimes.append(f"{date_str} {time_str}")
                        if not booking_url:
                            booking_url = a["href"]

        synopsis_el = card.select_one("p.synopsis")
        desc = synopsis_el.get_text(strip=True) if synopsis_el else ""

        movies.append(Movie(
            title=title_el.get_text(strip=True),
            theater="Metrograph",
            url=url,
            booking_url=booking_url,
            year=year,
            director=director,
            description=desc,
            showtimes=showtimes,
            show_dates=sorted(show_dates_set),
        ))
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
        href = title_el.get("href", "")
        url = _abs(href, "https://www.filmnoircinema.com")

        month_el = article.select_one(".eventlist-datetag-startdate--month")
        day_el   = article.select_one(".eventlist-datetag-startdate--day")
        time_el  = article.select_one("time.event-time-12hr-start")
        showtime = ""
        show_date = ""
        if month_el and day_el and time_el:
            showtime = f"{month_el.get_text(strip=True)} {day_el.get_text(strip=True)} {time_el.get_text(strip=True)}"
        if month_el and day_el:
            try:
                m_num = datetime.strptime(month_el.get_text(strip=True)[:3], "%b").month
                d_num = int(day_el.get_text(strip=True))
                show_date = f"{_infer_year(m_num, d_num)}-{m_num:02d}-{d_num:02d}"
            except (ValueError, TypeError):
                pass

        year, country, description = "", "", ""
        desc_div = article.select_one("div.eventlist-description")
        if desc_div:
            paras = [p.get_text(strip=True) for p in desc_div.select("p") if p.get_text(strip=True)]
            if paras:
                meta_m = re.match(r"^([A-Za-z/]+)[.,]?\s*(\d{4})", paras[0])
                if meta_m:
                    country = meta_m.group(1).strip()
                    year    = meta_m.group(2)
                    description = " ".join(paras[1:])
                else:
                    description = " ".join(paras)

        by_title[title.lower()].append({
            "title": title, "url": url, "showtime": showtime, "show_date": show_date,
            "year": year, "country": country, "description": description,
        })

    movies = []
    for entries in by_title.values():
        first = entries[0]
        showtimes = [e["showtime"] for e in entries if e["showtime"]]
        show_dates = sorted({e["show_date"] for e in entries if e.get("show_date")})
        movies.append(Movie(
            title=first["title"],
            theater="Film Noir Cinema",
            url=first["url"],
            year=first["year"],
            country=first["country"],
            description=first["description"],
            showtimes=showtimes,
            show_dates=show_dates,
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
            title = m.group(4).strip().title()
        else:
            m2 = re.match(r"^[A-Za-z]{3}[\s\d\-–]+\s+(.*)", raw)
            title = (m2.group(1) if m2 else raw).strip().title()
        if not title:
            continue
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
        result["description"] = desc_el.get_text(" ", strip=True)

    # Date range lives in div.bam-block-hero-date on detail pages
    hero_date = soup.select_one("div.bam-block-hero-date")
    if hero_date:
        date_text = hero_date.get_text(strip=True)
        _, _, date_start, date_end = _bam_parse_date(date_text)
        if date_start:
            result["date_start"] = date_start
        if date_end:
            result["date_end"] = date_end

    # Fallback: parse individual show dates from JSON-LD Event schema
    # (used when hero-date only says "Now Playing" but events have specific startDates)
    if not result.get("date_start"):
        import json as _json
        show_dates: set = set()
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                ld = _json.loads(script.string or "")
                events = ld.get("graph", [ld]) if isinstance(ld, dict) else []
                for e in events:
                    if e.get("@type") == "Event" and e.get("startDate"):
                        show_dates.add(e["startDate"][:10])
            except Exception:
                pass
        if show_dates:
            result["show_dates"] = sorted(show_dates)

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
            title=entry["title"].title(),
            theater="BAM",
            url=entry["url"],
            status=status,
            opens=opens,
            date_start=date_start,
            date_end=date_end,
            show_dates=details.get("show_dates", []),
            director=details.get("director", ""),
            year=details.get("year", ""),
            description=details.get("description", ""),
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
    for c in series_children:
        status, opens, date_start, date_end = _bam_parse_date(c["date_text"])
        child_movies.append(Movie(
            title=c["title"].title(),
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
# Paris Theater  (Next.js — data embedded in RSC inline scripts)
# ---------------------------------------------------------------------------

def scrape_paris() -> list:
    import json as _json
    movies = []
    r = requests.get(
        "https://www.paristheaternyc.com/",
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
        timeout=15,
    )
    if r.status_code != 200:
        print(f"[warn] Paris Theater: HTTP {r.status_code}", file=sys.stderr)
        return movies

    # Collect all RSC chunk strings
    scripts = re.findall(
        r"<script>self\.__next_f\.push\((\[.*?\])\)</script>", r.text, re.DOTALL
    )
    all_content = ""
    for s in scripts:
        try:
            data = _json.loads(s)
            if len(data) >= 2 and isinstance(data[1], str):
                all_content += data[1] + "\n"
        except Exception:
            pass

    today = date.today()
    seen: set = set()
    for m in re.finditer(r'"FilmName"\s*:\s*"([^"]+)"', all_content):
        film_name = m.group(1)
        if film_name in seen:
            continue
        ctx = all_content[m.start() : m.start() + 1500]

        def _rsc(field: str) -> str:
            fm = re.search(rf'"{re.escape(field)}"\s*:\s*"([^"]*)"', ctx)
            return fm.group(1) if fm else ""

        slug       = _rsc("Slug")
        director   = _rsc("Director")
        cast_      = _rsc("Cast")
        opening    = _rsc("OpeningDate")    # "YYYY-MM-DD"
        closing    = _rsc("ClosingDate")
        year       = _rsc("Year")
        # Only show films active within the current or upcoming season
        try:
            open_date  = date.fromisoformat(opening) if opening else None
            close_date = date.fromisoformat(closing) if closing else None
        except ValueError:
            open_date = close_date = None

        # Skip stale past screenings (closed more than 7 days ago)
        if close_date and close_date < today - timedelta(days=7):
            continue
        # Skip far-future entries with no opening date
        if not open_date:
            continue

        seen.add(film_name)
        status = "Coming Soon" if open_date > today else "Now Playing"
        opens  = f"{_MONTH_FULL[open_date.month]} {open_date.day}" if status == "Coming Soon" else ""
        url    = f"https://www.paristheaternyc.com/films/{slug}" if slug else "https://www.paristheaternyc.com/"

        movies.append(Movie(
            title=film_name.title(),
            theater="Paris Theater",
            url=url,
            status=status,
            opens=opens,
            year=year,
            director=director,
            cast=cast_,
            date_start=open_date.isoformat() if open_date else "",
            date_end=close_date.isoformat() if close_date else "",
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

        # FLC listing shows today's showtimes; tag today as a known show date
        has_showtimes = bool(re.search(r"\d{1,2}:\d{2}\s*(?:AM|PM)", card.get_text(" ", strip=True), re.IGNORECASE))
        movies.append(Movie(
            title=title.title(),
            theater="Film at Lincoln Center",
            url=url,
            status=status,
            opens=opens,
            show_dates=[date.today().isoformat()] if (status == "Now Playing" and has_showtimes) else [],
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
        title = (f.get("name") or f.get("movieName") or "").strip()
        if not title:
            continue
        title = title.title()
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

        showtimes = []
        show_dates_set: set = set()
        showdates = f.get("showdates") or {}
        if isinstance(showdates, list):
            for sd in showdates:
                date_iso = (sd.get("date") or "")[:10]
                if date_iso:
                    show_dates_set.add(date_iso)
                for st_type in sd.get("showtypes", []):
                    for st in st_type.get("showtimes", []):
                        dt_str = st.get("date_time", "")
                        try:
                            # Normalize short UTC offset "-04" → "-04:00" for Python 3.9
                            dt_norm = re.sub(r"([+-]\d{2})$", r"\1:00", dt_str)
                            dt = datetime.fromisoformat(dt_norm)
                            showtimes.append(dt.strftime("%a %b %-d %-I:%M %p"))
                        except ValueError:
                            pass

        movies.append(Movie(
            title=title,
            theater="Angelika Film Center",
            url=url,
            booking_url=url,
            status=status,
            opens=opens,
            director=(f.get("director") or "").strip(),
            cast=(f.get("cast") or "").strip().rstrip(","),
            description=re.sub(r"<[^>]+>", "", f.get("synopsis") or "").strip(),
            showtimes=showtimes,
            show_dates=sorted(show_dates_set),
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

    presentations = data.get("presentations", [])
    today = date.today().isoformat()

    # Build slug → sorted show_dates from the market-level sessions list
    slug_dates: dict = {}
    for s in data.get("sessions", []):
        slug = s.get("presentationSlug", "")
        bdate = s.get("businessDateClt", "")
        if slug and bdate:
            slug_dates.setdefault(slug, set()).add(bdate[:10])
    slug_dates = {k: sorted(v) for k, v in slug_dates.items()}

    seen_titles: set[str] = set()
    to_fetch: list[dict] = []
    for p in presentations:
        slug = p.get("slug", "")
        if any(slug.startswith(pfx) for pfx in _ALAMO_SKIP_PREFIXES):
            continue
        title = p["show"].get("title", "").strip()
        if not title or title.lower() in seen_titles:
            continue
        seen_titles.add(title.lower())
        opening = p.get("openingDateClt")
        status = "Coming Soon" if (opening and opening > today) else "Now Playing"
        to_fetch.append({"slug": slug, "title": title, "status": status, "opening": opening})

    movies = []

    def _build(item: dict) -> Optional[Movie]:
        detail = _alamo_fetch_detail(item["slug"])
        show = detail.get("show", {})
        description = re.sub(r"<[^>]+>", "", show.get("description") or "").strip()
        release = show.get("nationalReleaseDateUtc") or ""
        year = release[:4] if release else ""
        directors = show.get("directors") or []
        director = ", ".join(d.get("name", d) if isinstance(d, dict) else d for d in directors)
        opens = _normalize_opens(item["opening"] or "")
        url = _ALAMO_SHOW.format(slug=item["slug"])
        date_start = item["opening"][:10] if item["opening"] else ""
        show_dates = slug_dates.get(item["slug"], [])
        return Movie(
            title=item["title"],
            theater="Alamo Drafthouse NYC",
            url=url,
            booking_url=url,
            status=item["status"],
            opens=opens if item["status"] == "Coming Soon" else "",
            director=director,
            year=year,
            description=description,
            show_dates=show_dates,
            date_start=date_start,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_build, item): item for item in to_fetch}
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
        description = re.sub(r"<[^>]+>", "", f.get("Synopsis") or "").strip()
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
        # Show dates from the date-grid squares (data-date="YYYY-MM-DD")
        show_dates_set: set = {
            el["data-date"]
            for el in card.select("span.date-square[data-date]")
            if el.get("data-date")
        }

        # Fetch film detail page for director/year/country/description
        detail = _fetch(film_url)
        director = year = country = description = ""
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
                # Drop the legal boilerplate (final paragraph starting with "All sales are final")
                body_paras = [p for p in body_paras if not p.startswith("All sales are final")]
                description = " ".join(body_paras)

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
            show_dates=sorted(show_dates_set),
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

def _fmt_showtimes(movie: Movie, limit: int = 5) -> str:
    if movie.showtimes:
        lines = movie.showtimes[:limit]
        if len(movie.showtimes) > limit:
            lines.append(f"+{len(movie.showtimes) - limit} more")
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
        if m.showtimes:
            print(f"TIMES:    {', '.join(m.showtimes[:5])}")
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
        if m.showtimes:
            times_html = "<br>".join(m.showtimes[:8])
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
          <td><a href="{m.url}" target="_blank">{m.title}</a></td>
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
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="NYC indie movie theater scraper")
    parser.add_argument("--html", action="store_true", help="Save movies.html")
    parser.add_argument("--json", action="store_true", help="Save movies.json")
    parser.add_argument("--site", action="store_true", help="Save docs/movies.json for GitHub Pages")
    args = parser.parse_args()

    active = list(THEATERS)

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

    # Deduplicate: same (normalised URL, theater) — Now Playing beats Coming Soon
    # Strip non-identifying query params (e.g. ?date=today) but preserve
    # identity params like Metrograph's ?vista_film_id=...
    _STRIP_PARAMS = {"date", "utm_source", "utm_medium", "utm_campaign"}

    def _url_key(url: str) -> str:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        p = urlparse(url)
        qs = {k: v for k, v in parse_qs(p.query).items() if k not in _STRIP_PARAMS}
        return urlunparse(p._replace(query=urlencode(qs, doseq=True), fragment="")).rstrip("/")

    seen: dict = {}
    for m in sorted(raw, key=lambda x: 0 if x.status == "Now Playing" else 1):
        key = (_url_key(m.url), m.theater)
        if key not in seen:
            seen[key] = m
    movies = list(seen.values())
    removed = len(raw) - len(movies)
    if removed:
        print(f"\n  (deduplicated {removed} duplicate entries)")

    # Cross-theater enrichment: fill missing fields from other entries with same title
    _norm = lambda s: re.sub(r"[^a-z0-9]", "", (s or "").lower())
    best: dict = {}
    for m in movies:
        key = _norm(m.title)
        if key not in best:
            best[key] = {}
        for field in ("director", "cast", "country", "year", "description"):
            if not best[key].get(field) and getattr(m, field):
                best[key][field] = getattr(m, field)
    for m in movies:
        key = _norm(m.title)
        for field in ("director", "cast", "country", "year", "description"):
            if not getattr(m, field) and best[key].get(field):
                setattr(m, field, best[key][field])

    # Convert opens date to date_start for Coming Soon films with no range
    for m in movies:
        if m.status == "Coming Soon" and not m.date_start and m.opens:
            iso = _parse_show_date(m.opens)
            if iso:
                m.date_start = iso
                m.date_end = iso

    # Fallback: parse show_dates from showtimes strings when not yet populated
    for m in movies:
        if not m.show_dates and m.showtimes:
            dates = set()
            for st in m.showtimes:
                iso = _parse_show_date(st)
                if iso:
                    dates.add(iso)
            if dates:
                m.show_dates = sorted(dates)

    # Expand date ranges into individual show_dates (e.g. BAM "Apr 17—Apr 23")
    for m in movies:
        if not m.show_dates and m.date_start:
            try:
                start = date.fromisoformat(m.date_start)
                end = date.fromisoformat(m.date_end) if m.date_end else start
                current = start
                dates = []
                while current <= end and len(dates) < 60:
                    dates.append(current.isoformat())
                    current += timedelta(days=1)
                m.show_dates = dates
            except (ValueError, TypeError):
                pass

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
