#!/usr/bin/env python3
"""
NYC indie/foreign movie theater scraper.
Always saves movies.html (sortable/filterable) and movies.json.

Usage:
    python scraper.py           # all server-rendered theaters
    python scraper.py --js      # also include JS-rendered theaters (needs playwright)

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
from datetime import datetime
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
    description: str = ""


# ---------------------------------------------------------------------------
# Common utilities
# ---------------------------------------------------------------------------

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
                date_str = datetime.fromtimestamp(int(ts)).strftime("%a %b %-d")
                showtimes.append(f"{date_str} {time_str}")
            except (ValueError, OSError):
                showtimes.append(time_str)
        movies.append(Movie(
            title=title_el.get_text(strip=True),
            theater="Nitehawk (Prospect Park)",
            url=url,
            booking_url=first_st["href"] if first_st else "",
            showtimes=showtimes,
            description=desc.get_text(strip=True) if desc else "",
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
        if date_el:
            raw_date = date_el.get_text(strip=True)
            opens = raw_date if raw_date.lower().startswith("opens") else f"Opens {raw_date}"
        else:
            opens = ""
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
        opens = opens_el.get_text(strip=True) if opens_el else ""
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
        showtimes = [s.get_text(strip=True) for s in p_parent.select("span") if s.get_text(strip=True)]
        movies.append(Movie(title=title, theater="Film Forum", url=url, showtimes=showtimes))
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
        opens = opens_el.get_text(strip=True) if opens_el else ""
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
        for h6 in card.select("div.showtimes h6"):
            date_str = h6.get_text(strip=True)
            day_div = h6.find_next_sibling("div")
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
        if month_el and day_el and time_el:
            showtime = f"{month_el.get_text(strip=True)} {day_el.get_text(strip=True)} {time_el.get_text(strip=True)}"

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
            "title": title, "url": url, "showtime": showtime,
            "year": year, "country": country, "description": description,
        })

    movies = []
    for entries in by_title.values():
        first = entries[0]
        showtimes = [e["showtime"] for e in entries if e["showtime"]]
        movies.append(Movie(
            title=first["title"],
            theater="Film Noir Cinema",
            url=first["url"],
            year=first["year"],
            country=first["country"],
            description=first["description"],
            showtimes=showtimes,
        ))
    return movies


# ---------------------------------------------------------------------------
# JS-rendered theaters (require --js / playwright)
# ---------------------------------------------------------------------------

def _playwright_scrape(url: str, theater_name: str) -> list:
    """Generic Playwright loader — renders JS then hands HTML to BeautifulSoup."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(f"[warn] {theater_name} requires playwright: pip install playwright && playwright install chromium", file=sys.stderr)
        return []
    movies = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(url, timeout=30000)
            page.wait_for_load_state("networkidle")
            soup = BeautifulSoup(page.content(), "html.parser")
            # Generic fallback: find any <h2>/<h3> near film links
            for el in soup.select("h2, h3"):
                text = el.get_text(strip=True)
                if not text or len(text) < 3:
                    continue
                link = el.find_parent("a") or el.find("a")
                film_url = link["href"] if link and link.get("href") else url
                movies.append(Movie(title=text, theater=theater_name, url=film_url))
        except Exception as e:
            print(f"[warn] {theater_name}: {e}", file=sys.stderr)
        finally:
            browser.close()
    return movies


def scrape_paris() -> list:
    return _playwright_scrape("https://www.paristheaternyc.com/", "Paris Theater")


def scrape_filmlinc() -> list:
    return _playwright_scrape("https://www.filmlinc.org/", "Film at Lincoln Center")


def scrape_angelika() -> list:
    return _playwright_scrape("https://angelikafilmcenter.com/nyc", "Angelika Film Center")


# ---------------------------------------------------------------------------
# Theater registry  ← add new theaters here
# ---------------------------------------------------------------------------
#
# Each entry: (display_name, now_playing_fn, coming_soon_fn or None)
# coming_soon_fn is called separately; return [] if not supported.

THEATERS: list[tuple] = [
    ("Nitehawk (Prospect Park)", scrape_nitehawk,          scrape_nitehawk_coming_soon),
    ("IFC Center",               scrape_ifc,               scrape_ifc_coming_soon),
    ("Film Forum",               scrape_filmforum,         scrape_filmforum_coming_soon),
    ("Metrograph",               scrape_metrograph,        None),
    ("Film Noir Cinema",         scrape_filmnoircinema,    None),
]

# These use Playwright (--js flag required):
JS_THEATERS: list[tuple] = [
    ("Paris Theater",            scrape_paris,             None),
    ("Film at Lincoln Center",   scrape_filmlinc,          None),
    ("Angelika Film Center",     scrape_angelika,          None),
]


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
    parser.add_argument("--js",   action="store_true", help="Include JS-rendered theaters (needs playwright)")
    parser.add_argument("--html", action="store_true", help="Save movies.html")
    parser.add_argument("--json", action="store_true", help="Save movies.json")
    parser.add_argument("--site", action="store_true", help="Save docs/movies.json for GitHub Pages")
    args = parser.parse_args()

    active = list(THEATERS)
    if args.js:
        active.extend(JS_THEATERS)

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
