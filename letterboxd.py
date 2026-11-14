"""
Film lookup for posters, cast, and Letterboxd links.

Pipeline per film:
    1. IMDb's public title-suggestion endpoint finds the IMDb id (title + year).
    2. letterboxd.com/imdb/<id>/ redirects to the Letterboxd film page.
    3. The film page's JSON-LD gives poster, directors, cast, year, and rating.

Results (including misses) are cached in letterboxd_cache.json so daily runs
only look up films they haven't seen before.
"""

import json
import os
import re
import sys
import threading
import time
import unicodedata
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests

CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "letterboxd_cache.json")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HIT_TTL_DAYS = 45      # refresh ratings/posters occasionally
MISS_TTL_DAYS = 7      # retry films that weren't found (new releases get added)
_FILM_TYPES = {"movie", "tvMovie", "short", "tvSpecial", "video", "tvMiniSeries", "documentary"}

_lock = threading.Lock()


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c)).replace("&", "and")
    return re.sub(r"[^a-z0-9]", "", s)


def _surnames(names: str) -> set:
    out = set()
    for n in re.split(r",|&|\band\b|/", names or ""):
        toks = [t for t in re.split(r"\s+", n.strip()) if t and t != "."]
        if toks:
            out.add(_norm(toks[-1]))
    return {s for s in out if s}


def _get(url: str, **kw) -> requests.Response:
    for attempt in range(3):
        r = requests.get(url, headers={"User-Agent": UA}, timeout=15, **kw)
        if r.status_code == 429:
            time.sleep(2 + 3 * attempt)
            continue
        return r
    return r


# ── IMDb search ──────────────────────────────────────────────────────────────

def _imdb_candidates(title: str, year: str) -> list:
    q = re.sub(r"[^\w\s'’:&.-]", " ", f"{title} {year}".strip() if year else title)
    q = re.sub(r"\s+", " ", q).strip().lower()
    if not q:
        return []
    url = "https://v3.sg.media-imdb.com/suggestion/x/" + urllib.parse.quote(q) + ".json"
    r = _get(url)
    if r.status_code != 200:
        return []
    return r.json().get("d", []) or []


def _pick_imdb(cands: list, title: str, year: str) -> tuple:
    """Return (candidate, needs_director_check) or (None, False)."""
    tk = _norm(title)
    best, best_score, best_rank = None, -99.0, 0
    for rank, c in enumerate(cands):
        if not str(c.get("id", "")).startswith("tt"):
            continue
        if c.get("qid") and c["qid"] not in _FILM_TYPES:
            continue
        ck = _norm(c.get("l", ""))
        score = 4.0 if ck == tk else (1.0 if tk and (tk in ck or ck in tk) else 0.0)
        cy = c.get("y")
        if year and cy:
            dy = abs(int(year) - int(cy))
            score += 4 if dy == 0 else (3 if dy == 1 else -6)
        elif year:
            score -= 2
        if c.get("qid") == "movie":
            score += 0.5
        score -= rank * 0.3
        if score > best_score:
            best, best_score, best_rank = c, score, rank
    if not best:
        return None, False
    ck, cy = _norm(best.get("l", "")), best.get("y")
    if year:
        if not cy or abs(int(year) - int(cy)) > 1:
            return None, False
        # Year matches; accept an exact/partial title match, or IMDb's top hit
        # for "title year" (handles foreign vs. English titles: Nayak → The Hero)
        if ck == tk or tk in ck or ck in tk or best_rank == 0:
            return best, False
        return None, False
    # No year known: require an exact title; verify by director when possible,
    # otherwise only trust recent releases (undated listings are usually new films)
    if ck != tk:
        return None, False
    recent = bool(cy) and int(cy) >= date.today().year - 2
    return best, not recent


# ── Letterboxd ───────────────────────────────────────────────────────────────

def _letterboxd_film(imdb_id: str) -> dict:
    r = _get(f"https://letterboxd.com/imdb/{imdb_id}/", allow_redirects=True)
    if r.status_code != 200 or "/film/" not in r.url:
        return {}
    html = r.text
    m = re.search(r'<script type="application/ld\+json">(.*?)</script>', html, re.S)
    if not m:
        return {}
    raw = re.sub(r"/\*.*?\*/", "", m.group(1), flags=re.S).strip()
    try:
        ld = json.loads(raw)
    except ValueError:
        return {}
    year = ""
    ym = re.search(r'<meta property="og:title" content="[^"]*\((\d{4})\)"', html)
    if ym:
        year = ym.group(1)       # (JSON-LD dateCreated is not reliably the release year)
    poster = ld.get("image") or ""
    poster = re.sub(r"-0-\d+-0-\d+-crop", "-0-460-0-690-crop", poster)
    rating = (ld.get("aggregateRating") or {}).get("ratingValue")
    return {
        "letterboxd_url": ld.get("url") or r.url,
        "poster": poster,
        "directors": ", ".join(p.get("name", "") for p in ld.get("director") or [] if p.get("name")),
        "cast": ", ".join(p.get("name", "") for p in (ld.get("actor") or [])[:6] if p.get("name")),
        "year": year,
        "rating": f"{rating:.1f}" if isinstance(rating, (int, float)) else "",
        "description": (ld.get("description") or "").strip(),
    }


def _imdb_poster(c: dict) -> str:
    url = (c.get("i") or {}).get("imageUrl", "")
    return re.sub(r"\._V1_.*\.jpg$", "._V1_UX460_.jpg", url) if url else ""


def lookup(title: str, year: str = "", director: str = "") -> dict:
    """Find one film. Returns {} when no confident match."""
    if not year and not director:
        return {}   # undated, uncredited listings are almost always events/programs, not films
    try:
        # Theater years are occasionally wrong (re-release dates, typos): if the dated search
        # fails or the director disagrees, retry undated and require the director to match.
        attempts = [(year, False)] + ([("", True)] if year and director else [])
        for y, force_verify in attempts:
            cand, verify = _pick_imdb(_imdb_candidates(title, y), title, y)
            if not cand:
                continue
            verify = verify or force_verify
            lb = _letterboxd_film(cand["id"])
            ours, theirs = _surnames(director), _surnames(lb.get("directors", ""))
            if verify and not (ours and theirs and ours & theirs):
                continue
            if ours and theirs and not ours & theirs:
                continue
            return {
                "imdb": cand["id"],
                "letterboxd_url": lb.get("letterboxd_url", ""),
                "poster": lb.get("poster") or _imdb_poster(cand),
                "cast": lb.get("cast") or (cand.get("s") or ""),
                "directors": lb.get("directors", ""),
                "year": str(cand.get("y") or "") or lb.get("year", ""),
                "rating": lb.get("rating", ""),
                "description": lb.get("description", ""),
            }
        return {}
    except Exception as e:  # network hiccups shouldn't break the scrape
        print(f"[warn] letterboxd lookup {title!r}: {e}", file=sys.stderr)
        return {"error": True}


# ── Cache + batch enrichment ────────────────────────────────────────────────

def load_cache(path: str = CACHE_PATH) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_cache(cache: dict, path: str = CACHE_PATH) -> None:
    # Drop entries untouched for a long time so the file doesn't grow forever
    cutoff = (date.today() - timedelta(days=180)).isoformat()
    cache = {k: v for k, v in cache.items() if v.get("seen", v.get("ts", "")) >= cutoff}
    with open(path, "w") as f:
        json.dump(dict(sorted(cache.items())), f, indent=1, ensure_ascii=False)


def _fresh(entry: dict) -> bool:
    if not entry or entry.get("error"):
        return False
    ttl = MISS_TTL_DAYS if entry.get("miss") else HIT_TTL_DAYS
    return entry.get("ts", "") >= (date.today() - timedelta(days=ttl)).isoformat()


def cache_key(title: str, year: str) -> str:
    return f"{_norm(title)}|{year or ''}"


def lookup_many(films: list, workers: int = 6, cache_path: str = CACHE_PATH) -> dict:
    """films: [(title, year, director)] → {cache_key: result}. Uses and updates the cache."""
    cache = load_cache(cache_path)
    today = date.today().isoformat()
    todo = []
    for title, year, director in films:
        key = cache_key(title, year)
        if _fresh(cache.get(key)):
            cache[key]["seen"] = today
        elif key not in {cache_key(t, y) for t, y, _ in todo}:
            todo.append((title, year, director))

    if todo:
        print(f"  Looking up {len(todo)} films on IMDb/Letterboxd ({len(films) - len(todo)} cached)…")
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(lookup, t, y, d): (t, y) for t, y, d in todo}
            for fut in as_completed(futures):
                t, y = futures[fut]
                res = fut.result()
                if res.get("error"):
                    continue          # transient failure: don't cache, retry next run
                entry = res if res else {"miss": True}
                entry.update({"ts": today, "seen": today, "title": t})
                with _lock:
                    cache[cache_key(t, y)] = entry
    save_cache(cache, cache_path)
    return {cache_key(t, y): cache.get(cache_key(t, y), {}) for t, y, _ in films}
