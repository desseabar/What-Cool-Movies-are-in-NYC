# What Cool Movies Are in NYC?

A scraper that checks indie, foreign, and repertory film listings across NYC theaters and surfaces them in a sortable terminal table, a local HTML file, and a live GitHub Pages site.

## Theaters covered

| Theater | Now Playing | Coming Soon |
|---|:---:|:---:|
| Nitehawk Cinema (Prospect Park) | ✓ | ✓ |
| Nitehawk Cinema (Williamsburg) | ✓ | — |
| IFC Center | ✓ | ✓ |
| Film Forum | ✓ | ✓ |
| Metrograph | ✓ | — |
| Film Noir Cinema | ✓ | — |
| BAM | ✓ | ✓ |
| Paris Theater | ✓ | ✓ |
| Film at Lincoln Center | ✓ | ✓ |
| Angelika Film Center | ✓ | ✓ |

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# Terminal table only
python3 scraper.py

# Also save a local HTML file and JSON
python3 scraper.py --html --json

# Update the GitHub Pages data file
python3 scraper.py --site
```

### Output files

| Flag | File | Description |
|---|---|---|
| `--html` | `movies.html` | Standalone sortable/filterable HTML page |
| `--json` | `movies.json` | Machine-readable JSON with all fields |
| `--site` | `docs/movies.json` | Data file for the GitHub Pages site |

`movies.html` and `movies.json` are gitignored. `docs/movies.json` is committed and served by GitHub Pages.

### Viewing in a browser

**Quickest — standalone file:**

```bash
python3 scraper.py --html
open movies.html          # macOS
# or: xdg-open movies.html  (Linux)
```

This opens a fully self-contained page — no server needed.

**Full site locally** (`docs/index.html` fetches `movies.json` via `fetch()`, so it needs a server):

```bash
python3 scraper.py --site
python3 -m http.server 8000 --directory docs
```

Then open `http://localhost:8000` in your browser.

## GitHub Pages site

The live site lives at `docs/index.html` and loads `docs/movies.json` at runtime. It shows all films in a filterable table grouped by title across theaters, with each theater linking to its booking page.

### Deploying

1. Fork or clone this repo.
2. In **Settings → Pages**, set Source to *Deploy from branch*, branch `main`, folder `/docs`.
3. In **Settings → Actions → General → Workflow permissions**, enable *Read and write permissions*.
4. Push — GitHub Actions will refresh the listings daily at 10 am ET via `.github/workflows/update-movies.yml`.

To trigger a manual refresh: **Actions → Update movie listings → Run workflow**.

## Adding a new theater

1. Write a scraper function in `scraper.py` that returns a `list[Movie]`.
2. Add a row to the `THEATERS` list near the bottom of the file:
   ```python
   ("My Theater", scrape_my_theater, scrape_my_theater_coming_soon),
   ```
   Pass `None` as the third element if the theater has no coming-soon page.
3. Run `python3 scraper.py --site` to verify, then push.

## Data model

Each film is a `Movie` dataclass with these fields:

| Field | Description |
|---|---|
| `title` | Film title |
| `theater` | Theater name |
| `url` | Film detail page URL |
| `status` | `"Now Playing"` or `"Coming Soon"` |
| `opens` | Opening date string, e.g. `"Opens May 1"` |
| `booking_url` | Direct ticket link (when available) |
| `year` | Release year |
| `director` | Director name(s) |
| `cast` | Top-billed cast |
| `country` | Country of origin |
| `showtimes` | List of showtime strings |
| `description` | Film synopsis |
