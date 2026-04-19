// Serverless function: fetch a Letterboxd watchlist and return titles + directors.
// Two-layer cache:
//   dirCache  — slug → directors[], lives for the lifetime of the warm function instance
//   wlCache   — username → full result, TTL 1 hour
// On a cold start both are empty; they warm up quickly and persist across requests.

const dirCache = new Map(); // slug → { dirs: string[], ts: number }
const wlCache  = new Map(); // username → { films: Film[], ts: number }

const DIR_TTL = 7 * 24 * 3600 * 1000; // 7 days
const WL_TTL  =      1 * 3600 * 1000; // 1 hour

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36';

async function get(url) {
  const r = await fetch(url, { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error(`HTTP ${r.status} — ${url}`);
  return r.text();
}

// Regex-based parse (no DOM available in Node edge runtime)
function parseWatchlistHtml(html) {
  const films = [];
  const seen  = new Set();
  // Each film entry has data-item-slug and data-item-name on the same element
  const re = /data-item-slug="([^"]+)"[^>]*?data-item-(?:full-display-)?name="([^"]+)"/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const slug = m[1];
    if (seen.has(slug)) continue;
    seen.add(slug);
    const title = m[2].replace(/\s*\(\d{4}\)\s*$/, '').trim();
    films.push({ slug, title });
  }
  return films;
}

async function fetchDirs(slug) {
  const hit = dirCache.get(slug);
  if (hit && Date.now() - hit.ts < DIR_TTL) return hit.dirs;

  const html = await get(`https://letterboxd.com/film/${slug}/`);
  const dirs = [];
  const re   = /href="\/director\/[^"]+"\s*><span class="prettify">([^<]+)<\/span>/g;
  let m;
  while ((m = re.exec(html)) !== null) dirs.push(m[1]);

  dirCache.set(slug, { dirs, ts: Date.now() });
  return dirs;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { username } = req.query;
  if (!username || !/^[a-zA-Z0-9_.-]{1,50}$/.test(username)) {
    return res.status(400).json({ error: 'Invalid username' });
  }

  const key = username.toLowerCase();

  // Return cached watchlist result if fresh
  const hit = wlCache.get(key);
  if (hit && Date.now() - hit.ts < WL_TTL) {
    res.setHeader('X-Cache', 'HIT');
    return res.json({ films: hit.films });
  }

  try {
    // ── 1. Fetch all watchlist pages ──
    const allFilms = [];
    const seenSlugs = new Set();
    for (let page = 1; page <= 30; page++) {
      const html  = await get(`https://letterboxd.com/${key}/watchlist/page/${page}/`);
      const films = parseWatchlistHtml(html);
      if (films.length === 0) break;
      for (const f of films) {
        if (!seenSlugs.has(f.slug)) {
          seenSlugs.add(f.slug);
          allFilms.push(f);
        }
      }
    }

    if (allFilms.length === 0) {
      return res.status(404).json({ error: 'No films found — check the username.' });
    }

    // ── 2. Fetch directors in parallel batches ──
    const BATCH = 12;
    for (let i = 0; i < allFilms.length; i += BATCH) {
      const batch = allFilms.slice(i, i + BATCH);
      const dirs  = await Promise.all(batch.map(f => fetchDirs(f.slug).catch(() => [])));
      batch.forEach((f, j) => { f.directors = dirs[j]; });
    }

    wlCache.set(key, { films: allFilms, ts: Date.now() });
    res.setHeader('X-Cache', 'MISS');
    res.json({ films: allFilms });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
}
