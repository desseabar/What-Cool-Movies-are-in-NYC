// Shared film-detail rendering for index.html (calendar) and list.html:
// poster, Letterboxd link + rating, per-theater event notes, and showtimes grouped by day.
(function () {
  const CSS = `
    .fd-head { display: flex; gap: 18px; align-items: flex-start; margin-bottom: 12px; }
    .fd-head-text { flex: 1; min-width: 0; }
    .fd-poster { width: 118px; flex-shrink: 0; border-radius: 4px; display: block;
                 box-shadow: 0 2px 10px rgba(0,0,0,.55); background: #222; }
    .fd-links { display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
                font-size: .78rem; margin: 2px 0 10px; }
    .fd-lb { color: #00e054; text-decoration: none; border: 1px solid #1d5c33; border-radius: 4px;
             padding: 2px 8px; white-space: nowrap; }
    .fd-lb:hover { border-color: #00e054; }
    .fd-lb.fd-search { color: #8aa; border-color: #345; }
    .fd-rating { color: #bbb; white-space: nowrap; }
    .fd-theater { margin-top: 14px; }
    .fd-theater-name { font-size: .84rem; color: #e5c100; text-decoration: none; font-weight: 600; }
    .fd-theater-name:hover { text-decoration: underline; }
    .fd-notes { margin: 4px 0 6px; padding: 0; list-style: none; font-size: .74rem; color: #c9b45c; line-height: 1.45; }
    .fd-notes li::before { content: "› "; color: #776a2c; }
    .fd-day { display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; margin: 4px 0; font-size: .78rem; }
    .fd-date { color: #888; width: 84px; flex-shrink: 0; }
    .fd-date.fd-today { color: #e5c100; }
    .fd-times { display: flex; gap: 5px; flex-wrap: wrap; flex: 1; }
    .fd-time { color: #eee; background: #262626; border: 1px solid #383838; border-radius: 3px;
               padding: 1px 7px; text-decoration: none; font-variant-numeric: tabular-nums; white-space: nowrap; }
    a.fd-time:hover { border-color: #e5c100; color: #e5c100; }
    .fd-time.fd-sold { opacity: .45; text-decoration: line-through; }
    .fd-label { color: #9a9a9a; font-size: .68rem; margin-left: 4px; }
    .fd-more, .fd-range { font-size: .74rem; color: #888; margin-top: 3px; }
    @media (max-width: 520px) { .fd-poster { width: 84px; } .fd-date { width: 72px; } }
  `;
  const style = document.createElement('style');
  style.textContent = CSS;
  document.head.appendChild(style);

  const DAY = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const MAX_DAYS = 10;

  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function isoToDate(s) { const [y, m, d] = s.split('-').map(Number); return new Date(y, m - 1, d); }
  function todayIso() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  }
  function fmtDay(iso) { const d = isoToDate(iso); return `${DAY[d.getDay()]} ${MON[d.getMonth()]} ${d.getDate()}`; }
  function safeUrl(u) { return /^https?:\/\//i.test(u || '') ? u : ''; }

  function posterHtml(m) {
    const src = safeUrl(m.poster);
    return src ? `<img class="fd-poster" src="${esc(src)}" alt="Poster for ${esc(m.title)}" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()">` : '';
  }

  function linksHtml(m, title) {
    const url = safeUrl(m.letterboxd_url);
    let html = url
      ? `<a class="fd-lb" href="${esc(url)}" target="_blank" rel="noopener">Letterboxd ↗</a>`
      : `<a class="fd-lb fd-search" href="https://letterboxd.com/search/films/${encodeURIComponent(title || m.title)}/" target="_blank" rel="noopener">Search Letterboxd ↗</a>`;
    if (m.letterboxd_rating) html += `<span class="fd-rating">★ ${esc(m.letterboxd_rating)} avg</span>`;
    return `<div class="fd-links">${html}</div>`;
  }

  function timeHtml(st) {
    const sold = /sold out/i.test(st.label || '');
    const label = (st.label || '').replace(/(^|\s*·\s*)Sold out/i, '').trim();
    const inner = `${esc(st.time || 'Time TBA')}${label ? `<span class="fd-label">${esc(label)}</span>` : ''}`;
    // Plain chips: the theater name above links to the film's page on the theater's site
    return `<span class="fd-time${sold ? ' fd-sold' : ''}"${sold ? ' title="Sold out"' : ''}>${inner}</span>`;
  }

  function datesFallback(t) {
    const today = todayIso();
    const dates = (t.show_dates || []).filter(d => d >= today);
    if (dates.length) {
      const shown = dates.slice(0, 12).map(fmtDay).join(', ');
      return `<div class="fd-range">${esc(shown)}${dates.length > 12 ? ` +${dates.length - 12} more` : ''} · times on theater site</div>`;
    }
    if (t.date_start) {
      let label = fmtDay(t.date_start);
      if (t.date_end && t.date_end !== t.date_start) label += ` – ${fmtDay(t.date_end)}`;
      return `<div class="fd-range">${esc(label)}</div>`;
    }
    return t.opens ? `<div class="fd-range">Opens ${esc(t.opens)}</div>` : '';
  }

  function theaterHtml(t) {
    const today = todayIso();
    const link = safeUrl(t.url) || safeUrl(t.booking_url);
    let html = `<div class="fd-theater">`;
    html += link ? `<a class="fd-theater-name" href="${esc(link)}" target="_blank" rel="noopener">${esc(t.name)}</a>`
                 : `<span class="fd-theater-name">${esc(t.name)}</span>`;
    const notes = (t.notes || []).slice(0, 6);
    if (notes.length) html += `<ul class="fd-notes">${notes.map(n => `<li>${esc(n)}</li>`).join('')}</ul>`;

    const upcoming = (t.showtimes || []).filter(s => s.date >= today);
    if (upcoming.length) {
      const byDay = new Map();
      upcoming.forEach(s => { if (!byDay.has(s.date)) byDay.set(s.date, []); byDay.get(s.date).push(s); });
      const days = [...byDay.keys()];
      days.slice(0, MAX_DAYS).forEach(d => {
        html += `<div class="fd-day"><span class="fd-date${d === today ? ' fd-today' : ''}">${d === today ? 'Today' : fmtDay(d)}</span>`
              + `<span class="fd-times">${byDay.get(d).map(timeHtml).join('')}</span></div>`;
      });
      if (days.length > MAX_DAYS) html += `<div class="fd-more">+${days.length - MAX_DAYS} more days</div>`;
    } else {
      html += datesFallback(t);
    }
    return html + '</div>';
  }

  function theatersHtml(theaters) { return (theaters || []).map(theaterHtml).join(''); }

  // "6:30 PM, 9:00 PM" for one day across theaters — used for calendar cell tooltips
  function timesOn(theaters, iso) {
    const out = [];
    (theaters || []).forEach(t => (t.showtimes || []).forEach(s => {
      if (s.date === iso && s.time) out.push((theaters.length > 1 ? `${s.time} (${t.name})` : s.time) + (s.label ? ` · ${s.label}` : ''));
    }));
    return out;
  }

  window.FilmDetails = { posterHtml, linksHtml, theatersHtml, timesOn, esc };
})();
