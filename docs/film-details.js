// Shared film-detail rendering for index.html (calendar) and list.html:
// poster, Letterboxd link + rating, per-theater event notes, and the showtimes
// panel shown when a film is opened (one row per date, a dot + time label per
// screening, coloured by theater). Also defines the morning / afternoon /
// evening time-of-day bands used by both pages' filters.
//
// Timelines are drawn as HTML, then FilmDetails.layout(container) measures each
// track and stacks time labels into lanes so they never overlap. Call it after
// inserting timeline HTML into a visible element (it also re-runs on resize).
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

    /* Theater legend (colour, link, event notes) */
    .fd-legend { margin-top: 16px; display: flex; flex-direction: column; gap: 8px; }
    .fd-theater-name { font-size: .84rem; color: #e8e8e8; text-decoration: none; font-weight: 600;
                       display: inline-flex; align-items: center; gap: 7px; }
    a.fd-theater-name:hover { text-decoration: underline; }
    .fd-swatch { width: 10px; height: 10px; border-radius: 50%; background: var(--c); flex-shrink: 0;
                 box-shadow: 0 0 0 2px rgba(0,0,0,.4); }
    .fd-notes { margin: 3px 0 0 17px; padding: 0; list-style: none; font-size: .74rem; color: #b9b9b9; line-height: 1.45; }
    .fd-notes li::before { content: "› "; color: #666; }
    .fd-range { font-size: .74rem; color: #888; margin: 2px 0 0 17px; }

    /* Day rows: each date is its own clearly separated row */
    .fd-schedule { margin-top: 16px; border-bottom: 1px solid #2c2c2c; }
    .fd-row { display: grid; grid-template-columns: 76px 1fr; align-items: end; gap: 12px;
              padding: 8px 0 9px; border-top: 1px solid #2c2c2c; }
    .fd-row.fd-axis-row { border-top: 0; padding: 0 0 4px; align-items: center; }
    .fd-row.fd-weekend { background: linear-gradient(90deg, rgba(229,193,0,.05), transparent 70%); }
    .fd-date { align-self: center; line-height: 1.2; white-space: nowrap; }
    .fd-date b { display: block; font-size: .8rem; color: #e8e8e8; font-weight: 700; }
    .fd-date span { font-size: .72rem; color: #888; }
    .fd-date.fd-today b { color: #e5c100; }
    .fd-axis { position: relative; height: 14px; font-size: .64rem; color: #666; }
    .fd-axis span { position: absolute; transform: translateX(-50%); white-space: nowrap; }

    /* Track: dots sit on the baseline; time labels stack above them */
    .tl-track { position: relative; height: 34px; }
    .tl-base { position: absolute; left: 0; right: 0; bottom: 3px; height: 12px; border-radius: 6px; background: #1d1d1d; }
    .tl-band { position: absolute; top: 0; bottom: 0; }
    .tl-band.b-morning   { background: rgba(255, 220, 120, .06); border-radius: 6px 0 0 6px; }
    .tl-band.b-evening   { background: rgba(120, 140, 255, .08); border-radius: 0 6px 6px 0; }
    .tl-band.b-on { box-shadow: inset 0 -2px 0 rgba(229, 193, 0, .7); }
    .tl-tick { position: absolute; bottom: 3px; height: 12px; width: 1px; background: #333; }
    .tl-dot { position: absolute; bottom: 9px; width: 20px; height: 20px; margin: 0 0 -10px -10px; padding: 0;
              border: 0; background: none; cursor: pointer; border-radius: 50%; font: inherit; color: var(--c); }
    .tl-dot::after { content: ""; position: absolute; inset: 5px; border-radius: 50%; background: var(--c);
                     box-shadow: 0 0 0 2px #111; transition: transform .12s; }
    .tl-dot:hover::after, .tl-dot:focus-visible::after, .tl-dot.tl-active::after { transform: scale(1.4); }
    .tl-dot:focus { outline: none; }
    .tl-dot.tl-sold::after { background: #111; box-shadow: inset 0 0 0 2px var(--c), 0 0 0 2px #111; }
    .tl-lbl { position: absolute; left: 50%; bottom: calc(15px + var(--lane, 0) * 13px); transform: translateX(-50%);
              font-size: .68rem; font-weight: 700; line-height: 1; white-space: nowrap; color: var(--c);
              font-variant-numeric: tabular-nums; pointer-events: none; text-shadow: 0 0 3px #111, 0 0 3px #111; }
    .tl-lbl::before { content: ""; position: absolute; left: var(--tick-x, 50%); top: 100%; width: 1px;
                      height: calc(var(--lane, 0) * 13px + 1px); background: var(--c); opacity: .45; }
    .tl-dot.tl-nolabel .tl-lbl { display: none; }
    .tl-dot.tl-sold .tl-lbl { text-decoration: line-through; }
    .tl-dot.tl-dim { opacity: .28; }
    .fd-more { font-size: .72rem; color: #777; padding: 6px 0 8px 88px; border-top: 1px solid #2c2c2c; }

    /* Tooltip */
    .fd-tip { position: fixed; z-index: 1000; pointer-events: none; background: #f2f2f2; color: #111;
              font: 600 .74rem/1.35 Helvetica, Arial, sans-serif; padding: 5px 8px; border-radius: 4px;
              box-shadow: 0 4px 14px rgba(0,0,0,.5); white-space: nowrap; opacity: 0; transition: opacity .08s; }
    .fd-tip.on { opacity: 1; }
    .fd-tip small { display: block; font-weight: 400; color: #555; }

    @media (max-width: 520px) {
      .fd-poster { width: 84px; }
      .fd-row { grid-template-columns: 58px 1fr; gap: 8px; }
      .fd-more { padding-left: 66px; }
    }
  `;
  const style = document.createElement('style');
  style.textContent = CSS;
  document.head.appendChild(style);

  const DAY = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  // One fixed colour per theater, everywhere on the site
  const THEATER_COLORS = {
    'Metrograph':               '#f2c94c',
    'Film Forum':               '#ff6b6b',
    'IFC Center':               '#4dabf7',
    'Angelika Film Center':     '#ff922b',
    'BAM':                      '#cc5de8',
    'Alamo Drafthouse NYC':     '#20c997',
    'Nitehawk (Prospect Park)': '#f783ac',
    'Nitehawk (Williamsburg)':  '#e599f7',
    'Paris Theater':            '#a9e34b',
    'Film at Lincoln Center':   '#dee2e6',
    'Film Noir Cinema':         '#ffa8a8',
    'Low Cinema':               '#99e9f2',
    'Cobble Hill Cinemas':      '#ffc078',
    'Williamsburg Cinemas':     '#66d9e8',
    'Kew Gardens Cinema':       '#b197fc',
    'Mamaroneck Cinemas':       '#8ce99a',
  };
  const FALLBACK = ['#ffd8a8', '#63e6be', '#91a7ff', '#faa2c1', '#c0eb75', '#ffe066'];
  function theaterColor(name) {
    if (THEATER_COLORS[name]) return THEATER_COLORS[name];
    let h = 0;
    for (const ch of String(name)) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
    return FALLBACK[h % FALLBACK.length];
  }

  // Timeline axis: 10 AM → 1 AM (late shows past midnight count as the same evening)
  const AXIS_START = 10 * 60, AXIS_END = 25 * 60;
  const BANDS = [
    { key: 'morning',   label: 'Morning',   hint: 'before 12 PM', from: 0,       to: 12 * 60 },
    { key: 'afternoon', label: 'Afternoon', hint: '12–5 PM',      from: 12 * 60, to: 17 * 60 },
    { key: 'evening',   label: 'Evening',   hint: '5 PM–late',    from: 17 * 60, to: 29 * 60 },
  ];

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

  // "6:30 PM" → minutes since midnight; shows before 5 AM belong to the previous evening
  function minutesOf(t) {
    const m = /^(\d{1,2}):(\d{2})\s*(AM|PM)$/i.exec((t || '').trim());
    if (!m) return null;
    let mins = (Number(m[1]) % 12 + (m[3].toUpperCase() === 'PM' ? 12 : 0)) * 60 + Number(m[2]);
    if (mins < 5 * 60) mins += 24 * 60;
    return mins;
  }
  // "6:30 PM" → "6:30p", "7:00 PM" → "7p" (compact label for the timeline)
  function shortTime(t) {
    const m = /^(\d{1,2}):(\d{2})\s*(AM|PM)$/i.exec((t || '').trim());
    if (!m) return t || '';
    return `${m[1]}${m[2] === '00' ? '' : ':' + m[2]}${m[3][0].toLowerCase()}`;
  }
  function bandOf(t) {
    const mins = minutesOf(t);
    if (mins === null) return null;
    return (BANDS.find(b => mins >= b.from && mins < b.to) || BANDS[2]).key;
  }
  // bands: Set of band keys; empty/undefined = no filter
  function inBands(st, bands) { return !bands || !bands.size || bands.has(bandOf(st.time)); }
  function pct(mins) {
    const p = (Math.min(Math.max(mins, AXIS_START), AXIS_END) - AXIS_START) / (AXIS_END - AXIS_START) * 100;
    return Math.min(Math.max(p, 2), 98);
  }

  // Dates on which a film has a screening inside the selected bands (for filtering)
  function bandDates(theaters, bands) {
    const out = new Set();
    (theaters || []).forEach(t => (t.showtimes || []).forEach(s => { if (s.time && inBands(s, bands)) out.add(s.date); }));
    return [...out].sort();
  }

  // ── Header bits ──────────────────────────────────────────────────────────
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

  // ── Timelines ────────────────────────────────────────────────────────────
  // Map date → [{st, mins, theater, color}] for screenings with known times
  function dotsByDay(theaters, { from = todayIso(), onlyIso = null } = {}) {
    const byDay = new Map();
    const merged = new Map();   // same theater + date + time (e.g. one Alamo time at 3 venues) → one dot
    (theaters || []).forEach(t => (t.showtimes || []).forEach(st => {
      const mins = minutesOf(st.time);
      if (mins === null || (onlyIso ? st.date !== onlyIso : st.date < from)) return;
      const key = `${t.name}|${st.date}|${st.time}`;
      const prev = merged.get(key);
      if (prev) {
        const sold = /sold out/i.test(prev.st.label) && /sold out/i.test(st.label || '');
        const labels = [prev.st.label, st.label || ''].join(' · ').split(' · ')
          .filter(l => l && !/sold out/i.test(l));
        prev.st = { ...prev.st, label: [...new Set(labels)].concat(sold ? ['Sold out'] : []).join(' · ') };
        return;
      }
      const entry = { st: { ...st, label: st.label || '' }, mins, theater: t.name, color: theaterColor(t.name) };
      merged.set(key, entry);
      if (!byDay.has(st.date)) byDay.set(st.date, []);
      byDay.get(st.date).push(entry);
    }));
    byDay.forEach(list => list.sort((a, b) => a.mins - b.mins));
    return new Map([...byDay.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1)));
  }

  function dotHtml(d, { bands, label = true }) {
    const sold = /sold out/i.test(d.st.label || '');
    const extra = (d.st.label || '').replace(/(^|\s*·\s*)Sold out/i, '').trim();
    const sub = [d.theater, extra, sold ? 'Sold out' : ''].filter(Boolean).join(' · ');
    const cls = ['tl-dot', sold ? 'tl-sold' : '', inBands(d.st, bands) ? '' : 'tl-dim'].filter(Boolean).join(' ');
    return `<button type="button" class="${cls}" style="left:${pct(d.mins).toFixed(2)}%;--c:${d.color}"`
         + ` data-tip="${esc(d.st.time)}" data-sub="${esc(sub)}" aria-label="${esc(`${d.st.time}, ${sub}`)}">`
         + (label ? `<span class="tl-lbl">${esc(shortTime(d.st.time))}</span>` : '') + `</button>`;
  }

  function trackHtml(dots, { bands } = {}) {
    let html = '<div class="tl-track"><div class="tl-base">';
    BANDS.forEach(b => {
      if (b.key === 'afternoon' && !(bands && bands.has(b.key))) return;  // afternoon = plain base
      const l = b.from <= AXIS_START ? 0 : pct(b.from);
      const r = b.to >= AXIS_END ? 100 : pct(b.to);
      html += `<div class="tl-band b-${b.key}${bands && bands.has(b.key) ? ' b-on' : ''}" style="left:${l.toFixed(2)}%;width:${(r - l).toFixed(2)}%"></div>`;
    });
    html += '</div>';
    [12 * 60, 17 * 60, 21 * 60].forEach(m => { html += `<div class="tl-tick" style="left:${pct(m)}%"></div>`; });
    dots.forEach(d => { html += dotHtml(d, { bands }); });
    return html + '</div>';
  }

  function axisHtml() {
    const marks = [[11 * 60, '11a'], [12 * 60, 'Noon'], [17 * 60, '5 PM'], [21 * 60, '9 PM'], [24 * 60, '12a']];
    return `<div class="fd-row fd-axis-row"><span></span><div class="fd-axis">`
         + marks.map(([m, l]) => `<span style="left:${pct(m)}%">${l}</span>`).join('') + `</div></div>`;
  }

  function dayRow(iso, list, { bands }) {
    const d = isoToDate(iso), today = iso === todayIso();
    const weekend = [0, 5, 6].includes(d.getDay());
    return `<div class="fd-row${weekend ? ' fd-weekend' : ''}">`
         + `<div class="fd-date${today ? ' fd-today' : ''}"><b>${today ? 'Today' : DAY[d.getDay()]}</b><span>${MON[d.getMonth()]} ${d.getDate()}</span></div>`
         + trackHtml(list, { bands }) + '</div>';
  }

  function datesFallback(t) {
    const today = todayIso();
    const dates = (t.show_dates || []).filter(d => d >= today);
    let text = '';
    if (dates.length) {
      text = dates.slice(0, 8).map(fmtDay).join(', ') + (dates.length > 8 ? ` +${dates.length - 8} more` : '');
    } else if (t.date_start) {
      text = fmtDay(t.date_start) + (t.date_end && t.date_end !== t.date_start ? ` – ${fmtDay(t.date_end)}` : '');
    } else if (t.opens) {
      text = `Opens ${t.opens}`;
    }
    return text ? `<div class="fd-range">${esc(text)} · daily times on theater site</div>` : '';
  }

  // Modal: theater legend (colour, link, notes) + one row per date
  function scheduleHtml(theaters, { bands, maxDays = 14 } = {}) {
    theaters = theaters || [];
    const byDay = dotsByDay(theaters);
    const timed = new Set();
    byDay.forEach(list => list.forEach(d => timed.add(d.theater)));

    let html = '<div class="fd-legend">';
    theaters.forEach(t => {
      const link = safeUrl(t.url) || safeUrl(t.booking_url);
      const name = `<span class="fd-swatch" style="--c:${theaterColor(t.name)}"></span>${esc(t.name)}`;
      html += '<div>' + (link ? `<a class="fd-theater-name" href="${esc(link)}" target="_blank" rel="noopener">${name} ↗</a>`
                               : `<span class="fd-theater-name">${name}</span>`);
      const notes = (t.notes || []).slice(0, 6);
      if (notes.length) html += `<ul class="fd-notes">${notes.map(n => `<li>${esc(n)}</li>`).join('')}</ul>`;
      if (!timed.has(t.name)) html += datesFallback(t);
      html += '</div>';
    });
    html += '</div>';

    if (byDay.size) {
      const days = [...byDay.keys()];
      html += '<div class="fd-schedule">' + axisHtml();
      days.slice(0, maxDays).forEach(iso => { html += dayRow(iso, byDay.get(iso), { bands }); });
      if (days.length > maxDays) html += `<div class="fd-more">+${days.length - maxDays} more days</div>`;
      html += '</div>';
    }
    return html;
  }

  // ── Label layout: stack time labels into lanes so none overlap ───────────
  const LANE_H = 13, BASE_H = 32, GAP = 4;
  // Two passes (measure everything, then write) so long lists don't force a reflow per label
  function layout(root) {
    const tracks = [...(root || document).querySelectorAll('.tl-track')];
    tracks.forEach(t => t.querySelectorAll('.tl-nolabel').forEach(d => d.classList.remove('tl-nolabel')));
    const measured = tracks.map(track => ({
      track,
      width: track.clientWidth,
      maxLanes: 5,
      dots: [...track.querySelectorAll('.tl-dot')].map(dot => {
        const lbl = dot.querySelector('.tl-lbl');
        return { dot, lbl, w: lbl ? lbl.offsetWidth : 0, half: dot.offsetWidth / 2 };
      }).filter(x => x.lbl),
    }));
    measured.forEach(({ track, width, maxLanes, dots }) => {
      if (!width) return;
      const laneRight = [];
      let used = 0;
      dots.forEach(({ dot, lbl, w, half }) => {
        const center = parseFloat(dot.style.left) / 100 * width;
        const left = Math.max(0, Math.min(center - w / 2, width - w));   // keep labels inside the track
        let lane = laneRight.findIndex(r => left >= r + GAP);
        if (lane === -1 && laneRight.length < maxLanes) lane = laneRight.length;
        if (lane === -1) { dot.classList.add('tl-nolabel'); return; }    // no room: time stays in the tooltip
        laneRight[lane] = left + w;
        used = Math.max(used, lane + 1);
        // the dot's button is centred on `center`, so its left edge sits at center - half
        lbl.style.left = '0';
        lbl.style.transform = `translateX(${(left - center + half).toFixed(1)}px)`;
        lbl.style.setProperty('--tick-x', `${(center - left).toFixed(1)}px`);  // connector down to the dot
        dot.style.setProperty('--lane', lane);
      });
      track.style.height = `${BASE_H + Math.max(0, used - 1) * LANE_H}px`;
    });
  }
  let resizeTimer = null;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => layout(document), 120);
  });

  // ── Tooltip (hover on desktop, tap on touch) ─────────────────────────────
  const tip = document.createElement('div');
  tip.className = 'fd-tip';
  tip.setAttribute('role', 'tooltip');
  let tipFor = null;

  function showTip(dot) {
    if (!document.body.contains(tip)) document.body.appendChild(tip);
    if (tipFor && tipFor !== dot) tipFor.classList.remove('tl-active');
    tipFor = dot;
    dot.classList.add('tl-active');
    tip.innerHTML = `${esc(dot.dataset.tip)}${dot.dataset.sub ? `<small>${esc(dot.dataset.sub)}</small>` : ''}`;
    tip.classList.add('on');
    const r = dot.getBoundingClientRect(), tr = tip.getBoundingClientRect();
    let left = r.left + r.width / 2 - tr.width / 2;
    left = Math.max(6, Math.min(left, window.innerWidth - tr.width - 6));
    let top = r.bottom + 6;
    if (top + tr.height > window.innerHeight - 6) top = r.top - tr.height - 6;
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
  }
  function hideTip() {
    tip.classList.remove('on');
    if (tipFor) tipFor.classList.remove('tl-active');
    tipFor = null;
  }

  document.addEventListener('pointerover', e => {
    const dot = e.target.closest && e.target.closest('.tl-dot');
    if (dot && e.pointerType === 'mouse') showTip(dot);
  });
  document.addEventListener('pointerout', e => {
    const dot = e.target.closest && e.target.closest('.tl-dot');
    if (dot && e.pointerType === 'mouse' && !dot.contains(e.relatedTarget)) hideTip();
  });
  document.addEventListener('focusin', e => { if (e.target.classList && e.target.classList.contains('tl-dot')) showTip(e.target); });
  document.addEventListener('focusout', e => { if (e.target === tipFor) hideTip(); });
  // Capture phase: a tap on a dot shows its details instead of triggering the row/modal underneath
  document.addEventListener('click', e => {
    const dot = e.target.closest && e.target.closest('.tl-dot');
    if (dot) {
      e.stopPropagation();
      e.preventDefault();
      if (tipFor === dot && tip.classList.contains('on') && e.pointerType !== 'mouse') hideTip(); else showTip(dot);
    } else if (tipFor) {
      hideTip();
    }
  }, true);
  window.addEventListener('scroll', hideTip, true);
  window.addEventListener('resize', hideTip);

  window.FilmDetails = {
    posterHtml, linksHtml, scheduleHtml, layout, theaterColor,
    BANDS, bandOf, inBands, bandDates, esc,
  };
})();
