/* global React, PageShell, PageHero */
const { useState, useMemo } = React;
const h = React.createElement;

// Simple squarified-ish treemap for one sector's stocks (using market_cap as area)
function layoutTreemap(items, W, H) {
  const total = items.reduce((s, it) => s + it.weight, 0);
  if (!total) return [];
  const out = [];
  let x = 0, y = 0;
  let remaining = items.slice();
  let boundsW = W, boundsH = H;
  let boundsX = 0, boundsY = 0;

  while (remaining.length) {
    const horizontal = boundsW >= boundsH;
    // take biggest, then add siblings until aspect ratio stops improving
    remaining.sort((a, b) => b.weight - a.weight);
    const row = [];
    let rowSum = 0;
    let bestRatio = Infinity;
    for (const it of remaining) {
      const trial = [...row, it];
      const tSum = rowSum + it.weight;
      const side = horizontal ? boundsH : boundsW;
      const cross = (tSum / remaining.reduce((s, x) => s + x.weight, 0)) * (horizontal ? boundsW : boundsH);
      const worst = Math.max(...trial.map(t => {
        const area = (t.weight / tSum) * side * cross;
        const s1 = side * (t.weight / tSum);
        const s2 = cross;
        return Math.max(s1 / s2, s2 / s1);
      }));
      if (worst <= bestRatio) { row.push(it); rowSum = tSum; bestRatio = worst; }
      else break;
    }
    const sumRow = row.reduce((s, it) => s + it.weight, 0);
    const sumRem = remaining.reduce((s, it) => s + it.weight, 0);
    const cross = (sumRow / sumRem) * (horizontal ? boundsW : boundsH);
    let cursor = 0;
    for (const it of row) {
      const side = horizontal ? boundsH : boundsW;
      const len = (it.weight / sumRow) * side;
      if (horizontal) {
        out.push({ ...it, x: boundsX, y: boundsY + cursor, w: cross, h: len });
      } else {
        out.push({ ...it, x: boundsX + cursor, y: boundsY, w: len, h: cross });
      }
      cursor += len;
    }
    if (horizontal) { boundsX += cross; boundsW -= cross; } else { boundsY += cross; boundsH -= cross; }
    remaining = remaining.filter(it => !row.includes(it));
  }
  return out;
}

function heatColor(change) {
  if (change > 0) {
    const t = Math.min(Math.abs(change) / 5, 1);
    return `rgb(${24 + (1 - t) * 12},${ Math.round(100 + t * 80)},${24 + (1 - t) * 16})`;
  }
  if (change < 0) {
    const t = Math.min(Math.abs(change) / 5, 1);
    return `rgb(${Math.round(100 + t * 120)},${36 - t * 4},${36 - t * 4})`;
  }
  return '#2A2A2A';
}

window.HeatmapPage = function HeatmapPage() {
  const d = window.UI_DEMO;
  const [period, setPeriod] = useState('1d');
  const changeKey = { '1d': 'change_1d', '1w': 'change_1w', '1m': 'change_1m', '1y': 'change_1y' }[period];

  // Sector boxes weighted by total market cap
  const W = 1200, H = 620;
  const sectorItems = d.sectors.map(s => ({
    ...s, weight: s.stocks.reduce((sum, st) => sum + st.market_cap, 0),
  }));
  const sectorLayout = useMemo(() => layoutTreemap(sectorItems, W, H), [d]);

  return h(PageShell, null,
    h(PageHero, {
      icon: 'map',
      title: 'Карта рынка',
      subtitle: 'Изменение за выбранный период · размер = капитализация',
      right: h('div', { style: { display: 'inline-flex', background: 'var(--bg-primary)', border: '1px solid var(--border)', borderRadius: 12, padding: 3, gap: 2 } },
        [['1d','Д'], ['1w','Н'], ['1m','М'], ['1y','Г']].map(([id, lbl]) =>
          h('button', { key: id, onClick: () => setPeriod(id),
            style: { all: 'unset', cursor: 'pointer', padding: '6px 14px', fontSize: 13, fontWeight: 500, borderRadius: 9,
              color: period === id ? 'var(--fg-inverse)' : 'var(--fg-secondary)',
              background: period === id ? 'var(--accent)' : 'transparent' } }, lbl)
        )),
    }),

    h('div', { style: { background: 'var(--bg-widget)', border: '1px solid var(--border)', borderRadius: 16, padding: 16, boxShadow: 'var(--shadow-md)' } },
      h('div', { style: { position: 'relative', width: '100%', aspectRatio: `${W} / ${H}` } },
        sectorLayout.map((sector, si) => {
          const stockItems = sector.stocks.map(s => ({ ...s, weight: s.market_cap }));
          const inner = layoutTreemap(stockItems, sector.w, sector.h - 24);
          return h('div', { key: si, style: {
            position: 'absolute', left: `${sector.x / W * 100}%`, top: `${sector.y / H * 100}%`,
            width: `${sector.w / W * 100}%`, height: `${sector.h / H * 100}%`,
            padding: 2, boxSizing: 'border-box',
          } },
            h('div', { style: { fontSize: 11, fontWeight: 600, color: 'var(--fg-secondary)', padding: '4px 4px 2px', textTransform: 'uppercase', letterSpacing: '.05em', height: 22, overflow: 'hidden', whiteSpace: 'nowrap' } }, sector.name),
            h('div', { style: { position: 'relative', width: '100%', height: 'calc(100% - 22px)' } },
              inner.map((st, i) => {
                const ch = st[changeKey] ?? 0;
                const bg = heatColor(ch);
                const small = st.w < 60 || st.h < 40;
                return h('div', { key: i, title: `${st.secId} ${ch > 0 ? '+' : ''}${ch.toFixed(2)}%`,
                  style: {
                    position: 'absolute', left: st.x, top: st.y, width: st.w - 2, height: st.h - 2,
                    background: bg, borderRadius: 3,
                    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
                    color: '#fff', textShadow: '0 1px 3px rgba(0,0,0,0.8)', overflow: 'hidden', cursor: 'pointer',
                  } },
                  h('div', { style: { fontSize: small ? 9 : 13, fontWeight: 700 } }, st.secId),
                  !small && h('div', { style: { fontSize: 11, fontWeight: 600, opacity: .9, fontVariantNumeric: 'tabular-nums' } }, (ch > 0 ? '+' : '') + ch.toFixed(2) + '%'),
                );
              }),
            ),
          );
        }),
      ),

      // Legend
      h('div', { style: { marginTop: 16, padding: '12px 16px', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10, borderTop: '1px solid var(--border)' } },
        h('span', { style: { fontSize: 11, color: 'var(--fg-muted)' } }, '−5%'),
        h('div', { style: { display: 'flex', height: 10, borderRadius: 999, overflow: 'hidden', width: 280 } },
          [-5,-3,-1,0,1,3,5].map((v, i) =>
            h('div', { key: i, style: { flex: 1, background: heatColor(v) } })
          )),
        h('span', { style: { fontSize: 11, color: 'var(--fg-muted)' } }, '+5%'),
      ),
    ),
  );
};
