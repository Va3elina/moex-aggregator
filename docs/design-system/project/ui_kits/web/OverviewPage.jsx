/* global React, Widget, PageShell, Icon, IconTile, ChartBox, PeriodGroup, hexA */
const h = React.createElement;

// ── Tiny line chart (SVG, no deps)
function Spark({ data, color = 'var(--accent)', height = 60, fill = true }) {
  if (!data || !data.length) return null;
  const w = 100, hh = height;
  const ys = data.map(d => d.value ?? d.fear_index ?? d);
  const min = Math.min(...ys), max = Math.max(...ys);
  const pts = ys.map((y, i) => [i / (ys.length - 1) * w, hh - ((y - min) / (max - min || 1)) * (hh - 4) - 2]);
  const d = pts.map((p, i) => (i === 0 ? 'M' : 'L') + p[0].toFixed(2) + ',' + p[1].toFixed(2)).join(' ');
  const dFill = d + ` L${w},${hh} L0,${hh} Z`;
  return h('svg', { viewBox: `0 0 ${w} ${hh}`, preserveAspectRatio: 'none', style: { width: '100%', height: hh, display: 'block' } },
    fill && h('path', { d: dFill, fill: color, opacity: .15 }),
    h('path', { d, stroke: color, strokeWidth: 1.5, fill: 'none' }),
  );
}
window.Spark = Spark;

window.OverviewPage = function OverviewPage({ setRoute }) {
  const d = window.UI_DEMO;
  const fearColor = d.fearCurrent.fear_index >= 55 ? '#F97316' : '#22c55e';
  const breadthPct = d.breadth.percent_above;

  // Overview OI chart
  const price = d.oi.price;

  return h(PageShell, null,
    // Hero row
    h('div', { style: { display: 'flex', alignItems: 'flex-end', justifyContent: 'space-between', marginBottom: 28, flexWrap: 'wrap', gap: 16 } },
      h('div', null,
        h('h1', { style: { fontSize: 32, fontWeight: 700, margin: 0, letterSpacing: '-.02em' } }, 'Обзор рынка'),
        h('div', { style: { fontSize: 14, color: 'var(--fg-secondary)', marginTop: 4 } }, 'Все ключевые индикаторы настроений — в одном месте'),
      ),
      h('div', { style: { display: 'flex', alignItems: 'center', gap: 8, fontSize: 12, color: 'var(--fg-muted)' } },
        h('span', { style: { width: 8, height: 8, borderRadius: '50%', background: '#10b981' } }),
        'Обновлено в 14:32 MSK',
      ),
    ),

    // 3×2 widget grid
    h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(300px,1fr))', gap: 20 } },
      h(Widget, { title: 'Индекс страха', subtitle: 'Страх / жадность', tone: 'warning', onClick: () => setRoute('fear') },
        h('div', { style: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8 } },
          h('div', { style: { fontSize: 72, fontWeight: 700, color: fearColor, lineHeight: 1, fontVariantNumeric: 'tabular-nums' } }, d.fearCurrent.fear_index),
          h('div', { style: { fontSize: 12, color: fearColor, padding: '4px 10px', background: hexA(fearColor, .14), borderRadius: 999 } }, d.fearCurrent.fear_index >= 55 ? 'Страх' : 'Жадность'),
        ),
      ),

      h(Widget, { title: 'Карта рынка', subtitle: 'Сегодня', tone: 'positive', onClick: () => setRoute('heatmap') },
        h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gridTemplateRows: 'repeat(3, 28px)', gap: 3, width: '100%' } },
          d.stocks.slice(0, 12).map((s, i) => {
            const c = s.change_1d;
            const intensity = Math.min(Math.abs(c) / 5, 1);
            const bg = c > 0.2 ? `rgba(22,163,74,${0.25 + intensity * 0.6})` : c < -0.2 ? `rgba(220,38,38,${0.25 + intensity * 0.6})` : 'rgba(42,42,42,0.8)';
            return h('div', { key: i, style: { background: bg, borderRadius: 3, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 600, color: '#fff' } }, s.secId);
          }),
        ),
      ),

      h(Widget, { title: 'Сила рынка', subtitle: '% выше EMA200', tone: breadthPct >= 50 ? 'positive' : 'negative', onClick: () => setRoute('strength') },
        h('div', { style: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, width: '100%' } },
          h('div', { style: { fontSize: 72, fontWeight: 700, color: breadthPct >= 50 ? '#22c55e' : '#ef4444', lineHeight: 1 } }, breadthPct + '%'),
          h('div', { style: { width: '100%', height: 8, background: 'var(--bg-tertiary)', borderRadius: 999, overflow: 'hidden' } },
            h('div', { style: { width: breadthPct + '%', height: '100%', background: 'linear-gradient(90deg,#22c55e,#4ade80)' } }),
          ),
          h('div', { style: { fontSize: 11, color: 'var(--fg-muted)' } }, `${d.breadth.count_above} из ${d.breadth.count_total}`),
        ),
      ),

      h(Widget, { title: 'Индикатор Баффета', subtitle: 'Капитализация / ВВП', tone: 'positive', onClick: () => setRoute('buffett') },
        h('div', { style: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8 } },
          h('div', { style: { fontSize: 72, fontWeight: 700, color: '#22c55e', lineHeight: 1 } }, d.buffett.ratio + '%'),
          h('div', { style: { fontSize: 12, color: '#22c55e', padding: '4px 10px', background: 'rgba(34,197,94,.13)', borderRadius: 999 } }, 'Рынок недооценён'),
        ),
      ),

      h(Widget, { title: 'Открытый интерес', subtitle: 'RTS · крупные игроки', tone: 'neutral', onClick: () => setRoute('oi') },
        h('div', { style: { width: '100%' } }, h(Spark, { data: price, color: '#3B82F6', height: 80 })),
      ),

      h(Widget, { title: 'Фонды', subtitle: 'Потоки и СЧА', tone: 'neutral', onClick: () => setRoute('funds') },
        h('div', { style: { width: '100%', display: 'flex', flexDirection: 'column', gap: 8 } },
          d.funds.categories.map((c, i) =>
            h('div', { key: i, style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '4px 0', borderBottom: i < d.funds.categories.length - 1 ? '1px solid var(--border)' : 'none' } },
              h('span', { style: { fontSize: 13, color: 'var(--fg-primary)' } }, c.name),
              h('span', { style: { fontSize: 13, fontWeight: 600, color: c.change_pct >= 0 ? '#22c55e' : '#ef4444', fontVariantNumeric: 'tabular-nums' } }, (c.change_pct >= 0 ? '+' : '') + c.change_pct.toFixed(2) + '%'),
            )
          ),
        ),
      ),
    ),
  );
};
