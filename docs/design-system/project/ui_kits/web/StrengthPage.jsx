/* global React, PageShell, PageHero, MetricHero, ChartBox */
const h = React.createElement;

window.StrengthPage = function StrengthPage() {
  const d = window.UI_DEMO;
  const pct = d.breadth.percent_above;
  const color = pct >= 70 ? '#EF4444' : pct >= 50 ? '#22C55E' : pct >= 30 ? '#EAB308' : '#EF4444';
  const status = pct >= 70 ? 'Перекупленность' : pct >= 50 ? 'Восходящий тренд' : pct >= 30 ? 'Нейтрально' : 'Перепроданность';

  return h(PageShell, null,
    h(PageHero, {
      icon: 'strength',
      title: 'Сила рынка',
      subtitle: '% акций с ценой выше 200-дн. EMA',
    }),

    h('div', { style: { display: 'grid', gridTemplateColumns: '360px 1fr', gap: 20, marginBottom: 20 } },
      h(MetricHero, { value: pct + '%', status, statusColor: color, caption: `${d.breadth.count_above} из ${d.breadth.count_total} акций` }),

      h(ChartBox, { title: 'По секторам', height: 320 },
        h('div', { style: { display: 'flex', flexDirection: 'column', gap: 12, height: '100%', overflowY: 'auto' } },
          d.sectorBreadth.map((s, i) => {
            const sp = Math.round(s.above / s.total * 100);
            const c = sp >= 50 ? '#22C55E' : sp >= 30 ? '#EAB308' : '#EF4444';
            return h('div', { key: i },
              h('div', { style: { display: 'flex', justifyContent: 'space-between', fontSize: 13, marginBottom: 4 } },
                h('span', { style: { color: 'var(--fg-primary)', fontWeight: 500 } }, s.name),
                h('span', { style: { color: c, fontWeight: 600, fontVariantNumeric: 'tabular-nums' } }, sp + '%'),
              ),
              h('div', { style: { height: 8, background: 'var(--bg-tertiary)', borderRadius: 999, overflow: 'hidden' } },
                h('div', { style: { width: sp + '%', height: '100%', background: c, transition: 'width .4s' } })
              ),
              h('div', { style: { fontSize: 11, color: 'var(--fg-muted)', marginTop: 2 } }, `${s.above} из ${s.total}`),
            );
          }),
        ),
      ),
    ),

    // Mini EMA chart box
    h(ChartBox, { title: 'История силы рынка · 3М', height: 200 },
      h('div', { style: { height: '100%', display: 'flex', alignItems: 'flex-end', gap: 1 } },
        Array.from({ length: 90 }, (_, i) => {
          const v = 40 + Math.sin(i / 7) * 18 + (Math.random() - 0.5) * 6;
          const c = v >= 50 ? 'rgba(34,197,94,.7)' : 'rgba(239,68,68,.7)';
          return h('div', { key: i, style: { flex: 1, height: `${v}%`, background: c, borderRadius: '2px 2px 0 0' } });
        }),
      ),
    ),
  );
};
