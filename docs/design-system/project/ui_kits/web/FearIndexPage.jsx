/* global React, PageShell, PageHero, MetricHero, ChartBox, ScaleBar, PeriodGroup, Spark */
const { useState } = React;
const h = React.createElement;

window.FearIndexPage = function FearIndexPage() {
  const d = window.UI_DEMO;
  const [period, setPeriod] = useState('3М');

  const val = d.fearCurrent.fear_index;
  const color = val >= 75 ? '#EF4444' : val >= 55 ? '#F97316' : val >= 45 ? '#EAB308' : val >= 25 ? '#84CC16' : '#22C55E';
  const label = val >= 75 ? 'Экстремальный страх' : val >= 55 ? 'Страх' : val >= 45 ? 'Нейтрально' : val >= 25 ? 'Жадность' : 'Экстремальная жадность';

  // Period slice
  const days = { '1М': 30, '3М': 90, '6М': 90, '1Г': 90, 'Всё': 90 }[period];
  const history = d.fearHistory.slice(-days);

  // Components — visualized as mini-bars from raw drift
  const components = [
    { name: 'Моментум',   value: 58, desc: 'Цена против 125-дн. средней' },
    { name: 'Широта',     value: 68, desc: '% акций на максимумах' },
    { name: 'Volatility', value: 41, desc: 'VIX-подобный индекс' },
    { name: 'Put/Call',   value: 73, desc: 'Опционное соотношение' },
  ];

  return h(PageShell, null,
    h(PageHero, {
      icon: 'fear',
      title: 'Индекс страха',
      subtitle: 'Композитный индикатор настроений — 5 компонент',
      right: h(PeriodGroup, { value: period, onChange: setPeriod }),
    }),

    // Big number + history side-by-side
    h('div', { style: { display: 'grid', gridTemplateColumns: '340px 1fr', gap: 20, marginBottom: 20 } },
      h(MetricHero, { value: val, status: label, statusColor: color, caption: '0 (жадность) → 100 (страх)' }),
      h(ChartBox, { title: 'История · ' + period, height: 260 },
        h('div', { style: { height: '100%', display: 'flex', flexDirection: 'column' } },
          h('div', { style: { flex: 1 } }, h(Spark, { data: history.map(x => ({ value: x.fear_index })), color, height: 220, fill: true })),
          h('div', { style: { marginTop: 12, paddingTop: 12, borderTop: '1px solid var(--border)' } },
            h(ScaleBar, { value: val }),
          ),
        ),
      ),
    ),

    // Components grid
    h('div', { style: { background: 'var(--bg-widget)', border: '1px solid var(--border)', borderRadius: 16, padding: 24, boxShadow: 'var(--shadow-md)' } },
      h('div', { style: { fontSize: 16, fontWeight: 600, marginBottom: 16 } }, 'Компоненты индекса'),
      h('div', { style: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16 } },
        components.map((c, i) => {
          const col = c.value >= 55 ? '#F97316' : c.value >= 45 ? '#EAB308' : '#22C55E';
          return h('div', { key: i, style: { padding: 16, background: 'var(--bg-secondary)', borderRadius: 12, border: '1px solid var(--border)' } },
            h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 6 } },
              h('span', { style: { fontSize: 13, color: 'var(--fg-secondary)' } }, c.name),
              h('span', { style: { fontSize: 24, fontWeight: 700, color: col, fontVariantNumeric: 'tabular-nums' } }, c.value),
            ),
            h('div', { style: { height: 6, background: 'var(--bg-tertiary)', borderRadius: 999, overflow: 'hidden', margin: '8px 0' } },
              h('div', { style: { width: c.value + '%', height: '100%', background: col } })
            ),
            h('div', { style: { fontSize: 11, color: 'var(--fg-muted)' } }, c.desc),
          );
        }),
      ),
    ),
  );
};
