/* global React */
const { useState, useMemo } = React;
const h = React.createElement;

// ── Icon library — same visual family as production (lucide-ish, 1.75 stroke)
const IconSvg = ({ path, size = 20, stroke = 'currentColor', fill = 'none', sw = 1.75 }) =>
  h('svg', { width: size, height: size, viewBox: '0 0 24 24', fill, stroke, strokeWidth: sw, strokeLinecap: 'round', strokeLinejoin: 'round' }, path);

window.Icon = {
  Fear:       (p) => h(IconSvg, p, h('path', { d: 'M12 2v4M12 18v4M2 12h4M18 12h4M4.9 4.9l2.8 2.8M16.3 16.3l2.8 2.8M4.9 19.1l2.8-2.8M16.3 7.7l2.8-2.8' })),
  Map:        (p) => h(IconSvg, p, h('rect', { x: 3, y: 3, width: 7, height: 7, rx: 1 }), h('rect', { x: 14, y: 3, width: 7, height: 7, rx: 1 }), h('rect', { x: 3, y: 14, width: 7, height: 7, rx: 1 }), h('rect', { x: 14, y: 14, width: 7, height: 7, rx: 1 })),
  Strength:   (p) => h(IconSvg, p, h('path', { d: 'M3 17l6-6 4 4 8-8' }), h('path', { d: 'M14 7h7v7' })),
  OI:         (p) => h(IconSvg, p, h('path', { d: 'M3 3v18h18' }), h('path', { d: 'M7 14l4-4 3 3 5-6' })),
  Funds:      (p) => h(IconSvg, p, h('path', { d: 'M4 7h16M4 12h16M4 17h16' }), h('circle', { cx: 8, cy: 7, r: 1.6, fill: 'currentColor', stroke: 'none' }), h('circle', { cx: 14, cy: 12, r: 1.6, fill: 'currentColor', stroke: 'none' }), h('circle', { cx: 10, cy: 17, r: 1.6, fill: 'currentColor', stroke: 'none' })),
  Buffett:    (p) => h(IconSvg, p, h('path', { d: 'M3 12h18' }), h('path', { d: 'M12 3v18' }), h('circle', { cx: 12, cy: 12, r: 8 })),
  Arrow:      (p) => h(IconSvg, p, h('path', { d: 'M5 12h14M13 5l7 7-7 7' })),
  Info:       (p) => h(IconSvg, p, h('circle', { cx: 12, cy: 12, r: 10 }), h('path', { d: 'M12 8v.01M11 12h1v4h1' })),
  Search:     (p) => h(IconSvg, p, h('circle', { cx: 11, cy: 11, r: 7 }), h('path', { d: 'M20 20l-3.5-3.5' })),
  Moon:       (p) => h(IconSvg, p, h('path', { d: 'M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z' })),
  Dot:        () => h('span', { style: { width: 8, height: 8, borderRadius: '50%', background: '#10b981', display: 'inline-block' } }),
};

// ── IconTile — gradient rounded square used in page heroes
const GRADIENTS = {
  fear:     'linear-gradient(135deg,#F97316,#EF4444)',
  map:      'linear-gradient(135deg,#22C55E,#14B8A6)',
  strength: 'linear-gradient(135deg,#8B5CF6,#6366F1)',
  oi:       'linear-gradient(135deg,#3B82F6,#06B6D4)',
  funds:    'linear-gradient(135deg,#EC4899,#F43F5E)',
  buffett:  'linear-gradient(135deg,#F59E0B,#F97316)',
};
window.IconTile = function IconTile({ kind = 'map', size = 48 }) {
  const I = { fear: Icon.Fear, map: Icon.Map, strength: Icon.Strength, oi: Icon.OI, funds: Icon.Funds, buffett: Icon.Buffett }[kind];
  return h('div', {
    style: {
      width: size, height: size, borderRadius: 12, display: 'flex',
      alignItems: 'center', justifyContent: 'center', color: '#fff',
      background: GRADIENTS[kind], flexShrink: 0,
    },
  }, h(I, { size: size * 0.5 }));
};

// ── Widget — the card primitive
window.Widget = function Widget({ title, subtitle, tone = 'default', onClick, footer = 'Открыть', children, big }) {
  const [hover, setHover] = useState(false);
  const toneColor = {
    default: 'var(--accent)',
    positive: '#22c55e',
    negative: '#ef4444',
    warning: '#F97316',
    neutral: 'var(--fg-secondary)',
  }[tone];
  return h('button', {
    onClick,
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    style: {
      all: 'unset', cursor: 'pointer', textAlign: 'left',
      background: hover ? 'var(--bg-widget-hover)' : 'var(--bg-widget)',
      border: `1px solid ${hover ? 'rgba(0,230,118,0.3)' : 'var(--border)'}`,
      borderRadius: 16, padding: '22px 24px', boxShadow: 'var(--shadow-md)',
      transition: 'all .3s', display: 'flex', flexDirection: 'column',
      minHeight: big ? 260 : 180,
    },
  },
    h('div', { style: { display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 12 } },
      h('div', null,
        h('div', { style: { fontSize: 18, fontWeight: 600, color: 'var(--fg-primary)' } }, title),
        subtitle && h('div', { style: { fontSize: 12, color: 'var(--fg-muted)', marginTop: 2 } }, subtitle),
      ),
      h('span', { style: { color: hover ? 'var(--accent)' : 'var(--fg-secondary)', transition: 'all .3s', transform: hover ? 'translateX(2px)' : 'none' } }, h(Icon.Arrow, { size: 18 })),
    ),
    h('div', { style: { flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: toneColor } }, children),
    footer && h('div', { style: { borderTop: '1px solid var(--border)', marginTop: 14, paddingTop: 10, fontSize: 12, color: hover ? 'var(--accent)' : 'var(--fg-secondary)', textAlign: 'center', transition: 'color .3s' } }, footer),
  );
};

// ── MetricHero — big number + status pill (used at top of detail pages)
window.MetricHero = function MetricHero({ value, unit, status, statusColor = '#22c55e', caption }) {
  return h('div', { style: { background: 'var(--bg-widget)', border: '1px solid var(--border)', borderRadius: 16, padding: 32, display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center', boxShadow: 'var(--shadow-md)' } },
    h('div', { style: { display: 'flex', alignItems: 'baseline', gap: 6 } },
      h('span', { style: { fontSize: 96, fontWeight: 700, lineHeight: 1, letterSpacing: '-.02em', fontVariantNumeric: 'tabular-nums', color: statusColor } }, value),
      unit && h('span', { style: { fontSize: 32, fontWeight: 600, color: 'var(--fg-secondary)' } }, unit),
    ),
    h('div', { style: { marginTop: 14, fontSize: 13, fontWeight: 500, padding: '6px 14px', borderRadius: 999, color: statusColor, background: hexA(statusColor, 0.13), border: `1px solid ${hexA(statusColor, 0.3)}` } }, status),
    caption && h('div', { style: { fontSize: 12, color: 'var(--fg-muted)', marginTop: 10 } }, caption),
  );
};

// ── PageHero — shared tile+title+sub row on detail pages
window.PageHero = function PageHero({ icon, title, subtitle, right }) {
  return h('div', { style: { display: 'flex', alignItems: 'center', gap: 14, padding: '0 0 20px 0' } },
    h(IconTile, { kind: icon, size: 48 }),
    h('div', { style: { flex: 1 } },
      h('div', { style: { fontSize: 22, fontWeight: 700, lineHeight: 1.2 } }, title),
      subtitle && h('div', { style: { fontSize: 13, color: 'var(--fg-secondary)', marginTop: 2 } }, subtitle),
    ),
    right,
  );
};

// ── ScaleBar — 5-segment bar with marker
window.ScaleBar = function ScaleBar({ value, labels = ['Жадность', 'Страх'] }) {
  return h('div', null,
    h('div', { style: { height: 12, borderRadius: 999, overflow: 'hidden', display: 'flex', position: 'relative' } },
      ['#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444'].map((c, i) =>
        h('div', { key: i, style: { flex: 1, background: c } })
      ),
      h('div', { style: { position: 'absolute', top: -5, left: `${value}%`, transform: 'translateX(-50%)', width: 3, height: 22, background: '#fff', borderRadius: 2, boxShadow: '0 0 0 2px var(--bg-primary)' } }),
    ),
    h('div', { style: { display: 'flex', justifyContent: 'space-between', fontSize: 12, color: 'var(--fg-secondary)', marginTop: 6 } },
      h('span', null, labels[0]), h('span', null, labels[1]),
    ),
  );
};

// ── PeriodGroup — 1М / 3М / 6М / 1Г / Всё
window.PeriodGroup = function PeriodGroup({ value, onChange, options = ['1М', '3М', '6М', '1Г', 'Всё'] }) {
  return h('div', { style: { display: 'inline-flex', background: 'var(--bg-primary)', border: '1px solid var(--border)', borderRadius: 12, padding: 3, gap: 2 } },
    options.map((opt) =>
      h('button', {
        key: opt, onClick: () => onChange(opt),
        style: {
          all: 'unset', cursor: 'pointer', padding: '6px 14px', fontSize: 13, fontWeight: 500,
          borderRadius: 9, transition: 'all .2s',
          color: value === opt ? 'var(--fg-inverse)' : 'var(--fg-secondary)',
          background: value === opt ? 'var(--accent)' : 'transparent',
        },
      }, opt)
    ),
  );
};

// ── ChartBox — widget wrapper for charts, with title row
window.ChartBox = function ChartBox({ title, right, height = 300, children }) {
  return h('div', { style: { background: 'var(--bg-widget)', border: '1px solid var(--border)', borderRadius: 16, padding: 24, boxShadow: 'var(--shadow-md)' } },
    h('div', { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 } },
      h('div', { style: { fontSize: 16, fontWeight: 600 } }, title),
      right,
    ),
    h('div', { style: { height } }, children),
  );
};

// ── Nav — sticky top bar
window.Nav = function Nav({ route, setRoute }) {
  const items = [
    { id: 'overview',   label: 'Обзор' },
    { id: 'fear',       label: 'Индекс страха' },
    { id: 'heatmap',    label: 'Карта рынка' },
    { id: 'strength',   label: 'Сила рынка' },
    { id: 'buffett',    label: 'Баффет' },
    { id: 'oi',         label: 'Открытый интерес' },
    { id: 'funds',      label: 'Фонды' },
  ];
  return h('nav', { style: { position: 'sticky', top: 0, zIndex: 50, backdropFilter: 'blur(20px)', WebkitBackdropFilter: 'blur(20px)', background: 'var(--glass-bg)', borderBottom: '1px solid var(--border)', padding: '0 24px', height: 64, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 24 } },
    h('div', { style: { display: 'flex', alignItems: 'center', gap: 10 } },
      h('button', { onClick: () => setRoute('overview'), style: { all: 'unset', cursor: 'pointer', fontSize: 18, fontWeight: 700, color: 'var(--accent)', letterSpacing: '-.01em' } }, 'Фрейм'),
      h('span', { style: { fontSize: 11, color: 'var(--fg-secondary)', background: 'var(--border)', padding: '2px 8px', borderRadius: 999 } }, 'Beta'),
      h(Icon.Dot, null),
    ),
    h('div', { style: { display: 'flex', gap: 4, flex: 1, justifyContent: 'center', flexWrap: 'wrap' } },
      items.map(({ id, label }) =>
        h('button', {
          key: id, onClick: () => setRoute(id),
          style: {
            all: 'unset', cursor: 'pointer', padding: '8px 14px', borderRadius: 12, fontSize: 13, fontWeight: 500,
            color: route === id ? 'var(--accent)' : 'var(--fg-secondary)',
            background: route === id ? 'rgba(0,230,118,0.1)' : 'transparent',
            border: `1px solid ${route === id ? 'rgba(0,230,118,0.3)' : 'transparent'}`,
            transition: 'all .2s',
          },
        }, label)
      ),
    ),
    h('div', { style: { display: 'flex', alignItems: 'center', gap: 10 } },
      h('button', { style: { all: 'unset', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6, padding: '6px 12px', borderRadius: 8, color: 'var(--fg-secondary)', fontSize: 12 } }, h(Icon.Moon, { size: 14 }), 'OKX Green'),
      h('button', { style: { all: 'unset', cursor: 'pointer', padding: '8px 16px', borderRadius: 12, fontSize: 13, fontWeight: 500, color: 'var(--fg-secondary)', border: '1px solid var(--border)' } }, 'Войти'),
    ),
  );
};

// ── Utility — hex + alpha
function hexA(hex, a) {
  if (hex.startsWith('rgb')) return hex;
  const v = hex.replace('#', '');
  const r = parseInt(v.slice(0, 2), 16), g = parseInt(v.slice(2, 4), 16), b = parseInt(v.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}
window.hexA = hexA;

// ── PageShell — consistent padding for all pages
window.PageShell = function PageShell({ children }) {
  return h('div', { style: { maxWidth: 1280, margin: '0 auto', padding: '32px 24px 64px' } }, children);
};

Object.assign(window, { Icon, IconTile: window.IconTile, Widget: window.Widget, MetricHero: window.MetricHero, PageHero: window.PageHero, ScaleBar: window.ScaleBar, PeriodGroup: window.PeriodGroup, ChartBox: window.ChartBox, Nav: window.Nav, PageShell: window.PageShell, hexA });
