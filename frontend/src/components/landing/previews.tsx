/**
 * Static SVG-preview'ы для FeatureCarousel на Landing page.
 *
 * Каждый компонент — упрощённая иллюстрация индикатора. Не содержит
 * реальных данных (MVP: no API calls при рендере landing), но визуально
 * evoke'ит что покажет реальный индикатор.
 *
 * Все SVG имеют viewBox 400×250 и `preserveAspectRatio="xMidYMid meet"`
 * → адаптируются к контейнеру carousel'а (aspect 16:10).
 */
import type { ReactNode } from 'react';

// Общие константы — цвета из chartTheme
const C_GREEN = '#2EE59D';
const C_RED = '#FF4D4D';
const C_AMBER = '#F7931A';
const C_BLUE = '#4DA3FF';
const C_ACCENT = 'var(--accent)';
const C_MUTED = 'var(--text-muted)';
const C_GRID = 'rgba(255,255,255,0.06)';

/** Wrapper для SVG: размер, padding, reveal-анимация */
function SvgFrame({ children, label }: { children: ReactNode; label?: string }) {
  return (
    <svg viewBox="0 0 400 250" className="w-full h-full" preserveAspectRatio="xMidYMid meet">
      {label && (
        <text x="16" y="24" fontSize="10" fill={C_MUTED} letterSpacing="1.5" style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
          {label.toUpperCase()}
        </text>
      )}
      {children}
    </svg>
  );
}

/** 1. Карта рынка — treemap-like grid */
export function HeatmapPreview() {
  // Manually positioned tiles (simulating treemap layout)
  const tiles = [
    { x: 10, y: 40, w: 120, h: 90, c: C_GREEN, a: 0.8, label: 'SBER', pct: '+2.1%' },
    { x: 135, y: 40, w: 90, h: 90, c: C_GREEN, a: 0.5, label: 'GAZP', pct: '+0.8%' },
    { x: 230, y: 40, w: 75, h: 60, c: C_RED, a: 0.6, label: 'LKOH', pct: '−1.2%' },
    { x: 310, y: 40, w: 75, h: 60, c: C_RED, a: 0.4, label: 'NVTK', pct: '−0.4%' },
    { x: 230, y: 105, w: 155, h: 45, c: C_GREEN, a: 0.4, label: 'ROSN', pct: '+0.3%' },
    { x: 10, y: 135, w: 80, h: 60, c: C_RED, a: 0.7, label: 'YDEX', pct: '−1.8%' },
    { x: 95, y: 135, w: 70, h: 60, c: C_GREEN, a: 0.6, label: 'TCSG', pct: '+1.4%' },
    { x: 170, y: 135, w: 55, h: 60, c: C_GREEN, a: 0.3, label: 'PLZL', pct: '+0.2%' },
    { x: 230, y: 155, w: 100, h: 55, c: C_RED, a: 0.5, label: 'MTSS', pct: '−0.9%' },
    { x: 335, y: 155, w: 50, h: 55, c: C_GREEN, a: 0.4, label: 'MGNT', pct: '+0.5%' },
  ];
  return (
    <SvgFrame label="Карта рынка">
      {tiles.map((t, i) => (
        <g key={i}>
          <rect x={t.x} y={t.y} width={t.w} height={t.h} fill={t.c} opacity={t.a} rx="4" />
          {t.w > 60 && (
            <>
              <text x={t.x + t.w / 2} y={t.y + t.h / 2 - 2} fontSize="11" fill="white" textAnchor="middle" fontWeight="600"
                style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
                {t.label}
              </text>
              <text x={t.x + t.w / 2} y={t.y + t.h / 2 + 12} fontSize="9" fill="white" textAnchor="middle" opacity="0.8"
                style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
                {t.pct}
              </text>
            </>
          )}
        </g>
      ))}
    </SvgFrame>
  );
}

/** 3. Открытые позиции — price + OI bars */
export function OpenInterestPreview() {
  // Price line points
  const pricePts = [50, 55, 48, 52, 60, 65, 58, 62, 70, 68, 72, 78, 74, 80, 85];
  const barVals = [30, 35, 32, 40, 45, 48, 52, 50, 55, 58, 60, 65, 68, 70, 72];
  const W = 360, H = 120, xStep = W / (pricePts.length - 1);
  const maxP = Math.max(...pricePts), minP = Math.min(...pricePts);
  const pricePath = pricePts.map((p, i) => {
    const x = 20 + i * xStep;
    const y = 50 + (1 - (p - minP) / (maxP - minP)) * H;
    return (i === 0 ? 'M' : 'L') + x + ',' + y;
  }).join(' ');

  return (
    <SvgFrame label="Открытые позиции">
      {/* Grid */}
      {[0, 1, 2, 3].map(i => (
        <line key={i} x1="20" y1={50 + i * 30} x2={380} y2={50 + i * 30} stroke={C_GRID} strokeWidth="1" />
      ))}
      {/* Price area fill */}
      <path d={pricePath + ` L ${20 + (pricePts.length - 1) * xStep},170 L 20,170 Z`}
        fill="url(#oiGrad)" opacity="0.3" />
      <defs>
        <linearGradient id="oiGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={C_BLUE} stopOpacity="0.6" />
          <stop offset="100%" stopColor={C_BLUE} stopOpacity="0" />
        </linearGradient>
      </defs>
      {/* Price line */}
      <path d={pricePath} fill="none" stroke={C_BLUE} strokeWidth="2" strokeLinecap="round" />
      {/* OI bars bottom */}
      {barVals.map((v, i) => {
        const x = 20 + i * xStep - 4;
        const h = (v / 100) * 35;
        return <rect key={i} x={x} y={220 - h} width="8" height={h} fill={C_AMBER} opacity="0.7" rx="1" />;
      })}
    </SvgFrame>
  );
}

/** 4. Деньги в фондах — flow histogram */
export function FundsFlowPreview() {
  const flows = [3, 5, -2, 4, 8, -3, 6, 9, 2, -4, 7, 5];
  const W = 340, xStep = W / flows.length;
  return (
    <SvgFrame label="Деньги в фондах">
      {/* Zero line */}
      <line x1="20" y1="130" x2={380} y2="130" stroke={C_MUTED} strokeWidth="1" opacity="0.5" />
      {/* Bars from zero line */}
      {flows.map((v, i) => {
        const x = 30 + i * xStep;
        const h = Math.abs(v) * 8;
        const y = v >= 0 ? 130 - h : 130;
        const c = v >= 0 ? C_GREEN : C_RED;
        return <rect key={i} x={x} y={y} width={xStep - 6} height={h} fill={c} opacity="0.85" rx="2" />;
      })}
      {/* Labels */}
      <text x="20" y="80" fontSize="9" fill={C_MUTED} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>+80 млрд</text>
      <text x="20" y="190" fontSize="9" fill={C_MUTED} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>−40 млрд</text>
    </SvgFrame>
  );
}

/** 5. Состав фондов — donut chart */
export function FundsDonutPreview() {
  const segments = [
    { val: 35, c: C_GREEN, label: 'Акции' },
    { val: 28, c: C_BLUE, label: 'Облиг.' },
    { val: 20, c: C_AMBER, label: 'Золото' },
    { val: 10, c: '#9D4DFF', label: 'Ден. рынок' },
    { val: 7, c: '#FF6B9D', label: 'Другое' },
  ];
  const cx = 200, cy = 130, r = 65, ir = 40;
  let cumAngle = -90;
  return (
    <SvgFrame label="Состав фондов">
      {segments.map((s, i) => {
        const angle = (s.val / 100) * 360;
        const startRad = (cumAngle * Math.PI) / 180;
        const endRad = ((cumAngle + angle) * Math.PI) / 180;
        cumAngle += angle;
        const x1 = cx + r * Math.cos(startRad);
        const y1 = cy + r * Math.sin(startRad);
        const x2 = cx + r * Math.cos(endRad);
        const y2 = cy + r * Math.sin(endRad);
        const ix1 = cx + ir * Math.cos(endRad);
        const iy1 = cy + ir * Math.sin(endRad);
        const ix2 = cx + ir * Math.cos(startRad);
        const iy2 = cy + ir * Math.sin(startRad);
        const large = angle > 180 ? 1 : 0;
        const d = `M${x1},${y1} A${r},${r} 0 ${large} 1 ${x2},${y2} L${ix1},${iy1} A${ir},${ir} 0 ${large} 0 ${ix2},${iy2} Z`;
        return <path key={i} d={d} fill={s.c} opacity="0.9" />;
      })}
      {/* Center label */}
      <text x={cx} y={cy - 2} fontSize="18" fill="var(--text-primary)" textAnchor="middle" fontWeight="700"
        style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
        36
      </text>
      <text x={cx} y={cy + 14} fontSize="9" fill={C_MUTED} textAnchor="middle">
        фондов
      </text>
    </SvgFrame>
  );
}

/** 6. Сила рынка — line chart with reference lines */
export function StrengthPreview() {
  const pts = [70, 65, 60, 55, 52, 48, 45, 50, 55, 58, 62, 68, 72, 75, 73];
  const W = 360, xStep = W / (pts.length - 1);
  const path = pts.map((v, i) => {
    const x = 20 + i * xStep;
    const y = 50 + (1 - v / 100) * 150;
    return (i === 0 ? 'M' : 'L') + x + ',' + y;
  }).join(' ');
  return (
    <SvgFrame label="Сила рынка">
      {/* Reference lines */}
      <line x1="20" y1="80" x2="380" y2="80" stroke={C_RED} strokeDasharray="4 2" strokeWidth="1" opacity="0.5" />
      <line x1="20" y1="125" x2="380" y2="125" stroke={C_MUTED} strokeWidth="1" opacity="0.3" />
      <line x1="20" y1="170" x2="380" y2="170" stroke={C_GREEN} strokeDasharray="4 2" strokeWidth="1" opacity="0.5" />
      {/* Labels for reference */}
      <text x="384" y="83" fontSize="9" fill={C_RED} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>80%</text>
      <text x="384" y="128" fontSize="9" fill={C_MUTED} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>50%</text>
      <text x="384" y="173" fontSize="9" fill={C_GREEN} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>20%</text>
      {/* Main line */}
      <path d={path} fill="none" stroke={C_ACCENT} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      {/* Current point */}
      <circle cx={20 + (pts.length - 1) * xStep} cy={50 + (1 - pts[pts.length - 1] / 100) * 150}
        r="5" fill={C_ACCENT} stroke="var(--bg-primary)" strokeWidth="2" />
    </SvgFrame>
  );
}

/** 7. Индикатор Баффетта — line with danger-zone fill */
export function BuffettPreview() {
  const pts = [65, 70, 85, 95, 110, 120, 115, 105, 90, 78, 82, 95, 115, 108, 100];
  const W = 360, xStep = W / (pts.length - 1);
  const path = pts.map((v, i) => {
    const x = 20 + i * xStep;
    const y = 50 + (1 - (v - 50) / 100) * 150;
    return (i === 0 ? 'M' : 'L') + x + ',' + y;
  }).join(' ');
  // Danger zone (>100%): fill between 100 line and top
  const y100 = 50 + (1 - (100 - 50) / 100) * 150;
  return (
    <SvgFrame label="Индикатор Баффетта">
      {/* Danger zone bg */}
      <rect x="20" y="50" width={W} height={y100 - 50} fill={C_RED} opacity="0.08" />
      {/* Reference lines */}
      <line x1="20" y1={y100} x2="380" y2={y100} stroke={C_RED} strokeDasharray="4 2" strokeWidth="1" opacity="0.6" />
      <text x="384" y={y100 + 3} fontSize="9" fill={C_RED} style={{ fontFamily: "'IBM Plex Mono', monospace" }}>100%</text>
      {/* Main line */}
      <path d={path} fill="none" stroke={C_AMBER} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      {/* Current value label */}
      <g>
        <rect x="336" y="80" width="46" height="18" fill={C_AMBER} rx="3" />
        <text x="359" y="93" fontSize="11" fill="white" textAnchor="middle" fontWeight="700"
          style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
          100%
        </text>
      </g>
    </SvgFrame>
  );
}

/** 8. Сезонность — histogram (positive/negative bars) */
export function SeasonalityPreview() {
  const bars = [
    { m: 'Янв', v: 1.8 }, { m: 'Фев', v: -2.3 }, { m: 'Мар', v: 0.5 },
    { m: 'Апр', v: 2.1 }, { m: 'Май', v: -0.8 }, { m: 'Июн', v: 0.3 },
    { m: 'Июл', v: 1.2 }, { m: 'Авг', v: -1.5 }, { m: 'Сен', v: -0.6 },
    { m: 'Окт', v: 1.9 }, { m: 'Ноя', v: 2.4 }, { m: 'Дек', v: 1.1 },
  ];
  const W = 340, xStep = W / bars.length;
  const maxAbs = 3;
  return (
    <SvgFrame label="Сезонность">
      {/* Zero line */}
      <line x1="35" y1="130" x2={380} y2="130" stroke={C_MUTED} strokeWidth="1" opacity="0.4" />
      {/* Bars */}
      {bars.map((b, i) => {
        const x = 35 + i * xStep + 2;
        const h = (Math.abs(b.v) / maxAbs) * 60;
        const y = b.v >= 0 ? 130 - h : 130;
        const c = b.v >= 0 ? C_GREEN : C_RED;
        return (
          <g key={i}>
            <rect x={x} y={y} width={xStep - 4} height={h} fill={c} opacity="0.85" rx="2" />
            <text x={x + (xStep - 4) / 2} y={210} fontSize="9" fill={C_MUTED} textAnchor="middle"
              style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
              {b.m}
            </text>
          </g>
        );
      })}
      {/* Y labels */}
      <text x="30" y="75" fontSize="9" fill={C_MUTED} textAnchor="end" style={{ fontFamily: "'IBM Plex Mono', monospace" }}>+3%</text>
      <text x="30" y="133" fontSize="9" fill={C_MUTED} textAnchor="end" style={{ fontFamily: "'IBM Plex Mono', monospace" }}>0</text>
      <text x="30" y="195" fontSize="9" fill={C_MUTED} textAnchor="end" style={{ fontFamily: "'IBM Plex Mono', monospace" }}>−3%</text>
    </SvgFrame>
  );
}
