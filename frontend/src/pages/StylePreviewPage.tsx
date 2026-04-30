/**
 * StylePreviewPage — песочница для нового editorial-дизайна.
 *
 * Воспроизводит структуру `/oi` (Открытый интерес) в editorial / brutalist
 * стиле, портированном с design_handoff_frame_redesign/page.jsx.
 *
 * Управление:
 *   - Theme: Light (тёплая бумага) / Dark (тёплый чёрный)
 *   - Accent: Pumpkin (#FF5C2B) / Indigo (#5B5BD6)
 *
 * Standalone — без `<Layout>`, доступно по `/style-preview`.
 * Это МОК для оценки дизайна; не подключено к реальному API.
 *
 * Когда дизайн утвердим:
 *   1. Поднять токены в index.css как `[data-theme="editorial-light"]` и
 *      `[data-theme="editorial-dark"]`, добавить `data-accent` атрибут.
 *   2. Перенести структуру в `OpenInterestPage.tsx`, заменив текущие
 *      glass-cards на editorial-cards.
 *   3. Аналогично для остальных 6 страниц по образцу.
 */
import { useMemo, useState } from 'react';

// ============================================================================
//  Theme tokens (port из page.jsx → OI_THEMES)
// ============================================================================

type ThemeName = 'light' | 'dark';
type AccentName = 'pumpkin' | 'indigo';

interface EditorialTheme {
  bg: string;
  panel: string;
  panelInner: string;
  text: string;
  textMuted: string;
  textSubtle: string;
  border: string;
  grid: string;
  chip: string;
  chipText: string;
  chipActiveText: string;
  headerBg: string;
  shadow: string;
  lineA: string;
  lineB: string;
  brushFill: string;
  positiveText: string;
  negativeText: string;
}

const THEMES: Record<ThemeName, EditorialTheme> = {
  light: {
    bg: '#F4F1EA',
    panel: '#FFFFFF',
    panelInner: '#F7F4ED',
    text: '#0A0A0A',
    textMuted: '#6B6B6B',
    textSubtle: '#9A9A9A',
    border: '#0A0A0A',
    grid: 'rgba(10,10,10,0.12)',
    chip: '#FFFFFF',
    chipText: '#0A0A0A',
    chipActiveText: '#FFFFFF',
    headerBg: '#FFFFFF',
    shadow: '#0A0A0A',
    lineA: '#2B6CB0',
    lineB: '#E07A1F',
    brushFill: '#0A0A0A22',
    positiveText: '#0A6B3B',
    negativeText: '#B5301A',
  },
  dark: {
    bg: '#0E0E10',
    panel: '#17171A',
    panelInner: '#101012',
    text: '#F5F1E8',
    textMuted: '#9A9A9A',
    textSubtle: '#6B6B6B',
    border: '#F5F1E8',
    grid: 'rgba(245,241,232,0.10)',
    chip: '#17171A',
    chipText: '#F5F1E8',
    chipActiveText: '#0A0A0A',
    headerBg: '#0E0E10',
    shadow: '#000000',
    lineA: '#5DA3E9',
    lineB: '#F2A24A',
    brushFill: '#F5F1E81F',
    positiveText: '#5BD49C',
    negativeText: '#FF7A5C',
  },
};

const ACCENTS: Record<AccentName, string> = {
  pumpkin: '#FF5C2B',
  indigo: '#5B5BD6',
};

// ============================================================================
//  FrameLogo — port из logo.jsx
// ============================================================================

function FrameLogo({ size = 26, color }: { size?: number; color: string }) {
  return (
    <div style={{ display: 'inline-flex', alignItems: 'center', gap: size * 0.32 }}>
      <svg width={size} height={size} viewBox="0 0 32 32" fill="none" style={{ flexShrink: 0 }}>
        <path d="M3 3 H13 V8 H8 V13 H3 Z" fill={color} />
        <path d="M29 3 H19 V8 H24 V13 H29 Z" fill={color} />
        <path d="M3 29 H13 V24 H8 V19 H3 Z" fill={color} />
        <path d="M29 29 H19 V24 H24 V19 H29 Z" fill={color} />
      </svg>
      <span
        style={{
          fontFamily: "'Archivo', 'Inter', system-ui, sans-serif",
          fontWeight: 800,
          fontSize: size * 0.95,
          letterSpacing: '-0.02em',
          color,
          lineHeight: 1,
        }}
      >
        FRAME
      </span>
    </div>
  );
}

// ============================================================================
//  Chip — pill button с outline + опциональной hard-shadow
// ============================================================================

interface ChipProps {
  children: React.ReactNode;
  active?: boolean;
  accent: string;
  theme: EditorialTheme;
  withShadow?: boolean;
  sub?: boolean;
  onClick?: () => void;
}

function Chip({ children, active, accent, theme, withShadow, sub, onClick }: ChipProps) {
  const bg = active ? accent : theme.chip;
  const fg = active ? theme.chipActiveText : theme.chipText;
  return (
    <button
      onClick={onClick}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 8,
        padding: sub ? '8px 14px' : '9px 16px',
        background: bg,
        color: fg,
        border: `1.5px solid ${theme.border}`,
        borderRadius: 999,
        fontFamily: "'Inter', sans-serif",
        fontSize: 13,
        fontWeight: 600,
        letterSpacing: '-0.005em',
        cursor: 'pointer',
        boxShadow: withShadow ? `3px 3px 0 ${theme.shadow}` : 'none',
        whiteSpace: 'nowrap',
        lineHeight: 1,
        transition: 'transform 120ms ease-out, box-shadow 120ms ease-out',
      }}
    >
      {children}
    </button>
  );
}

// ============================================================================
//  NavLink — горизонтальная навигация в header
// ============================================================================

function NavLinkBtn({
  children,
  active,
  theme,
  accent,
}: {
  children: React.ReactNode;
  active?: boolean;
  theme: EditorialTheme;
  accent: string;
}) {
  return (
    <a
      style={{
        padding: '8px 14px',
        fontFamily: "'Inter', sans-serif",
        fontSize: 14,
        fontWeight: active ? 700 : 500,
        color: active ? theme.text : theme.textMuted,
        textDecoration: 'none',
        borderRadius: 8,
        position: 'relative',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
      }}
    >
      {children}
      {active && (
        <span
          style={{
            position: 'absolute',
            left: 14,
            right: 14,
            bottom: 2,
            height: 3,
            background: accent,
            borderRadius: 2,
          }}
        />
      )}
    </a>
  );
}

// ============================================================================
//  Editorial chart — port из chart.jsx (детерминированный squiggle)
// ============================================================================

function generateSeries(n: number, seed: number, base: number, amp: number, drift: number): number[] {
  let s = seed;
  const rand = () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return (s & 0xfffffff) / 0xfffffff;
  };
  const arr: number[] = [];
  let v = base;
  for (let i = 0; i < n; i++) {
    const t = i / (n - 1);
    const trend = drift * t;
    const noise = (rand() - 0.5) * amp * 0.4;
    v = v * 0.86 + (base + trend + Math.sin(t * Math.PI * 3 + seed) * amp * 0.6 + noise) * 0.14;
    arr.push(v);
  }
  return arr;
}

function pathFromSeries(series: number[], w: number, h: number, padTop = 8, padBottom = 8) {
  const min = Math.min(...series);
  const max = Math.max(...series);
  const range = max - min || 1;
  const usableH = h - padTop - padBottom;
  const stepX = w / (series.length - 1);
  let d = '';
  series.forEach((v, i) => {
    const x = i * stepX;
    const y = padTop + (1 - (v - min) / range) * usableH;
    d += (i === 0 ? 'M' : 'L') + x.toFixed(2) + ' ' + y.toFixed(2) + ' ';
  });
  return { d, min, max };
}

function fmtNum(v: number) {
  return Math.round(v).toLocaleString('ru-RU').replace(/,/g, ' ');
}

function EditorialChart({ theme, accent }: { theme: EditorialTheme; accent: string }) {
  const N = 180;
  const seriesA = useMemo(() => generateSeries(N, 7777, 31000, 1800, 1200), []);
  const seriesB = useMemo(() => generateSeries(N, 1234, 200000, 90000, 90000), []);

  const W = 880;
  const H = 360;
  const padL = 60;
  const padR = 70;
  const padT = 30;
  const padB = 40;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const a = pathFromSeries(seriesA, innerW, innerH);
  const b = pathFromSeries(seriesB, innerW, innerH);

  const ticksA = [0, 0.25, 0.5, 0.75, 1].map((t) => a.min + (a.max - a.min) * (1 - t));
  const ticksB = [0, 0.25, 0.5, 0.75, 1].map((t) => b.min + (b.max - b.min) * (1 - t));

  const xLabels = [
    '31 окт. 25 г.',
    '28 нояб. 25 г.',
    '26 дек. 25 г.',
    '03 февр. 26 г.',
    '03 мар. 26 г.',
    '01 апр. 26 г.',
    '29 апр. 26 г.',
  ];

  return (
    <div style={{ width: '100%' }}>
      {/* Legend */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          gap: 28,
          fontSize: 13,
          color: theme.text,
          fontWeight: 600,
          marginBottom: 8,
          fontFamily: "'Inter', sans-serif",
        }}
      >
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 10, height: 10, borderRadius: '50%', background: theme.lineA }} />
          Сбербанк
        </span>
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
          <span style={{ width: 10, height: 10, borderRadius: '50%', background: theme.lineB }} />
          Открытый интерес
        </span>
      </div>

      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
        {ticksA.map((_, i) => (
          <line
            key={i}
            x1={padL}
            x2={W - padR}
            y1={padT + (innerH / 4) * i}
            y2={padT + (innerH / 4) * i}
            stroke={theme.grid}
            strokeWidth="1"
            strokeDasharray="2 4"
          />
        ))}

        {ticksA.map((v, i) => (
          <text
            key={'yl' + i}
            x={padL - 10}
            y={padT + (innerH / 4) * i + 4}
            fill={theme.textMuted}
            fontSize="11"
            textAnchor="end"
            fontFamily="'JetBrains Mono', 'IBM Plex Mono', monospace"
            fontWeight="500"
          >
            {fmtNum(v)}
          </text>
        ))}

        {ticksB.map((v, i) => (
          <text
            key={'yr' + i}
            x={W - padR + 10}
            y={padT + (innerH / 4) * i + 4}
            fill={theme.textMuted}
            fontSize="11"
            textAnchor="start"
            fontFamily="'JetBrains Mono', 'IBM Plex Mono', monospace"
            fontWeight="500"
          >
            {fmtNum(v)}
          </text>
        ))}

        <g transform={`translate(${padL} ${padT})`}>
          <path d={a.d} stroke={theme.lineA} strokeWidth="2.25" fill="none" strokeLinejoin="round" strokeLinecap="round" />
          <path d={b.d} stroke={theme.lineB} strokeWidth="2.25" fill="none" strokeLinejoin="round" strokeLinecap="round" />
        </g>

        <g transform={`translate(${padL + 8} ${H - padB - 28})`} opacity="0.7">
          <rect width="14" height="14" rx="2" fill="none" stroke={theme.textMuted} strokeWidth="1.5" />
          <text x="20" y="11" fill={theme.textMuted} fontSize="11" fontFamily="'Archivo', sans-serif" fontWeight="700">
            FRAME
          </text>
        </g>

        {xLabels.map((lbl, i) => {
          const x = padL + (innerW / (xLabels.length - 1)) * i;
          return (
            <text
              key={'x' + i}
              x={x}
              y={H - padB + 18}
              fill={theme.textMuted}
              fontSize="11"
              textAnchor="middle"
              fontFamily="'Inter', sans-serif"
            >
              {lbl}
            </text>
          );
        })}
      </svg>

      <div
        style={{
          marginTop: 8,
          height: 44,
          background: theme.panelInner,
          border: `1.5px solid ${theme.border}`,
          borderRadius: 8,
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        <svg viewBox={`0 0 ${W} 44`} width="100%" height="44" preserveAspectRatio="none">
          <path d={pathFromSeries(seriesB, W, 44, 4, 4).d + ` L${W} 44 L0 44 Z`} fill={theme.brushFill} opacity="0.9" />
        </svg>
        <div
          style={{
            position: 'absolute',
            left: 0,
            top: 0,
            bottom: 0,
            width: 14,
            background: accent,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: theme.bg,
            fontWeight: 800,
            fontSize: 14,
            borderRight: `2px solid ${theme.border}`,
          }}
        >
          ‹
        </div>
        <div
          style={{
            position: 'absolute',
            right: 0,
            top: 0,
            bottom: 0,
            width: 14,
            background: accent,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: theme.bg,
            fontWeight: 800,
            fontSize: 14,
            borderLeft: `2px solid ${theme.border}`,
          }}
        >
          ›
        </div>
      </div>
    </div>
  );
}

// ============================================================================
//  Главная страница
// ============================================================================

export default function StylePreviewPage() {
  const [themeName, setThemeName] = useState<ThemeName>('light');
  const [accentName, setAccentName] = useState<AccentName>('pumpkin');
  const [period, setPeriod] = useState<string>('6М');
  const [interval, setIntervalVal] = useState<string>('1д');
  const [mode, setMode] = useState<string>('Позиции');
  const [oiVariant, setOiVariant] = useState<string>('Открытый интерес');
  const [clgroup, setClgroup] = useState<string>('Физлица');

  const theme = THEMES[themeName];
  const accent = ACCENTS[accentName];

  const navItems = [
    'Индекс страха',
    'Карта рынка',
    'Открытый интерес',
    'Деньги в фондах',
    'Состав фондов',
    'Сила рынка',
    'Сезонность',
  ];

  const periods = ['1Д', '1Н', '1М', '3М', '6М', '1Г', '2Г', '5Л', 'Всё'];
  const intervals = ['5м', '1ч', '1д'];
  const modes = ['Только цена', 'Позиции', 'Участники'];
  const oiVariants = ['Открытый интерес', 'Покупки', 'Продажи', 'Покупки + Продажи', 'Чистая позиция', 'Экспирации'];

  const kpis = [
    { k: 'Σ длинных', v: '273 269', d: '+4,2%', up: true as boolean | null },
    { k: 'Σ коротких', v: '221 093', d: '−1,8%', up: false as boolean | null },
    { k: 'Чистая позиция', v: '+52 176', d: 'Лонг', up: true as boolean | null },
    { k: 'Объём, ₽', v: '1,42 млрд', d: 'за день', up: null as boolean | null },
  ];

  return (
    <div
      style={{
        minHeight: '100vh',
        background: theme.bg,
        fontFamily: "'Inter', system-ui, sans-serif",
        color: theme.text,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      {/* ═══ Sandbox toggle (только для preview) ═══ */}
      <div
        style={{
          position: 'fixed',
          top: 12,
          right: 12,
          zIndex: 100,
          background: theme.panel,
          border: `1.5px solid ${theme.border}`,
          borderRadius: 12,
          padding: 12,
          boxShadow: `4px 4px 0 ${theme.shadow}`,
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
          fontSize: 11,
        }}
      >
        <div
          style={{
            fontWeight: 700,
            color: theme.textMuted,
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            fontFamily: "'Inter', sans-serif",
          }}
        >
          Sandbox
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <button onClick={() => setThemeName('light')} style={toggleBtnStyle(themeName === 'light', theme, accent)}>
            ◐ Light
          </button>
          <button onClick={() => setThemeName('dark')} style={toggleBtnStyle(themeName === 'dark', theme, accent)}>
            🌙 Dark
          </button>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <button
            onClick={() => setAccentName('pumpkin')}
            style={toggleBtnStyle(accentName === 'pumpkin', theme, ACCENTS.pumpkin)}
          >
            <span
              style={{
                width: 8,
                height: 8,
                background: ACCENTS.pumpkin,
                borderRadius: '50%',
                display: 'inline-block',
                marginRight: 6,
              }}
            />
            Pumpkin
          </button>
          <button
            onClick={() => setAccentName('indigo')}
            style={toggleBtnStyle(accentName === 'indigo', theme, ACCENTS.indigo)}
          >
            <span
              style={{
                width: 8,
                height: 8,
                background: ACCENTS.indigo,
                borderRadius: '50%',
                display: 'inline-block',
                marginRight: 6,
              }}
            />
            Indigo
          </button>
        </div>
      </div>

      {/* ═══ HEADER ═══ */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '18px 32px',
          background: theme.headerBg,
          borderBottom: `1.5px solid ${theme.border}`,
          gap: 16,
          flexWrap: 'wrap',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <FrameLogo size={26} color={accent} />
          <span
            style={{
              fontSize: 10,
              fontWeight: 700,
              letterSpacing: '0.08em',
              padding: '3px 8px',
              border: `1.5px solid ${theme.border}`,
              borderRadius: 4,
              color: theme.text,
              textTransform: 'uppercase',
            }}
          >
            BETA
          </span>
        </div>

        <nav style={{ display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap' }}>
          {navItems.map((label) => (
            <NavLinkBtn key={label} theme={theme} accent={accent} active={label === 'Открытый интерес'}>
              {label}
            </NavLinkBtn>
          ))}
        </nav>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 12, color: theme.textMuted, fontWeight: 500 }}>
            {accentName === 'pumpkin' ? 'Pumpkin' : 'Indigo'}
          </span>
          <div
            style={{
              width: 32,
              height: 32,
              borderRadius: '50%',
              background: theme.panel,
              border: `1.5px solid ${theme.border}`,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: 12,
              fontWeight: 700,
            }}
          >
            E
          </div>
          <button
            style={{
              padding: '8px 16px',
              background: accent,
              color: themeName === 'light' ? '#FFFFFF' : '#0A0A0A',
              border: `1.5px solid ${theme.border}`,
              borderRadius: 999,
              fontFamily: "'Inter', sans-serif",
              fontSize: 13,
              fontWeight: 700,
              cursor: 'pointer',
              boxShadow: `3px 3px 0 ${theme.shadow}`,
            }}
          >
            Plus
          </button>
        </div>
      </header>

      {/* ═══ TITLE ROW ═══ */}
      <div
        style={{
          padding: '28px 32px 16px',
          display: 'flex',
          alignItems: 'flex-end',
          justifyContent: 'space-between',
          gap: 16,
          flexWrap: 'wrap',
        }}
      >
        <div>
          <div
            style={{
              fontSize: 12,
              fontWeight: 600,
              letterSpacing: '0.12em',
              color: theme.textMuted,
              textTransform: 'uppercase',
              marginBottom: 6,
            }}
          >
            Аналитика → Деривативы
          </div>
          <h1
            style={{
              margin: 0,
              fontSize: 56,
              fontWeight: 800,
              letterSpacing: '-0.025em',
              fontFamily: "'Archivo', 'Inter', sans-serif",
              lineHeight: 1.0,
              color: theme.text,
            }}
          >
            Открытый интерес
          </h1>
        </div>
        <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
          <div style={{ textAlign: 'right' }}>
            <div
              style={{
                fontSize: 11,
                color: theme.textMuted,
                fontWeight: 600,
                letterSpacing: '0.05em',
                textTransform: 'uppercase',
              }}
            >
              Текущая цена
            </div>
            <div
              style={{
                fontSize: 24,
                fontWeight: 700,
                fontFamily: "'JetBrains Mono', 'IBM Plex Mono', monospace",
                letterSpacing: '-0.02em',
              }}
            >
              32 511 ₽
            </div>
          </div>
          <div
            style={{
              padding: '6px 10px',
              background: accent,
              color: themeName === 'light' ? '#FFFFFF' : '#0A0A0A',
              borderRadius: 6,
              fontSize: 13,
              fontWeight: 700,
              border: `1.5px solid ${theme.border}`,
              boxShadow: `2px 2px 0 ${theme.shadow}`,
            }}
          >
            +1,84%
          </div>
        </div>
      </div>

      {/* ═══ CONTROLS PANEL ═══ */}
      <div style={{ padding: '0 32px' }}>
        <div
          style={{
            background: theme.panel,
            border: `1.5px solid ${theme.border}`,
            borderRadius: 16,
            boxShadow: `5px 5px 0 ${theme.shadow}`,
            padding: 20,
          }}
        >
          {/* Row 1: ticker + entity + intervals */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center', marginBottom: 12 }}>
            <div
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 10,
                padding: '8px 14px 8px 8px',
                background: theme.chip,
                border: `1.5px solid ${theme.border}`,
                borderRadius: 999,
                fontWeight: 700,
                fontSize: 13,
                boxShadow: `3px 3px 0 ${theme.shadow}`,
                cursor: 'pointer',
                color: theme.chipText,
              }}
            >
              <span
                style={{
                  width: 26,
                  height: 26,
                  borderRadius: '50%',
                  background: '#1DB954',
                  color: '#fff',
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: 13,
                  fontWeight: 800,
                }}
              >
                S
              </span>
              <span style={{ display: 'flex', flexDirection: 'column', lineHeight: 1.1, gap: 2 }}>
                <span>Сбербанк</span>
                <span style={{ fontSize: 10, color: theme.textMuted, fontWeight: 600 }}>SR</span>
              </span>
              <span style={{ marginLeft: 4, fontSize: 14, color: theme.textMuted }}>▾</span>
            </div>

            <div style={{ width: 1, height: 28, background: theme.border, opacity: 0.3, margin: '0 4px' }} />

            <Chip theme={theme} accent={accent} active={clgroup === 'Физлица'} onClick={() => setClgroup('Физлица')}>
              Физлица
            </Chip>
            <Chip theme={theme} accent={accent} active={clgroup === 'Юрлица'} onClick={() => setClgroup('Юрлица')}>
              Юрлица
            </Chip>

            <div style={{ flex: 1 }} />

            {intervals.map((iv) => (
              <Chip key={iv} theme={theme} accent={accent} active={interval === iv} onClick={() => setIntervalVal(iv)}>
                {iv}
              </Chip>
            ))}
          </div>

          {/* Row 2: periods */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 14 }}>
            {periods.map((r) => (
              <Chip key={r} theme={theme} accent={accent} active={r === period} onClick={() => setPeriod(r)}>
                {r}
              </Chip>
            ))}
          </div>

          {/* Row 3: mode */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 14 }}>
            {modes.map((m) => (
              <Chip key={m} theme={theme} accent={accent} active={m === mode} onClick={() => setMode(m)}>
                {m}
              </Chip>
            ))}
          </div>

          {/* Row 4: sub-tabs */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 18 }}>
            {oiVariants.map((v) => (
              <Chip
                key={v}
                theme={theme}
                accent={accent}
                active={v === oiVariant}
                onClick={() => setOiVariant(v)}
                sub
                withShadow={v === oiVariant}
              >
                {v}
              </Chip>
            ))}
          </div>

          {/* Chart */}
          <div
            style={{
              background: theme.panelInner,
              border: `1.5px solid ${theme.border}`,
              borderRadius: 12,
              padding: '20px 16px 12px',
            }}
          >
            <EditorialChart theme={theme} accent={accent} />
          </div>
        </div>
      </div>

      {/* ═══ KPI FOOTER ═══ */}
      <div
        style={{
          padding: '20px 32px 32px',
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          gap: 14,
        }}
      >
        {kpis.map((kpi, i) => (
          <div
            key={i}
            style={{
              background: theme.panel,
              border: `1.5px solid ${theme.border}`,
              borderRadius: 12,
              padding: '14px 16px',
              boxShadow: `3px 3px 0 ${theme.shadow}`,
            }}
          >
            <div
              style={{
                fontSize: 11,
                color: theme.textMuted,
                fontWeight: 600,
                letterSpacing: '0.05em',
                textTransform: 'uppercase',
                marginBottom: 6,
              }}
            >
              {kpi.k}
            </div>
            <div
              style={{
                fontSize: 22,
                fontWeight: 800,
                fontFamily: "'JetBrains Mono', 'IBM Plex Mono', monospace",
                letterSpacing: '-0.02em',
                marginBottom: 4,
              }}
            >
              {kpi.v}
            </div>
            <div
              style={{
                fontSize: 12,
                fontWeight: 700,
                color: kpi.up === true ? theme.positiveText : kpi.up === false ? theme.negativeText : theme.textMuted,
              }}
            >
              {kpi.d}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Helper для toggle-кнопок sandbox-панели
function toggleBtnStyle(active: boolean, theme: EditorialTheme, accent: string): React.CSSProperties {
  return {
    padding: '6px 10px',
    fontSize: 11,
    fontFamily: "'Inter', sans-serif",
    fontWeight: 600,
    background: active ? accent : 'transparent',
    color: active ? (theme.bg === '#0E0E10' ? '#0A0A0A' : '#FFFFFF') : theme.text,
    border: `1.5px solid ${theme.border}`,
    borderRadius: 6,
    cursor: 'pointer',
    display: 'inline-flex',
    alignItems: 'center',
    whiteSpace: 'nowrap',
  };
}
