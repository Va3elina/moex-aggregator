/**
 * PositionComet — «след кометы» для строки скринера сигналов ОИ.
 *
 * Ось перекоса ФИКСИРОВАНА −100…+100 (ноль по центру, слева шорт, справа
 * лонг) — строки сравнимы глазами. Голова кометы = позиция сегодня, хвост
 * тянется оттуда, откуда пришли (вчера). Читается сразу:
 *   • ГДЕ стоит толпа (голова у края = «полный лонг/шорт»);
 *   • НА СКОЛЬКО и КУДА двинулись за день (длина и направление хвоста).
 *
 * Проблема «крошечный сдвиг теряется» решена так же, как в дизайн-макете:
 * длина хвоста по СЖАТОЙ √-шкале с минимумом (микро-сдвиг всё равно виден),
 * размер головы растёт с силой сигнала ×N.
 *
 * Цвет = нога (зелёный лонг / красный шорт), в тон числу перекоса в строке;
 * приглушён для не-sharp рядов (контекст, не сигнал).
 */
import type { CSSProperties } from 'react';

interface Props {
  netPct: number | null;         // перекос сегодня, −100…+100
  netPctPrev: number | null;     // перекос вчера (для хвоста)
  ratio: number | null;          // сила сигнала ×N (размер головы)
  status: 'sharp' | 'normal' | 'illiquid' | 'nodata';
}

// ── геометрия (viewBox 210×30, ось y=15) ──
const ZERO_X = 78;
const PER_PCT = 0.72;            // (150−78)/100 — правый край +100% на x=150
const AX_MIN = 6, AX_MAX = 150;
const Y = 15;

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));
const xOf = (p: number) => ZERO_X + clamp(p, -100, 100) * PER_PCT;

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

export default function PositionComet({ netPct, netPctPrev, ratio, status }: Props) {
  if (netPct == null) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const legColor = netPct >= 0 ? 'var(--oi-green)' : 'var(--oi-red)';
  const dim = status !== 'sharp';
  const opacity = dim ? 0.5 : 1;

  const todayX = xOf(netPct);
  // Голова: радиус растёт с ×N (3.5…6). Без ratio — минимум.
  const headR = ratio != null ? clamp(3.5 + (ratio - 2) * 0.55, 3.5, 6) : 3.5;

  // Хвост: длина по √-шкале с минимумом 7px — крупные и микро-сдвиги читаются.
  let tail: { x: number; poly: string } | null = null;
  if (netPctPrev != null) {
    const yestX = xOf(netPctPrev);
    const dpp = Math.abs(netPct - netPctPrev);
    const mv = Math.sign(todayX - yestX) || 1;         // направление движения
    const tailLen = clamp(6 + 4 * Math.sqrt(dpp), 7, 40);
    const tailX = clamp(todayX - mv * tailLen, AX_MIN, AX_MAX);
    // Комета: широкая (±headR) у головы, сходит на нет (±1) у хвоста.
    tail = {
      x: tailX,
      poly: `${todayX},${Y - headR} ${tailX},${Y - 1} ${tailX},${Y + 1} ${todayX},${Y + headR}`,
    };
  }

  const pctStr = (netPct >= 0 ? '+' : '−') + Math.abs(netPct).toFixed(1).replace('.', ',') + '%';

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
      <svg viewBox="0 0 210 30" style={{ width: '100%', maxWidth: 210, flexShrink: 1 }} aria-hidden="true">
        {/* зоны: слева шорт, справа лонг (лёгкий тон для контекста ноги) */}
        <rect x={AX_MIN} y={Y - 6} width={ZERO_X - AX_MIN} height={12} rx={3} fill="var(--oi-red)" opacity={0.08} />
        <rect x={ZERO_X} y={Y - 6} width={AX_MAX - ZERO_X} height={12} rx={3} fill="var(--oi-green)" opacity={0.10} />
        {/* ось */}
        <line x1={AX_MIN} y1={Y} x2={AX_MAX} y2={Y} stroke="var(--text-muted)" strokeWidth={1} opacity={0.4} />
        {/* ноль */}
        <line x1={ZERO_X} y1={Y - 8} x2={ZERO_X} y2={Y + 8} stroke="var(--text-muted)" strokeWidth={1.2} strokeDasharray="2.5 2.5" opacity={0.7} />
        {/* хвост кометы */}
        {tail && <polygon points={tail.poly} fill={legColor} opacity={opacity * 0.55} />}
        {/* голова кометы */}
        <circle cx={todayX} cy={Y} r={headR} fill={legColor} opacity={opacity} />
      </svg>
      <span style={{ ...MONO, fontSize: 'var(--fs-sm)', fontWeight: 700, color: legColor, opacity, whiteSpace: 'nowrap' }}>
        {pctStr}
      </span>
    </div>
  );
}
