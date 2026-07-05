/**
 * PositionComet — «след кометы» для строки скринера сигналов ОИ.
 *
 * Ось перекоса ФИКСИРОВАНА −100…+100 (ноль по центру, слева шорт, справа
 * лонг) — строки сравнимы глазами. Голова кометы = позиция сегодня, хвост
 * тянется оттуда, откуда пришли (вчера). Читается сразу:
 *   • ГДЕ стоит толпа (голова у края = «полный лонг/шорт»);
 *   • НА СКОЛЬКО и КУДА двинулись за день (длина и направление хвоста).
 *
 * Число справа = ПЕРЕКОС: |net_pct|% + слово лонг/шорт (0 = поровну,
 * 100% = все в одну сторону). Слово убирает неоднозначность «что за %».
 *
 * Проблема «крошечный сдвиг теряется» решена как в дизайн-макете:
 * длина хвоста по СЖАТОЙ √-шкале с минимумом (микро-сдвиг всё равно виден),
 * размер головы растёт с силой сигнала ×N.
 *
 * Цвет = нога (зелёный лонг / красный шорт), в тон числу перекоса;
 * приглушён для не-sharp рядов (контекст, не сигнал).
 */
import type { CSSProperties } from 'react';

interface Props {
  netPct: number | null;         // перекос сегодня, −100…+100
  netPctPrev: number | null;     // перекос вчера (для хвоста)
  ratio: number | null;          // сила сигнала ×N (размер головы)
  status: 'sharp' | 'normal' | 'illiquid' | 'nodata';
}

// ── геометрия (viewBox 230×38, ось y=19) — крупнее для читаемости ──
const ZERO_X = 86;
const PER_PCT = 0.74;            // +100% на x=160, −100% на x=12
const AX_MIN = 12, AX_MAX = 160;
const Y = 19;

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));
const xOf = (p: number) => ZERO_X + clamp(p, -100, 100) * PER_PCT;

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

function fmtPct(p: number): string {
  return Math.abs(p).toFixed(1).replace('.', ',') + '%';
}

export default function PositionComet({ netPct, netPctPrev, ratio, status }: Props) {
  if (netPct == null) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const netLong = netPct >= 0;
  const legColor = netLong ? 'var(--oi-green)' : 'var(--oi-red)';
  const legWord = netLong ? 'лонг' : 'шорт';
  const dim = status !== 'sharp';
  const headOp = dim ? 0.6 : 1;
  const tailOp = dim ? 0.45 : 0.78;

  const todayX = xOf(netPct);
  // Голова: радиус растёт с ×N (4.5…8). Без ratio — минимум.
  const headR = ratio != null ? clamp(4.5 + (ratio - 2) * 0.65, 4.5, 8) : 4.5;

  // Хвост: длина по √-шкале с минимумом 8px — крупные и микро-сдвиги читаются.
  let tail: { poly: string } | null = null;
  if (netPctPrev != null) {
    const yestX = xOf(netPctPrev);
    const dpp = Math.abs(netPct - netPctPrev);
    const mv = Math.sign(todayX - yestX) || 1;         // направление движения
    const tailLen = clamp(7 + 4.2 * Math.sqrt(dpp), 8, 46);
    const tailX = clamp(todayX - mv * tailLen, AX_MIN, AX_MAX);
    // Комета: широкая (±headR) у головы, сходит на нет (±1.4) у хвоста.
    tail = {
      poly: `${todayX},${Y - headR} ${tailX},${Y - 1.4} ${tailX},${Y + 1.4} ${todayX},${Y + headR}`,
    };
  }

  const title = `Перекос: ${fmtPct(netPct)} в ${legWord} (0 = поровну, 100% = все в одну сторону).`
    + (netPctPrev != null ? ` Вчера ${fmtPct(netPctPrev)} → сегодня ${fmtPct(netPct)}; хвост кометы = сдвиг за день.` : '');

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }} title={title}>
      <svg viewBox="0 0 230 38" style={{ width: '100%', maxWidth: 240, flexShrink: 1 }} aria-hidden="true">
        {/* зоны: слева шорт, справа лонг (тон для контекста ноги) */}
        <rect x={AX_MIN} y={Y - 7} width={ZERO_X - AX_MIN} height={14} rx={3} fill="var(--oi-red)" opacity={0.14} />
        <rect x={ZERO_X} y={Y - 7} width={AX_MAX - ZERO_X} height={14} rx={3} fill="var(--oi-green)" opacity={0.16} />
        {/* ось */}
        <line x1={AX_MIN} y1={Y} x2={AX_MAX} y2={Y} stroke="var(--text-secondary)" strokeWidth={1.3} opacity={0.55} />
        {/* ноль */}
        <line x1={ZERO_X} y1={Y - 10} x2={ZERO_X} y2={Y + 10} stroke="var(--text-primary)" strokeWidth={1.4} strokeDasharray="2.5 2.5" opacity={0.6} />
        {/* хвост кометы */}
        {tail && <polygon points={tail.poly} fill={legColor} opacity={tailOp} />}
        {/* голова кометы */}
        <circle cx={todayX} cy={Y} r={headR} fill={legColor} opacity={headOp} />
      </svg>
      <span style={{ ...MONO, fontSize: 'var(--fs-base)', fontWeight: 800, color: legColor, opacity: headOp, whiteSpace: 'nowrap', lineHeight: 1.1 }}>
        {fmtPct(netPct)}<br /><span style={{ fontSize: 'var(--fs-2xs)', fontWeight: 700, letterSpacing: '0.04em', textTransform: 'uppercase' }}>{legWord}</span>
      </span>
    </div>
  );
}
