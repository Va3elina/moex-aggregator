/**
 * PositionComet — «след кометы» для строки скринера сигналов ОИ.
 *
 * Ось перекоса ФИКСИРОВАНА −100…+100 (ноль по центру, слева шорт, справа
 * лонг) — строки сравнимы глазами. Три величины закодированы:
 *   • X головы   = ГДЕ стоит толпа сейчас (у края = «полный лонг/шорт»);
 *   • длина хвоста = НА СКОЛЬКО сдвинулись за день (было → сегодня);
 *   • РАЗМЕР головы = сила ×N (2× — маленькая, 5× — крупная): видно
 *     «размах» движения не только цифрой, но и глазом.
 *
 * Число справа = ПЕРЕКОС: |net_pct|% + слово ЛОНГ/ШОРТ (0 = поровну,
 * 100% = все в одну сторону).
 *
 * Крошечный сдвиг не теряется: хвост по сжатой √-шкале с минимумом.
 * Цвет = нога (зелёный лонг / красный шорт); приглушён для не-sharp рядов.
 */
import type { CSSProperties } from 'react';

interface Props {
  netPct: number | null;         // перекос сегодня, −100…+100
  netPctPrev: number | null;     // перекос вчера (для хвоста)
  ratio: number | null;          // сила сигнала ×N (размер головы)
  status: 'sharp' | 'normal' | 'illiquid' | 'nodata';
  hideLabel?: boolean;           // скрыть подпись «%+лонг/шорт» — вынесена в свой столбец «Перекос»
}

// ── геометрия (viewBox 200×44, ось y=22) — крупная, размер головы «говорит» ×N ──
// Ось занимает 84% ширины viewBox: запас справа только под радиус головы (14px).
// Раньше viewBox был 250 при оси до 176 — треть ширины уходила в пустоту (остаток
// от версии с подписью «%+лонг/шорт» рядом; подпись давно в своём столбце).
const ZERO_X = 100;
const PER_PCT = 0.84;            // +100% на x=184, −100% на x=16
const AX_MIN = 16, AX_MAX = 184;
const Y = 22;
const TICKS = [-100, -50, 50, 100];   // ориентиры шкалы — иначе полоса без масштаба

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));
const xOf = (p: number) => ZERO_X + clamp(p, -100, 100) * PER_PCT;

const MONO: CSSProperties = {
  fontFamily: 'var(--font-mono, ui-monospace, monospace)',
  fontVariantNumeric: 'tabular-nums',
};

function fmtPct(p: number): string {
  return Math.abs(p).toFixed(1).replace('.', ',') + '%';
}

export default function PositionComet({ netPct, netPctPrev, ratio, status, hideLabel }: Props) {
  if (netPct == null) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const netLong = netPct >= 0;
  const legColor = netLong ? 'var(--oi-green)' : 'var(--oi-short)';
  const legWord = netLong ? 'лонг' : 'шорт';
  const dim = status !== 'sharp';
  const headOp = dim ? 0.6 : 1;
  const tailOp = dim ? 0.45 : 0.78;

  const todayX = xOf(netPct);
  // Голова: радиус ЯВНО растёт с ×N (4…14). 2× ≈ 4px, 5× ≈ 9px, ≥7× → 14px —
  // размах виден глазом, не только цифрой. Без ratio (не-sharp) — минимум.
  const headR = ratio != null ? clamp(4 + (ratio - 2) * 1.7, 4, 14) : 4;

  // Хвост: длина по √-шкале с минимумом 9px — крупные и микро-сдвиги читаются.
  let tail: { poly: string } | null = null;
  if (netPctPrev != null) {
    const yestX = xOf(netPctPrev);
    const dpp = Math.abs(netPct - netPctPrev);
    const mv = Math.sign(todayX - yestX) || 1;         // направление движения
    const tailLen = clamp(8 + 4.5 * Math.sqrt(dpp), 9, 52);
    const tailX = clamp(todayX - mv * tailLen, AX_MIN, AX_MAX);
    // Комета: широкая (±headR) у головы, сходит на нет (±1.6) у хвоста.
    tail = {
      poly: `${todayX},${Y - headR} ${tailX},${Y - 1.6} ${tailX},${Y + 1.6} ${todayX},${Y + headR}`,
    };
  }

  const title = `Перекос: ${fmtPct(netPct)} в ${legWord} (0 = поровну, 100% = все в одну сторону).`
    + (netPctPrev != null ? ` Вчера ${fmtPct(netPctPrev)} → сегодня ${fmtPct(netPct)}; хвост кометы = сдвиг за день, размер головы = сила ×N.` : '');

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }} title={title}>
      <svg viewBox="0 0 200 44" style={{ width: '100%', flexShrink: 1 }} aria-hidden="true">
        {/* зоны: слева шорт, справа лонг (тон для контекста ноги) */}
        <rect x={AX_MIN} y={Y - 10} width={ZERO_X - AX_MIN} height={20} rx={5} fill="var(--oi-short)" opacity={0.14} />
        <rect x={ZERO_X} y={Y - 10} width={AX_MAX - ZERO_X} height={20} rx={5} fill="var(--oi-green)" opacity={0.16} />
        {/* ось */}
        <line x1={AX_MIN} y1={Y} x2={AX_MAX} y2={Y} stroke="var(--text-secondary)" strokeWidth={1.4} opacity={0.55} />
        {/* деления ±50 / ±100 — задают масштаб полосы */}
        {TICKS.map((p) => (
          <line key={p} x1={xOf(p)} y1={Y - 10} x2={xOf(p)} y2={Y + 10}
                stroke="var(--text-secondary)" strokeWidth={1} opacity={0.3} />
        ))}
        {/* ноль */}
        <line x1={ZERO_X} y1={Y - 15} x2={ZERO_X} y2={Y + 15} stroke="var(--text-primary)" strokeWidth={1.5} strokeDasharray="3 3" opacity={0.6} />
        {/* хвост кометы */}
        {tail && <polygon points={tail.poly} fill={legColor} opacity={tailOp} />}
        {/* голова кометы */}
        <circle cx={todayX} cy={Y} r={headR} fill={legColor} opacity={headOp} />
      </svg>
      {!hideLabel && (
        <span style={{ ...MONO, fontSize: 'var(--fs-base)', fontWeight: 800, color: legColor, opacity: headOp, whiteSpace: 'nowrap', lineHeight: 1.1 }}>
          {fmtPct(netPct)}<br /><span style={{ fontSize: 'var(--fs-2xs)', fontWeight: 700, letterSpacing: '0.04em', textTransform: 'uppercase' }}>{legWord}</span>
        </span>
      )}
    </div>
  );
}
