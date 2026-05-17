/**
 * Num — форматированное числовое значение.
 *
 * TradingView/Bloomberg-pattern: числа всегда monospace, tabular-nums,
 * с опциональным color-coding по знаку (+ зелёный / − красный).
 *
 * Usage:
 *   <Num value={3287.45} />                    → "3 287.45" (моно, tabular)
 *   <Num value={-1.2} sign percent />          → "−1.20%" красным
 *   <Num value={0.4} sign percent />           → "+0.40%" зелёным
 *   <Num value={-500} size="xl" />             → крупно
 *   <Num value={null} />                       → "—"
 */
import type { CSSProperties } from 'react';
import { formatNumber } from '../utils/formatNumber';

interface NumProps {
  /** Значение (null = показываем "—") */
  value: number | null | undefined;
  /** Знаков после запятой (default: 2) */
  decimals?: number;
  /** Добавить знак (+X / −X) и color-coded (зелёный/красный) */
  sign?: boolean;
  /** Добавить "%" суффикс */
  percent?: boolean;
  /** Префикс (напр. "$" или "₽") */
  prefix?: string;
  /** Размер шрифта — xs/sm/md/lg/xl */
  size?: 'xs' | 'sm' | 'md' | 'lg' | 'xl';
  /** Вес — bold для hero-чисел */
  bold?: boolean;
  /** Цвет вручную (перебивает auto-coloring от sign) */
  color?: string;
  /** Inline-style для spacing */
  style?: CSSProperties;
  /** CSS class для положения / margin */
  className?: string;
}

const SIZE_MAP = {
  xs: '10px',
  sm: '12px',
  md: '14px',
  lg: '18px',
  xl: '24px',
} as const;

export default function Num({
  value,
  decimals = 2,
  sign = false,
  percent = false,
  prefix,
  size = 'md',
  bold = false,
  color,
  style,
  className,
}: NumProps) {
  // null state
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return (
      <span
        className={className}
        style={{
          fontFamily: "'IBM Plex Mono', 'SF Mono', monospace",
          fontVariantNumeric: 'tabular-nums',
          fontSize: SIZE_MAP[size],
          color: color ?? 'var(--text-muted)',
          ...style,
        }}
      >
        —
      </span>
    );
  }

  // Авто-цвет: + зелёный, − красный
  const autoColor = sign
    ? value > 0 ? 'var(--success)'
    : value < 0 ? 'var(--danger)'
    : 'var(--text-secondary)'
    : 'var(--text-primary)';

  // Format: пробел тысяч + точка дробной (единый стиль сайта).
  const formatted = formatNumber(Math.abs(value), decimals);
  const signChar = sign ? (value > 0 ? '+' : value < 0 ? '−' : '') : '';

  return (
    <span
      className={className}
      style={{
        fontFamily: "'IBM Plex Mono', 'SF Mono', 'Menlo', monospace",
        fontVariantNumeric: 'tabular-nums slashed-zero',
        fontSize: SIZE_MAP[size],
        fontWeight: bold ? 700 : 500,
        color: color ?? autoColor,
        letterSpacing: '-0.02em',
        ...style,
      }}
    >
      {prefix}
      {signChar}
      {formatted}
      {percent && '%'}
    </span>
  );
}
