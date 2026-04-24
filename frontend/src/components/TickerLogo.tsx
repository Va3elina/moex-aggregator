/**
 * TickerLogo — отображает логотип компании по её тикеру.
 *
 * Стратегия:
 *   1. Пытается загрузить SVG из /public/logos/<TICKER>.svg
 *   2. Если 404 — рендерит fallback: цветной круг с 2-буквенными инициалами
 *      (цвет deterministic от hash тикера — одинаковый при каждом рендере)
 *
 * Это даёт постепенную миграцию: сначала все используют fallback, по мере
 * добавления SVG-файлов в /public/logos/ они автоматически начинают
 * показываться. Zero config.
 *
 * Использование:
 *   <TickerLogo ticker="SBER" size={40} />
 *   <TickerLogo ticker="GAZP" size={24} rounded="full" />
 */
import { useState } from 'react';

interface TickerLogoProps {
  ticker: string;
  size?: number;
  /** Скругление контейнера. Default: md (8px). Для маленьких иконок — full. */
  rounded?: 'none' | 'sm' | 'md' | 'full';
  className?: string;
}

// Deterministic hash → [0, 360) HSL hue. Одинаковый ticker → одинаковый цвет.
function tickerHue(ticker: string): number {
  let hash = 0;
  for (let i = 0; i < ticker.length; i++) {
    hash = (hash << 5) - hash + ticker.charCodeAt(i);
    hash |= 0; // 32-bit int
  }
  return Math.abs(hash) % 360;
}

const RADIUS_MAP = {
  none: '0',
  sm: '4px',
  md: '8px',
  full: '50%',
} as const;

export default function TickerLogo({ ticker, size = 40, rounded = 'md', className = '' }: TickerLogoProps) {
  const [useFallback, setUseFallback] = useState(false);

  const initials = ticker.slice(0, 2).toUpperCase();
  const hue = tickerHue(ticker);
  const borderRadius = RADIUS_MAP[rounded];

  // Fallback: coloured circle with initials
  if (useFallback) {
    // Font size: ~35% of container size, но не меньше 9px
    const fontSize = Math.max(9, Math.round(size * 0.38));
    return (
      <div
        className={`flex items-center justify-center flex-shrink-0 ${className}`}
        style={{
          width: size,
          height: size,
          borderRadius,
          backgroundColor: `hsl(${hue}, 55%, 35%)`,
          color: 'rgba(255,255,255,0.92)',
          fontSize,
          fontWeight: 700,
          fontFamily: "'IBM Plex Mono', monospace",
          letterSpacing: '-0.02em',
          lineHeight: 1,
        }}
        aria-label={ticker}
      >
        {initials}
      </div>
    );
  }

  // Primary: actual SVG/PNG from /public/logos/
  return (
    <img
      src={`/logos/${ticker}.svg`}
      alt={ticker}
      width={size}
      height={size}
      className={`flex-shrink-0 ${className}`}
      style={{ borderRadius, objectFit: 'contain' }}
      onError={() => setUseFallback(true)}
    />
  );
}
