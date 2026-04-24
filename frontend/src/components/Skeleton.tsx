/**
 * Skeleton — placeholder для загружающихся данных.
 *
 * Зачем: когда виджет ещё не получил данные от API, мы хотим показать
 * серый "каркас" того же размера. Это решает проблему Cumulative Layout Shift:
 * без skeleton дашборд "прыгает" по мере загрузки разных API, с skeleton —
 * стабильный layout с первого кадра.
 *
 * Usage:
 *   <Skeleton width="100%" height={24} />
 *   <Skeleton width={120} height={40} rounded />
 *   <Skeleton className="h-4 w-32" />  (через Tailwind)
 */
import type { CSSProperties } from 'react';

interface SkeletonProps {
  width?: number | string;
  height?: number | string;
  rounded?: boolean | 'sm' | 'md' | 'lg' | 'full';
  className?: string;
  style?: CSSProperties;
}

export default function Skeleton({
  width,
  height,
  rounded = 'sm',
  className = '',
  style,
}: SkeletonProps) {
  const radiusMap = {
    sm: 'var(--radius-sm, 4px)',
    md: 'var(--radius-md, 8px)',
    lg: 'var(--radius-lg, 12px)',
    full: '9999px',
  };
  const borderRadius = rounded === true
    ? radiusMap.md
    : typeof rounded === 'string'
      ? radiusMap[rounded]
      : '0';

  return (
    <div
      className={className}
      style={{
        width,
        height,
        borderRadius,
        // Цвет — чуть светлее bg-secondary (fade-in feel).
        // Pulsing animation даёт UX-сигнал "это грузится".
        backgroundColor: 'color-mix(in srgb, var(--text-muted) 15%, transparent)',
        animation: 'skeleton-pulse 1.5s ease-in-out infinite',
        ...style,
      }}
    />
  );
}
