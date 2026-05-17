/**
 * MobileSkeleton — placeholder-blocks для loading states на мобильных страницах.
 *
 * Использует существующий @keyframes skeleton-pulse из index.css
 * (opacity 1 → 0.5 → 1 за 1.5s) — нативно работает в обеих темах через
 * var(--bg-secondary).
 *
 * Variants:
 *   <MobileSkeleton variant="line" />     — горизонтальная плашка (текст)
 *   <MobileSkeleton variant="chart" />    — chart-area imitation
 *   <MobileSkeleton variant="card" />     — карточка
 *   <MobileSkeleton variant="chip" />     — chip-таблетка
 *
 * Эта обёртка специфична для мобильной версии — на десктопе используется
 * другой <Skeleton /> компонент с разными размерами.
 */
import type { CSSProperties } from 'react';

interface MobileSkeletonProps {
  variant?: 'line' | 'chart' | 'card' | 'chip';
  /** Ширина (CSS-значение). Если не задано — variant'ом определяется. */
  width?: string | number;
  /** Высота (CSS-значение). Если не задано — variant'ом определяется. */
  height?: string | number;
  /** Дополнительный inline-style. */
  style?: CSSProperties;
  className?: string;
}

const VARIANT_DEFAULTS: Record<string, { w: string; h: string; r: string }> = {
  line:  { w: '100%', h: '12px',  r: '4px' },
  chart: { w: '100%', h: '260px', r: '12px' },
  card:  { w: '100%', h: '64px',  r: '12px' },
  chip:  { w: '60px', h: '28px',  r: '999px' },
};

export default function MobileSkeleton({
  variant = 'line',
  width,
  height,
  style,
  className = '',
}: MobileSkeletonProps) {
  const defaults = VARIANT_DEFAULTS[variant];
  return (
    <div
      className={`fm-skel ${className}`}
      style={{
        width: width ?? defaults.w,
        height: height ?? defaults.h,
        borderRadius: defaults.r,
        background: 'color-mix(in srgb, var(--text-primary) 8%, var(--bg-secondary))',
        animation: 'skeleton-pulse 1.5s ease-in-out infinite',
        ...style,
      }}
    />
  );
}

/**
 * Готовые композиции для типичных мобильных layout'ов.
 */
export function MobileChartSkeleton({ height = 280 }: { height?: number }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <MobileSkeleton variant="chart" height={height} />
      <div style={{ display: 'flex', gap: 6 }}>
        <MobileSkeleton variant="line" width="40%" height={10} />
        <MobileSkeleton variant="line" width="30%" height={10} />
        <MobileSkeleton variant="line" width="20%" height={10} />
      </div>
    </div>
  );
}

export function MobileChipRowSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div style={{ display: 'flex', gap: 6, padding: '4px 12px 8px' }}>
      {Array.from({ length: count }).map((_, i) => (
        <MobileSkeleton key={i} variant="chip" width="100%" height={32} />
      ))}
    </div>
  );
}

export function MobileCardListSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '0 12px' }}>
      {Array.from({ length: count }).map((_, i) => (
        <MobileSkeleton key={i} variant="card" />
      ))}
    </div>
  );
}
