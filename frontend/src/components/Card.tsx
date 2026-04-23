/**
 * Card — универсальный container для widget-подобных блоков.
 *
 * Заменяет повсеместный паттерн:
 *   <div className="rounded-2xl border p-4"
 *     style={{backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)'}}>
 *     ...
 *   </div>
 * на
 *   <Card>...</Card>
 *
 * Поддерживает варианты, размеры, hover state — всё через единый API.
 */
import type { ReactNode, CSSProperties, HTMLAttributes } from 'react';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  /** Variant — default (bg + border) / elevated (тень) / flat (без border) */
  variant?: 'default' | 'elevated' | 'flat';
  /** Размер padding внутри (sm=12px / md=16px / lg=24px / none) */
  padding?: 'none' | 'sm' | 'md' | 'lg';
  /** Включить hover-эффект (border подсвечивается accent'ом) */
  hoverable?: boolean;
  /** Дополнительный inline-style для исключительных случаев */
  style?: CSSProperties;
  children: ReactNode;
}

const PADDING_MAP = {
  none: '0',
  sm: '12px',
  md: '16px',
  lg: '24px',
} as const;

export default function Card({
  variant = 'default',
  padding = 'md',
  hoverable = false,
  style,
  className = '',
  children,
  ...rest
}: CardProps) {
  const baseStyle: CSSProperties = {
    backgroundColor: 'var(--bg-secondary)',
    border: variant === 'flat' ? 'none' : '1px solid var(--border-color)',
    borderRadius: 'var(--radius-lg, 12px)',
    padding: PADDING_MAP[padding],
    boxShadow: variant === 'elevated' ? 'var(--shadow-md)' : 'none',
    transition: hoverable ? 'border-color 0.15s ease, background-color 0.15s ease' : undefined,
    ...style,
  };

  return (
    <div
      className={className}
      style={baseStyle}
      onMouseEnter={hoverable ? (e) => {
        e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--accent) 40%, transparent)';
      } : undefined}
      onMouseLeave={hoverable ? (e) => {
        e.currentTarget.style.borderColor = 'var(--border-color)';
      } : undefined}
      {...rest}
    >
      {children}
    </div>
  );
}
