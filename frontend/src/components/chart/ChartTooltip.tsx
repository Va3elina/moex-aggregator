/**
 * Карточка-tooltip при наведении на график.
 * Позиционируется absolute внутри chart-контейнера.
 */
import { TOOLTIP } from '../../config/chartTheme';
import type { ReactNode } from 'react';

interface ChartTooltipProps {
  /** X-позиция в пикселях от левого края контейнера */
  x: number;
  /** Y-позиция в пикселях от верхнего края контейнера */
  y: number;
  /** Содержимое (строки TooltipRow) */
  children: ReactNode;
  /** Порог для переключения tooltip влево (по умолчанию 300px) */
  flipAt?: number;
}

export default function ChartTooltip({ x, y, children, flipAt = 300 }: ChartTooltipProps) {
  const isRight = x > flipAt;
  return (
    <div
      className="absolute pointer-events-none z-30"
      style={{
        left: isRight ? x - 150 : x + 12,
        top: Math.max(y - 40, 4),
      }}
    >
      <div className={TOOLTIP.containerClass}>
        {children}
      </div>
    </div>
  );
}

/** Одна строка внутри tooltip: цветная точка + label + value */
interface TooltipRowProps {
  color: string;
  label: string;
  value: string;
  /** Дополнительный CSS-класс для value */
  valueClass?: string;
}

export function TooltipRow({ color, label, value, valueClass }: TooltipRowProps) {
  return (
    <div className="flex items-center gap-2">
      <span className={TOOLTIP.dotSize} style={{ backgroundColor: color }} />
      <span className={TOOLTIP.labelClass}>{label}</span>
      <span className={`${TOOLTIP.valueClass} ml-auto pl-2 ${valueClass ?? ''}`} style={{ color }}>
        {value}
      </span>
    </div>
  );
}
