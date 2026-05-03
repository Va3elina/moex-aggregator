/**
 * Карточка-tooltip при наведении на график.
 * Позиционируется absolute внутри chart-контейнера.
 *
 * Автоматически определяет ширину родителя и свою собственную ширину:
 * - В левой половине → tooltip справа от курсора
 * - В правой половине → tooltip слева от курсора
 * Не нужно передавать flipAt / cardWidth — всё измеряется.
 */
import { useRef, useLayoutEffect, useState, useEffect } from 'react';
import { TOOLTIP } from '../../config/chartTheme';
import type { ReactNode } from 'react';

interface ChartTooltipProps {
  /** X-позиция в пикселях от левого края контейнера */
  x: number;
  /** Y-позиция в пикселях от верхнего края контейнера */
  y: number;
  /** Содержимое (строки TooltipRow) */
  children: ReactNode;
  /** Ручной override: порог flip'а в px (если не задан — 50% ширины родителя) */
  flipAt?: number;
}

const GAP = 12; // отступ между курсором и тултипом

export default function ChartTooltip({ x, y, children, flipAt }: ChartTooltipProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const cardRef = useRef<HTMLDivElement>(null);
  const [parentW, setParentW] = useState(800);
  const [cardW, setCardW] = useState(160);

  // Измеряем ширину родителя один раз (+ на resize)
  useEffect(() => {
    const el = wrapperRef.current?.parentElement;
    if (!el) return;
    const update = () => setParentW(el.clientWidth);
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Измеряем ширину карточки при каждом изменении содержимого
  useLayoutEffect(() => {
    if (cardRef.current) setCardW(cardRef.current.offsetWidth);
  }, [children]);

  const threshold = flipAt ?? parentW / 2;
  const isRight = x > threshold;

  return (
    <div
      ref={wrapperRef}
      className="absolute pointer-events-none z-30"
      style={{
        left: isRight ? Math.max(4, x - cardW - GAP) : x + GAP,
        top: Math.max(y - 40, 4),
      }}
    >
      <div ref={cardRef} className={TOOLTIP.containerClass} style={TOOLTIP.containerStyle}>
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
    <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
      <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: color }} />
      <span className={TOOLTIP.labelClass} style={TOOLTIP.labelStyle}>{label}</span>
      <span className={`${TOOLTIP.valueClass} ml-auto pl-2 ${valueClass ?? ''}`} style={{ ...TOOLTIP.valueStyle, color }}>
        {value}
      </span>
    </div>
  );
}
