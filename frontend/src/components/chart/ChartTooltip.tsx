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
  /** Вертикальный коридор: отступ от ВЕРХА родителя (px) — обычно позиция
      верхней грид-линии (--chart-pad-top). Карточка не поднимается выше. */
  clampTop?: number;
  /** Отступ от НИЗА родителя (px) — обычно высота зоны X-подписей
      (--chart-pad-bottom). Карточка не опускается ниже нижней грид-линии
      и не перекрывает даты оси. */
  clampBottom?: number;
}

const GAP = 12; // отступ между курсором и тултипом

export default function ChartTooltip({ x, y, children, flipAt, clampTop, clampBottom }: ChartTooltipProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const cardRef = useRef<HTMLDivElement>(null);
  const [parentW, setParentW] = useState(800);
  const [parentH, setParentH] = useState(400);
  const [cardW, setCardW] = useState(160);
  const [cardH, setCardH] = useState(56);

  // Измеряем размеры родителя один раз (+ на resize)
  useEffect(() => {
    const el = wrapperRef.current?.parentElement;
    if (!el) return;
    const update = () => { setParentW(el.clientWidth); setParentH(el.clientHeight); };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Измеряем размеры карточки при каждом изменении содержимого
  useLayoutEffect(() => {
    if (cardRef.current) { setCardW(cardRef.current.offsetWidth); setCardH(cardRef.current.offsetHeight); }
  }, [children]);

  const threshold = flipAt ?? parentW / 2;
  const isRight = x > threshold;

  // Вертикальный кламп: карточка целиком в коридоре между крайними
  // горизонтальными линиями графика, с запасом GAP_Y внутрь — чтобы не
  // прилипать к линиям и не перекрывать дату-пилюлю (её низ на 3px ниже
  // верхней линии). Math.max последним — при карточке выше коридора
  // прижимаемся к верхней границе (вылезет вниз, но не наверх).
  const GAP_Y = 6;
  const minTop = (clampTop ?? 4) + GAP_Y;
  const maxTop = parentH - (clampBottom ?? 4) - cardH - GAP_Y;

  return (
    <div
      ref={wrapperRef}
      className="absolute pointer-events-none z-30"
      style={{
        left: isRight ? Math.max(4, x - cardW - GAP) : x + GAP,
        top: Math.max(minTop, Math.min(y - 40, maxTop)),
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
