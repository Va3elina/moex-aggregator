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
import type { ReactNode, CSSProperties } from 'react';

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
  /** Полная замена TOOLTIP.containerClass для этого инстанса (напр. крупная
   *  карточка Сезонности). Не мёрджится с дефолтным классом — Tailwind
   *  utility-конфликты (rounded-lg vs rounded-2xl) непредсказуемы по source order. */
  cardClassName?: string;
  /** Мёрджится поверх TOOLTIP.containerStyle (spread, later keys win). */
  cardStyle?: CSSProperties;
  /** Точка на графике (тот же px-фрейм что x/y, обычно вершина бара/значение) —
   *  если задана, рисуется маленький маркер + линия-коннектор до ближайшего
   *  края карточки (кламп по вертикали в границы карточки). */
  marker?: { x: number; y: number; color: string };
}

const GAP = 12; // отступ между курсором и тултипом

export default function ChartTooltip({
  x, y, children, flipAt, clampTop, clampBottom, cardClassName, cardStyle, marker,
}: ChartTooltipProps) {
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

  const cardLeft = isRight ? Math.max(4, x - cardW - GAP) : x + GAP;
  const cardTop = Math.max(minTop, Math.min(y - 40, maxTop));

  const card = (
    <div
      ref={wrapperRef}
      className="absolute pointer-events-none z-30"
      style={{ left: cardLeft, top: cardTop }}
    >
      <div ref={cardRef} className={cardClassName ?? TOOLTIP.containerClass} style={{ ...TOOLTIP.containerStyle, ...cardStyle }}>
        {children}
      </div>
    </div>
  );

  if (!marker) return card;

  // Линия-коннектор — кратчайший путь от точки данных до ближайшего
  // вертикального края карточки (кламп Y в границы карточки, как у
  // Highcharts/аналогов: линия всегда упирается В край, а не улетает
  // выше/ниже него).
  const edgeX = isRight ? cardLeft + cardW : cardLeft;
  const edgeY = Math.min(Math.max(marker.y, cardTop + 8), cardTop + Math.max(cardH - 8, 8));
  const minX = Math.min(marker.x, edgeX);
  const minY = Math.min(marker.y, edgeY);

  return (
    <>
      <svg
        className="absolute pointer-events-none z-30"
        style={{ left: minX, top: minY, overflow: 'visible' }}
        width={Math.max(1, Math.abs(edgeX - marker.x))}
        height={Math.max(1, Math.abs(edgeY - marker.y))}
      >
        <line
          x1={marker.x - minX} y1={marker.y - minY}
          x2={edgeX - minX} y2={edgeY - minY}
          stroke={marker.color} strokeWidth={1.5} strokeDasharray="3,3" opacity={0.7}
        />
      </svg>
      <div
        className="absolute pointer-events-none z-30 rounded-full"
        style={{
          left: marker.x, top: marker.y, width: 8, height: 8,
          transform: 'translate(-50%, -50%)',
          backgroundColor: marker.color,
          border: '2px solid var(--bg-primary)',
          boxShadow: `0 0 0 1px ${marker.color}`,
        }}
      />
      {card}
    </>
  );
}

/** Одна строка внутри tooltip: цветная точка + label + value */
interface TooltipRowProps {
  color: string;
  label: string;
  value: string;
  /** Дополнительный CSS-класс для value */
  valueClass?: string;
  /** Не рисовать точку слева — текст уезжает влево на её ширину + gap. */
  hideDot?: boolean;
  /** Цвет значения. По умолчанию = color (точка и число одного цвета).
   *  Нейтральный токен → число не красится. */
  valueColor?: string;
  /** Дополнительный CSS-класс для label (напр. `font-bold` для итоговой строки). */
  labelClass?: string;
  /** Цвет label. По умолчанию — вторичный текст (из TOOLTIP.labelClass). */
  labelColor?: string;
}

export function TooltipRow({
  color, label, value, valueClass, hideDot, valueColor, labelClass, labelColor,
}: TooltipRowProps) {
  return (
    <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
      {!hideDot && (
        <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: color }} />
      )}
      <span
        className={`${TOOLTIP.labelClass} ${labelClass ?? ''}`}
        // inline color перебивает text-theme-secondary из labelClass
        style={{ ...TOOLTIP.labelStyle, ...(labelColor ? { color: labelColor } : {}) }}
      >
        {label}
      </span>
      <span
        className={`${TOOLTIP.valueClass} ml-auto pl-2 ${valueClass ?? ''}`}
        style={{ ...TOOLTIP.valueStyle, color: valueColor ?? color }}
      >
        {value}
      </span>
    </div>
  );
}
