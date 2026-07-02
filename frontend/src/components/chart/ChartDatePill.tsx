/**
 * Плавающая дата над графиком при hover (crosshair/annotation).
 * Центрируется на x, низ прижат вплотную к верхней линии графика (topLineY),
 * и клампится в [minX, maxX] — у краёв plot-области пилюля останавливается
 * и не наезжает на цифры Y-шкал (как date-label на Polymarket).
 *
 * Ширина текста меряется через ref (шрифт/длина даты → разная ширина),
 * первый hover-кадр может быть без клампа (labelW ещё 0) — незаметно.
 */
import { useLayoutEffect, useRef, useState } from 'react';
import { TOOLTIP } from '../../config/chartTheme';
import { PILL_GAP_ABOVE_LINE } from './datePillLayout';

interface ChartDatePillProps {
  /** Текст даты */
  date: string;
  /** Желаемый центр пилюли по X (в координатах offsetParent) */
  x: number;
  /** Y верхней горизонтальной линии графика — низ пилюли прижимается к ней */
  topLineY: number;
  /** Левая граница plot-области — пилюля не заходит левее (левая Y-шкала) */
  minX?: number;
  /** Правая граница plot-области — пилюля не заходит правее (правая Y-шкала) */
  maxX?: number;
  /** Доп. классы обёртки (например chart-hover-ui для скрытия в export) */
  className?: string;
}

export default function ChartDatePill({ date, x, topLineY, minX, maxX, className = '' }: ChartDatePillProps) {
  const labelRef = useRef<HTMLSpanElement>(null);
  const [labelW, setLabelW] = useState(0);

  useLayoutEffect(() => {
    if (labelRef.current) setLabelW(labelRef.current.offsetWidth);
  }, [date]);

  const half = labelW / 2;
  let cx = x;
  if (maxX != null) cx = Math.min(cx, maxX - half);
  if (minX != null) cx = Math.max(cx, minX + half);

  return (
    <div
      className={`absolute z-20 pointer-events-none ${className}`}
      style={{ left: cx, top: topLineY - PILL_GAP_ABOVE_LINE, transform: 'translate(-50%, -100%)' }}
    >
      <span ref={labelRef} className={TOOLTIP.datePillClass} style={TOOLTIP.datePillStyle}>
        {date}
      </span>
    </div>
  );
}
