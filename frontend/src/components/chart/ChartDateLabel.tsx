/**
 * Плавающая дата над графиком при hover.
 * Автоматически не выходит за края контейнера.
 */
import { useRef, useEffect, useState } from 'react';
import { TOOLTIP } from '../../config/chartTheme';

interface ChartDateLabelProps {
  /** Текст даты */
  date: string;
  /** X-позиция в пикселях (центр) */
  x: number;
  /** Ширина контейнера в пикселях (для clamp) */
  containerWidth?: number;
}

export default function ChartDateLabel({ date, x, containerWidth }: ChartDateLabelProps) {
  const ref = useRef<HTMLSpanElement>(null);
  const [labelW, setLabelW] = useState(100);

  useEffect(() => {
    if (ref.current) setLabelW(ref.current.offsetWidth);
  }, [date]);

  const maxX = containerWidth ?? 9999;
  const half = labelW / 2;
  const clampedX = Math.max(half, Math.min(x, maxX - half));

  return (
    <div className="relative" style={{ height: 22 }}>
      <div
        className="absolute pointer-events-none"
        style={{ left: clampedX, transform: 'translateX(-50%)', top: 0 }}
      >
        <span ref={ref} className={TOOLTIP.dateClass}>
          {date}
        </span>
      </div>
    </div>
  );
}
