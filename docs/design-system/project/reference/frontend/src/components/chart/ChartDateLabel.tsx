/**
 * Плавающая дата над графиком при hover.
 * Автоматически измеряет ширину родителя и не выходит за края.
 */
import { useRef, useLayoutEffect, useState } from 'react';
import { TOOLTIP } from '../../config/chartTheme';

interface ChartDateLabelProps {
  /** Текст даты */
  date: string;
  /** X-позиция в пикселях (центр) */
  x: number;
  /** Ширина контейнера в пикселях (если не передана — измеряется автоматически) */
  containerWidth?: number;
}

export default function ChartDateLabel({ date, x, containerWidth }: ChartDateLabelProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const labelRef = useRef<HTMLSpanElement>(null);
  const [labelW, setLabelW] = useState(100);
  const [measuredW, setMeasuredW] = useState<number | null>(null);

  // Измеряем ширину лейбла при смене даты (шрифт/буквы → разная ширина)
  useLayoutEffect(() => {
    if (labelRef.current) setLabelW(labelRef.current.offsetWidth);
  }, [date]);

  // Измеряем ширину родителя (если не передана через prop)
  // ResizeObserver — автоматически реагирует на resize/zoom/DPI
  useLayoutEffect(() => {
    if (containerWidth != null) return;
    const el = wrapperRef.current?.parentElement;
    if (!el) return;
    const update = () => setMeasuredW(el.clientWidth);
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, [containerWidth]);

  const maxX = containerWidth ?? measuredW ?? 9999;
  const half = labelW / 2;
  const clampedX = Math.max(half, Math.min(x, maxX - half));

  return (
    <div ref={wrapperRef} className="relative" style={{ height: 22 }}>
      <div
        className="absolute pointer-events-none"
        style={{ left: clampedX, transform: 'translateX(-50%)', top: 0 }}
      >
        <span ref={labelRef} className={TOOLTIP.dateClass}>
          {date}
        </span>
      </div>
    </div>
  );
}
