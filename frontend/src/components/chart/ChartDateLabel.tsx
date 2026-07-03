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
  /** Кламп в границах plot-области: отступ слева (px) — дата не заходит
      на левую Y-шкалу. Default 0 (кламп по краю контейнера). */
  padLeft?: number;
  /** Отступ справа (px) — дата не заходит на правую Y-шкалу. Default 0. */
  padRight?: number;
}

export default function ChartDateLabel({ date, x, containerWidth, padLeft = 0, padRight = 0 }: ChartDateLabelProps) {
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

  const maxX = (containerWidth ?? measuredW ?? 9999) - padRight;
  const half = labelW / 2;
  const clampedX = Math.max(padLeft + half, Math.min(x, maxX - half));

  return (
    // Высота синхронизирована с placeholder'ом через ту же CSS var — без
    // этого при появлении tooltip'а контейнер прыгал бы на 22-Xpx (CLS).
    <div ref={wrapperRef} className="relative" style={{ height: 'var(--chart-date-placeholder-height, 22px)' }}>
      <div
        className="absolute pointer-events-none"
        style={{ left: clampedX, transform: 'translateX(-50%)', top: 0 }}
      >
        {/* display:block изолирует от наследуемого line-height контекста:
            inline-span выравнивался по baseline строки родителя (в сезонности
            line-height ~30px) и съезжал на ~11px вниз — дата оказывалась ПОД
            верхней грид-линией вместо НАД ней. */}
        <span ref={labelRef} className={TOOLTIP.datePillClass} style={{ ...TOOLTIP.datePillStyle, display: 'block' }}>
          {date}
        </span>
      </div>
    </div>
  );
}
