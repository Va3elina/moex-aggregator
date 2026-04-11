/**
 * Плавающая дата над графиком при hover.
 */
import { TOOLTIP } from '../../config/chartTheme';

interface ChartDateLabelProps {
  /** Текст даты */
  date: string;
  /** X-позиция в пикселях */
  x: number;
}

export default function ChartDateLabel({ date, x }: ChartDateLabelProps) {
  return (
    <div className="relative" style={{ height: 22 }}>
      <div className="absolute pointer-events-none" style={{ left: x, transform: 'translateX(-50%)' }}>
        <span className={TOOLTIP.dateClass}>
          {date}
        </span>
      </div>
    </div>
  );
}
