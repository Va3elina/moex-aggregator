/**
 * Кружок-маркер внизу графика (дивиденд, событие).
 * С tooltip при hover и вертикальной пунктирной линией.
 */
import { TOOLTIP } from '../../config/chartTheme';
import type { ReactNode } from 'react';

interface ChartMarkerProps {
  /** Текст внутри кружка (1-2 символа) */
  label: string;
  /** Позиция в % от ширины области (0-100) */
  xPct: number;
  /** Содержимое tooltip при hover */
  children?: ReactNode;
  /** Высота пунктирной линии */
  guideHeight?: string;
  /** Callback при hover */
  onHover?: () => void;
  /** Callback при уходе */
  onLeave?: () => void;
}

export default function ChartMarker({ label, xPct, children, guideHeight, onHover, onLeave }: ChartMarkerProps) {
  return (
    <div
      className="absolute -translate-x-1/2 group"
      style={{ left: `${xPct}%` }}
      onMouseEnter={onHover}
      onMouseLeave={onLeave}
    >
      {/* Кружок */}
      <div
        className="w-7 h-7 rounded-full flex items-center justify-center cursor-pointer transition-opacity opacity-50 hover:opacity-100"
        style={{ backgroundColor: '#3a3f4f', color: '#9CA3B8', fontSize: 11, fontWeight: 600 }}
      >
        {label}
      </div>

      {/* Вертикальная пунктирная линия */}
      {guideHeight && (
        <div
          className="hidden group-hover:block absolute left-1/2 pointer-events-none"
          style={{
            bottom: 28,
            height: guideHeight,
            borderLeft: '1px dashed rgba(156, 163, 184, 0.4)',
          }}
        />
      )}

      {/* Tooltip при hover */}
      {children && (
        <div className="hidden group-hover:block absolute bottom-full left-1/2 -translate-x-1/2 mb-2 whitespace-nowrap z-30">
          <div className={TOOLTIP.containerClass}>
            {children}
          </div>
        </div>
      )}
    </div>
  );
}
