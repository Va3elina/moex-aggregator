/**
 * Горизонтальные линии сетки + нулевая линия.
 * Рендерится внутри SVG viewBox="0 0 1000 500".
 */
import { GRID, SVG } from '../../config/chartTheme';

interface ChartGridProps {
  /** Y-позиции линий в % (0-100) от высоты viewBox */
  yTicks: { pct: number }[];
  /** Показать нулевую линию? Передать позицию в % */
  zeroPct?: number;
  /** Вертикальные разделители (X-позиции в % от ширины viewBox) */
  xSeparators?: number[];
  /** emphasize=true → толще, контрастный цвет (text-primary), opacity 0.6.
      Используется когда 0 — это reference baseline для распределения
      положительных/отрицательных значений (yearly seasonality avg %). */
  emphasizeZero?: boolean;
}

export default function ChartGrid({ yTicks, zeroPct, xSeparators, emphasizeZero }: ChartGridProps) {
  const W = SVG.viewBoxWidth;
  const H = SVG.viewBoxHeight;

  return (
    <g className="chart-grid">
      {/* Горизонтальные линии */}
      {yTicks.map((t, i) => (
        <line key={`h-${i}`}
          x1="0" x2={W} y1={t.pct / 100 * H} y2={t.pct / 100 * H}
          stroke={GRID.major} strokeWidth="1" vectorEffect="non-scaling-stroke"
        />
      ))}
      {/* Нулевая линия */}
      {zeroPct !== undefined && (
        <line
          x1="0" x2={W} y1={zeroPct / 100 * H} y2={zeroPct / 100 * H}
          stroke={emphasizeZero ? 'var(--text-primary)' : GRID.zero}
          strokeWidth={emphasizeZero ? '1.75' : '1'}
          opacity={emphasizeZero ? 0.6 : 1}
          vectorEffect="non-scaling-stroke"
        />
      )}
      {/* Вертикальные разделители (месяцы и т.д.) */}
      {xSeparators?.map((xPct, i) => (
        <line key={`v-${i}`}
          x1={xPct / 100 * W} x2={xPct / 100 * W} y1="0" y2={H}
          stroke={GRID.separator} strokeWidth="1" vectorEffect="non-scaling-stroke"
        />
      ))}
    </g>
  );
}
