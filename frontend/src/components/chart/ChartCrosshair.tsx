/**
 * Вертикальная пунктирная линия при hover.
 * Рендерится внутри SVG viewBox="0 0 1000 500".
 */
import { CROSSHAIR, SVG } from '../../config/chartTheme';

interface ChartCrosshairProps {
  /** X-позиция в SVG координатах (0-1000) */
  x: number;
  /** Цвет (по умолчанию — из темы) */
  color?: string;
  /** Паттерн пунктира */
  dashArray?: string;
}

export default function ChartCrosshair({ x, color, dashArray }: ChartCrosshairProps) {
  return (
    <line
      x1={x} x2={x} y1="0" y2={SVG.viewBoxHeight}
      stroke={color ?? CROSSHAIR.color}
      strokeWidth={CROSSHAIR.strokeWidth}
      strokeDasharray={dashArray ?? CROSSHAIR.dashArray}
      vectorEffect="non-scaling-stroke"
      style={{ pointerEvents: 'none' }}
    />
  );
}
