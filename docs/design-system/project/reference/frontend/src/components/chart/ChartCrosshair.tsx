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
  /** Верхняя граница линии (по умолчанию 0) */
  y1?: number;
  /** Нижняя граница линии (по умолчанию viewBoxHeight) */
  y2?: number;
  /** Opacity */
  opacity?: number;
}

export default function ChartCrosshair({ x, color, dashArray, y1, y2, opacity }: ChartCrosshairProps) {
  return (
    <line
      x1={x} x2={x} y1={y1 ?? 0} y2={y2 ?? SVG.viewBoxHeight}
      stroke={color ?? CROSSHAIR.color}
      strokeWidth={CROSSHAIR.strokeWidth}
      strokeDasharray={dashArray ?? CROSSHAIR.dashArray}
      opacity={opacity}
      vectorEffect="non-scaling-stroke"
      style={{ pointerEvents: 'none' }}
    />
  );
}
