/**
 * Точка на линии при hover.
 * Рендерится внутри SVG viewBox="0 0 1000 500".
 */
import { DOT } from '../../config/chartTheme';

interface ChartDotProps {
  /** X-позиция в SVG координатах */
  x: number;
  /** Y-позиция в SVG координатах */
  y: number;
  /** Цвет заливки */
  color: string;
  /** Радиус (по умолчанию из темы) */
  radius?: number;
}

export default function ChartDot({ x, y, color, radius }: ChartDotProps) {
  return (
    <circle
      cx={x} cy={y} r={radius ?? DOT.radius}
      fill={color}
      stroke={DOT.strokeColor}
      strokeWidth={DOT.strokeWidth}
      vectorEffect="non-scaling-stroke"
    />
  );
}
