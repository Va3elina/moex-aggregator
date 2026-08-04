/**
 * Иконки инструментов рисования — свои SVG вместо универсальных lucide-стрелок.
 *
 * Lucide даёт «стрелку вверх-вправо» и на трендовую линию, и на луч, и на
 * стрелку: в рейле три соседние кнопки выглядели одинаково и не читались.
 * Терминальная манера (TradingView и родня): сама геометрия фигуры + кружки-
 * якоря на её опорных точках — по иконке сразу видно, сколько точек ставить.
 * Рисуем от руки, чужие ассеты не тянем.
 *
 * Сигнатура совместима с lucide-иконками (size/style/className), чтобы рейл
 * рендерил их одинаково.
 */
import type { CSSProperties } from 'react';

export interface DrawIconProps { size?: number; style?: CSSProperties; className?: string }

const S = {
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
};
/** Кружок-якорь опорной точки: полый, той же обводкой что и линия. */
const R = 2.2;

function Svg({ size = 16, style, className, children }: DrawIconProps & { children: React.ReactNode }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" style={style} className={className} aria-hidden="true" {...S}>
      {children}
    </svg>
  );
}

/** Отрезок между двумя точками — якоря на обоих концах. */
export function TrendLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="7.4" y1="16.6" x2="16.6" y2="7.4" />
      <circle cx="5.5" cy="18.5" r={R} />
      <circle cx="18.5" cy="5.5" r={R} />
    </Svg>
  );
}

/** Луч — якорь только в начале, дальше линия уходит за край. */
export function RayIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="7.4" y1="16.6" x2="20.5" y2="3.5" />
      <circle cx="5.5" cy="18.5" r={R} />
    </Svg>
  );
}

/** Стрелка — якорь в начале, наконечник в конце. */
export function ArrowLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="7.4" y1="16.6" x2="18.6" y2="5.4" />
      <path d="M13.5 5 19 5 19 10.5" />
      <circle cx="5.5" cy="18.5" r={R} />
    </Svg>
  );
}

/** Горизонталь — линия во всю ширину с якорем посередине. */
export function HLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="3" y1="12" x2="9.3" y2="12" />
      <line x1="14.7" y1="12" x2="21" y2="12" />
      <circle cx="12" cy="12" r={R} />
    </Svg>
  );
}

/** Вертикаль — то же самое, повёрнутое на 90°. */
export function VLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="12" y1="3" x2="12" y2="9.3" />
      <line x1="12" y1="14.7" x2="12" y2="21" />
      <circle cx="12" cy="12" r={R} />
    </Svg>
  );
}
