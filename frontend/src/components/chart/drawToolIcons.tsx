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

/** Курсор выделения — стрелка с «хвостом», как указатель мыши. */
export function SelectIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <path d="M6 3.5 6 17 9.7 13.6 12.2 19.4 14.6 18.3 12.1 12.7 17 12.2Z" />
    </Svg>
  );
}

/** Прямоугольник — якоря в противоположных углах (две точки построения). */
export function RectIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <rect x="5" y="6" width="14" height="12" rx="1" />
      <circle cx="5" cy="6" r={R} />
      <circle cx="19" cy="18" r={R} />
    </Svg>
  );
}

/** Эллипс — вписан в те же две точки построения. */
export function EllipseIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <ellipse cx="12" cy="12" rx="7" ry="5.6" />
      <circle cx="5.4" cy="6.6" r={R} />
      <circle cx="18.6" cy="17.4" r={R} />
    </Svg>
  );
}

/** Фибоначчи — сетка уровней плюс диагональ базового движения. */
export function FibIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="4" y1="5.5" x2="20" y2="5.5" />
      <line x1="4" y1="10" x2="20" y2="10" />
      <line x1="4" y1="14" x2="20" y2="14" />
      <line x1="4" y1="18.5" x2="20" y2="18.5" />
      <line x1="7" y1="17.6" x2="17" y2="6.4" strokeDasharray="2.6 2.4" />
    </Svg>
  );
}

/** Кисть — свободный росчерк. */
export function BrushIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <path d="M3.5 16.5c2.4 0 2.6-7 5-7s2.6 9 5 9 3.4-9 7-9" />
    </Svg>
  );
}

/** Линейка — отрезок с засечками и якорями на концах. */
export function RulerIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="6" y1="17.5" x2="18" y2="6.5" />
      <line x1="7.4" y1="13.6" x2="10.6" y2="16.4" />
      <line x1="10.4" y1="10.8" x2="13.6" y2="13.6" />
      <line x1="13.4" y1="8" x2="16.6" y2="10.8" />
      <circle cx="4.5" cy="19" r={R} />
      <circle cx="19.5" cy="5" r={R} />
    </Svg>
  );
}

/** Текст — литера «T» с засечками, чтобы не путалась с крестом. */
export function TextIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="5" y1="6" x2="19" y2="6" />
      <line x1="12" y1="6" x2="12" y2="19" />
      <line x1="9" y1="19" x2="15" y2="19" />
    </Svg>
  );
}
