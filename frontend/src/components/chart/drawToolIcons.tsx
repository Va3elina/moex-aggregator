/**
 * Иконки инструментов рисования — свои SVG вместо универсальных lucide-стрелок.
 *
 * Lucide даёт «стрелку вверх-вправо» и на трендовую линию, и на луч, и на
 * стрелку: в рейле три соседние кнопки выглядели одинаково и не читались.
 * Терминальная манера (TradingView и родня): сама геометрия фигуры + кружки-
 * якоря на её опорных точках — по иконке сразу видно, сколько точек ставить.
 * Рисуем от руки, чужие ассеты не тянем.
 *
 * ⚠️ Оптический вес держим общий с lucide-кнопками того же рейла (глаз, замок,
 * корзина): обводка 2 (дефолт lucide) и поле 24×24 занято целиком — рисунок
 * идёт от ~2.5 до ~21.5. С прежними 1.6 и полями по 5px иконки выглядели
 * мелкими и тонкими рядом с соседями.
 *
 * Сигнатура совместима с lucide-иконками (size/style/className), чтобы рейл
 * рендерил их одинаково.
 */
import type { CSSProperties, ReactNode } from 'react';

export interface DrawIconProps { size?: number; style?: CSSProperties; className?: string }

const S = {
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 2,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
};
/** Кружок-якорь опорной точки: полый, той же обводкой что и линия. */
const R = 2.4;

function Svg({ size = 16, style, className, children }: DrawIconProps & { children: ReactNode }) {
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
      <line x1="6.6" y1="17.4" x2="17.4" y2="6.6" />
      <circle cx="4.6" cy="19.4" r={R} />
      <circle cx="19.4" cy="4.6" r={R} />
    </Svg>
  );
}

/** Луч — якорь только в начале, дальше линия уходит за край. */
export function RayIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="6.6" y1="17.4" x2="21.4" y2="2.6" />
      <circle cx="4.6" cy="19.4" r={R} />
    </Svg>
  );
}

/** Стрелка — якорь в начале, наконечник в конце. */
export function ArrowLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="6.6" y1="17.4" x2="19.4" y2="4.6" />
      <path d="M13.4 4 20 4 20 10.6" />
      <circle cx="4.6" cy="19.4" r={R} />
    </Svg>
  );
}

/** Горизонталь — линия во всю ширину с якорем посередине. */
export function HLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="2.2" y1="12" x2="8.8" y2="12" />
      <line x1="15.2" y1="12" x2="21.8" y2="12" />
      <circle cx="12" cy="12" r={R} />
    </Svg>
  );
}

/** Вертикаль — то же самое, повёрнутое на 90°. */
export function VLineIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="12" y1="2.2" x2="12" y2="8.8" />
      <line x1="12" y1="15.2" x2="12" y2="21.8" />
      <circle cx="12" cy="12" r={R} />
    </Svg>
  );
}

/** Курсор выделения — стрелка с «хвостом», как указатель мыши. */
export function SelectIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <path d="M5 2.6 5 19.4 9.7 15 12.7 21.4 15.9 19.9 12.9 13.7 19 13.1Z" />
    </Svg>
  );
}

/** Прямоугольник — якоря в противоположных углах (две точки построения). */
export function RectIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <rect x="4.6" y="5.6" width="14.8" height="12.8" rx="1.5" />
      <circle cx="4.6" cy="5.6" r={R} />
      <circle cx="19.4" cy="18.4" r={R} />
    </Svg>
  );
}

/** Эллипс — вписан в те же две точки построения. */
export function EllipseIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <ellipse cx="12" cy="12" rx="7.6" ry="6.4" />
      <circle cx="4.6" cy="5.8" r={R} />
      <circle cx="19.4" cy="18.2" r={R} />
    </Svg>
  );
}

/** Фибоначчи — сетка уровней плюс диагональ базового движения. */
export function FibIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="2.5" y1="4.5" x2="21.5" y2="4.5" />
      <line x1="2.5" y1="9.5" x2="21.5" y2="9.5" />
      <line x1="2.5" y1="14.5" x2="21.5" y2="14.5" />
      <line x1="2.5" y1="19.5" x2="21.5" y2="19.5" />
      <line x1="5.5" y1="18.5" x2="18.5" y2="5.5" strokeDasharray="2.8 2.6" />
    </Svg>
  );
}

/** Кисть — свободный росчерк. */
export function BrushIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <path d="M2.6 17.4c2.7 0 2.9-9 5.7-9s2.9 11 5.7 11 3.7-11 7.4-11" />
    </Svg>
  );
}

/** Линейка — отрезок с засечками и якорями на концах. */
export function RulerIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="5.4" y1="18.6" x2="18.6" y2="5.4" />
      <line x1="6.9" y1="14.3" x2="9.7" y2="17.1" />
      <line x1="10.6" y1="10.6" x2="13.4" y2="13.4" />
      <line x1="14.3" y1="6.9" x2="17.1" y2="9.7" />
      <circle cx="3.6" cy="20.4" r={R} />
      <circle cx="20.4" cy="3.6" r={R} />
    </Svg>
  );
}

/** Текст — литера «T» с засечками, чтобы не путалась с крестом. */
export function TextIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <line x1="3.6" y1="4.6" x2="20.4" y2="4.6" />
      <line x1="12" y1="4.6" x2="12" y2="19.8" />
      <line x1="8.2" y1="19.8" x2="15.8" y2="19.8" />
    </Svg>
  );
}
