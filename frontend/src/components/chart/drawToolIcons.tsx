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

/** Прямоугольник — чистая фигура, без якорей. */
export function RectIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <rect x="3" y="4.5" width="18" height="15" rx="1.5" />
    </Svg>
  );
}

/** Эллипс — чистая фигура, без якорей. */
export function EllipseIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <ellipse cx="12" cy="12" rx="9" ry="7.5" />
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

/** Кисть — ручка, обжимка и ворс, наискосок. Геометрия рисуется по горизонтали
 *  и разворачивается группой: так проще править пропорции. */
export function BrushIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <g transform="rotate(-45 12 12)">
        <path d="M12.5 8.6 21 8.6a1.4 1.4 0 0 1 1.4 1.4v4a1.4 1.4 0 0 1-1.4 1.4h-8.5Z" />
        <path d="M12.5 8.2 7.4 8.2 2.2 12 7.4 15.8 12.5 15.8Z" />
        <line x1="9.6" y1="8.4" x2="9.6" y2="15.6" />
      </g>
    </Svg>
  );
}

/** Линейка — наклонная линейка с делениями (как обычная канцелярская). */
export function RulerIcon(p: DrawIconProps) {
  return (
    <Svg {...p}>
      <g transform="rotate(-45 12 12)">
        <rect x="1.6" y="9" width="20.8" height="6" rx="1.4" />
        <line x1="6" y1="9" x2="6" y2="11.8" />
        <line x1="9.7" y1="9" x2="9.7" y2="13" />
        <line x1="13.4" y1="9" x2="13.4" y2="11.8" />
        <line x1="17.1" y1="9" x2="17.1" y2="13" />
      </g>
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
