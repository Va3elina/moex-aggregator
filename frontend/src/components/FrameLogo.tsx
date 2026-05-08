/**
 * FrameLogo — лого Frame с 4 угловыми брекетами + wordmark.
 *
 * Используется в editorial-теме как primary brand mark:
 *   - В шапке (Layout)
 *   - Как watermark на графиках (SimpleChart)
 *
 * Глиф 32×32 — четыре L-образных угла, образующие рамку. Wordmark "FRAME"
 * пишется Archivo 800 рядом, размер пропорционален глифу.
 *
 * Цвет: по умолчанию `currentColor` — наследует от родителя. Для accent-варианта
 * передавай `color="var(--accent)"`.
 *
 * Источник: design_handoff_frame_redesign/logo.jsx
 */

interface FrameLogoProps {
  /** Размер глифа в px. Wordmark будет 0.95× от этого размера. */
  size?: number;
  /** Цвет лого (CSS color). По умолчанию currentColor. */
  color?: string;
  /** Показывать ли wordmark "FRAME" рядом с глифом. */
  showWordmark?: boolean;
  /** Толщина шрифта wordmark (700 / 800). По умолчанию 800. */
  weight?: 400 | 500 | 600 | 700 | 800;
  /** Дополнительный класс контейнера. */
  className?: string;
}

export default function FrameLogo({
  size = 28,
  color = 'currentColor',
  showWordmark = true,
  weight = 800,
  className = '',
}: FrameLogoProps) {
  // Single-SVG approach: glyph + text в одном SVG с явным y-positioning.
  // Раньше HTML inline-flex/inline-block — alignment зависел от font metrics
  // (cap-height, x-height, baseline-shift), которые отличаются между live
  // browser и html2canvas рендером. SVG baseline через `dominant-baseline`
  // и явное y= даёт идеальный alignment в обоих контекстах.
  if (!showWordmark) {
    return (
      <span className={className} style={{ display: 'inline-block', verticalAlign: 'middle' }}>
        <svg
          width={size}
          height={size}
          viewBox="0 0 32 32"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
          aria-label="Frame"
        >
          <path d="M3 3 H13 V8 H8 V13 H3 Z" fill={color} />
          <path d="M29 3 H19 V8 H24 V13 H29 Z" fill={color} />
          <path d="M3 29 H13 V24 H8 V19 H3 Z" fill={color} />
          <path d="M29 29 H19 V24 H24 V19 H29 Z" fill={color} />
        </svg>
      </span>
    );
  }

  // Wordmark version — single SVG с glyph (32×32 viewBox unit) + text.
  // dominantBaseline="central" + y=16 (центр глифа) — em-box wordmark'а сидит
  // на той же y что центр глифа. "central" = geometric middle of em-box (vs
  // "alphabetic" baseline которая зависит от font metrics → wordmark "торчал"
  // вверх). Identical behavior в browser и в html2canvas.
  const gap = 11;
  const textWidth = 110; // bumped с 95 — fontSize 30 (вместо 26) занимает больше
  const totalWidth = 32 + gap + textWidth;
  const renderHeight = size;
  const renderWidth = (totalWidth / 32) * size;

  return (
    <span className={className} style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <svg
        width={renderWidth}
        height={renderHeight}
        viewBox={`0 0 ${totalWidth} 32`}
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-label="Frame"
      >
        {/* Glyph corner brackets */}
        <path d="M3 3 H13 V8 H8 V13 H3 Z" fill={color} />
        <path d="M29 3 H19 V8 H24 V13 H29 Z" fill={color} />
        <path d="M3 29 H13 V24 H8 V19 H3 Z" fill={color} />
        <path d="M29 29 H19 V24 H24 V19 H29 Z" fill={color} />
        {/* Wordmark — em-box центр на y=16 (центр глифа). FontSize 30 — на
            один шаг больше относительно glyph 32x32 (≈ 94% от glyph height). */}
        <text
          x={32 + gap}
          y={16}
          fontFamily="'Archivo', 'Inter', system-ui, sans-serif"
          fontWeight={weight}
          fontSize={30}
          letterSpacing={-0.5}
          fill={color}
          dominantBaseline="central"
        >
          FRAME
        </text>
      </svg>
    </span>
  );
}
