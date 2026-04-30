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
  const glyph = (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      style={{ flexShrink: 0 }}
      aria-label="Frame"
    >
      <path d="M3 3 H13 V8 H8 V13 H3 Z" fill={color} />
      <path d="M29 3 H19 V8 H24 V13 H29 Z" fill={color} />
      <path d="M3 29 H13 V24 H8 V19 H3 Z" fill={color} />
      <path d="M29 29 H19 V24 H24 V19 H29 Z" fill={color} />
    </svg>
  );

  if (!showWordmark) {
    return <span className={className}>{glyph}</span>;
  }

  return (
    <span
      className={className}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: size * 0.32,
      }}
    >
      {glyph}
      <span
        style={{
          fontFamily: "'Archivo', 'Inter', system-ui, sans-serif",
          fontWeight: weight,
          fontSize: size * 0.95,
          letterSpacing: '-0.02em',
          color,
          lineHeight: 1,
        }}
      >
        FRAME
      </span>
    </span>
  );
}
