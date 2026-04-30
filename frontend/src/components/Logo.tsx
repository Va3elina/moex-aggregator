/**
 * Logo — эмблема бренда «Frame».
 *
 * Концепт: 4 corner-brackets образуют viewfinder/рамку (literal: «frame»),
 * рядом — слово FRAME жирным заглавным sans-serif.
 *
 * Inline SVG — наследует цвет от родителя через currentColor / `var(--accent)`.
 * Это позволяет логотипу автоматически перекрашиваться при смене темы
 * (OKX зелёный → фиолетовый/lime в новых темах).
 *
 * Два варианта использования:
 *   <Logo />              — full lockup (icon + word)
 *   <Logo iconOnly />     — только иконка (для favicon/mobile/avatar)
 */
interface LogoProps {
  size?: number;
  /** Только иконка без текста FRAME */
  iconOnly?: boolean;
  /** Если нужно другой цвет чем accent — можно указать. */
  color?: string;
  className?: string;
}

export default function Logo({ size = 32, iconOnly = false, color, className = '' }: LogoProps) {
  const fill = color ?? 'var(--accent)';

  if (iconOnly) {
    return (
      <svg
        viewBox="0 0 96 96"
        width={size}
        height={size}
        xmlns="http://www.w3.org/2000/svg"
        className={className}
        aria-label="Frame"
      >
        <FrameIcon fill={fill} />
      </svg>
    );
  }

  // Full lockup — icon + FRAME text. Aspect ~3.6:1 (icon 96 + text ~250 = ~340 wide on 96 tall)
  // size param scales height (96px reference).
  const height = size * 1.0;
  const width = size * 3.6;

  return (
    <svg
      viewBox="0 0 340 96"
      width={width}
      height={height}
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-label="Frame"
    >
      <FrameIcon fill={fill} />
      <text
        x="110"
        y="74"
        fontSize="68"
        fontWeight="900"
        letterSpacing="-2.5"
        fill={fill}
        fontFamily="'IBM Plex Sans', system-ui, -apple-system, sans-serif"
      >
        FRAME
      </text>
    </svg>
  );
}

/** Frame brackets — 4 corner-marker'а образуют viewfinder. */
function FrameIcon({ fill }: { fill: string }) {
  // Все размеры relative к 96×96 viewBox
  const t = 12; // толщина bracket'а
  const len = 32; // длина каждого ребра bracket'а
  const sz = 96;
  return (
    <g fill={fill}>
      {/* Top-left corner — горизонтальная + вертикальная полоски */}
      <rect x="0" y="0" width={len} height={t} />
      <rect x="0" y="0" width={t} height={len} />
      {/* Top-right corner */}
      <rect x={sz - len} y="0" width={len} height={t} />
      <rect x={sz - t} y="0" width={t} height={len} />
      {/* Bottom-left corner */}
      <rect x="0" y={sz - t} width={len} height={t} />
      <rect x="0" y={sz - len} width={t} height={len} />
      {/* Bottom-right corner */}
      <rect x={sz - len} y={sz - t} width={len} height={t} />
      <rect x={sz - t} y={sz - len} width={t} height={len} />
    </g>
  );
}
