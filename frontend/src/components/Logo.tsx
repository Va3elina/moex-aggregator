/**
 * Logo — эмблема бренда "Фрейм".
 *
 * Концепт: Cyrillic Ф в квадратной рамке (literal: "фрейм" = рамка).
 * Использует `currentColor` → наследует цвет от родителя через CSS var.
 *
 * Inline SVG (не <img>) нужен чтобы:
 *   - цвет менялся по теме (var(--accent))
 *   - масштабировался без loss качества
 *   - отдельные элементы можно анимировать (hover, etc.)
 *
 * Для favicon используется отдельный файл /public/logo.svg с
 * hardcoded-цветом (browser favicon не поддерживает currentColor).
 */
interface LogoProps {
  size?: number;
  /** Если нужно другой цвет чем accent — можно указать. */
  color?: string;
  className?: string;
}

export default function Logo({ size = 32, color, className = '' }: LogoProps) {
  const strokeColor = color ?? 'var(--accent)';
  return (
    <svg
      viewBox="0 0 40 40"
      width={size}
      height={size}
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-label="Фрейм"
    >
      {/* Рамка */}
      <rect
        x="2.5"
        y="2.5"
        width="35"
        height="35"
        rx="6"
        fill="none"
        stroke={strokeColor}
        strokeWidth="3"
      />
      {/* Буква Ф внутри рамки */}
      <text
        x="20"
        y="29"
        textAnchor="middle"
        fontSize="26"
        fontWeight="800"
        fill={strokeColor}
        fontFamily="'IBM Plex Sans', system-ui, -apple-system, sans-serif"
        style={{ letterSpacing: '-1px' }}
      >
        Ф
      </text>
    </svg>
  );
}
