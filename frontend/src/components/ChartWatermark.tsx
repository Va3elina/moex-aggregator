/**
 * ChartWatermark — водяной знак «Фрейм» на графиках.
 *
 * Располагается внутри chart-области (не прилепленный к углу контейнера),
 * как у msc-insider / tradingview: правая нижняя четверть графика,
 * вне данных но внутри визуального поля.
 *
 * Компонент: эмблема Ф + слово «Фрейм» — как в header сайта.
 * Цвет через `currentColor` → наследует `--text-primary`.
 *
 * Контраст: drop-shadow для эмблемы + text-shadow для надписи дают
 * тёмный outline. На чёрном фоне shadow не виден, на цветных
 * столбиках гистограмм — обрамляет watermark тёмной каймой и делает
 * его читаемым на любом background.
 */
import Logo from './Logo';

interface ChartWatermarkProps {
  opacity?: number;
  size?: number;
  bottom?: number | string;
  right?: number | string;
  showText?: boolean;
}

export default function ChartWatermark({
  opacity = 0.32,
  size = 28,
  bottom = '14%',
  right = '10%',
  showText = true,
}: ChartWatermarkProps) {
  return (
    <div
      aria-hidden="true"
      style={{
        position: 'absolute',
        bottom,
        right,
        pointerEvents: 'none',
        opacity,
        zIndex: 1,
        color: 'var(--text-primary)',
        display: 'flex',
        alignItems: 'center',
        gap: Math.round(size * 0.25),
        // Тёмный drop-shadow → кайма вокруг watermark на любом фоне.
        filter: 'drop-shadow(0 1px 2px rgba(0,0,0,0.75))',
      }}
    >
      <Logo size={size} color="currentColor" />
      {showText && (
        <span
          style={{
            fontSize: Math.round(size * 0.72),
            fontWeight: 700,
            letterSpacing: '-0.02em',
            lineHeight: 1,
            color: 'currentColor',
            fontFamily: "'IBM Plex Sans', system-ui, -apple-system, sans-serif",
            // Text-shadow для текста — drop-shadow на родителе обрамляет и
            // эмблему и надпись как единое целое, но textShadow добавляет
            // ещё внутренний контур текста → лучше читается на цветном
            // столбике гистограммы.
            textShadow: '0 0 3px rgba(0,0,0,0.85), 0 1px 2px rgba(0,0,0,0.65)',
          }}
        >
          Фрейм
        </span>
      )}
    </div>
  );
}
