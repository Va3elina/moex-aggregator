/**
 * ChartWatermark — водяной знак «Фрейм» на графиках.
 *
 * Располагается внутри chart-области (не прилепленный к углу контейнера),
 * как у msc-insider / tradingview: левая нижняя четверть графика,
 * вне данных но внутри визуального поля.
 *
 * Компонент: эмблема Ф + слово «Фрейм» — как в header сайта.
 * Цвет через `currentColor` → наследует `--text-primary`.
 *
 * Адаптивно: на мобиле (< 768px) автоматически уменьшается до 18px эмблема +
 * 13px текст — иначе на iPhone SE водяной знак занимает 35% экрана и выглядит
 * как UI-элемент, а не «фоновый» бренд-маркер.
 *
 * Контраст: drop-shadow для эмблемы + text-shadow для надписи дают
 * тёмный outline. На чёрном фоне shadow не виден, на цветных
 * столбиках гистограмм — обрамляет watermark тёмной каймой и делает
 * его читаемым на любом background.
 */
import { useEffect, useState } from 'react';
import Logo from './Logo';

interface ChartWatermarkProps {
  opacity?: number;
  size?: number;
  /** Размер на мобиле (<768px). По умолчанию 18 (вместо desktop 28). */
  mobileSize?: number;
  bottom?: number | string;
  left?: number | string;
  showText?: boolean;
}

export default function ChartWatermark({
  opacity = 0.55,
  size = 28,
  mobileSize = 18,
  bottom = '14%',
  left = '10%',
  showText = true,
}: ChartWatermarkProps) {
  // Адаптивный размер. Listener resize даёт мгновенный rerender при повороте
  // или открытии devtools (изменение viewport).
  const [isMobile, setIsMobile] = useState(
    typeof window !== 'undefined' && window.innerWidth < 768,
  );
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, []);

  const effSize = isMobile ? mobileSize : size;

  return (
    <div
      aria-hidden="true"
      style={{
        position: 'absolute',
        bottom,
        left,
        pointerEvents: 'none',
        opacity,
        zIndex: 1,
        color: 'var(--text-primary)',
        display: 'flex',
        alignItems: 'center',
        gap: Math.round(effSize * 0.25),
        // Тёмный drop-shadow → кайма вокруг watermark на любом фоне.
        filter: 'drop-shadow(0 1px 2px rgba(0,0,0,0.75))',
      }}
    >
      <Logo size={effSize} color="currentColor" />
      {showText && (
        <span
          style={{
            fontSize: Math.round(effSize * 0.72),
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
