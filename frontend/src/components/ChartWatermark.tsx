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
import Logo from './Logo';
import FrameLogo from './FrameLogo';
import { useTheme } from '../contexts/ThemeContext';
import { useViewportWidth } from '../config/fluidScale';

interface ChartWatermarkProps {
  opacity?: number;
  /** Максимальный размер глифа (на широком экране). По умолчанию 28. */
  size?: number;
  /** Минимальный размер глифа (на mobile). По умолчанию 18. */
  minSize?: number;
  /**
   * @deprecated Используется minSize. Оставлено для backwards compat.
   * Раньше: discrete jump 28→18 на 768px breakpoint.
   * Сейчас: плавный fluid scale между minSize и size.
   */
  mobileSize?: number;
  bottom?: number | string;
  left?: number | string;
  showText?: boolean;
}

export default function ChartWatermark({
  opacity = 0.55,
  size = 28,
  minSize = 18,
  mobileSize,
  bottom = '14%',
  left = '10%',
  showText = true,
}: ChartWatermarkProps) {
  const { theme } = useTheme();
  const isEditorial = theme.startsWith('editorial');

  // Fluid size: линейная интерполяция между minSize (на 375vw) и size (на 1440vw).
  // Заменяет discrete jump 28→18 на 768px breakpoint — теперь watermark плавно
  // меняет размер при resize окна, что важно при PWA orientation change или
  // при открытии devtools панели.
  // useViewportWidth re-renderит компонент при resize.
  const vw = useViewportWidth();
  const lo = mobileSize ?? minSize; // backwards compat: mobileSize wins если задан
  const effSize = Math.round(
    Math.max(lo, Math.min(size, lo + ((vw - 375) * (size - lo)) / (1440 - 375))),
  );

  // В editorial — FRAME-лого (брекеты + wordmark "FRAME"), 1в1 как в шапке.
  // Тёмная тень убирается на light-фонах (она для контраста с цветными
  // гистограммами на dark-теме, но на бумажном #F4F1EA выглядит как UI-артефакт).
  if (isEditorial) {
    return (
      <div
        aria-hidden="true"
        style={{
          position: 'absolute',
          bottom,
          left,
          pointerEvents: 'none',
          opacity: opacity * 0.9,
          zIndex: 1,
          color: 'var(--text-primary)',
          display: 'flex',
          alignItems: 'center',
        }}
      >
        <FrameLogo size={effSize} color="currentColor" showWordmark={showText} weight={700} />
      </div>
    );
  }

  // Non-editorial — текущая реализация (Logo + "Фрейм" + drop-shadow для контраста).
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
            textShadow: '0 0 3px rgba(0,0,0,0.85), 0 1px 2px rgba(0,0,0,0.65)',
          }}
        >
          Фрейм
        </span>
      )}
    </div>
  );
}
