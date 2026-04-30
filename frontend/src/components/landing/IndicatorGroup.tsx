/**
 * IndicatorGroup — секция лендинга для группы из 2-3 индикаторов под общей темой.
 *
 * Структура:
 *   [Заголовок группы]
 *   [Подзаголовок 1-2 строки]
 *   [Grid карточек индикаторов]
 *
 * Карточка (IndicatorCard) поддерживает либо <video loop> (если videoUrl задан),
 * либо SVG/JSX иллюстрацию (fallback). Это позволяет постепенно заменять
 * static previews на записанные видео-демо без изменения структуры лендинга.
 */
import { Link } from 'react-router-dom';
import type { ReactNode } from 'react';
import HlsVideo from '../HlsVideo';

export interface Indicator {
  /** Название индикатора (короткое, для заголовка карточки) */
  title: string;
  /** Описание 1-2 строки — что показывает / зачем нужен */
  desc: string;
  /** Иконка в карточке (lucide-react component) */
  icon: ReactNode;
  /** Иконка для CTA-кнопки (обычно та же что icon, но size меньше) */
  ctaIcon?: ReactNode;
  /** Маршрут индикатора */
  href: string;
  /** URL видео-демо (.webm). MP4 fallback автоматически: тот же URL с .mp4 */
  videoUrl?: string;
  /** Постер/первый кадр видео (.jpg/.webp) — пока видео грузится */
  posterUrl?: string;
  /** SVG/JSX preview если видео ещё не записано (fallback) */
  illustration?: ReactNode;
  /** Текст CTA-кнопки (default: «Открыть») */
  ctaLabel?: string;
}

interface GroupProps {
  /** Заголовок группы (большой) */
  title: string;
  /** Подзаголовок — что узнаешь из этой группы */
  subtitle: string;
  /** Индикаторы (рекомендуется 2-3) */
  indicators: Indicator[];
}

export default function IndicatorGroup({ title, subtitle, indicators }: GroupProps) {
  return (
    <section className="mb-14 md:mb-20">
      {/* Header группы */}
      <div className="mb-8 md:mb-10 max-w-3xl">
        <h2
          className="text-2xl md:text-4xl font-bold mb-3"
          style={{
            color: 'var(--text-primary)',
            letterSpacing: '-0.02em',
            lineHeight: 1.1,
          }}
        >
          {title}
        </h2>
        <p
          className="text-sm md:text-base"
          style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}
        >
          {subtitle}
        </p>
      </div>

      {/* Карточки в стек — каждая на всю ширину контейнера, внутри
          горизонтальный split video|text на lg+. Это даёт максимум места
          под видео (~770×480px на десктопе) при компактном scroll. */}
      <div className="flex flex-col gap-4 md:gap-5">
        {indicators.map(ind => (
          <IndicatorCard key={ind.href} indicator={ind} />
        ))}
      </div>
    </section>
  );
}

/**
 * IndicatorCard — карточка одного индикатора.
 * Layout:
 *   mobile/md (<lg):   stack: header → desc → video → CTA
 *   lg+:               horizontal: [video 60%] | [text 40% column]
 *
 * Видео занимает ~770px ширины на десктопе (1280-padding-text-area-gaps).
 */
function IndicatorCard({ indicator }: { indicator: Indicator }) {
  const { title, desc, icon, ctaIcon, href, videoUrl, posterUrl, illustration, ctaLabel = 'Открыть' } = indicator;

  return (
    <Link
      to={href}
      className="group rounded-2xl border p-5 md:p-6 flex flex-col lg:flex-row gap-5 lg:gap-7 transition-all hover:scale-[1.005] active:scale-[0.995]"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        textDecoration: 'none',
      }}
      onMouseEnter={e => (e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--accent) 30%, var(--border-color))')}
      onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--border-color)')}
    >
      {/* Media: video (preferred) или SVG illustration.
          lg+: занимает основную часть ширины (flex-1 + max-width).
          mobile: full width сверху. */}
      <div
        className="rounded-xl overflow-hidden border w-full lg:flex-1 lg:max-w-[820px]"
        style={{
          backgroundColor: 'var(--bg-primary)',
          borderColor: 'var(--border-color)',
          aspectRatio: '1280 / 900',
        }}
      >
        <MediaArea videoUrl={videoUrl} posterUrl={posterUrl} illustration={illustration} />
      </div>

      {/* Text column — на lg выровнен по центру высоты video. */}
      <div className="flex flex-col lg:justify-center min-w-0 lg:flex-shrink-0 lg:w-[320px]">
        {/* Header: icon + title в одну строку */}
        <div className="flex items-center gap-3 mb-3">
          <div
            className="flex-shrink-0 flex items-center justify-center rounded-lg"
            style={{
              width: 40,
              height: 40,
              color: 'var(--accent)',
              backgroundColor: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            }}
          >
            {icon}
          </div>
          <h3
            className="text-xl md:text-2xl font-bold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em', lineHeight: 1.15 }}
          >
            {title}
          </h3>
        </div>

        {/* Description */}
        <p
          className="text-sm md:text-base mb-5"
          style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}
        >
          {desc}
        </p>

        {/* CTA — pseudo-button */}
        <span
          className="inline-flex items-center gap-2 text-sm font-semibold transition-opacity group-hover:opacity-100 opacity-90"
          style={{ color: 'var(--accent)' }}
        >
          {ctaLabel}
          {ctaIcon && <span className="inline-flex">{ctaIcon}</span>}
        </span>
      </div>
    </Link>
  );
}

/**
 * Media area — HLS video (streaming) или SVG fallback.
 *
 * HLS даёт мгновенный старт: первый сегмент ~200-400 KB подгружается за 1-2с,
 * остальные сегменты тянутся параллельно во время воспроизведения. Browser
 * HTTP/2 multiplexing работает эффективнее на коротких .ts чем на 7 длинных
 * video downloads → нет 15-секундной очереди при загрузке лендинга.
 *
 * URL convention: videoUrl=`/videos/<name>.webm` → HLS=`/videos/<name>/index.m3u8`,
 * mp4 fallback=`/videos/<name>.mp4`.
 */
function MediaArea({
  videoUrl, posterUrl, illustration,
}: { videoUrl?: string; posterUrl?: string; illustration?: ReactNode }) {
  if (videoUrl) {
    const baseName = videoUrl.replace(/\.webm$/, '').replace(/^\/videos\//, '');
    return (
      <HlsVideo
        hlsSrc={`/videos/${baseName}/index.m3u8`}
        mp4Src={videoUrl.replace(/\.webm$/, '.mp4')}
        poster={posterUrl}
        className="w-full h-full object-cover"
      />
    );
  }
  if (illustration) return <>{illustration}</>;
  return (
    <div
      className="w-full h-full flex items-center justify-center text-xs"
      style={{ color: 'var(--text-muted)' }}
    >
      (превью скоро)
    </div>
  );
}
