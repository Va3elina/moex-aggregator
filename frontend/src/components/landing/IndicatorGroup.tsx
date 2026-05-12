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
  /** Опциональный бэдж рядом с заголовком (например «Alfa тест» для сырых индикаторов) */
  badge?: string;
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
      {/* Header группы — editorial: H2 Archivo + подзаголовок. */}
      <div className="mb-8 md:mb-10 max-w-3xl">
        <h2
          className="font-bold mb-3"
          style={{
            color: 'var(--text-primary)',
            fontSize: 'clamp(22px, 2vw + 0.8rem, 40px)',
            letterSpacing: '-0.03em',
            lineHeight: 1.05,
          }}
        >
          {title}
        </h2>
        <p
          className="text-sm md:text-base"
          style={{ color: 'var(--text-secondary)', lineHeight: 1.55, maxWidth: '60ch' }}
        >
          {subtitle}
        </p>
      </div>

      {/* Карточки в стек — каждая editorial-frame с 1.5px outline + hard-shadow. */}
      <div className="flex flex-col gap-5 md:gap-6">
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
  const { title, desc, icon, ctaIcon, href, videoUrl, posterUrl, illustration, ctaLabel = 'Открыть', badge } = indicator;

  return (
    <Link
      to={href}
      className="group editorial-frame editorial-press flex flex-col lg:flex-row gap-5 lg:gap-7"
      style={{
        textDecoration: 'none',
      }}
    >
      {/* Media: editorial-style frame — 1.5px outline + paper-bg внутри.
          Aspect 1280/800 = 16:10. Match'ит размер видео записанных через
          record-indicator.mjs (viewport 1280×800). */}
      <div
        className="overflow-hidden w-full lg:flex-1 lg:max-w-[820px]"
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1.5px solid var(--text-primary)',
          aspectRatio: '16 / 10',
        }}
      >
        <MediaArea videoUrl={videoUrl} posterUrl={posterUrl} illustration={illustration} />
      </div>

      {/* Text column — на lg выровнен по центру высоты video. */}
      <div className="flex flex-col lg:justify-center min-w-0 lg:flex-shrink-0 lg:w-[320px]">
        {/* Header: icon + title в одну строку. Иконка — editorial chip
            (accent fill + ink outline + hard-shadow), как PageHeader.icon. */}
        <div className="flex items-center gap-3 mb-3">
          <div
            className="flex-shrink-0 flex items-center justify-center"
            style={{
              width: 'clamp(36px, 2.5vw + 1.5rem, 44px)',
              height: 'clamp(36px, 2.5vw + 1.5rem, 44px)',
              color: 'var(--text-inverse)',
              backgroundColor: 'var(--accent)',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 8,
              boxShadow: 'var(--shadow-hard-chip)',
            }}
          >
            {icon}
          </div>
          <h3
            className="font-bold"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'clamp(18px, 1.0vw + 0.7rem, 26px)',
              letterSpacing: '-0.02em',
              lineHeight: 1.2,
            }}
          >
            {title}
          </h3>
          {badge && (
            <span
              className="uppercase font-bold flex-shrink-0"
              style={{
                fontSize: '0.65rem',
                letterSpacing: '0.06em',
                color: 'var(--accent)',
                border: '1px solid var(--accent)',
                borderRadius: '3px',
                padding: '2px 6px',
                lineHeight: 1.2,
                whiteSpace: 'nowrap',
              }}
            >
              {badge}
            </span>
          )}
        </div>

        {/* Description */}
        <p
          className="text-sm md:text-base mb-5"
          style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}
        >
          {desc}
        </p>

        {/* CTA — uppercase в editorial-стиле */}
        <span
          className="inline-flex items-center gap-2 font-bold uppercase transition-opacity group-hover:opacity-100 opacity-80"
          style={{
            color: 'var(--accent)',
            fontSize: 'var(--fs-2xs)',
            letterSpacing: '0.16em',
          }}
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
