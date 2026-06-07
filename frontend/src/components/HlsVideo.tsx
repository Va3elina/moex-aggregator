/**
 * HlsVideo — HLS streaming видео-компонент с lazy load.
 *
 * Архитектура:
 *  - При mount: <video preload="none">, ничего не грузится
 *  - IntersectionObserver триггер'ит загрузку когда карточка въезжает в viewport
 *    (rootMargin 400px → запас на быстрый scroll)
 *  - Safari: native HLS через прямое присваивание video.src
 *  - Chrome/Firefox/Edge: hls.js MSE adapter
 *  - Fallback: mp4 если hls.js не доступен (очень старые браузеры)
 *
 * Зачем HLS:
 *  - Стартовый сегмент (~200-400 KB) → видео начинает играть с первой секунды,
 *    остальные сегменты подкачиваются параллельно во время воспроизведения.
 *  - Browser HTTP/2 multiplexing работает эффективнее на маленьких segments
 *    чем на 7 одновременных длинных video downloads.
 */
import { useEffect, useRef, useState } from 'react';
import type HlsType from 'hls.js';

interface Props {
  /** URL HLS playlist (.m3u8) */
  hlsSrc: string;
  /** URL .mp4 для Safari/fallback */
  mp4Src?: string;
  /** Постер пока видео не запустилось */
  poster?: string;
  /** Дополнительные классы для <video> */
  className?: string;
  /** rootMargin для IntersectionObserver */
  rootMargin?: string;
}

export default function HlsVideo({
  hlsSrc, mp4Src, poster, className, rootMargin = '400px',
}: Props) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [inView, setInView] = useState(false);

  // Trigger load когда видео близко к viewport
  useEffect(() => {
    if (!videoRef.current) return;
    const node = videoRef.current;
    const obs = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        setInView(true);
        obs.disconnect();
      }
    }, { rootMargin });
    obs.observe(node);
    return () => obs.disconnect();
  }, [rootMargin]);

  // Setup HLS / native / fallback после inView=true
  useEffect(() => {
    if (!inView || !videoRef.current) return;
    const video = videoRef.current;

    // Safari (Mac/iOS) — native HLS support
    if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = hlsSrc;
      video.play().catch(() => {});
      return;
    }

    // Modern browsers — hls.js грузим ДИНАМИЧЕСКИ (отдельный чанк ~500KB, НЕ в
    // entry-бандле): только когда видео реально въехало в viewport (inView).
    let hls: HlsType | null = null;
    let cancelled = false;
    void import('hls.js').then(({ default: Hls }) => {
      if (cancelled || !videoRef.current) return;
      if (Hls.isSupported()) {
        hls = new Hls({
          enableWorker: true,
          // Низкая задержка стартового сегмента — критично для лендинга
          lowLatencyMode: false,
          backBufferLength: 30,
        });
        hls.loadSource(hlsSrc);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          video.play().catch(() => {});
        });
      } else if (mp4Src) {
        // Last resort — direct mp4 (очень старые браузеры)
        video.src = mp4Src;
        video.play().catch(() => {});
      }
    });
    return () => { cancelled = true; if (hls) hls.destroy(); };
  }, [inView, hlsSrc, mp4Src]);

  return (
    <video
      ref={videoRef}
      autoPlay
      loop
      muted
      playsInline
      poster={poster}
      preload="none"
      className={className}
    />
  );
}
