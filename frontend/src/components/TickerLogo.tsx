/**
 * TickerLogo — отображает логотип компании по её тикеру.
 *
 * Sprite-based: все 84 лого упакованы в один PNG (`/logos/sprite.png`)
 * + JSON-манифест с координатами (`/logos/sprite-manifest.json`).
 *
 * Преимущества vs отдельные файлы:
 *   - 2 HTTP-запроса вместо 70+ (sprite + manifest)
 *   - Один decode у браузера на всю коллекцию
 *   - Один объект в SW cache
 *   - 656 KB sprite vs ~3.4 MB суммы отдельных PNG
 *
 * Render: <div> с background-image + background-position.
 * Размер картинки масштабируется через background-size пропорционально.
 *
 * Если тикера нет в манифесте → fallback circle с инициалами.
 */
import { useEffect, useState } from 'react';
import plzlLogo from '../assets/plzl.png';

// Лого вне спрайта (отдельные картинки через Vite-import → бандлятся в dist/assets
// с content-hash, мимо очистки dist/logos в postbuild). Полюс — новый знак
// (золотые лепестки); спрайтовый PLZL/raw_054 был старым красно-чёрным.
const INDIVIDUAL_LOGOS: Record<string, string> = {
  PLZL: plzlLogo,
};

interface TickerLogoProps {
  ticker: string;
  size?: number;
  rounded?: 'none' | 'sm' | 'md' | 'full';
  className?: string;
  /** No-op для обратной совместимости. */
  eager?: boolean;
}

interface SpriteManifest {
  cell: number;             // размер ячейки в sprite (px)
  sprite: string;           // URL sprite-картинки
  logos: Record<string, [number, number]>;  // ticker → [x, y]
}

// ── Singleton manifest loader ──
// Один fetch на всё приложение. Все TickerLogo компоненты делятся
// одним результатом через subscribe-pattern.
let cachedManifest: SpriteManifest | null = null;
let pendingPromise: Promise<SpriteManifest | null> | null = null;
const subscribers = new Set<(m: SpriteManifest | null) => void>();

function loadManifest(): Promise<SpriteManifest | null> {
  if (cachedManifest) return Promise.resolve(cachedManifest);
  if (pendingPromise) return pendingPromise;
  pendingPromise = fetch('/logos/sprite-manifest.json')
    .then((r) => (r.ok ? r.json() : null))
    .then((m: SpriteManifest | null) => {
      cachedManifest = m;
      // Pre-decode sprite, чтобы первый рендер не моргал
      if (m?.sprite) {
        const img = new Image();
        img.decoding = 'async';
        img.src = m.sprite;
      }
      // Уведомляем всех подписчиков
      subscribers.forEach((fn) => fn(m));
      return m;
    })
    .catch(() => null);
  return pendingPromise;
}

/** Хук-обёртка над singleton-manifest. */
function useSpriteManifest(): SpriteManifest | null {
  const [manifest, setManifest] = useState(cachedManifest);
  useEffect(() => {
    if (manifest) return;
    let mounted = true;
    const cb = (m: SpriteManifest | null) => {
      if (mounted) setManifest(m);
    };
    subscribers.add(cb);
    loadManifest();
    return () => {
      mounted = false;
      subscribers.delete(cb);
    };
  }, [manifest]);
  return manifest;
}

function tickerHue(ticker: string): number {
  let hash = 0;
  for (let i = 0; i < ticker.length; i++) {
    hash = (hash << 5) - hash + ticker.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash) % 360;
}

const RADIUS_MAP = {
  none: '0',
  sm: '4px',
  md: '8px',
  full: '50%',
} as const;

export default function TickerLogo({
  ticker,
  size = 40,
  rounded = 'md',
  className = '',
}: TickerLogoProps) {
  const manifest = useSpriteManifest();
  const borderRadius = RADIUS_MAP[rounded];
  const initials = ticker.slice(0, 2).toUpperCase();
  const hue = tickerHue(ticker);

  // Отдельное картиночное лого (вне спрайта)? → <img>. Приоритет над спрайтом.
  const individual = INDIVIDUAL_LOGOS[ticker];
  if (individual) {
    return (
      <div
        className={`flex-shrink-0 ${className}`}
        style={{ width: size, height: size, borderRadius, overflow: 'hidden' }}
        aria-label={ticker}
      >
        <img
          src={individual}
          alt={ticker}
          width={size}
          height={size}
          style={{ width: size, height: size, objectFit: 'cover', display: 'block' }}
        />
      </div>
    );
  }

  // Тикер в манифесте? → sprite render через background-position
  const coords = manifest?.logos[ticker];
  if (manifest && coords) {
    const cell = manifest.cell;
    const [x, y] = coords;
    // Scale: native cell-size → требуемый size. Применяется и к bg-size, и к bg-position.
    const scale = size / cell;
    // Sprite размер: 10 cols × N rows (см. build-sprite.py).
    const COLS = 10;
    const totalLogos = Object.keys(manifest.logos).length;
    const rows = Math.ceil(totalLogos / COLS);
    return (
      <div
        className={`flex-shrink-0 ${className}`}
        style={{
          width: size,
          height: size,
          borderRadius,
          overflow: 'hidden',
          backgroundImage: `url(${manifest.sprite})`,
          backgroundPosition: `-${x * scale}px -${y * scale}px`,
          backgroundSize: `${COLS * cell * scale}px ${rows * cell * scale}px`,
          backgroundRepeat: 'no-repeat',
        }}
        aria-label={ticker}
      />
    );
  }

  // Fallback: цветной круг с инициалами (если тикера нет в sprite или
  // manifest ещё не загружен)
  const fontSize = Math.max(9, Math.round(size * 0.38));
  return (
    <div
      className={`flex items-center justify-center flex-shrink-0 ${className}`}
      style={{
        width: size,
        height: size,
        borderRadius,
        backgroundColor: `hsl(${hue}, 55%, 35%)`,
        color: 'rgba(255,255,255,0.92)',
        fontSize,
        fontWeight: 700,
        fontFamily: "'IBM Plex Mono', monospace",
        letterSpacing: '-0.02em',
        lineHeight: 1,
      }}
      aria-label={ticker}
    >
      {initials}
    </div>
  );
}
