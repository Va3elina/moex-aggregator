/**
 * TickerLogo — отображает логотип компании по её тикеру.
 *
 * Рендер: <img> из `/public/logos/sm/<ticker>.png` — уменьшенная копия 128×128
 * (генерит scripts/build-sprite.py из мастер-исходников 256×256 в prebuild).
 * Ничто в приложении не показывает лого крупнее 36px (см. usage в InstrumentIcon) —
 * 128px даёт запас на retina (36×3=108px) при кратно меньшем весе файла, чем
 * полный 256px-исходник (тот раньше грузился напрямую — на модалку выбора
 * актива, ОИ/Сезонность, ~30 видимых лого, это было ~1MB трафика на первое
 * открытие). Ещё раньше был 80px-спрайт (`sprite.png`), который мылился при
 * дробном масштабе браузера — на полноразмерные <img> перешли ради чёткости,
 * 128px-копия сохраняет эту чёткость при доле веса оригинала.
 *
 * `/logos/sprite-manifest.json` всё ещё грузится — но только как реестр
 * «у какого тикера есть лого» (один fetch на всё приложение, см. singleton ниже).
 * Набор отображаемых лого не меняется — меняется только разрешение источника.
 *
 * Приоритет источника: INDIVIDUAL_LOGOS (Vite-import) → PNG по тикеру →
 * fallback circle с инициалами (если тикера нет в реестре или картинка не загрузилась).
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
  const [failed, setFailed] = useState(false);
  const borderRadius = RADIUS_MAP[rounded];
  const initials = ticker.slice(0, 2).toUpperCase();
  const hue = tickerHue(ticker);

  // Источник лого: индивидуальный override (Vite-import) или уменьшенный
  // (128px) PNG из /public/logos/sm/<ticker>.png. Манифест используется
  // только как реестр «у какого тикера есть лого» — набор отображаемых лого
  // не зависит от того, из какой директории берётся картинка.
  const individual = INDIVIDUAL_LOGOS[ticker];
  const hasLogo = !!manifest?.logos[ticker];
  const src = individual ?? (hasLogo ? `/logos/sm/${ticker}.png` : null);

  if (src && !failed) {
    return (
      <div
        className={`flex-shrink-0 ${className}`}
        style={{ width: size, height: size, borderRadius, overflow: 'hidden' }}
        aria-label={ticker}
      >
        <img
          src={src}
          alt={ticker}
          width={size}
          height={size}
          loading="lazy"
          decoding="async"
          onError={() => setFailed(true)}
          style={{ width: size, height: size, objectFit: 'contain', display: 'block' }}
        />
      </div>
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
