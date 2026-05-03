import { useEffect, useLayoutEffect, useRef, useState } from 'react';

/**
 * Возвращает высоту, которую можно отдать «главному» элементу страницы (графику)
 * чтобы вместе со всем что над ним — поместиться в один экран.
 *
 * Работает по anchor-ref: ставишь ref на тот элемент, ОТ которого начинается
 * «гибкая» зона (обычно — wrapper самого графика). Хук считает
 *   height = viewport.innerHeight − anchor.top − bottomBuffer
 * и возвращает clamp(min, height, max).
 *
 * Это автоматически учитывает ВСЁ что выше anchor: sticky-nav, padding'и
 * страницы, margins, заголовок, controls bar, error-плашки — без хардкода.
 *
 * Использование:
 *   const chartAnchorRef = useRef<HTMLDivElement>(null);
 *   const chartHeight = useFitToViewport(chartAnchorRef, {
 *     min: 360, max: 720,
 *     bottomBuffer: 80, // page bottom padding + slider под графиком
 *   });
 *
 *   <PageHeader />
 *   <ControlsBar />
 *   <div ref={chartAnchorRef}>
 *     <SimpleChart height={chartHeight} />
 *   </div>
 */
type Ref = React.RefObject<HTMLElement | null>;

interface Options {
  /** Минимальная высота — даже если viewport крошечный */
  min?: number;
  /** Максимальная высота — на 4K чтобы не растягивало в нечитаемое полотно */
  max?: number;
  /**
   * Сколько вычитать снизу: page-padding-bottom + любой контент под графиком,
   * который должен оставаться выше fold (range slider внутри SimpleChart,
   * подпись с timestamp и т.п.).
   */
  bottomBuffer?: number;
  /** Дополнительные ref'ы за которыми надо наблюдать (триггер пересчёта при изменении их высоты) */
  watchRefs?: Ref[];
}

// Кэш последнего вычисленного height для устранения CLS при mount/reload.
//
// Three-tier стратегия (от быстрого к медленному):
//   1) module-level cache — переключение между индикаторами в одной сессии
//   2) localStorage — переживает reload страницы
//   3) viewport estimate — fallback для самого первого визита когда нет данных
//
// localStorage key включает params (min/max/buffer) + viewport bucket чтобы:
//   - разные конфиги не делили кэш
//   - изменение размера окна (например смена ландшафт/портрет на mobile) сбрасывало
let moduleCache: Map<string, number> = new Map();

function getCacheKey(min: number, max: number, bottomBuffer: number): string {
  if (typeof window === 'undefined') return '';
  // viewport bucket — округление до 100px чтобы 1820 и 1840 шарили кэш
  const vh = Math.round(window.innerHeight / 100) * 100;
  const vw = Math.round(window.innerWidth / 100) * 100;
  return `fitvp:${min}:${max}:${bottomBuffer}:${vw}x${vh}`;
}

function readCachedHeight(key: string): number | null {
  if (!key) return null;
  // 1) module-level (быстрее всего)
  const mc = moduleCache.get(key);
  if (mc !== undefined) return mc;
  // 2) localStorage
  try {
    const ls = localStorage.getItem(key);
    if (ls !== null) {
      const n = parseInt(ls, 10);
      if (!Number.isNaN(n)) {
        moduleCache.set(key, n); // подгружаем в module-cache на будущее
        return n;
      }
    }
  } catch { /* localStorage может быть отключён */ }
  return null;
}

function writeCachedHeight(key: string, value: number): void {
  if (!key) return;
  moduleCache.set(key, value);
  try {
    localStorage.setItem(key, String(Math.round(value)));
  } catch { /* quota exceeded или disabled */ }
}

export function useFitToViewport(
  anchorRef: Ref,
  { min = 320, max = 720, bottomBuffer = 64, watchRefs = [] }: Options = {},
): number {
  // Initial — берём из кэша (mod → localStorage), иначе оцениваем от viewport.
  const [height, setHeight] = useState(() => {
    if (typeof window === 'undefined') return min;
    const cached = readCachedHeight(getCacheKey(min, max, bottomBuffer));
    if (cached !== null) return cached;
    // Estimate: anchor типично на ~40% viewport (PageHeader + controls bar)
    const estimatedAnchorTop = window.innerHeight * 0.4;
    const estimated = window.innerHeight - estimatedAnchorTop - bottomBuffer;
    return Math.max(min, Math.min(max, estimated));
  });
  const computeRef = useRef<() => void>(() => {});

  useLayoutEffect(() => {
    const compute = () => {
      const anchor = anchorRef.current;
      if (!anchor) return;
      // top — позиция anchor относительно VIEWPORT (меняется при scroll).
      // Если страница прокручена так что anchor выше viewport (scrollY больше
      // позиции anchor в документе), top становится отрицательным → available
      // разрастается за пределы экрана. Math.max(0, top) трактует anchor «как
      // будто он на верху viewport» — даёт стабильный размер chart независимо
      // от scroll-позиции при mount/resize.
      const top = Math.max(0, anchor.getBoundingClientRect().top);
      const available = window.innerHeight - top - bottomBuffer;
      const next = Math.max(min, Math.min(max, available));
      // Сохраняем в module-cache + localStorage → следующий mount/reload стартует
      // с правильным значением (zero CLS).
      writeCachedHeight(getCacheKey(min, max, bottomBuffer), next);
      setHeight((prev) => (Math.abs(prev - next) > 1 ? next : prev));
    };

    computeRef.current = compute;
    compute();

    // Наблюдаем за всем что может изменить позицию якоря:
    //   - sticky nav (если в нём что-то свернулось/развернулось),
    //   - явно переданные watchRefs (PageHeader, Controls и т.п.),
    //   - сам якорь (на случай если он сам сдвинулся через CSS).
    const ro = new ResizeObserver(() => computeRef.current());
    if (anchorRef.current) ro.observe(anchorRef.current);

    const navEl = document.querySelector<HTMLElement>('nav');
    if (navEl) ro.observe(navEl);

    watchRefs.forEach((r) => r.current && ro.observe(r.current));

    const onResize = () => computeRef.current();
    window.addEventListener('resize', onResize);

    return () => {
      ro.disconnect();
      window.removeEventListener('resize', onResize);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [min, max, bottomBuffer]);

  // Доп. пересчёт после загрузки шрифтов (page-header может «дорасти» на 1-2px
  // когда system-font меняется на custom).
  useEffect(() => {
    if (!('fonts' in document)) return;
    document.fonts.ready.then(() => computeRef.current());
  }, []);

  return height;
}
