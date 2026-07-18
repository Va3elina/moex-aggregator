import { useLayoutEffect, useRef, useState } from 'react';

/**
 * Возвращает высоту для `height`-пропа графика (SimpleChart) такую, чтобы ВЕСЬ
 * видимый блок (anchor: рамка + паддинг + легенда + сам график + range-slider)
 * держал фиксированное соотношение сторон к своей ширине — в отличие от
 * useFitToViewport (высота = остаток вьюпорта, независимо от ширины), здесь
 * anchor.height = anchor.width / ratio на любом экране.
 *
 * Почему не просто `height = width / ratio`: значение, которое мы отдаём в
 * `height`-проп SimpleChart, — это высота ТОЛЬКО его SVG-области; сверху и
 * снизу к ней добавляются паддинг/легенда/навигатор (см. комментарий у
 * placeholderHeight в SimpleChart.tsx) — их совместная высота не входит в
 * переданный height. Если отдать width/ratio напрямую, весь видимый блок
 * (anchor) окажется ВЫШЕ 16:9 на эту «рамку» — заметно на скриншоте (получится
 * площе, а не 16:9). Поэтому хук сам меряет фактическую разницу между тем,
 * что было передано в height, и итоговой отрисованной высотой anchor'а
 * («chrome» — рамка вокруг SVG), и на следующем тике компенсирует её — работает
 * для любого набора легенда/навигатор/паддинг, без хардкода константы.
 *
 * Использование (как useFitToViewport, но по ширине, не по вьюпорту):
 *   const chartAnchorRef = useRef<HTMLDivElement>(null);
 *   const chartHeight = useAspectHeight(chartAnchorRef, { ratio: 16 / 9, min: 220 });
 *   <div ref={chartAnchorRef}><SimpleChart height={chartHeight} /></div>
 */
type Ref = React.RefObject<HTMLElement | null>;

interface Options {
  /** width / height видимого блока целиком, напр. 16/9 (по умолчанию) */
  ratio?: number;
  /** Пол для height-пропа — чтобы график не схлопнулся в ноль на очень узких контейнерах */
  min?: number;
}

export function useAspectHeight(anchorRef: Ref, { ratio = 16 / 9, min = 180 }: Options = {}): number {
  const [height, setHeight] = useState(min);
  const computeRef = useRef<() => void>(() => {});
  const lastPassedRef = useRef(min);
  const chromeRef = useRef(0);

  useLayoutEffect(() => {
    const compute = () => {
      const anchor = anchorRef.current;
      if (!anchor) return;
      const rect = anchor.getBoundingClientRect();
      const width = rect.width;
      if (width <= 0) return;

      // Калибровка «рамки» по факту предыдущего рендера: anchor уже отрисован
      // с height=lastPassedRef.current — разница с реальной высотой блока это
      // и есть паддинг+легенда+навигатор сверх SVG-области.
      const observedChrome = rect.height - lastPassedRef.current;
      if (observedChrome >= 0 && observedChrome < rect.height) {
        chromeRef.current = observedChrome;
      }

      const desiredTotal = width / ratio;
      const next = Math.max(min, desiredTotal - chromeRef.current);
      if (Math.abs(next - lastPassedRef.current) > 1) {
        lastPassedRef.current = next;
        setHeight(next);
      }
    };

    computeRef.current = compute;
    compute();

    const ro = new ResizeObserver(() => computeRef.current());
    if (anchorRef.current) ro.observe(anchorRef.current);

    // Как в useFitToViewport — window resize как fallback (не везде ResizeObserver
    // одинаково быстро реагирует на смену viewport, напр. смена ориентации).
    const onResize = () => computeRef.current();
    window.addEventListener('resize', onResize);

    // Найдено 2026-07-18 (реальный скриншот Вадима): на первом layout-эффекте
    // ширина anchor'а иногда ещё «черновая» — шрифты, скролл-бар, догрузка
    // контента над графиком сдвигают финальную ширину ПОЗЖЕ, чем срабатывает
    // единственный compute() выше. Несколько дешёвых доп. пересчётов в первую
    // секунду — подстраховка, не зависящая от источника гонки; заодно даёт
    // калибровке chrome ещё пару тиков сойтись к точному значению.
    const settleTimers = [50, 200, 500, 1000].map((ms) => setTimeout(() => computeRef.current(), ms));
    if ('fonts' in document) {
      document.fonts.ready.then(() => computeRef.current());
    }

    return () => {
      ro.disconnect();
      window.removeEventListener('resize', onResize);
      settleTimers.forEach(clearTimeout);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ratio, min]);

  return height;
}
