/**
 * useElementWidth — реактивная ширина конкретного DOM-элемента через
 * ResizeObserver. В отличие от useViewportWidth (window.innerWidth) меряет
 * реальную доступную ширину контейнера — с учётом sidebar'а, паддингов и
 * прочего layout'а вокруг.
 *
 * Используется тулбарами для адаптивного решения «горизонтальные сегменты или
 * сворачиваем в Dropdown» (см. AdaptiveSegmented): viewport-брейкпоинт тут врал
 * бы, потому что доступная ширина зависит не только от ширины окна.
 *
 * useLayoutEffect + синхронный первый замер — чтобы значение было готово до
 * первой отрисовки и контрол не мигал (segmented → dropdown) на первом кадре.
 */
import { useLayoutEffect, useState, type RefObject } from 'react';

export function useElementWidth(ref: RefObject<HTMLElement | null>): number {
  const [width, setWidth] = useState(0);

  useLayoutEffect(() => {
    const node = ref.current;
    if (!node) return;

    const update = () => setWidth(node.getBoundingClientRect().width);
    update();

    const ro = new ResizeObserver(update);
    ro.observe(node);
    return () => ro.disconnect();
  }, [ref]);

  return width;
}
