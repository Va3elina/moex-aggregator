/**
 * usePointerTracking — универсальный хук для chart hover/tooltip работающий
 * одинаково на мыши (desktop) и пальце (mobile/touch).
 *
 * Проблема: SVG-графики используют onMouseMove для определения позиции курсора.
 * На touch-устройствах mouse-события НЕ срабатывают — только touch-* события.
 * Результат: пользователь не может пальцем "водить по графику", только тыкать.
 *
 * Это хук возвращает handlers, которые нужно разложить на SVG-контейнер:
 *   const handlers = usePointerTracking(onMove, onLeave);
 *   <div {...handlers}>...</div>
 *
 * При mouse — работает как раньше (onMouseMove → onMove).
 * При touch — touchStart и touchMove вызывают onMove (с координатами из touches[0]),
 *             touchEnd → onLeave.
 *
 * Bonus: touchAction: 'none' предотвращает нативный scroll/zoom при водении
 * пальцем по чарту (без этого браузер начинает скроллить страницу).
 */

import { useMemo } from 'react';

export interface PointerMoveEvent {
  clientX: number;
  clientY: number;
  /** Ссылка на исходный element (для getBoundingClientRect) */
  currentTarget: EventTarget & HTMLElement;
}

interface PointerHandlers {
  onMouseMove: (e: React.MouseEvent<HTMLElement>) => void;
  onMouseLeave: () => void;
  onTouchStart: (e: React.TouchEvent<HTMLElement>) => void;
  onTouchMove: (e: React.TouchEvent<HTMLElement>) => void;
  onTouchEnd: () => void;
  style: { touchAction: string };
}

export function usePointerTracking(
  onMove: (e: PointerMoveEvent) => void,
  onLeave: () => void,
): PointerHandlers {
  return useMemo(() => ({
    onMouseMove: (e: React.MouseEvent<HTMLElement>) => {
      onMove({
        clientX: e.clientX,
        clientY: e.clientY,
        currentTarget: e.currentTarget,
      });
    },
    onMouseLeave: onLeave,

    onTouchStart: (e: React.TouchEvent<HTMLElement>) => {
      if (e.touches.length > 0) {
        onMove({
          clientX: e.touches[0].clientX,
          clientY: e.touches[0].clientY,
          currentTarget: e.currentTarget,
        });
      }
    },
    onTouchMove: (e: React.TouchEvent<HTMLElement>) => {
      if (e.touches.length > 0) {
        onMove({
          clientX: e.touches[0].clientX,
          clientY: e.touches[0].clientY,
          currentTarget: e.currentTarget,
        });
      }
    },
    onTouchEnd: onLeave,

    // touchAction: 'none' — критично для chart UX на mobile.
    // Без этого iOS/Android начинают scroll страницы когда пользователь
    // водит пальцем по чарту. 'none' полностью отключает жесты браузера.
    style: { touchAction: 'none' },
  }), [onMove, onLeave]);
}
