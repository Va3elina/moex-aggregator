/**
 * useIsPhone — хук ТОЛЬКО для routing-гейтов (выбор mobile vs desktop
 * СТРАНИЦЫ: App/HomeRoute, ResponsiveRoute, Layout). НЕ для адаптива
 * компонентов — для width-адаптивных решений остаётся useIsMobile.
 *
 * Отличие от useIsMobile: на touch-устройствах (coarse pointer) телефон
 * определяется по МЕНЬШЕЙ стороне viewport'а — поэтому поворот телефона
 * в ландшафт НЕ переключает юзера на десктопную страницу (современные
 * iPhone в ландшафте имеют innerWidth 812–932px > 768, из-за чего чистый
 * width-check размонтировал мобильную страницу при повороте).
 *
 * - coarse pointer:  min(w, h) < 768  → телефон стабилен в обеих ориентациях;
 *                    iPad (min-сторона ≥768) — десктоп всегда, как раньше.
 * - fine pointer:    w < 768          → прежнее поведение (узкое окно на
 *                    десктопе = мобильная вёрстка, удобно для dev).
 *
 * Подписки: resize + orientation MQL — на iOS resize после поворота может
 * прийти с задержкой, MQL-событие срабатывает надёжнее.
 */
import { useEffect, useState } from 'react';

/** Touch-устройство (телефон/планшет)? Основной ввод — палец. */
export function isCoarsePointer(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(pointer: coarse)').matches;
}

function computeIsPhone(breakpoint: number): boolean {
  if (typeof window === 'undefined') return false;
  const { innerWidth: w, innerHeight: h } = window;
  // На самом первом рендере innerWidth/innerHeight иногда ещё 0 (layout не
  // готов) — реальный viewport НИКОГДА не бывает 0×0. Без этой проверки
  // 0 < breakpoint даёт ложный isPhone=true → ResponsiveRoute на миг монтирует
  // мобильную страницу поверх десктопного окна (напр. /buffett: мобильный
  // loadData стартует со своим дефолтным period, падает 403 у guest/free,
  // showUpgrade показывает мобильный текст модалки — та не гасится, когда
  // компонент секунду спустя размонтируется обратно в desktop).
  if (w === 0 && h === 0) return false;
  return isCoarsePointer() ? Math.min(w, h) < breakpoint : w < breakpoint;
}

export function useIsPhone(breakpoint = 768): boolean {
  const [isPhone, setIsPhone] = useState(() => computeIsPhone(breakpoint));

  useEffect(() => {
    // Дедуп: resize и MQL-change могут стрельнуть оба на один поворот —
    // setState с тем же значением не вызывает re-render, но обёртка
    // делает намерение явным.
    const handler = () => setIsPhone((prev) => {
      const next = computeIsPhone(breakpoint);
      return next === prev ? prev : next;
    });
    const mql = window.matchMedia('(orientation: landscape)');
    window.addEventListener('resize', handler);
    mql.addEventListener('change', handler);
    return () => {
      window.removeEventListener('resize', handler);
      mql.removeEventListener('change', handler);
    };
  }, [breakpoint]);

  return isPhone;
}
