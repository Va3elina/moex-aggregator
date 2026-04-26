/**
 * useIsMobile — единый хук для media-query "mobile" (<768px).
 *
 * Используется компонентами которые не имеют ссылки на ResizeObserver
 * (FlowsHistogram, IndexChart, BreadthChart). Реактивен на resize окна:
 * при пересечении breakpoint компонент перерендерится.
 *
 * Default 768px совпадает с CSS media-query в index.css `@media (max-width: 767px)`,
 * что гарантирует sync между JS-логикой и CSS-vars.
 */
import { useEffect, useState } from 'react';

export function useIsMobile(breakpoint = 768): boolean {
  const [isMobile, setIsMobile] = useState(
    typeof window !== 'undefined' && window.innerWidth < breakpoint,
  );

  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < breakpoint);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, [breakpoint]);

  return isMobile;
}
