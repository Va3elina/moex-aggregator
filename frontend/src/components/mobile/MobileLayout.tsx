/**
 * MobileLayout — единый шаблон всех мобильных страниц-индикаторов.
 *
 * Структура:
 *   .fm-app
 *     ├── MobileTopBar       (sticky top)
 *     ├── MobileMain         (scrollable content)
 *     │     └── {children}
 *     └── MobileBottomRail   (fixed bottom)
 *
 * Импортирует mobile.css → стили попадают в split-chunk вместе со
 * страницей, desktop не качает их.
 */
import type { ReactNode } from 'react';
import MobileTopBar from './MobileTopBar';
import MobileBottomRail from './MobileBottomRail';
import '../../styles/mobile.css';

interface MobileLayoutProps {
  children: ReactNode;
}

export default function MobileLayout({ children }: MobileLayoutProps) {
  return (
    <div className="fm-app">
      <MobileTopBar />
      <main className="fm-main">{children}</main>
      <MobileBottomRail />
    </div>
  );
}
