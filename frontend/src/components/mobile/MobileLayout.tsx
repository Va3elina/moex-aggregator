/**
 * MobileLayout — единый шаблон всех мобильных страниц-индикаторов.
 *
 * Структура:
 *   .fm-app
 *     ├── MobileTopBar         (sticky top)
 *     ├── .fm-main             (scrollable content, flex:1)
 *     │     └── {children}
 *     ├── .fm-page-actions     (FIXED, OPTIONAL — page-specific кнопки)
 *     └── MobileBottomRail     (FIXED bottom — навигация по 8 индикаторам)
 *
 * Если `bottomActions` передан — рендерится фиксированный bar между
 * контентом и BottomRail, контент chart-area получает flex:1 и забирает
 * всё доступное место.
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
  /** Опциональный fixed-bar над BottomRail с контекстными action-кнопками
   *  страницы (asset/time/options и т.д.). Когда передан — main padding-bottom
   *  увеличивается, чтобы скролл не упирался в bar. */
  bottomActions?: ReactNode;
}

export default function MobileLayout({ children, bottomActions }: MobileLayoutProps) {
  return (
    <div className="fm-app">
      <MobileTopBar />
      <main className={`fm-main ${bottomActions ? '' : 'no-actions'}`}>{children}</main>
      {bottomActions && <div className="fm-page-actions">{bottomActions}</div>}
      <MobileBottomRail />
    </div>
  );
}
