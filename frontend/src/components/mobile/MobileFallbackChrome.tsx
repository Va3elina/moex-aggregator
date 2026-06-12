/**
 * MobileFallbackChrome — облегчённый мобильный chrome для маршрутов
 * БЕЗ собственной мобильной версии (/login, /faq, legal, billing,
 * methodology, admin). Такие страницы рендерят десктопную вёрстку,
 * но без этого wrapper'а на мобиле у них не было ни top-bar'а,
 * ни кнопки «Назад» (см. аудит .claude/MOBILE_AUDIT_2026-06-12.md).
 *
 * Отличие от MobileLayout: нет bottom-rail (навигация по индикаторам
 * на сервисных страницах неуместна), нет page-actions, и main —
 * обычный page-scroll (.fm-fallback-main), а не fixed-app паттерн:
 * десктопный контент длиннее viewport'а.
 */
import type { ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import MobileTopBar from './MobileTopBar';
import '../../styles/mobile.css';

export default function MobileFallbackChrome({ children }: { children: ReactNode }) {
  const navigate = useNavigate();

  const handleBack = () => {
    // React Router кладёт индекс в history.state.idx — если мы первая
    // запись (прямой заход по ссылке), «Назад» ведёт на главную.
    const idx = (window.history.state as { idx?: number } | null)?.idx ?? 0;
    if (idx > 0) navigate(-1);
    else navigate('/');
  };

  return (
    <div className="fm-app">
      <MobileTopBar onBack={handleBack} />
      <main className="fm-fallback-main">{children}</main>
    </div>
  );
}
