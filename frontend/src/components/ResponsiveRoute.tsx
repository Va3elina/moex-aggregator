/**
 * ResponsiveRoute — рендерит mobile или desktop компонент в зависимости
 * от ширины viewport. Mobile-вариант lazy-loaded через React.lazy →
 * desktop-юзеры не качают мобильный код.
 *
 * Usage:
 *   const MobileOI = lazy(() => import('./pages/mobile/MobileOpenInterestPage'));
 *   <Route path="/oi" element={
 *     <ResponsiveRoute mobile={<MobileOI />} desktop={<OpenInterestPage />} />
 *   } />
 */
import { Suspense } from 'react';
import type { ReactNode } from 'react';
import { useIsMobile } from '../hooks/useIsMobile';

interface ResponsiveRouteProps {
  mobile: ReactNode;
  desktop: ReactNode;
  /** Fallback при загрузке lazy mobile-компонента. По умолчанию — пустой div. */
  fallback?: ReactNode;
}

export default function ResponsiveRoute({
  mobile,
  desktop,
  fallback,
}: ResponsiveRouteProps) {
  const isMobile = useIsMobile();

  if (isMobile) {
    return (
      <Suspense fallback={fallback ?? <div style={{ minHeight: '100vh', background: 'var(--bg-primary)' }} />}>
        {mobile}
      </Suspense>
    );
  }

  return <>{desktop}</>;
}
