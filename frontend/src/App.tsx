import { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { AnalyticsProvider, AnalyticsPageViewTracker } from './contexts/AnalyticsContext';
import CookieConsentBanner from './components/CookieConsentBanner';
import ErrorBoundary from './components/ErrorBoundary';
import Layout from './components/Layout';
import OverviewPage from './pages/OverviewPage';
import LandingPage from './pages/LandingPage';
import OpenInterestPage from './pages/OpenInterestPage';
import HeatmapPage from './pages/HeatmapPage';
import FundsMoneyPage from './pages/FundsMoneyPage';
import StrengthPage from './pages/StrengthPage';
import BuffettPage from './pages/BuffettPage';
import SeasonalityPage from './pages/SeasonalityPage';
import FundsCatalogPage from './pages/FundsCatalogPage';
import LoginPage from './pages/LoginPage';
import AuthCallback from './pages/AuthCallback';
import ProfilePage from './pages/ProfilePage';
import PricingPage from './pages/PricingPage';
import BillingSuccessPage from './pages/BillingSuccessPage';
import BillingStubPage from './pages/BillingStubPage';
import BillingRedeemPage from './pages/BillingRedeemPage';
import OIMethodologyPage from './pages/methodology/OIMethodologyPage';
import HeatmapMethodologyPage from './pages/methodology/HeatmapMethodologyPage';
import FundsMoneyMethodologyPage from './pages/methodology/FundsMoneyMethodologyPage';
import FundsCatalogMethodologyPage from './pages/methodology/FundsCatalogMethodologyPage';
import BuffettMethodologyPage from './pages/methodology/BuffettMethodologyPage';
import StrengthMethodologyPage from './pages/methodology/StrengthMethodologyPage';
import SeasonalityMethodologyPage from './pages/methodology/SeasonalityMethodologyPage';
import StylePreviewPage from './pages/StylePreviewPage';
import PrivacyPage from './pages/PrivacyPage';
import AdminStatsPage from './pages/AdminStatsPage';
import AdminUserDetailPage from './pages/AdminUserDetailPage';

/** "/" conditional: auth → Overview, guest → Landing.
    Loading state → Overview как fallback (быстрее, avoids flash). */
function HomeRoute() {
  const { isAuthenticated, loading } = useAuth();
  if (loading) return null;
  return isAuthenticated ? <OverviewPage /> : <LandingPage />;
}

/** ErrorBoundary с автосбросом на смене URL.
    Если страница падает — навигация на другой роут восстановит boundary
    автоматически. Без этого hasError=true залипает навсегда. */
function RouterErrorBoundary({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  return <ErrorBoundary resetKey={location.pathname}>{children}</ErrorBoundary>;
}

/** Auto scroll-to-top на смене pathname.
    React Router 6 не делает этого автоматически. Без скролла:
    - пользователь видит низ предыдущей страницы наложенный на новую
    - useFitToViewport получает отрицательный anchor.top → растягивает chart
    - вообще теряется ощущение «новая страница» */
function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => {
    window.scrollTo(0, 0);
  }, [pathname]);
  return null;
}

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
      <BrowserRouter>
      <AnalyticsProvider>
      <ScrollToTop />
      <AnalyticsPageViewTracker />
      <CookieConsentBanner />
      <RouterErrorBoundary>
        <Routes>
          {/* Auth callback — без Layout */}
          <Route path="/auth/callback/google" element={<AuthCallback />} />
          <Route path="/auth/callback/vk" element={<AuthCallback />} />
          <Route path="/auth/callback/yandex" element={<AuthCallback />} />
          <Route path="/auth/callback/telegram" element={<AuthCallback />} />

          {/* Style preview — standalone без Layout, для оценки нового дизайна */}
          <Route path="/style-preview" element={<StylePreviewPage />} />

          {/* Основное приложение */}
          <Route element={<Layout />}>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<HomeRoute />} />
            <Route path="/oi" element={<OpenInterestPage />} />
            <Route path="/heatmap" element={<HeatmapPage />} />
            <Route path="/funds" element={<Navigate to="/funds-money" replace />} />
            <Route path="/funds-money" element={<FundsMoneyPage />} />
            <Route path="/funds-catalog" element={<FundsCatalogPage />} />
            <Route path="/buffett" element={<BuffettPage />} />
            <Route path="/strength" element={<StrengthPage />} />
            <Route path="/seasonality" element={<SeasonalityPage />} />
            {/* Методология индикаторов */}
            <Route path="/methodology/oi" element={<OIMethodologyPage />} />
            <Route path="/methodology/heatmap" element={<HeatmapMethodologyPage />} />
            <Route path="/methodology/funds-money" element={<FundsMoneyMethodologyPage />} />
            <Route path="/methodology/funds-catalog" element={<FundsCatalogMethodologyPage />} />
            <Route path="/methodology/buffett" element={<BuffettMethodologyPage />} />
            <Route path="/methodology/strength" element={<StrengthMethodologyPage />} />
            <Route path="/methodology/seasonality" element={<SeasonalityMethodologyPage />} />
            <Route path="/profile" element={<ProfilePage />} />
            {/* Billing */}
            <Route path="/pricing" element={<PricingPage />} />
            <Route path="/billing/success" element={<BillingSuccessPage />} />
            <Route path="/billing/stub" element={<BillingStubPage />} />
            <Route path="/billing/redeem" element={<BillingRedeemPage />} />
            {/* Privacy */}
            <Route path="/privacy" element={<PrivacyPage />} />
            {/* Admin */}
            <Route path="/admin/stats" element={<AdminStatsPage />} />
            <Route path="/admin/users/:userId" element={<AdminUserDetailPage />} />
          </Route>
        </Routes>
      </RouterErrorBoundary>
      </AnalyticsProvider>
      </BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}
