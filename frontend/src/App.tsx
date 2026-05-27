import { useEffect, lazy } from 'react';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import ResponsiveRoute from './components/ResponsiveRoute';
import { useIsMobile } from './hooks/useIsMobile';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { AnalyticsProvider, AnalyticsPageViewTracker } from './contexts/AnalyticsContext';
import { TierFeaturesProvider } from './contexts/TierFeaturesContext';
import { UpgradePromptProvider } from './components/tier/UpgradeModal';
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
import CbrFlowsPage from './pages/CbrFlowsPage';
import LoginPage from './pages/LoginPage';
import AuthCallback from './pages/AuthCallback';
import ProfilePage from './pages/ProfilePage';
import PricingPage from './pages/PricingPage';
import BillingSuccessPage from './pages/BillingSuccessPage';
import BillingFailPage from './pages/BillingFailPage';
import BillingStubPage from './pages/BillingStubPage';
import BillingRedeemPage from './pages/BillingRedeemPage';
import OIMethodologyPage from './pages/methodology/OIMethodologyPage';
import HeatmapMethodologyPage from './pages/methodology/HeatmapMethodologyPage';
import FundsMoneyMethodologyPage from './pages/methodology/FundsMoneyMethodologyPage';
import FundsCatalogMethodologyPage from './pages/methodology/FundsCatalogMethodologyPage';
import BuffettMethodologyPage from './pages/methodology/BuffettMethodologyPage';
import StrengthMethodologyPage from './pages/methodology/StrengthMethodologyPage';
import SeasonalityMethodologyPage from './pages/methodology/SeasonalityMethodologyPage';
import CbrFlowsMethodologyPage from './pages/methodology/CbrFlowsMethodologyPage';
import StylePreviewPage from './pages/StylePreviewPage';
import SignalExportPage from './pages/SignalExportPage';
import PrivacyPage from './pages/PrivacyPage';
import AgreementPage from './pages/legal/AgreementPage';
import OfferPage from './pages/legal/OfferPage';
import RecurringPage from './pages/legal/RecurringPage';
import ContactsPage from './pages/ContactsPage';
import RefundPage from './pages/RefundPage';
import DeliveryPage from './pages/DeliveryPage';
import AdminStatsPage from './pages/AdminStatsPage';
import AdminUserDetailPage from './pages/AdminUserDetailPage';
import ApiDocsPage from './pages/ApiDocsPage';
import FundTradesPage from './pages/FundTradesPage';
import AddEmailPage from './pages/AddEmailPage';

// Mobile pages — lazy-loaded, desktop юзеры не качают этот код
const MobileOpenInterestPage = lazy(() => import('./pages/mobile/MobileOpenInterestPage'));
const MobileHeatmapPage = lazy(() => import('./pages/mobile/MobileHeatmapPage'));
const MobileBuffettPage = lazy(() => import('./pages/mobile/MobileBuffettPage'));
const MobileCbrFlowsPage = lazy(() => import('./pages/mobile/MobileCbrFlowsPage'));
const MobileFundsMoneyPage = lazy(() => import('./pages/mobile/MobileFundsMoneyPage'));
const MobileSeasonalityPage = lazy(() => import('./pages/mobile/MobileSeasonalityPage'));
const MobileStrengthPage = lazy(() => import('./pages/mobile/MobileStrengthPage'));
const MobileFundsCatalogPage = lazy(() => import('./pages/mobile/MobileFundsCatalogPage'));
const MobileProfilePage = lazy(() => import('./pages/mobile/MobileProfilePage'));
const MobilePricingPage = lazy(() => import('./pages/mobile/MobilePricingPage'));

/** "/" conditional: auth → Overview, guest → Landing.
    Loading state → Overview как fallback (быстрее, avoids flash). */
function HomeRoute() {
  const { isAuthenticated, loading } = useAuth();
  const isMobile = useIsMobile();
  if (loading) return null;
  // На мобиле главной/лендинга нет — все (гости и авторизованные) сразу
  // попадают на карту рынка. Heatmap доступен с free-уровня, так что
  // гость тоже увидит контент, а не маркетинговый лендинг.
  if (isMobile) return <Navigate to="/heatmap" replace />;
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

/**
 * Banner показываем на всех маршрутах **кроме** /signal-export — там
 * headless Playwright рендерит чарт для Telegram-постов и cookie-overlay
 * только мешает (закрывает footer, попадает в скриншот). useLocation()
 * требует жить внутри <BrowserRouter>, поэтому wrapper рендерим там же,
 * где раньше был CookieConsentBanner.
 */
function ConditionalCookieBanner() {
  const loc = useLocation();
  if (loc.pathname.startsWith('/signal-export')) return null;
  return <CookieConsentBanner />;
}

/**
 * Раньше здесь жил EmailSetupGate — глобальный redirect на /add-email для
 * OAuth-юзеров с synthetic email (`*@oauth.local`). Поведение оказалось
 * слишком жёстким (юзер мог даже бесплатные индикаторы посмотреть только
 * после ввода email). Сейчас защита перенесена В МОМЕНТ ОПЛАТЫ:
 * ConsentModal в PricingPage показывает обязательное email-поле перед
 * checkout'ом если requires_email_setup=true. Без оплаты email — опциональный.
 */

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
      <TierFeaturesProvider>
      <BrowserRouter>
      <UpgradePromptProvider>
      <AnalyticsProvider>
      <ScrollToTop />
      <AnalyticsPageViewTracker />
      <ConditionalCookieBanner />
      <RouterErrorBoundary>
        <Routes>
          {/* Auth callback — без Layout */}
          <Route path="/auth/callback/google" element={<AuthCallback />} />
          <Route path="/auth/callback/vk" element={<AuthCallback />} />
          <Route path="/auth/callback/yandex" element={<AuthCallback />} />
          <Route path="/auth/callback/telegram" element={<AuthCallback />} />

          {/* Style preview — standalone без Layout, для оценки нового дизайна */}
          <Route path="/style-preview" element={<StylePreviewPage />} />

          {/* Headless-render для signal-engine — без Layout (только chart+frame) */}
          <Route path="/signal-export" element={<SignalExportPage />} />

          {/* Привязка реального email — обязательная страница для OAuth-юзеров
              с synthetic email (Telegram/VK без email). Без Layout — fullscreen
              форма, dismissible через logout. Гейт реализован в EmailSetupGate. */}
          <Route path="/add-email" element={<AddEmailPage />} />

          {/* Основное приложение */}
          <Route element={<Layout />}>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<HomeRoute />} />
            <Route path="/oi" element={
              <ResponsiveRoute
                mobile={<MobileOpenInterestPage />}
                desktop={<OpenInterestPage />}
              />
            } />
            <Route path="/heatmap" element={
              <ResponsiveRoute
                mobile={<MobileHeatmapPage />}
                desktop={<HeatmapPage />}
              />
            } />
            <Route path="/funds" element={<Navigate to="/funds-money" replace />} />
            <Route path="/funds-money" element={
              <ResponsiveRoute
                mobile={<MobileFundsMoneyPage />}
                desktop={<FundsMoneyPage />}
              />
            } />
            <Route path="/funds-catalog" element={
              <ResponsiveRoute
                mobile={<MobileFundsCatalogPage />}
                desktop={<FundsCatalogPage />}
              />
            } />
            <Route path="/buffett" element={
              <ResponsiveRoute
                mobile={<MobileBuffettPage />}
                desktop={<BuffettPage />}
              />
            } />
            <Route path="/strength" element={
              <ResponsiveRoute
                mobile={<MobileStrengthPage />}
                desktop={<StrengthPage />}
              />
            } />
            <Route path="/seasonality" element={
              <ResponsiveRoute
                mobile={<MobileSeasonalityPage />}
                desktop={<SeasonalityPage />}
              />
            } />
            <Route path="/cbr-flows" element={
              <ResponsiveRoute
                mobile={<MobileCbrFlowsPage />}
                desktop={<CbrFlowsPage />}
              />
            } />
            {/* Методология индикаторов */}
            <Route path="/methodology/oi" element={<OIMethodologyPage />} />
            <Route path="/methodology/heatmap" element={<HeatmapMethodologyPage />} />
            <Route path="/methodology/funds-money" element={<FundsMoneyMethodologyPage />} />
            <Route path="/methodology/funds-catalog" element={<FundsCatalogMethodologyPage />} />
            <Route path="/methodology/buffett" element={<BuffettMethodologyPage />} />
            <Route path="/methodology/strength" element={<StrengthMethodologyPage />} />
            <Route path="/methodology/seasonality" element={<SeasonalityMethodologyPage />} />
            <Route path="/methodology/cbr-flows" element={<CbrFlowsMethodologyPage />} />
            <Route path="/profile" element={
              <ResponsiveRoute
                mobile={<MobileProfilePage />}
                desktop={<ProfilePage />}
              />
            } />
            {/* Billing */}
            <Route path="/pricing" element={
              <ResponsiveRoute
                mobile={<MobilePricingPage />}
                desktop={<PricingPage />}
              />
            } />
            <Route path="/billing/success" element={<BillingSuccessPage />} />
            <Route path="/billing/fail" element={<BillingFailPage />} />
            <Route path="/billing/stub" element={<BillingStubPage />} />
            <Route path="/billing/redeem" element={<BillingRedeemPage />} />
            {/* Privacy + Legal */}
            <Route path="/privacy" element={<PrivacyPage />} />
            <Route path="/agreement" element={<AgreementPage />} />
            <Route path="/offer" element={<OfferPage />} />
            <Route path="/recurring" element={<RecurringPage />} />
            <Route path="/contacts" element={<ContactsPage />} />
            <Route path="/refund" element={<RefundPage />} />
            <Route path="/delivery" element={<DeliveryPage />} />
            {/* /security удалён 2026-05-18, редирект на главную для старых bookmark'ов */}
            <Route path="/security" element={<Navigate to="/" replace />} />
            {/* API docs (Pro tier feature) — публичная страница, доступ без auth */}
            <Route path="/api-docs" element={<ApiDocsPage />} />
            {/* Fund trades (Pro tier) — что покупают/продают БПИФы */}
            <Route path="/fund-trades" element={<FundTradesPage />} />
            {/* Admin */}
            <Route path="/admin/stats" element={<AdminStatsPage />} />
            <Route path="/admin/users/:userId" element={<AdminUserDetailPage />} />
          </Route>
        </Routes>
      </RouterErrorBoundary>
      </AnalyticsProvider>
      </UpgradePromptProvider>
      </BrowserRouter>
      </TierFeaturesProvider>
      </AuthProvider>
    </ThemeProvider>
  );
}
