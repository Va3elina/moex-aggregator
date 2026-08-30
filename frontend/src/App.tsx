import { useEffect, lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import ResponsiveRoute from './components/ResponsiveRoute';
import { useIsPhone } from './hooks/useIsPhone';
import { useViewportWidth } from './hooks/useViewportWidth';
import SandboxMobileStub from './pages/sandbox/SandboxMobileStub';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { AnalyticsProvider, AnalyticsPageViewTracker } from './contexts/AnalyticsContext';
import { TierFeaturesProvider } from './contexts/TierFeaturesContext';
import { UpgradePromptProvider } from './components/tier/UpgradeModal';
import { AnomalyProvider } from './contexts/AnomalyContext';
import { ToastHost } from './components/anomaly/ToastHost';
import CookieConsentBanner from './components/CookieConsentBanner';
import PendingRedeemApplier from './components/PendingRedeemApplier';
import FounderOfferBanner from './components/FounderOfferBanner';
import ErrorBoundary from './components/ErrorBoundary';
import Layout from './components/Layout';
import LandingPage from './pages/LandingPage';
import OpenInterestPage from './pages/OpenInterestPage';
import HeatmapPage from './pages/HeatmapPage';
import FundsMoneyPage from './pages/FundsMoneyPage';
import StrengthPage from './pages/StrengthPage';
// MobileStrengthPage — единственная mobile-страница НЕ через lazy(): «Сила рынка» —
// частый nav-таб, а lazy-чанк добавлял сериализованный round-trip (чанк → потом
// fetch данных) ПЕРЕД первым API-запросом. Остальные mobile-страницы остаются lazy.
import MobileStrengthPage from './pages/mobile/MobileStrengthPage';
import BuffettPage from './pages/BuffettPage';
import CbrFlowsPage from './pages/CbrFlowsPage';
import LoginPage from './pages/LoginPage';
import AuthCallback from './pages/AuthCallback';
import ProfilePage from './pages/ProfilePage';
import BillingSuccessPage from './pages/BillingSuccessPage';
import BillingFailPage from './pages/BillingFailPage';
import BillingSbpPage from './pages/BillingSbpPage';
import TrialSuccessPage from './pages/TrialSuccessPage';
import TrialFailPage from './pages/TrialFailPage';
import BillingStubPage from './pages/BillingStubPage';
import BillingUnavailablePage from './pages/BillingUnavailablePage';
import BillingRedeemPage from './pages/BillingRedeemPage';
import OIMethodologyPage from './pages/methodology/OIMethodologyPage';
import HeatmapMethodologyPage from './pages/methodology/HeatmapMethodologyPage';
import FundsMoneyMethodologyPage from './pages/methodology/FundsMoneyMethodologyPage';
import FundsCatalogMethodologyPage from './pages/methodology/FundsCatalogMethodologyPage';
import BuffettMethodologyPage from './pages/methodology/BuffettMethodologyPage';
import StrengthMethodologyPage from './pages/methodology/StrengthMethodologyPage';
import SeasonalityMethodologyPage from './pages/methodology/SeasonalityMethodologyPage';
import CbrFlowsMethodologyPage from './pages/methodology/CbrFlowsMethodologyPage';
import EmbedPage from './pages/embed/EmbedPage';
import PrivacyPage from './pages/PrivacyPage';
import AgreementPage from './pages/legal/AgreementPage';
import OfferPage from './pages/legal/OfferPage';
import RecurringPage from './pages/legal/RecurringPage';
import ContactsPage from './pages/ContactsPage';
import RefundPage from './pages/RefundPage';
import DeliveryPage from './pages/DeliveryPage';
import AddEmailPage from './pages/AddEmailPage';
import VerifyEmailPage from './pages/VerifyEmailPage';
import ForgotPasswordPage from './pages/ForgotPasswordPage';
import FAQPage from './pages/FAQPage';
import GlossaryPage from './pages/GlossaryPage';
import { API_CSV_ENABLED } from './config/features';

// Mobile pages — lazy-loaded, desktop юзеры не качают этот код
const MobileOpenInterestPage = lazy(() => import('./pages/mobile/MobileOpenInterestPage'));
const MobileHeatmapPage = lazy(() => import('./pages/mobile/MobileHeatmapPage'));
const MobileBuffettPage = lazy(() => import('./pages/mobile/MobileBuffettPage'));
const MobileCbrFlowsPage = lazy(() => import('./pages/mobile/MobileCbrFlowsPage'));
const MobileFundsMoneyPage = lazy(() => import('./pages/mobile/MobileFundsMoneyPage'));
const MobileFundTradesPage = lazy(() => import('./pages/mobile/MobileFundTradesPage'));
const MobileSeasonalityPage = lazy(() => import('./pages/mobile/MobileSeasonalityPage'));
const MobileProfilePage = lazy(() => import('./pages/mobile/MobileProfilePage'));
const MobilePricingPage = lazy(() => import('./pages/mobile/MobilePricingPage'));
// Песочница/конструктор — приватный роут /sandbox (вне навигации). Тянет все embed'ы → lazy.
const SandboxPage = lazy(() => import('./pages/sandbox/SandboxPage'));

// Тяжёлые/редкие desktop-страницы — code-split через lazy(), чтобы не тянуть их
// в монолитный главный чанк (был 1.11MB). Гость на лендинге больше не качает код
// ApiDocs/FundTrades/Seasonality/Admin/StylePreview. Грузятся по требованию под
// общим <Suspense> ниже (а пары с мобилкой — ещё и под Suspense в ResponsiveRoute).
const SeasonalityPage = lazy(() => import('./pages/SeasonalityPage'));
const PricingPage = lazy(() => import('./pages/PricingPage'));
const FundTradesPage = lazy(() => import('./pages/FundTradesPage'));
const ApiDocsPage = lazy(() => import('./pages/ApiDocsPage'));
const AdminStatsPage = lazy(() => import('./pages/AdminStatsPage'));
const AdminUserDetailPage = lazy(() => import('./pages/AdminUserDetailPage'));
const AdminContentNewsPage = lazy(() => import('./pages/AdminContentNewsPage'));
// Экспериментальная «Перекраска» (admin-only): % free float, сменивший руки за месяц.
const RepaintPage = lazy(() => import('./pages/RepaintPage'));
const StylePreviewPage = lazy(() => import('./pages/StylePreviewPage'));
// Стенд геометрии графика. ТОЛЬКО dev: в прод-сборке import.meta.env.DEV = false,
// ветка с lazy() выпадает при сборке и на прод не уезжает.
const ChartLabPage = lazy(() => import('./pages/dev/ChartLabPage'));
const SignalExportPage = lazy(() => import('./pages/SignalExportPage'));
// Репо в акциях — экспериментальная вкладка (тест гипотезы «репо ≈ шорты»),
// desktop-only: мобильной версии нет, на телефоне отдаётся fallback-chrome.
const RepoVolumePage = lazy(() => import('./pages/RepoVolumePage'));

/** "/" conditional: auth → карта рынка, guest → Landing. */
function HomeRoute() {
  const { isAuthenticated, loading } = useAuth();
  // useIsPhone (не useIsMobile): телефон в ландшафте остаётся «мобилой» —
  // поворот не должен переключать юзера на десктопные страницы.
  const isMobile = useIsPhone();
  if (loading) return null;
  // На мобиле главной/лендинга нет — все (гости и авторизованные) сразу
  // попадают на карту рынка. Heatmap доступен с free-уровня, так что
  // гость тоже увидит контент, а не маркетинговый лендинг.
  if (isMobile) return <Navigate to="/heatmap" replace />;
  // Авторизованные попадают сразу на карту рынка (дашборд-обзор скрыт),
  // гостям остаётся лендинг с описанием индикаторов.
  if (isAuthenticated) return <Navigate to="/heatmap" replace />;
  return <LandingPage />;
}

/** Роут терминала: узкий экран → заглушка, иначе сам рабочий стол.
 *
 *  Порог 900px, а не useIsPhone: тулбар оболочки перестаёт помещаться задолго
 *  до телефонных ширин (на 375px кнопка «Выстроить» уезжает за край и
 *  недоступна — горизонтального скролла у оболочки нет), так что узкие планшеты
 *  тоже отправляем на заглушку.
 *
 *  Проверка ДО Suspense: на телефоне тяжёлый SandboxPage (тянет за собой все
 *  embed'ы индикаторов) тогда даже не скачивается. */
function SandboxRoute() {
  const vw = useViewportWidth();
  if (vw > 0 && vw < 900) return <SandboxMobileStub />;
  return <Suspense fallback={null}><SandboxPage /></Suspense>;
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
  if (loc.pathname.startsWith('/signal-export') || loc.pathname.startsWith('/embed')) return null;
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
      <AnomalyProvider>
      <AnalyticsProvider>
      <ToastHost />
      <ScrollToTop />
      <PendingRedeemApplier />
      <AnalyticsPageViewTracker />
      <ConditionalCookieBanner />
      <FounderOfferBanner />
      <RouterErrorBoundary>
        <Suspense fallback={<div style={{ minHeight: '100vh', background: 'var(--bg-primary)' }} />}>
        <Routes>
          {/* Auth callback — без Layout */}
          <Route path="/auth/callback/google" element={<AuthCallback />} />
          <Route path="/auth/callback/vk" element={<AuthCallback />} />
          <Route path="/auth/callback/yandex" element={<AuthCallback />} />
          <Route path="/auth/callback/telegram" element={<AuthCallback />} />

          {/* Style preview — standalone без Layout, для оценки нового дизайна */}
          <Route path="/style-preview" element={<StylePreviewPage />} />

          {/* Стенд геометрии графика — dev-only, на проде роут ведёт на главную
              (в SEO_META его нет намеренно: там он и не должен открываться). */}
          <Route
            path="/chart-lab"
            element={import.meta.env.DEV
              ? <Suspense fallback={null}><ChartLabPage /></Suspense>
              : <Navigate to="/" replace />}
          />

          {/* Headless-render для signal-engine — без Layout (только chart+frame) */}
          <Route path="/signal-export" element={<SignalExportPage />} />

          {/* Embed-роуты для встраивания индикаторов в терминал Т-Инвестиций
              (расширение) и как shareable-ссылки на «голый» график. Без Layout.
              План: .claude/TERMINAL_EXTENSION_PLAN.md */}
          <Route path="/embed/:indicator" element={<EmbedPage />} />

          {/* Песочница/конструктор — приватная про-версия (плавающие панели с
              нашими индикаторами). НЕ в навигации; прямой URL /sandbox. Без Layout.
              На узком экране вместо неё заглушка — см. SandboxRoute. */}
          <Route path="/sandbox" element={<SandboxRoute />} />

          {/* Привязка реального email — обязательная страница для OAuth-юзеров
              с synthetic email (Telegram/VK без email). Без Layout — fullscreen
              форма, dismissible через logout. Гейт реализован в EmailSetupGate. */}
          <Route path="/add-email" element={<AddEmailPage />} />

          {/* Подтверждение email кодом из письма (email+password юзеры).
              Без Layout — fullscreen форма. Редирект сюда после регистрации. */}
          <Route path="/verify-email" element={<VerifyEmailPage />} />

          {/* Основное приложение */}
          <Route element={<Layout />}>
            <Route path="/login" element={<LoginPage />} />
            {/* Восстановление пароля — тоже модалка поверх сайта, как вход:
                уходить из модалки на fullscreen-страницу и обратно — разрыв. */}
            <Route path="/forgot-password" element={<ForgotPasswordPage />} />
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
            {/* «Состав фондов» слит в /fund-trades (таб «Состав фондов» + донат). */}
            <Route path="/funds-catalog" element={<Navigate to="/fund-trades" replace />} />
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
            <Route path="/repo" element={<RepoVolumePage />} />
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
            <Route path="/billing/sbp" element={<BillingSbpPage />} />
            <Route path="/billing/fail" element={<BillingFailPage />} />
            <Route path="/billing/trial-success" element={<TrialSuccessPage />} />
            <Route path="/billing/trial-fail" element={<TrialFailPage />} />
            <Route path="/billing/stub" element={<BillingStubPage />} />
            <Route path="/billing/unavailable" element={<BillingUnavailablePage />} />
            <Route path="/billing/redeem" element={<BillingRedeemPage />} />
            {/* Privacy + Legal */}
            <Route path="/privacy" element={<PrivacyPage />} />
            <Route path="/agreement" element={<AgreementPage />} />
            <Route path="/offer" element={<OfferPage />} />
            <Route path="/recurring" element={<RecurringPage />} />
            <Route path="/contacts" element={<ContactsPage />} />
            <Route path="/refund" element={<RefundPage />} />
            <Route path="/delivery" element={<DeliveryPage />} />
            <Route path="/faq" element={<FAQPage />} />
            <Route path="/glossary" element={<GlossaryPage />} />
            {/* /security удалён 2026-05-18, редирект на главную для старых bookmark'ов */}
            <Route path="/security" element={<Navigate to="/" replace />} />
            {/* API docs — KILL-SWITCH: скрыто до официального запуска (config/features.ts) */}
            {API_CSV_ENABLED && <Route path="/api-docs" element={<ApiDocsPage />} />}
            {/* Fund trades — открыт для всех тиров; что покупают/продают БПИФы */}
            <Route path="/fund-trades" element={
              <ResponsiveRoute
                mobile={<MobileFundTradesPage />}
                desktop={<FundTradesPage />}
              />
            } />
            {/* Admin */}
            <Route path="/admin/stats" element={<AdminStatsPage />} />
            <Route path="/admin/users/:userId" element={<AdminUserDetailPage />} />
            <Route path="/admin/content-news" element={<AdminContentNewsPage />} />
            <Route path="/admin/repaint" element={<RepaintPage />} />
          </Route>
        </Routes>
        </Suspense>
      </RouterErrorBoundary>
      </AnalyticsProvider>
      </AnomalyProvider>
      </UpgradePromptProvider>
      </BrowserRouter>
      </TierFeaturesProvider>
      </AuthProvider>
    </ThemeProvider>
  );
}
