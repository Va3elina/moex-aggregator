import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import ErrorBoundary from './components/ErrorBoundary';
import Layout from './components/Layout';
import OverviewPage from './pages/OverviewPage';
import LandingPage from './pages/LandingPage';
import OpenInterestPage from './pages/OpenInterestPage';
import HeatmapPage from './pages/HeatmapPage';
import FearIndexPage from './pages/FearIndexPage';
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

/** "/" conditional: auth → Overview, guest → Landing.
    Loading state → Overview как fallback (быстрее, avoids flash). */
function HomeRoute() {
  const { isAuthenticated, loading } = useAuth();
  if (loading) return null;
  return isAuthenticated ? <OverviewPage /> : <LandingPage />;
}

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
      <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          {/* Auth callback — без Layout */}
          <Route path="/auth/callback/google" element={<AuthCallback />} />
          <Route path="/auth/callback/vk" element={<AuthCallback />} />
          <Route path="/auth/callback/yandex" element={<AuthCallback />} />
          <Route path="/auth/callback/telegram" element={<AuthCallback />} />

          {/* Основное приложение */}
          <Route element={<Layout />}>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<HomeRoute />} />
            <Route path="/oi" element={<OpenInterestPage />} />
            <Route path="/heatmap" element={<HeatmapPage />} />
            <Route path="/fear" element={<FearIndexPage />} />
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
          </Route>
        </Routes>
      </BrowserRouter>
      </ErrorBoundary>
      </AuthProvider>
    </ThemeProvider>
  );
}
