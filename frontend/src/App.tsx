import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider } from './contexts/AuthContext';
import Layout from './components/Layout';
import OverviewPage from './pages/OverviewPage';
import OpenInterestPage from './pages/OpenInterestPage';
import TotalOIPage from './pages/TotalOIPage';
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

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
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
            <Route path="/" element={<OverviewPage />} />
            <Route path="/oi" element={<OpenInterestPage />} />
            <Route path="/oi-total" element={<TotalOIPage />} />
            <Route path="/heatmap" element={<HeatmapPage />} />
            <Route path="/fear" element={<FearIndexPage />} />
            <Route path="/funds" element={<Navigate to="/funds-money" replace />} />
            <Route path="/funds-money" element={<FundsMoneyPage />} />
            <Route path="/funds-catalog" element={<FundsCatalogPage />} />
            <Route path="/buffett" element={<BuffettPage />} />
            <Route path="/strength" element={<StrengthPage />} />
            <Route path="/seasonality" element={<SeasonalityPage />} />
            <Route path="/profile" element={<ProfilePage />} />
          </Route>
        </Routes>
      </BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}
