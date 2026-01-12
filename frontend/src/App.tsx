import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import OverviewPage from './pages/OverviewPage';
import OpenInterestPage from './pages/OpenInterestPage';
import TotalOIPage from './pages/TotalOIPage';
import HeatmapPage from './pages/HeatmapPage';
import FearIndexPage from './pages/FearIndexPage';
import FundsFlowPage from './pages/FundsFlowPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/oi" element={<OpenInterestPage />} />
          <Route path="/oi-total" element={<TotalOIPage />} />
          <Route path="/heatmap" element={<HeatmapPage />} />
          <Route path="/fear" element={<FearIndexPage />} />
          <Route path="/funds" element={<FundsFlowPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}