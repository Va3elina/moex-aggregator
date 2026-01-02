import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './componets/Layout';
import HomePage from './pages/HomePage';
import ChartPage from './pages/ChartPage';
import HeatmapPage from './pages/HeatmapPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<HomePage />} />
          <Route path="/heatmap" element={<HeatmapPage />} />
        </Route>
        <Route path="/chart/:secId" element={<ChartPage />} />
      </Routes>
    </BrowserRouter>
  );
}