import { BrowserRouter, Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import ChartPage from './pages/ChartPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/chart/:secId" element={<ChartPage />} />
      </Routes>
    </BrowserRouter>
  );
}