import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, ExternalLink } from 'lucide-react';
import SimpleChart from '../components/SimpleChart';
import { getChartData } from '../services/api';

// Типы
interface HeatmapStock {
  sec_id: string;
  name: string;
  change_1d: number;
}

interface TelegramPost {
  id: number;
  title: string;
  preview: string;
  time: string;
}

export default function OverviewPage() {
  const [fearIndex] = useState(62);
  const [yesterdayFear] = useState(58);
  const [weekAgoFear] = useState(45);
  const [heatmapData, setHeatmapData] = useState<HeatmapStock[]>([]);
  const [heatmapLoading, setHeatmapLoading] = useState(true);

  // OI данные
  const [oiLoading, setOiLoading] = useState(true);
  const [oiChartData, setOiChartData] = useState<{time: string; value: number}[]>([]);
  const [oiBuys, setOiBuys] = useState<{time: string; value: number}[]>([]);
  const [oiSells, setOiSells] = useState<{time: string; value: number}[]>([]);

  // Telegram посты (mock)
  const [telegramPosts] = useState<TelegramPost[]>([
    {
      id: 1,
      title: 'Анализ решения ЦБ по ставке',
      preview: 'Разбор влияния на рынок после решения Центробанка. Ключевые наблюдения по ликвидности...',
      time: '2 часа назад'
    },
    {
      id: 2,
      title: 'Нефтегазовый сектор: обзор недели',
      preview: 'Значительные движения в энергетических акциях на фоне глобальных колебаний цен...',
      time: '5 часов назад'
    },
    {
      id: 3,
      title: 'Технологический сектор: смена настроений',
      preview: 'Растущий оптимизм в IT-секторе. YNDX лидирует по объёмам...',
      time: '1 день назад'
    }
  ]);

  // Загрузка heatmap
  useEffect(() => {
    async function loadHeatmap() {
      try {
        const resp = await fetch('/api/heatmap/stocks');
        const data = await resp.json();
        const stocks = (data.stocks || []).slice(0, 12);
        setHeatmapData(stocks);
      } catch (err) {
        console.error('Ошибка загрузки heatmap:', err);
      } finally {
        setHeatmapLoading(false);
      }
    }
    loadHeatmap();
  }, []);

  // Загрузка OI для превью (Сбербанк, 1 месяц) — как на странице OI
  useEffect(() => {
    async function loadOI() {
      try {
        const result = await getChartData('SR', 'SR', 'futures', 24, 'FIZ', true, '1m');

        // Цена
        const chartData = result.candles.map((c: any) => ({
          time: c.time,
          value: c.close,
        }));

        // Покупки (pos_long)
        const buys = result.open_interest?.map((oi: any) => ({
          time: oi.time,
          value: oi.pos_long || 0,
        })) || [];

        // Продажи (abs(pos_short))
        const sells = result.open_interest?.map((oi: any) => ({
          time: oi.time,
          value: Math.abs(oi.pos_short || 0),
        })) || [];

        setOiChartData(chartData);
        setOiBuys(buys);
        setOiSells(sells);
      } catch (err) {
        console.error('Ошибка загрузки OI:', err);
      } finally {
        setOiLoading(false);
      }
    }
    loadOI();
  }, []);

  // Цвет для Fear Index
  const getFearColor = (value: number) => {
    if (value <= 25) return '#FF4D4D';
    if (value <= 45) return '#FF8C00';
    if (value <= 55) return '#FFD700';
    if (value <= 75) return '#C8FF2E';
    return '#2EE59D';
  };

  const getFearText = (value: number) => {
    if (value <= 25) return 'Сильный страх';
    if (value <= 45) return 'Страх';
    if (value <= 55) return 'Нейтрально';
    if (value <= 75) return 'Умеренная жадность';
    return 'Сильная жадность';
  };

  // Цвет для heatmap
  const getHeatmapColor = (change: number) => {
    if (change >= 2) return 'bg-[#22C55E]';
    if (change >= 0.5) return 'bg-[#16A34A]';
    if (change >= 0) return 'bg-[#15803D]';
    if (change >= -0.5) return 'bg-[#991B1B]';
    if (change >= -2) return 'bg-[#DC2626]';
    return 'bg-[#EF4444]';
  };

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-[#F4F6FA] mb-2">Обзор рынка</h1>
        <p className="text-[#A7ADBC]">
          Аналитика и индикаторы настроений российского рынка в реальном времени
        </p>
      </div>

      {/* Widgets Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">

        {/* Fear Index Widget */}
        <Link
          to="/fear"
          className="bg-[#1A1F2E] border border-white/10 rounded-2xl p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-[#F4F6FA]">Индекс страха</h2>
            <ArrowRight size={18} className="text-[#A7ADBC] group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          {/* Circular Gauge */}
          <div className="flex justify-center mb-4 flex-1">
            <div className="relative w-32 h-32">
              <svg className="w-full h-full -rotate-90" viewBox="0 0 100 100">
                <circle cx="50" cy="50" r="40" fill="none" stroke="#2A2F3E" strokeWidth="10" />
                <circle
                  cx="50" cy="50" r="40" fill="none"
                  stroke={getFearColor(fearIndex)}
                  strokeWidth="10"
                  strokeLinecap="round"
                  strokeDasharray={`${(fearIndex / 100) * 251} 251`}
                  className="transition-all duration-1000"
                />
              </svg>
              <div className="absolute inset-0 flex flex-col items-center justify-center">
                <span className="text-3xl font-bold" style={{ color: getFearColor(fearIndex) }}>
                  {fearIndex}
                </span>
              </div>
            </div>
          </div>

          <div className="text-center mb-4">
            <span className="text-sm text-[#A7ADBC]">{getFearText(fearIndex)}</span>
          </div>

          {/* Historical */}
          <div className="space-y-2 text-sm border-t border-white/10 pt-4">
            <div className="flex justify-between">
              <span className="text-[#A7ADBC]">Вчера</span>
              <span className="text-[#F4F6FA] font-medium">{yesterdayFear}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[#A7ADBC]">Неделю назад</span>
              <span className="text-[#F4F6FA] font-medium">{weekAgoFear}</span>
            </div>
          </div>

          <div className="mt-4 pt-4 border-t border-white/10 text-center">
            <span className="text-[#A7ADBC] text-sm">Открыть</span>
          </div>
        </Link>

        {/* Market Heatmap Widget */}
        <Link
          to="/heatmap"
          className="bg-[#1A1F2E] border border-white/10 rounded-2xl p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-[#F4F6FA]">Карта рынка</h2>
            <ArrowRight size={18} className="text-[#A7ADBC] group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          {/* Mini Heatmap */}
          <div className="flex-1 flex items-center">
            {heatmapLoading ? (
              <div className="w-full flex items-center justify-center py-8">
                <div className="w-8 h-8 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
              </div>
            ) : (
              <div className="grid grid-cols-4 gap-2 w-full">
                {heatmapData.map((stock) => (
                  <div
                    key={stock.sec_id}
                    className={`${getHeatmapColor(stock.change_1d)} rounded-lg p-3 text-center aspect-square flex flex-col items-center justify-center`}
                  >
                    <div className="text-white text-xs font-bold">{stock.sec_id}</div>
                    <div className="text-white/80 text-[10px]">
                      {stock.change_1d >= 0 ? '+' : ''}{stock.change_1d?.toFixed(1)}%
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="mt-4 pt-4 border-t border-white/10 text-center">
            <span className="text-[#A7ADBC] text-sm">Открыть</span>
          </div>
        </Link>

        {/* Telegram Widget */}
        <div className="bg-[#1A1F2E] border border-white/10 rounded-2xl p-6 flex flex-col">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-[#F4F6FA]">Новости из Telegram</h2>
            <a
              href="https://t.me/Thor_INV"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[#A7ADBC] hover:text-[#C8FF2E] transition-colors"
            >
              <ExternalLink size={18} />
            </a>
          </div>

          {/* Posts */}
          <div className="space-y-4 flex-1">
            {telegramPosts.map((post) => (
              <div key={post.id}>
                <h3 className="text-[#F4F6FA] font-medium text-sm mb-1">
                  {post.title}
                </h3>
                <p className="text-[#A7ADBC] text-xs line-clamp-2 mb-1">
                  {post.preview}
                </p>
                <span className="text-[#5E6576] text-xs">{post.time}</span>
              </div>
            ))}
          </div>

          <a
            href="https://t.me/Thor_INV"
            target="_blank"
            rel="noopener noreferrer"
            className="mt-4 pt-4 border-t border-white/10 flex items-center justify-center gap-2 text-[#A7ADBC] text-sm hover:text-[#C8FF2E] transition-colors"
          >
            Открыть канал <ExternalLink size={14} />
          </a>
        </div>
      </div>

      {/* Open Interest Preview */}
      <div className="bg-[#1A1F2E] border border-white/10 rounded-2xl p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-xl font-semibold text-[#F4F6FA]">Открытый интерес</h2>
            <p className="text-sm text-[#A7ADBC]">Сбербанк (SR) • Физлица • 1 месяц</p>
          </div>
          <Link
            to="/oi"
            className="flex items-center gap-2 text-[#C8FF2E] hover:text-[#9DCC24] transition-colors text-sm font-medium"
          >
            Полный график <ArrowRight size={16} />
          </Link>
        </div>

        {/* OI Chart Preview — Покупки + Продажи как на странице OI */}
        <SimpleChart
          data={oiChartData}
          secondaryData={oiBuys}
          thirdData={oiSells}
          showSecondary={true}
          showThird={true}
          primaryColor="#6366f1"
          secondaryColor="#2EE59D"
          thirdColor="#FF4D4D"
          height={350}
          loading={oiLoading}
          formatValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
          primaryLabel="Цена"
          secondaryLabel="Покупки"
          thirdLabel="Продажи"
        />
      </div>
    </div>
  );
}