import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, ExternalLink, Activity, TrendingUp, TrendingDown, LayoutGrid, MessageCircle, BarChart3, Compass } from 'lucide-react';
import SimpleChart from '../components/SimpleChart';
import { getChartData, getFearIndex, getFearIndexHistory } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { FearIndexResponse, FearIndexHistoryResponse } from '../services/api';

// Типы
interface HeatmapStock {
  secId: string;
  name: string;
  change_1m: number;
}

interface TelegramPost {
  id: number;
  title: string;
  preview: string;
  time: string;
}

// Цвета для Fear Index
const FEAR_COLORS: Record<string, string> = {
  'Extreme Greed': '#22c55e',
  'Greed': '#84cc16',
  'Neutral': '#eab308',
  'Fear': '#f97316',
  'Extreme Fear': '#ef4444',
};

const FEAR_LABELS_RU: Record<string, string> = {
  'Extreme Greed': 'Экстремальная жадность',
  'Greed': 'Жадность',
  'Neutral': 'Нейтрально',
  'Fear': 'Страх',
  'Extreme Fear': 'Экстремальный страх',
};

function getFearColorByScore(score: number): string {
  if (score < 25) return FEAR_COLORS['Extreme Greed'];
  if (score < 45) return FEAR_COLORS['Greed'];
  if (score < 55) return FEAR_COLORS['Neutral'];
  if (score < 75) return FEAR_COLORS['Fear'];
  return FEAR_COLORS['Extreme Fear'];
}

export default function OverviewPage() {
  // Fear Index state
  const [fearData, setFearData] = useState<FearIndexResponse | null>(null);
  const [fearHistory, setFearHistory] = useState<FearIndexHistoryResponse | null>(null);
  const [fearLoading, setFearLoading] = useState(true);

  const [heatmapData, setHeatmapData] = useState<HeatmapStock[]>([]);
  const [heatmapLoading, setHeatmapLoading] = useState(true);

  // OI данные
  const [oiLoading, setOiLoading] = useState(true);
  const [oiChartData, setOiChartData] = useState<{ time: string; value: number }[]>([]);
  const [oiBuys, setOiBuys] = useState<{ time: string; value: number }[]>([]);
  const [oiSells, setOiSells] = useState<{ time: string; value: number }[]>([]);

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

  // Загрузка Fear Index
  const loadFearIndex = useCallback(async () => {
    try {
      const [current, history] = await Promise.all([
        getFearIndex(),
        getFearIndexHistory('1m')
      ]);
      setFearData(current);
      setFearHistory(history);
    } catch (err) {
      console.error('Ошибка загрузки Fear Index:', err);
    } finally {
      setFearLoading(false);
    }
  }, []);

  useEffect(() => { loadFearIndex(); }, [loadFearIndex]);

  // Загрузка heatmap (за месяц — более стабильные данные)
  const loadHeatmap = useCallback(async () => {
    try {
      const resp = await fetch('/api/heatmap/stocks?size_by=value_1m&color_by=change_1m&group_by=none');
      const data = await resp.json();
      const stocks = (data.stocks || []).slice(0, 12);
      setHeatmapData(stocks);
    } catch (err) {
      console.error('Ошибка загрузки heatmap:', err);
    } finally {
      setHeatmapLoading(false);
    }
  }, []);

  useEffect(() => { loadHeatmap(); }, [loadHeatmap]);

  // SSE: автоматическое обновление
  useRealtimeData(['5min', 'mv_refresh', 'funds'], useCallback(() => {
    loadFearIndex();
    loadHeatmap();
  }, [loadFearIndex, loadHeatmap]));

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

  // Расчёт изменений Fear Index
  const getYesterdayFear = () => {
    if (!fearHistory?.history?.length || fearHistory.history.length < 2) return null;
    return fearHistory.history[fearHistory.history.length - 2]?.fear_index;
  };

  const fearIndex = fearData?.fear_index ?? 50;
  const fearColor = getFearColorByScore(fearIndex);
  const yesterdayFear = getYesterdayFear();
  const fearChange = yesterdayFear ? fearIndex - yesterdayFear : 0;

  // Цвет для heatmap (для месячных данных — шире диапазон)
  const getHeatmapColor = (change: number) => {
    if (change >= 10) return 'bg-[#16A34A]';
    if (change >= 5) return 'bg-[#22C55E]';
    if (change >= 2) return 'bg-[#4ADE80]';
    if (change >= 0) return 'bg-[#86EFAC]';
    if (change >= -2) return 'bg-[#FCA5A5]';
    if (change >= -5) return 'bg-[#F87171]';
    if (change >= -10) return 'bg-[#EF4444]';
    return 'bg-[#DC2626]';
  };

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-3 bg-gradient-to-br from-[#C8FF2E] to-[#22c55e] rounded-xl">
          <Compass className="w-6 h-6 text-black" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-theme-primary">Обзор рынка</h1>
          <p className="text-theme-secondary text-sm">Аналитика и индикаторы в реальном времени</p>
        </div>
      </div>

      {/* Widgets Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">

        {/* Fear Index Widget — УЛУЧШЕННЫЙ */}
        <Link
          to="/fear"
          className="widget p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          {/* Header */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Activity size={20} style={{ color: fearColor }} />
              <h2 className="text-xl font-semibold text-theme-primary">Индекс страха</h2>
            </div>
            <ArrowRight size={18} className="text-theme-secondary group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          {/* Main Value */}
          <div className="flex-1 flex flex-col items-center justify-center py-8">
            {fearLoading ? (
              <div className="w-10 h-10 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
            ) : (
              <>
                {/* Большое число */}
                <div
                  className="text-8xl font-bold mb-4 transition-all duration-500"
                  style={{ color: fearColor }}
                >
                  {fearIndex.toFixed(0)}
                </div>

                {/* Метка */}
                <div
                  className="text-base font-medium px-4 py-2 rounded-full mb-4"
                  style={{
                    backgroundColor: `${fearColor}22`,
                    color: fearColor,
                    border: `1px solid ${fearColor}44`
                  }}
                >
                  {fearData?.classification ? FEAR_LABELS_RU[fearData.classification] || fearData.classification : '—'}
                </div>

                {/* Изменение */}
                <div className="flex items-center gap-2 text-base">
                  {fearChange > 0 ? (
                    <TrendingUp size={18} className="text-[#ef4444]" />
                  ) : fearChange < 0 ? (
                    <TrendingDown size={18} className="text-[#22c55e]" />
                  ) : null}
                  <span className={fearChange > 0 ? 'text-[#ef4444]' : fearChange < 0 ? 'text-[#22c55e]' : 'text-theme-secondary'}>
                    {fearChange > 0 ? '+' : ''}{fearChange.toFixed(1)} за день
                  </span>
                </div>
              </>
            )}
          </div>

          {/* Scale */}
          <div className="mt-auto">
            <div className="h-3 rounded-full overflow-hidden flex">
              <div className="flex-1 bg-[#22c55e]" />
              <div className="flex-1 bg-[#84cc16]" />
              <div className="flex-1 bg-[#eab308]" />
              <div className="flex-1 bg-[#f97316]" />
              <div className="flex-1 bg-[#ef4444]" />
            </div>
            <div className="flex justify-between mt-2 text-sm text-theme-secondary">
              <span>Жадность</span>
              <span>Страх</span>
            </div>
            {/* Indicator */}
            <div className="relative h-0">
              <div
                className="absolute -top-5 w-1 h-4 bg-white rounded-full transition-all duration-500"
                style={{ left: `${fearIndex}%`, transform: 'translateX(-50%)' }}
              />
            </div>
          </div>
        </Link>

        {/* Market Heatmap Widget */}
        <Link
          to="/heatmap"
          className="widget p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <LayoutGrid size={20} className="text-[#C8FF2E]" />
              <div>
                <h2 className="text-xl font-semibold text-theme-primary">Карта рынка</h2>
                <span className="text-xs text-theme-secondary">За месяц</span>
              </div>
            </div>
            <ArrowRight size={18} className="text-theme-secondary group-hover:text-[#C8FF2E] transition-colors" />
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
                    key={stock.secId}
                    className={`${getHeatmapColor(stock.change_1m)} rounded-lg p-2 text-center aspect-square flex flex-col items-center justify-center shadow-lg`}
                  >
                    <div className="text-white text-sm font-bold drop-shadow-md">{stock.secId}</div>
                    <div className="text-white text-xs font-semibold drop-shadow-md">
                      {stock.change_1m >= 0 ? '+' : ''}{stock.change_1m?.toFixed(1)}%
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="mt-4 pt-4 border-t border-white/10 text-center">
            <span className="text-theme-secondary text-sm">Открыть</span>
          </div>
        </Link>

        {/* Telegram Widget */}
        <div className="widget p-6 flex flex-col">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <MessageCircle size={20} className="text-[#3B82F6]" />
              <h2 className="text-xl font-semibold text-theme-primary">Новости из Telegram</h2>
            </div>
            <a
              href="https://t.me/Thor_INV"
              target="_blank"
              rel="noopener noreferrer"
              className="text-theme-secondary hover:text-[#C8FF2E] transition-colors"
            >
              <ExternalLink size={18} />
            </a>
          </div>

          {/* Posts */}
          <div className="space-y-4 flex-1">
            {telegramPosts.map((post) => (
              <div key={post.id}>
                <h3 className="text-theme-primary font-medium text-sm mb-1">
                  {post.title}
                </h3>
                <p className="text-theme-secondary text-xs line-clamp-2 mb-1">
                  {post.preview}
                </p>
                <span className="text-theme-muted text-xs">{post.time}</span>
              </div>
            ))}
          </div>

          <a
            href="https://t.me/Thor_INV"
            target="_blank"
            rel="noopener noreferrer"
            className="mt-4 pt-4 border-t border-white/10 flex items-center justify-center gap-2 text-theme-secondary text-sm hover:text-[#C8FF2E] transition-colors"
          >
            Открыть канал <ExternalLink size={14} />
          </a>
        </div>
      </div>

      {/* Open Interest Preview */}
      <div className="widget p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <BarChart3 size={24} className="text-[#6366f1]" />
            <div>
              <h2 className="text-xl font-semibold text-theme-primary">Открытый интерес</h2>
              <p className="text-sm text-theme-secondary">Сбербанк (SR) • Физлица • 1 месяц</p>
            </div>
          </div>
          <Link
            to="/oi"
            className="flex items-center gap-2 text-theme-accent hover:text-[#9DCC24] transition-colors text-sm font-medium"
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
          showNavigator={true}
        />
      </div>
    </div>
  );
}