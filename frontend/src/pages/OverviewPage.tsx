import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, ExternalLink, Activity, TrendingUp, TrendingDown, LayoutGrid, MessageCircle, BarChart3, Compass, Gauge, DollarSign } from 'lucide-react';
import SimpleChart from '../components/SimpleChart';
import { getChartData, getFearIndex, getFearIndexHistory, getBreadthCurrent, getFundsSummary, getBuffettCapGdp } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { FearIndexResponse, FearIndexHistoryResponse, BreadthCurrentResponse, FundsSummaryResponse } from '../services/api';
import { FEAR_LABELS_RU, getFearColor } from '../config/fearConfig';

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

  // Сила рынка
  const [breadthData, setBreadthData] = useState<BreadthCurrentResponse | null>(null);
  const [breadthLoading, setBreadthLoading] = useState(true);

  // Фонды
  const [fundsSummary, setFundsSummary] = useState<FundsSummaryResponse | null>(null);
  const [fundsLoading, setFundsLoading] = useState(true);

  // Баффетт
  const [buffettRatio, setBuffettRatio] = useState<number | null>(null);
  const [buffettLoading, setBuffettLoading] = useState(true);

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

  // Загрузка Сила рынка
  useEffect(() => {
    getBreadthCurrent(200)
      .then(data => setBreadthData(data))
      .catch(err => console.error('Ошибка загрузки breadth:', err))
      .finally(() => setBreadthLoading(false));
  }, []);

  // Загрузка Фонды
  useEffect(() => {
    getFundsSummary()
      .then(data => setFundsSummary(data))
      .catch(err => console.error('Ошибка загрузки фондов:', err))
      .finally(() => setFundsLoading(false));
  }, []);

  // Загрузка Баффетт
  useEffect(() => {
    getBuffettCapGdp('1y', true)
      .then(data => {
        const last = data.data?.[data.data.length - 1];
        if (last) setBuffettRatio(last.buffett);
      })
      .catch(err => console.error('Ошибка загрузки Баффетт:', err))
      .finally(() => setBuffettLoading(false));
  }, []);

  // Расчёт изменений Fear Index
  const getYesterdayFear = () => {
    if (!fearHistory?.history?.length || fearHistory.history.length < 2) return null;
    return fearHistory.history[fearHistory.history.length - 2]?.fear_index;
  };

  const fearIndex = fearData?.fear_index ?? 50;
  const fearColor = getFearColor(fearIndex);
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
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8">
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

      {/* Вторая строка виджетов — Сила рынка, Фонды, Баффетт */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Сила рынка */}
        <Link
          to="/strength"
          className="widget p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <TrendingUp size={20} className="text-[#8b5cf6]" />
              <h2 className="text-xl font-semibold text-theme-primary">Сила рынка</h2>
            </div>
            <ArrowRight size={18} className="text-theme-secondary group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          <div className="flex-1 flex flex-col items-center justify-center py-6">
            {breadthLoading ? (
              <div className="w-10 h-10 border-2 border-[#8b5cf6] border-t-transparent rounded-full animate-spin" />
            ) : breadthData ? (
              <>
                <div className="text-7xl font-bold mb-3" style={{
                  color: breadthData.percent_above >= 70 ? '#22c55e'
                    : breadthData.percent_above >= 50 ? '#4ade80'
                    : breadthData.percent_above >= 30 ? '#fbbf24'
                    : '#ef4444'
                }}>
                  {breadthData.percent_above.toFixed(0)}%
                </div>
                <div className="text-theme-secondary text-sm mb-3">акций выше EMA200</div>
                <div className="flex items-center gap-4 text-sm">
                  <span className="text-green-400 font-medium">{breadthData.count_above} выше</span>
                  <span className="text-red-400 font-medium">{breadthData.count_total - breadthData.count_above} ниже</span>
                </div>
              </>
            ) : (
              <span className="text-theme-muted">Нет данных</span>
            )}
          </div>

          {breadthData && (
            <div className="mt-auto">
              <div className="h-2 rounded-full overflow-hidden bg-theme-tertiary">
                <div
                  className="h-full rounded-full transition-all duration-500"
                  style={{
                    width: `${breadthData.percent_above}%`,
                    background: breadthData.percent_above >= 70 ? 'linear-gradient(90deg, #22c55e, #4ade80)'
                      : breadthData.percent_above >= 50 ? 'linear-gradient(90deg, #4ade80, #86efac)'
                      : breadthData.percent_above >= 30 ? 'linear-gradient(90deg, #fbbf24, #fcd34d)'
                      : 'linear-gradient(90deg, #ef4444, #f87171)'
                  }}
                />
              </div>
              <div className="flex justify-between mt-2 text-xs text-theme-muted">
                <span>Перепроданность</span>
                <span>Перекупленность</span>
              </div>
            </div>
          )}
        </Link>

        {/* Фонды */}
        <Link
          to="/funds-money"
          className="widget p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <DollarSign size={20} className="text-[#2EE59D]" />
              <h2 className="text-xl font-semibold text-theme-primary">Фонды</h2>
            </div>
            <ArrowRight size={18} className="text-theme-secondary group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          <div className="flex-1">
            {fundsLoading ? (
              <div className="flex items-center justify-center py-8">
                <div className="w-8 h-8 border-2 border-[#2EE59D] border-t-transparent rounded-full animate-spin" />
              </div>
            ) : fundsSummary?.categories ? (
              <div className="space-y-3">
                {fundsSummary.categories.map((cat) => (
                  <div key={cat.name} className="flex items-center justify-between p-3 rounded-lg bg-theme-tertiary/50">
                    <div>
                      <div className="text-theme-primary text-sm font-medium">{cat.name}</div>
                      <div className="text-theme-muted text-xs">{cat.funds_count} фондов</div>
                    </div>
                    <div className="text-right">
                      <div className="text-theme-primary text-sm font-semibold">
                        {(cat.total_nav / 1e9).toFixed(0)} млрд ₽
                      </div>
                      <div className={`text-xs font-medium ${cat.change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {cat.change_pct >= 0 ? '+' : ''}{cat.change_pct.toFixed(2)}%
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <span className="text-theme-muted">Нет данных</span>
            )}
          </div>

          <div className="mt-4 pt-4 border-t border-white/10 text-center">
            <span className="text-theme-secondary text-sm">Открыть</span>
          </div>
        </Link>

        {/* Индикатор Баффетта */}
        <Link
          to="/buffett"
          className="widget p-6 hover:border-[#C8FF2E]/30 transition-all group flex flex-col"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Gauge size={20} className="text-[#f59e0b]" />
              <h2 className="text-xl font-semibold text-theme-primary">Индикатор Баффетта</h2>
            </div>
            <ArrowRight size={18} className="text-theme-secondary group-hover:text-[#C8FF2E] transition-colors" />
          </div>

          <div className="flex-1 flex flex-col items-center justify-center py-6">
            {buffettLoading ? (
              <div className="w-10 h-10 border-2 border-[#f59e0b] border-t-transparent rounded-full animate-spin" />
            ) : buffettRatio !== null ? (
              <>
                <div className="text-7xl font-bold mb-3" style={{
                  color: buffettRatio > 100 ? '#ef4444'
                    : buffettRatio > 75 ? '#f59e0b'
                    : '#22c55e'
                }}>
                  {buffettRatio.toFixed(0)}%
                </div>
                <div className="text-theme-secondary text-sm mb-3">Капитализация / ВВП</div>
                <div className="px-4 py-2 rounded-full text-xs font-medium" style={{
                  backgroundColor: buffettRatio > 100 ? '#ef444422' : buffettRatio > 75 ? '#f59e0b22' : '#22c55e22',
                  color: buffettRatio > 100 ? '#ef4444' : buffettRatio > 75 ? '#f59e0b' : '#22c55e',
                  border: `1px solid ${buffettRatio > 100 ? '#ef444444' : buffettRatio > 75 ? '#f59e0b44' : '#22c55e44'}`
                }}>
                  {buffettRatio > 100 ? 'Рынок переоценён'
                    : buffettRatio > 75 ? 'Справедливая оценка'
                    : 'Рынок недооценён'}
                </div>
              </>
            ) : (
              <span className="text-theme-muted">Нет данных</span>
            )}
          </div>

          <div className="mt-auto">
            <div className="flex items-center gap-2 text-xs text-theme-muted">
              <div className="flex-1 h-2 rounded-full overflow-hidden flex">
                <div className="flex-1 bg-[#22c55e]" />
                <div className="flex-1 bg-[#f59e0b]" />
                <div className="flex-1 bg-[#ef4444]" />
              </div>
            </div>
            <div className="flex justify-between mt-2 text-xs text-theme-muted">
              <span>Недооценён</span>
              <span>Переоценён</span>
            </div>
          </div>
        </Link>
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