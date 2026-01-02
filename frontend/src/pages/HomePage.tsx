import { useEffect, useState, useRef } from 'react';
import { Link } from 'react-router-dom';
import {
  getInstruments,
  getStats
} from '../services/api';
import type { Instrument, StatsResponse } from '../types';

// Иконки
const IconActivity = () => (
  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
  </svg>
);

const IconTrending = () => (
  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
  </svg>
);

const IconChart = () => (
  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
  </svg>
);

const IconUsers = () => (
  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
  </svg>
);

interface UniqueInstrument {
  sectype: string;
  name: string;
  type: string | null;
  group: string | null;
  contracts: Instrument[];
}

type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d': '1Д',
  '1w': '1Н',
  '1m': '1М',
  '3m': '3М',
  '6m': '6М',
  '1y': '1Г',
  'all': 'Всё'
};

// Форматирование больших чисел
function formatNumber(num: number): string {
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
  return num.toLocaleString('ru-RU');
}

// Компонент статистической карточки
function StatsCard({
  title,
  value,
  change,
  icon: Icon,
  loading = false,
}: {
  title: string;
  value: string;
  change?: number;
  icon: React.FC;
  loading?: boolean;
}) {
  const isPositive = change !== undefined && change >= 0;

  if (loading) {
    return (
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-2xl p-5 border border-slate-700/50">
        <div className="animate-pulse">
          <div className="h-4 w-32 bg-slate-700 rounded mb-3" />
          <div className="h-8 w-24 bg-slate-700 rounded mb-2" />
          <div className="h-4 w-16 bg-slate-700 rounded" />
        </div>
      </div>
    );
  }

  return (
    <div className="bg-slate-800/50 backdrop-blur-sm rounded-2xl p-5 border border-slate-700/50 hover:border-emerald-500/30 transition-all">
      <div className="flex items-center justify-between mb-3">
        <span className="text-slate-400 text-sm font-medium uppercase tracking-wide">{title}</span>
        <div className="w-10 h-10 rounded-xl bg-slate-700/50 flex items-center justify-center text-slate-400">
          <Icon />
        </div>
      </div>
      <div className="text-3xl font-bold text-white mb-1">
        {value}
      </div>
      {change !== undefined && (
        <div className={`inline-flex items-center px-2 py-0.5 rounded-lg text-sm font-medium ${
          isPositive ? 'text-emerald-400 bg-emerald-500/10' : 'text-rose-400 bg-rose-500/10'
        }`}>
          {isPositive ? '↑' : '↓'} {Math.abs(change).toFixed(2)}%
        </div>
      )}
    </div>
  );
}

// Интерактивный график OI (полная ширина)
function InteractiveOIChart({
  data,
  title,
  loading = false,
}: {
  data: { date: string; total_oi: number }[];
  title: string;
  loading?: boolean;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);
  const height = 320;
  const [tooltip, setTooltip] = useState<{
    visible: boolean;
    x: number;
    y: number;
    value: number;
    date: string;
  }>({ visible: false, x: 0, y: 0, value: 0, date: '' });
  const [animated, setAnimated] = useState(false);

  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect();
        if (rect.width > 0) {
          setWidth(rect.width);
        }
      }
    };

    updateWidth();
    const timer1 = setTimeout(updateWidth, 50);
    const timer2 = setTimeout(updateWidth, 200);

    window.addEventListener('resize', updateWidth);

    return () => {
      window.removeEventListener('resize', updateWidth);
      clearTimeout(timer1);
      clearTimeout(timer2);
    };
  }, [data]);

  useEffect(() => {
    if (data.length > 0 && !loading) {
      setAnimated(false);
      const timer = setTimeout(() => setAnimated(true), 50);
      return () => clearTimeout(timer);
    }
  }, [data, loading]);

  if (loading) {
    return (
      <div className="bg-slate-800/30 backdrop-blur-sm rounded-2xl p-6 border border-slate-700/50">
        <div className="h-4 w-48 bg-slate-700 rounded mb-6 animate-pulse" />
        <div className="h-80 bg-slate-700/30 rounded-xl animate-pulse" />
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="bg-slate-800/30 backdrop-blur-sm rounded-2xl p-6 border border-slate-700/50">
        <h3 className="text-white font-semibold mb-4">{title}</h3>
        <div className="h-80 flex items-center justify-center text-slate-500">
          Нет данных
        </div>
      </div>
    );
  }

  const padding = { top: 20, right: 20, bottom: 50, left: 70 };
  const chartWidth = Math.max(width - padding.left - padding.right, 100);
  const chartHeight = height - padding.top - padding.bottom;

  const maxOI = Math.max(...data.map(d => d.total_oi));
  const minOI = Math.min(...data.map(d => d.total_oi));
  const range = maxOI - minOI || 1;
  const yPadding = range * 0.1;
  const yMin = minOI - yPadding;
  const yMax = maxOI + yPadding;

  const scaleX = (index: number) => (index / Math.max(data.length - 1, 1)) * chartWidth;
  const scaleY = (value: number) => chartHeight - ((value - yMin) / (yMax - yMin)) * chartHeight;

  const points = data.map((d, i) => ({
    x: scaleX(i),
    y: scaleY(d.total_oi),
    value: d.total_oi,
    date: d.date
  }));

  const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
  const areaPath = `${linePath} L ${points[points.length - 1].x} ${chartHeight} L ${points[0].x} ${chartHeight} Z`;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(pct => ({
    value: yMin + (yMax - yMin) * pct,
    y: scaleY(yMin + (yMax - yMin) * pct)
  }));

  const xTickCount = Math.min(10, data.length);
  const xTicks = Array.from({ length: xTickCount }, (_, i) => {
    const index = Math.floor((i / Math.max(xTickCount - 1, 1)) * (data.length - 1));
    return {
      label: new Date(data[index].date).toLocaleDateString('ru-RU', { day: '2-digit', month: 'short' }),
      x: scaleX(index)
    };
  });

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const mouseX = e.clientX - rect.left - padding.left;

    if (mouseX < 0 || mouseX > chartWidth) {
      setTooltip(prev => ({ ...prev, visible: false }));
      return;
    }

    let closest = points[0];
    let closestDist = Math.abs(points[0].x - mouseX);
    for (const p of points) {
      const dist = Math.abs(p.x - mouseX);
      if (dist < closestDist) {
        closestDist = dist;
        closest = p;
      }
    }

    setTooltip({
      visible: true,
      x: closest.x + padding.left,
      y: closest.y + padding.top,
      value: closest.value,
      date: closest.date
    });
  };

  const handleMouseLeave = () => {
    setTooltip(prev => ({ ...prev, visible: false }));
  };

  const tooltipX = Math.min(Math.max(tooltip.x, padding.left + 10), Math.max(width, 300) - 170);
  const tooltipY = Math.max(30, Math.min(tooltip.y - 70, height - 90));

  return (
    <div className="bg-slate-800/30 backdrop-blur-sm rounded-2xl p-6 border border-slate-700/50">
      <h3 className="text-white font-semibold mb-4 text-lg">{title}</h3>

      <div ref={containerRef} className="w-full">
        <svg
          width={width}
          height={height}
          className="cursor-crosshair block"
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
        >
          <defs>
            <linearGradient id="oiGradientHome" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity="0.4" />
              <stop offset="50%" stopColor="#10b981" stopOpacity="0.15" />
              <stop offset="100%" stopColor="#10b981" stopOpacity="0" />
            </linearGradient>
            <clipPath id="chartClipHome">
              <rect x={0} y={0} width={chartWidth} height={chartHeight} />
            </clipPath>
          </defs>

          <g transform={`translate(${padding.left}, ${padding.top})`}>
            {yTicks.map((tick, i) => (
              <g key={i}>
                <line
                  x1={0}
                  y1={tick.y}
                  x2={chartWidth}
                  y2={tick.y}
                  stroke="#334155"
                  strokeWidth="1"
                  strokeDasharray="4,6"
                  opacity="0.5"
                />
                <text
                  x={-12}
                  y={tick.y + 4}
                  textAnchor="end"
                  fill="#64748b"
                  fontSize="12"
                >
                  {formatNumber(tick.value)}
                </text>
              </g>
            ))}

            {xTicks.map((tick, i) => (
              <text
                key={i}
                x={tick.x}
                y={chartHeight + 30}
                textAnchor="middle"
                fill="#64748b"
                fontSize="12"
              >
                {tick.label}
              </text>
            ))}

            <g clipPath="url(#chartClipHome)">
              <path
                d={areaPath}
                fill="url(#oiGradientHome)"
                className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
              />

              <path
                d={linePath}
                fill="none"
                stroke="#10b981"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
              />

              {tooltip.visible && (
                <rect
                  x={tooltip.x - padding.left}
                  y={0}
                  width={Math.max(0, chartWidth - (tooltip.x - padding.left))}
                  height={chartHeight}
                  fill="#0f172a"
                  opacity="0.5"
                  className="transition-opacity duration-150"
                />
              )}
            </g>

            {tooltip.visible && (
              <>
                <line
                  x1={tooltip.x - padding.left}
                  y1={0}
                  x2={tooltip.x - padding.left}
                  y2={chartHeight}
                  stroke="#10b981"
                  strokeWidth="1.5"
                  strokeDasharray="4,4"
                />
                <circle
                  cx={tooltip.x - padding.left}
                  cy={tooltip.y - padding.top}
                  r="7"
                  fill="#10b981"
                  stroke="#fff"
                  strokeWidth="3"
                  className="drop-shadow-lg"
                />
              </>
            )}
          </g>

          {tooltip.visible && (
            <foreignObject x={tooltipX} y={tooltipY} width="160" height="80">
              <div className="bg-slate-800/95 backdrop-blur-sm rounded-xl p-3 border border-slate-600/50 shadow-xl">
                <p className="text-slate-400 text-xs mb-1">
                  {new Date(tooltip.date).toLocaleDateString('ru-RU', {
                    day: '2-digit', month: 'short', year: 'numeric'
                  })}
                </p>
                <p className="text-emerald-400 font-bold text-xl">
                  {formatNumber(tooltip.value)}
                </p>
              </div>
            </foreignObject>
          )}
        </svg>
      </div>
    </div>
  );
}

export default function HomePage() {
  const [instruments, setInstruments] = useState<UniqueInstrument[]>([]);
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [statsLoading, setStatsLoading] = useState(true);

  const [typeFilter, setTypeFilter] = useState<string>('futures');
  const [search, setSearch] = useState<string>('');
  const [period, setPeriod] = useState<Period>('1w');
  const [clgroup, setClgroup] = useState<'FIZ' | 'YUR'>('FIZ');

  useEffect(() => {
    async function loadInstruments() {
      try {
        setLoading(true);
        const instrumentsRes = await getInstruments(typeFilter || undefined);

        const grouped = new Map<string, Instrument[]>();
        for (const inst of instrumentsRes.instruments) {
          const key = `${inst.sectype}|${inst.type}`;
          if (!grouped.has(key)) grouped.set(key, []);
          grouped.get(key)!.push(inst);
        }

        const unique: UniqueInstrument[] = [];
        for (const [, contracts] of grouped) {
          const first = contracts[0];
          unique.push({
            sectype: first.sectype,
            name: first.name,
            type: first.type,
            group: first.group,
            contracts,
          });
        }
        unique.sort((a, b) => a.name.localeCompare(b.name));

        setInstruments(unique);
      } catch (err) {
        console.error('Ошибка загрузки инструментов:', err);
      } finally {
        setLoading(false);
      }
    }
    void loadInstruments();
  }, [typeFilter]);

  useEffect(() => {
    async function loadStats() {
      try {
        setStatsLoading(true);
        const statsRes = await getStats(period, clgroup);
        setStats(statsRes);
      } catch (err) {
        console.error('Ошибка загрузки статистики:', err);
      } finally {
        setStatsLoading(false);
      }
    }
    void loadStats();
  }, [period, clgroup]);

  const filteredInstruments = instruments.filter((inst) =>
    inst.name.toLowerCase().includes(search.toLowerCase()) ||
    inst.sectype.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl md:text-4xl font-bold text-white tracking-tight">
          Открытый интерес
        </h1>
        <p className="text-slate-400 mt-2">
          Мониторинг открытого интереса на Московской бирже
        </p>
      </div>

      {/* Период и группа */}
      <div className="flex gap-4 mb-6 flex-wrap">
        <div className="flex gap-1 bg-slate-800/50 p-1 rounded-xl">
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                period === p
                  ? 'bg-emerald-600 text-white shadow-lg'
                  : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
              }`}
            >
              {PERIOD_LABELS[p]}
            </button>
          ))}
        </div>

        <div className="flex gap-1 bg-slate-800/50 p-1 rounded-xl">
          <button
            onClick={() => setClgroup('FIZ')}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${
              clgroup === 'FIZ'
                ? 'bg-emerald-600 text-white shadow-lg'
                : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
            }`}
          >
            Физлица
          </button>
          <button
            onClick={() => setClgroup('YUR')}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all ${
              clgroup === 'YUR'
                ? 'bg-emerald-600 text-white shadow-lg'
                : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
            }`}
          >
            Юрлица
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatsCard
          title="Total Open Interest"
          value={stats ? formatNumber(stats.stats.total_oi) : '—'}
          change={stats?.stats.oi_change}
          icon={IconActivity}
          loading={statsLoading}
        />
        <StatsCard
          title="Покупки (Long)"
          value={stats ? formatNumber(stats.stats.total_long) : '—'}
          change={stats?.stats.long_change}
          icon={IconChart}
          loading={statsLoading}
        />
        <StatsCard
          title="Продажи (Short)"
          value={stats ? formatNumber(stats.stats.total_short) : '—'}
          change={stats?.stats.short_change}
          icon={IconTrending}
          loading={statsLoading}
        />
        <StatsCard
          title="Участников"
          value={stats ? formatNumber(stats.stats.participants) : '—'}
          change={stats?.stats.participants_change}
          icon={IconUsers}
          loading={statsLoading}
        />
      </div>

      {/* График на всю ширину */}
      <div className="mb-8">
        <InteractiveOIChart
          data={stats?.chart_data || []}
          title={`Динамика Open Interest за ${PERIOD_LABELS[period].toLowerCase()}`}
          loading={statsLoading}
        />
      </div>

      {/* Поиск и фильтры */}
      <div className="bg-slate-800/30 backdrop-blur-sm rounded-2xl border border-slate-700/50 p-4 mb-6">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1 relative">
            <input
              type="text"
              placeholder="Поиск инструмента..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full bg-slate-900/50 text-white placeholder-slate-500 border border-slate-700/50 rounded-xl px-4 py-3 pl-11 focus:outline-none focus:ring-2 focus:ring-emerald-500/50 transition-all"
            />
            <svg
              className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500"
              fill="none" stroke="currentColor" viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>

          <div className="flex gap-2">
            {['', 'futures', 'stock'].map((type) => (
              <button
                key={type}
                onClick={() => setTypeFilter(type)}
                className={`px-4 py-2.5 rounded-xl font-medium transition-all ${
                  typeFilter === type
                    ? 'bg-emerald-600 text-white shadow-lg'
                    : 'bg-slate-700/50 text-slate-400 hover:bg-slate-600/50 hover:text-white'
                }`}
              >
                {type === '' ? 'Все' : type === 'futures' ? '🔥 Фьючерсы' : '📈 Акции'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Список инструментов */}
      {!loading && (
        <div className="bg-slate-800/30 backdrop-blur-sm rounded-2xl border border-slate-700/50 overflow-hidden">
          <div className="hidden md:grid grid-cols-12 gap-4 px-5 py-3 bg-slate-900/50 border-b border-slate-700/50 text-sm text-slate-400 font-medium">
            <div className="col-span-3">Инструмент</div>
            <div className="col-span-4">Название</div>
            <div className="col-span-2">Тип</div>
            <div className="col-span-3 text-right">Контракты</div>
          </div>

          {filteredInstruments.slice(0, 50).map((inst, index) => (
            <Link
              key={`${inst.sectype}-${inst.type}`}
              to={`/chart/${inst.contracts[0].sec_id}?sectype=${inst.sectype}&type=${inst.type}`}
              className={`grid grid-cols-12 gap-4 px-5 py-4 items-center hover:bg-slate-700/30 transition-colors group ${
                index !== Math.min(filteredInstruments.length, 50) - 1 ? 'border-b border-slate-700/30' : ''
              }`}
            >
              <div className="col-span-6 md:col-span-3 flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500/20 to-blue-500/20 flex items-center justify-center text-white font-bold text-sm">
                  {inst.sectype.slice(0, 2)}
                </div>
                <span className="text-lg font-bold text-white group-hover:text-emerald-400 transition-colors">
                  {inst.sectype}
                </span>
              </div>
              <div className="hidden md:block col-span-4 text-slate-400 truncate">{inst.name}</div>
              <div className="hidden md:block col-span-2">
                <span className={`px-2.5 py-1 rounded-lg text-xs font-medium ${
                  inst.type === 'futures' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-emerald-500/20 text-emerald-300'
                }`}>
                  {inst.type === 'futures' ? 'Фьючерс' : 'Акция'}
                </span>
              </div>
              <div className="col-span-6 md:col-span-3 flex items-center justify-end gap-2">
                <span className="text-slate-500">{inst.contracts.length} шт</span>
                <span className="text-slate-500 group-hover:text-emerald-400">→</span>
              </div>
            </Link>
          ))}

          {filteredInstruments.length === 0 && (
            <div className="text-center py-16 text-slate-500">Ничего не найдено</div>
          )}
        </div>
      )}

      {/* Footer */}
      <div className="mt-12 text-center text-slate-600 text-sm">
        <p>MOEX Analytics • {instruments.length} инструментов • {new Date().getFullYear()}</p>
      </div>
    </div>
  );
}