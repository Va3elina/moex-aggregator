/**
 * OverviewPage — главная для АВТОРИЗОВАННЫХ пользователей.
 *
 * Структура (Variant C "Mosaic"):
 *  1. Quote tiles — IMOEX/RTSI/USD/CNY/GLD с sparkline 30 дней
 *  2. Movers + Sectors row — что движется сегодня
 *  3. Скриннер акций — interactive filter/sort over heatmap data
 *  4. Top Volume + Funds — куда идут деньги
 *  5. Indicator grid — 8 индикаторов
 *
 * Раньше были «Привет, Вадим» + 3 KEY METRICS (Страх/Сила/Баффетт),
 * убраны: первое — лишний персональный noise; вторые — дублируют контент
 * на их собственных страницах. Workspace, не приветствие.
 *
 * Незалогиненные видят LandingPage (routing conditional в App.tsx).
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowRight,
  Activity,
  Scale,
  Grid3X3,
  BarChart3,
  Wallet,
  LayoutGrid,
  CalendarDays,
  Search,
  TrendingUp,
  TrendingDown,
  Building2,
} from 'lucide-react';
import {
  getHeatmapImoex,
  getFundsSummary,
  getSeasonalityPrice,
} from '../services/api';
import type { HeatmapStock, FundsSummaryResponse } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { formatNumber } from '../utils/formatNumber';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';

const INDICATORS: {
  path: string;
  title: string;
  desc: string;
  icon: typeof Grid3X3;
  badge?: string;
}[] = [
  { path: '/heatmap', title: 'Карта рынка', desc: 'Акции MOEX по секторам, размер = капитализация', icon: Grid3X3 },
  { path: '/oi', title: 'Открытый интерес', desc: 'Позиции участников по фьючерсам Мосбиржи', icon: BarChart3 },
  { path: '/funds-money', title: 'Деньги в фондах', desc: 'Динамика СЧА и притоки-оттоки фондов', icon: Wallet },
  { path: '/strength', title: 'Сила рынка', desc: '% акций выше EMA 50/100/200', icon: Activity },
  { path: '/buffett', title: 'Индикатор Баффетта', desc: 'Капитализация / ВВП + Cap / M2', icon: Scale },
  { path: '/seasonality', title: 'Сезонность', desc: 'Среднее изменение цены по периодам', icon: CalendarDays },
  { path: '/cbr-flows', title: 'Потоки ЦБ', desc: 'Кто покупает и продаёт по типам активов', icon: Building2 },
  // «Состав фондов» — сырой alpha-индикатор, ставим последним.
  { path: '/funds-catalog', title: 'Состав фондов', desc: 'Портфели фондов акций и облигаций', icon: LayoutGrid, badge: 'Alfa тест' },
];

/** Тикеры для quote-tiles сверху страницы. Label = отображаемое имя. */
const QUOTE_TICKERS: { secid: string; label: string }[] = [
  { secid: 'IMOEX',        label: 'IMOEX' },
  { secid: 'RTSI',         label: 'RTSI' },
  { secid: 'USD000UTSTOM', label: 'USD/₽' },
  { secid: 'CNYRUB_TOM',   label: 'CNY/₽' },
  { secid: 'GLDRUB_TOM',   label: 'Золото' },
];

/** Параллельно фетчит closes за последние 30 дней для каждого тикера.
    Возвращает { IMOEX: {label, closes}, USD000UTSTOM: {...}, ... }. */
async function fetchQuotes(): Promise<Record<string, { label: string; closes: number[] }>> {
  const results = await Promise.allSettled(
    QUOTE_TICKERS.map(t =>
      getSeasonalityPrice(t.secid, 30).then(r => ({
        secid: t.secid,
        label: t.label,
        closes: r.data.map(d => d.close),
      })),
    ),
  );
  const out: Record<string, { label: string; closes: number[] }> = {};
  results.forEach(r => {
    if (r.status === 'fulfilled' && r.value.closes.length > 0) {
      out[r.value.secid] = { label: r.value.label, closes: r.value.closes };
    }
  });
  return out;
}

export default function OverviewPage() {
  // Полный список акций с heatmap — используется для movers, top volume, sectors,
  // и interactive скриннера. 1 fetch → 4 view'а из одного array.
  const [allStocks, setAllStocks] = useState<HeatmapStock[]>([]);
  const [gainers, setGainers] = useState<HeatmapStock[]>([]);
  const [losers, setLosers] = useState<HeatmapStock[]>([]);
  const [topVolume, setTopVolume] = useState<HeatmapStock[]>([]);
  const [sectors, setSectors] = useState<{ name: string; change: number; count: number }[]>([]);
  const [fundsSummary, setFundsSummary] = useState<FundsSummaryResponse | null>(null);
  // Index / currency tiles (IMOEX, RTSI, USD, CNY, GLD).
  // Храним массив [{secid, label, closes[30]}] → рендерим sparkline + last/prev.
  const [quotes, setQuotes] = useState<Record<string, { label: string; closes: number[] }>>({});

  /** ГОРЯЧИЕ данные — обновляются каждые 5 минут (SSE '5min' push). */
  const loadHotData = useCallback(async () => {
    await Promise.allSettled([
      // /imoex (не /stocks) — обзор строим по индексным голубым фишкам.
      // /stocks tier-gated (Basic+), на главную попадают и free-юзеры →
      // imoex работает для всех тиров без 403 и даёт меньше неликвид-шума.
      getHeatmapImoex().then(r => {
        setAllStocks(r.stocks);

        // Топ-5 gainers и losers по change_1d.
        const withChange = r.stocks.filter(s => Number.isFinite(s.change_1d));
        const sorted = [...withChange].sort((a, b) => b.change_1d - a.change_1d);
        setGainers(sorted.slice(0, 5));
        setLosers(sorted.slice(-5).reverse());

        // Топ-5 по объёму (value_1d = торговый оборот в рублях).
        const byVolume = [...r.stocks]
          .filter(s => Number.isFinite(s.value_1d) && s.value_1d > 0)
          .sort((a, b) => b.value_1d - a.value_1d);
        setTopVolume(byVolume.slice(0, 5));

        // Equal-weighted average по секторам.
        const sectorMap = new Map<string, number[]>();
        withChange.forEach(s => {
          if (!s.sector) return;
          if (!sectorMap.has(s.sector)) sectorMap.set(s.sector, []);
          sectorMap.get(s.sector)!.push(s.change_1d);
        });
        const secArr = Array.from(sectorMap.entries()).map(([name, changes]) => ({
          name,
          change: changes.reduce((a, b) => a + b, 0) / changes.length,
          count: changes.length,
        }));
        secArr.sort((a, b) => b.change - a.change);
        setSectors(secArr);
      }),
      getFundsSummary().then(setFundsSummary),
    ]);
  }, []);

  /** HEAVY/DAILY данные — Quotes (5 × 30-дневных историй).
   *   Меняются раз в сутки → рефетчить каждые 5 минут = перегруз API.
   *   Загружаем 1 раз на mount + при visibilitychange (если >1 час неактивности). */
  const loadDailyData = useCallback(async () => {
    await Promise.allSettled([
      fetchQuotes().then(setQuotes),
    ]);
  }, []);

  // 1. Initial load на mount — и горячие, и ежедневные
  useEffect(() => {
    loadHotData();
    loadDailyData();
  }, [loadHotData, loadDailyData]);

  // 2. SSE realtime — только горячие (5-мин) метрики.
  //    Quotes/Buffett не трогаем — они daily, рефетч раз в 5 мин = DB pool exhaustion.
  useRealtimeData(['5min', 'mv_refresh'], loadHotData);

  // 3. Visibilitychange — возвращение на вкладку рефетчит горячие.
  //    Daily рефетчим только если был >1ч отсутствия (порог hysteresis).
  const lastDailyFetchRef = useRef<number>(Date.now());
  useEffect(() => {
    const onVis = () => {
      if (document.visibilityState !== 'visible') return;
      loadHotData();
      if (Date.now() - lastDailyFetchRef.current > 3600_000) {
        loadDailyData();
        lastDailyFetchRef.current = Date.now();
      }
    };
    document.addEventListener('visibilitychange', onVis);
    return () => document.removeEventListener('visibilitychange', onVis);
  }, [loadHotData, loadDailyData]);

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">

      {/* ═══ QUOTE TILES — IMOEX / RTSI / USD / CNY / Gold ═══
          Live-цифры сверху страницы. Sparkline показывает движение за 30 дней.
          Всегда рендерим 5 slot'ов: skeleton если data нет → реальный tile при load. */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-2 md:gap-3 mb-2 md:mb-3">
        {QUOTE_TICKERS.map(t => {
          const q = quotes[t.secid];
          const hasData = q && q.closes.length >= 2;
          return hasData
            ? <QuoteTile key={t.secid} label={q.label} closes={q.closes} />
            : <Skeleton key={t.secid} height={68} rounded="md" />;
        })}
      </section>
      {/* Памятка об источнике: котировки индексов и валют — данные Мосбиржи. */}
      <p
        className="mb-8 md:mb-10"
        style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-2xs)' }}
      >
        Источник: Московская биржа
      </p>

      {/* ═══ TOP MOVERS (Gainers / Losers) ═══
          Секция всегда рендерится: skeleton если data грузится → MoversList при load.
          Это фиксит layout-shift: container размера не меняется. */}
      <section className="mb-10 md:mb-12">
        <div className="flex items-center gap-3 mb-5 md:mb-6">
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Движение дня
          </p>
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
          <Link
            to="/heatmap"
            className="text-xs flex items-center gap-1 transition-opacity hover:opacity-80"
            style={{ color: 'var(--text-secondary)' }}
          >
            Карта рынка
            <ArrowRight size={12} />
          </Link>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-3 md:gap-4">
          {gainers.length > 0
            ? <MoversList title="Лидеры роста" stocks={gainers} direction="up" />
            : <Skeleton height={240} rounded="lg" />}
          {losers.length > 0
            ? <MoversList title="Лидеры падения" stocks={losers} direction="down" />
            : <Skeleton height={240} rounded="lg" />}
        </div>
      </section>

      {/* ═══ SECTOR PERFORMANCE ═══ */}
      <section className="mb-10 md:mb-12">
        <div className="flex items-center gap-3 mb-5 md:mb-6">
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Сектора дня
          </p>
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
          {sectors.length > 0 && (
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
              {sectors.length} секторов
            </span>
          )}
        </div>

        {sectors.length > 0
          ? <SectorBars sectors={sectors} />
          : <Skeleton height={320} rounded="lg" />}
      </section>

      {/* ═══ СКРИННЕР АКЦИЙ ═══
          Interactive filter/sort over allStocks. Без новых API запросов —
          использует уже подгруженный heatmap data. Sector dropdown собирается
          dynamically из самих stocks. */}
      <section className="mb-10 md:mb-12">
        <div className="flex items-center gap-3 mb-5 md:mb-6">
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Скриннер акций
          </p>
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
          {allStocks.length > 0 && (
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
              {allStocks.length} бумаг
            </span>
          )}
        </div>

        {allStocks.length > 0
          ? <Screener stocks={allStocks} />
          : <Skeleton height={420} rounded="lg" />}
      </section>

      {/* ═══ TOP VOLUME + FUNDS ═══ */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-10 md:mb-12">
        {topVolume.length > 0
          ? <VolumeList stocks={topVolume} />
          : <Skeleton height={240} rounded="lg" />}
        {fundsSummary
          ? <FundsCategories summary={fundsSummary} />
          : <Skeleton height={240} rounded="lg" />}
      </section>

      {/* ═══ INDICATOR GRID ═══ */}
      <section>
        <div className="flex items-center gap-3 mb-5 md:mb-6">
          <p
            className="text-xs uppercase"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Все индикаторы
          </p>
          <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3 md:gap-4">
          {INDICATORS.map(ind => (
            <Link
              key={ind.path}
              to={ind.path}
              className="group block p-4 md:p-5 border transition-all"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                borderColor: 'var(--border-color)',
                borderRadius: 'var(--radius-lg, 12px)',
              }}
              onMouseEnter={e => (e.currentTarget.style.borderColor = 'color-mix(in srgb, var(--accent) 40%, transparent)')}
              onMouseLeave={e => (e.currentTarget.style.borderColor = 'var(--border-color)')}
            >
              <div className="flex items-start gap-3 mb-2">
                <div
                  className="flex items-center justify-center flex-shrink-0"
                  style={{
                    width: 36,
                    height: 36,
                    borderRadius: 'var(--radius-md, 8px)',
                    background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
                    color: 'var(--accent)',
                  }}
                >
                  <ind.icon size={18} strokeWidth={1.8} />
                </div>
                <div className="flex-1 min-w-0">
                  <h3
                    className="font-semibold text-sm md:text-base truncate flex items-center gap-2"
                    style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
                  >
                    <span className="truncate">{ind.title}</span>
                    {ind.badge && (
                      <span
                        className="uppercase font-bold flex-shrink-0"
                        style={{
                          fontSize: '0.62rem',
                          letterSpacing: '0.06em',
                          color: 'var(--accent)',
                          border: '1px solid var(--accent)',
                          borderRadius: '3px',
                          padding: '1px 5px',
                          lineHeight: 1.2,
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {ind.badge}
                      </span>
                    )}
                  </h3>
                </div>
                <ArrowRight
                  size={16}
                  className="opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0 mt-1"
                  style={{ color: 'var(--accent)' }}
                />
              </div>
              <p className="text-xs md:text-sm" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                {ind.desc}
              </p>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ═══════════════════════════════════════════════════════════

/** Quote tile — компактная плитка с тикером, ценой, % изменением и sparkline.
    Sparkline = inline SVG path за ~30 дней, без осей (просто силуэт).
    Цвет линии зелёный если close[last] > close[first], красный если меньше. */
function QuoteTile({ label, closes }: { label: string; closes: number[] }) {
  const last = closes[closes.length - 1];
  const prev = closes[closes.length - 2];
  const first = closes[0];
  const changeDay = prev ? ((last - prev) / prev) * 100 : 0;
  const changePeriod = first ? ((last - first) / first) * 100 : 0;

  // Day-change и period-change имеют РАЗНЫЕ направления — это фича:
  // текст дневного изменения красится по дневному знаку, sparkline — по 30-дневному.
  // Так user видит одновременно "сегодня" + "тренд".
  const dayColor = changeDay >= 0 ? 'var(--success)' : 'var(--danger)';
  const periodColor = changePeriod >= 0 ? 'var(--success)' : 'var(--danger)';

  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const range = max - min || 1;
  const w = 100, h = 20;
  const path = closes
    .map((c, i) => {
      const x = (i / (closes.length - 1)) * w;
      const y = h - ((c - min) / range) * h;
      return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1);
    })
    .join(' ');

  // Компактный формат: без пробелов в малых числах
  const priceFmt = last >= 1000
    ? formatNumber(last, 0)
    : formatNumber(last, 2);

  return (
    <div
      className="px-3 py-2 border flex items-center gap-3"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-md, 8px)',
      }}
    >
      {/* Label + price inline — компактно */}
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2">
          <span
            className="uppercase flex-shrink-0"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600, fontSize: 'var(--fs-2xs)' }}
          >
            {label}
          </span>
          <span
            className="text-xs font-semibold"
            style={{
              color: dayColor,
              fontFamily: "'IBM Plex Mono', monospace",
              fontVariantNumeric: 'tabular-nums',
            }}
          >
            {changeDay > 0 ? '+' : ''}{changeDay.toFixed(2)}%
          </span>
        </div>
        <div
          className="text-base font-semibold leading-tight"
          style={{
            color: 'var(--text-primary)',
            fontFamily: "'IBM Plex Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
            letterSpacing: '-0.02em',
          }}
        >
          {priceFmt}
        </div>
      </div>
      {/* Sparkline — inline справа, тонкий */}
      <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" width="60" height="28" className="opacity-70 flex-shrink-0">
        <path d={path} fill="none" stroke={periodColor} strokeWidth="1.5" />
      </svg>
    </div>
  );
}

/** Топ-5 по обороту дня (value_1d в рублях).
    Формат value: "12,3 млрд ₽" / "450 млн ₽". */
function VolumeList({ stocks }: { stocks: HeatmapStock[] }) {
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-3">
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Топ обороту дня
        </span>
      </div>
      <div className="space-y-1.5">
        {stocks.map(s => (
          <Link
            key={s.secId}
            to="/heatmap"
            className="flex items-center justify-between py-1.5 px-2 -mx-2 transition-colors hover:bg-white/[0.03]"
            style={{ borderRadius: 'var(--radius-sm, 4px)' }}
            title={`${s.name} · оборот ${formatMoney(s.value_1d)}`}
          >
            <div className="flex items-baseline gap-2 min-w-0 flex-1">
              <span
                className="text-sm font-semibold"
                style={{
                  color: 'var(--text-primary)',
                  fontFamily: "'IBM Plex Mono', monospace",
                }}
              >
                {s.secId}
              </span>
              <span className="text-xs truncate" style={{ color: 'var(--text-secondary)' }}>
                {s.name}
              </span>
            </div>
            <span
              className="text-sm flex-shrink-0 ml-2"
              style={{
                color: 'var(--text-primary)',
                fontFamily: "'IBM Plex Mono', monospace",
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {formatMoney(s.value_1d)}
            </span>
          </Link>
        ))}
      </div>
    </Card>
  );
}

/** Сводка по категориям фондов (money_market / stocks / bonds / gold).
    4 строки с NAV + % change за день.*/
function FundsCategories({ summary }: { summary: FundsSummaryResponse }) {
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-3">
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Фонды по категориям
        </span>
      </div>
      <div className="space-y-1.5">
        {summary.categories.map(c => {
          const change = c.change_pct;
          const isUp = change >= 0;
          const color = isUp ? 'var(--success)' : 'var(--danger)';
          return (
            <Link
              key={c.category}
              to="/funds-money"
              className="flex items-center justify-between py-1.5 px-2 -mx-2 transition-colors hover:bg-white/[0.03]"
              style={{ borderRadius: 'var(--radius-sm, 4px)' }}
              title={`${c.funds_count} фондов · индекс ${c.index}`}
            >
              <div className="flex items-baseline gap-2 min-w-0 flex-1">
                <span
                  className="text-sm font-semibold"
                  style={{ color: 'var(--text-primary)' }}
                >
                  {c.name}
                </span>
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
                  {c.funds_count}
                </span>
              </div>
              <div className="flex items-baseline gap-3 flex-shrink-0 ml-2">
                <span
                  className="text-xs"
                  style={{
                    color: 'var(--text-secondary)',
                    fontFamily: "'IBM Plex Mono', monospace",
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {c.total_nav_formatted}
                </span>
                <span
                  className="text-sm font-semibold w-14 text-right"
                  style={{
                    color,
                    fontFamily: "'IBM Plex Mono', monospace",
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {isUp ? '+' : ''}{change.toFixed(2)}%
                </span>
              </div>
            </Link>
          );
        })}
      </div>
    </Card>
  );
}

/** Помощник: отформатировать деньги в виде "12,3 млрд ₽" / "450 млн ₽". */
function formatMoney(value: number): string {
  if (value >= 1e9) return `${(value / 1e9).toFixed(1)} млрд ₽`;
  if (value >= 1e6) return `${(value / 1e6).toFixed(0)} млн ₽`;
  if (value >= 1e3) return `${(value / 1e3).toFixed(0)} тыс ₽`;
  return `${value.toFixed(0)} ₽`;
}

/** Сектора дня — горизонтальные bar'ы, каждый сектор = строка.
    Бар растёт от центра (0) направо для роста, налево для падения.
    Ширина бара пропорциональна |change| относительно максимального abs change. */
function SectorBars({ sectors }: { sectors: { name: string; change: number; count: number }[] }) {
  // Диапазон для нормализации — max |change| среди всех секторов.
  // Добавляем 0.5% отступ чтобы бары не упирались в края.
  const maxAbs = Math.max(...sectors.map(s => Math.abs(s.change)), 0.5);

  return (
    <Card padding="md">
      <div className="space-y-2">
        {sectors.map(s => {
          const isUp = s.change >= 0;
          const barWidth = `${(Math.abs(s.change) / maxAbs) * 48}%`; // 48% — каждая половина
          const color = isUp ? 'var(--success)' : 'var(--danger)';
          return (
            <div key={s.name} className="flex items-center gap-3">
              {/* Sector name — fixed width на desktop, чтобы бары выровнялись */}
              <div
                className="flex-shrink-0 text-xs md:text-sm truncate"
                style={{ color: 'var(--text-primary)', width: 'min(140px, 35%)', fontWeight: 500 }}
                title={`${s.name} · ${s.count} бумаг`}
              >
                {s.name}
              </div>

              {/* Bar track — 50/50 split с центральной линией */}
              <div className="flex-1 relative h-5 flex">
                {/* Left half (negative side) */}
                <div className="flex-1 flex justify-end relative">
                  {!isUp && (
                    <div
                      className="h-full"
                      style={{
                        width: barWidth,
                        background: color,
                        opacity: 0.85,
                        borderRadius: '2px 0 0 2px',
                      }}
                    />
                  )}
                </div>
                {/* Center divider — 1px линия */}
                <div
                  className="w-px flex-shrink-0"
                  style={{ backgroundColor: 'var(--border-color)' }}
                />
                {/* Right half (positive side) */}
                <div className="flex-1 flex justify-start relative">
                  {isUp && (
                    <div
                      className="h-full"
                      style={{
                        width: barWidth,
                        background: color,
                        opacity: 0.85,
                        borderRadius: '0 2px 2px 0',
                      }}
                    />
                  )}
                </div>
              </div>

              {/* Change % — right-aligned, tabular */}
              <div
                className="flex-shrink-0 text-right text-xs md:text-sm font-semibold"
                style={{
                  color,
                  fontFamily: "'IBM Plex Mono', monospace",
                  fontVariantNumeric: 'tabular-nums',
                  width: 60,
                }}
              >
                {isUp ? '+' : ''}{s.change.toFixed(2)}%
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}

/** Список лидеров роста/падения — 5 акций в столбик, clickable.
    Каждая строка: тикер, название, изменение в % (tabular nums). */
function MoversList({
  title,
  stocks,
  direction,
}: {
  title: string;
  stocks: HeatmapStock[];
  direction: 'up' | 'down';
}) {
  const accentColor = direction === 'up' ? 'var(--success)' : 'var(--danger)';
  const Icon = direction === 'up' ? TrendingUp : TrendingDown;

  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-3">
        <Icon size={14} style={{ color: accentColor }} />
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          {title}
        </span>
      </div>
      <div className="space-y-1.5">
        {stocks.map(s => (
          <Link
            key={s.secId}
            to="/heatmap"
            className="flex items-center justify-between py-1.5 px-2 -mx-2 transition-colors hover:bg-white/[0.03]"
            style={{ borderRadius: 'var(--radius-sm, 4px)' }}
            title={`${s.name} · перейти на карту рынка`}
          >
            <div className="flex items-baseline gap-2 min-w-0 flex-1">
              <span
                className="text-sm font-semibold"
                style={{
                  color: 'var(--text-primary)',
                  fontFamily: "'IBM Plex Mono', monospace",
                }}
              >
                {s.secId}
              </span>
              <span
                className="text-xs truncate"
                style={{ color: 'var(--text-secondary)' }}
              >
                {s.name}
              </span>
            </div>
            <span
              className="text-sm font-semibold flex-shrink-0 ml-2"
              style={{
                color: accentColor,
                fontFamily: "'IBM Plex Mono', monospace",
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {s.change_1d > 0 ? '+' : ''}{s.change_1d.toFixed(2)}%
            </span>
          </Link>
        ))}
      </div>
    </Card>
  );
}

/** Скриннер акций — interactive table с filter/sort.
 *
 *  Filters:
 *   - Search (ticker substring или name substring, case-insensitive)
 *   - Sector dropdown (Все + список из stocks dynamically)
 *   - Period (1д/1н/1м/1г) — определяет какую change колонку показывать
 *   - Sort (по выбранному period change / volume / market cap)
 *
 *  Display: top 20 строк, кнопка «Показать ещё» добавляет +20.
 *  Click по тикеру → /heatmap (consistency с MoversList/VolumeList).
 *
 *  Без backend запросов — derives view из allStocks через useMemo.
 *  Mobile: горизонтальный scroll table (компактная таблица не помещается на 320px).
 */
type ScreenerPeriod = '1d' | '1w' | '1m' | '1y';
type ScreenerSort = 'change' | 'volume' | 'mcap';

function Screener({ stocks }: { stocks: HeatmapStock[] }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sectorFilter, setSectorFilter] = useState<string>('all');
  const [period, setPeriod] = useState<ScreenerPeriod>('1d');
  const [sortBy, setSortBy] = useState<ScreenerSort>('mcap');
  const [limit, setLimit] = useState(20);

  // Уникальные сектора из stocks для dropdown'а. Sort by alphabet.
  const sectorOptions = useMemo(() => {
    const set = new Set<string>();
    stocks.forEach(s => { if (s.sector) set.add(s.sector); });
    const list = Array.from(set).sort((a, b) => a.localeCompare(b, 'ru'));
    return [
      { key: 'all', label: 'Все сектора' },
      ...list.map(s => ({ key: s, label: s })),
    ];
  }, [stocks]);

  const periodOptions: { key: ScreenerPeriod; label: string }[] = [
    { key: '1d', label: '1 день' },
    { key: '1w', label: '1 неделя' },
    { key: '1m', label: '1 месяц' },
    { key: '1y', label: '1 год' },
  ];

  const sortOptions: { key: ScreenerSort; label: string }[] = [
    { key: 'mcap', label: 'Капитализация' },
    { key: 'volume', label: 'Объём' },
    { key: 'change', label: 'Изменение' },
  ];

  // Helper: change% по выбранному периоду
  const getChange = (s: HeatmapStock): number => {
    if (period === '1w') return s.change_1w;
    if (period === '1m') return s.change_1m;
    if (period === '1y') return s.change_1y;
    return s.change_1d;
  };

  // Filter + sort через useMemo чтобы не пересчитывать на каждый render
  const filtered = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    let out = stocks.filter(s => {
      // Search по secId или name (case-insensitive)
      if (q && !(s.secId.toLowerCase().includes(q) || s.name.toLowerCase().includes(q))) {
        return false;
      }
      // Sector filter
      if (sectorFilter !== 'all' && s.sector !== sectorFilter) {
        return false;
      }
      // Drop entries без price (защита от пустых rows)
      if (!Number.isFinite(s.price) || s.price <= 0) return false;
      return true;
    });

    // Sort: всегда desc для всех режимов (большие cap / больший объём / большее change сверху).
    out = out.sort((a, b) => {
      if (sortBy === 'mcap') return (b.market_cap || 0) - (a.market_cap || 0);
      if (sortBy === 'volume') return (b.value_1d || 0) - (a.value_1d || 0);
      // change — sort по abs так "лидеры" и "аутсайдеры" оба на верху,
      // но внутри pos/neg сортировка по знаку чтобы pos шли первыми.
      const ca = getChange(a), cb = getChange(b);
      return cb - ca;
    });
    return out;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stocks, searchQuery, sectorFilter, sortBy, period]);

  const visible = filtered.slice(0, limit);
  const hasMore = filtered.length > limit;

  return (
    <Card padding="md" className="md:p-5">
      {/* === Filters row === */}
      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
        {/* Search input */}
        <div
          className="flex items-center flex-1 min-w-[180px]"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            border: '1.5px solid var(--text-primary)',
            borderRadius: '9999px',
            padding: 'var(--sp-2) var(--sp-3)',
            gap: 'var(--sp-2)',
          }}
        >
          <Search size={14} style={{ color: 'var(--text-muted)' }} />
          <input
            type="text"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            placeholder="Поиск тикера или названия"
            className="flex-1 bg-transparent outline-none"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              minWidth: 0,
            }}
          />
        </div>
        <Dropdown<string>
          options={sectorOptions}
          value={sectorFilter}
          onChange={setSectorFilter}
        />
        <Dropdown<ScreenerPeriod>
          options={periodOptions}
          value={period}
          onChange={setPeriod}
        />
        <Dropdown<ScreenerSort>
          options={sortOptions}
          value={sortBy}
          onChange={setSortBy}
        />
      </div>

      {/* === Table === */}
      <div className="overflow-x-auto -mx-2">
        <table className="w-full" style={{ minWidth: 560 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
              <th
                className="text-left px-2 py-2 text-xs uppercase"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                Тикер
              </th>
              <th
                className="text-left px-2 py-2 text-xs uppercase hidden md:table-cell"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                Название
              </th>
              <th
                className="text-right px-2 py-2 text-xs uppercase"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                Цена
              </th>
              <th
                className="text-right px-2 py-2 text-xs uppercase"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                {period === '1d' ? '1д' : period === '1w' ? '1н' : period === '1m' ? '1м' : '1г'}
              </th>
              <th
                className="text-right px-2 py-2 text-xs uppercase hidden md:table-cell"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                Объём
              </th>
              <th
                className="text-right px-2 py-2 text-xs uppercase hidden lg:table-cell"
                style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}
              >
                Кап-ция
              </th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td
                  colSpan={6}
                  className="text-center py-6 text-sm"
                  style={{ color: 'var(--text-muted)' }}
                >
                  Ничего не найдено
                </td>
              </tr>
            ) : (
              visible.map(s => {
                const ch = getChange(s);
                const isUp = ch >= 0;
                const chColor = isUp ? 'var(--success)' : 'var(--danger)';
                return (
                  <tr
                    key={s.secId}
                    style={{ borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)' }}
                    className="hover:bg-white/[0.03] transition-colors"
                  >
                    <td className="px-2 py-2">
                      <Link
                        to="/heatmap"
                        className="text-sm font-semibold"
                        style={{
                          color: 'var(--text-primary)',
                          fontFamily: "'IBM Plex Mono', monospace",
                        }}
                      >
                        {s.secId}
                      </Link>
                    </td>
                    <td className="px-2 py-2 hidden md:table-cell">
                      <span
                        className="text-xs truncate block"
                        style={{ color: 'var(--text-secondary)', maxWidth: 180 }}
                        title={s.name}
                      >
                        {s.name}
                      </span>
                    </td>
                    <td
                      className="text-right px-2 py-2 text-sm"
                      style={{
                        color: 'var(--text-primary)',
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {formatNumber(s.price, s.price < 100 ? 2 : 0)}
                    </td>
                    <td
                      className="text-right px-2 py-2 text-sm font-semibold"
                      style={{
                        color: chColor,
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {Number.isFinite(ch) ? `${isUp ? '+' : ''}${ch.toFixed(2)}%` : '—'}
                    </td>
                    <td
                      className="text-right px-2 py-2 text-xs hidden md:table-cell"
                      style={{
                        color: 'var(--text-secondary)',
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {s.value_1d > 0 ? formatMoney(s.value_1d) : '—'}
                    </td>
                    <td
                      className="text-right px-2 py-2 text-xs hidden lg:table-cell"
                      style={{
                        color: 'var(--text-secondary)',
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontVariantNumeric: 'tabular-nums',
                      }}
                    >
                      {s.market_cap > 0 ? formatMoney(s.market_cap) : '—'}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* === "Показать ещё" === */}
      {hasMore && (
        <div className="flex justify-center mt-4">
          <button
            onClick={() => setLimit(l => l + 20)}
            className="editorial-press font-semibold rounded-full"
            style={{
              backgroundColor: 'var(--bg-secondary)',
              color: 'var(--text-primary)',
              border: '1.5px solid var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
            }}
          >
            Показать ещё ({filtered.length - limit})
          </button>
        </div>
      )}
    </Card>
  );
}
