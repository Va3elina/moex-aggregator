/**
 * OverviewPage — главная для АВТОРИЗОВАННЫХ пользователей.
 *
 * Pulse-стиль аналогично LandingPage, но:
 *  - Персонализированное приветствие с именем user'а
 *  - Нет "Войти / Тарифы" CTA (уже залогинен)
 *  - Секция "Ваш тариф" вместо продающего
 *  - Быстрый доступ к 8 индикаторам
 *
 * Незалогиненные видят LandingPage (routing conditional в App.tsx).
 */
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ArrowRight,
  Gauge,
  Activity,
  Scale,
  Grid3X3,
  BarChart3,
  Wallet,
  LayoutGrid,
  CalendarDays,
  User,
  TrendingUp,
  TrendingDown,
} from 'lucide-react';
import {
  getFearIndex,
  getBreadthCurrent,
  getBuffettCapGdp,
  getHeatmapData,
  getFundsSummary,
  getSeasonalityPrice,
} from '../services/api';
import type { HeatmapStock, FundsSummaryResponse } from '../services/api';
import { useAuth } from '../contexts/AuthContext';

const INDICATORS = [
  { path: '/heatmap', title: 'Карта рынка', desc: 'Акции MOEX по секторам, размер = капитализация', icon: Grid3X3 },
  { path: '/oi', title: 'Открытый интерес', desc: 'Позиции участников по фьючерсам Мосбиржи', icon: BarChart3 },
  { path: '/funds-money', title: 'Деньги в фондах', desc: 'Динамика СЧА и притоки-оттоки фондов', icon: Wallet },
  { path: '/funds-catalog', title: 'Состав фондов', desc: 'Портфели фондов акций и облигаций', icon: LayoutGrid },
  { path: '/strength', title: 'Сила рынка', desc: '% акций выше EMA 50/100/200', icon: Activity },
  { path: '/buffett', title: 'Индикатор Баффетта', desc: 'Капитализация / ВВП + Cap / M2', icon: Scale },
  { path: '/seasonality', title: 'Сезонность', desc: 'Среднее изменение цены по периодам', icon: CalendarDays },
  { path: '/fear', title: 'Индекс страха', desc: 'Настроения инвесторов по 4 метрикам', icon: Gauge },
];

const ROLE_LABELS: Record<string, string> = {
  free: 'Free',
  basic: 'Basic',
  pro: 'Pro',
  premium: 'Premium',
  admin: 'Admin',
  user: 'Free',
};

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
  const { user } = useAuth();

  const [fear, setFear] = useState<number | null>(null);
  const [strength, setStrength] = useState<number | null>(null);
  const [buffett, setBuffett] = useState<number | null>(null);
  const [gainers, setGainers] = useState<HeatmapStock[]>([]);
  const [losers, setLosers] = useState<HeatmapStock[]>([]);
  const [topVolume, setTopVolume] = useState<HeatmapStock[]>([]);
  const [sectors, setSectors] = useState<{ name: string; change: number; count: number }[]>([]);
  const [fundsSummary, setFundsSummary] = useState<FundsSummaryResponse | null>(null);
  // Index / currency tiles (IMOEX, RTSI, USD, CNY, GLD).
  // Храним массив [{secid, label, closes[30]}] → рендерим sparkline + last/prev.
  const [quotes, setQuotes] = useState<Record<string, { label: string; closes: number[] }>>({});
  const [lastUpdate, setLastUpdate] = useState<string>('');

  useEffect(() => {
    Promise.allSettled([
      getFearIndex().then(r => setFear(r.fear_index)),
      getBreadthCurrent(50, 'imoex').then(r => setStrength(r.percent_above)),
      getBuffettCapGdp('1y').then(r => {
        const last = r.data[r.data.length - 1];
        if (last) setBuffett(last.buffett);
      }),
      getHeatmapData().then(r => {
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
      // Quotes для tiles (IMOEX + RTSI + валюты + золото).
      // Параллельные fetch'и через seasonality/price (30 дней = достаточно для sparkline).
      fetchQuotes().then(setQuotes),
    ]).then(() => {
      setLastUpdate(
        new Date().toLocaleTimeString('ru-RU', {
          hour: '2-digit',
          minute: '2-digit',
          timeZone: 'Europe/Moscow',
        })
      );
    });
  }, []);

  // Приоритет: display_name → email local part → "пользователь"
  const displayName = user?.display_name
    || user?.email?.split('@')[0]
    || 'пользователь';

  const roleLabel = user?.role ? ROLE_LABELS[user.role] || user.role : 'Free';

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">

      {/* ═══ QUOTE TILES — IMOEX / RTSI / USD / CNY / Gold ═══
          Live-цифры сверху страницы. Sparkline показывает движение за 30 дней. */}
      {Object.keys(quotes).length > 0 && (
        <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-2 md:gap-3 mb-8">
          {QUOTE_TICKERS.map(t => {
            const q = quotes[t.secid];
            if (!q || q.closes.length < 2) return null;
            return <QuoteTile key={t.secid} label={q.label} closes={q.closes} />;
          })}
        </section>
      )}

      {/* ═══ GREETING HERO ═══ */}
      <section className="flex flex-wrap items-start justify-between gap-4 mb-8 md:mb-10">
        <div>
          <p
            className="text-xs uppercase mb-1"
            style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
          >
            Обзор рынка
          </p>
          <h1
            className="text-3xl md:text-4xl font-bold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.02em', lineHeight: 1.15 }}
          >
            Добро пожаловать,{' '}
            <span style={{ color: 'var(--accent)' }}>{displayName}</span>
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-secondary)' }}>
            {lastUpdate ? <>обновлено в <span style={{ color: 'var(--text-primary)' }}>{lastUpdate}</span> МСК</> : 'загрузка...'}
          </p>
        </div>

        {/* Тариф-бейдж */}
        <Link
          to="/profile"
          className="flex items-center gap-2 px-3 py-2 border transition-all hover:opacity-90"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            borderColor: 'var(--border-color)',
            borderRadius: 'var(--radius-md, 8px)',
          }}
        >
          <User size={16} style={{ color: 'var(--text-secondary)' }} />
          <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>Тариф:</span>
          <span
            className="text-xs font-semibold"
            style={{
              color: roleLabel === 'Admin' ? 'var(--danger)' :
                    roleLabel === 'Pro' || roleLabel === 'Premium' ? 'var(--warning)' :
                    'var(--accent)'
            }}
          >
            {roleLabel}
          </span>
        </Link>
      </section>

      {/* ═══ 3 KEY METRICS ═══ */}
      <section className="grid grid-cols-3 gap-3 md:gap-4 mb-10 md:mb-12">
        <MetricCard
          label="Индекс страха"
          value={fear}
          unit="/ 100"
          color="amber"
          description={fearClassification(fear)}
        />
        <MetricCard
          label="Сила рынка"
          value={strength}
          unit="%"
          color={strength !== null && strength > 50 ? 'green' : 'red'}
          description={strength !== null ? `${strength.toFixed(0)}% акций > EMA50` : ''}
        />
        <MetricCard
          label="Баффетт"
          value={buffett}
          unit="%"
          color="amber"
          description="Cap / GDP"
        />
      </section>

      {/* ═══ TOP MOVERS (Gainers / Losers) ═══
          Рыночный pulse: кто больше всего вырос/упал за день.
          Фетчатся из /api/heatmap/stocks и сортируются по change_1d. */}
      {(gainers.length > 0 || losers.length > 0) && (
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
            <MoversList title="Лидеры роста" stocks={gainers} direction="up" />
            <MoversList title="Лидеры падения" stocks={losers} direction="down" />
          </div>
        </section>
      )}

      {/* ═══ SECTOR PERFORMANCE ═══
          Секторы отсортированы по средней дневной динамике.
          Горизонтальные бары визуализируют magnitude — как в Bloomberg. */}
      {sectors.length > 0 && (
        <section className="mb-10 md:mb-12">
          <div className="flex items-center gap-3 mb-5 md:mb-6">
            <p
              className="text-xs uppercase"
              style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
            >
              Сектора дня
            </p>
            <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
              {sectors.length} секторов
            </span>
          </div>

          <SectorBars sectors={sectors} />
        </section>
      )}

      {/* ═══ TOP VOLUME + FUNDS ═══
          Двухколоночный блок: лидеры оборота + категории фондов.
          Grid-cols-1 на мобиле / 2 на desktop. */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-10 md:mb-12">
        {topVolume.length > 0 && <VolumeList stocks={topVolume} />}
        {fundsSummary && <FundsCategories summary={fundsSummary} />}
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
                    className="font-semibold text-sm md:text-base truncate"
                    style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
                  >
                    {ind.title}
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
// SUBCOMPONENTS (идентичны Landing — можно позже extract'нуть)
// ═══════════════════════════════════════════════════════════

interface MetricCardProps {
  label: string;
  value: number | null;
  unit: string;
  color: 'green' | 'amber' | 'red';
  description?: string;
  decimals?: number;
}

function MetricCard({ label, value, unit, color, description, decimals = 1 }: MetricCardProps) {
  const colorVar = color === 'amber' ? 'var(--warning)' : color === 'red' ? 'var(--danger)' : 'var(--accent)';
  return (
    <div
      className="p-4 md:p-5 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg, 12px)',
      }}
    >
      <p
        className="text-[10px] md:text-xs uppercase mb-2"
        style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
      >
        {label}
      </p>
      <div className="flex items-baseline gap-1 mb-1">
        <span
          className="text-2xl md:text-4xl font-bold"
          style={{
            color: colorVar,
            fontFamily: "'IBM Plex Mono', 'SF Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
            letterSpacing: '-0.02em',
          }}
        >
          {value !== null ? value.toFixed(decimals) : '—'}
        </span>
        <span className="text-xs md:text-sm" style={{ color: 'var(--text-muted)' }}>
          {unit}
        </span>
      </div>
      {description && (
        <p className="text-[10px] md:text-xs truncate" style={{ color: 'var(--text-secondary)' }}>
          {description}
        </p>
      )}
    </div>
  );
}

/** Quote tile — компактная плитка с тикером, ценой, % изменением и sparkline.
    Sparkline = inline SVG path за ~30 дней, без осей (просто силуэт).
    Цвет линии зелёный если close[last] > close[first], красный если меньше. */
function QuoteTile({ label, closes }: { label: string; closes: number[] }) {
  const last = closes[closes.length - 1];
  const prev = closes[closes.length - 2];
  const first = closes[0];
  const changeDay = prev ? ((last - prev) / prev) * 100 : 0;
  const changePeriod = first ? ((last - first) / first) * 100 : 0;
  const isUp = changePeriod >= 0;
  const color = isUp ? 'var(--success)' : 'var(--danger)';

  // Sparkline SVG path — нормализация closes в [0,1], y-flip (SVG y=0 наверху).
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const range = max - min || 1;
  const w = 100, h = 24;
  const path = closes
    .map((c, i) => {
      const x = (i / (closes.length - 1)) * w;
      const y = h - ((c - min) / range) * h;
      return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1);
    })
    .join(' ');

  // Формат цены: IMOEX/RTSI без декималов, валюты 2, gold 0
  const priceFmt = last >= 1000 ? last.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) :
                   last.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  return (
    <div
      className="p-3 border relative overflow-hidden"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-md, 8px)',
      }}
    >
      <div className="flex items-center justify-between mb-1">
        <span
          className="text-[10px] uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          {label}
        </span>
        <span
          className="text-[11px] font-semibold"
          style={{
            color,
            fontFamily: "'IBM Plex Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {changeDay > 0 ? '+' : ''}{changeDay.toFixed(2)}%
        </span>
      </div>
      <div
        className="text-lg md:text-xl font-bold mb-1"
        style={{
          color: 'var(--text-primary)',
          fontFamily: "'IBM Plex Mono', monospace",
          fontVariantNumeric: 'tabular-nums',
          letterSpacing: '-0.02em',
        }}
      >
        {priceFmt}
      </div>
      {/* Sparkline */}
      <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" width="100%" height="18" className="opacity-80">
        <path d={path} fill="none" stroke={color} strokeWidth="1.5" />
      </svg>
    </div>
  );
}

/** Топ-5 по обороту дня (value_1d в рублях).
    Формат value: "12,3 млрд ₽" / "450 млн ₽". */
function VolumeList({ stocks }: { stocks: HeatmapStock[] }) {
  return (
    <div
      className="p-4 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg, 12px)',
      }}
    >
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
    </div>
  );
}

/** Сводка по категориям фондов (money_market / stocks / bonds / gold).
    4 строки с NAV + % change за день.*/
function FundsCategories({ summary }: { summary: FundsSummaryResponse }) {
  return (
    <div
      className="p-4 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg, 12px)',
      }}
    >
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
    </div>
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
    <div
      className="p-4 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg, 12px)',
      }}
    >
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
    </div>
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
    <div
      className="p-4 border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
        borderRadius: 'var(--radius-lg, 12px)',
      }}
    >
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
    </div>
  );
}

function fearClassification(fear: number | null): string {
  if (fear === null) return '';
  if (fear <= 20) return 'крайний страх';
  if (fear <= 40) return 'страх';
  if (fear <= 60) return 'нейтрально';
  if (fear <= 80) return 'жадность';
  return 'крайняя жадность';
}
