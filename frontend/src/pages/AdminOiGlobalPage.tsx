/**
 * AdminOiGlobalPage — admin-only страница с международным Open Interest
 * (CFTC/NSE/Eurex/TAIFEX) и «Силой рынка по ОИ» — breadth по реальным
 * компаниям (NSE + TAIFEX голубые фишки), CFTC/Eurex-индексы в breadth не
 * участвуют — см. Candles/compute_oi_intl_strength.py.
 *
 * Только для role=admin (Вадим + Саша Тория) — не публичная фича.
 * Source endpoints (все role=admin): /api/admin/oi-intl/*
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Globe2 } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import EditorialChip from '../components/editorial/EditorialChip';
import { useAuth } from '../contexts/AuthContext';
import {
  getOiIntlAssets,
  getOiIntlCategories,
  getOiIntlHistory,
  getOiIntlCandles,
  getOiIntlStrengthHistory,
  getOiIntlPriceBreadth,
} from '../services/api';
import type {
  OiIntlAsset, OiIntlHistoryPoint, OiIntlCandlePoint, OiIntlStrengthPoint, PriceBreadthResponse,
} from '../services/api';

const EMA_OPTIONS = [
  { key: '50', label: 'EMA 50' },
  { key: '100', label: 'EMA 100' },
  { key: '200', label: 'EMA 200' },
];

// Свечи (candles_intl) сейчас есть только для NSE/TAIFEX — остальные страны
// оставлены в фильтре ради навигации по «сырому» OI (CFTC/Eurex), просто
// график там будет без ценовой линии.
const COUNTRY_OPTIONS = [
  { key: 'ALL', label: 'Все страны' },
  { key: 'IN', label: '🇮🇳 Индия (NSE)' },
  { key: 'TW', label: '🇹🇼 Тайвань (TAIFEX)' },
  { key: 'US', label: '🇺🇸 США (CFTC)' },
  { key: 'DE', label: '🇩🇪 Германия (Eurex)' },
];

// Только рынки, где breadth считается по РЕАЛЬНЫМ отдельным компаниям —
// CFTC (индексы/товары/валюты/ставки) и "чистый" Eurex (DAX/Bund/Bobl/
// Schatz/Buxl/STOXX50) сознательно не включены: это не компании, смешивать
// их в одну % не является осмысленной breadth (см. compute_oi_intl_strength.py).
const STRENGTH_EXCHANGE_OPTIONS = [
  { key: 'ALL', label: 'Все компании (NSE + TAIFEX)' },
  { key: 'NSE', label: 'NSE — голубые фишки Индии' },
  { key: 'TAIFEX', label: 'TAIFEX — голубые фишки Тайваня' },
];

// «Сила рынка» ПО ЦЕНЕ — точный аналог RF-индикатора Strength
// (Candles/compute_breadth_history.py), просто на international-данных.
// В отличие от секции выше (та — по ОИ) здесь EMA_PERIODS совпадают с RF
// один в один (20/50/100/200) — см. Candles/compute_price_breadth_intl.py.
const PRICE_BREADTH_MARKETS = [
  { key: 'RF', label: '🇷🇺 Россия' },
  { key: 'NSE', label: '🇮🇳 Индия' },
  { key: 'TAIFEX', label: '🇹🇼 Тайвань' },
] as const;

const BENCHMARK_OPTIONS = [
  { key: 'IMOEX', label: 'IMOEX' },
  { key: 'NIFTY', label: 'Nifty 50' },
  { key: 'TX', label: 'TAIEX' },
];

const EMA_OPTIONS_PRICE = [
  { key: '20', label: 'EMA 20' },
  { key: '50', label: 'EMA 50' },
  { key: '100', label: 'EMA 100' },
  { key: '200', label: 'EMA 200' },
];

const MARKET_COLORS: Record<string, string> = {
  RF: 'var(--accent)',
  NSE: 'var(--funds-flow-positive)',
  TAIFEX: 'var(--funds-flow-negative)',
};

export default function AdminOiGlobalPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  if (authLoading || !user || user.role !== 'admin') {
    return null;
  }

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">
      <div className="flex items-start gap-3 mb-6 md:mb-8">
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 44, height: 44,
            borderRadius: 'var(--radius-md, 8px)',
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <Globe2 size={22} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Международный Open Interest
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            CFTC / NSE / Eurex · только для администратора
          </p>
        </div>
      </div>

      <AssetHistorySection />
      <StrengthSection />
      <PriceBreadthSection />
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 1 — история ОИ по выбранному активу/категории
// ════════════════════════════════════════════════════════════════════════════

function AssetHistorySection() {
  const [assets, setAssets] = useState<OiIntlAsset[]>([]);
  const [assetsLoading, setAssetsLoading] = useState(true);
  const [country, setCountry] = useState('ALL');
  const [selectedKey, setSelectedKey] = useState<string>('');
  const [categories, setCategories] = useState<string[]>(['TOTAL']);
  const [category, setCategory] = useState('TOTAL');
  const [history, setHistory] = useState<OiIntlHistoryPoint[]>([]);
  const [candles, setCandles] = useState<OiIntlCandlePoint[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    getOiIntlAssets()
      .then((r) => {
        setAssets(r.assets);
        if (r.assets.length > 0) {
          setSelectedKey(`${r.assets[0].exchange}::${r.assets[0].asset_code}`);
        }
      })
      .catch(() => setAssets([]))
      .finally(() => setAssetsLoading(false));
  }, []);

  const filteredAssets = useMemo(
    () => (country === 'ALL' ? assets : assets.filter((a) => a.country === country)),
    [assets, country]
  );

  // Смена страны — если текущий выбранный актив выпал из фильтра, берём первый доступный.
  useEffect(() => {
    if (filteredAssets.length === 0) return;
    const stillValid = filteredAssets.some((a) => `${a.exchange}::${a.asset_code}` === selectedKey);
    if (!stillValid) {
      setSelectedKey(`${filteredAssets[0].exchange}::${filteredAssets[0].asset_code}`);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filteredAssets]);

  const selected = useMemo(() => {
    const [exchange, assetCode] = selectedKey.split('::');
    return { exchange, assetCode };
  }, [selectedKey]);

  useEffect(() => {
    if (!selected.exchange || !selected.assetCode) return;
    getOiIntlCategories(selected.exchange, selected.assetCode)
      .then((r) => {
        setCategories(r.categories.length > 0 ? r.categories : ['TOTAL']);
        setCategory('TOTAL');
      })
      .catch(() => setCategories(['TOTAL']));
  }, [selected.exchange, selected.assetCode]);

  useEffect(() => {
    if (!selected.exchange || !selected.assetCode) return;
    setLoading(true);
    Promise.all([
      getOiIntlHistory(selected.exchange, selected.assetCode, category),
      // Свечи есть только для NSE/TAIFEX — для остальных бирж endpoint просто
      // вернёт пустой data, чарт тогда рисуется без ценовой линии.
      getOiIntlCandles(selected.exchange, selected.assetCode).catch(() => ({ data: [] as OiIntlCandlePoint[] })),
    ])
      .then(([histRes, candlesRes]) => {
        setHistory(histRes.data);
        setCandles(candlesRes.data);
      })
      .catch(() => {
        setHistory([]);
        setCandles([]);
      })
      .finally(() => setLoading(false));
  }, [selected.exchange, selected.assetCode, category]);

  const assetOptions = filteredAssets.map((a) => ({
    key: `${a.exchange}::${a.asset_code}`,
    label: `${a.asset_name} (${a.exchange})`,
  }));
  const categoryOptions = categories.map((c) => ({ key: c, label: c }));

  // Цена — только там, где хоть одна точка реально не null (close либо settlement_price).
  const priceByDate = useMemo(() => {
    const m = new Map<string, number>();
    for (const c of candles) {
      const v = c.close ?? c.settlement_price;
      if (v !== null) m.set(c.date, v);
    }
    return m;
  }, [candles]);
  const hasPrice = priceByDate.size > 0;

  return (
    <Card padding="md" className="md:p-5 mb-6 md:mb-8">
      <div className="flex flex-wrap items-center justify-between mb-4" style={{ gap: 'var(--sp-2)' }}>
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Открытый интерес по активу{hasPrice ? ' + цена' : ''}
        </span>
        <Dropdown<string> options={COUNTRY_OPTIONS} value={country} onChange={setCountry} />
      </div>

      {assetsLoading ? (
        <Skeleton height={320} rounded="lg" />
      ) : filteredAssets.length === 0 ? (
        <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
          {assets.length === 0
            ? 'Данных ещё нет — запустите фетчеры (IntlOI/fetch_cftc.py, fetch_nse.py, fetch_eurex.py, fetch_taifex.py)'
            : 'Нет активов для этой страны'}
        </p>
      ) : (
        <>
          <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
            <Dropdown<string> options={assetOptions} value={selectedKey} onChange={setSelectedKey} />
            <Dropdown<string> options={categoryOptions} value={category} onChange={setCategory} />
          </div>
          {loading ? (
            <Skeleton height={320} rounded="lg" />
          ) : history.length === 0 ? (
            <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
              Нет истории для этого сочетания
            </p>
          ) : (
            <SimpleChart
              data={
                hasPrice
                  ? history.filter((h) => priceByDate.has(h.date)).map((h) => ({ time: h.date, value: priceByDate.get(h.date)! }))
                  : history.map((h) => ({ time: h.date, value: h.oi_total }))
              }
              secondaryData={hasPrice ? history.map((h) => ({ time: h.date, value: h.oi_total })) : undefined}
              showSecondary={hasPrice}
              primaryColor="var(--accent)"
              secondaryColor="var(--accent-secondary)"
              primaryLabel={hasPrice ? 'Цена (передний месяц)' : 'Открытый интерес'}
              secondaryLabel="OI"
              formatValue={(v) => (hasPrice ? v.toLocaleString('ru-RU') : Math.round(v).toLocaleString('ru-RU'))}
              formatSecondaryAxis={(v) => Math.round(v).toLocaleString('ru-RU')}
              showValueHeader={false}
              legendPosition="top"
              showDownloadButton={false}
              showNavigator={false}
              hideTime={true}
              height={320}
            />
          )}
        </>
      )}
    </Card>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 2 — сила рынка по ОИ (% активов выше EMA)
// ════════════════════════════════════════════════════════════════════════════

function StrengthSection() {
  const [exchange, setExchange] = useState('ALL');
  const [emaPeriod, setEmaPeriod] = useState(200);
  const [data, setData] = useState<OiIntlStrengthPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getOiIntlStrengthHistory(exchange, emaPeriod)
      .then((r) => setData(r.data))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [exchange, emaPeriod]);

  const latest = data.length > 0 ? data[data.length - 1] : null;

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex flex-wrap items-center justify-between mb-4" style={{ gap: 'var(--sp-2)' }}>
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Сила рынка по ОИ — % активов выше своей EMA
        </span>
        <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
          <Dropdown<string> options={STRENGTH_EXCHANGE_OPTIONS} value={exchange} onChange={setExchange} />
          <Dropdown<string>
            options={EMA_OPTIONS}
            value={String(emaPeriod)}
            onChange={(v) => setEmaPeriod(Number(v))}
          />
        </div>
      </div>

      {latest && (
        <div
          className="font-bold mb-4"
          style={{
            color: 'var(--text-primary)',
            fontSize: 'clamp(1.5rem, 3vw, 2.25rem)',
            letterSpacing: '-0.02em',
            fontFamily: "'IBM Plex Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {latest.percent_above}%
          <span
            className="text-sm font-normal ml-2"
            style={{ color: 'var(--text-muted)', fontFamily: 'inherit' }}
          >
            ({latest.count_above} из {latest.count_total} активов, {latest.date})
          </span>
        </div>
      )}

      {loading ? (
        <Skeleton height={280} rounded="lg" />
      ) : data.length === 0 ? (
        <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
          Недостаточно истории для EMA{emaPeriod} — запустите Candles/compute_oi_intl_strength.py
        </p>
      ) : (
        <SimpleChart
          data={data.map((d) => ({ time: d.date, value: d.percent_above }))}
          primaryColor="var(--accent)"
          primaryLabel="% активов выше EMA"
          formatValue={(v) => `${v.toFixed(1)}%`}
          showValueHeader={false}
          legendPosition="top"
          showDownloadButton={false}
          showNavigator={false}
          hideTime={true}
          height={280}
        />
      )}
    </Card>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 3 — сила рынка ПО ЦЕНЕ, аналог RF Strength, РФ + Индия + Тайвань
// ════════════════════════════════════════════════════════════════════════════

function PriceBreadthSection() {
  const [markets, setMarkets] = useState<string[]>(['RF', 'NSE', 'TAIFEX']);
  const [benchmark, setBenchmark] = useState('IMOEX');
  const [emaPeriod, setEmaPeriod] = useState(50);
  const [resp, setResp] = useState<PriceBreadthResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const toggleMarket = (key: string) => {
    setMarkets((prev) => (prev.includes(key) ? prev.filter((m) => m !== key) : [...prev, key]));
  };

  useEffect(() => {
    if (markets.length === 0) {
      setResp(null);
      setLoading(false);
      return;
    }
    setLoading(true);
    getOiIntlPriceBreadth(markets, emaPeriod, benchmark)
      .then(setResp)
      .catch(() => setResp(null))
      .finally(() => setLoading(false));
  }, [markets, emaPeriod, benchmark]);

  const benchmarkByDate = useMemo(() => {
    const m = new Map<string, number>();
    for (const p of resp?.benchmark_data ?? []) m.set(p.date, p.close);
    return m;
  }, [resp]);
  const hasBenchmark = benchmarkByDate.size > 0;
  const benchmarkLabel = BENCHMARK_OPTIONS.find((b) => b.key === benchmark)?.label ?? benchmark;

  return (
    <Card padding="md" className="md:p-5 mt-6 md:mt-8">
      <div className="flex flex-wrap items-center justify-between mb-4" style={{ gap: 'var(--sp-2)' }}>
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Сила рынка по цене — аналог RF Strength
        </span>
        <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
          <Dropdown<string> options={BENCHMARK_OPTIONS} value={benchmark} onChange={setBenchmark} />
          <Dropdown<string>
            options={EMA_OPTIONS_PRICE}
            value={String(emaPeriod)}
            onChange={(v) => setEmaPeriod(Number(v))}
          />
        </div>
      </div>

      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
        {PRICE_BREADTH_MARKETS.map((m) => (
          <EditorialChip key={m.key} active={markets.includes(m.key)} onClick={() => toggleMarket(m.key)} size="sm">
            {m.label}
          </EditorialChip>
        ))}
      </div>

      {markets.length === 0 ? (
        <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
          Выберите хотя бы один рынок
        </p>
      ) : loading ? (
        <Skeleton height={220 * markets.length} rounded="lg" />
      ) : (
        <div className="flex flex-col" style={{ gap: 'var(--sp-3)' }}>
          {markets.map((key) => {
            const series = resp?.series[key] ?? [];
            const label = PRICE_BREADTH_MARKETS.find((m) => m.key === key)?.label ?? key;
            if (series.length === 0) {
              return (
                <p key={key} className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
                  {label}: нет истории для EMA{emaPeriod} — запустите Candles/compute_price_breadth_intl.py
                </p>
              );
            }
            const latest = series[series.length - 1];
            return (
              <div key={key}>
                <div className="flex items-baseline gap-2 mb-2">
                  <span className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{label}</span>
                  <span className="text-sm" style={{ color: 'var(--text-muted)', fontFamily: "'IBM Plex Mono', monospace" }}>
                    {latest.percent_above}% ({latest.count_above}/{latest.count_total}, {latest.date})
                  </span>
                </div>
                <SimpleChart
                  data={series.map((d) => ({ time: d.date, value: d.percent_above }))}
                  secondaryData={
                    hasBenchmark
                      ? series.filter((d) => benchmarkByDate.has(d.date)).map((d) => ({ time: d.date, value: benchmarkByDate.get(d.date)! }))
                      : undefined
                  }
                  showSecondary={hasBenchmark}
                  primaryColor={MARKET_COLORS[key] ?? 'var(--accent)'}
                  secondaryColor="var(--text-muted)"
                  primaryLabel={`% выше EMA${emaPeriod}`}
                  secondaryLabel={benchmarkLabel}
                  formatValue={(v) => `${v.toFixed(1)}%`}
                  formatSecondaryAxis={(v) => v.toLocaleString('ru-RU')}
                  showValueHeader={false}
                  legendPosition="top"
                  showDownloadButton={false}
                  showNavigator={false}
                  hideTime={true}
                  height={220}
                />
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
