/**
 * OiIntlPanel — международный Open Interest (CFTC/NSE/Eurex/TAIFEX),
 * admin-only вкладка на реальной странице /oi (OpenInterestPage.tsx), не
 * отдельная страница. Логика перенесена из снесённой AdminOiGlobalPage.tsx
 * (2026-07-25, см. memory international_oi.md) — сами эндпоинты
 * (/api/admin/oi-intl/*) не менялись.
 */
import { useEffect, useMemo, useState } from 'react';
import Skeleton from '../Skeleton';
import Dropdown from '../Dropdown';
import SimpleChart from '../SimpleChart';
import {
  getOiIntlAssets,
  getOiIntlCategories,
  getOiIntlHistory,
  getOiIntlCandles,
} from '../../services/api';
import type { OiIntlAsset, OiIntlHistoryPoint, OiIntlCandlePoint } from '../../services/api';

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

export default function OiIntlPanel() {
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
    <div className="px-1 md:px-4 py-3">
      <div className="flex flex-wrap items-center justify-between mb-4" style={{ gap: 'var(--sp-2)' }}>
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          Международный ОИ{hasPrice ? ' + цена' : ''}
        </span>
        <Dropdown<string> options={COUNTRY_OPTIONS} value={country} onChange={setCountry} />
      </div>

      {assetsLoading ? (
        <Skeleton height={320} rounded="lg" />
      ) : filteredAssets.length === 0 ? (
        <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
          {assets.length === 0
            ? 'Данных ещё нет — запустите фетчеры (IntlOI/fetch_cftc.py, fetch_nse_stocks.py, fetch_eurex.py, fetch_taifex.py)'
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
              showNavigator={true}
              hideTime={true}
              height={450}
            />
          )}
        </>
      )}
    </div>
  );
}
