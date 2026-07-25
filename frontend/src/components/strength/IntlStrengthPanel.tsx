/**
 * IntlStrengthPanel — «сила рынка» для не-РФ рынков (Индия/Тайвань),
 * admin-only, рендерится StrengthPage.tsx вместо RF-версии при выборе
 * страны. Точная методология RF Strength (% компаний выше EMA цены), но
 * два режима на страну (см. Candles/compute_price_breadth_intl.py):
 *   'index' — голубые фишки (кураторский список, для NSE ≈ Nifty50)
 *   'all'   — все тикеры биржи, что есть в candles_intl (для TAIFEX пока
 *             совпадает с 'index' — нет надёжного полного списка
 *             single-stock-futures тикеров биржи)
 * Бенчмарк (IMOEX/NIFTY/TX) выбирается сервером по exchange — не здесь,
 * ровно как для RF бенчмарк определяется валютой, а не отдельным полем.
 */
import { useEffect, useMemo, useState } from 'react';
import SimpleChart from '../SimpleChart';
import Skeleton from '../Skeleton';
import EditorialChip from '../editorial/EditorialChip';
import Dropdown from '../Dropdown';
import { getOiIntlStrength } from '../../services/api';
import type { OiIntlStrengthResponse } from '../../services/api';

export type IntlCountry = 'RF' | 'NSE' | 'TAIFEX';

const EXCHANGE_BY_COUNTRY: Record<Exclude<IntlCountry, 'RF'>, 'NSE' | 'TAIFEX'> = {
  NSE: 'NSE',
  TAIFEX: 'TAIFEX',
};

const BENCHMARK_LABEL: Record<string, string> = { IMOEX: 'IMOEX', NIFTY: 'Nifty 50', TX: 'TAIEX' };

const EMA_OPTIONS = [
  { key: '20', label: 'EMA 20' },
  { key: '50', label: 'EMA 50' },
  { key: '100', label: 'EMA 100' },
  { key: '200', label: 'EMA 200' },
];

interface Props {
  country: Exclude<IntlCountry, 'RF'>;
}

export default function IntlStrengthPanel({ country }: Props) {
  const exchange = EXCHANGE_BY_COUNTRY[country];
  const [universe, setUniverse] = useState<'index' | 'all'>('index');
  const [emaPeriod, setEmaPeriod] = useState(50);
  const [resp, setResp] = useState<OiIntlStrengthResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getOiIntlStrength(exchange, universe, emaPeriod)
      .then(setResp)
      .catch(() => setResp(null))
      .finally(() => setLoading(false));
  }, [exchange, universe, emaPeriod]);

  const benchmarkByDate = useMemo(() => {
    const m = new Map<string, number>();
    for (const p of resp?.benchmark_data ?? []) m.set(p.date, p.close);
    return m;
  }, [resp]);
  const hasBenchmark = benchmarkByDate.size > 0;
  const data = resp?.data ?? [];
  const latest = data.length > 0 ? data[data.length - 1] : null;
  const benchmarkLabel = resp ? (BENCHMARK_LABEL[resp.benchmark] ?? resp.benchmark) : '';

  return (
    <div className="px-1 md:px-4 py-3">
      <div className="flex flex-wrap items-center justify-between mb-3" style={{ gap: 'var(--sp-2)' }}>
        <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
          <EditorialChip active={universe === 'index'} onClick={() => setUniverse('index')} size="sm">
            Активы из индекса
          </EditorialChip>
          <EditorialChip active={universe === 'all'} onClick={() => setUniverse('all')} size="sm">
            Все активы
          </EditorialChip>
        </div>
        <Dropdown<string> options={EMA_OPTIONS} value={String(emaPeriod)} onChange={(v) => setEmaPeriod(Number(v))} />
      </div>

      {latest && (
        <div
          className="font-bold mb-3"
          style={{
            color: 'var(--text-primary)',
            fontSize: 'clamp(1.5rem, 3vw, 2.25rem)',
            letterSpacing: '-0.02em',
            fontFamily: "'IBM Plex Mono', monospace",
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {latest.percent_above}%
          <span className="text-sm font-normal ml-2" style={{ color: 'var(--text-muted)', fontFamily: 'inherit' }}>
            ({latest.count_above} из {latest.count_total}, {latest.date})
          </span>
        </div>
      )}

      {loading ? (
        <Skeleton height={400} rounded="lg" />
      ) : data.length === 0 ? (
        <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
          Нет истории для EMA{emaPeriod} ({universe === 'index' ? 'активы из индекса' : 'все активы'}) —
          запустите Candles/compute_price_breadth_intl.py
        </p>
      ) : (
        <SimpleChart
          data={data.map((d) => ({ time: d.date, value: d.percent_above }))}
          secondaryData={
            hasBenchmark
              ? data.filter((d) => benchmarkByDate.has(d.date)).map((d) => ({ time: d.date, value: benchmarkByDate.get(d.date)! }))
              : undefined
          }
          showSecondary={hasBenchmark}
          primaryColor="var(--accent)"
          secondaryColor="var(--text-muted)"
          primaryLabel={`% выше EMA${emaPeriod}`}
          secondaryLabel={benchmarkLabel}
          formatValue={(v) => `${v.toFixed(1)}%`}
          formatSecondaryAxis={(v) => v.toLocaleString('ru-RU')}
          showValueHeader={false}
          legendPosition="top"
          showDownloadButton={false}
          showNavigator={true}
          hideTime={true}
          height={450}
        />
      )}
    </div>
  );
}
