/**
 * EmbedBuffett — виджет индикатора Баффетта (рыночный, тикер не нужен).
 *
 * Оси свопнуты как на странице: главное значение (коэффициент Кап/ВВП или Кап/M2)
 * на ПРАВОЙ оси (secondaryData), капитализация на ЛЕВОЙ (data), reverseLegend.
 *
 * Параметры (query, опц.): mode (cap-gdp|cap-m2), period.
 * Контрол режима — внутри виджета.
 */
import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import { getBuffettCapGdp, getBuffettCapM2 } from '../../services/api';
import { EmbedMsg, embedColumn, embedHeader, segBtn } from './embedUi';

type ViewMode = 'cap-gdp' | 'cap-m2';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Series = { time: string; value: number }[];

export default function EmbedBuffett() {
  const [params] = useSearchParams();
  const [viewMode, setViewMode] = useState<ViewMode>(
    params.get('mode') === 'cap-m2' ? 'cap-m2' : 'cap-gdp',
  );
  const period = params.get('period') || '10y';

  const [cap, setCap] = useState<Series>([]);
  const [ratio, setRatio] = useState<Series>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    const load =
      viewMode === 'cap-gdp'
        ? getBuffettCapGdp(period as Parameters<typeof getBuffettCapGdp>[0], false, '1m').then((r) => ({
            cap: (r?.data ?? []).map((d) => ({ time: d.date, value: d.cap ?? 0 })),
            ratio: (r?.data ?? []).map((d) => ({ time: d.date, value: d.buffett ?? 0 })),
          }))
        : getBuffettCapM2(period as Parameters<typeof getBuffettCapM2>[0], false, '1m').then((r) => ({
            cap: (r?.data ?? []).map((d) => ({ time: d.date, value: d.cap ?? 0 })),
            ratio: (r?.data ?? []).map((d) => ({ time: d.date, value: d.ratio ?? 0 })),
          }));
    load
      .then((s) => {
        if (cancelled) return;
        setCap(s.cap);
        setRatio(s.ratio);
        setStatus(s.ratio.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/buffett load failed:', err);
        setStatus('error');
      });
    return () => {
      cancelled = true;
    };
  }, [viewMode, period]);

  // Резиновая высота графика.
  const chartBoxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
  useEffect(() => {
    const el = chartBoxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Кап/ВВП: buffett уже в %. Кап/M2: ratio — доля, ×100.
  const ratioVal = (v: number) =>
    viewMode === 'cap-gdp' ? `${v.toFixed(2)}%` : `${(v * 100).toFixed(2)}%`;
  const ratioAxis = (v: number) =>
    viewMode === 'cap-gdp' ? `${v.toFixed(1)}%` : `${(v * 100).toFixed(1)}%`;

  return (
    <div style={embedColumn}>
      <div style={embedHeader}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Индикатор Баффетта</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
          {(['cap-gdp', 'cap-m2'] as const).map((m) => (
            <button key={m} style={segBtn(viewMode === m)} onClick={() => setViewMode(m)}>
              {m === 'cap-gdp' ? 'Кап/ВВП' : 'Кап/M2'}
            </button>
          ))}
        </div>
      </div>

      <div ref={chartBoxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && (
          <SimpleChart
            data={cap}
            secondaryData={ratio}
            height={chartH}
            primaryColor="var(--accent-secondary)"
            secondaryColor="var(--accent)"
            showPrimary
            showSecondary
            reverseLegend
            formatValue={(v) => `${v.toFixed(2)} трлн ₽`}
            formatPrimaryAxis={(v) => `${v.toFixed(2)} трлн ₽`}
            formatSecondaryValue={ratioVal}
            formatSecondaryAxis={ratioAxis}
            primaryLabel="Капитализация (трлн ₽)"
            secondaryLabel={viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2'}
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={false}
            hideTime
            chartPadding={{ left: 70, right: 70 }}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </div>
  );
}
