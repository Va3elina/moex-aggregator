/**
 * EmbedBuffett — виджет индикатора Баффетта (рыночный, тикер не нужен).
 * База (Кап/ВВП ↔ Кап/M2) и период выбираются в drawer'е настроек.
 *
 * Оси свопнуты как на странице: коэффициент — на ПРАВОЙ оси (secondaryData),
 * капитализация — на ЛЕВОЙ (data), reverseLegend.
 */
import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import { getBuffettCapGdp, getBuffettCapM2, type BuffettPeriod } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup } from './EmbedSettings';

type ViewMode = 'cap-gdp' | 'cap-m2';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Series = { time: string; value: number }[];

const PERIODS: { id: BuffettPeriod; label: string }[] = [
  { id: '1y', label: '1Г' },
  { id: '3y', label: '3Г' },
  { id: '5y', label: '5Л' },
  { id: '10y', label: '10Л' },
  { id: '20y', label: '20Л' },
  { id: 'all', label: 'Всё' },
];

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedBuffett() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [viewMode, setViewMode] = useState<ViewMode>(() =>
    (params.get('mode') === 'cap-m2' || readLS('frame:embed:buffett:mode', 'cap-gdp') === 'cap-m2') ? 'cap-m2' : 'cap-gdp',
  );
  const [period, setPeriod] = useState<BuffettPeriod>(() =>
    (params.get('period') || readLS('frame:embed:buffett:period', '10y')) as BuffettPeriod,
  );

  const [cap, setCap] = useState<Series>([]);
  const [ratio, setRatio] = useState<Series>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { try { localStorage.setItem('frame:embed:buffett:mode', viewMode); } catch { /* quota */ } }, [viewMode]);
  useEffect(() => { try { localStorage.setItem('frame:embed:buffett:period', period); } catch { /* quota */ } }, [period]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    const load =
      viewMode === 'cap-gdp'
        ? getBuffettCapGdp(period, false, '1m').then((r) => ({
            cap: (r?.data ?? []).map((d) => ({ time: d.date, value: d.cap ?? 0 })),
            ratio: (r?.data ?? []).map((d) => ({ time: d.date, value: d.buffett ?? 0 })),
          }))
        : getBuffettCapM2(period, false, '1m').then((r) => ({
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
    return () => { cancelled = true; };
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
  const ratioVal = (v: number) => (viewMode === 'cap-gdp' ? `${v.toFixed(2)}%` : `${(v * 100).toFixed(2)}%`);
  const ratioAxis = (v: number) => (viewMode === 'cap-gdp' ? `${v.toFixed(1)}%` : `${(v * 100).toFixed(1)}%`);

  return (
    <EmbedShell
      settings={settings}
      title="Индикатор Баффетта"
      subtitle={viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2'}
      drawer={
        <>
          <DrawerSection label="База сравнения">
            <SegGroup
              value={viewMode}
              options={[{ id: 'cap-gdp', label: 'Кап / ВВП' }, { id: 'cap-m2', label: 'Кап / M2' }]}
              onChange={(v) => setViewMode(v)}
            />
          </DrawerSection>
          <DrawerSection label="Период">
            <SegGroup value={period} options={PERIODS} onChange={(v) => setPeriod(v)} />
          </DrawerSection>
        </>
      }
    >
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
    </EmbedShell>
  );
}
