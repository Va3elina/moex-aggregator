/**
 * EmbedBuffett — виджет индикатора Баффетта (рыночный, тикер не нужен).
 * База (Кап/ВВП ↔ Кап/M2) и период выбираются в drawer'е настроек.
 *
 * Оси свопнуты как на странице: коэффициент — на ПРАВОЙ оси (secondaryData),
 * капитализация — на ЛЕВОЙ (data), reverseLegend.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import {
  getBuffettCapGdp,
  getBuffettCapM2,
  type BuffettCapGdpPoint,
  type BuffettPeriod,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup, ToggleRow } from './EmbedSettings';
import { EmbedFrame, PillGroup, Dropdown, PeriodReadout, WheelHint } from './EmbedToolbar';
import { readLS, writeLS, readBoolLS } from './embedPersist';

type ViewMode = 'cap-gdp' | 'cap-m2';
type Timeframe = '1d' | '1w' | '1m';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Point = { time: string; value: number };
type Series = Point[];

const PERIODS: { id: BuffettPeriod; label: string }[] = [
  { id: '1y', label: '1Г' },
  { id: '3y', label: '3Г' },
  { id: '5y', label: '5Л' },
  { id: '10y', label: '10Л' },
  { id: '20y', label: '20Л' },
  { id: 'all', label: 'Всё' },
];

const TIMEFRAMES: { id: Timeframe; label: string }[] = [
  { id: '1d', label: '1Д' },
  { id: '1w', label: '1Н' },
  { id: '1m', label: '1М' },
];

// '' → прогноз выкл; '10'..'110' (шаг 10) → целевой Кап/ВВП в %.
const FORECASTS: { id: string; label: string }[] = [
  { id: '', label: 'Выкл' },
  ...[10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110].map((v) => ({
    id: String(v),
    label: `Прогноз ${v}%`,
  })),
];

export default function EmbedBuffett() {
  const [params] = useSearchParams();

  const [viewMode, setViewMode] = useState<ViewMode>(() =>
    (params.get('mode') === 'cap-m2' || readLS('frame:embed:buffett:mode', 'cap-gdp') === 'cap-m2') ? 'cap-m2' : 'cap-gdp',
  );
  const [period, setPeriod] = useState<BuffettPeriod>(() =>
    (params.get('period') || readLS('frame:embed:buffett:period', '10y')) as BuffettPeriod,
  );
  const [timeframe, setTimeframe] = useState<Timeframe>(() => {
    const v = params.get('timeframe') || readLS('frame:embed:buffett:timeframe', '1m');
    return v === '1d' || v === '1w' ? v : '1m';
  });
  // Прогноз (только cap-gdp): null = выкл, иначе целевой Кап/ВВП в %.
  const [forecastTarget, setForecastTarget] = useState<number | null>(() => {
    const v = params.get('forecast') ?? readLS('frame:embed:buffett:forecast', '');
    const n = Number(v);
    return v && Number.isFinite(n) ? n : null;
  });
  const [showCap, setShowCap] = useState<boolean>(() =>
    readBoolLS('frame:embed:buffett:showCap', true),
  );

  // Сырые точки cap-gdp нужны для клиентского прогноза (требуется gdp_ttm),
  // поэтому держим полный ответ, а не только cap/ratio как у cap-m2.
  const [capGdpRaw, setCapGdpRaw] = useState<BuffettCapGdpPoint[]>([]);
  const [cap, setCap] = useState<Series>([]);
  const [ratio, setRatio] = useState<Series>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { writeLS('frame:embed:buffett:mode', viewMode); }, [viewMode]);
  useEffect(() => { writeLS('frame:embed:buffett:period', period); }, [period]);
  useEffect(() => { writeLS('frame:embed:buffett:timeframe', timeframe); }, [timeframe]);
  useEffect(() => { writeLS('frame:embed:buffett:forecast', forecastTarget !== null ? String(forecastTarget) : ''); }, [forecastTarget]);
  useEffect(() => { writeLS('frame:embed:buffett:showCap', String(showCap)); }, [showCap]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    const load =
      viewMode === 'cap-gdp'
        ? getBuffettCapGdp(period, false, timeframe).then((r) => {
            const rows = r?.data ?? [];
            return {
              raw: rows,
              cap: rows.map((d) => ({ time: d.date, value: d.cap ?? 0 })),
              ratio: rows.map((d) => ({ time: d.date, value: d.buffett ?? 0 })),
            };
          })
        : getBuffettCapM2(period, false, timeframe).then((r) => ({
            raw: [] as BuffettCapGdpPoint[],
            cap: (r?.data ?? []).map((d) => ({ time: d.date, value: d.cap ?? 0 })),
            ratio: (r?.data ?? []).map((d) => ({ time: d.date, value: d.ratio ?? 0 })),
          }));
    load
      .then((s) => {
        if (cancelled) return;
        setCapGdpRaw(s.raw);
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
  }, [viewMode, period, timeframe]);

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

  // Клиентский прогноз (только cap-gdp): к cap (primary в графике) и ratio
  // (secondary) дописываем 12 синтетических месячных точек, тянущих серию к
  // целевому Кап/ВВП = forecastTarget% от последнего gdp_ttm. Формула —
  // verbatim из BuffettPage.capGdpChartData (seed ratio = target*7+13,
  // seed cap = target*11+37). В графике data=cap, secondaryData=ratio.
  const projected = useMemo<{ cap: Series; ratio: Series }>(() => {
    if (viewMode !== 'cap-gdp' || forecastTarget === null || capGdpRaw.length === 0) {
      return { cap, ratio };
    }
    const last = capGdpRaw[capGdpRaw.length - 1];
    if (!(last.gdp_ttm > 0)) return { cap, ratio };

    const nextCap: Series = [...cap];
    const nextRatio: Series = [...ratio];

    const targetCap = (last.gdp_ttm * forecastTarget) / 100;
    const currentRatio = last.buffett;
    const currentCap = last.cap;
    const lastDate = new Date(last.date);
    const steps = 12;
    const ratioDiff = forecastTarget - currentRatio;
    const capDiff = targetCap - currentCap;
    // Разный шум для ratio и cap (разные seed) — как на странице.
    const seedR = forecastTarget * 7 + 13;
    const seedC = forecastTarget * 11 + 37;
    for (let i = 1; i <= steps; i++) {
      const t = i / steps;
      const noiseR = Math.sin(seedR * i * 0.7) * 0.2 * (1 - t * 0.5);
      const noiseC = Math.sin(seedC * i * 0.9 + 2) * 0.18 * (1 - t * 0.5);
      const stepDate = new Date(lastDate);
      stepDate.setMonth(stepDate.getMonth() + i);
      const stepDateStr = stepDate.toISOString().slice(0, 10);
      // primary(страница) = ratio; secondary(страница) = cap.
      nextRatio.push({ time: stepDateStr, value: currentRatio + ratioDiff * Math.max(0, t + noiseR) });
      nextCap.push({ time: stepDateStr, value: currentCap + capDiff * Math.max(0, t + noiseC) });
    }
    return { cap: nextCap, ratio: nextRatio };
  }, [viewMode, forecastTarget, capGdpRaw, cap, ratio]);

  const showForecast = viewMode === 'cap-gdp' && forecastTarget !== null;
  const periodLabel = PERIODS.find((p) => p.id === period)?.label ?? '';

  // Shift+колесо: гориз. зум по PERIODS (1Г…Всё). dir=+1 короче, -1 длиннее.
  const zoomPeriod = (dir: 1 | -1) => {
    const i = PERIODS.findIndex((p) => p.id === period);
    if (i < 0) return;
    const ni = Math.min(PERIODS.length - 1, Math.max(0, i - dir));
    if (PERIODS[ni] && PERIODS[ni].id !== period) setPeriod(PERIODS[ni].id);
  };

  return (
    <EmbedFrame
      onZoomTime={zoomPeriod}
      toolbar={
        <>
          <Dropdown
            value={viewMode}
            options={[{ id: 'cap-gdp', label: 'Кап / ВВП' }, { id: 'cap-m2', label: 'Кап / M2' }]}
            onChange={(v) => setViewMode(v)}
            title="База сравнения"
          />
          <PillGroup value={timeframe} options={TIMEFRAMES} onChange={(v) => setTimeframe(v)} />
          <PeriodReadout label={periodLabel} />
        </>
      }
      more={
        <>
          {viewMode === 'cap-gdp' && (
            <DrawerSection label="Прогноз">
              <SegGroup
                value={forecastTarget !== null ? String(forecastTarget) : ''}
                options={FORECASTS}
                onChange={(v) => setForecastTarget(v ? Number(v) : null)}
              />
            </DrawerSection>
          )}
          <DrawerSection label="Слои">
            <ToggleRow label="Показывать капитализацию" checked={showCap} onChange={setShowCap} />
          </DrawerSection>
          <WheelHint>
            Период: <b style={{ color: 'var(--text-primary)' }}>{periodLabel}</b> — <b>Shift + колесо</b> над графиком.
            Обычное колесо — высота графика.
          </WheelHint>
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && (
          <SimpleChart
            data={projected.cap}
            secondaryData={projected.ratio}
            height={chartH}
            primaryColor="var(--accent-secondary)"
            secondaryColor="var(--accent)"
            showPrimary={showCap}
            showSecondary
            reverseLegend
            formatValue={(v) => `${v.toFixed(2)} трлн ₽`}
            formatPrimaryAxis={(v) => String(Math.round(v))}
            niceTicks={true}
            // Сетка по ПРАВОЙ оси (ratio), как на странице индикатора.
            niceTicksSecondary={true}
            gridAxis="secondary"
            formatSecondaryValue={ratioVal}
            formatSecondaryAxis={ratioAxis}
            primaryLabel="Капитализация (трлн ₽)"
            secondaryLabel={viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2'}
            forecastCount={showForecast ? 12 : 0}
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
    </EmbedFrame>
  );
}
