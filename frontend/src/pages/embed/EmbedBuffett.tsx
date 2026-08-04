/**
 * EmbedBuffett — виджет индикатора Баффетта (рыночный, тикер не нужен).
 * База (Кап/ВВП ↔ Кап/M2) и период выбираются в drawer'е настроек.
 *
 * Оси свопнуты как на странице: коэффициент — на ПРАВОЙ оси (secondaryData),
 * капитализация — на ЛЕВОЙ (data), reverseLegend.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Scale } from 'lucide-react';
import { monthsYearsTickFmt, type LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes, { type LwChartPanesHandle } from '../../components/LwChartPanes';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getBuffettCapGdp,
  getBuffettCapM2,
  type BuffettCapGdpPoint,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useChartFormat, type ChartFormat } from './EmbedFormat';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';

type ViewMode = 'cap-gdp' | 'cap-m2';
type Timeframe = '1d' | '1w' | '1m';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Point = { time: string; value: number };
type Series = Point[];

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

// Дата 'YYYY-MM-DD' → UNIX-секунды (UTC-полночь) для LwChart. Баффетт всегда на
// дневных/агрегированных датах (не интрадей) → разбор без таймзонного сдвига.
const toSec = (t: string): number => {
  const [y, m, d] = t.slice(0, 10).split('-').map(Number);
  return Math.floor(Date.UTC(y, m - 1, d) / 1000);
};

export default function EmbedBuffett() {
  const { rd, wr, rdBool } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const { fmt, setKind, setColor } = useChartFormat('frame:embed:buffett:fmt');
  const [viewMode, setViewMode] = useState<ViewMode>(() =>
    (params.get('mode') === 'cap-m2' || rd('frame:embed:buffett:mode', 'cap-gdp') === 'cap-m2') ? 'cap-m2' : 'cap-gdp',
  );
  const [timeframe, setTimeframe] = useState<Timeframe>(() => {
    const v = params.get('timeframe') || rd('frame:embed:buffett:timeframe', '1m');
    return v === '1d' || v === '1w' ? v : '1m';
  });
  // Прогноз (только cap-gdp): null = выкл, иначе целевой Кап/ВВП в %.
  const [forecastTarget, setForecastTarget] = useState<number | null>(() => {
    const v = params.get('forecast') ?? rd('frame:embed:buffett:forecast', '');
    const n = Number(v);
    return v && Number.isFinite(n) ? n : null;
  });
  const [showCap, setShowCap] = useState<boolean>(() =>
    rdBool('frame:embed:buffett:showCap', true),
  );

  // Сырые точки cap-gdp нужны для клиентского прогноза (требуется gdp_ttm),
  // поэтому держим полный ответ, а не только cap/ratio как у cap-m2.
  const [capGdpRaw, setCapGdpRaw] = useState<BuffettCapGdpPoint[]>([]);
  const [cap, setCap] = useState<Series>([]);
  const [ratio, setRatio] = useState<Series>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { wr('frame:embed:buffett:mode', viewMode); }, [viewMode]);
  useEffect(() => { wr('frame:embed:buffett:timeframe', timeframe); }, [timeframe]);
  useEffect(() => { wr('frame:embed:buffett:forecast', forecastTarget !== null ? String(forecastTarget) : ''); }, [forecastTarget]);
  useEffect(() => { wr('frame:embed:buffett:showCap', String(showCap)); }, [showCap]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    // Грузим ВСЮ историю: время меняется перетаскиванием/зумом оси дат (LwChart),
    // дискретного периода больше нет (фидбэк Вадима — как в TradingView).
    const load =
      viewMode === 'cap-gdp'
        ? getBuffettCapGdp('all', false, timeframe).then((r) => {
            const rows = r?.data ?? [];
            return {
              raw: rows,
              cap: rows.map((d) => ({ time: d.date, value: d.cap ?? 0 })),
              ratio: rows.map((d) => ({ time: d.date, value: d.buffett ?? 0 })),
            };
          })
        : getBuffettCapM2('all', false, timeframe).then((r) => ({
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
  }, [viewMode, timeframe]);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();
  // Рисование + экспорт графика (см. useDrawTools.tsx) — рыночный индикатор без
  // инструмента → статичный ключ персиста (не per-instrument, как у ОИ).
  const draw = useDrawTools('frame:embed:buffett:draw');
  const lwChartRef = useRef<LwChartPanesHandle>(null);

  // Высота графика НЕ считается: LwChartPanes всегда занимает 100% родителя, а
  // родитель — <div position:absolute;inset:0>. Прежний chartH + ResizeObserver
  // существовали только ради пропа height у LwChart.
  const chartBoxRef = useRef<HTMLDivElement>(null);

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

  // Серии для LwChart: капитализация (левая ось, под toggle showCap) + коэффициент
  // (правая ось, всегда). Прогнозный хвост (последние 12 точек) выносим в отдельную
  // ПУНКТИРНУЮ серию, стыкующуюся с реальной на граничной точке (slice(cut-1)) —
  // проекцию не выдаём за факт. Оси как на странице: cap слева, ratio справа.
  const lwSeries = useMemo<LwSeries[]>(() => {
    const out: LwSeries[] = [];
    const FC = 12;
    const ratioLabel = viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2';
    const capTip = (v: number) => `${v.toFixed(2)} трлн ₽`;
    const capAxis = (v: number) => String(Math.round(v));
    const rTip = (v: number) => (viewMode === 'cap-gdp' ? `${v.toFixed(2)}%` : `${(v * 100).toFixed(2)}%`);
    const rAxis = (v: number) => (viewMode === 'cap-gdp' ? `${v.toFixed(1)}%` : `${(v * 100).toFixed(1)}%`);
    const map = (arr: Series) => arr.map((p) => ({ time: toSec(p.time), value: p.value }));

    // `format` — ⚙ «Формат» (§6) применяется к ОСНОВНОЙ части серии; прогнозный
    // хвост остаётся пунктирной линией (dashed-гистограмм не бывает), но цвет
    // наследует выбранный.
    const pushSplit = (
      pts: Series, id: string, scale: 'left' | 'right', color: string,
      label: string, tipFmt: (v: number) => string, axisFmt: (v: number) => string,
      format?: ChartFormat,
    ) => {
      if (pts.length === 0) return;
      const mk = (d: LwSeries): LwSeries => (format ? applyFormat(d, format) : d);
      if (showForecast && pts.length > FC) {
        const cut = pts.length - FC;
        out.push(mk({ id, type: 'line', scale, color, lineWidth: 2, label, data: map(pts.slice(0, cut)), tipFmt, axisFmt }));
        out.push({ id: `${id}-fore`, type: 'line', scale, color: format?.color ?? color, lineWidth: 2, dashed: true, lastValueVisible: false, label: `${label} · прогноз`, data: map(pts.slice(cut - 1)), tipFmt, axisFmt });
      } else {
        out.push(mk({ id, type: 'line', scale, color, lineWidth: 2, label, data: map(pts), tipFmt, axisFmt }));
      }
    };

    if (showCap) pushSplit(projected.cap, 'cap', 'left', 'var(--accent-secondary)', 'Капитализация (трлн ₽)', capTip, capAxis);
    pushSplit(projected.ratio, 'ratio', 'right', 'var(--accent)', ratioLabel, rTip, rAxis, fmt);
    return out;
  }, [projected, showCap, showForecast, viewMode, fmt]);

  return (
    <EmbedFrame
      toolbarUnified
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <Dropdown
              value={viewMode}
              options={[{ id: 'cap-gdp', label: 'Кап / ВВП' }, { id: 'cap-m2', label: 'Кап / M2' }]}
              onChange={(v) => setViewMode(v)}
              title="База сравнения"
              icon={<Scale size={14} />}
            />
            <PillGroup value={timeframe} options={TIMEFRAMES} onChange={(v) => setTimeframe(v)} />
          </div>
          <Dropdown
            value={viewMode}
            options={[{ id: 'cap-gdp', label: 'Кап / ВВП' }, { id: 'cap-m2', label: 'Кап / M2' }]}
            onChange={(v) => setViewMode(v)}
            title="База сравнения"
            icon={<Scale size={14} />}
            compact={toolbarCompact}
          />
          <PillGroup value={timeframe} options={TIMEFRAMES} onChange={(v) => setTimeframe(v)} />
        </div>
      }
      actions={<DrawExportActions draw={draw} visible={status === 'ok' && lwSeries.length > 0} />}
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
          <FormatSection fmt={fmt} onKind={setKind} onColor={setColor} />
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && lwSeries.length > 0 && (
          <LwChartPanes
            ref={lwChartRef}
            panes={[{ series: lwSeries }]}
            drawPaneIndex={0}
            dark={dark}
            fitKey={`${viewMode}|${timeframe}|${forecastTarget ?? 'off'}`}
            tickFmt={monthsYearsTickFmt}
            drawActive={draw.drawMode}
            drawTool={draw.drawTool}
            drawings={draw.drawings}
            onDrawingsChange={draw.setDrawings}
            drawColor={draw.drawColor}
            drawWidth={draw.drawWidth}
            drawDash={draw.drawDash}
            drawOpacity={draw.drawOpacity}
            selectedDrawId={draw.selectedDrawId}
            onSelectDraw={draw.setSelectedDrawId}
            onSelectionRect={draw.setSelRect}
            onToolReset={draw.onToolReset}
            drawHidden={draw.drawHidden}
            drawLocked={draw.drawLocked}
          />
        )}
        <DrawToolsOverlay draw={draw} visible={status === 'ok' && lwSeries.length > 0} />
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        <ChartExportModal
          draw={draw}
          targetElement={chartBoxRef.current}
          lwChartRef={lwChartRef}
          filename={`frame-buffett-${viewMode}`}
          metadata={{
            title: 'Индикатор Баффетта',
            details: [viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2', TIMEFRAMES.find((t) => t.id === timeframe)?.label].filter((x): x is string => !!x),
          }}
        />
      </div>
    </EmbedFrame>
  );
}
