/**
 * EmbedStrength — виджет «Сила рынка». Движок — LwChartPanes (§5.7 макета):
 * ДВА связанных Lightweight-графика как один индикатор — сверху индекс
 * (IMOEX ₽ / RTSI $), снизу breadth (% акций выше EMA) линией-областью или
 * бинарной гистограммой (зел ≥50% / крас <50%). Между панелями: общая ось
 * времени (пан/зум синхронны), общий кроссхэйр, ЕДИНЫЙ тултип с обоими
 * значениями; ось дат только на нижней.
 *
 * Вся история грузится сразу; период меняется перетаскиванием/зумом оси
 * (кнопок периода нет — как в TradingView/макете). EMA и показ индекса — в ⚙.
 * Виджет целиком под PRO-токеном, поэтому тир-гейтинга нет.
 */
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { LineChart, BarChart3, Landmark, Grid3x3 } from 'lucide-react';
import LwChartPanes, { type LwPane } from '../../components/LwChartPanes';
import { useTheme } from '../../contexts/ThemeContext';
import { getBreadthHistory, type BreadthUniverse } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup, ToggleRow } from './EmbedSettings';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Synced = { time: number; breadth: number; imoex: number }[];
type ChartMode = 'line' | 'histogram';
type Ema = 20 | 50 | 100 | 200;
type UniverseBase = 'imoex' | 'all';
type Currency = 'rub' | 'usd';

const EMAS: { id: Ema; label: string }[] = [
  { id: 20, label: 'EMA 20' },
  { id: 50, label: 'EMA 50' },
  { id: 100, label: 'EMA 100' },
  { id: 200, label: 'EMA 200' },
];
const CHART_MODES: { id: ChartMode; label: string; icon: ReactNode }[] = [
  { id: 'line', label: 'Линия', icon: <LineChart size={14} /> },
  { id: 'histogram', label: 'Гистограмма', icon: <BarChart3 size={14} /> },
];
const UNIVERSE_ICONS: Record<UniverseBase, ReactNode> = {
  imoex: <Landmark size={14} />,
  all: <Grid3x3 size={14} />,
};

// Вся история сразу (время — перетаскиванием оси, как в макете); дефолт-окно ≈ год.
const ALL_DAYS = 7000;
const INITIAL_BARS = 252;

// 'YYYY-MM-DD' → UNIX-секунды (UTC-полночь) для LwChart.
const toSec = (t: string): number => {
  const [y, m, d] = t.slice(0, 10).split('-').map(Number);
  return Math.floor(Date.UTC(y, m - 1, d) / 1000);
};

export default function EmbedStrength() {
  const { rd, wr } = useEmbedPersist();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';
  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  const [ema, setEma] = useState<Ema>(() => (Number(rd('frame:embed:strength:ema', '200')) || 200) as Ema);
  const [chartMode, setChartMode] = useState<ChartMode>(() => rd('frame:embed:strength:chartMode', 'histogram') as ChartMode);
  const [universeBase, setUniverseBase] = useState<UniverseBase>(() => rd('frame:embed:strength:universeBase', 'imoex') as UniverseBase);
  const [currency, setCurrency] = useState<Currency>(() => rd('frame:embed:strength:currency', 'rub') as Currency);
  const [showPrice, setShowPrice] = useState<boolean>(() => rd('frame:embed:strength:showPrice', 'true') !== 'false');

  const [synced, setSynced] = useState<Synced>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { wr('frame:embed:strength:ema', String(ema)); }, [ema]);
  useEffect(() => { wr('frame:embed:strength:chartMode', chartMode); }, [chartMode]);
  useEffect(() => { wr('frame:embed:strength:universeBase', universeBase); }, [universeBase]);
  useEffect(() => { wr('frame:embed:strength:currency', currency); }, [currency]);
  useEffect(() => { wr('frame:embed:strength:showPrice', String(showPrice)); }, [showPrice]);

  // Итоговый universe — как на странице: добавляем _usd в долларовом режиме.
  const universe: BreadthUniverse = currency === 'usd'
    ? `${universeBase}_usd` as BreadthUniverse
    : universeBase;

  // Лейбл верхней панели всегда отражает реально нарисованный индекс
  // (IMOEX в ₽ / RTSI в $) и НЕ зависит от вселенной breadth-метрики.
  const indexLegend = currency === 'usd' ? 'Индекс РТС (RTSI)' : 'Индекс МосБиржи (IMOEX)';

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getBreadthHistory(ema, ALL_DAYS, universe)
      .then((res) => {
        if (cancelled) return;
        // Симметричный мёрж: точка попадает в ряд только если есть ОБА значения
        // (USD-ряд может отставать → иначе окна панелей разъезжаются).
        const imoexMap = new Map((res?.imoex ?? []).map((d) => [d.date, d.close]));
        const s: Synced = (res?.data ?? [])
          .filter((d) => imoexMap.has(d.date))
          .map((d) => ({ time: toSec(d.date), breadth: d.percent_above, imoex: imoexMap.get(d.date)! }));
        setSynced(s);
        setStatus(s.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/strength load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [ema, universe]);

  // Панели §5.7: [индекс?] + breadth. Индекс — синяя линия; breadth — по режиму:
  // area (циан, градиент 22%→0) или бинарная гистограмма (зел ≥50 / крас <50).
  const panes = useMemo<LwPane[]>(() => {
    if (!synced.length) return [];
    const fmtIdx = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 });
    const fmtPct = (v: number) => v.toFixed(1).replace('.', ',') + '%';
    const out: LwPane[] = [];
    if (showPrice) {
      out.push({
        flex: 1.1,
        series: [{
          id: 'idx', type: 'line', color: 'var(--chart-line-1)', lineWidth: 2,
          label: indexLegend,
          data: synced.map((d) => ({ time: d.time, value: d.imoex })),
          tipFmt: fmtIdx, axisFmt: fmtIdx,
        }],
      });
    }
    const breadthLabel = `% акций выше EMA${ema}`;
    out.push({
      flex: showPrice ? 0.9 : 1,
      series: [chartMode === 'line'
        ? {
            id: 'breadth', type: 'area', color: 'var(--oi-cyan)',
            areaTop: 'color-mix(in srgb, var(--oi-cyan) 22%, transparent)', lineWidth: 2,
            label: breadthLabel,
            data: synced.map((d) => ({ time: d.time, value: d.breadth })),
            tipFmt: fmtPct, axisFmt: (v) => Math.round(v) + '%', minMove: 0.1,
          }
        : {
            id: 'breadth', type: 'histogram', color: 'var(--oi-green)', base: 0,
            label: breadthLabel,
            data: synced.map((d) => ({
              time: d.time, value: d.breadth,
              color: d.breadth >= 50 ? 'var(--oi-green)' : 'var(--oi-red)',
            })),
            tipFmt: fmtPct, axisFmt: (v) => Math.round(v) + '%', minMove: 0.1,
          }],
    });
    return out;
  }, [synced, showPrice, chartMode, ema, indexLegend]);

  return (
    <EmbedFrame
      toolbarUnified
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <PillGroup<ChartMode> value={chartMode} options={CHART_MODES} onChange={setChartMode} />
            <Dropdown<UniverseBase>
              value={universeBase}
              options={[
                { id: 'imoex', label: currency === 'usd' ? 'Индекс RTSI' : 'Индекс IMOEX' },
                { id: 'all', label: '100 акций' },
              ]}
              onChange={setUniverseBase}
              title="Вселенная"
              icon={UNIVERSE_ICONS[universeBase]}
            />
            <PillGroup<Currency> value={currency} options={[{ id: 'rub', label: '₽' }, { id: 'usd', label: '$' }]} onChange={setCurrency} />
          </div>
          <PillGroup<ChartMode> value={chartMode} options={CHART_MODES} onChange={setChartMode} compact={toolbarCompact} />
          <Dropdown<UniverseBase>
            value={universeBase}
            options={[
              { id: 'imoex', label: currency === 'usd' ? 'Индекс RTSI' : 'Индекс IMOEX' },
              { id: 'all', label: '100 акций' },
            ]}
            onChange={setUniverseBase}
            title="Вселенная"
            icon={UNIVERSE_ICONS[universeBase]}
            compact={toolbarCompact}
          />
          <PillGroup<Currency> value={currency} options={[{ id: 'rub', label: '₽' }, { id: 'usd', label: '$' }]} onChange={setCurrency} />
        </div>
      }
      more={
        <>
          <DrawerSection label="Скользящая (EMA)">
            <SegGroup<Ema> value={ema} options={EMAS} onChange={setEma} />
          </DrawerSection>
          <DrawerSection label="Индекс сверху">
            <ToggleRow
              label={currency === 'usd' ? 'Показывать RTS' : 'Показывать IMOEX'}
              checked={showPrice}
              onChange={setShowPrice}
            />
          </DrawerSection>
        </>
      }
    >
      <div style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && panes.length > 0 && (
          <LwChartPanes
            panes={panes}
            dark={dark}
            fitKey={`${universe}|${ema}|${showPrice}`}
            initialBars={INITIAL_BARS}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
