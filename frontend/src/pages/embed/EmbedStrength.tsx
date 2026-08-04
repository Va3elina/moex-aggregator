/**
 * EmbedStrength — виджет «Сила рынка». Движок — LwChartPanes (§5.7 макета):
 * ДВА связанных Lightweight-графика как один индикатор — сверху индекс
 * (IMOEX ₽ / RTSI $), снизу breadth (% акций выше EMA) линией-областью или
 * бинарной гистограммой (зел ≥50% / крас <50%). Между панелями: общая ось
 * времени (пан/зум синхронны), общий кроссхэйр; курсорный тултип отключён
 * (showTooltip=false — легенда сверху панелей уже даёт значения, Вадим);
 * ось дат только на нижней.
 *
 * Вся история грузится сразу; период меняется перетаскиванием/зумом оси
 * (кнопок периода нет — как в TradingView/макете). EMA и показ индекса — в ⚙.
 * Виджет целиком под PRO-токеном, поэтому тир-гейтинга нет.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { LineChart, BarChart3, Landmark, Grid3x3, TrendingUp } from 'lucide-react';
import LwChartPanes, { type LwPane, type LwChartPanesHandle } from '../../components/LwChartPanes';
// Дефолт оси времени в LwChartPanes сменился на ruTickMark (нужен интрадею ОИ) —
// «Сила рынка» живёт на дневках, поэтому свой формат задаём явно.
import { monthsYearsTickFmt } from '../../components/chart/lwTypes';
import { useTheme } from '../../contexts/ThemeContext';
import { getBreadthHistory, type BreadthUniverse } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';
import {
  useIndicators, useIndicatorSeries, indicatorValues, IndicatorList, PaneIndicatorList,
  IndicatorsButton, type NativeRow,
} from './EmbedIndicators';

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
// ⚠️ Раньше здесь был безусловный ALL_DAYS=7000 — бэкенд (enforce_tier_limits)
// HARD-REJECT'ит days > max_history_days тарифа (403, а не clamp), поэтому free
// (365) и basic (3650) получали не укороченный график, а «Ошибка загрузки».
// Выбираем максимальную глубину, разрешённую текущему тарифу — тот же приём, что
// в EmbedOpenInterest.bestDailyPeriod и в StrengthPage (tier-коррекция периода).
const PERIOD_DAYS_DESC: { period: string; days: number }[] = [
  { period: 'all', days: 7000 },
  { period: '10y', days: 3650 },
  { period: '5y', days: 1825 },
  { period: '1y', days: 365 },
];
function bestHistoryDays(canUsePeriod: (p: string) => boolean): number {
  for (const { period, days } of PERIOD_DAYS_DESC) {
    if (canUsePeriod(period)) return days;
  }
  return 365;
}
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
  const chartBoxRef = useRef<HTMLDivElement>(null);
  // Рисование + экспорт (см. useDrawTools.tsx) — статичный ключ, рыночный
  // индикатор без инструмента. Рисуем всегда на ПЕРВОЙ панели (drawPaneIndex=0):
  // индекс, если showPrice включён, иначе breadth — см. LwChartPanes.
  const draw = useDrawTools('frame:embed:strength:draw');
  const lwChartRef = useRef<LwChartPanesHandle>(null);
  const strengthAccess = useTierAccess('strength');

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

  // Пользовательские индикаторы. Ключ БЕЗ вселенной/валюты: набор индикаторов —
  // предпочтение пользователя, а не свойство конкретного среза данных.
  // Панель breadth участвует в перестановке НАРАВНЕ с индикаторами: иначе
  // «Переместить → Выше/Ниже» у единственного индикатора не с чем менять
  // местами, и пункт просто ничего не делал. Номера панелей — общее
  // пространство, поэтому breadth уходит в reserved, а перестановку обе
  // стороны применяют одну и ту же (см. useIndicators).
  const [breadthPane, setBreadthPane] = useState<number>(() => Number(rd('frame:embed:strength:breadthPane', '1')) || 1);
  useEffect(() => { wr('frame:embed:strength:breadthPane', String(breadthPane)); }, [breadthPane]);
  const reservedPanes = useMemo(() => [breadthPane], [breadthPane]);
  const onSwapPanes = useCallback((a: number, b: number) => {
    setBreadthPane((p) => (p === a ? b : p === b ? a : p));
  }, []);
  const inds = useIndicators('frame:embed:strength:indicators', { reserved: reservedPanes, onSwapPanes });
  // ⚠️ Разбор коллизии номеров. Индикаторы, добавленные ДО того как breadth стал
  // занимать панель, могли сесть на её номер: тогда две разные панели считались
  // «панелью breadth», строка индикатора подменялась строкой breadth и её нельзя
  // было ни настроить, ни удалить. Съезжаем на первый свободный номер — данные
  // пользователя при этом целы, меняется только порядковый номер панели.
  useEffect(() => {
    if (!inds.list.some((i) => i.pane === breadthPane)) return;
    const used = new Set(inds.list.filter((i) => i.pane > 0).map((i) => i.pane));
    let free = 1;
    while (used.has(free)) free++;
    setBreadthPane(free);
  }, [inds.list, breadthPane]);
  const [paneSizes, setPaneSizes] = useState<number[] | undefined>(undefined);
  const onPaneSizesChange = useCallback((sizes: number[]) => {
    setPaneSizes(sizes);
    wr(`frame:embed:strength:paneSizes:${sizes.length}`, JSON.stringify(sizes.map((v) => Math.round(v * 1000) / 1000)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Итоговый universe — как на странице: добавляем _usd в долларовом режиме.
  const universe: BreadthUniverse = currency === 'usd'
    ? `${universeBase}_usd` as BreadthUniverse
    : universeBase;

  // Лейбл верхней панели всегда отражает реально нарисованный индекс
  // (IMOEX в ₽ / RTSI в $) и НЕ зависит от вселенной breadth-метрики.
  const indexLegend = currency === 'usd' ? 'Индекс РТС (RTSI)' : 'Индекс МосБиржи (IMOEX)';

  useEffect(() => {
    // Пока тариф не разрешился — безопасный минимум (365), чтобы никогда не
    // словить 403 на первом запросе; как только isLoading спадёт, эффект
    // перезапустится (isLoading/tier в deps) и подтянет полную историю.
    const days = strengthAccess.isLoading ? 365 : bestHistoryDays(strengthAccess.canUsePeriod);
    let cancelled = false;
    setStatus('loading');
    getBreadthHistory(ema, days, universe)
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
    // canUsePeriod — не мемоизированная функция (новый reference каждый рендер),
    // в deps нельзя — будет бесконечный рефетч. tier/isLoading — примитивы.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ema, universe, strengthAccess.isLoading, strengthAccess.tier]);

  // Индикаторы считаются по ИНДЕКСУ: это единственный ценовой ряд панели.
  // Свечей нет — только close, поэтому источник цены у всех индикаторов
  // вырождается в value, а объёмов нет вовсе (hasVolume=false в кнопке).
  const indCandles = useMemo(
    () => synced.map((d) => ({ time: String(d.time), value: d.imoex })),
    [synced],
  );
  const toSecFn = useCallback((t: string) => Number(t), []);
  const indSeries = useIndicatorSeries(inds.list, indCandles, toSecFn, inds.colorOf);
  const indValues = useMemo(() => indicatorValues(indSeries), [indSeries]);

  const indPanes = useMemo(
    () => [...new Set(inds.list.filter((i) => i.pane > 0).map((i) => i.pane))].sort((a, b) => a - b),
    [inds.list],
  );

  // Порядок нижних панелей — ЕДИНЫЙ источник правды и для сборки панелей, и для
  // строк-оверлеев. Раньше overlay вычислял его заново «по совпадению номера», и
  // при коллизии номеров строка breadth рисовалась на чужой панели.
  const lowerOrder = useMemo(
    () => [
      { kind: 'breadth' as const, pane: breadthPane },
      ...indPanes.map((pane) => ({ kind: 'ind' as const, pane })),
    ].sort((a, b) => a.pane - b.pane),
    [breadthPane, indPanes],
  );

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
        series: [
          {
            id: 'idx', type: 'line', color: 'var(--chart-line-1)', lineWidth: 2,
            label: indexLegend,
            data: synced.map((d) => ({ time: d.time, value: d.imoex })),
            tipFmt: fmtIdx, axisFmt: fmtIdx,
          },
          // Наложения (MA/EMA поверх индекса) — ПОСЛЕ индекса: первой в
          // массиве должна остаться сама метрика, от неё считают магнит и линейка.
          // ⚠️ И ОБЯЗАТЕЛЬНО на ТУ ЖЕ ШКАЛУ, что индекс. Движок индикаторов
          // кладёт наложения на ЛЕВУЮ ось (у ОИ там цена), а здесь индекс живёт
          // на правой — левая оставалась пустой, автомасштабировалась сама по
          // себе, и линии «прыгали» относительно графика, к которому нарисованы.
          ...(indSeries[0] ?? []).map((d) => ({ ...d, scale: 'right' as const })),
        ],
      });
    }
    const breadthLabel = `% акций выше EMA${ema}`;
    const breadthPaneDef: LwPane = {
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
    };
    // Порядок нижних панелей — по НОМЕРУ: breadth и индикаторы в одном
    // пространстве, поэтому «Выше/Ниже» реально меняет их местами.
    // Панели держатся по СПИСКУ, а не по наличию серий: скрытый «глазом»
    // индикатор серий не даёт, и панель схлопнулась бы вместе со строкой.
    for (const x of lowerOrder) {
      out.push(x.kind === 'breadth' ? breadthPaneDef : { series: indSeries[x.pane] ?? [], flex: 0.7 });
    }
    return out;
  }, [synced, showPrice, chartMode, ema, indexLegend, indSeries, lowerOrder, breadthPane]);

  const paneCountNow = panes.length;
  useEffect(() => {
    const raw = rd(`frame:embed:strength:paneSizes:${paneCountNow}`, '');
    try {
      const v = raw ? (JSON.parse(raw) as number[]) : undefined;
      setPaneSizes(v && v.length === paneCountNow && v.every((n) => Number.isFinite(n) && n > 0) ? v : undefined);
    } catch { setPaneSizes(undefined); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paneCountNow]);

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
            <Dropdown<Ema> value={ema} options={EMAS} onChange={setEma} title="Скользящая" icon={<TrendingUp size={14} />} />
            <PillGroup<Currency> value={currency} options={[{ id: 'rub', label: '₽' }, { id: 'usd', label: '$' }]} onChange={setCurrency} />
            <IndicatorsButton api={inds} hasVolume={false} />
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
          {/* Скользящая — В ТУЛБАРЕ, а не в шестерёнке: на сайте это основной
              переключатель вида (EMA 20/50/100/200 меняет саму метрику), и в
              drawer'е его попросту не находили. */}
          <Dropdown<Ema> value={ema} options={EMAS} onChange={setEma} title="Скользящая" icon={<TrendingUp size={14} />} compact={toolbarCompact} />
          <PillGroup<Currency> value={currency} options={[{ id: 'rub', label: '₽' }, { id: 'usd', label: '$' }]} onChange={setCurrency} />
          <IndicatorsButton api={inds} hasVolume={false} compact={toolbarCompact} />
        </div>
      }
      actions={<DrawExportActions draw={draw} visible={status === 'ok' && panes.length > 0} />}
      more={
        <>
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
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && panes.length > 0 && (
          <LwChartPanes
            tickFmt={monthsYearsTickFmt}
            ref={lwChartRef}
            panes={panes}
            // Легенду движка гасим: её заменили строки индикаторов (иначе на
            // экране две подписи одного ряда — своя слева и центрированная).
            hideLegend
            paneSizes={paneSizes}
            onPaneSizesChange={onPaneSizesChange}
            // Строка индикатора живёт над СВОЕЙ панелью (индекс — панель 0).
            paneOverlay={(i) => {
              // Панель breadth — своя нативная строка. Номер -1: у индикаторов
              // панели всегда ≥1, так что пересечься с ними эта строка не может,
              // а фильтр по pane в PaneIndicatorList работает как есть.
              const base = showPrice ? 1 : 0;
              const slot = lowerOrder[i - base];
              if (!slot) return null;
              if (slot.kind === 'breadth') {
                return (
                  <PaneIndicatorList
                    api={inds}
                    pane={-1}
                    values={indValues}
                    native={[{
                      id: 'breadth', label: `% акций выше EMA${ema}`,
                      color: chartMode === 'line' ? 'var(--oi-cyan)' : 'var(--oi-green)',
                      visible: true, onToggle: () => {}, pane: -1,
                      // Двигается наравне с индикаторами — тем же swapPanes.
                      onMove: (to) => {
                        if (to === 'up' || to === 'down') {
                          const occ = inds.occupiedPanes;
                          const target = occ[occ.indexOf(breadthPane) + (to === 'up' ? -1 : 1)];
                          if (target != null) inds.swapPanes(breadthPane, target);
                        }
                      },
                      canUp: inds.occupiedPanes.indexOf(breadthPane) > 0,
                      canDown: inds.occupiedPanes.indexOf(breadthPane) < inds.occupiedPanes.length - 1,
                    }]}
                  />
                );
              }
              return <PaneIndicatorList api={inds} pane={slot.pane} values={indValues} />;
            }}
            dark={dark}
            fitKey={`${universe}|${ema}|${showPrice}`}
            initialBars={INITIAL_BARS}
            showTooltip={false}
            drawPaneIndex={0}
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
        {/* Список индикаторов панели 0. native — сам индекс: его «глаз» это
            существующий тумблер showPrice, второй источник правды не заводим. */}
        <IndicatorList
          api={inds}
          native={showPrice ? [{
            id: 'idx', label: indexLegend, color: 'var(--chart-line-1)',
            visible: showPrice, onToggle: () => setShowPrice(false),
          } as NativeRow] : []}
          visible={status === 'ok' && panes.length > 0}
          values={indValues}
        />
        <DrawToolsOverlay draw={draw} visible={status === 'ok' && panes.length > 0} />
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        <ChartExportModal
          draw={draw}
          targetElement={chartBoxRef.current}
          lwChartRef={lwChartRef}
          filename={`frame-strength-${universe}`}
          metadata={{
            title: 'Сила рынка',
            details: [currency === 'usd' ? 'Индекс RTSI' : 'Индекс IMOEX', `EMA ${ema}`].filter((x): x is string => !!x),
          }}
        />
      </div>
    </EmbedFrame>
  );
}
