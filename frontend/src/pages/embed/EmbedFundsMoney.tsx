/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Движок — общий LwChart (как ОИ/
 * Баффетт), без SVG. Два режима (`viewMode`):
 *   • flows → чистые притоки/оттоки: одна histogram-серия, per-bar цвет по знаку
 *             (зел приток / крас отток), нулевая линия. Легенда «Приток/Отток».
 *   • aum   → суммарная СЧА (area, правая ось) + индекс (line, левая ось).
 * Категория / период / таймфрейм / тоглы — в тулбаре и ⚙.
 */
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ArrowLeftRight, Wallet, Landmark, TrendingUp, Coins, Banknote, Clock, CalendarDays, CalendarRange } from 'lucide-react';
import { monthsYearsTickFmt, type LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes, { type LwChartPanesHandle } from '../../components/LwChartPanes';
import StackedBidirectionalHistogram from '../../components/cbr/StackedBidirectionalHistogram';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getFundsChartData,
  getFundsFlows,
  type FundPeriod,
  type FundCategory,
  type FlowTimeframe,
  type FundsFlowsResponse,
  type CbrFlowsPeriod,
  type FlowDataPoint,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useChartFormat } from './EmbedFormat';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';
import { useTierAccess } from '../../contexts/TierFeaturesContext';

type Category = FundCategory;
type ViewMode = 'aum' | 'flows';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type FundsResp = Awaited<ReturnType<typeof getFundsChartData>>;

const CATS: { id: Category; label: string; icon: ReactNode }[] = [
  { id: 'money_market', label: 'Денежный', icon: <Wallet size={14} /> },
  { id: 'stocks', label: 'Акции', icon: <TrendingUp size={14} /> },
  { id: 'bonds', label: 'Облигации', icon: <Landmark size={14} /> },
  { id: 'gold', label: 'Золото', icon: <Coins size={14} /> },
  { id: 'yuan', label: 'Юань', icon: <Banknote size={14} /> },
];
// Категория рендерится через Dropdown (не PillGroup) — иконка там одна, по
// текущему значению (см. CAT_ICONS), а не per-option как в PillGroup.
const CAT_ICONS: Record<Category, ReactNode> = Object.fromEntries(CATS.map((c) => [c.id, c.icon])) as Record<Category, ReactNode>;
/** Период истории потоков. «Авто» = как было: глубина выводится из шага
 *  столбца (день → год, неделя → 3 года, месяц → всё). */
const MONTHS_RU = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
const MONTHS_SHORT_RU = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
const FLOW_PERIODS: { id: FundPeriod | 'auto'; label: string }[] = [
  { id: 'auto', label: 'Авто' },
  { id: '1m', label: '1 месяц' },
  { id: '1y', label: '1 год' },
  { id: '3y', label: '3 года' },
  { id: 'all', label: 'Всё время' },
];
const FLOW_TFS: { id: FlowTimeframe; label: string; icon: ReactNode }[] = [
  { id: '1d', label: 'День', icon: <Clock size={14} /> },
  { id: '1w', label: 'Неделя', icon: <CalendarDays size={14} /> },
  { id: '1m', label: 'Месяц', icon: <CalendarRange size={14} /> },
];

// 'YYYY-MM-DD' → UNIX-секунды (UTC-полночь) для LwChart (даты дневные/агрегированные).
const toSec = (t: string): number => {
  const [y, m, d] = t.slice(0, 10).split('-').map(Number);
  return Math.floor(Date.UTC(y, m - 1, d) / 1000);
};
// Компактный рублёвый формат (без знака): «2,1 млрд» / «5 млн» / «320 тыс».
function fmtAbs(a: number): string {
  a = Math.abs(a);
  if (a >= 1e9) return (a / 1e9).toFixed(a >= 1e10 ? 0 : 1).replace('.', ',') + ' млрд';
  if (a >= 1e6) return (a / 1e6).toFixed(a >= 1e7 ? 0 : 1).replace('.', ',') + ' млн';
  if (a >= 1e3) return Math.round(a / 1e3).toLocaleString('ru-RU') + ' тыс';
  return Math.round(a).toString();
}
const fmtSigned = (v: number): string => (v < 0 ? '−' : '') + fmtAbs(v);
const fmtInt = (v: number): string => Math.round(v).toLocaleString('ru-RU');

function initCat(p: string | null, rd: (k: string, d: string) => string): Category {
  if (p && CATS.some((c) => c.id === p)) return p as Category;
  const s = rd('frame:embed:funds:category', '');
  if (s && CATS.some((c) => c.id === s)) return s as Category;
  // Дефолт — облигации: самая массовая категория БПИФов.
  return 'bonds';
}

/** `initialCategory` — стартовая категория от песочницы (спавн по клику на сигнале). */
export default function EmbedFundsMoney({ initialCategory }: { initialCategory?: string } = {}) {
  const { rd, wr } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const fundsAccess = useTierAccess('funds_money');

  const { fmt, setKind, setColor } = useChartFormat('frame:embed:funds:fmt', 'area');
  const [category, setCategory] = useState<Category>(() => initCat(initialCategory || params.get('category'), rd));
  // Default режим — Притоки-Оттоки (как дефолт страницы).
  const [viewMode, setViewMode] = useState<ViewMode>(() => (params.get('view') || rd('frame:embed:funds:viewMode', 'flows')) as ViewMode);
  const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>(() => (rd('frame:embed:funds:flowTimeframe', '1d')) as FlowTimeframe);
  const [flowPeriod, setFlowPeriod] = useState<FundPeriod | 'auto'>(() => (rd('frame:embed:funds:flowPeriod', '1y')) as FundPeriod | 'auto');
  // Период убран из UI (неуместен в песочнице) — но НЕ 'all' всегда: дневная
  // сетка потоков за всю историю фонда — сотни-тысячи баров вплотную, график
  // читается как шум. Сайт (FundsMoneyPage.FLOW_MIN_PERIODS) по умолчанию
  // кэпает дневной срез 1 годом — зеркалим тот же кэп программно, без
  // дискретного контрола: зум остаётся колесом, просто первичная загрузка
  // не тянет вообще всё. Недельный/месячный срез уже редкий → можно шире.
  // ⚠️ 'all' нельзя слать безусловно: бэкенд (enforce_tier_limits) HARD-REJECT'ит
  // period > max_history_days тарифа (403, а не clamp), а у free тут лимит 3 года —
  // режим СЧА и месячные потоки отдавали ему «Ошибка загрузки» вместо графика.
  // Понижаем до максимально разрешённого тарифу (тот же приём, что в
  // EmbedOpenInterest.bestDailyPeriod / EmbedStrength.bestHistoryDays).
  // Период выбирается ЯВНО (как у потоков ЦБ), а не выводится из таймфрейма:
  // раньше «сколько истории» было жёстко привязано к шагу столбца, и увидеть
  // дневные потоки за 3 года было нельзя в принципе.
  const wantPeriod: FundPeriod = viewMode !== 'flows' ? 'all'
    : flowPeriod !== 'auto' ? flowPeriod
    : flowTimeframe === '1d' ? '1y'
    : flowTimeframe === '1w' ? '3y'
    : 'all';
  const period: FundPeriod = fundsAccess.isLoading || fundsAccess.canUsePeriod(wantPeriod)
    ? wantPeriod
    : (['3y', '1y', '1m', '1w'] as FundPeriod[]).find((p) => fundsAccess.canUsePeriod(p)) ?? '1y';
  const [showIndex, setShowIndex] = useState<boolean>(() => rd('frame:embed:funds:showIndex', '1') !== '0');

  const [data, setData] = useState<FundsResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [flowsStatus, setFlowsStatus] = useState<LoadStatus>('idle');

  // Persist
  useEffect(() => { wr('frame:embed:funds:category', category); }, [category]);
  useEffect(() => { wr('frame:embed:funds:viewMode', viewMode); }, [viewMode]);
  useEffect(() => { wr('frame:embed:funds:flowTimeframe', flowTimeframe); }, [flowTimeframe]);
  useEffect(() => { wr('frame:embed:funds:flowPeriod', flowPeriod); }, [flowPeriod]);
  useEffect(() => { wr('frame:embed:funds:showIndex', showIndex ? '1' : '0'); }, [showIndex]);

  // ── AUM load ──
  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getFundsChartData(category, period)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.total_nav?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/funds load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [category, period]);

  // ── Flows load (только в режиме flows) ──
  useEffect(() => {
    if (viewMode !== 'flows') return;
    let cancelled = false;
    setFlowsStatus('loading');
    getFundsFlows(category, flowTimeframe, period)
      .then((res) => {
        if (cancelled) return;
        setFlowsData(res);
        setFlowsStatus((res?.flows?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/funds flows failed:', err);
        setFlowsStatus('error');
      });
    return () => { cancelled = true; };
  }, [viewMode, category, flowTimeframe, period]);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();
  // Рисование + экспорт графика (см. useDrawTools.tsx) — персист per-категория
  // (данные категорий несопоставимы, как per-инструмент у ОИ).
  const draw = useDrawTools(`frame:embed:funds:draw:${category}`);
  const lwChartRef = useRef<LwChartPanesHandle>(null);
  const chartBoxRef = useRef<HTMLDivElement>(null);

  // Высоту не считаем: LwChartPanes всегда 100% родителя (absolute inset:0).

  // Серии LwChart.
  // Потоки → формат движка ЦБ (StackedBidirectionalHistogram): не временной ряд,
  // а ПЛИТКА ПЕРИОДОВ. Приток и отток — две «категории», они и складываются в
  // столбец вверх/вниз от нуля. Цвета лежат в общей палитре (CBR_CATEGORY_COLORS),
  // поэтому берутся тем же путём, что у категорий ЦБ, и остаются тема-зависимыми.
  // Движку ЦБ нужна ЯВНАЯ высота в пикселях (он рисует SVG в фиксированный
  // бокс, а не тянется по родителю) — меряем контейнер, как в потоках ЦБ.
  const [chartH, setChartH] = useState(300);
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

  const flowPeriods = useMemo<CbrFlowsPeriod[]>(() => {
    const flows = flowsData?.flows ?? [];
    return flows.map((f) => {
      const d = new Date(f.period_end);
      const label = flowTimeframe === '1m'
        ? MONTHS_RU[d.getUTCMonth()]
        : `${d.getUTCDate()} ${MONTHS_SHORT_RU[d.getUTCMonth()]}`;
      return {
        year: d.getUTCFullYear(),
        label,
        // Движок различает только месяц и квартал; день и неделя для него —
        // «месяц» (влияет лишь на подпись сравнения в тултипе).
        kind: 'month' as const,
        end_date: f.period_end,
        // ⚠️ ЧИСТЫЙ поток, как на сайте: ОДИН столбец на период. Приток и отток
        // одновременно (gross_in + gross_out) рисовали два столбика в разные
        // стороны и не отвечали на главный вопрос — «сколько в итоге пришло».
        // Ненулевая всегда ровно одна «категория», поэтому цвет = направление.
        values: (f.flow ?? 0) >= 0
          ? { 'Приток': f.flow ?? 0, 'Отток': 0 }
          : { 'Приток': 0, 'Отток': f.flow ?? 0 },
      };
    });
  }, [flowsData, flowTimeframe]);

  const lwSeries = useMemo<LwSeries[]>(() => {
    if (viewMode === 'flows') {
      const flows = flowsData?.flows ?? [];
      if (!flows.length) return [];
      // Двунаправленные столбцы, как в потоках ЦБ: приток ВВЕРХ, отток ВНИЗ —
      // двумя отдельными рядами от нуля. Один ряд «нетто» скрывал главное:
      // период с большим оборотом и почти нулевым сальдо выглядел как пустой.
      // flow приходит в МЛРД ₽ → в рубли (×1e9), чтобы компактный формат дал «млрд».
      const mk = (id: string, label: string, color: string, pick: (f: FlowDataPoint) => number): LwSeries => ({
        id, type: 'histogram', scale: 'right', base: 0, color, label,
        // Периодический поток — «последнее значение» на оси неинформативно
        // (не тренд, не текущая цена), только пилюля лишняя.
        lastValueVisible: false,
        data: flows.map((f) => ({ time: toSec(f.period_end), value: pick(f) * 1e9 })),
        axisFmt: fmtSigned,
        tipFmt: (v) => (v >= 0 ? '+' : '−') + fmtAbs(v) + ' ₽',
      });
      return [
        mk('flow-in', 'Приток', 'var(--oi-green)', (f) => Math.max(0, f.gross_in ?? 0)),
        mk('flow-out', 'Отток', 'var(--oi-red)', (f) => Math.min(0, f.gross_out ?? 0)),
      ];
    }
    // aum: индекс (линия, левая, синий) + СЧА (область, правая, зелёная).
    const nav = data?.total_nav ?? [];
    if (!nav.length) return [];
    const out: LwSeries[] = [];
    const idx = data?.index?.data;
    if (showIndex && idx?.length) {
      out.push({
        id: 'idx', type: 'line', scale: 'left', color: 'var(--chart-line-1)', lineWidth: 2, label: 'Индекс',
        data: idx.flatMap((d) => (d.close != null ? [{ time: toSec(d.date), value: d.close }] : [])),
        axisFmt: fmtInt, tipFmt: (v) => Math.round(v).toLocaleString('ru-RU'),
      });
    }
    out.push(applyFormat({
      id: 'scha', type: 'area', scale: 'right', color: 'var(--chart-line-3)',
      areaTop: 'color-mix(in srgb, var(--chart-line-3) 22%, transparent)', lineWidth: 2, label: 'СЧА',
      data: nav.flatMap((p) => (p.nav != null ? [{ time: toSec(p.date), value: p.nav }] : [])),
      axisFmt: fmtAbs, tipFmt: (v) => fmtAbs(v) + ' ₽',
    }, fmt));
    return out;
  }, [viewMode, flowsData, data, showIndex, fmt]);

  const st = viewMode === 'flows' ? flowsStatus : status;

  return (
    <EmbedFrame
      toolbarUnified
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <PillGroup<ViewMode>
              value={viewMode}
              options={[{ id: 'flows', label: 'Потоки', icon: <ArrowLeftRight size={14} /> }, { id: 'aum', label: 'СЧА', icon: <Wallet size={14} /> }]}
              onChange={(v) => setViewMode(v)}
            />
            <Dropdown value={category} options={CATS} onChange={(v) => setCategory(v)} title="Категория фондов" icon={CAT_ICONS[category]} />
            <PillGroup value={flowTimeframe} options={FLOW_TFS} onChange={(v) => setFlowTimeframe(v)} />
            <Dropdown value={flowPeriod} options={FLOW_PERIODS} onChange={(v) => setFlowPeriod(v)} title="Период" icon={<CalendarRange size={14} />} />
          </div>
          <PillGroup<ViewMode>
            value={viewMode}
            options={[{ id: 'flows', label: 'Потоки', icon: <ArrowLeftRight size={14} /> }, { id: 'aum', label: 'СЧА', icon: <Wallet size={14} /> }]}
            onChange={(v) => setViewMode(v)}
            compact={toolbarCompact}
          />
          <Dropdown value={category} options={CATS} onChange={(v) => setCategory(v)} title="Категория фондов" icon={CAT_ICONS[category]} compact={toolbarCompact} />
          {viewMode === 'flows' && (
            <>
              <PillGroup value={flowTimeframe} options={FLOW_TFS} onChange={(v) => setFlowTimeframe(v)} compact={toolbarCompact} />
              <Dropdown value={flowPeriod} options={FLOW_PERIODS} onChange={(v) => setFlowPeriod(v)} title="Период" icon={<CalendarRange size={14} />} compact={toolbarCompact} />
            </>
          )}
        </div>
      }
      actions={<DrawExportActions draw={draw} visible={st === 'ok' && lwSeries.length > 0} />}
      more={viewMode === 'aum' ? (
        <>
          <DrawerSection label="Отображение">
            <ToggleRow label="Индекс" checked={showIndex} onChange={setShowIndex} hint="Индекс на второй оси" />
          </DrawerSection>
          <FormatSection fmt={fmt} onKind={setKind} onColor={setColor} />
        </>
      ) : undefined}
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {viewMode === 'flows' && flowsStatus === 'ok' && flowPeriods.length > 0 && (
          <StackedBidirectionalHistogram
            periods={flowPeriods}
            categories={['Приток', 'Отток']}
            unit="млрд ₽"
            height={chartH}
            animTrigger={`${category}|${flowTimeframe}|${period}`}
          />
        )}
        {viewMode !== 'flows' && st === 'ok' && lwSeries.length > 0 && (
          <LwChartPanes
            ref={lwChartRef}
            panes={[{ series: lwSeries }]}
            // Статичный вид (как у потоков капитала): пан и зум выключены,
            // график всегда показывает всю историю и подстраивается под панель.
            // Здесь смысл в картине целиком, а не в разглядывании участка.
            staticView
            drawPaneIndex={0}
            dark={dark}
            fitKey={`${viewMode}|${category}|${flowTimeframe}|${period}`}
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
        <DrawToolsOverlay draw={draw} visible={st === 'ok' && lwSeries.length > 0} />
        {st === 'loading' && <EmbedMsg text="Загрузка…" />}
        {st === 'empty' && <EmbedMsg text="Нет данных" />}
        {st === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        <ChartExportModal
          draw={draw}
          targetElement={chartBoxRef.current}
          lwChartRef={lwChartRef}
          filename={`frame-funds-${category}-${viewMode}`}
          metadata={{
            title: 'Деньги в фондах',
            details: [CATS.find((c) => c.id === category)?.label, viewMode === 'flows' ? 'Потоки' : 'СЧА'].filter((x): x is string => !!x),
          }}
        />
      </div>
    </EmbedFrame>
  );
}
