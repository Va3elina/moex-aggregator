/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Движок — общий LwChart (как ОИ/
 * Баффетт), без SVG. Два режима (`viewMode`):
 *   • flows → чистые притоки/оттоки: одна histogram-серия, per-bar цвет по знаку
 *             (зел приток / крас отток), нулевая линия. Легенда «Приток/Отток».
 *   • aum   → суммарная СЧА (area, правая ось) + индекс (line, левая ось).
 * Категория / период / таймфрейм / тоглы — в тулбаре и ⚙.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import LwChart, { monthsYearsTickFmt, type LwSeries } from '../../components/LwChart';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getFundsChartData,
  getFundsFlows,
  type FundPeriod,
  type FundCategory,
  type FlowTimeframe,
  type FundsFlowsResponse,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useChartFormat } from './EmbedFormat';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';

type Category = FundCategory;
type ViewMode = 'aum' | 'flows';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type FundsResp = Awaited<ReturnType<typeof getFundsChartData>>;

const CATS: { id: Category; label: string }[] = [
  { id: 'money_market', label: 'Денежный' },
  { id: 'stocks', label: 'Акции' },
  { id: 'bonds', label: 'Облигации' },
  { id: 'gold', label: 'Золото' },
  { id: 'yuan', label: 'Юань' },
];
const FLOW_TFS: { id: FlowTimeframe; label: string }[] = [
  { id: '1d', label: 'День' },
  { id: '1w', label: 'Неделя' },
  { id: '1m', label: 'Месяц' },
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
  return 'money_market';
}

/** `initialCategory` — стартовая категория от песочницы (спавн по клику на сигнале). */
export default function EmbedFundsMoney({ initialCategory }: { initialCategory?: string } = {}) {
  const { rd, wr } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const { fmt, setKind, setColor } = useChartFormat('frame:embed:funds:fmt', 'area');
  const [category, setCategory] = useState<Category>(() => initCat(initialCategory || params.get('category'), rd));
  // Период убран из UI (неуместен в песочнице) — грузим всю историю, зум колесом,
  // как ОИ/Сила рынка/Сезонность.
  const period: FundPeriod = 'all';
  // Default режим — Притоки-Оттоки (как дефолт страницы).
  const [viewMode, setViewMode] = useState<ViewMode>(() => (params.get('view') || rd('frame:embed:funds:viewMode', 'flows')) as ViewMode);
  const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>(() => (rd('frame:embed:funds:flowTimeframe', '1d')) as FlowTimeframe);
  const [showIndex, setShowIndex] = useState<boolean>(() => rd('frame:embed:funds:showIndex', '1') !== '0');

  const [data, setData] = useState<FundsResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [flowsStatus, setFlowsStatus] = useState<LoadStatus>('idle');

  // Persist
  useEffect(() => { wr('frame:embed:funds:category', category); }, [category]);
  useEffect(() => { wr('frame:embed:funds:viewMode', viewMode); }, [viewMode]);
  useEffect(() => { wr('frame:embed:funds:flowTimeframe', flowTimeframe); }, [flowTimeframe]);
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

  // Резиновая высота графика (как в Баффетте).
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

  // Серии LwChart.
  const lwSeries = useMemo<LwSeries[]>(() => {
    if (viewMode === 'flows') {
      const flows = flowsData?.flows ?? [];
      if (!flows.length) return [];
      // flow приходит в МЛРД ₽ → в рубли (×1e9), чтобы компактный формат дал «млрд».
      return [{
        id: 'flow', type: 'histogram', scale: 'right', base: 0, zeroLine: true,
        color: 'var(--oi-green)', label: 'Чистый поток',
        // Периодический нетто-поток — «последнее значение» на оси неинформативно
        // (не тренд, не текущая цена), только пилюля лишняя.
        lastValueVisible: false,
        data: flows.map((f) => {
          const val = (f.flow ?? 0) * 1e9;
          return { time: toSec(f.period_end), value: val, color: val >= 0 ? 'var(--oi-green)' : 'var(--oi-red)' };
        }),
        axisFmt: fmtSigned,
        tipFmt: (v) => (v >= 0 ? '+' : '−') + fmtAbs(v) + ' ₽',
      }];
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
      toolbar={
        <>
          <PillGroup<ViewMode>
            value={viewMode}
            options={[{ id: 'flows', label: 'Потоки' }, { id: 'aum', label: 'СЧА' }]}
            onChange={(v) => setViewMode(v)}
          />
          <Dropdown value={category} options={CATS} onChange={(v) => setCategory(v)} title="Категория фондов" />
          {viewMode === 'flows' && (
            <PillGroup value={flowTimeframe} options={FLOW_TFS} onChange={(v) => setFlowTimeframe(v)} />
          )}
        </>
      }
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
        {st === 'ok' && lwSeries.length > 0 && (
          <LwChart
            series={lwSeries}
            height={chartH}
            dark={dark}
            fitKey={`${viewMode}|${category}|${flowTimeframe}|${period}`}
            tickFmt={monthsYearsTickFmt}
            legendItems={viewMode === 'flows'
              ? [{ label: 'Приток', color: 'var(--oi-green)' }, { label: 'Отток', color: 'var(--oi-red)' }]
              : undefined}
          />
        )}
        {st === 'loading' && <EmbedMsg text="Загрузка…" />}
        {st === 'empty' && <EmbedMsg text="Нет данных" />}
        {st === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
