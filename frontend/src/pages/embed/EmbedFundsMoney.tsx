/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Два режима (master switch
 * `viewMode` в drawer'е):
 *   • aum   → суммарная СЧА (AUM) через SimpleChart + индекс на вторичной оси.
 *   • flows → гистограмма притоков/оттоков через FlowsHistogram (порт со
 *             страницы FundsMoneyPage: rAF-волна баров + tooltip + navigator).
 * Категория / период / таймфрейм / тоглы — в выезжающей панели настроек.
 * Виджет целиком под PRO-токеном, тир-гейтинг/онбординг/таблица/экспорт — нет.
 */
import { useEffect, useLayoutEffect, useMemo, useRef, useState, type MouseEvent } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import FlowsHistogram from '../../components/funds/FlowsHistogram';
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
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { readLS, writeLS } from './embedPersist';

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
// Родительный падеж — для legend'ов гистограммы («Приток в фонды {genitive}»).
const CAT_GENITIVE: Record<Category, string> = {
  money_market: 'денежного рынка',
  stocks: 'акций',
  bonds: 'облигаций',
  gold: 'золота',
  yuan: 'юаня',
};
const PERIODS: { id: FundPeriod; label: string }[] = [
  { id: '1y', label: '1Г' },
  { id: '3y', label: '3Г' },
  { id: 'all', label: 'Всё' },
];
const FLOW_TFS: { id: FlowTimeframe; label: string }[] = [
  { id: '1d', label: 'День' },
  { id: '1w', label: 'Неделя' },
  { id: '1m', label: 'Месяц' },
];

function initCat(p: string | null): Category {
  if (p && CATS.some((c) => c.id === p)) return p as Category;
  const s = readLS('frame:embed:funds:category', '');
  if (s && CATS.some((c) => c.id === s)) return s as Category;
  return 'money_market';
}

export default function EmbedFundsMoney() {
  const [params] = useSearchParams();

  const [category, setCategory] = useState<Category>(() => initCat(params.get('category')));
  const [period, setPeriod] = useState<FundPeriod>(() => (params.get('period') || readLS('frame:embed:funds:period', '1y')) as FundPeriod);
  // Default режим — Притоки-Оттоки (как дефолт страницы).
  const [viewMode, setViewMode] = useState<ViewMode>(() => (params.get('view') || readLS('frame:embed:funds:viewMode', 'flows')) as ViewMode);
  const [flowTimeframe, setFlowTimeframe] = useState<FlowTimeframe>(() => (readLS('frame:embed:funds:flowTimeframe', '1d')) as FlowTimeframe);
  const [showIndex, setShowIndex] = useState<boolean>(() => readLS('frame:embed:funds:showIndex', '1') !== '0');

  const [data, setData] = useState<FundsResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // ── Flows state (порт со страницы) ──
  const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
  const [flowsStatus, setFlowsStatus] = useState<LoadStatus>('idle');
  const [animatedBarsIn, setAnimatedBarsIn] = useState<number[]>([]);
  const [animatedBarsOut, setAnimatedBarsOut] = useState<number[]>([]);
  const [flowNavRange, setFlowNavRange] = useState<[number, number]>([0, 0]);
  const [hoveredFlowIndex, setHoveredFlowIndex] = useState<number | null>(null);
  const [flowTooltipPos, setFlowTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const flowChartRef = useRef<SVGSVGElement>(null);
  const flowContainerRef = useRef<HTMLDivElement>(null);

  // Persist
  useEffect(() => { writeLS('frame:embed:funds:category', category); }, [category]);
  useEffect(() => { writeLS('frame:embed:funds:period', period); }, [period]);
  useEffect(() => { writeLS('frame:embed:funds:viewMode', viewMode); }, [viewMode]);
  useEffect(() => { writeLS('frame:embed:funds:flowTimeframe', flowTimeframe); }, [flowTimeframe]);
  useEffect(() => { writeLS('frame:embed:funds:showIndex', showIndex ? '1' : '0'); }, [showIndex]);

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

  // Сброс навигатора при смене данных — useLayoutEffect (как на странице), чтобы
  // обновление сработало ДО первого paint'a после прихода данных.
  useLayoutEffect(() => {
    if (flowsData?.flows?.length) {
      setFlowNavRange([0, flowsData.flows.length - 1]);
    }
  }, [flowsData]);

  // Бары гистограммы: ставим ФИНАЛЬНЫЕ значения сразу (без rAF-волны).
  // Проверено в браузере: в iframe-контексте flowsData приходил несколько раз
  // (повторные fetch'и), rAF-эффект перезапускался и анимация зависала на 1-м
  // кадре → большинство баров оставались на нуле и схлопывались к левому краю.
  // Статичные бары надёжнее: данные важнее косметической волны.
  useEffect(() => {
    if (viewMode !== 'flows' || !flowsData?.flows?.length) {
      setAnimatedBarsIn([]);
      setAnimatedBarsOut([]);
      return;
    }
    const target = flowsData.flows.map((f) => f.flow);
    setAnimatedBarsIn(target.map((v) => Math.max(0, v)));
    setAnimatedBarsOut(target.map((v) => Math.min(0, v)));
  }, [flowsData, viewMode]);

  // handleFlowMouseMove — порт: измеряем РЕАЛЬНУЮ геометрию SVG (не CSS-vars).
  const handleFlowMouseMove = (e: MouseEvent<HTMLDivElement>) => {
    if (!flowsData?.flows?.length || !flowContainerRef.current || !flowChartRef.current) return;
    const containerRect = flowContainerRef.current.getBoundingClientRect();
    const svgRect = flowChartRef.current.getBoundingClientRect();
    const xInChart = e.clientX - svgRect.left;
    if (xInChart < 0 || xInChart > svgRect.width) return;
    const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
    const barWidth = svgRect.width / visibleCount;
    const idx = Math.floor(xInChart / barWidth);
    if (idx >= 0 && idx < visibleCount) {
      setHoveredFlowIndex(idx);
      const slotCenterInContainer = (svgRect.left - containerRect.left) + idx * barWidth + barWidth / 2;
      const y = e.clientY - containerRect.top;
      setFlowTooltipPos({ x: slotCenterInContainer, y });
    }
  };

  const handleFlowMouseLeave = () => {
    setHoveredFlowIndex(null);
    setFlowTooltipPos(null);
  };

  const boxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const chartData = useMemo(
    () => (data?.total_nav ?? []).map((p) => ({ time: p.date, value: (p.nav ?? 0) / 1e9 })),
    [data],
  );
  const indexData = useMemo(
    () => (data?.index?.data ? data.index.data.map((d) => ({ time: d.date, value: d.close || 0 })) : undefined),
    [data],
  );

  const fmtNav = (v: number) => (category === 'gold' || category === 'stocks' ? v.toFixed(2) : v.toFixed(0));
  const genitive = CAT_GENITIVE[category] ?? '';

  // FlowsHistogram занимает доступную высоту панели. Компонент сам добавляет
  // legend (~36) + navigator (~64) к --chart-height, поэтому под сам график
  // оставляем chartH минус этот overhead (но не меньше разумного минимума).
  const flowsChartH = Math.max(140, chartH - 110);

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
          <Dropdown value={period} options={PERIODS} onChange={(v) => setPeriod(v)} title="Период" />
        </>
      }
      more={viewMode === 'aum' ? (
        <DrawerSection label="Отображение">
          <ToggleRow label="Индекс" checked={showIndex} onChange={setShowIndex} hint="Индекс на второй оси" />
        </DrawerSection>
      ) : undefined}
    >
      <div ref={boxRef} style={{ position: 'absolute', inset: 0 }}>
        {viewMode === 'aum' ? (
          <>
            {/* Оси свопнуты как на странице: СЧА — на ПРАВОЙ оси (secondaryData),
                индекс — на ЛЕВОЙ (data), reverseLegend держит СЧА первой. */}
            {status === 'ok' && chartData.length > 0 && (
              <SimpleChart
                data={indexData ?? []}
                secondaryData={chartData}
                height={chartH}
                primaryColor="var(--funds-flow-positive)"
                secondaryColor="var(--accent)"
                showPrimary={showIndex && !!indexData}
                showSecondary={true}
                reverseLegend
                formatValue={(v) => v.toFixed(2)}
                formatPrimaryAxis={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                niceTicks={true}
                niceTicksSecondary={true}
                gridAxis="secondary"
                formatSecondaryValue={fmtNav}
                formatSecondaryAxis={fmtNav}
                primaryLabel="Индекс"
                secondaryLabel="СЧА (млрд ₽)"
                showValueHeader={false}
                legendPosition="top"
                showDownloadButton={false}
                showNavigator={false}
                hideTime
                chartPadding={{ left: 60, right: 100 }}
                bare
              />
            )}
            {status === 'loading' && <EmbedMsg text="Загрузка…" />}
            {status === 'empty' && <EmbedMsg text="Нет данных" />}
            {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
          </>
        ) : (
          <>
            {/* Компактные pad-override: страница рассчитана на ~100px паддинги,
                в узкой панели Y-подписи/даты клипают — ужимаем left/right. */}
            <div
              style={{
                ['--chart-height' as string]: `${flowsChartH}px`,
                ['--chart-pad-left' as string]: '70px',
                ['--chart-pad-right-single' as string]: '55px',
              }}
            >
              {flowsStatus !== 'error' && (
                <FlowsHistogram
                  flowsData={flowsData}
                  noFundsSelected={false}
                  animatedBarsIn={animatedBarsIn}
                  animatedBarsOut={animatedBarsOut}
                  flowNavRange={flowNavRange}
                  hoveredFlowIndex={hoveredFlowIndex}
                  flowTitle={`Чистые притоки и оттоки из фондов ${genitive} (млрд ₽)`}
                  loading={flowsStatus === 'loading'}
                  flowContainerRef={flowContainerRef}
                  flowChartRef={flowChartRef}
                  flowTooltipPos={flowTooltipPos}
                  onMouseMove={handleFlowMouseMove}
                  onMouseLeave={handleFlowMouseLeave}
                  onSetFlowNavRange={setFlowNavRange}
                />
              )}
            </div>
            {flowsStatus === 'error' && <EmbedMsg text="Ошибка загрузки" />}
          </>
        )}
      </div>
    </EmbedFrame>
  );
}
