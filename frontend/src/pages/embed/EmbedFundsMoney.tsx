/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Два режима (`viewMode`):
 *   • flows → двухпанельный вид сайта (FlowsHistogram, bare): сверху линия
 *             бенчмарка категории (IMOEX/RGBITR/…), снизу биполярная
 *             гистограмма чистых притоков/оттоков. Курсор и тултип общие.
 *   • aum   → суммарная СЧА (area, правая ось) + индекс (line, левая ось),
 *             движок LwChart (как ОИ/Баффетт).
 * Категория / период / таймфрейм — в тулбаре, слои и формат — за ⚙.
 */
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ArrowLeftRight, Wallet, Landmark, TrendingUp, Coins, Banknote, Clock, Columns3, Grid3x3, CalendarRange, ListFilter, Lock } from 'lucide-react';
import { monthsYearsTickFmt, type LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes, { type LwChartPanesHandle } from '../../components/LwChartPanes';
import FlowsHistogram from '../../components/funds/FlowsHistogram';
import FundPickerModal from '../../components/funds/FundPickerModal';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getFundsChartData,
  getFundsFlows,
  type FundPeriod,
  type FundCategory,
  type FlowTimeframe,
  type FlowRolling,
  type FundsFlowsResponse,
  type IndexDataPoint,
} from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { FormatSection, applyFormat, useChartFormat } from './EmbedFormat';
import { EmbedFrame, PillGroup, Dropdown, ToolbarButton } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { useDrawTools, DrawExportActions, DrawToolsOverlay, ChartExportModal } from './useDrawTools';
import { useTierAccess } from '../../contexts/TierFeaturesContext';

type Category = FundCategory;
type ViewMode = 'aum' | 'flows';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type FundsResp = Awaited<ReturnType<typeof getFundsChartData>>;

// `genitive` — родительный падеж для заголовка легенды «…из фондов <чего>»,
// один в один со страницей (FundsMoneyPage.CATEGORIES).
// `index` — тикер бенчмарка, им подписана линия индекса на сайте.
const CATS: { id: Category; label: string; genitive: string; index: string; icon: ReactNode }[] = [
  { id: 'money_market', label: 'Денежный', genitive: 'денежного рынка', index: 'RUSFAR3M', icon: <Wallet size={14} /> },
  { id: 'stocks', label: 'Акции', genitive: 'акций', index: 'IMOEX', icon: <TrendingUp size={14} /> },
  { id: 'bonds', label: 'Облигации', genitive: 'облигаций', index: 'RGBITR', icon: <Landmark size={14} /> },
  { id: 'gold', label: 'Золото', genitive: 'золота', index: 'GLDRUB_TOM', icon: <Coins size={14} /> },
  { id: 'yuan', label: 'Юань', genitive: 'юаня', index: 'CNYRUB_TOM', icon: <Banknote size={14} /> },
];
// Категория рендерится через Dropdown (не PillGroup) — иконка там одна, по
// текущему значению (см. CAT_ICONS), а не per-option как в PillGroup.
const CAT_ICONS: Record<Category, ReactNode> = Object.fromEntries(CATS.map((c) => [c.id, c.icon])) as Record<Category, ReactNode>;
/** Период истории потоков. «Авто» = как было: глубина выводится из шага
 *  столбца (день → год, неделя → 3 года, месяц → всё). */
const FLOW_PERIODS: { id: FundPeriod | 'auto'; label: string }[] = [
  { id: 'auto', label: 'Авто' },
  { id: '1m', label: '1 месяц' },
  { id: '1y', label: '1 год' },
  { id: '3y', label: '3 года' },
  { id: 'all', label: 'Всё время' },
];
// Иконки шага столбца НАРОЧНО без календарей: в компакт-режиме тулбара
// подписи скрыты, и CalendarDays/CalendarRange на 14px были близнецами —
// и между собой, и с календарём соседнего дропдауна «Период» (он остаётся
// единственным календарём тулбара). Метафора — шаг агрегации: часы →
// столбцы недель → сетка месяца.
const FLOW_TFS: { id: FlowTimeframe; label: string; icon: ReactNode }[] = [
  { id: '1d', label: 'День', icon: <Clock size={14} /> },
  { id: '1w', label: 'Неделя', icon: <Columns3 size={14} /> },
  { id: '1m', label: 'Месяц', icon: <Grid3x3 size={14} /> },
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

  const { fmt, setKind, setColor, setOpacity, setWidth } = useChartFormat('frame:embed:funds:fmt', 'area');
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
  // Слой «Индекс» — общий на оба режима, как на сайте: в СЧА это линия на левой
  // оси, в потоках — верхняя панель бенчмарка. По умолчанию ВКЛЮЧЁН: панель
  // «Деньги в фондах» открывается сразу с бенчмарком.
  const [showIndex, setShowIndex] = useState<boolean>(() => rd('frame:embed:funds:showIndex', '1') !== '0');
  // Сглаживание потоков — скользящая сумма за 3 месяца (тот же слой, что на
  // сайте: низкочастотный тумблер живёт за ⚙, а не в горячем ряду тулбара).
  const [flowRolling, setFlowRolling] = useState<FlowRolling | 'none'>(() => (rd('frame:embed:funds:flowRolling', 'none') === '3m' ? '3m' : 'none'));

  // Скрытые фонды — персист ПО КАТЕГОРИИ, как на сайте (frame:funds:hidden:<cat>):
  // наборы фондов в категориях не пересекаются, общий список бессмыслен.
  // Храним именно СКРЫТЫЕ, а не выбранные: новый фонд в категории должен
  // появляться включённым, а не молча выпадать из расчёта (тот же урок, что в
  // фильтре фондов у /fund-trades, #966).
  const hiddenKey = `frame:embed:funds:hidden:${category}`;
  const [hiddenFunds, setHiddenFunds] = useState<Set<number>>(new Set());
  const [fundPickerOpen, setFundPickerOpen] = useState(false);
  // Выбор ПОДМНОЖЕСТВА фондов — с Basic (матрица funds_money.fund_picker).
  const canPickFunds = fundsAccess.isLoading || fundsAccess.canUseFlag('fund_picker');
  useEffect(() => {
    const raw = rd(hiddenKey, '');
    setHiddenFunds(new Set(raw ? raw.split(',').map(Number).filter((n) => !Number.isNaN(n)) : []));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [category]);
  // Санитайз слетевшего с тарифа: сохранённый в панели набор скрытых фондов
  // (переживает перезагрузку песочницы) больше не должен применяться.
  useEffect(() => {
    if (fundsAccess.isLoading || canPickFunds) return;
    setHiddenFunds((prev) => (prev.size > 0 ? new Set() : prev));
  }, [fundsAccess.isLoading, canPickFunds]);

  // Загруженные данные держим ВМЕСТЕ с ключом запроса, которым они получены.
  // Волна появления столбцов и фит графика должны перезапускаться, когда новые
  // данные ПРИЕХАЛИ, а не когда пользователь нажал кнопку: иначе анимация
  // проигрывается сначала на старом наборе, потом ещё раз на новом.
  const [data, setData] = useState<{ res: FundsResp; key: string } | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  // Потокам свой ключ запроса не нужен: и волна баров, и подписи заводятся от
  // ЭХА бека в ответе (category/timeframe/period/rolling) — см. flowsPair ниже.
  const [flowsLoaded, setFlowsLoaded] = useState<FundsFlowsResponse | null>(null);
  const [flowsStatus, setFlowsStatus] = useState<LoadStatus>('idle');

  // Persist
  useEffect(() => { wr('frame:embed:funds:category', category); }, [category]);
  useEffect(() => { wr('frame:embed:funds:viewMode', viewMode); }, [viewMode]);
  useEffect(() => { wr('frame:embed:funds:flowTimeframe', flowTimeframe); }, [flowTimeframe]);
  useEffect(() => { wr('frame:embed:funds:flowPeriod', flowPeriod); }, [flowPeriod]);
  useEffect(() => { wr('frame:embed:funds:showIndex', showIndex ? '1' : '0'); }, [showIndex]);
  useEffect(() => { wr('frame:embed:funds:flowRolling', flowRolling); }, [flowRolling]);

  // Реалтайм: ингест фондов обновил данные (SSE 'funds'/'daily') → тихий
  // рефетч обоих режимов. Тихий = без setStatus('loading'), панель не мигает.
  const [refreshTick, setRefreshTick] = useState(0);
  const silentRef = useRef(false);
  useRealtimeData(['funds', 'daily'], () => { silentRef.current = true; setRefreshTick((t) => t + 1); });

  // ── AUM load ──
  useEffect(() => {
    let cancelled = false;
    if (!silentRef.current) setStatus('loading');
    getFundsChartData(category, period)
      .then((res) => {
        if (cancelled) return;
        setData({ res, key: `${category}|${period}` });
        setStatus((res?.total_nav?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/funds load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [category, period, refreshTick]);

  // Список фондов категории приезжает вместе с данными СЧА — отдельного
  // запроса не нужно (AUM-эффект выше идёт в обоих режимах).
  const funds = useMemo(() => data?.res.funds ?? [], [data]);
  const visibleFundIds = useMemo(
    () => funds.filter((f) => !hiddenFunds.has(f.fund_id)).map((f) => f.fund_id),
    [funds, hiddenFunds],
  );
  // Пока ничего не скрыто — параметр НЕ шлём вовсе: бэкенд сам берёт категорию
  // целиком (и не надо ждать, пока приедет список фондов, чтобы начать грузить
  // потоки). Строка-ключ, а не массив — стабильная зависимость эффекта.
  const fundIdsKey = hiddenFunds.size > 0 ? visibleFundIds.join(',') : '';
  useEffect(() => {
    if (hiddenFunds.size > 0) wr(hiddenKey, [...hiddenFunds].join(','));
    else wr(hiddenKey, '');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hiddenFunds, hiddenKey]);

  // ── Flows load (только в режиме flows) ──
  useEffect(() => {
    if (viewMode !== 'flows') return;
    let cancelled = false;
    if (!silentRef.current) setFlowsStatus('loading');
    const ids = fundIdsKey ? fundIdsKey.split(',').map(Number) : undefined;
    getFundsFlows(category, flowTimeframe, period, ids, flowRolling === '3m' ? '3m' : undefined)
      .then((res) => {
        if (cancelled) return;
        setFlowsLoaded(res);
        setFlowsStatus((res?.flows?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/funds flows failed:', err);
        setFlowsStatus('error');
      });
    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [viewMode, category, flowTimeframe, period, fundIdsKey, flowRolling, refreshTick]);

  // Сброс «тихого» флага ПОСЛЕ обоих load-эффектов (они читают его в порядке
  // объявления в этом же коммите): сбрось его первый — второй бы мигнул.
  useEffect(() => { silentRef.current = false; }, [refreshTick]);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();
  // Рисование + экспорт графика (см. useDrawTools.tsx) — персист per-категория
  // (данные категорий несопоставимы, как per-инструмент у ОИ).
  const draw = useDrawTools(`frame:embed:funds:draw:${category}`);
  const lwChartRef = useRef<LwChartPanesHandle>(null);
  const chartBoxRef = useRef<HTMLDivElement>(null);

  // Высоту не считаем: LwChartPanes всегда 100% родителя (absolute inset:0).

  // Потоки рисует FlowsHistogram — тот же компонент, что на сайте: ему нужна
  // ЯВНАЯ высота в пикселях (SVG в фиксированном боксе, а не тянется по
  // родителю) — меряем контейнер, как в потоках ЦБ.
  const [chartH, setChartH] = useState(300);
  // Ширину меряем ради заголовка легенды: на сайте он сокращается по viewport,
  // а в панели песочницы viewport — это окно браузера, а не панель.
  const [containerW, setContainerW] = useState(0);
  useEffect(() => {
    const el = chartBoxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const r = entries[0]?.contentRect;
      if (r?.height) setChartH(Math.round(r.height));
      if (r?.width) setContainerW(Math.round(r.width));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // ── Согласованная пара «потоки + индекс» (как на сайте, PR #1248). ──
  // flows и nav-данные (источник линии бенчмарка) грузятся независимо, и между
  // их приходами диапазоны расходятся: линия нового периода ложилась бы на
  // слоты потоков старого. Пропускаем в график только пары одного
  // (category, period) — бек эхоит оба поля; пока свежая не собралась, на
  // экране целиком держится предыдущая, и бары с линией обновляются одним
  // атомарным морфом.
  const [flowsPair, setFlowsPair] = useState<{ flows: FundsFlowsResponse; index?: IndexDataPoint[] } | null>(null);
  useEffect(() => {
    const flows = flowsLoaded;
    if (!flows) return;
    const nav = data?.res;
    // Нет nav-данных вовсе (ранний рендер / ошибка запроса) — пара без индекса:
    // гистограмма не должна ждать линию, которой не будет.
    if (!nav || (nav.category === flows.category && nav.period === flows.period)) {
      setFlowsPair({ flows, index: nav?.index?.data });
    }
  }, [flowsLoaded, data]);
  // Подписи (легенда бенчмарка, заголовок гистограммы) — от категории ПАРЫ, а
  // не текущей: пока держим старую пару, «RGBITR» над линией IMOEX — ложь.
  const pairCat = CATS.find((c) => c.id === flowsPair?.flows.category) ?? CATS.find((c) => c.id === category);

  // Ряды LwChart нужны только режиму СЧА: потоки рисует FlowsHistogram.
  const lwSeries = useMemo<LwSeries[]>(() => {
    if (viewMode === 'flows') return [];
    // aum: индекс (линия, левая, синий) + СЧА (область, правая, зелёная).
    const nav = data?.res.total_nav ?? [];
    if (!nav.length) return [];
    const out: LwSeries[] = [];
    const cat = CATS.find((c) => c.id === category);
    // Подписи — как на сайте (FundsMoneyPage): индекс назван своим тикером
    // (IMOEX/RGBITR/…), СЧА — с категорией в родительном падеже. Единицу СЧА
    // в подпись не выносим: ось здесь адаптивная (млрд/млн/тыс), в отличие от
    // сайта, где она всегда в млрд. СЧА идёт ПЕРВОЙ (site reverseLegend).
    out.push(applyFormat({
      id: 'scha', type: 'area', scale: 'right', color: 'var(--chart-line-3)',
      areaTop: 'color-mix(in srgb, var(--chart-line-3) 22%, transparent)', lineWidth: 2,
      label: `СЧА фондов ${cat?.genitive ?? ''}`.trim(),
      data: nav.flatMap((p) => (p.nav != null ? [{ time: toSec(p.date), value: p.nav }] : [])),
      axisFmt: fmtAbs, tipFmt: (v) => fmtAbs(v) + ' ₽',
    }, fmt));
    const idx = data?.res.index?.data;
    if (showIndex && idx?.length) {
      out.push({
        id: 'idx', type: 'line', scale: 'left', color: 'var(--chart-line-1)', lineWidth: 2,
        label: cat?.index ?? 'Индекс',
        data: idx.flatMap((d) => (d.close != null ? [{ time: toSec(d.date), value: d.close }] : [])),
        axisFmt: fmtInt, tipFmt: (v) => Math.round(v).toLocaleString('ru-RU'),
      });
    }
    return out;
  }, [viewMode, data, showIndex, fmt, category]);

  // Заголовок гистограммы — как на сайте, но короткий вариант включается по
  // ШИРИНЕ ПАНЕЛИ, а не по viewport: в песочнице viewport это окно браузера, а
  // строка живёт в панели. Режим сглаживания берём из эха бека (отображаемая
  // пара), а не из контрола: заголовок не должен опережать бары.
  const flowTitle = useMemo(() => {
    const gen = pairCat?.genitive ?? '';
    const short = containerW > 0 && containerW < 420;
    return flowsPair?.flows.rolling === '3m'
      ? (short ? 'Потоки за скользящие 3 месяца (млрд ₽)' : `Потоки из фондов ${gen} за скользящие 3 месяца (млрд ₽)`)
      : (short ? 'Чистые притоки и оттоки (млрд ₽)' : `Чистые притоки и оттоки из фондов ${gen} (млрд ₽)`);
  }, [pairCat, containerW, flowsPair]);

  // Фильтр фондов — кнопка тулбара с чек-листом (тот же приём, что «Участники»
  // у потоков ЦБ). ⚠️ Только для режима потоков: /api/funds/flows принимает
  // fund_ids, а /api/funds/chart отдаёт СЧА уже просуммированной по категории —
  // фильтровать её на клиенте нечем, и контрол в режиме СЧА молча ничего бы
  // не делал.
  const toggleFund = (id: number) => {
    setHiddenFunds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      // Последний видимый фонд выключить нельзя — иначе считать нечего.
      else if (visibleFundIds.length > 1) next.add(id);
      return next;
    });
  };
  // Открывается ТА ЖЕ модалка, что на странице (FundPickerModal со списком
  // FundsTable — тикер, название, СЧА, доходность), а не свой чек-лист: одно
  // действие не должно иметь два разных вида. Модалка идёт порталом в body,
  // поэтому не обрезается панелью песочницы.
  // Гость/free: модалка открывается всегда (2026-08-10) — запертому тиру
  // список слегка заблюрен, поверх апселл на Basic (FundPickerModal locked);
  // бэкенд у таких тиров игнорирует fund_ids, так что данные не утекают.
  const fundsFilter = viewMode === 'flows' && funds.length > 1 ? (
    <ToolbarButton
      label="Фонды"
      title={canPickFunds ? 'Какие фонды учитывать' : 'Выбор фондов — на тарифе Basic или Pro'}
      icon={canPickFunds ? <ListFilter size={14} /> : <Lock size={14} />}
      compact={toolbarCompact}
      onClick={() => setFundPickerOpen(true)}
    />
  ) : null;

  const st = viewMode === 'flows' ? flowsStatus : status;
  // Что уже нарисовано ПРЯМО СЕЙЧАС. Пока едет новый запрос, прежняя картинка
  // остаётся на экране — как у потоков ЦБ, где смена периода это клиентская
  // нарезка без похода на бэкенд. Раньше любой клик по таймфрейму/периоду ронял
  // flowsStatus в 'loading', а на нём висел рендер и графика, и кнопок справа:
  // ось значений и весь правый блок мигали и «возвращались» вместе с волной.
  const hasView = viewMode === 'flows' ? (flowsPair?.flows.flows.length ?? 0) > 0 : lwSeries.length > 0;
  const showChart = hasView && st !== 'error';

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
            <ToolbarButton label="Фонды" icon={<ListFilter size={14} />} onClick={() => {}} />
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
          {/* «Фонды» стоит сразу за категорией: это уточнение того же выбора —
              сначала какая категория, потом какие из её фондов. Шаг столбца и
              период — про ось времени, они правее. */}
          {fundsFilter}
          {viewMode === 'flows' && (
            <>
              <PillGroup value={flowTimeframe} options={FLOW_TFS} onChange={(v) => setFlowTimeframe(v)} compact={toolbarCompact} />
              <Dropdown value={flowPeriod} options={FLOW_PERIODS} onChange={(v) => setFlowPeriod(v)} title="Период" icon={<CalendarRange size={14} />} compact={toolbarCompact} />
            </>
          )}
        </div>
      }
      actions={<DrawExportActions draw={draw} visible={showChart} />}
      // ⚙ есть в ОБОИХ режимах (раньше — только в СЧА): слой «Индекс» на сайте
      // общий, и в потоках он включает верхнюю панель бенчмарка. Формат линии
      // относится к серии СЧА — он только в своём режиме.
      more={
        <>
          <DrawerSection label="Слои">
            <ToggleRow
              label="Индекс"
              checked={showIndex}
              onChange={setShowIndex}
              hint={viewMode === 'aum'
                ? `Линия ${pairCat?.index ?? 'индекса'} на второй оси`
                : `Панель ${pairCat?.index ?? 'индекса'} над гистограммой`}
            />
            {viewMode === 'flows' && (
              <ToggleRow
                label="Сумма за 3 месяца"
                checked={flowRolling === '3m'}
                onChange={(v) => setFlowRolling(v ? '3m' : 'none')}
                hint="Столбец — суммарный чистый поток за скользящие 3 месяца: сглаживает шум и проявляет устойчивые волны"
              />
            )}
          </DrawerSection>
          {viewMode === 'aum' && (
            <FormatSection fmt={fmt} onKind={setKind} onColor={setColor} onOpacity={setOpacity} onWidth={setWidth} defaultColor="var(--chart-line-3)" />
          )}
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {viewMode === 'flows' && showChart && (
          // emb-tooltip-ok — тултип этого графика в панели ОСТАВЛЯЕМ (см.
          // sandbox.css): значений в легенде нет, разбивку по бенчмарку и
          // потоку показывает только подсказка под курсором.
          // PAD_TOP — воздух под легенду: карточки с её паддингом в панели нет
          // (bare), а компонент подтягивает легенду на 20px вверх (компенсация
          // p-5 сайта). CHART_CHROME — легенды, подписи дат и навигатор:
          // --chart-height задаёт только поле графика (та же арифметика, что в
          // CompanyFlowsTab embedded).
          <div className="emb-tooltip-ok" style={{ position: 'absolute', inset: 0, paddingTop: 12, boxSizing: 'border-box' }}>
            <FlowsHistogram
              bare
              flowsData={flowsPair?.flows ?? null}
              loading={flowsStatus === 'loading'}
              flowTitle={flowTitle}
              indexData={flowsPair?.index}
              indexLabel={pairCat?.index}
              showIndex={showIndex}
              height={Math.max(160, chartH - 12 - 56)}
              animTrigger={`${flowsPair?.flows.category}|${flowsPair?.flows.timeframe}|${flowsPair?.flows.period}|${flowsPair?.flows.rolling ?? 'none'}`}
            />
          </div>
        )}
        {viewMode !== 'flows' && showChart && (
          <LwChartPanes
            ref={lwChartRef}
            panes={[{ series: lwSeries }]}
            // Статичный вид (как у потоков капитала): пан и зум выключены,
            // график всегда показывает всю историю и подстраивается под панель.
            // Здесь смысл в картине целиком, а не в разглядывании участка.
            staticView
            drawPaneIndex={0}
            dark={dark}
            fitKey={`aum|${data?.key ?? ''}`}
            tickFmt={monthsYearsTickFmt}
            drawActive={draw.drawMode}
            drawTool={draw.drawTool}
            drawings={draw.drawings}
            onDrawingsChange={draw.setDrawings}
            drawColor={draw.drawColor}
            drawWidth={draw.drawWidth}
            drawDash={draw.drawDash}
            drawOpacity={draw.drawOpacity}
            drawFill={draw.drawFill}
            drawFillColor={draw.drawFillColor}
            drawFillOpacity={draw.drawFillOpacity}
            selectedDrawId={draw.selectedDrawId}
            onSelectDraw={draw.setSelectedDrawId}
            onSelectionRect={draw.setSelRect}
            onToolReset={draw.onToolReset}
            drawHidden={draw.drawHidden}
            drawLocked={draw.drawLocked}
          />
        )}
        <DrawToolsOverlay draw={draw} visible={showChart} />
        {st === 'loading' && !hasView && <EmbedMsg text="Загрузка…" />}
        {st === 'empty' && <EmbedMsg text="Нет данных" />}
        {st === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        {fundPickerOpen && (
          <FundPickerModal
            data={data?.res ?? null}
            hiddenFunds={hiddenFunds}
            onSetHiddenFunds={setHiddenFunds}
            onToggleFundVisibility={toggleFund}
            locked={!canPickFunds}
            onClose={() => setFundPickerOpen(false)}
            categoryGenitive={CATS.find((c) => c.id === category)?.genitive}
          />
        )}
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
