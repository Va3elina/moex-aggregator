import React, { useEffect, useLayoutEffect, useState, useMemo, useRef, useCallback } from 'react';
import { TrendingUp, DollarSign, Banknote, Wallet, JapaneseYen, AlarmClock, Lock, ChevronDown } from 'lucide-react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import PageHeader from '../components/PageHeader';
import SegmentedControl from '../components/SegmentedControl';
import HelpTooltip from '../components/HelpTooltip';
import ChartTabs from '../components/ChartTabs';
import LayersButton from '../components/LayersButton';
import ChartActionsMenu from '../components/ChartActionsMenu';
import { METHODOLOGY } from '../data/methodology';
import {
    getFundsChartData,
    getFundsFlows,
    type FundsChartResponse,
    type FundsFlowsResponse,
    type FundCategory,
    type FundPeriod,
    type FlowTimeframe
} from '../services/api';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { usePersistedState, usePersistedSet } from '../hooks/usePersistedState';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { useViewportWidth } from '../hooks/useViewportWidth';
import FundPickerModal from '../components/funds/FundPickerModal';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { buildFundsMoneyTour } from '../data/tours/funds-money';
import FlowsHistogram from '../components/funds/FlowsHistogram';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import ChartSettings from '../components/chart/ChartSettings';
import CsvExportButton from '../components/export/CsvExportButton';
import { periodToQuery } from '../utils/csvPeriod';
import { useTierAccess, useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { handleTierError } from '../utils/tierError';
import CreateFundAlertModal from '../components/alerts/CreateFundAlertModal';

// Режимы отображения
type ViewMode = 'aum' | 'flows';

// Периоды
type Period = '1m' | '1y' | '3y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
    '1m': '1М',
    '1y': '1Г',
    '3y': '3Г',
    'all': 'Всё'
};

// Видимый набор периодов: 1М / 1Г / 3Г / Всё (унификация). 5Л у фондов нет —
// глубина данных БПИФов меньше пяти лет. Используется и для СЧА, и как фильтр
// списка периода.
const AUM_PERIODS: Period[] = ['1m', '1y', '3y', 'all'];

// Категории
// `genitive` — родительный падеж для подстановки в шаблоны вида
// Категория «Золото»: глиф из логотипа TradingView для GLDRUB_TOM — три слитка
// пирамидкой, нарисованы как полые трапеции (внешняя минус внутренняя, nonzero-
// заливка). Золотой фон-квадрат TradingView убран (прозрачный), заливка →
// currentColor, поэтому иконка монохромная и наследует цвет вкладки, как соседние
// lucide-иконки. viewBox обрезан по bounding box слитков, чтобы глиф заполнял слот
// по ширине (иначе выглядел мелким). Пропсы как у lucide: ChartTabs зовёт с
// className="ct-ico" (размер из CSS --ico-sm), шапка-селектор — size + цвет в style.
function GoldBarsIcon({ size = 24, className, style }: { size?: number; strokeWidth?: number; className?: string; style?: React.CSSProperties }) {
    return (
        <svg
            className={className}
            width={size}
            height={size}
            viewBox="8 13 40 25"
            fill="currentColor"
            aria-hidden="true"
            style={style}
        >
            <path d="M21.248 21.555h13.784l-2.01-5.393a1.17 1.17 0 00-.41-.553l-11.364 5.946zm-.038-6.401C21.698 13.842 22.772 13 23.956 13h8.151c1.184 0 2.258.842 2.747 2.154l2.009 5.393c.603 1.618-.371 3.453-1.831 3.453h-14c-1.46 0-2.433-1.835-1.831-3.453l2.01-5.393h-.001zM10.235 35.555h13.757l-2.01-5.393a1.171 1.171 0 00-.41-.553l-11.337 5.946zm-.039-6.401C10.685 27.842 11.76 27 12.943 27h8.124c1.184 0 2.259.842 2.747 2.154l2.009 5.393c.603 1.618-.37 3.453-1.831 3.453H10.017c-1.46 0-2.433-1.835-1.83-3.453l2.01-5.393zm35.89 6.401h-13.85l11.43-5.945c.179.126.323.316.413.553l2.008 5.392zM34.945 27c-1.184 0-2.259.842-2.747 2.154l-2.009 5.393c-.603 1.618.37 3.453 1.831 3.453h14.067c1.46 0 2.433-1.835 1.83-3.453l-2.01-5.393C45.422 27.842 44.348 27 43.164 27h-8.22z" />
        </svg>
    );
}

// «фонды {genitive}» / «приток в фонды {genitive}». Русское склонение
// нерегулярное — храним как data, а не вычисляем.
const CATEGORIES: { key: FundCategory; name: string; genitive: string; icon: React.ElementType; index: string; comingSoon?: boolean }[] = [
    { key: 'money_market', name: 'Денежный рынок', genitive: 'денежного рынка', icon: Banknote, index: 'RUSFAR3M' },
    { key: 'stocks', name: 'Акции', genitive: 'акций', icon: TrendingUp, index: 'IMOEX' },
    { key: 'bonds', name: 'Облигации', genitive: 'облигаций', icon: DollarSign, index: 'RGBITR' },
    { key: 'gold', name: 'Золото', genitive: 'золота', icon: GoldBarsIcon, index: 'GLDRUB_TOM' },
    // Раздел «Юань» — рабочий (NAV юаневых фондов в ₽, бенчмарк RUSFARCNY).
    { key: 'yuan', name: 'Юань', genitive: 'юаня', icon: JapaneseYen, index: 'RUSFARCNY' },
];

// Цвета СЧА графика — theme-aware. Primary (СЧА) = accent (рыжий), secondary
// (индекс) = forest-green из funds-flow палитры — единый visual-язык с flows.
const INDEX_COLOR = 'var(--funds-flow-positive)';
const NAV_COLOR   = 'var(--accent)';

// Easing для анимации гистограммы
import { ANIMATION } from '../config/chartTheme';
const easeOutCubic = ANIMATION.easing;


export default function FundsMoneyPage() {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();
    // Настройки отображения персистятся в localStorage — не сбрасываются на новой сессии.
    const [category, setCategory] = usePersistedState<FundCategory>('frame:funds:category', 'money_market');
    // Выбор раздела пишем в ?category= (replace, без замусоривания истории) — чтобы
    // адресная строка всегда отражала текущий фонд и URL можно было сохранить в
    // избранное, как ?instrument= на /oi. Чтение ?category= ниже остаётся.
    const selectCategory = (c: FundCategory) => {
        setCategory(c);
        const next = new URLSearchParams(searchParams);
        next.set('category', c);
        setSearchParams(next, { replace: true });
    };
    // Диплинк из сигнала/аномалии: ?category= преселектит раздел. Применяем при
    // КАЖДОЙ навигации (не только на маунте) — иначе клик по второй fund-аномалии
    // другой категории не переключал бы раздел: SPA не перемонтирует /funds-money.
    // Гард по строке URL — пользовательское переключение (localStorage, без
    // ?category=) не вызывает повторов.
    const appliedFundsUrlRef = useRef('');
    useEffect(() => {
        const urlKey = searchParams.toString();
        if (urlKey === appliedFundsUrlRef.current) return;
        appliedFundsUrlRef.current = urlKey;
        const c = searchParams.get('category');
        if (c && ['money_market', 'stocks', 'bonds', 'gold', 'yuan'].includes(c)) {
            setCategory(c as FundCategory);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchParams]);
    const [period, setPeriod] = usePersistedState<Period>('frame:funds:period', getDefaultPeriod('1y', isAuthenticated) as Period);
    // Default режим — Притоки-Оттоки (более информативно для нового пользователя)
    const [viewMode, setViewMode] = usePersistedState<ViewMode>('frame:funds:viewMode', 'flows');
    const [flowTimeframe, setFlowTimeframeRaw] = usePersistedState<FlowTimeframe>('frame:funds:flowTimeframe', '1d');
    const fundsAccess = useTierAccess('funds_money');
    const { showUpgrade } = useUpgradePrompt();
    // Алерты в мессенджере — квота по тарифу (0=Free/гость → апселл, как у OI-колокола).
    const alertsQuota = useCommonFeatures().telegram_alerts_quota;
    const alertsLocked = alertsQuota === 0;
    const [fundAlertOpen, setFundAlertOpen] = useState(false);

    // Динамическая высота графика — chartAnchorRef как в OI page
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const chartHeight = useFitToViewport(chartAnchorRef, {
        min: 360,
        max: 720,
        bottomBuffer: 64,
    });

    // Ограничения периодов для flow таймфреймов (как на ОИ)
    const FLOW_MIN_PERIODS: Record<FlowTimeframe, Period[]> = {
        '1d': ['1m', '1y', '3y', 'all'],
        '1w': ['1y', '3y', 'all'],
        '1m': ['1y', '3y', 'all'],
        '3m': ['3y', 'all'],
        '1y': ['3y', 'all'],
    };

    const isFlowPeriodAvailable = (p: Period): boolean => {
        if (viewMode !== 'flows') return true;
        const allowed = FLOW_MIN_PERIODS[flowTimeframe] || FLOW_MIN_PERIODS['1w'];
        return allowed.includes(p);
    };

    const setFlowTimeframe = (tf: FlowTimeframe) => {
        setFlowTimeframeRaw(tf);
        const allowed = FLOW_MIN_PERIODS[tf];
        if (!allowed.includes(period)) {
            setPeriod(allowed[0]);
        }
    };

    // Smart default: для Free '1d' недоступен → переключаем на '1w'
    const defaultTfSwitchedRef = useRef(false);
    useEffect(() => {
        if (fundsAccess.isLoading || defaultTfSwitchedRef.current) return;
        defaultTfSwitchedRef.current = true;
        if (!fundsAccess.canUseTimeframe(flowTimeframe)) {
            setFlowTimeframeRaw('1w');
        }
    }, [fundsAccess.isLoading, fundsAccess, flowTimeframe]);

    // Tier-коррекция периода: сохранённый/дефолтный период мог быть заперт тарифом
    // (гость: дефолт '1y' через GUEST_MAX_PERIOD, а funds_money лимит 180д → 403 на
    // загрузке). Опускаем до максимально доступного. Только в AUM: в flows период
    // подчиняется FLOW_MIN_PERIODS (≥ минимума ТФ), а для гостя flows тариф-недоступен
    // целиком → там 403 ловит handleTierError в loadFlowsData (upgrade-модалка).
    useEffect(() => {
        if (fundsAccess.isLoading || viewMode !== 'aum') return;
        if (!fundsAccess.canUsePeriod(period)) {
            const allowed = AUM_PERIODS.filter(p => fundsAccess.canUsePeriod(p));
            if (allowed.length) setPeriod(allowed[allowed.length - 1]);
        }
    }, [fundsAccess.isLoading, fundsAccess, viewMode, period, setPeriod]);

    // Доступны ли потоки на текущем тарифе ВООБЩЕ: нужна хоть одна пара (ТФ, период),
    // где ТФ открыт по тарифу И его минимальный период (FLOW_MIN_PERIODS) тоже открыт.
    // Для free лимит истории 180д несовместим с недельной/месячной гранулярностью
    // (нужно ≥1Г), а дневной ТФ — Pro → потоки недоступны целиком. `!isLoading &&`
    // — пока tier грузится, НЕ показываем замок/НЕ дёргаем (не запираем платника).
    const flowsTierOk = !fundsAccess.isLoading && (['1d', '1w', '1m'] as FlowTimeframe[]).some(
        tf => fundsAccess.canUseTimeframe(tf)
            && (FLOW_MIN_PERIODS[tf] ?? []).some(p => fundsAccess.canUsePeriod(p)),
    );
    // Текущий период заперт тарифом (tier уже резолвнут). persisted-период от прошлой
    // авторизованной сессии (напр. 1Г у гостя) → 403 → ВСПЫШКА модалки до того, как
    // tier-коррекция опустит период. Гейтим загрузчики этим флагом. (При isLoading
    // флаг false → грузим как обычно: не знаем лимита + не виснем, если матрица упала.)
    const periodLocked = !fundsAccess.isLoading && !fundsAccess.canUsePeriod(period);

    // Гость/free на дефолтном flows+1Г → один раз при загрузке тарифа переводим на
    // СЧА (рабочий вид). Сам клик по залоченным «Притоки-Оттоки» → upgrade-промпт.
    const flowsModeCheckedRef = useRef(false);
    useEffect(() => {
        if (fundsAccess.isLoading || flowsModeCheckedRef.current) return;
        flowsModeCheckedRef.current = true;
        if (!flowsTierOk && viewMode === 'flows') setViewMode('aum');
    }, [fundsAccess.isLoading, flowsTierOk, viewMode, setViewMode]);

    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<FundsChartResponse | null>(null);
    const [flowsData, setFlowsData] = useState<FundsFlowsResponse | null>(null);
    // Отдельный спиннер для притоков-оттоков: nav-загрузчик (loadData) и flows-загрузчик
    // делят страницу, но НЕ должны делить флаг loading — иначе быстрый nav-ответ гасил
    // спиннер, пока flows ещё грузится («даже не начал обновляться» при смене категории).
    const [flowsLoading, setFlowsLoading] = useState(false);
    // Скрытые фонды. Персистим по категории (frame:funds:hidden:<category>) —
    // выбор не сбрасывается на новой сессии и хранится отдельно для каждой
    // категории; смена категории перечитывает набор под новый ключ (это заменяет
    // прежний reset в пустой Set).
    const [hiddenFunds, setHiddenFunds] = usePersistedSet<number>(`frame:funds:hidden:${category}`);
    // ?funds= из диплинка применяется один раз (после загрузки funds); флаг от повторов.
    const fundsFilterAppliedRef = useRef(false);
    const [collapsedSubcats, setCollapsedSubcats] = useState<Set<string>>(new Set());
    const [navSortDir, setNavSortDir] = useState<'desc' | 'asc'>('desc');
    // Выбор фондов вынесен в разворачивающийся виджет-модалку (масштаб как у
    // селектора актива на ОИ): таблетка сверху → модалка со списком фондов.
    const [fundPickerOpen, setFundPickerOpen] = useState(false);
    const [hoveredFlowIndex, setHoveredFlowIndex] = useState<number | null>(null);
    // Tooltip position через STATE а не DOM-мутацию — иначе после React re-render
    // позиция сбрасывается до следующего mousemove → визуальный "коэффициент".
    // Паттерн как в SeasonalityHistogram (handlePointerMove → setTooltip({x,y})).
    const [flowTooltipPos, setFlowTooltipPos] = useState<{ x: number; y: number } | null>(null);
    const [showIndex, setShowIndex] = usePersistedState('frame:funds:showIndex', true);
    const [flowNavRange, setFlowNavRange] = useState<[number, number]>([0, 0]);

    // Onboarding tour. Steps собираются через factory чтобы тур мог
    // автоматически переключать viewMode (СЧА ↔ Притоки) когда подсвечивает
    // соответствующий контрол — иначе юзер читает про СЧА а на графике
    // ещё Притоки (или наоборот).
    const tour = useOnboardingTour('funds-money');
    const fundsMoneyTourSteps = useMemo(
      () => buildFundsMoneyTour(setViewMode),
      [setViewMode],
    );
    const flowChartRef = useRef<SVGSVGElement>(null);
    const flowContainerRef = useRef<HTMLDivElement>(null);

    // Анимация баров гистограммы (морфинг при смене данных)
    const [animatedBarsIn, setAnimatedBarsIn] = useState<number[]>([]);
    const [animatedBarsOut, setAnimatedBarsOut] = useState<number[]>([]);
    const prevBarsInRef = useRef<number[]>([]);
    const prevBarsOutRef = useRef<number[]>([]);
    const barsAnimRef = useRef<number | null>(null);
    const isFirstBarsRender = useRef(true);

    // Загрузка данных
    // Stale-guard: при быстром переключении категории/периода медленный ранний
    // ответ мог перезаписать свежий. reqId фиксирует «последний» запрос.
    const reqIdRef = useRef(0);
    const loadData = useCallback(async () => {
        // Не фетчим с запертым тарифом периодом (persisted 1Г у гостя) → не даём 403/
        // вспышку модалки. Держим спиннер: tier-коррекция опустит период до доступного
        // → periodLocked станет false → loadData перезапустится и догрузит данные.
        if (periodLocked) { setLoading(true); return; }
        const reqId = ++reqIdRef.current;
        const isStale = () => reqId !== reqIdRef.current;
        try {
            setLoading(true);
            setError(null);
            const result = await getFundsChartData(category, period as FundPeriod);
            if (isStale()) return;
            setData(result);
        } catch (err) {
            if (isStale()) return;
            if (!handleTierError(err, {
                showUpgrade,
                indicator: 'funds_money',
                featureName: 'индикатор «Деньги в фондах»',
                onTier: () => setError(null),
            })) {
                setError('Ошибка загрузки данных');
            }
            console.error(err);
        } finally {
            if (!isStale()) setLoading(false);
        }
    }, [category, period, showUpgrade, periodLocked]);

    useEffect(() => { loadData(); }, [loadData]);

    // SSE: автоматическое обновление при новых данных
    useRealtimeData(['funds'], loadData);

    // Видимые fund_ids (для фильтрации flows)
    // Доступные (не tier-locked) фонды — только они дают данные на графики.
    // Locked-фонды есть в data.funds (для FundsTable-тизера), но в расчётах
    // притоков/СЧА не участвуют.
    const accessibleFunds = useMemo(
        () => data?.funds.filter(f => !f.tier_locked) ?? [],
        [data?.funds],
    );
    // Диплинк ?funds= из Telegram-сигнала: показать ТОЛЬКО сигнальные фонды внутри
    // категории (остальные прячем через hiddenFunds). Один раз — после загрузки funds
    // и после category-reset; tier-locked не в accessibleFunds → просто отсутствуют.
    useEffect(() => {
        if (fundsFilterAppliedRef.current || !data?.funds) return;
        const raw = searchParams.get('funds');
        if (!raw) return;
        fundsFilterAppliedRef.current = true;
        const want = new Set(raw.split(',').map(s => parseInt(s, 10)).filter(n => !isNaN(n)));
        if (want.size === 0) return;
        setHiddenFunds(new Set(accessibleFunds.filter(f => !want.has(f.fund_id)).map(f => f.fund_id)));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data?.funds, accessibleFunds]);

    // Видимые доступные = accessible минус скрытые пользователем.
    const visibleAccessibleFunds = useMemo(
        () => accessibleFunds.filter(f => !hiddenFunds.has(f.fund_id)),
        [accessibleFunds, hiddenFunds],
    );


    // Все доступные фонды выключены — нужен empty-state вместо ошибки.
    const noFundsSelected = data != null
        && accessibleFunds.length > 0
        && visibleAccessibleFunds.length === 0;

    // Выбраны не все фонды → счётчик на кнопке подсвечивается акцентом
    // (сигнал, что на графике не вся категория, а подвыборка).
    const fundsPartiallySelected = accessibleFunds.length > 0
        && visibleAccessibleFunds.length < accessibleFunds.length;

    const visibleFundIds = useMemo(() => {
        if (!data?.funds) return undefined;
        // Все доступные видимы — не передаём фильтр (backend вернёт tier-set).
        if (visibleAccessibleFunds.length === accessibleFunds.length) return undefined;
        return visibleAccessibleFunds.map(f => f.fund_id);
    }, [data?.funds, visibleAccessibleFunds, accessibleFunds]);

    // Загрузка данных притоков/оттоков
    // Stale-guard (как у loadData): эффект перезапускается несколько раз при смене
    // категории (сначала category, затем — после загрузки nav-data — пересчёт
    // visibleFundIds, затем reset hiddenFunds). Без reqId медленный РАННИЙ ответ
    // (старая категория) мог перезаписать свежий → «график не обновился до рефреша».
    const flowsReqIdRef = useRef(0);
    useEffect(() => {
        if (viewMode !== 'flows') return;
        // Запертый тарифом период (persisted 1Г у гостя) → не дёргаем flows (403/
        // вспышка модалки). Part B уведёт на СЧА, либо tier-коррекция опустит период.
        if (periodLocked) return;

        // Все фонды выключены — не дёргаем backend, ставим пустой результат.
        // FlowsHistogram по noFundsSelected покажет «Выберите фонды».
        if (noFundsSelected) {
            flowsReqIdRef.current++; // отменяем любой in-flight flows-запрос
            setFlowsData({ category, timeframe: flowTimeframe, period, flows: [] });
            setFlowsLoading(false);
            return;
        }

        const reqId = ++flowsReqIdRef.current;
        const isStale = () => reqId !== flowsReqIdRef.current;

        async function loadFlowsData() {
            try {
                setFlowsLoading(true);
                const result = await getFundsFlows(category, flowTimeframe, period as FundPeriod, visibleFundIds);
                if (isStale()) return;
                setFlowsData(result);
            } catch (err) {
                if (isStale()) return;
                // Tier-ошибка (403: период/таймфрейм за лимитом гостя) → upgrade-модалка,
                // как у loadData. Раньше тут был тихий console.error → притоки-оттоки
                // молча ломались для гостя («весь в ошибках, нельзя смотреть»).
                if (!handleTierError(err, {
                    showUpgrade,
                    indicator: 'funds_money',
                    featureName: 'притоки-оттоки фондов',
                    onTier: () => setFlowsData({ category, timeframe: flowTimeframe, period, flows: [] }),
                })) {
                    console.error('Flows error:', err);
                }
            } finally {
                if (!isStale()) setFlowsLoading(false);
            }
        }
        loadFlowsData();
    }, [viewMode, category, flowTimeframe, period, visibleFundIds, noFundsSelected, showUpgrade, periodLocked]);

    // Агрегация данных на основе видимых фондов
    const aggregatedData = useMemo(() => {
        if (!data?.funds) return { chartData: [], totalCurrentNav: 0 };

        const visibleFunds = data.funds.filter(f => !hiddenFunds.has(f.fund_id));

        // Собираем все уникальные даты
        const allDates = new Set<string>();
        visibleFunds.forEach(fund => {
            fund.data.forEach(p => allDates.add(p.date));
        });
        const responsiveDates = Array.from(allDates).sort();

        // Forward-fill: для каждого фонда строим карту date→nav с протяжкой
        // (ПИФы и БПИФы публикуют данные в разные дни — без ffill будут провалы)
        const fundNavMaps = visibleFunds.map(fund => {
            const map = new Map<string, number>();
            let lastNav = 0;
            const sorted = [...fund.data].sort((a, b) => a.date.localeCompare(b.date));
            for (const d of responsiveDates) {
                const point = sorted.find(p => p.date === d);
                if (point?.nav) lastNav = point.nav;
                if (lastNav > 0) map.set(d, lastNav);
            }
            return map;
        });

        // Суммируем NAV по датам (с forward-fill)
        const chartData = responsiveDates.map(date => {
            let totalNav = 0;
            fundNavMaps.forEach(navMap => {
                totalNav += navMap.get(date) || 0;
            });
            return {
                time: date,
                value: totalNav / 1e9 // млрд руб
            };
        });

        // Текущая суммарная СЧА (последняя точка)
        const totalCurrentNav = chartData.length > 0 ? chartData[chartData.length - 1].value : 0;

        return { chartData, totalCurrentNav };
    }, [data, hiddenFunds]);

    // Данные индекса (на вторичной оси)
    const indexData = useMemo(() => {
        if (!data?.index?.data) return undefined;
        return data.index.data.map(d => ({
            time: d.date,
            value: d.close || 0
        }));
    }, [data]);

    // Форматирование значений СЧА:
    // Золото и акции — 2 знака после точки (сотые млрд = десятки млн)
    // Остальные (облигации, денежный рынок) — без дробной части (крупные суммы)
    const formatNav = (value: number) => {
        if (category === 'gold' || category === 'stocks') {
            return value.toFixed(2);
        }
        return value.toFixed(0);
    };


    const toggleFundVisibility = (fundId: number) => {
        setHiddenFunds(prev => {
            const next = new Set(prev);
            if (next.has(fundId)) {
                next.delete(fundId);
            } else {
                next.add(fundId);
            }
            return next;
        });
    };

    const handleFlowMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!flowsData?.flows?.length || !flowContainerRef.current || !flowChartRef.current) return;
        // ВАЖНО: измеряем РЕАЛЬНУЮ геометрию через SVG-элемент, а не через
        // parseFloat(getComputedStyle(--chart-pad-left)). На mobile чарт-pad
        // задан через clamp() — getPropertyValue вернёт сырую строку
        // "clamp(34px, 11vw, 58px)", parseFloat = NaN → fallback 100. Реальный
        // pad на mobile ~41px → mismatch создаёт "коэффициент 2x" между
        // движением пальца и cursor'ом. SVG getBoundingClientRect даёт точные
        // computed pixels.
        const containerRect = flowContainerRef.current.getBoundingClientRect();
        const svgRect = flowChartRef.current.getBoundingClientRect();
        const xInChart = e.clientX - svgRect.left;
        if (xInChart < 0 || xInChart > svgRect.width) return;
        const visibleCount = flowNavRange[1] - flowNavRange[0] + 1;
        const barWidth = svgRect.width / visibleCount;
        const idx = Math.floor(xInChart / barWidth);
        if (idx >= 0 && idx < visibleCount) {
            setHoveredFlowIndex(idx);
            // x в координатах outer-container'а: offset до SVG + позиция в SVG
            const slotCenterInContainer = (svgRect.left - containerRect.left) + idx * barWidth + barWidth / 2;
            const y = e.clientY - containerRect.top;
            setFlowTooltipPos({ x: slotCenterInContainer, y });
        }
    };

    const handleFlowMouseLeave = useCallback(() => {
        setHoveredFlowIndex(null);
        setFlowTooltipPos(null);
    }, []);

    // Сброс анимации при выходе из режима flows — следующее появление будет fade-in из нуля
    useEffect(() => {
        if (viewMode !== 'flows') {
            if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);
            setAnimatedBarsIn([]);
            setAnimatedBarsOut([]);
            prevBarsInRef.current = [];
            prevBarsOutRef.current = [];
            isFirstBarsRender.current = true;
        }
    }, [viewMode]);

    // Сброс навигатора при смене данных — useLayoutEffect (а не useEffect) чтобы
    // обновление срабатывало ДО первого paint'a после прихода данных, иначе rect
    // селект-окна моментально мелькает с width=0 → full width.
    useLayoutEffect(() => {
        if (flowsData?.flows?.length) {
            setFlowNavRange([0, flowsData.flows.length - 1]);
        }
    }, [flowsData]);

    // Анимация гистограммы при смене flowsData.
    // Всегда начинаем с нуля + каскад слева направо (волна),
    // а не морфим из предыдущих значений — при переключении
    // день/неделя/месяц данные полностью разные, морфинг
    // показывал хаотичную перестановку баров.
    useEffect(() => {
        if (!flowsData?.flows?.length) return;

        if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);

        const targetFlows = flowsData.flows.map(f => f.flow);
        const fromFlows = new Array(targetFlows.length).fill(0);

        isFirstBarsRender.current = false;

        // Каскадная анимация: бары появляются слева направо (волна).
        // Параметры из единого конфига chartTheme.ANIMATION.
        const totalDuration = ANIMATION.waveDuration;
        const staggerDelay = ANIMATION.waveStagger;
        let startTime: number | null = null;

        const animate = (timestamp: number) => {
            if (!startTime) startTime = timestamp;
            const elapsed = timestamp - startTime;

            const flows = targetFlows.map((v, i) => {
                const barDelay = (i / targetFlows.length) * staggerDelay;
                const barElapsed = Math.max(0, elapsed - barDelay);
                const t = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
                return fromFlows[i] + (v - fromFlows[i]) * easeOutCubic(t);
            });

            // Разделяем на in/out по знаку текущего анимированного значения
            setAnimatedBarsIn(flows.map(v => Math.max(0, v)));
            setAnimatedBarsOut(flows.map(v => Math.min(0, v)));

            if (elapsed < totalDuration) {
                barsAnimRef.current = requestAnimationFrame(animate);
            } else {
                prevBarsInRef.current = targetFlows;
                prevBarsOutRef.current = [];
            }
        };

        barsAnimRef.current = requestAnimationFrame(animate);

        return () => {
            if (barsAnimRef.current) cancelAnimationFrame(barsAnimRef.current);
        };
    }, [flowsData]);

    const currentCategory = CATEGORIES.find(c => c.key === category);
    const CatIcon = currentCategory?.icon;

    // Дата данных + флаг «часть фондов запаздывает» — для шапки модалки выбора
    // фондов (та же логика, что в FundsTable для меток строк). maxDate = самый
    // свежий trade_date; laggardDate = самый ранний среди не-locked фондов.
    let fundsMaxDate = '';
    let fundsLaggardDate = '';
    for (const f of data?.funds ?? []) {
        const d = f.data[f.data.length - 1]?.date;
        if (!d) continue;
        if (d > fundsMaxDate) fundsMaxDate = d;
        if (f.tier_locked !== true && (!fundsLaggardDate || d < fundsLaggardDate)) fundsLaggardDate = d;
    }
    const fundsHasStale = !!(fundsLaggardDate && fundsMaxDate && fundsLaggardDate < fundsMaxDate);

    // Обобщающий заголовок гистограммы притоков/оттоков. Единица — в скобках
    // «(млрд ₽)». На узких viewport'ах убираем подробности категории, чтобы
    // строка влезала в одну линию (540px — порог, на котором длинный заголовок
    // ещё помещается в типичный mobile-card padding ~16px по бокам).
    const vw = useViewportWidth();
    const useShortFlowLabels = vw < 540;
    const flowTitle = useShortFlowLabels
        ? 'Чистые притоки и оттоки (млрд ₽)'
        : `Чистые притоки и оттоки из фондов ${currentCategory?.genitive ?? ''} (млрд ₽)`;

    return (
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
            <PageHeader
                icon={Wallet}
                title="Деньги в фондах"
                subtitle="Динамика СЧА фондов и индексов"
                help={METHODOLOGY.fundsMoney}
                helpLink="/methodology/funds-money"
                sourceNote="Индексы (IMOEX, RGBI, IMOEX2, GLDRUB): ПАО Московская Биржа"
            />

            {/* Карточка с вкладками: обёртка несёт единую editorial-тень на
                [вкладки + панель], иначе тень обрывалась бы у вкладок справа. */}
            <div className="tabbed-card">

            {/* Вкладки выбора фонда — приклеены к верхней кромке editorial-frame.
                Активная сливается с панелью, неактивные затемнены. Контролы
                графика живут внутри панели ниже (см. has-tabs). */}
            <ChartTabs<FundCategory>
                tourId="funds-categories"
                value={category}
                onChange={selectCategory}
                items={CATEGORIES.map(c => ({
                    key: c.key,
                    label: c.name,
                    sublabel: c.comingSoon ? 'Скоро' : undefined,
                    Icon: c.icon,
                    disabled: c.comingSoon,
                    title: c.comingSoon ? 'Раздел скоро появится' : c.name,
                }))}
            />

            {/* Editorial frame — обнимает controls + chart. has-tabs: верх под вкладки. */}
            <div className="editorial-frame has-tabs">

            {/* Контролы. items-center — иначе сегмент-контролы (40px) прилипают к
                верху рядом с более высокой (~57px) кнопкой выбора фондов. */}
            <div className="flex flex-wrap items-center mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
                {/* Селектор фондов — светлая таблетка того же формата, что селектор
                    актива на ОИ (widget-flat, иконка 24px без круга, две строки +
                    трейлинг-шеврон). Счётчик подсвечивается акцентом, когда выбраны
                    не все фонды — сигнал активного фильтра. */}
                <div data-tour="funds-table" style={{ order: 0 }}>
                <button
                    onClick={() => setFundPickerOpen(true)}
                    title="Выбрать фонды для графика"
                    className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
                    style={{
                        color: 'var(--text-primary)',
                        fontSize: 'var(--fs-sm)',
                        padding: 'var(--sp-2) var(--sp-4)',
                        gap: 'var(--sp-3)',
                        minWidth: 'clamp(150px, 22vw, 190px)',
                        maxWidth: 240,
                    }}
                >
                    {CatIcon && <CatIcon size={24} strokeWidth={2.2} style={{ flexShrink: 0, color: 'var(--text-secondary)' }} />}
                    <div className="flex-1 text-left" style={{ minWidth: 0 }}>
                        <div className="font-medium" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            Фонды: {currentCategory?.name ?? ''}
                        </div>
                        {/* Счётчик выбранных — цифры в небольшом сером бейдже (как в
                            прошлой версии кнопки). При частичном выборе бейдж
                            заливается акцентом — сигнал активного фильтра. */}
                        <div style={{ marginTop: 2 }}>
                            <span
                                style={{
                                    display: 'inline-block',
                                    fontSize: 'var(--fs-2xs)',
                                    fontWeight: 800,
                                    lineHeight: 1,
                                    borderRadius: 6,
                                    padding: '3px 6px',
                                    whiteSpace: 'nowrap',
                                    ...(fundsPartiallySelected
                                        ? { color: 'var(--text-inverse)', background: 'var(--accent)' }
                                        : { color: 'var(--text-secondary)', background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)' }),
                                }}
                            >
                                {visibleAccessibleFunds.length} / {accessibleFunds.length}
                            </span>
                        </div>
                    </div>
                    <ChevronDown size={14} className="text-theme-secondary" style={{ flexShrink: 0 }} />
                </button>
                </div>

                <div data-tour="funds-period" style={{ order: 3 }}>
                <SegmentedControl<Period>
                    options={(Object.keys(PERIOD_LABELS) as Period[])
                        .filter(p => AUM_PERIODS.includes(p))
                        .map((p) => ({
                            key: p,
                            label: PERIOD_LABELS[p],
                            // tier-замок по ПЕР-ИНДИКАТОРНОМУ лимиту funds_money
                            // (canUsePeriod = бэковый max_history_days, 180д для гостя),
                            // а НЕ по глобальному GUEST_MAX_PERIOD='1y'. Иначе «1Г» для
                            // гостя не запиралась (фронт думал 1y ок) → клик → 403.
                            locked: !fundsAccess.isLoading && !fundsAccess.canUsePeriod(p),
                        }))}
                    value={period}
                    onChange={(p) => {
                        // Период не влезает в текущий ТФ потоков → авто-переключаем
                        // на самый крупный ТФ, который его поддерживает и открыт по
                        // тарифу (без замочка-обманки, как на OI). Если ни один не
                        // открыт (нужен дневной, а он по тарифу) — апселл.
                        if (viewMode === 'flows' && !isFlowPeriodAvailable(p)) {
                            const tf = (['1m', '1w', '1d'] as FlowTimeframe[]).find(t =>
                                (FLOW_MIN_PERIODS[t] ?? []).includes(p)
                                && (fundsAccess.isLoading || fundsAccess.canUseTimeframe(t)));
                            if (tf) { setFlowTimeframeRaw(tf); setPeriod(p); return; }
                            const tier = fundsAccess.requiredTierFor({ timeframe: '1d' });
                            if (tier) { showUpgrade({ tier, featureName: 'дневной таймфрейм', indicator: 'funds_money' }); return; }
                        }
                        setPeriod(p);
                    }}
                    onLockedClick={(p) => {
                        // Tier-блокировка → upgrade modal; иначе legacy guest gate → /login.
                        // Locked только из-за !isFlowPeriodAvailable (нет данных) — ничего.
                        if (!fundsAccess.canUsePeriod(p)) {
                            const tier = fundsAccess.requiredTierFor({ period: p });
                            if (tier) {
                                showUpgrade({ tier, featureName: `период «${PERIOD_LABELS[p]}»`, indicator: 'funds_money' });
                                return;
                            }
                        }
                        if (!isPeriodAllowed(p, isAuthenticated)) navigate('/login');
                    }}
                />
                </div>

                {/* Режим: Притоки-оттоки / СЧА — горизонтальный переключатель + «?» с пояснением режимов */}
                <div data-tour="funds-view-mode" style={{ order: 1, display: 'flex', alignItems: 'center', gap: 6 }}>
                <SegmentedControl<ViewMode>
                    options={[
                        // Потоки для free недоступны целиком (лимит 180д vs ≥1Г + дневной
                        // ТФ=Pro) → показываем замок, а не «кликабельно но сразу 403».
                        { key: 'flows', label: 'Притоки-Оттоки', locked: !fundsAccess.isLoading && !flowsTierOk },
                        { key: 'aum',   label: 'СЧА' },
                    ]}
                    value={viewMode}
                    onChange={(m) => {
                        setViewMode(m);
                        if (m === 'aum') {
                            if (!AUM_PERIODS.includes(period)) setPeriod('1m');
                        } else if (!(FLOW_MIN_PERIODS[flowTimeframe] ?? []).includes(period)) {
                            // Возврат в потоки: текущий период не влезает в текущий ТФ
                            // (напр. 1М на месячном ТФ) → поднимаем до минимально
                            // допустимого, иначе на графике один столбец.
                            setPeriod((FLOW_MIN_PERIODS[flowTimeframe] ?? ['all'])[0]);
                        }
                    }}
                    onLockedClick={() => {
                        // «Притоки-Оттоки» заперты тарифом (реальный блокер — лимит
                        // истории: потокам нужен ≥1Г, а free=180д) → upgrade-промпт.
                        const tier = fundsAccess.requiredTierFor({ period: '1y' }) ?? 'basic';
                        showUpgrade({ tier, featureName: 'притоки-оттоки фондов', indicator: 'funds_money' });
                    }}
                    trailing={
                        <HelpTooltip
                            sections={[
                                { heading: 'Притоки-Оттоки', body: 'Показывают чистый приток средств: буквально, сколько людей внесли деньги в тот или иной инструмент. Это общее настроение рынка.' },
                                { heading: 'СЧА', body: 'Ознакомительный режим: видно, сколько всего средств под управлением и как менялась эта сумма. Но динамика СЧА зависит ещё и от доходности фонда, поэтому не показывает, сколько денег люди действительно внесли.' },
                            ]}
                            size={18}
                        />
                    }
                />
                </div>

                {/* Таймфрейм для flows — плитки (как на OI) */}
                {viewMode === 'flows' && (
                    <div data-tour="funds-flow-timeframe" style={{ order: 2 }}>
                    <SegmentedControl<FlowTimeframe>
                        options={[
                            {
                                key: '1d',
                                label: '1д',
                                // Free → только 1w/1m, дневной заблокирован
                                locked: !fundsAccess.isLoading && !fundsAccess.canUseTimeframe('1d'),
                            },
                            { key: '1w', label: '1н' },
                            { key: '1m', label: '1м' },
                        ]}
                        value={flowTimeframe}
                        onChange={setFlowTimeframe}
                        onLockedClick={() => {
                            const tier = fundsAccess.requiredTierFor({ timeframe: '1d' });
                            if (tier) {
                                showUpgrade({
                                    tier,
                                    featureName: 'дневной таймфрейм',
                                    indicator: 'funds_money',
                                });
                            }
                        }}
                    />
                    </div>
                )}

                {/* Действия (Слои/Скриншот/CSV) свёрнуты в kebab «⋮» в углу графика
                    (паттерн OI). Через portal монтируется в обёртку графика
                    (containerRef=chartAnchorRef). Слои зависят от режима. */}
                <ChartActionsMenu containerRef={chartAnchorRef} tourId="funds-export">
                {viewMode === 'aum' && (
                <LayersButton
                    tourId="funds-layers"
                    layers={[{ key: 'index', label: 'Индекс', hint: `Линия ${currentCategory?.index ?? 'индекса'} на левой оси`, checked: showIndex, onChange: setShowIndex }]}
                />
                )}
                <CsvExportButton
                    indicator="funds_money"
                    config={() => {
                      const periodDays: Record<string, number> = {
                        '1m': 30, '3m': 90, '6m': 180, '1y': 365,
                        '2y': 730, '3y': 1095, 'all': 7000,
                      };
                      const visibleTickers = data?.funds
                        ?.filter(f => !f.tier_locked && !hiddenFunds.has(f.fund_id))
                        ?.map(f => f.ticker) ?? [];
                      return {
                        indicator: 'funds_money',
                        title: 'Экспорт: Деньги в фондах',
                        layers: [{
                          id: 'nav',
                          label: 'История СЧА фондов',
                          description: 'Daily NAV per fund по выбранной категории',
                          defaultSelected: true,
                        }],
                        // Unified порядок: фонды (актив) → категории (mode) → период.
                        selectors: [
                          {
                            kind: 'instrument-picker',
                            id: 'funds',
                            label: 'Фонды (опционально)',
                            default: [],
                            source: 'funds',
                            pickerTitle: 'Выберите фонды',
                            hint: 'Пусто = все фонды выбранных категорий. Иначе — только эти.',
                          },
                          {
                            kind: 'multiselect',
                            id: 'categories',
                            label: 'Категории',
                            default: [category],
                            hint: 'Несколько → ZIP с CSV per категория',
                            options: CATEGORIES.map(c => ({ value: c.key, label: c.name })),
                          },
                          {
                            kind: 'period',
                            id: 'period',
                            label: 'Период',
                            default: { type: 'preset', value: period },
                            presets: [
                              { value: '1m', label: '1М', days: 30 },
                              { value: '1y', label: '1Г', days: 365 },
                              { value: '3y', label: '3Г', days: 1095 },
                              { value: 'all', label: 'Всё', days: 7000 },
                            ],
                          },
                        ],
                        params: [],
                        buildUrl: (_layers, vals) => {
                          const cats = (vals.categories as string[] ?? [category]).join(',');
                          const periodParam = periodToQuery(vals.period, periodDays[period] ?? 365);
                          // Picker override — если юзер выбрал → этими фондами; иначе UI hidden funds.
                          const pickedFunds = (vals.funds as string[] ?? []);
                          const effectiveFundsList = pickedFunds.length > 0 ? pickedFunds : visibleTickers;
                          const fundsParam = effectiveFundsList.length > 0
                            ? `&funds=${encodeURIComponent(effectiveFundsList.join(','))}`
                            : '';
                          return `/api/export/funds-money.csv?category=${cats}&${periodParam}${fundsParam}`;
                        },
                        buildFilename: () => `funds_${Date.now()}.zip`,
                      };
                    }}
                />
                {/* Сигналы по фондам — рабочая кнопка-колокол. Только в режиме
                    притоков-оттоков. Открывает CreateFundAlertModal БЕЗ привязки к
                    текущей категории — фонды/категории выбираются внутри (дефолт —
                    все фонды).
                    Tier-гейт как у OI-колокола: quota=0 (Free/гость) → замочек +
                    upgrade-промпт; иначе — модалка создания. Стиль зеркалит
                    AlertBellButton (paper pill, 2px border), с подписью «Сигнал»
                    инлайн (в kebab-стеке бейдж-overlay задевал бы соседей). */}
                {viewMode === 'flows' && (
                    <span
                        data-export-ignore="true"
                        className="relative inline-flex"
                        style={{ flex: '0 0 auto' }}
                    >
                        <button
                            type="button"
                            data-export-ignore="true"
                            onClick={() => {
                                if (alertsLocked) {
                                    showUpgrade({ tier: 'basic', featureName: 'Сигналы по фондам', indicator: 'alerts' });
                                    return;
                                }
                                setFundAlertOpen(true);
                            }}
                            className="editorial-press rounded-full inline-flex items-center justify-center"
                            style={{
                                width: 44,
                                height: 44,
                                backgroundColor: 'var(--bg-secondary)',
                                border: '2px solid var(--text-primary)',
                                color: 'var(--text-primary)',
                                opacity: alertsLocked ? 0.78 : 1,
                            }}
                            aria-label={alertsLocked ? 'Сигналы по фондам — доступно на тарифе Basic и Pro' : 'Создать сигнал по фондам'}
                            title={alertsLocked
                                ? 'Сигналы в мессенджере — на тарифе Basic и Pro. Нажмите, чтобы улучшить.'
                                : 'Создать сигнал по аномальному потоку фондов'}
                        >
                            <AlarmClock size={18} />
                        </button>
                        {alertsLocked && (
                            <span
                                aria-hidden="true"
                                data-export-ignore="true"
                                style={{
                                    position: 'absolute', right: -3, bottom: -3,
                                    width: 18, height: 18, borderRadius: '50%',
                                    background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
                                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                                    color: 'var(--text-primary)', pointerEvents: 'none',
                                }}
                            >
                                <Lock size={10} strokeWidth={2.6} />
                            </span>
                        )}
                    </span>
                )}
                <ChartCaptureButton
                    getTargetElement={() => chartAnchorRef.current}
                    filename={`frame-funds-${category}-${viewMode}-${period}`}
                    metadata={{
                        // На скриншоте главный заголовок — название индикатора
                        // «Деньги в фондах», а не категория. asset не задаём (иначе он
                        // станет primary-заголовком); категория уходит в subtitle-чипы.
                        title: 'Деньги в фондах',
                        details: [
                            currentCategory?.name ?? category,
                            viewMode === 'aum' ? 'СЧА' : 'Притоки-Оттоки',
                            PERIOD_LABELS[period] ?? period,
                            viewMode === 'flows' ? (flowTimeframe === '1d' ? 'День' : flowTimeframe === '1w' ? 'Неделя' : 'Месяц') : null,
                        ].filter(Boolean) as string[],
                    }}
                    getExportStyles={(): Record<string, string> => {
                        // Только для flows mode. Chart имеет only-right axis,
                        // справа padding-strip шириной --chart-pad-right-single
                        // содержит Y-numbers с внутренним отступом left:12px.
                        // Visual distance container-right → начало labels = pad-right - 12.
                        // Mirror this как left padding в export → симметричное empty
                        // пространство по обе стороны chart-area, без injecting empty width.
                        if (viewMode !== 'flows') return {};
                        return { '--chart-pad-left': 'calc(var(--chart-pad-right-single) - 12px)' };
                    }}
                />
                {/* Тип графика применим только в режиме СЧА (линейный SimpleChart);
                    «Притоки-Оттоки» — гистограмма, там в модалке только палитра. */}
                <ChartSettings showType={viewMode === 'aum'} scopeLabels={{ primary: 'СЧА', secondary: 'Индекс' }} />
                </ChartActionsMenu>
            </div>

            {/* График — стабильная обёртка (ref+position:relative+--chart-height
                не пересоздаются при смене режима aum↔flows), иначе portal kebab'а
                терял бы host. */}
            <div ref={chartAnchorRef} data-tour="funds-chart" style={{ position: 'relative', ['--chart-height' as string]: `${chartHeight}px` }}>
            {error ? (
                <div className="flex items-center justify-center" style={{ height: chartHeight }}>
                    <div className="text-theme-danger text-center">
                        <p className="text-lg font-medium">{error}</p>
                        <p className="text-sm text-theme-secondary mt-2">Попробуйте обновить страницу</p>
                    </div>
                </div>
            ) : viewMode === 'aum' ? (
                <div>
                    {/* Свопнутые оси (TradingView-style): СЧА — главная серия —
                        на ПРАВОЙ оси (secondaryData), индекс — на ЛЕВОЙ (data).
                        Цвета закреплены за сериями через swap primaryColor/
                        secondaryColor; reverseLegend держит СЧА первой в легенде.
                        indexData ?? [] — на этапе загрузки indexData undefined, а
                        data обязателен (иначе краш на data.length). */}
                    <SimpleChart
                        data={indexData ?? []}
                        secondaryData={aggregatedData.chartData}
                        height={chartHeight}
                        primaryColor={INDEX_COLOR}
                        secondaryColor={NAV_COLOR}
                        showPrimary={showIndex}
                        showSecondary={true}
                        reverseLegend={true}
                        niceTicks={true}
                        niceTicksSecondary={true}
                        gridAxis="secondary"
                        formatValue={(v) => v.toFixed(2)}
                        formatPrimaryAxis={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
                        formatSecondaryValue={formatNav}
                        formatSecondaryAxis={formatNav}
                        primaryLabel={currentCategory?.index || 'Индекс'}
                        secondaryLabel={`СЧА фондов ${currentCategory?.genitive ?? ''} (млрд ₽)`}
                        loading={loading}
                        showValueHeader={false}
                        legendPosition="top"
                        showDownloadButton={false}
                        showNavigator={true}
                        chartPadding={{ left: 120 }}
                        hideTime={true}
                    />
                </div>
            ) : (
            <div>
            <FlowsHistogram
                        flowsData={flowsData}
                        noFundsSelected={noFundsSelected}
                        animatedBarsIn={animatedBarsIn}
                        animatedBarsOut={animatedBarsOut}
                        flowNavRange={flowNavRange}
                        hoveredFlowIndex={hoveredFlowIndex}
                        flowTitle={flowTitle}
                        loading={flowsLoading}
                        flowContainerRef={flowContainerRef}
                        flowChartRef={flowChartRef}
                        flowTooltipPos={flowTooltipPos}
                        onMouseMove={handleFlowMouseMove}
                        onMouseLeave={handleFlowMouseLeave}
                        onSetFlowNavRange={setFlowNavRange}
                    />
            </div>
            )}
            </div>{/* /funds-chart — стабильная обёртка */}

            </div>{/* /editorial-frame */}

            </div>{/* /tabbed-card */}

            {/* Модалка-виджет выбора фондов — открывается таблеткой в контролах.
                Масштаб/шелл как у InstrumentSearchModal на ОИ: top-anchored,
                max-w-xl, max-h-90vh, 2px border + hard shadow, скролл внутри. */}
            {fundPickerOpen && (
                <FundPickerModal
                    data={data}
                    hiddenFunds={hiddenFunds}
                    onSetHiddenFunds={setHiddenFunds}
                    onToggleFundVisibility={toggleFundVisibility}
                    onClose={() => setFundPickerOpen(false)}
                    categoryGenitive={currentCategory?.genitive}
                    maxDate={fundsMaxDate || undefined}
                    hasStale={fundsHasStale}
                    aggregatedData={aggregatedData}
                    collapsedSubcats={collapsedSubcats}
                    onSetCollapsedSubcats={setCollapsedSubcats}
                    navSortDir={navSortDir}
                    onSetNavSortDir={setNavSortDir}
                />
            )}

            {/* Карточки фонда здесь нет: список фондов — это выбор того, что
                показывать на графике, клик по строке переключает галочку.
                Путь к карточке фонда не лежит через «Деньги в фондах». */}

            {/* Конструктор сигнала по фондам — категория/фонды выбираются ВНУТРИ
                модалки (дефолт — все фонды), без привязки к текущей категории. */}
            {fundAlertOpen && (
                <CreateFundAlertModal
                    onClose={() => setFundAlertOpen(false)}
                />
            )}

            <OnboardingTour
                steps={fundsMoneyTourSteps}
                open={tour.open}
                onClose={tour.close}
            />
        </div>
    );
}
