/**
 * CompanyFlowsTab — раздел «Потоки по компании».
 *
 * Выбор бумаги (таблетка-поиск в стиле «Сезонности») → её помесячная история
 * по фондам, что её держат. ОДИН выпадающий список режимов (Dropdown; с
 * четвёртым пунктом ряд-SegmentedControl перестал влезать), у всех общая
 * ось месяцев и линия цены сверху — меняется только нижний слой:
 *  - «Сделки» — CompanyFlowsPriceMap: недельная линия цены акции с кругляшами
 *    месячных нетто-сделок (площадь ∝ |нетто|) — видно, на каких уровнях цены
 *    фонды покупали и продавали. Цена — /price-weekly по тикеру
 *    (resolveFundTicker); нет тикера или истории → empty-state внутри чарта.
 *  - «Позиция» — CompanyShareChart: бары абсолютной позиции фондов в ₽
 *    (Σ СЧА×доля из /company-weights).
 *  - «% в обращении» — тот же чарт, бары в % от free-float капы компании
 *    (ffcap). Пункт СКРЫТ, если ffcap пуст: облигация/ОФЗ или бумага вне
 *    индекса широкого рынка MOEXBMI.
 *  - «Навес» — тот же чарт, бары в ДНЯХ: позиция фондов / медианный дневной
 *    оборот торгов за скользящие три месяца, только будние сессии
 *    (med_turnover из /price-weekly; окно трёхмесячное, потому что месячная
 *    медиана шумит на порядок сильнее числителя, а выходные торги МосБиржи
 *    исключены, так как ПИФы в них не участвуют — см. price_weekly). Пункт
 *    скрыт, если оборота нет (не акция / нет истории в candles).
 * Нет цены (облигация/ОФЗ) → гистограмма во всю высоту.
 *
 * Прежний двухуровневый выбор («Сделки/Позиция» + субтумблер веса) сплющен в
 * один ряд, вес «По доле» (среднее долей по фондам) убран — решение владельца
 * 2026-08-10. Миграция сохранённого выбора — см. mode ниже.
 *
 * Цвет фонда: UK_LOGOS[String(uk_id)]?.bg, иначе DONUT_COLORS[idx % len].
 * Значения приходят в ₽ → чарты переводят в млн (÷1e6).
 *
 * Контрактные импорты из services/api: listFundTradeAssets, getCompanyFlows,
 * типы FundTradeAsset, CompanyFlowsResponse. Их добавляет бэкенд-агент по
 * общему контракту — здесь импортируем строго по контрактным именам.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { CalendarRange, CandlestickChart, ChevronDown, Coins, Hourglass, Percent, TrendingUp } from 'lucide-react';
import { UK_LOGOS, DONUT_COLORS, resolveFundTicker, fundAssetName, fundAssetColor, isOfzBond } from '../../config/fundConfig';
import {
    listFundTradeAssets,
    listFundsWithHistory,
    getCompanyFlows,
    getCompanyPriceWeekly,
    getCompanyWeights,
    type FundTradeAsset,
    type CompanyFlowsResponse,
    type CompanyPriceWeeklyResponse,
    type CompanyWeightsResponse,
    type FundWithHistory,
} from '../../services/api';
import InstrumentIcon from '../InstrumentIcon';
import Skeleton from '../Skeleton';
import { type CompanyFlowsSeries } from './CompanyFlowsHistogram';
import CompanyFlowsPriceMap from './CompanyFlowsPriceMap';
import CompanyShareChart, { type CompanyShareFundSeries, type ShareMode } from './CompanyShareChart';
import { useFitToViewport } from '../../hooks/useFitToViewport';
import AssetPickerModal from './AssetPickerModal';
import PortfolioFundPicker, { indexFundTickers } from './PortfolioFundPicker';
import SegmentedControl from '../SegmentedControl';
import Dropdown from '../Dropdown';
import ChartActionsMenu from '../ChartActionsMenu';
import ChartCaptureButton from '../export/ChartCaptureButton';
import ChartSettings from '../chart/ChartSettings';
import { usePersistedState } from '../../hooks/usePersistedState';
// UI-кит панелей (тулбар окна). Лежит в pages/embed, но это именно набор
// примитивов, а не страница: сайтовые SegmentedControl/таблетки в тулбар окна
// не влезают (та же причина, по которой скринер не переиспользует OiScreenerTable).
import { Dropdown as EmbDropdown, PillGroup } from '../../pages/embed/EmbedToolbar';

type Metric = 'amount' | 'weight';

// Период графика — окно последних N месяцев. «Всё» = вся доступная история
// (левый пустой хвост всё равно обрезает сам чарт).
type Period = '1y' | '3y' | 'all';
const PERIOD_LABELS: Record<Period, string> = { '1y': '1 год', '3y': '3 года', 'all': 'Всё' };
const CF_PERIODS: Period[] = ['1y', '3y', 'all'];
const PERIOD_MONTHS: Record<Period, number | null> = { '1y': 12, '3y': 36, 'all': null };

// Режим отображения — с четвёртым пунктом («Навес») ряд перестал влезать в
// строку и на сайте стал ВЫПАДАЮЩИМ СПИСКОМ (решение владельца 2026-08-13):
// сделки на цене / позиция в ₽ / % от free-float капы / навес в днях оборота.
// 'rub', 'cap' и 'overhang' — это ShareMode чарта (см. ниже).
type ChartMode = 'map' | ShareMode;
const MODE_LABELS: Record<ChartMode, string> = {
    map: 'Сделки',
    rub: 'Позиция',
    cap: '% в обращении',
    overhang: 'Навес',
};
const CF_MODES: ChartMode[] = ['map', 'rub', 'cap', 'overhang'];
// Подсказки режимов — «?» у пунктов выпадающего списка (заменили прежний общий
// HelpTooltip у SegmentedControl: у списка подсказка живёт при каждом пункте).
const MODE_HELP: Record<ChartMode, string> = {
    map: 'Кругляши покупок и продаж на линии цены: видно, на каких уровнях фонды набирали позицию, а на каких выходили. Размер кругляша — объём чистой сделки за месяц.',
    rub: 'Сколько рублей выбранные фонды держат в бумаге: доля в портфеле × СЧА фонда, суммой по фондам.',
    cap: 'Позиция фондов в процентах от стоимости акций компании в свободном обращении (free-float, данные МосБиржи). Показывает, какая часть торгуемых акций лежит в этих фондах.',
    overhang: 'Позиция выбранных фондов, делённая на медианный дневной оборот торгов за последние три месяца (будние сессии — по выходным фонды не торгуют). Показывает, за сколько дней торгов фонды могли бы выйти из бумаги, забирая весь оборот: чем больше дней, тем труднее им двигаться без влияния на цену.',
};
// Ширина места под контролы в тулбаре окна, ниже которой подписи не влезают.
const TOOLBAR_COMPACT_PX = 560;
// Иконки режимов для compact-тулбара окна (порядок и смысл 1-в-1 с сайтом:
// сделки на цене / позиция в ₽ / доля во free-float / навес в днях).
const MODE_ICONS: Record<ChartMode, ReactNode> = {
    map: <CandlestickChart size={14} />,
    rub: <Coins size={14} />,
    cap: <Percent size={14} />,
    overhang: <Hourglass size={14} />,
};

// Ключ прежней схемы (субтумблер веса). Читаем один раз при миграции старого
// значения mode='share' и чистим, чтобы не оставлять мусор в localStorage.
const LEGACY_SHAREMODE_KEY = 'frame:companyflows:sharemode';

// ITEM 4b/5 — логотип бумаги: резолвим по ISIN в каноничный тикер (как в
// Сезонности) и рендерим через InstrumentIcon (STOCK_LOGO_OVERRIDE → стикерпак →
// /logos/<тикер>.png). ОФЗ → герб Минфина (raw_152). Иначе (корпоблигация/
// денежный рынок без тикера) → цветная точка.
function AssetMark({ name, isin, size = 22 }: { name: string; isin?: string | null; size?: number }) {
    const ticker = resolveFundTicker(name, isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={size} rounded="full" />;
    if (isOfzBond(name)) return <InstrumentIcon sectype="RB" size={size} rounded="full" />;
    const dot = fundAssetColor(name, isin) ?? 'var(--text-muted)';
    return (
        <span
            style={{
                width: size,
                height: size,
                borderRadius: '50%',
                background: dot,
                flexShrink: 0,
                display: 'inline-block',
            }}
        />
    );
}

function pluralFunds(n: number): string {
    const mod10 = n % 10;
    const mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return 'фонд';
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'фонда';
    return 'фондов';
}

// ФИЛЬТР ФОНДОВ ХРАНИТ СНЯТЫЕ, А НЕ ВЫБРАННЫЕ.
//
// Держатели у каждой бумаги свои, а настройка фильтра одна на все бумаги, и
// раньше хранился набор ВЫБРАННЫХ тикеров. Из-за этого выбор перетекал между
// бумагами и деградировал: выбрал 5 фондов на Сбере → перешёл на Ренессанс, где
// из них бумагу держат двое → тронул пикер → в память попали только эти двое →
// вернулся на Сбер и видишь двоих вместо пяти. Плюс когда пересечение выходило
// пустым, фильтр молча показывал всех.
//
// Теперь помним только выключенные фонды. На любой бумаге видны все её
// держатели, кроме выключенных: набор держателей больше не может «сузить»
// память, а «этот фонд мне не интересен» продолжает действовать везде.
//
// Дефолт (пользователь ещё ничего не трогал) — выключены индексные фонды: их
// сделки это ребалансировка вслед за индексом, а не решения управляющих.
// Считаем по ГЛОБАЛЬНОМУ списку фондов, а не по держателям текущей бумаги,
// иначе индексные, не державшие первую открытую бумагу, всплыли бы на второй.
const FUNDS_OFF_KEY = 'frame:companyflows:funds-off';
// Ключи прежней схемы (выбранные + флаг «трогал пикер») — чистим при первом
// заходе, чтобы не оставлять в localStorage мусор, который уже никто не читает.
const LEGACY_FUNDS_KEYS = ['frame:companyflows:funds', 'frame:companyflows:funds-touched'];

// ─────────────────────────────────────────────────────────────────────────────
// Главный компонент
// ─────────────────────────────────────────────────────────────────────────────
// ITEM 2 (cross-tab) — предвыбор бумаги из movers («Сделки фондов»).
export interface CompanyFlowsTabProps {
    /** Если задан — выбрать эту бумагу (по isin || asset_name) в селекторе. */
    presetAsset?: { asset_name: string; isin: string | null } | null;
    /** Дёрнуть после применения presetAsset (родитель сбросит state). */
    onPresetConsumed?: () => void;
    /** Kebab «⋮» в углу графика (Скриншот + Настройки), как в «Деньги в фондах».
     *  Только для десктопа: у мобилки свой тулбар-шит, у эмбеда — EmbedToolbar,
     *  и ChartCaptureButton тянет AnalyticsContext, которого в эмбеде может не быть. */
    showChartActions?: boolean;
    /** Панельный режим (окно расширения / песочницы): сайтовый ряд контролов не
     *  рисуется на месте, а УЕЗЖАЕТ ПОРТАЛОМ в тулбар окна (controlsTarget) в
     *  компактных примитивах; сам таб отдаёт только график во всю площадь.
     *  Данные, фильтры и методология — те же, второй реализации нет. */
    embedded?: boolean;
    /** Куда портировать контролы в панельном режиме (узел внутри тулбара окна). */
    controlsTarget?: HTMLElement | null;
}

export default function CompanyFlowsTab({
    presetAsset, onPresetConsumed, showChartActions = false, embedded = false, controlsTarget = null,
}: CompanyFlowsTabProps = {}) {
    // Высота графика «под экран» — anchor на обёртке чарта (как в «Деньги в фондах»).
    // min = 475: карточка графика = chartHeight + ~39px (padding + легенда/навигатор),
    // то есть floor даёт блок ~514px — ровно фиксированный размер блока в «Силе рынка»
    // (--strength-chart-top-height 300 + --strength-chart-bottom-height 150 + chrome).
    // Ниже этого блок не ужимается даже на низком окне.
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const fitHeight = useFitToViewport(chartAnchorRef, { min: 475, max: 720, bottomBuffer: 64 });
    // В ОКНЕ высота берётся у контейнера панели, а не у вьюпорта: useFitToViewport
    // меряет окно браузера, а панель — маленький прямоугольник внутри него, и
    // график вылезал за нижний край (проверено вживую: ось дат уходила под обрез).
    // Узкая панель → подписи контролов схлопываются в иконки. Меряем НЕ сам ряд
    // (он content-sized и всегда «влезает» в себя), а место, которое даёт тулбар
    // окна: контейнер, куда ряд уехал порталом.
    const [tbCompact, setTbCompact] = useState(false);
    useEffect(() => {
        const host = controlsTarget?.parentElement;
        if (!embedded || !host || typeof ResizeObserver === 'undefined') return;
        const check = () => setTbCompact(host.clientWidth < TOOLBAR_COMPACT_PX);
        check();
        const ro = new ResizeObserver(check);
        ro.observe(host);
        return () => ro.disconnect();
    }, [embedded, controlsTarget]);
    const [boxH, setBoxH] = useState(0);
    const roRef = useRef<ResizeObserver | null>(null);
    // Callback-ref, а НЕ useEffect: на первом рендере компонент отдаёт скелет
    // загрузки бумаг — якоря графика ещё нет, и однократный эффект уходил ни с
    // чем, после чего панель навсегда оставалась с полом высоты 160px. Ref
    // срабатывает ровно тогда, когда узел появляется (и исчезает).
    const setChartAnchor = useCallback((el: HTMLDivElement | null) => {
        chartAnchorRef.current = el;
        roRef.current?.disconnect();
        roRef.current = null;
        if (!el || !embedded || typeof ResizeObserver === 'undefined') return;
        const measure = () => setBoxH(el.clientHeight);
        measure();
        const ro = new ResizeObserver(measure);
        ro.observe(el);
        roRef.current = ro;
    }, [embedded]);
    useEffect(() => () => roRef.current?.disconnect(), []);

    // PAD_TOP — воздух под легенду графика: карточку с её padding'ом в панели
    // убрали (bare), и подпись серии упиралась в тулбар.
    const PAD_TOP = 8;
    // --chart-height задаёт только ПОЛЕ графика; сверху компонент рисует легенду,
    // снизу — подписи дат и навигатор. Без этого запаса низ уезжал за край окна
    // (замерено вживую: контент был выше контейнера примерно на эту величину).
    const CHART_CHROME = 56;
    const chartHeight = embedded ? Math.max(160, boxH - PAD_TOP - CHART_CHROME) : fitHeight;
    const [assets, setAssets] = useState<FundTradeAsset[]>([]);
    const [assetsLoading, setAssetsLoading] = useState(true);
    const [assetsError, setAssetsError] = useState<string | null>(null);
    // Карточки фондов (СЧА, доходность, подкатегория) — их показывает пикер
    // фондов, тот же, что в «Общем портфеле». Универсум совпадает: и /funds, и
    // /company-flows отдают только whitelist-фонды акций. Ошибку глотаем: без
    // метаданных пикер всё равно соберётся из flows.funds (см. pickerFunds).
    const [fundsMeta, setFundsMeta] = useState<FundWithHistory[]>([]);

    // Выбранная бумага переживает перезагрузку и уход со страницы: ключ = mkey
    // из /assets (канонический ISIN, стабилен между сессиями). Раньше при каждом
    // заходе подставлялась первая бумага списка, и выбор терялся.
    const [selectedKey, setSelectedKey] = usePersistedState<string | null>('frame:companyflows:asset', null);
    const [flows, setFlows] = useState<CompanyFlowsResponse | null>(null);
    const [flowsLoading, setFlowsLoading] = useState(false);
    const [flowsError, setFlowsError] = useState<string | null>(null);

    // ITEM 3 — открыта ли модалка выбора бумаги.
    const [pickerOpen, setPickerOpen] = useState(false);

    // ВЫКЛЮЧЕННЫЕ фонды (ключ = ticker), общие на все бумаги. null = пользователь
    // ещё ничего не трогал → действует дефолт «выключены индексные» (см. offFunds).
    // Пустой массив — это уже осознанный выбор «показывать всех», и он переживает
    // перезагрузку, поэтому отличать его от null обязательно.
    const [fundsOff, setFundsOff] = usePersistedState<string[] | null>(FUNDS_OFF_KEY, null);

    // metric toggle — default ₽ (amount).
    const [metric] = useState<Metric>('amount');

    // Период графика (окно последних N месяцев). Персист между сессиями,
    // дефолт «Всё» — сохраняем прежнее поведение (показывали всю историю).
    const [period, setPeriod] = usePersistedState<Period>('frame:companyflows:period', 'all');

    // Режим отображения. Персист между сессиями. Значения прежних схем чиним
    // на месте, иначе восстановился бы несуществующий режим:
    //   'share' (двухуровневая схема) → бывший субтумблер веса, причём
    //            удалённый вес 'share' («По доле») → 'rub';
    //   'bars' и прочий мусор → 'map'.
    const [modeRaw, setMode] = usePersistedState<ChartMode>('frame:companyflows:mode', 'map');
    const mode: ChartMode = CF_MODES.includes(modeRaw) ? modeRaw : 'map';
    useEffect(() => {
        if (CF_MODES.includes(modeRaw)) return;
        let next: ChartMode = 'map';
        if (modeRaw === ('share' as ChartMode)) {
            let legacy: string | null = null;
            try {
                legacy = window.localStorage.getItem(LEGACY_SHAREMODE_KEY);
            } catch { /* приватный режим */ }
            next = legacy?.includes('cap') ? 'cap' : 'rub';
        }
        setMode(next);
        try {
            window.localStorage.removeItem(LEGACY_SHAREMODE_KEY);
        } catch { /* приватный режим */ }
    }, [modeRaw, setMode]);

    // История позиции (/company-weights) — грузим при ЛЮБОМ режиме, включая
    // «Сделки»: из ответа берётся ffcap, а он решает, показывать ли вообще
    // пункт «% в обращении». При ленивой загрузке ряд режимов был бы то из
    // двух пунктов, то из трёх — прыгал бы при первом переключении.
    // Кэш по ключу бумаги: смена режимов туда-сюда не перезапрашивает.
    const [weightsData, setWeightsData] = useState<CompanyWeightsResponse | null>(null);
    const [weightsKey, setWeightsKey] = useState<string | null>(null);
    const [weightsLoading, setWeightsLoading] = useState(false);
    const [weightsError, setWeightsError] = useState<string | null>(null);

    // Недельная цена для «Карты сделок» — грузим лениво, только в режиме map.
    // priceError: NO_PRICE_HISTORY (404 / нет тикера) → empty-state в чарте,
    // прочее → красная плашка (как flowsError).
    const [price, setPrice] = useState<CompanyPriceWeeklyResponse | null>(null);
    const [priceLoading, setPriceLoading] = useState(false);
    const [priceError, setPriceError] = useState<string | null>(null);

    // Загрузка списка бумаг → восстановить последнюю просмотренную, если нет
    // pending-preset (presetAsset выбирается отдельным эффектом и имеет приоритет).
    // Сохранённой бумаги может уже не быть в списке (вышла из составов фондов или
    // отсеклась фильтром релевантности) — тогда падаем на первую, самую крупную.
    useEffect(() => {
        let cancelled = false;
        setAssetsLoading(true);
        listFundTradeAssets()
            .then(resp => {
                if (cancelled) return;
                setAssets(resp.assets);
                if (resp.assets.length > 0 && !presetAsset) {
                    setSelectedKey(prev =>
                        prev && resp.assets.some(a => a.key === prev) ? prev : resp.assets[0].key);
                }
                setAssetsError(null);
            })
            .catch(err => {
                if (cancelled) return;
                setAssetsError(err instanceof Error ? err.message : 'Не удалось загрузить список бумаг');
            })
            .finally(() => {
                if (!cancelled) setAssetsLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, []);

    // Список фондов с метаданными — один раз, независимо от выбранной бумаги.
    useEffect(() => {
        let cancelled = false;
        listFundsWithHistory()
            .then(r => { if (!cancelled) setFundsMeta(r.funds); })
            .catch(() => { /* пикер переживёт без СЧА/доходности */ });
        return () => {
            cancelled = true;
        };
    }, []);

    const selectedAsset = useMemo(
        () => assets.find(a => a.key === selectedKey) ?? null,
        [assets, selectedKey],
    );

    // Тикер выбранной бумаги для подписи в таблетке (как в «Сезонности», где
    // под именем — тикер). Резолвим по ISIN/имени; нет тикера (облигация/ОФЗ) →
    // undefined, и подпись падает на счётчик фондов.
    const selectedTicker = useMemo(
        () => (selectedAsset ? resolveFundTicker(selectedAsset.asset_name, selectedAsset.isin) : undefined),
        [selectedAsset],
    );

    // ITEM 2 (cross-tab) — применить presetAsset: выбрать бумагу по isin || name.
    // Ждём загрузки assets, затем матчим и сразу «потребляем» preset.
    useEffect(() => {
        if (!presetAsset || assets.length === 0) return;
        const wantIsin = presetAsset.isin;
        const wantName = presetAsset.asset_name;
        const match =
            (wantIsin ? assets.find(a => a.isin === wantIsin) : undefined) ??
            assets.find(a => a.asset_name === wantName);
        if (match) setSelectedKey(match.key);
        onPresetConsumed?.();
    }, [presetAsset, assets, onPresetConsumed]);

    // Загрузка потоков при смене выбранной бумаги / метрики.
    useEffect(() => {
        if (!selectedAsset) {
            setFlows(null);
            return;
        }
        let cancelled = false;
        setFlowsLoading(true);
        getCompanyFlows({
            isin: selectedAsset.isin ?? undefined,
            assetName: selectedAsset.isin ? undefined : selectedAsset.asset_name,
            metric,
        })
            .then(resp => {
                if (cancelled) return;
                setFlows(resp);
                setFlowsError(null);
            })
            .catch(err => {
                if (cancelled) return;
                setFlowsError(err instanceof Error ? err.message : 'Не удалось загрузить потоки');
                setFlows(null);
            })
            .finally(() => {
                if (!cancelled) setFlowsLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [selectedAsset, metric]);

    // Цена для «Карты сделок» и «Доли»: грузим при входе в режим / смене бумаги.
    // Кэш — сам price (тикер совпал → не перезапрашиваем). Нет тикера (облигация,
    // ОФЗ, денежный рынок) → сразу «нет истории», без похода на бэкенд.
    useEffect(() => {
        if (!selectedTicker) {
            setPrice(null);
            setPriceError('NO_PRICE_HISTORY');
            return;
        }
        if (price?.ticker === selectedTicker) return;
        let cancelled = false;
        setPriceLoading(true);
        setPriceError(null);
        getCompanyPriceWeekly(selectedTicker)
            .then(resp => {
                if (cancelled) return;
                setPrice(resp);
            })
            .catch(err => {
                if (cancelled) return;
                setPrice(null);
                setPriceError(err instanceof Error ? err.message : 'Не удалось загрузить историю цены');
            })
            .finally(() => {
                if (!cancelled) setPriceLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [mode, selectedTicker, price]);

    // История позиции: грузим при смене бумаги, независимо от режима (ffcap
    // нужен ряду режимов, см. выше).
    useEffect(() => {
        if (!selectedAsset) return;
        if (weightsData && weightsKey === selectedAsset.key) return;
        let cancelled = false;
        setWeightsLoading(true);
        setWeightsError(null);
        getCompanyWeights({
            isin: selectedAsset.isin ?? undefined,
            assetName: selectedAsset.isin ? undefined : selectedAsset.asset_name,
        })
            .then(resp => {
                if (cancelled) return;
                setWeightsData(resp);
                setWeightsKey(selectedAsset.key);
            })
            .catch(err => {
                if (cancelled) return;
                setWeightsData(null);
                setWeightsKey(null);
                setWeightsError(err instanceof Error ? err.message : 'Не удалось загрузить историю доли');
            })
            .finally(() => {
                if (!cancelled) setWeightsLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [mode, selectedAsset, weightsData, weightsKey]);

    // Фонды для пикера — только держатели этой бумаги (flows.funds), но карточкой
    // из /funds: СЧА, доходность, подкатегория, дата последнего состава. Пока
    // метаданные не пришли (или не пришли вовсе) — минимальная карточка из flows,
    // чтобы фильтр работал с первой же секунды.
    const pickerFunds: FundWithHistory[] = useMemo(() => {
        if (!flows) return [];
        const meta = new Map(fundsMeta.map(f => [f.ticker, f]));
        return flows.funds.map((f, idx) => meta.get(f.ticker) ?? ({
            fund_id: -1 - idx,
            ticker: f.ticker,
            name: f.fund_name,
            uk: null,
            category: 'stocks',
            subcategory: null,
            last_snapshot_date: null,
            snapshot_count: 0,
            holdings_count: 0,
            uk_id: f.uk_id,
            nav_rub: null,
            returns: { m1: null, m3: null, m6: null, y1: null },
            top_holdings: [],
        } satisfies FundWithHistory));
    }, [flows, fundsMeta]);

    // Разовая уборка ключей прежней схемы фильтра (см. FUNDS_OFF_KEY выше).
    useEffect(() => {
        try {
            LEGACY_FUNDS_KEYS.forEach(k => window.localStorage.removeItem(k));
        } catch { /* приватный режим — не беда */ }
    }, []);

    // Индексные фонды по ГЛОБАЛЬНОМУ списку: они одинаковы для всех бумаг, и
    // выключать их надо везде, а не только там, где они оказались держателями.
    // Пока /funds не доехал — падаем на держателей текущей бумаги, чтобы фильтр
    // работал с первой секунды.
    const indexTickers = useMemo(
        () => indexFundTickers(fundsMeta.length > 0 ? fundsMeta : pickerFunds),
        [fundsMeta, pickerFunds],
    );

    // Выключенные фонды: сохранённый набор, а до первого касания — индексные.
    const offFunds = useMemo(
        () => new Set(fundsOff ?? indexTickers),
        [fundsOff, indexTickers],
    );

    // Что показываем по ЭТОЙ бумаге: все её держатели минус выключенные. Набор
    // держателей больше не сужает память — он только фильтрует её на показ.
    const effectiveFunds = useMemo(() => {
        if (!flows) return new Set<string>();
        return new Set(flows.funds.map(f => f.ticker).filter(t => !offFunds.has(t)));
    }, [flows, offFunds]);

    // Правка из пикера: пришёл набор ВЫБРАННЫХ держателей этой бумаги, переводим
    // его в дельту выключенных. Фонды, которые бумагу не держат, не трогаем —
    // они остаются в том состоянии, в котором их оставили на своих бумагах.
    //
    // Индексные — исключение: тумблер «Без индексных фондов» глобальный по смыслу
    // («не хочу видеть механическую ребалансировку»), поэтому его переключение
    // распространяем на все индексные фонды сразу, а не только на держателей.
    const handleFundsChange = useCallback((next: Set<string>) => {
        const holders = flows?.funds.map(f => f.ticker) ?? [];
        // КОНТРАКТ ПИКЕРА: когда отмечены все фонды, он отдаёт ПУСТОЙ набор
        // (у него семантика «пусто = все»), а «снять все» схлопывает в пул
        // доступных — пустым «ничего не выбрано» он не присылает никогда.
        // Понятый буквально, пустой набор выключал разом всех держателей, и
        // попытка вернуть все фонды давала «Не выбрано ни одного фонда».
        const chosen = next.size === 0 ? new Set(holders) : next;

        const nextOff = new Set(offFunds);
        holders.forEach(t => (chosen.has(t) ? nextOff.delete(t) : nextOff.add(t)));

        const idxHolders = holders.filter(t => indexTickers.includes(t));
        if (idxHolders.length > 0) {
            const indexShown = idxHolders.some(t => chosen.has(t));
            indexTickers.forEach(t => (indexShown ? nextOff.delete(t) : nextOff.add(t)));
        }
        setFundsOff([...nextOff]);
    }, [flows, offFunds, indexTickers, setFundsOff]);

    // ITEM 2 — фонды, попадающие в чарт: фильтр по выбранным тикерам (пусто = все).
    // Цвет фонда привязан к индексу в ПОЛНОМ списке (стабилен при фильтрации).
    // Бары рисуют ЧИСТЫЙ поток (сумму по этим сериям), тултип — разбивку по фондам.
    const fundSeries: CompanyFlowsSeries[] = useMemo(() => {
        if (!flows) return [];
        return flows.funds
            .map((f, idx) => {
                const ukColor = f.uk_id != null ? UK_LOGOS[String(f.uk_id)]?.bg : undefined;
                return {
                    ticker: f.ticker,
                    series: {
                        label: f.fund_name,
                        color: ukColor ?? DONUT_COLORS[idx % DONUT_COLORS.length],
                        values: f.values,
                    } as CompanyFlowsSeries,
                };
            })
            // effectiveFunds — уже готовый список к показу (держатели минус
            // выключенные), поэтому никаких «пусто = все»: пустой набор значит,
            // что выключены все держатели, и ниже сработает empty-state.
            .filter(x => effectiveFunds.has(x.ticker))
            .map(x => x.series);
    }, [flows, effectiveFunds]);

    // Все фонды сняты пользователем (но у бумаги фонды есть) — empty-state.
    const noFundsSelected = !!flows && flows.funds.length > 0 && fundSeries.length === 0;

    // Окно периода: индекс первого видимого месяца (последние N месяцев).
    // «Всё» или истории меньше окна → 0 (не режем). Значения серий выровнены с
    // flows.months по индексу, поэтому режем их тем же срезом.
    const periodStart = useMemo(() => {
        const total = flows?.months.length ?? 0;
        const n = PERIOD_MONTHS[period];
        return n == null || total <= n ? 0 : total - n;
    }, [flows, period]);

    const visibleMonths = useMemo(
        () => (flows?.months ?? []).slice(periodStart),
        [flows, periodStart],
    );
    const visibleSeries = useMemo<CompanyFlowsSeries[]>(
        () => fundSeries.map(s => ({ ...s, values: s.values.slice(periodStart) })),
        [fundSeries, periodStart],
    );

    // ── Режим «Доля»: производные от /company-weights ──
    // Серии фондов: цвет тем же правилом (UK_LOGOS → DONUT_COLORS по индексу в
    // ПОЛНОМ списке), фильтр — те же effectiveFunds, что у остальных режимов.
    const shareFundSeries = useMemo(() => {
        if (!weightsData) return [] as (CompanyShareFundSeries & { ticker: string })[];
        return weightsData.funds
            .map((f, idx) => {
                const ukColor = f.uk_id != null ? UK_LOGOS[String(f.uk_id)]?.bg : undefined;
                return {
                    ticker: f.ticker,
                    label: f.fund_name,
                    color: ukColor ?? DONUT_COLORS[idx % DONUT_COLORS.length],
                    weights: f.weights,
                    navs: f.navs,
                };
            })
            .filter(f => effectiveFunds.has(f.ticker));
    }, [weightsData, effectiveFunds]);

    const noFundsSelectedShare = !!weightsData && weightsData.funds.length > 0 && shareFundSeries.length === 0;

    // Окно периода для оси /company-weights (она своя, не равна flows.months).
    const sharePeriodStart = useMemo(() => {
        const total = weightsData?.months.length ?? 0;
        const n = PERIOD_MONTHS[period];
        return n == null || total <= n ? 0 : total - n;
    }, [weightsData, period]);
    const visibleShareMonths = useMemo(
        () => (weightsData?.months ?? []).slice(sharePeriodStart),
        [weightsData, sharePeriodStart],
    );
    const visibleShareFunds = useMemo<CompanyShareFundSeries[]>(
        () => shareFundSeries.map(f => ({
            label: f.label,
            color: f.color,
            weights: f.weights.slice(sharePeriodStart),
            navs: f.navs.slice(sharePeriodStart),
        })),
        [shareFundSeries, sharePeriodStart],
    );
    const visibleFfcap = useMemo(
        () => (weightsData?.ffcap ?? []).slice(sharePeriodStart),
        [weightsData, sharePeriodStart],
    );

    // Медианный дневной оборот по месяцам (/price-weekly) на оси месяцев
    // /company-weights — знаменатель режима «Навес». Ключ join'а — "YYYY-MM".
    const turnoverAll = useMemo<(number | null)[]>(() => {
        if (!weightsData) return [];
        const byMonth = new Map<string, number>();
        if (price && price.ticker === selectedTicker && price.turnover_months && price.med_turnover) {
            price.turnover_months.forEach((m, i) => {
                const v = price.med_turnover![i];
                if (v != null && v > 0) byMonth.set(m, v);
            });
        }
        return weightsData.months.map(m => byMonth.get(m) ?? null);
    }, [weightsData, price, selectedTicker]);
    const visibleTurnover = useMemo(
        () => turnoverAll.slice(sharePeriodStart),
        [turnoverAll, sharePeriodStart],
    );

    // «% в обращении» доступен, только если у бумаги вообще есть free-float
    // капа (акция из MOEXBMI). Облигации/ОФЗ/внеиндексные — пункт скрыт, а
    // персистнутый выбор 'cap' на такой бумаге тихо падает на «Позицию»
    // (не пишем в localStorage: вернёшься на акцию — режим восстановится).
    const capAvailable = useMemo(
        () => (weightsData?.ffcap ?? []).some(v => v != null),
        [weightsData],
    );
    // «Навес» требует оборота торгов (акция с историей в candles) — у
    // облигаций/ОФЗ пункт скрыт, персистнутый выбор падает на «Позицию» тем же
    // правилом, что и 'cap'. Пока цена грузится, пункт не прячем: иначе список
    // мигал бы при каждой смене бумаги (оборот приходит вместе с ценой).
    const overhangAvailable = useMemo(
        () => priceLoading || turnoverAll.some(v => v != null),
        [priceLoading, turnoverAll],
    );
    const effectiveMode: ChartMode =
        (mode === 'cap' && !capAvailable) || (mode === 'overhang' && !overhangAvailable) ? 'rub' : mode;
    const visibleModes = useMemo(
        () => CF_MODES.filter(m => (m !== 'cap' || capAvailable) && (m !== 'overhang' || overhangAvailable)),
        [capAvailable, overhangAvailable],
    );

    // Триггер сброса навигатора и морфа при смене бумаги, набора фондов,
    // периода, режима ИЛИ веса доли. Entrance-анимации (волна/reveal) играют
    // только на первом рендере с данными — схема как в OI.
    const animTrigger = `${selectedAsset?.key ?? ''}|${[...effectiveFunds].sort().join(',')}|${period}|${effectiveMode}`;

    // ── Рендер ──
    if (assetsLoading) {
        // Высота skeleton'а держит примерный размер финального контента (ряд
        // контролов + график chartHeight), чтобы контейнер не «прыгал», когда
        // список бумаг приходит с бэкенда.
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-4)' }}>
                <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 'var(--sp-2)' }}>
                    <Skeleton width={170} height={40} rounded="md" />
                    <Skeleton width={90} height={40} rounded="md" />
                    <Skeleton width={150} height={40} rounded="md" />
                </div>
                <Skeleton height={chartHeight} rounded="lg" />
            </div>
        );
    }

    if (assetsError) {
        return (
            <div
                className="rounded-2xl border border-theme"
                style={{ padding: 'var(--sp-5)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
            >
                {assetsError}
            </div>
        );
    }

    if (assets.length === 0) {
        return (
            <div
                className="flex flex-col items-center justify-center text-center rounded-2xl bg-theme-primary border border-theme"
                style={{ padding: 'var(--sp-10)', gap: 'var(--sp-3)' }}
            >
                <div
                    style={{
                        width: 56,
                        height: 56,
                        borderRadius: 12,
                        background: 'var(--accent)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        marginBottom: 'var(--sp-2)',
                    }}
                >
                    <TrendingUp size={28} strokeWidth={2.4} color="#FFFFFF" />
                </div>
                <div className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-lg)' }}>
                    Нет данных по бумагам
                </div>
                <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)', maxWidth: 360 }}>
                    Потоки по компаниям появятся, когда накопится история составов фондов.
                </div>
            </div>
        );
    }

    // Ряд контролов. На сайте рисуется тут же над графиком; в панели уезжает
    // порталом в тулбар окна (см. controlsRow ниже) и собирается из компактных
    // примитивов — сайтовые SegmentedControl в строку тулбара не помещаются.
    const controlsRow = (
            <div
                className={embedded ? 'emb-ctl-row' : undefined}
                style={embedded
                    ? { display: 'flex', alignItems: 'center', gap: 6 }
                    : { display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 'var(--sp-2)' }}
            >
                {/* Таблетка бумаги — widget-flat (icon + имя + тикер + ▾),
                    1-в-1 как селектор актива на «Сезонности» (под именем — тикер).
                    Открывает строковый поиск (AssetPickerModal — то же оформление,
                    что и InstrumentSearchModal). */}
                <button
                    type="button"
                    data-tour="ft-company-asset"
                    onClick={() => setPickerOpen(true)}
                    title={selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined}
                    className={embedded ? 'font-medium flex items-center' : 'widget-flat font-medium transition-colors flex items-center hover:opacity-90'}
                    style={embedded ? {
                        // Как AssetButton других панелей: рамка, компактные кегли,
                        // имя обрезается — тулбар окна живёт в одну строку.
                        color: 'var(--text-primary)',
                        fontSize: 11.5,
                        padding: '2px 6px',
                        gap: 5,
                        borderRadius: 6,
                        border: '1.5px solid var(--border-strong, var(--border-color, rgba(128,128,128,0.4)))',
                        background: 'transparent',
                        maxWidth: 170,
                        minWidth: 0,
                        cursor: 'pointer',
                    } : {
                        color: 'var(--text-primary)',
                        fontSize: 'var(--fs-sm)',
                        padding: 'var(--sp-2) var(--sp-4)',
                        gap: 'var(--sp-3)',
                        // Ширины 1-в-1 как у селектора актива на «Сезонности»:
                        // длинное имя обрезается ellipsis'ом и НЕ тянет кнопку
                        // дальше вправо (cap на maxWidth, как там).
                        minWidth: 'clamp(140px, 22vw, 170px)',
                        maxWidth: 220,
                        cursor: 'pointer',
                    }}
                >
                    {selectedAsset
                        ? <AssetMark name={selectedAsset.asset_name} isin={selectedAsset.isin} size={embedded ? 16 : 28} />
                        : <span style={{ width: embedded ? 16 : 28, height: embedded ? 16 : 28, flexShrink: 0 }} />}
                    <div className="flex-1 text-left" style={{ minWidth: 0 }}>
                        <div
                            className="font-medium"
                            style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                        >
                            {selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : 'Выберите бумагу'}
                        </div>
                        {selectedAsset && !embedded && (
                            <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>
                                {selectedTicker ?? `${selectedAsset.funds_count} ${pluralFunds(selectedAsset.funds_count)}`}
                            </div>
                        )}
                    </div>
                    <ChevronDown size={14} className="text-theme-secondary" style={{ flexShrink: 0 }} />
                </button>

                {/* Фонды — тот же пикер, что в «Общем портфеле»: таблица со СЧА и
                    доходностью, группы-подкатегории, «Выбрать все», таблетка «Без
                    индексных фондов», применение по закрытию. Набор сужен до
                    держателей бумаги — отсюда свои заголовок и подпись таблетки. */}
                <PortfolioFundPicker
                    funds={pickerFunds}
                    selected={effectiveFunds}
                    onChange={handleFundsChange}
                    title="Фонды с этой бумагой"
                    allLabel="Все фонды"
                    compact={embedded}
                    iconOnly={embedded && tbCompact}
                />

                {/* Режим — выпадающий список (4 пункта в ряд уже не влезали).
                    «% в обращении» и «Навес» появляются только у бумаг со
                    своими данными (free-float капа / оборот торгов). Подсказка
                    «?» — при каждом пункте списка. Обёртка — якорь
                    онбординг-тура (шаг «Четыре взгляда на бумагу»);
                    display:flex, чтобы в flex-ряду контролов ничего не поехало. */}
                {embedded ? (
                    <PillGroup<ChartMode>
                        value={effectiveMode}
                        options={visibleModes.map(m => ({ id: m, label: MODE_LABELS[m], icon: MODE_ICONS[m] }))}
                        onChange={setMode}
                        compact={tbCompact}
                    />
                ) : (
                <div data-tour="ft-company-modes" style={{ display: 'flex' }}>
                <Dropdown<ChartMode>
                    options={visibleModes.map(m => ({
                        key: m,
                        label: MODE_LABELS[m],
                        help: { title: MODE_LABELS[m], content: MODE_HELP[m] },
                    }))}
                    value={effectiveMode}
                    onChange={setMode}
                />
                </div>
                )}

                {/* Период — окно последних N месяцев (1 год / 3 года / Всё).
                    Последний в левой группе контролов: идёт после тумблеров, но
                    к правому краю ряда НЕ прижимается. */}
                {embedded ? (
                    // Одной кнопкой-списком: три пилюли съедали полосу и гнали
                    // тулбар в скролл (фидбек Вадима).
                    <EmbDropdown<Period>
                        value={period}
                        options={CF_PERIODS.map(p => ({ id: p, label: PERIOD_LABELS[p] }))}
                        onChange={setPeriod}
                        title="Период"
                        icon={<CalendarRange size={14} />}
                        compact={tbCompact}
                    />
                ) : (
                <SegmentedControl<Period>
                    options={CF_PERIODS.map(p => ({ key: p, label: PERIOD_LABELS[p] }))}
                    value={period}
                    onChange={setPeriod}
                />
                )}

                {/* Скриншот + Настройки — kebab «⋮» в углу графика (как в «Деньги в
                    фондах»). JSX живёт тут, рядом со state, а DOM через portal уезжает
                    в chartAnchorRef (position:relative). */}
                {showChartActions && (
                    <ChartActionsMenu containerRef={chartAnchorRef}>
                        <ChartCaptureButton
                            getTargetElement={() => chartAnchorRef.current}
                            filename={`frame-company-flows-${effectiveMode}-${selectedTicker ?? selectedAsset?.key ?? 'asset'}-${period}`}
                            metadata={{
                                title: 'Сделки фондов',
                                asset: selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined,
                                ticker: selectedTicker,
                                details: [
                                    PERIOD_LABELS[period],
                                    // «Все фонды» ⇔ не выключен ни один держатель;
                                    // иначе сколько именно осталось на графике.
                                    effectiveFunds.size === (flows?.funds.length ?? 0)
                                        ? 'Все фонды'
                                        : `${effectiveFunds.size} ${pluralFunds(effectiveFunds.size)}`,
                                ],
                            }}
                            getExportStyles={(): Record<string, string> => ({
                                // Ось Y только справа: в PNG зеркалим правый strip слева,
                                // иначе график прижат к левому краю (как в FlowsHistogram).
                                '--chart-pad-left': 'calc(var(--chart-pad-right-single) - 12px)',
                            })}
                        />
                        {/* Гистограмма, а не SimpleChart → тип графика неприменим,
                            в модалке остаётся палитра. */}
                        <ChartSettings showType={false} />
                    </ChartActionsMenu>
                )}
            </div>
    );

    return (
        <div
            // Тултип этой вкладки в панели ОСТАВЛЯЕМ (см. sandbox.css): движок
            // старый, значений в легенде нет, и разбивку сделок по фондам
            // показывает только подсказка под курсором.
            className={embedded ? 'emb-tooltip-ok' : undefined}
            style={embedded
            // Панель: контролы уехали в тулбар, здесь только график во всю площадь.
            ? { display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }
            : { display: 'flex', flexDirection: 'column', gap: 'var(--sp-4)' }}
        >
            {embedded
                ? (controlsTarget ? createPortal(controlsRow, controlsTarget) : null)
                : controlsRow}

            {flowsError && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {flowsError}
                </div>
            )}
            {effectiveMode !== 'map' && weightsError && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {weightsError}
                </div>
            )}
            {/* Сетевая ошибка загрузки цены (NO_PRICE_HISTORY — не ошибка, а
                empty-state внутри чарта: карта — заглушка, доля — без линии). */}
            {priceError && priceError !== 'NO_PRICE_HISTORY' && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {priceError}
                </div>
            )}

            {/* Карта сделок: недельная цена + кругляши месячных нетто.
                Доля: та же цена сверху + гистограмма доли снизу. Оба чарта
                получают одинаковое окно месяцев — режимы синхронны. */}
            {/* position:relative — host для portal'а kebab-меню (ChartActionsMenu
                позиционируется absolute относительно этой обёртки). */}
            <div
                ref={setChartAnchor}
                data-tour="ft-company-chart"
                style={embedded
                    ? { position: 'relative', flex: 1, minHeight: 0, paddingTop: PAD_TOP, boxSizing: 'border-box' }
                    : { position: 'relative' }}
            >
                {effectiveMode === 'map' ? (
                    <CompanyFlowsPriceMap
                        months={visibleMonths}
                        series={visibleSeries}
                        weeks={price && price.ticker === selectedTicker ? price.weeks : []}
                        closes={price && price.ticker === selectedTicker ? price.closes : []}
                        assetName={selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined}
                        height={chartHeight}
                        loading={flowsLoading || priceLoading}
                        noFundsSelected={noFundsSelected}
                        priceMissing={!priceLoading && (priceError === 'NO_PRICE_HISTORY')}
                        animTrigger={animTrigger}
                        bare={embedded}
                    />
                ) : (
                    <CompanyShareChart
                        months={visibleShareMonths}
                        funds={visibleShareFunds}
                        ffcap={visibleFfcap}
                        turnover={visibleTurnover}
                        weeks={price && price.ticker === selectedTicker ? price.weeks : []}
                        closes={price && price.ticker === selectedTicker ? price.closes : []}
                        shareMode={effectiveMode as ShareMode}
                        assetName={selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined}
                        height={chartHeight}
                        loading={weightsLoading || priceLoading || flowsLoading}
                        noFundsSelected={noFundsSelectedShare}
                        priceMissing={!priceLoading && (priceError === 'NO_PRICE_HISTORY')}
                        animTrigger={animTrigger}
                        bare={embedded}
                    />
                )}
            </div>

            {/* ITEM 3 — модалка выбора бумаги (assets = текущий список). */}
            {pickerOpen && (
                <AssetPickerModal
                    assets={assets}
                    onSelect={a => setSelectedKey(a.key)}
                    onClose={() => setPickerOpen(false)}
                />
            )}
        </div>
    );
}
