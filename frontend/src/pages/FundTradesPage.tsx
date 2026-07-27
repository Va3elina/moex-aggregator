/**
 * FundTradesPage — отслеживание покупок/продаж в БПИФах.
 *
 * Архитектура:
 *   - Открыт для ВСЕХ тиров (флаг useCommonFeatures().fund_trades_access;
 *     LockedView-ветка оставлена на случай будущего ре-гейтинга).
 *   - Три таба: «Состав фондов» (карточки) + «Сделки фондов» (консенсус-
 *     гистограмма across фондов) + «Обзор снапшота» (per-fund помесячно).
 *   - При клике на фонд → детальный diff с current_holdings и изменениями.
 *
 * Источники данных: backend `/api/fund-trades/*`. Snapshot истории строится
 * скриптом `Funds/fetch_funds_realtime.py` который cron-аем дёргает раз в день.
 * Основной источник составов = monthly, ВИМ-парсер (WIP) = daily.
 *
 * Editorial design:
 *   - Card-based layout с иконками категорий
 *   - Diff показывается цветом: accumulated=success, reduced=danger,
 *     new=accent, sold_out=muted
 *   - Шаг данных — 1 снапшот в месяц; сравнение всегда «месяц vs предыдущий».
 */
import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { Link } from 'react-router-dom';
import {
    Lock,
    Sparkles,
    Wallet,
    TrendingUp,
    TrendingDown,
    Activity,
    ArrowLeftRight,
    Briefcase,
} from 'lucide-react';
import {
    listFundsWithHistory,
    getFundTradesDetail,
    getFundTradesMovers,
    getFundSnapshots,
    getFundSnapshotReview,
    getFundPortfolio,
    type FundTradesPeriod,
    type FundPortfolio,
    type FundWithHistory,
    type FundTradesMovers,
    type FundSnapshotsList,
    type FundSnapshotReview,
    type FundDiffRow,
} from '../services/api';
import { useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useAuth } from '../contexts/AuthContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import PageHeader from '../components/PageHeader';
import ChartTabs from '../components/ChartTabs';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SegmentedControl from '../components/SegmentedControl';
import { UK_LOGOS, DONUT_COLORS, fundAssetName, fundAssetColor, resolveFundLogo, resolveFundTicker, stripUkName, isOfzBond } from '../config/fundConfig';
import Donut from '../components/funds/Donut';
import InstrumentIcon from '../components/InstrumentIcon';
import CompanyFlowsTab from '../components/fundtrades/CompanyFlowsTab';
import DelayedDataBadge from '../components/fundtrades/DelayedDataBadge';
import LockedSnapshotTeaser from '../components/fundtrades/LockedSnapshotTeaser';
import UkMultiSelect, { type UkOption } from '../components/fundtrades/UkMultiSelect';
import FundPicker, { type FundPickerFund } from '../components/fundtrades/FundPicker';
import CombinedPortfolioView from '../components/fundtrades/CombinedPortfolioView';
import PortfolioMoversPanel, { type MoversPeriod } from '../components/fundtrades/PortfolioMoversPanel';
import { type MonthRange } from '../components/fundtrades/MonthRangePicker';
import { useViewportWidth } from '../hooks/useViewportWidth';
import { useGrowReveal } from '../hooks/useGrowReveal';
import { usePersistedState, usePersistedSet } from '../hooks/usePersistedState';
import FundDetailModal, {
    AssetHistoryModal,
    formatRubShort,
    formatReturnPct,
    returnColor,
    formatShares,
} from '../components/funds/FundDetailModal';

type Tab = 'funds' | 'portfolio' | 'movers' | 'snapshots' | 'company';

const CATEGORY_LABEL: Record<string, string> = {
    stocks: 'Акции',
    'Авторские': 'Авторские',
    bonds: 'Облигации',
    money_market: 'Денежный рынок',
    gold: 'Золото',
};

// ════════════════════════════════════════════════════════════════════
// Lock screen для non-Pro юзеров
// ════════════════════════════════════════════════════════════════════

function LockedView() {
    const { isAuthenticated } = useAuth();
    const { showUpgrade } = useUpgradePrompt();
    return (
        <div className="max-w-3xl mx-auto px-4 md:px-6 py-12">
            <div
                style={{
                    padding: 32,
                    textAlign: 'center',
                    background: 'var(--bg-secondary)',
                    border: '2px solid color-mix(in srgb, var(--accent) 30%, transparent)',
                    borderRadius: 16,
                }}
            >
                <div
                    style={{
                        width: 64,
                        height: 64,
                        margin: '0 auto 20px',
                        borderRadius: 14,
                        background: 'color-mix(in srgb, var(--accent) 18%, transparent)',
                        color: 'var(--accent)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}
                >
                    <Lock size={28} strokeWidth={2.2} />
                </div>
                <h1
                    style={{
                        fontSize: 'var(--fs-2xl)',
                        fontWeight: 800,
                        color: 'var(--text-primary)',
                        marginBottom: 8,
                        letterSpacing: '-0.01em',
                    }}
                >
                    Сделки фондов
                </h1>
                <p
                    style={{
                        fontSize: 'var(--fs-base)',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.55,
                        maxWidth: 520,
                        margin: '0 auto 20px',
                    }}
                >
                    Отслеживайте куда направляются деньги крупных фондов акций: какие
                    акции управляющие компании накапливают, а что распродают.
                </p>
                <ul
                    style={{
                        listStyle: 'none',
                        padding: 0,
                        margin: '0 auto 24px',
                        maxWidth: 460,
                        textAlign: 'left',
                    }}
                >
                    {[
                        'Изменения портфеля по каждому БПИФ за период',
                        'Топ-аккумуляция / распродажа across всех фондов',
                        'История портфельных движений по месяцам',
                    ].map((t) => (
                        <li
                            key={t}
                            style={{
                                display: 'flex',
                                gap: 10,
                                padding: '8px 0',
                                fontSize: 'var(--fs-sm)',
                                color: 'var(--text-secondary)',
                            }}
                        >
                            <span style={{ color: 'var(--accent)' }}>✓</span>
                            <span>{t}</span>
                        </li>
                    ))}
                </ul>
                {isAuthenticated ? (
                    <button
                        onClick={() =>
                            showUpgrade({
                                tier: 'pro',
                                featureName: 'Сделки фондов',
                                indicator: 'fund_trades',
                            })
                        }
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 6,
                            padding: '10px 24px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            cursor: 'pointer',
                            boxShadow: 'var(--shadow-hard-chip)',
                        }}
                    >
                        <Sparkles size={14} />
                        Перейти на Pro
                    </button>
                ) : (
                    <Link
                        to="/login"
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 6,
                            padding: '10px 24px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            textDecoration: 'none',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                        }}
                    >
                        Войти
                    </Link>
                )}
            </div>
        </div>
    );
}

// ════════════════════════════════════════════════════════════════════
// Fund Detail Modal + Asset History drill-down вынесены в общий компонент
// `components/funds/FundDetailModal` (переиспользуются в «Деньги в фондах»).
// ════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════
// Main page
// ════════════════════════════════════════════════════════════════════

export default function FundTradesPage() {
    const common = useCommonFeatures();
    const { showUpgrade } = useUpgradePrompt(); // пейволл на locked-месяце movers (Free/гость)
    const [tab, setTab] = usePersistedState<Tab>('frame:fundtrades:tab', 'funds');
    // Вкладки movers/snapshots скрыты — если в localStorage лежит одна из них
    // (пользователь выбирал раньше), откатываем на «Состав фондов».
    useEffect(() => {
        if (tab === 'movers' || tab === 'snapshots') setTab('funds');
    }, [tab]);
    // Шаг данных — 1 снапшот/месяц. Период фиксирован '1m' (месяц vs предыдущий);
    // селектор месяца появится в Заходе 2 (нужен backend as_of/available_months).
    const [period] = useState<FundTradesPeriod>('1m');
    const [selectedTicker, setSelectedTicker] = useState<string | null>(null);
    // Фильтры «Состав фондов» — combinable (AND): период доходности + сортировка + УК.
    // Период доходности: показывается на плитках И используется для сортировки по доходности.
    const [returnPeriod, setReturnPeriod] = usePersistedState<ReturnPeriodKey>('frame:fundtrades:returnPeriod', 'y1');
    // Сортировка карточек: по доходности (за returnPeriod) / объёму СЧА.
    const [fundSort, setFundSort] = usePersistedState<FundSortKey>('frame:fundtrades:fundSort', 'return');
    // Персист мог сохранить убранные значения (период m3/m6, сортировка «Имя») —
    // нормализуем к валидным, иначе у SegmentedControl не будет активной пилюли.
    useEffect(() => {
        if (returnPeriod !== 'm1' && returnPeriod !== 'y1' && returnPeriod !== 'y5') setReturnPeriod('y1');
        if ((fundSort as string) === 'name') setFundSort('return');
        // '3y' убран из пресетов «Сделок фондов» (2026-07) — у кого он был
        // персистнут, откатываем на 1Г (дольше — теперь через свой диапазон).
        if ((portfolioMoversPeriod as string) === '3y') setPortfolioMoversPeriod('1y');
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
    // Мультиселект УК (пусто = все). Ключ — uk_id (стабильнее имени), fallback на uk.
    const [selectedUks, setSelectedUks] = useState<Set<string>>(new Set());
    // Hover-связь пончик↔список на плитке. Ключуем по fund_id (карточки в map, своего
    // state у каждой нет) + индекс слайса. Наведение на сектор/строку подсвечивает обоих.
    const [tileHover, setTileHover] = useState<{ fund: number; idx: number } | null>(null);

    const [funds, setFunds] = useState<FundWithHistory[]>([]);
    const [movers, setMovers] = useState<FundTradesMovers | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    // Заход 2: управление табом «Сделки фондов».
    const [asOf, setAsOf] = useState<string | undefined>(undefined);    // выбранный месяц (undefined = последний)
    // Мультиселект КОНКРЕТНЫХ фондов (пусто = все). Ключ = ticker; бэкенд /movers
    // принимает comma-separated тикеры в параметре `funds` (приоритет над manager).
    // Раньше тут был мультиселект УК (uk_id) — заменён на выбор фондов через FundPicker.
    const [selectedMoverFunds, setSelectedMoverFunds] = useState<Set<string>>(new Set());
    const [metric, setMetric] = usePersistedState<'weight' | 'amount'>('frame:fundtrades:metric', 'weight'); // % веса | объём ₽
    // ITEM 2 — предвыбранная бумага для перехода movers → «Потоки по компании».
    const [companyPreset, setCompanyPreset] = useState<{ asset_name: string; isin: string | null } | null>(null);

    // Общий портфель — агрегированный состав выбранных фондов акций как один портфель.
    // Выбор УК персистится (usePersistedSet, пусто = все); режим веса и период — тоже.
    const [portfolioUks, setPortfolioUks] = usePersistedSet<string>('frame:fundtrades:portfolioUks');
    const [portfolioMode, setPortfolioMode] = usePersistedState<'rub' | 'share'>('frame:fundtrades:portfolioMode', 'rub');
    // Доходность в плашке — на 1 год (с фолбэком на длиннейший доступный внутри вью).
    // Период плашки доходности «Общего портфеля» — только штатные периоды
    // (без 5л, у CombinedPortfolioView свой PeriodKey). Всегда 'y1'.
    const [portfolioPeriod] = usePersistedState<'m1' | 'm3' | 'm6' | 'y1'>('frame:fundtrades:portfolioPeriod', 'y1');
    const [portfolio, setPortfolio] = useState<FundPortfolio | null>(null);
    const [portfolioLoading, setPortfolioLoading] = useState(false);
    // Месяц-срез портфеля (month-picker в шапке «Состав портфеля»); undefined = последний.
    const [portfolioAsOf, setPortfolioAsOf] = useState<string | undefined>(undefined);
    // Блок «Сделки фондов» рядом с составом: чистая покупка за период (1м/6м/1г/3г).
    const [portfolioMoversPeriod, setPortfolioMoversPeriod] = usePersistedState<MoversPeriod>('frame:fundtrades:portfolioMoversPeriod', '1m');
    // Свой диапазон месяцев (кнопка-календарь) — задан, отменяет пресет. Персистится
    // вместе с ним: вернулся на страницу — тот же период, что и оставил.
    const [portfolioMoversRange, setPortfolioMoversRange] = usePersistedState<MonthRange | null>('frame:fundtrades:portfolioMoversRange', null);
    const [portfolioMovers, setPortfolioMovers] = useState<FundTradesMovers | null>(null);
    const [portfolioMoversLoading, setPortfolioMoversLoading] = useState(false);

    // Load funds list (один раз).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        setLoading(true);
        listFundsWithHistory()
            .then((r) => setFunds(r.funds))
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [common.fund_trades_access]);

    // Load movers when tab=movers (или меняются параметры: месяц/фонды/метрика).
    // selectedMoverFunds (Set ticker) → comma-separated строка для бэкенда; пусто = все фонды.
    const fundsParam = useMemo(
        () => Array.from(selectedMoverFunds).join(','),
        [selectedMoverFunds],
    );

    // Задержка на вкладке «Сделки фондов» (Free/гость): месяцы с датой > snapshot_cutoff
    // заблокированы (свежий консенсус по подписке). Дефолт-месяц = последний ДОСТУПНЫЙ
    // (не locked) — чтобы дропдаун сразу показывал актуальный срез, а не пустой «Выбрать».
    const isMoverMonthLocked = (m: string) => {
        const c = movers?.snapshot_cutoff ?? null;
        return c != null && m > c;
    };
    const defaultMoverMonth = movers?.available_months.find((m) => !isMoverMonthLocked(m))
        ?? movers?.available_months[0];
    useEffect(() => {
        if (!common.fund_trades_access) return;
        if (tab !== 'movers') return;
        setLoading(true);
        getFundTradesMovers(period, { asOf, funds: fundsParam || undefined, sort: metric })
            .then(setMovers)
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [tab, period, asOf, fundsParam, metric, common.fund_trades_access]);

    // Общий портфель: выбранные УК → тикеры фондов (пусто = все whitelist-акции).
    const portfolioFundsParam = useMemo(() => {
        if (portfolioUks.size === 0) return '';
        return funds.filter((f) => portfolioUks.has(ukKey(f))).map((f) => f.ticker).join(',');
    }, [funds, portfolioUks]);

    // Месяц-срез портфеля: месяцы > snapshot_cutoff заблокированы (Free/гость —
    // свежий срез по подписке), дефолт = последний ДОСТУПНЫЙ, как в «Покупках фондов».
    const isPortfolioMonthLocked = (m: string) => {
        const c = portfolio?.snapshot_cutoff ?? null;
        return c != null && m > c;
    };

    // Календарь своего периода в «Сделках фондов»: отсечка своя (из /movers), не из
    // /portfolio — гейтится именно свежий консенсус сделок.
    const isMoversRangeMonthLocked = (m: string) => {
        const c = portfolioMovers?.snapshot_cutoff ?? null;
        return c != null && m > c;
    };

    // Load общий портфель (tab=portfolio; смена набора УК или выбранного месяца).
    // Ждём загрузки списка фондов — без него не резолвить УК→тикеры (иначе пустой
    // набор ошибочно = «все»).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        if (tab !== 'portfolio') return;
        if (funds.length === 0) return;
        setPortfolioLoading(true);
        getFundPortfolio({ funds: portfolioFundsParam || undefined, as_of: portfolioAsOf })
            .then(setPortfolio)
            .catch((e: Error) => setError(e.message))
            .finally(() => setPortfolioLoading(false));
    }, [tab, portfolioFundsParam, portfolioAsOf, funds.length, common.fund_trades_access]);

    // Сделки фондов для блока рядом с составом: чистая покупка за выбранный период,
    // тот же набор УК (→ тикеры), ранжирование по рублям.
    useEffect(() => {
        if (!common.fund_trades_access) return;
        if (tab !== 'portfolio') return;
        if (funds.length === 0) return;
        setPortfolioMoversLoading(true);
        getFundTradesMovers(portfolioMoversPeriod, {
            funds: portfolioFundsParam || undefined,
            sort: 'amount',
            // Свой диапазон приоритетнее пресета (бэкенд игнорирует period при from+to).
            from: portfolioMoversRange?.from,
            to: portfolioMoversRange?.to,
        })
            .then(setPortfolioMovers)
            .catch((e: Error) => setError(e.message))
            .finally(() => setPortfolioMoversLoading(false));
    }, [tab, portfolioFundsParam, portfolioMoversPeriod, portfolioMoversRange?.from, portfolioMoversRange?.to, funds.length, common.fund_trades_access]);

    // Уникальные УК из загруженных фондов — список для UkMultiSelect на вкладке
    // «Состав фондов». Ключ — uk_id (стабильнее имени), name — uk-имя из
    // UK_LOGOS/данных, uk_id — для аватара. Сортируем по имени.
    const ukOptions = useMemo<UkOption[]>(() => {
        const map = new Map<string, UkOption>();
        const count = new Map<string, number>();
        for (const f of funds) {
            const key = ukKey(f);
            if (!key) continue;
            count.set(key, (count.get(key) ?? 0) + 1);
            if (map.has(key)) continue;
            map.set(key, {
                key,
                name: UK_LOGOS[key]?.name || f.uk || key,
                uk_id: f.uk_id ?? key,
            });
        }
        // Порядок: УК с наибольшим числом фондов сверху, при равенстве — по имени.
        return Array.from(map.values()).sort((a, b) => {
            const d = (count.get(b.key) ?? 0) - (count.get(a.key) ?? 0);
            return d !== 0 ? d : a.name.localeCompare(b.name);
        });
    }, [funds]);

    // Все whitelist-фонды для FundPicker (multi) на вкладке «Сделки фондов».
    // FundPicker сам группирует по УК; передаём минимум полей ({ticker, name, uk, uk_id}).
    const moverPickerFunds = useMemo<FundPickerFund[]>(
        () => funds.map((f) => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
        [funds],
    );

    const fundsByCategory = useMemo(() => {
        // E: combinable AND-фильтры — сначала фильтр по УК, потом группировка+сортировка.
        const filtered = selectedUks.size > 0
            ? funds.filter((f) => selectedUks.has(ukKey(f)))
            : funds;

        const groups: Record<string, FundWithHistory[]> = {};
        for (const f of filtered) {
            // Авторские (блогерские) фонды — отдельной группой, остальные по категории
            const key = f.subcategory === 'Авторские' ? 'Авторские' : (f.category || 'other');
            if (!groups[key]) groups[key] = [];
            groups[key].push(f);
        }
        // Сортировка внутри группы. null'ы (нет данных) всегда в хвост.
        // Доходность — за выбранный returnPeriod; объём — nav_rub; имя — ticker A→Z.
        const cmp = (a: FundWithHistory, b: FundWithHistory): number => {
            const av = fundSort === 'return' ? returnForPeriod(a.returns, returnPeriod) : a.nav_rub;
            const bv = fundSort === 'return' ? returnForPeriod(b.returns, returnPeriod) : b.nav_rub;
            if (av === null && bv === null) return a.ticker.localeCompare(b.ticker);
            if (av === null) return 1;
            if (bv === null) return -1;
            return bv - av; // DESC
        };
        for (const k of Object.keys(groups)) groups[k] = [...groups[k]].sort(cmp);
        return groups;
    }, [funds, fundSort, returnPeriod, selectedUks]);

    // (A) Колонки сетки плиток по ширине вьюпорта: ≥1024 → 3, ≥640 → 2, иначе 1.
    const vw = useViewportWidth();
    const cols = vw >= 1280 ? 4 : vw >= 1024 ? 3 : vw >= 640 ? 2 : 1;

    // ITEM 2 — клик по активу в movers → «Потоки по компании» с предвыбранной бумагой.
    // mover.akey = ISIN (если есть в снапшоте), иначе имя. Если akey похож на ISIN —
    // передаём как isin, иначе фолбэк на asset_name (CompanyFlowsTab матчит по любому).
    const openCompanyFlows = (m: FundTradesMovers['top_accumulated'][number]) => {
        const isin = isIsin(m.akey) ? m.akey : null;
        setCompanyPreset({ asset_name: m.asset_name, isin });
        setTab('company');
    };

    if (!common.fund_trades_access) {
        return <LockedView />;
    }

    return (
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            {/* Header — единый PageHeader как у всех индикаторов
                (иконка стилизуется через .page-header-icon → выравнивание как везде) */}
            <PageHeader
                icon={Wallet}
                title="Сделки фондов"
                subtitle="Состав портфелей крупных фондов акций — что управляющие компании накапливают и распродают"
            />

            {/* Постоянный нудж «данные с задержкой» — виден на всех табах (Free/гость) */}
            <DelayedDataBadge />

            {/* Ошибка загрузки — НАД карточкой: между вкладками и панелью не должно
                быть ничего, иначе язычок папки оторвётся от панели (см. .has-tabs). */}
            {error && (
                <div
                    style={{
                        padding: 12,
                        marginBottom: 16,
                        background: 'color-mix(in srgb, var(--danger, #ef4444) 10%, transparent)',
                        border: '1px solid var(--danger, #ef4444)',
                        borderRadius: 8,
                        color: 'var(--danger, #ef4444)',
                        fontSize: 'var(--fs-sm)',
                    }}
                >
                    {error}
                </div>
            )}

            {/* Карточка с вкладками: обёртка несёт единую editorial-тень на
                [вкладки + панель]. Папка-вкладки 1:1 с «Деньгами в фондах». */}
            <div className="tabbed-card">

            {/* Вкладки — editorial folder-tabs (ChartTabs). Movers/snapshots скрыты
                с сайта по просьбе Вадима: вернуть = дописать сюда строки обратно. */}
            <ChartTabs<Tab>
                value={tab}
                onChange={setTab}
                items={[
                    { key: 'portfolio', label: 'Общий портфель', Icon: Briefcase },
                    { key: 'company', label: 'По бумаге', Icon: ArrowLeftRight },
                    { key: 'funds', label: 'Витрина', Icon: Wallet },
                ]}
            />

            {/* Tab content — активная вкладка сливается с панелью (.has-tabs) */}
            {tab === 'funds' && (
                // Editorial-frame has-tabs — панель папки: контролы сверху на белом,
                // а сетка карточек — на бежевой paper-card внутри.
                <div className="editorial-frame has-tabs">
                    {/* Контролы карточек — единый формат индикаторов: сортировка и
                        период доходности отдельными SegmentedControl (как период/режим
                        на «Открытых позициях» и «Деньгах в фондах»), плюс мультиселект УК.
                        Период влияет и на сортировку по доходности, и на число, которое
                        показывает карточка («Доходность · …»). Combinable AND-фильтры. */}
                    {funds.length > 0 && (
                        <div className="flex flex-wrap items-center gap-2 md:gap-3 mb-3 md:mb-4">
                            <SegmentedControl<FundSortKey>
                                options={[
                                    { key: 'return', label: 'Доходность' },
                                    { key: 'volume', label: 'Объём СЧА' },
                                ]}
                                value={fundSort}
                                onChange={setFundSort}
                            />
                            <SegmentedControl<ReturnPeriodKey>
                                options={(['m1', 'y1', 'y5'] as ReturnPeriodKey[]).map((k) => ({
                                    key: k,
                                    label: RETURN_PERIOD_LABEL[k],
                                }))}
                                value={returnPeriod}
                                onChange={setReturnPeriod}
                            />
                            {ukOptions.length > 1 && (
                                <UkMultiSelect
                                    options={ukOptions}
                                    selected={selectedUks}
                                    onChange={setSelectedUks}
                                    size="md"
                                />
                            )}
                        </div>
                    )}
                    {/* Сетка карточек лежит прямо на editorial-frame — без
                        дополнительной бежевой подложки и внутренней рамки. */}
                    <div>
                    {loading && funds.length === 0 && (
                        <div
                            style={{
                                display: 'grid',
                                gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`,
                                gap: 14,
                            }}
                        >
                            {Array.from({ length: cols * 2 }, (_, i) => (
                                <Skeleton key={i} height={220} rounded="md" />
                            ))}
                        </div>
                    )}
                    {!loading && funds.length === 0 && !error && (
                        <EmptyState message="Фонды не найдены." />
                    )}
                    {Object.entries(fundsByCategory).map(([cat, list]) => (
                        <div key={cat} style={{ marginBottom: 20 }}>
                            <h2
                                style={{
                                    fontSize: 'var(--fs-md)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 8,
                                    textTransform: 'uppercase',
                                    letterSpacing: '0.06em',
                                }}
                            >
                                {CATEGORY_LABEL[cat] || cat} <span style={{ color: 'var(--text-muted)', fontWeight: 500 }}>· {list.length}</span>
                            </h2>
                            <div
                                style={{
                                    display: 'grid',
                                    gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`,
                                    gap: 14,
                                }}
                            >
                                {list.map((f) => {
                                    const uk = resolveFundLogo(f.ticker, f.uk_id);
                                    // (C) holdings для пончика = топ-10 + «Прочее» (100 − Σтоп), иначе
                                    // донат нормализует топ как 100% и завышает концентрацию. colors —
                                    // ПАРАЛЛЕЛЬНЫЙ массив: фирменный цвет бумаги или DONUT_COLORS по индексу,
                                    // «Прочее» — серый. maxSlices велик → Donut не агрегирует сам.
                                    const top = f.top_holdings ?? [];
                                    // Донат подсвечивает только те 5 бумаг, что показаны в списке;
                                    // всё остальное сворачивается в один серый сектор «Прочее».
                                    const listed = top.slice(0, 5);
                                    const sum = listed.reduce((s, h) => s + (h.weight || 0), 0);
                                    const other = 100 - sum;
                                    const donutHoldings = other > 1
                                        ? [...listed, { name: 'Прочее', isin: null, weight: other }]
                                        : listed;
                                    const donutColors = donutHoldings.map((h, i) =>
                                        h.name === 'Прочее'
                                            ? 'var(--text-muted)'
                                            : (fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length]),
                                    );
                                    const ret = displayReturn(f.returns, returnPeriod);
                                    return (
                                    <button
                                        key={f.fund_id}
                                        onClick={() => setSelectedTicker(f.ticker)}
                                        className="editorial-press"
                                        style={{
                                            padding: 14,
                                            background: 'var(--bg-secondary)',
                                            border: '1.5px solid var(--border-color)',
                                            borderRadius: 12,
                                            // Тень не по умолчанию, а только на hover — как у всех кнопок
                                            // сайта: класс editorial-press добавляет hard-shadow при :hover.
                                            textAlign: 'left',
                                            cursor: 'pointer',
                                            // (B) колоночный flex full-height → все карточки в ряду равны.
                                            display: 'flex',
                                            flexDirection: 'column',
                                            height: '100%',
                                        }}
                                    >
                                        {/* Header: УК-аватар + имя (2 строки) + тикер-eyebrow */}
                                        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                                            {uk && (
                                                <div
                                                    title={uk.name}
                                                    style={{
                                                        width: 44,
                                                        height: 44,
                                                        borderRadius: '50%',
                                                        display: 'flex',
                                                        alignItems: 'center',
                                                        justifyContent: 'center',
                                                        flexShrink: 0,
                                                        fontWeight: 700,
                                                        fontSize: 'var(--fs-base)',
                                                        overflow: 'hidden',
                                                        backgroundColor: uk.img ? undefined : uk.bg,
                                                        color: uk.color,
                                                    }}
                                                >
                                                    {uk.img
                                                        ? <img src={uk.img} alt={uk.name} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
                                                        : uk.letter}
                                                </div>
                                            )}
                                            <div style={{ flex: 1, minWidth: 0 }}>
                                                <div
                                                    style={{
                                                        fontSize: 'var(--fs-sm)',
                                                        fontWeight: 700,
                                                        color: 'var(--text-primary)',
                                                        lineHeight: 1.25,
                                                        marginBottom: 3,
                                                        display: '-webkit-box',
                                                        WebkitLineClamp: 2,
                                                        WebkitBoxOrient: 'vertical',
                                                        overflow: 'hidden',
                                                    }}
                                                    title={f.name}
                                                >
                                                    {stripUkName(f.name, f.uk_id)}
                                                </div>
                                                <div
                                                    style={{
                                                        fontFamily: 'var(--font-mono, ui-monospace, monospace)',
                                                        fontSize: 'var(--fs-2xs)',
                                                        fontWeight: 500,
                                                        letterSpacing: '0.06em',
                                                        textTransform: 'uppercase',
                                                        color: 'var(--text-muted)',
                                                        lineHeight: 1.3,
                                                        whiteSpace: 'nowrap',
                                                        overflow: 'hidden',
                                                        textOverflow: 'ellipsis',
                                                    }}
                                                >
                                                    {f.ticker}
                                                </div>
                                            </div>
                                        </div>

                                        {/* Body: пончик (без центр-счётчика, уменьшен) + топ-5 */}
                                        <div
                                            style={{
                                                display: 'flex',
                                                gap: 14,
                                                alignItems: 'center',
                                                marginTop: 14,
                                            }}
                                        >
                                            {top.length > 0 ? (
                                                <div style={{ flexShrink: 0, lineHeight: 0 }}>
                                                    <Donut
                                                        holdings={donutHoldings}
                                                        colors={donutColors}
                                                        size={104}
                                                        outerRadius={90}
                                                        innerRadius={64}
                                                        maxSlices={donutHoldings.length}
                                                        showCenterText={false}
                                                        highlightIndex={tileHover?.fund === f.fund_id ? tileHover.idx : null}
                                                        onHoverChange={(i) => setTileHover(i == null ? null : { fund: f.fund_id, idx: i })}
                                                    />
                                                </div>
                                            ) : (
                                                <div
                                                    style={{
                                                        width: 104,
                                                        height: 104,
                                                        flexShrink: 0,
                                                        borderRadius: '50%',
                                                        border: '1.5px dashed var(--border-color)',
                                                        display: 'flex',
                                                        alignItems: 'center',
                                                        justifyContent: 'center',
                                                        color: 'var(--text-muted)',
                                                        fontSize: 'var(--fs-2xl)',
                                                    }}
                                                >
                                                    —
                                                </div>
                                            )}
                                            <div
                                                style={{
                                                    flex: 1,
                                                    minWidth: 0,
                                                    display: 'flex',
                                                    flexDirection: 'column',
                                                    justifyContent: 'center',
                                                    gap: 4,
                                                    // (B) фикс высота под 5 строк — список не «двигает» футер,
                                                    // даже если позиций <5. 5×22 + 4×4 = 126.
                                                    minHeight: 126,
                                                }}
                                            >
                                                {top.slice(0, 5).map((h, i) => (
                                                    <div
                                                        key={h.name + i}
                                                        onMouseEnter={() => setTileHover({ fund: f.fund_id, idx: i })}
                                                        onMouseLeave={() => setTileHover(null)}
                                                        style={{
                                                            display: 'flex',
                                                            alignItems: 'center',
                                                            gap: 7,
                                                            minHeight: 22,
                                                            fontSize: 'var(--fs-xs)',
                                                            borderRadius: 5,
                                                            padding: '0 4px',
                                                            margin: '0 -4px',
                                                            background: tileHover?.fund === f.fund_id && tileHover.idx === i ? 'var(--bg-primary)' : 'transparent',
                                                            transition: 'background 120ms',
                                                        }}
                                                    >
                                                        <span
                                                            style={{
                                                                width: 8,
                                                                height: 8,
                                                                borderRadius: '50%',
                                                                flexShrink: 0,
                                                                // (C) точка = цвет сектора пончика (фирменный/индекс).
                                                                backgroundColor: fundAssetColor(h.name, h.isin) ?? DONUT_COLORS[i % DONUT_COLORS.length],
                                                            }}
                                                        />
                                                        <span
                                                            style={{
                                                                flex: 1,
                                                                minWidth: 0,
                                                                overflow: 'hidden',
                                                                textOverflow: 'ellipsis',
                                                                whiteSpace: 'nowrap',
                                                                color: 'var(--text-secondary)',
                                                                fontWeight: 500,
                                                            }}
                                                        >
                                                            {fundAssetName(h.name, h.isin)}
                                                        </span>
                                                        <span
                                                            style={{
                                                                flexShrink: 0,
                                                                fontVariantNumeric: 'tabular-nums',
                                                                fontWeight: 700,
                                                                color: 'var(--text-primary)',
                                                            }}
                                                        >
                                                            {h.weight.toFixed(1)}%
                                                        </span>
                                                    </div>
                                                ))}
                                                {top.length === 0 && (
                                                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)' }}>
                                                        Состав не публикуется
                                                    </span>
                                                )}
                                            </div>
                                        </div>

                                        {/* Footer: Доходность + СЧА — прижат вниз (marginTop:auto) у всех плиток */}
                                        <div
                                            style={{
                                                marginTop: 'auto',
                                                paddingTop: 12,
                                                borderTop: '1px solid var(--border-color)',
                                                display: 'flex',
                                                flexDirection: 'column',
                                                gap: 7,
                                            }}
                                        >
                                            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8 }}>
                                                <span style={{ fontSize: 'var(--fs-2xs)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-muted)' }}>
                                                    Доходность · {ret?.period ?? RETURN_PERIOD_LABEL[returnPeriod]}
                                                </span>
                                                <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, fontVariantNumeric: 'tabular-nums', color: returnColor(ret?.v) }}>
                                                    {formatReturnPct(ret?.v)}
                                                </span>
                                            </div>
                                            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8 }}>
                                                <span style={{ fontSize: 'var(--fs-2xs)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-muted)' }}>
                                                    СЧА
                                                </span>
                                                <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, fontVariantNumeric: 'tabular-nums', color: 'var(--text-primary)' }}>
                                                    {f.nav_rub != null ? formatRubShort(f.nav_rub) : '—'}
                                                </span>
                                            </div>
                                        </div>
                                    </button>
                                    );
                                })}
                            </div>
                        </div>
                    ))}
                    </div>{/* /paper-card */}
                </div>
            )}

            {tab === 'portfolio' && (
                <>
                    {/* Единая карточка (макет Claude Design): тулбар с фильтром фондов
                        сверху влияет на оба блока; внутри «Сделки фондов» (уже, слева)
                        и «Состав портфеля» (шире, справа) за вертикальным разделителем.
                        Тумблеры режима и периода живут в шапках самих блоков. */}
                    {/* overflow НЕ hidden: поповер подсказки «?» и Dropdown месяца
                        должны свободно выходить за пределы карточки, не обрезаясь. */}
                    {/* Панель папки: padding:0 — тулбар и сетка идут от края до края
                        со своими отступами (как было у прежней карточки портфеля). */}
                    <div className="editorial-frame has-tabs" style={{ padding: 0 }}>
                        {ukOptions.length > 1 && (
                            <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12, padding: '12px 16px', borderBottom: '1.5px solid var(--border-color)' }}>
                                <UkMultiSelect
                                    options={ukOptions}
                                    selected={portfolioUks}
                                    onChange={setPortfolioUks}
                                    allLabel="Все фонды акций"
                                    size="md"
                                />
                            </div>
                        )}
                        <div style={{ display: 'grid', gridTemplateColumns: vw >= 1120 ? 'minmax(0, 1fr) minmax(0, 1.85fr)' : '1fr', alignItems: 'stretch' }}>
                            <div style={{ padding: '10px 16px 16px', minWidth: 0 }}>
                                <PortfolioMoversPanel
                                    movers={portfolioMovers}
                                    loading={portfolioMoversLoading}
                                    period={portfolioMoversPeriod}
                                    onPeriodChange={setPortfolioMoversPeriod}
                                    range={portfolioMoversRange}
                                    onRangeChange={setPortfolioMoversRange}
                                    availableMonths={portfolioMovers?.available_months}
                                    monthLocked={isMoversRangeMonthLocked}
                                    onMonthLockedClick={() => showUpgrade({ tier: 'basic', featureName: 'свежий срез фондов', indicator: 'fund_trades' })}
                                    variant="embedded"
                                    onAssetClick={openCompanyFlows}
                                />
                            </div>
                            <div style={{ padding: '10px 16px 16px', minWidth: 0, borderLeft: vw >= 1120 ? '1.5px solid var(--border-color)' : 'none', borderTop: vw >= 1120 ? 'none' : '1.5px solid var(--border-color)' }}>
                                <CombinedPortfolioView
                                    portfolio={portfolio}
                                    loading={portfolioLoading}
                                    mode={portfolioMode}
                                    onModeChange={setPortfolioMode}
                                    period={portfolioPeriod}
                                    variant="embedded"
                                    onAssetClick={(h) => { setCompanyPreset({ asset_name: h.asset_name, isin: h.isin }); setTab('company'); }}
                                    availableMonths={portfolio?.available_months}
                                    asOf={portfolioAsOf}
                                    onAsOfChange={setPortfolioAsOf}
                                    monthLocked={isPortfolioMonthLocked}
                                    onMonthLockedClick={() => showUpgrade({ tier: 'basic', featureName: 'свежий срез фондов', indicator: 'fund_trades' })}
                                />
                            </div>
                        </div>
                    </div>
                </>
            )}

            {tab === 'movers' && (
                <>
                    {/* Контролы в единый формат индикаторов (как ряд на «Открытых
                        позициях»): месяц (Dropdown) · фонды (FundPicker) · метрика
                        (SegmentedControl % веса / Объём, руб). */}
                    <div className="flex flex-wrap items-center gap-2 md:gap-3 mb-4 md:mb-6">
                        {movers && movers.available_months.length > 0 && (
                            <Dropdown<string>
                                options={movers.available_months.map((m) => ({
                                    key: m,
                                    label: formatMonthYear(m),
                                    locked: isMoverMonthLocked(m), // свежий месяц → замок (Free/гость)
                                }))}
                                value={asOf ?? defaultMoverMonth ?? movers.available_months[0]}
                                onChange={setAsOf}
                                onLockedClick={() => showUpgrade({
                                    tier: 'basic',
                                    featureName: 'свежий срез фондов',
                                    indicator: 'fund_trades',
                                })}
                                minWidth={150}
                            />
                        )}
                        {moverPickerFunds.length > 1 && (
                            <FundPicker
                                funds={moverPickerFunds}
                                mode="multi"
                                selected={selectedMoverFunds}
                                onChange={setSelectedMoverFunds}
                            />
                        )}
                        <SegmentedControl<'weight' | 'amount'>
                            options={[
                                { key: 'weight', label: '% веса' },
                                { key: 'amount', label: 'Объём, руб' },
                            ]}
                            value={metric}
                            onChange={setMetric}
                        />
                    </div>
                    {loading && !movers && (
                        <div style={{ color: 'var(--text-muted)' }}>Загружаем агрегаты…</div>
                    )}
                    {movers && (
                        <div
                            style={{
                                display: 'grid',
                                gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
                                gap: 16,
                            }}
                        >
                            <MoversColumn
                                title="Топ-аккумуляция"
                                icon={TrendingUp}
                                color="var(--success, #2dd478)"
                                items={movers.top_accumulated.slice(0, 5)}
                                empty="Накоплений нет"
                                metric={metric}
                                onAssetClick={openCompanyFlows}
                            />
                            <MoversColumn
                                title="Топ-распродажа"
                                icon={TrendingDown}
                                color="var(--danger, #ef4444)"
                                items={movers.top_reduced.slice(0, 5)}
                                empty="Распродаж нет"
                                negative
                                metric={metric}
                                onAssetClick={openCompanyFlows}
                            />
                        </div>
                    )}
                    {movers
                        && movers.top_accumulated.length === 0
                        && movers.top_reduced.length === 0 && (
                        <EmptyState message={
                            movers.funds_in_month === 0
                                ? `Для выбранных фондов нет данных за ${formatMonthYear(movers.resolved_month ?? asOf ?? '')}. Снапшот за этот месяц ещё не загружен — выберите другой месяц или дождитесь публикации.`
                                : 'Нет заметных движений между этими месяцами.'
                        } />
                    )}
                </>
            )}

            {tab === 'company' && (
                // Editorial-frame has-tabs — панель папки для «Потоков по компании».
                <div className="editorial-frame has-tabs">
                    <CompanyFlowsTab
                        presetAsset={companyPreset}
                        onPresetConsumed={() => setCompanyPreset(null)}
                        showChartActions
                    />
                </div>
            )}

            {tab === 'snapshots' && <SnapshotReviewTab />}

            </div>{/* /tabbed-card */}

            {selectedTicker && (() => {
                const listFund = funds.find((f) => f.ticker === selectedTicker) ?? null;
                return (
                    <FundDetailModal
                        ticker={selectedTicker}
                        loadDetail={() => getFundTradesDetail(selectedTicker, period)}
                        navRub={listFund?.nav_rub}
                        returns={listFund?.returns}
                        hasDistributions={listFund?.has_distributions}
                        enableDrilldown
                        onClose={() => setSelectedTicker(null)}
                    />
                );
            })()}
        </div>
    );
}

// ════════════════════════════════════════════════════════════════════
// Snapshot Review Tab — обзор каждого снапшота: купил/продал/новые/полностью вышел
// в editorial-стиле «Секторов дня». Сравниваем с предыдущим снапшотом.
// ════════════════════════════════════════════════════════════════════

// SNAPSHOT_TICKERS подгружается динамически из API /funds — туда попадают
// все фонды из whitelist с хотя бы 1 snapshot в fund_holdings_history.

// formatRubShort / formatShares / formatReturnPct / returnColor вынесены в
// components/funds/FundDetailModal (импортируются сверху).

// Лучшая доступная доходность для плитки: длиннейший период с данными (1г→6м→3м→1м).
// Новые фонды (<1 года) не имеют y1 → показываем 6м/3м/1м, чтобы метрика была видна,
// а не «—». На карточке подписываем периодом, чтобы было понятно, за какой срок.
function bestReturn(
    r?: { m1: number | null; m3: number | null; m6: number | null; y1: number | null; y5?: number | null } | null,
): { v: number; period: string } | null {
    if (!r) return null;
    if (r.y5 != null) return { v: r.y5, period: '5 лет' };
    if (r.y1 != null) return { v: r.y1, period: '1 год' };
    if (r.m6 != null) return { v: r.m6, period: '6 мес' };
    if (r.m3 != null) return { v: r.m3, period: '3 мес' };
    if (r.m1 != null) return { v: r.m1, period: '1 мес' };
    return null;
}

// Ключ сортировки карточек «Состав фондов».
type FundSortKey = 'return' | 'volume';

// Период доходности для фильтра/плиток. Маппится на поля FundReturns.
// m3/m6 остаются в типе (детальная модалка/fallback), но в пикере периода
// показываем только 1м / 1г / 5л.
type ReturnPeriodKey = 'm1' | 'm3' | 'm6' | 'y1' | 'y5';
const RETURN_PERIOD_LABEL: Record<ReturnPeriodKey, string> = {
    m1: '1 мес',
    m3: '3 мес',
    m6: '6 мес',
    y1: '1 год',
    y5: '5 лет',
};

// Сырое значение доходности за выбранный период (или null, если нет данных).
function returnForPeriod(
    r: { m1: number | null; m3: number | null; m6: number | null; y1: number | null; y5?: number | null } | null | undefined,
    period: ReturnPeriodKey,
): number | null {
    if (!r) return null;
    return r[period] ?? null;
}

// Доходность для отображения на плитке: выбранный период, а если за него нет
// данных (новый фонд) — fallback на лучший доступный (bestReturn).
function displayReturn(
    r: { m1: number | null; m3: number | null; m6: number | null; y1: number | null; y5?: number | null } | null | undefined,
    period: ReturnPeriodKey,
): { v: number; period: string } | null {
    const v = returnForPeriod(r, period);
    if (v != null) return { v, period: RETURN_PERIOD_LABEL[period] };
    return bestReturn(r);
}

// Стабильный ключ УК для фильтра-мультиселекта: uk_id, иначе имя, иначе ''.
function ukKey(f: { uk_id?: number | string | null; uk?: string | null }): string {
    if (f.uk_id != null && f.uk_id !== '') return String(f.uk_id);
    return f.uk || '';
}

// ISIN-детект для movers.akey (ISIN либо имя бумаги): 12 символов, 2 буквы + 10 алфанумерик.
function isIsin(s: string | null | undefined): s is string {
    return !!s && /^[A-Z]{2}[A-Z0-9]{10}$/.test(s);
}

// Стиль кнопки-фильтра «editorial»-пилюли: border 2px var(--text-primary),
// radius 999, active = accent bg + text-inverse + 3px hard-shadow, inactive =
// bg-secondary. Остался у переключателя метрики в «Обзоре снапшота»; основные
// режимы («Состав фондов», «Сделки фондов») переведены на SegmentedControl.
function filterPillStyle(active: boolean): CSSProperties {
    return {
        padding: '8px 18px',
        background: active ? 'var(--accent)' : 'var(--bg-secondary)',
        color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
        border: '2px solid var(--text-primary)',
        borderRadius: 999,
        fontSize: 'var(--fs-sm)',
        fontWeight: active ? 700 : 600,
        cursor: 'pointer',
        boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
        whiteSpace: 'nowrap',
    };
}

// Сплит-коррекция позиций (splitAdjustPositions/nearestSplitRatio) и
// formatSnapshotDate вынесены в components/funds/FundDetailModal.

// "2026-04-30" → "Апрель 2026" — для month-picker день не показываем
// (у разных УК конец месяца разный: 27/28/30/31), важен только месяц.
function formatMonthYear(iso: string): string {
    const d = new Date(iso);
    const months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
    return `${months[d.getMonth()]} ${d.getFullYear()}`;
}

// Horizontal bar — Editorial-стиль как у «Секторов дня».
// Бар растёт справа от центра (для buys) или слева (для sells).
// Размер = пропорционально |amount| / max_abs_amount среди всех групп.
function EditorialBar({
    label,
    subLabel,
    amount,
    maxAbs,
    isPositive,
    onClick,
    formatValue,
    progress = 1,
}: {
    label: string;
    subLabel?: string;
    amount: number;
    maxAbs: number;
    isPositive: boolean;
    onClick?: () => void;
    formatValue?: (absValue: number) => string;
    progress?: number;
}) {
    const fmt = formatValue ?? ((v: number) => formatRubShort(v));
    const widthPct = maxAbs > 0 ? Math.max(2, Math.abs(amount) / maxAbs * 100) : 2;
    const color = isPositive ? 'var(--mood-green, #4a9959)' : 'var(--mood-red, #b85645)';

    return (
        <div
            onClick={onClick}
            onMouseEnter={onClick ? (e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--accent) 9%, transparent)'; } : undefined}
            onMouseLeave={onClick ? (e) => { e.currentTarget.style.background = 'transparent'; } : undefined}
            style={{
                display: 'grid',
                gridTemplateColumns: '180px 1fr 140px',
                alignItems: 'center',
                gap: 12,
                padding: '8px 8px',
                margin: '0 -8px',
                borderRadius: 6,
                cursor: onClick ? 'pointer' : 'default',
                borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.06))',
                transition: 'background-color 120ms',
            }}
        >
            <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)', fontWeight: 500 }}>
                <div>{label}</div>
                {subLabel && (
                    <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-tertiary)', marginTop: 2 }}>
                        {subLabel}
                    </div>
                )}
            </div>
            <div style={{ height: 22, position: 'relative' }}>
                <div
                    style={{
                        width: `${widthPct * progress}%`,
                        height: '100%',
                        background: color,
                        borderRadius: 2,
                    }}
                />
            </div>
            <div
                style={{
                    fontSize: 'var(--fs-sm)',
                    textAlign: 'right',
                    color,
                    fontWeight: 700,
                    fontVariantNumeric: 'tabular-nums',
                }}
            >
                {isPositive ? '+' : '−'}{fmt(Math.abs(amount))}
            </div>
        </div>
    );
}

function SnapshotReviewTab() {
    const [availableFunds, setAvailableFunds] = useState<FundWithHistory[]>([]);
    const [ticker, setTicker] = usePersistedState<string>('frame:fundtrades:snapTicker', 'EQMX');
    const [snapshotsList, setSnapshotsList] = useState<FundSnapshotsList | null>(null);
    const [selectedDate, setSelectedDate] = useState<string | null>(null);
    const [review, setReview] = useState<FundSnapshotReview | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [drillDown, setDrillDown] = useState<{ asset_name: string; isin: string | null } | null>(null);

    // Load list of funds with history once
    useEffect(() => {
        listFundsWithHistory()
            .then((data) => setAvailableFunds(data.funds.filter(f => (f.snapshot_count || 0) > 0)))
            .catch(() => {});
    }, []);

    // Load snapshots list when ticker changes
    useEffect(() => {
        let cancel = false;
        setLoading(true);
        setError(null);
        setReview(null); // очистить прошлый фонд — не мигать чужими данными при смене тикера
        getFundSnapshots(ticker)
            .then((data) => {
                if (cancel) return;
                setSnapshotsList(data);
                // Дефолт = последний ДОСТУПНЫЙ тиру срез (не locked) → сразу реальные данные,
                // а не замок. Free/гость: свежайший locked → берём первый не-locked. Платные:
                // не-locked = снапшот[0] (свежайший). Так «по умолчанию выбран актуальный срез».
                if (data.snapshots.length > 0) {
                    const firstAvailable = data.snapshots.find((s) => !s.locked) ?? data.snapshots[0];
                    setSelectedDate(firstAvailable.snapshot_date);
                } else {
                    setSelectedDate(null);
                    setReview(null);
                }
            })
            .catch((e) => !cancel && setError(e.message))
            .finally(() => !cancel && setLoading(false));
        return () => { cancel = true; };
    }, [ticker]);

    // Load review when selectedDate changes
    useEffect(() => {
        if (!selectedDate) return;
        let cancel = false;
        setLoading(true);
        setError(null);
        getFundSnapshotReview(ticker, selectedDate)
            .then((data) => !cancel && setReview(data))
            .catch((e) => !cancel && setError(e.message))
            .finally(() => !cancel && setLoading(false));
        return () => { cancel = true; };
    }, [ticker, selectedDate]);

    const maxAbsAmount = useMemo(() => {
        if (!review) return 0;
        const allBars = [
            ...review.added.map(r => r.delta_amount_rub || 0),
            ...review.reduced.map(r => Math.abs(r.delta_amount_rub || 0)),
            ...review.new.map(r => r.curr_amount_rub || 0),
            ...review.sold_out.map(r => r.prev_amount_rub || 0),
        ];
        return Math.max(...allBars, 1);
    }, [review]);

    // ITEM 5 — единый FundPicker (single): иерархия УК → фонд. Заменяет прежние
    // UkMultiSelect-фильтр + Dropdown фонда. Группировка по УК — внутри FundPicker.
    const pickerFunds = useMemo<FundPickerFund[]>(
        () => availableFunds.map(f => ({ ticker: f.ticker, name: f.name, uk: f.uk, uk_id: f.uk_id })),
        [availableFunds],
    );
    const pickerSelected = useMemo(() => new Set([ticker]), [ticker]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
            {/* Ticker selector — единый FundPicker (single): иерархия УК → фонд.
                Кнопка сама показывает аватар УК + «тикер · имя» — отдельная подпись «Фонд:»
                и суффикс с именем УК убраны как избыточные. */}
            <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
                <FundPicker
                    funds={pickerFunds}
                    mode="single"
                    selected={pickerSelected}
                    onChange={(next) => {
                        const t = next.values().next().value as string | undefined;
                        if (t) setTicker(t);
                    }}
                    minWidth={280}
                />
            </div>

            {/* Snapshots timeline navigation */}
            {snapshotsList && snapshotsList.snapshots.length > 0 && (
                <div
                    style={{
                        display: 'flex',
                        gap: 8,
                        overflowX: 'auto',
                        padding: '12px 2px 16px',
                        borderTop: '1px solid var(--border-color)',
                        borderBottom: '1px solid var(--border-color)',
                    }}
                >
                    {snapshotsList.snapshots.map((s) => {
                        const active = s.snapshot_date === selectedDate;
                        return (
                            <button
                                key={s.snapshot_date}
                                onClick={() => setSelectedDate(s.snapshot_date)}
                                title={s.locked
                                    ? `${s.snapshot_date} · свежий срез — по подписке`
                                    : `${s.snapshot_date} · ${s.asset_count} активов`}
                                className="editorial-press"
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 5,
                                    padding: '6px 14px',
                                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: active ? 'var(--text-inverse)' : (s.locked ? 'var(--text-tertiary)' : 'var(--text-secondary)'),
                                    border: '2px solid var(--text-primary)',
                                    borderStyle: s.locked && !active ? 'dashed' : 'solid',
                                    fontSize: 'var(--fs-xs)',
                                    fontWeight: active ? 700 : 600,
                                    fontVariantNumeric: 'tabular-nums',
                                    cursor: 'pointer',
                                    whiteSpace: 'nowrap',
                                    flexShrink: 0,
                                    borderRadius: 999,
                                    boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
                                }}
                            >
                                {formatMonthYear(s.snapshot_date)}
                                {s.locked && <Lock size={11} strokeWidth={2.4} />}
                            </button>
                        );
                    })}
                </div>
            )}

            {loading && (
                <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-tertiary)' }}>
                    Загрузка...
                </div>
            )}
            {error && (
                <div style={{ padding: 16, background: 'var(--bg-secondary)', color: 'var(--mood-red)' }}>
                    {error}
                </div>
            )}

            {/* Review sections (locked свежий срез → тизер с блюром + ваш upgrade-модал) */}
            {!loading && review && (
                review.locked
                    ? <LockedSnapshotTeaser latestDate={review.latest_snapshot_date} requiredTier={review.required_tier} />
                    : <SnapshotReviewBody
                        review={review}
                        maxAbsAmount={maxAbsAmount}
                        onRowClick={(r) => setDrillDown({ asset_name: r.asset_name, isin: r.isin })}
                    />
            )}

            {!loading && !error && snapshotsList && snapshotsList.snapshots.length === 0 && (
                <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>
                    У {ticker} пока нет исторических снапшотов SCHA. Данные накапливаются с каждым месяцем.
                </div>
            )}

            {/* Drill-down modal */}
            {drillDown && (
                <AssetHistoryModal
                    ticker={ticker}
                    asset_name={drillDown.asset_name}
                    isin={drillDown.isin}
                    onClose={() => setDrillDown(null)}
                />
            )}
        </div>
    );
}

function SnapshotReviewBody({
    review,
    maxAbsAmount,
    onRowClick,
}: {
    review: FundSnapshotReview;
    maxAbsAmount: number;
    onRowClick: (r: FundDiffRow) => void;
}) {
    // Переключатель метрики (как в «Сделки фондов»): сортировка/бары по объёму ₽ или по доле.
    const [metric, setMetric] = usePersistedState<'amount' | 'weight'>('frame:fundtrades:snapMetric', 'amount');
    const isW = metric === 'weight';
    // value-getters: ₽ (delta/curr/prev amount) и вес (Δдоли / curr / prev).
    const wDelta = (r: FundDiffRow) => (r.curr_weight ?? 0) - (r.prev_weight ?? 0);
    const aAdded = (r: FundDiffRow) => r.delta_amount_rub ?? 0;
    const aNew = (r: FundDiffRow) => r.curr_amount_rub ?? 0;
    const aSold = (r: FundDiffRow) => -(r.prev_amount_rub ?? 0);
    const wNew = (r: FundDiffRow) => r.curr_weight ?? 0;
    const wSold = (r: FundDiffRow) => -(r.prev_weight ?? 0);
    const maxAbsWeight = useMemo(() => Math.max(
        0.01,
        ...review.added.map((r) => Math.abs(wDelta(r))),
        ...review.reduced.map((r) => Math.abs(wDelta(r))),
        ...review.new.map((r) => r.curr_weight ?? 0),
        ...review.sold_out.map((r) => r.prev_weight ?? 0),
    ), [review]);
    const maxAbs = isW ? maxAbsWeight : maxAbsAmount;
    const fmtVal = isW ? (v: number) => `${v.toFixed(2)}%` : (v: number) => formatRubShort(v);
    const sortByAbs = (items: FundDiffRow[], get: (r: FundDiffRow) => number) =>
        [...items].sort((a, b) => Math.abs(get(b)) - Math.abs(get(a)));
    const sumBy = (items: FundDiffRow[], get: (r: FundDiffRow) => number) =>
        items.reduce((s, r) => s + get(r), 0);
    // В режиме «% веса» докупил/продал бакетим по знаку Δдоли (а не по штукам):
    // иначе бумага с РОСШЕЙ долей, но проданная в штуках (напр. после сплита T —
    // доля +0.46%, но штук меньше), попала бы в «продал» с неверным знаком.
    const addedItems = isW
        ? [...review.added, ...review.reduced].filter((r) => wDelta(r) > 0)
        : review.added;
    const reducedItems = isW
        ? [...review.added, ...review.reduced].filter((r) => wDelta(r) < 0)
        : review.reduced;

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 32 }}>
            {/* Header: fund + dates */}
            <div>
                <h3
                    style={{
                        fontFamily: 'var(--font-serif, Georgia, serif)',
                        fontSize: 'var(--fs-2xl)',
                        margin: 0,
                        marginBottom: 8,
                        color: 'var(--text-primary)',
                    }}
                >
                    {review.fund.name}
                </h3>
                <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                    Снапшот <strong style={{ color: 'var(--text-primary)' }}>
                        {formatMonthYear(review.current_snapshot_date)}
                    </strong>
                    {review.previous_snapshot_date && (
                        <> · сравниваем с {formatMonthYear(review.previous_snapshot_date)}</>
                    )}
                    {' · '}{review.totals!.current_assets} активов
                </div>
            </div>

            {/* Нет предыдущего снапшота → показываем состав на эту дату */}
            {!review.previous_snapshot_date && (
                <div>
                    <div style={{
                        padding: 14,
                        marginBottom: 14,
                        background: 'var(--bg-secondary)',
                        borderRadius: 8,
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--text-secondary)',
                    }}>
                        Самый ранний снапшот — сравнивать не с чем. Состав фонда на эту дату:
                    </div>
                    {review.current_holdings.map((h) => (
                        <div
                            key={h.isin || h.asset_name}
                            style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'baseline',
                                gap: 12,
                                padding: '7px 0',
                                borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.06))',
                            }}
                        >
                            <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>{fundAssetName(h.asset_name, h.isin)}</span>
                            <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', fontVariantNumeric: 'tabular-nums', flexShrink: 0 }}>
                                {h.weight != null ? `${h.weight.toFixed(2)}%` : '—'}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {/* Переключатель метрики (как в «Сделки фондов») */}
            {review.previous_snapshot_date && (
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: -16, marginBottom: -8 }}>
                    {([['amount', 'Объём, руб'], ['weight', '% веса']] as const).map(([key, lbl]) => {
                        const on = metric === key;
                        return (
                            <button
                                key={key}
                                onClick={() => setMetric(key)}
                                className="editorial-press"
                                style={filterPillStyle(on)}
                            >
                                {lbl}
                            </button>
                        );
                    })}
                </div>
            )}

            {/* ДОКУПИЛ */}
            {addedItems.length > 0 && (
                <SnapshotSection
                    title="ДОКУПИЛ"
                    count={addedItems.length}
                    total={isW ? sumBy(addedItems, wDelta) : review.totals!.total_added_rub}
                    items={sortByAbs(addedItems, isW ? wDelta : aAdded)}
                    maxAbs={maxAbs}
                    isPositive={true}
                    valueGetter={isW ? wDelta : aAdded}
                    formatValue={fmtVal}
                    subLabelGetter={(r) =>
                        `+${formatShares(r.delta_positions || 0)} шт` +
                        (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')
                    }
                    onItemClick={onRowClick}
                />
            )}

            {/* ПРОДАЛ */}
            {reducedItems.length > 0 && (
                <SnapshotSection
                    title="ПРОДАЛ"
                    count={reducedItems.length}
                    total={isW ? Math.abs(sumBy(reducedItems, wDelta)) : Math.abs(review.totals!.total_reduced_rub)}
                    items={sortByAbs(reducedItems, isW ? wDelta : aAdded)}
                    maxAbs={maxAbs}
                    isPositive={false}
                    valueGetter={isW ? wDelta : aAdded}
                    formatValue={fmtVal}
                    subLabelGetter={(r) =>
                        `${formatShares(r.delta_positions || 0)} шт` +
                        (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')
                    }
                    onItemClick={onRowClick}
                />
            )}

            {/* НОВЫЕ ПОЗИЦИИ */}
            {review.new.length > 0 && (
                <SnapshotSection
                    title="НОВЫЕ ПОЗИЦИИ"
                    count={review.new.length}
                    total={isW ? sumBy(review.new, wNew) : review.totals!.total_new_rub}
                    items={sortByAbs(review.new, isW ? wNew : aNew)}
                    maxAbs={maxAbs}
                    isPositive={true}
                    valueGetter={isW ? wNew : aNew}
                    formatValue={fmtVal}
                    subLabelGetter={(r) =>
                        `${formatShares(r.curr_positions)} шт` +
                        (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')
                    }
                    onItemClick={onRowClick}
                />
            )}

            {/* ПОЛНОСТЬЮ ВЫШЕЛ */}
            {review.sold_out.length > 0 && (
                <SnapshotSection
                    title="ПОЛНОСТЬЮ ВЫШЕЛ"
                    count={review.sold_out.length}
                    total={isW ? Math.abs(sumBy(review.sold_out, wSold)) : review.totals!.total_sold_out_rub}
                    items={sortByAbs(review.sold_out, isW ? wSold : aSold)}
                    maxAbs={maxAbs}
                    isPositive={false}
                    valueGetter={isW ? wSold : aSold}
                    formatValue={fmtVal}
                    subLabelGetter={(r) =>
                        `было ${formatShares(r.prev_positions)} шт`
                    }
                    onItemClick={onRowClick}
                />
            )}

            {/* Если без изменений */}
            {review.previous_snapshot_date &&
              review.added.length === 0 &&
              review.reduced.length === 0 &&
              review.new.length === 0 &&
              review.sold_out.length === 0 && (
                <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>
                    Состав не изменился между снапшотами.
                </div>
            )}
        </div>
    );
}

function SnapshotSection({
    title,
    count,
    total,
    items,
    maxAbs,
    isPositive,
    valueGetter,
    subLabelGetter,
    onItemClick,
    formatValue,
}: {
    title: string;
    count: number;
    total: number;
    items: FundDiffRow[];
    maxAbs: number;
    isPositive: boolean;
    valueGetter: (r: FundDiffRow) => number;
    subLabelGetter: (r: FundDiffRow) => string;
    onItemClick?: (r: FundDiffRow) => void;
    formatValue?: (absValue: number) => string;
}) {
    const fmt = formatValue ?? ((v: number) => formatRubShort(v));
    const [expanded, setExpanded] = useState(false);
    const displayed = expanded ? items : items.slice(0, 3);
    // Entrance-волна баров: перезапуск при смене состава секции (новый снапшот/
    // фонд/метрика меняют items+maxAbs) и при разворачивании (новые строки).
    const reveal = useGrowReveal(
        displayed.length,
        `${title}|${displayed.length}|${items[0]?.asset_name ?? ''}|${maxAbs.toFixed(0)}`,
    );
    return (
        <div>
            <div
                style={{
                    display: 'flex',
                    alignItems: 'baseline',
                    justifyContent: 'space-between',
                    marginBottom: 12,
                    borderBottom: '1.5px solid var(--text-primary)',
                    paddingBottom: 6,
                }}
            >
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
                    <span
                        style={{
                            fontSize: 'var(--fs-xs)',
                            fontWeight: 700,
                            letterSpacing: '0.08em',
                            color: 'var(--text-primary)',
                        }}
                    >
                        {title}
                    </span>
                    <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                        {count} {count === 1 ? 'позиция' : count < 5 ? 'позиции' : 'позиций'}
                    </span>
                </div>
                <span
                    style={{
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 700,
                        color: isPositive ? 'var(--mood-green)' : 'var(--mood-red)',
                        fontVariantNumeric: 'tabular-nums',
                    }}
                >
                    {isPositive ? '+' : '−'}{fmt(Math.abs(total))}
                </span>
            </div>
            <div>
                {displayed.map((r, i) => (
                    <EditorialBar
                        key={`${r.asset_name}-${r.isin || ''}`}
                        label={fundAssetName(r.asset_name, r.isin)}
                        subLabel={subLabelGetter(r)}
                        amount={valueGetter(r)}
                        maxAbs={maxAbs}
                        isPositive={isPositive}
                        onClick={onItemClick ? () => onItemClick(r) : undefined}
                        formatValue={formatValue}
                        progress={reveal[i] ?? 1}
                    />
                ))}
                {items.length > 3 && (
                    <button
                        onClick={() => setExpanded((e) => !e)}
                        className="editorial-press"
                        style={{
                            marginTop: 10,
                            padding: '5px 14px',
                            background: 'var(--bg-secondary)',
                            border: '1.5px solid var(--border-color)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-xs)',
                            fontWeight: 600,
                            color: 'var(--text-secondary)',
                            cursor: 'pointer',
                        }}
                    >
                        {expanded ? '↑ Свернуть' : `Показать все · ${items.length} ↓`}
                    </button>
                )}
            </div>
        </div>
    );
}


// Лого бумаги в строке movers: ISIN → каноничный тикер → InstrumentIcon
// (STOCK_LOGO_OVERRIDE → стикерпак → /logos/<тикер>.png). Как в CompanyFlowsTab.
// ОФЗ → герб Минфина (raw_152); иначе (корпоблигация без тикера) → цветная точка.
function MoverAssetMark({ name, isin, size = 22 }: { name: string; isin?: string | null; size?: number }) {
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

function MoversColumn({
    title,
    icon: Icon,
    color,
    items,
    empty,
    negative,
    metric,
    onAssetClick,
}: {
    title: string;
    icon: typeof TrendingUp;
    color: string;
    items: FundTradesMovers['top_accumulated'];
    empty: string;
    negative?: boolean;
    metric: 'weight' | 'amount';
    // ITEM 2 — клик по строке актива открывает «Потоки по компании».
    onAssetClick?: (m: FundTradesMovers['top_accumulated'][number]) => void;
}) {
    // Значение по выбранной метрике: % веса (Δвеса) или объём ₽ (Δсуммы).
    const valOf = (m: FundTradesMovers['top_accumulated'][number]) =>
        metric === 'amount' ? m.total_delta_amount : m.total_delta_weight;
    const fmtVal = (v: number) => metric === 'amount'
        ? `${v > 0 ? '+' : '−'}${formatRubShort(Math.abs(v))}`
        : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
    // Гистограмма: ширина бара ∝ |значение| относительно максимума в колонке.
    const maxAbs = Math.max(...items.map((m) => Math.abs(valOf(m))), 0.0001);
    // Entrance-волна баров: перезапуск при смене данных/метрики/набора фондов.
    const reveal = useGrowReveal(
        items.length,
        `${title}|${metric}|${items.length}|${items[0]?.akey ?? ''}|${maxAbs.toFixed(0)}`,
    );
    return (
        <div
            style={{
                background: 'var(--bg-secondary)',
                border: '1.5px solid var(--border-color)',
                borderRadius: 12,
                padding: 16,
                boxShadow: 'var(--shadow-hard-chip)',
            }}
        >
            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    marginBottom: 12,
                    paddingBottom: 10,
                    borderBottom: '1px solid var(--border-color)',
                }}
            >
                <Icon size={18} color="var(--text-primary)" strokeWidth={2.2} />
                <h3
                    style={{
                        fontSize: 'var(--fs-md)',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                        margin: 0,
                    }}
                >
                    {title}
                </h3>
            </div>
            {items.length === 0 ? (
                <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>{empty}</p>
            ) : (
                <div>
                    {items.map((m, i) => {
                        const val = valOf(m);
                        const pct = Math.max(2, (Math.abs(val) / maxAbs) * 100);
                        const clickable = !!onAssetClick;
                        // Каноничное имя (формат Сезонности) по ISIN из akey.
                        const mIsin = isIsin(m.akey) ? m.akey : null;
                        const mName = fundAssetName(m.asset_name, mIsin);
                        return (
                        <div
                            key={m.akey}
                            onClick={clickable ? () => onAssetClick!(m) : undefined}
                            onKeyDown={clickable
                                ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onAssetClick!(m); } }
                                : undefined}
                            role={clickable ? 'button' : undefined}
                            tabIndex={clickable ? 0 : undefined}
                            title={clickable ? `По бумаге: ${mName}` : undefined}
                            onMouseEnter={clickable ? (e) => { e.currentTarget.style.background = 'color-mix(in srgb, var(--text-primary) 5%, transparent)'; } : undefined}
                            onMouseLeave={clickable ? (e) => { e.currentTarget.style.background = 'transparent'; } : undefined}
                            style={{
                                padding: clickable ? '9px 8px' : '9px 0',
                                margin: clickable ? '0 -8px' : 0,
                                borderRadius: clickable ? 8 : 0,
                                cursor: clickable ? 'pointer' : 'default',
                                transition: 'background 0.12s ease',
                                borderBottom: i === items.length - 1
                                    ? 'none'
                                    : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                            }}
                        >
                            {/* Верх: ранг + лого + имя + значение (% веса или ₽) */}
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <span
                                    style={{
                                        fontSize: 'var(--fs-xs)',
                                        color: 'var(--text-muted)',
                                        fontFamily: 'ui-monospace, monospace',
                                        flexShrink: 0,
                                    }}
                                >
                                    {i + 1}.
                                </span>
                                <MoverAssetMark name={m.asset_name} isin={mIsin} size={22} />
                                <span
                                    style={{
                                        flex: 1,
                                        minWidth: 0,
                                        fontSize: 'var(--fs-sm)',
                                        color: 'var(--text-primary)',
                                        fontWeight: 500,
                                        lineHeight: 1.3,
                                        overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    {mName}
                                </span>
                                <span
                                    style={{
                                        fontFamily: 'ui-monospace, "SF Mono", monospace',
                                        fontSize: 'var(--fs-sm)',
                                        fontWeight: 700,
                                        color,
                                        flexShrink: 0,
                                    }}
                                >
                                    {fmtVal(val)}
                                </span>
                            </div>
                            {/* Низ: гистограмма-бар + счётчик фондов */}
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 5 }}>
                                <div
                                    style={{
                                        flex: 1,
                                        height: 6,
                                        background: 'color-mix(in srgb, var(--text-primary) 8%, transparent)',
                                        borderRadius: 3,
                                        overflow: 'hidden',
                                    }}
                                >
                                    <div style={{ width: `${pct * (reveal[i] ?? 1)}%`, height: '100%', background: 'color-mix(in srgb, var(--text-primary) 30%, transparent)', borderRadius: 3 }} />
                                </div>
                                <span
                                    style={{
                                        fontSize: 'var(--fs-2xs)',
                                        color: 'var(--text-muted)',
                                        flexShrink: 0,
                                        minWidth: 80,
                                        textAlign: 'right',
                                    }}
                                >
                                    {negative
                                        ? `${m.funds_selling} продают`
                                        : `${m.funds_buying} покупают`}
                                </span>
                            </div>
                        </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

function EmptyState({ message }: { message: string }) {
    return (
        <div
            style={{
                padding: 32,
                textAlign: 'center',
                background: 'var(--bg-secondary)',
                border: '1.5px dashed var(--border-color)',
                borderRadius: 12,
            }}
        >
            <Activity size={28} style={{ color: 'var(--text-muted)', margin: '0 auto 10px' }} />
            <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', maxWidth: 420, margin: '0 auto', lineHeight: 1.5 }}>
                {message}
            </p>
        </div>
    );
}
