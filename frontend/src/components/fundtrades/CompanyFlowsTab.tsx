/**
 * CompanyFlowsTab — раздел «Потоки по компании».
 *
 * Выбор бумаги (таблетка-поиск в стиле «Сезонности») → её помесячные потоки
 * (Δ стоимости позиции) по фондам, что её держат. Два режима (SegmentedControl):
 *  - «Гистограмма» — CompanyFlowsHistogram, оформленный 1-в-1 как «Деньги в
 *    фондах»: бары рисуют ЧИСТЫЙ поток (сумму по выбранным фондам, приток
 *    зелёный / отток красный), тултип раскрывает разбивку по фондам.
 *  - «Карта сделок» — CompanyFlowsPriceMap: недельная линия цены акции с
 *    кругляшами месячных нетто-сделок (площадь ∝ |нетто|) — видно, на каких
 *    уровнях цены фонды покупали и продавали. Цена — /price-weekly по тикеру
 *    (resolveFundTicker); нет тикера или истории → empty-state внутри чарта.
 *
 * Цвет фонда: UK_LOGOS[String(uk_id)]?.bg, иначе DONUT_COLORS[idx % len].
 * Значения приходят в ₽ → CompanyFlowsHistogram переводит в млн (÷1e6).
 *
 * Контрактные импорты из services/api: listFundTradeAssets, getCompanyFlows,
 * типы FundTradeAsset, CompanyFlowsResponse. Их добавляет бэкенд-агент по
 * общему контракту — здесь импортируем строго по контрактным именам.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, TrendingUp } from 'lucide-react';
import { UK_LOGOS, DONUT_COLORS, resolveFundTicker, fundAssetName, fundAssetColor, isOfzBond } from '../../config/fundConfig';
import {
    listFundTradeAssets,
    listFundsWithHistory,
    getCompanyFlows,
    getCompanyPriceWeekly,
    type FundTradeAsset,
    type CompanyFlowsResponse,
    type CompanyPriceWeeklyResponse,
    type FundWithHistory,
} from '../../services/api';
import InstrumentIcon from '../InstrumentIcon';
import Skeleton from '../Skeleton';
import CompanyFlowsHistogram, { type CompanyFlowsSeries } from './CompanyFlowsHistogram';
import CompanyFlowsPriceMap from './CompanyFlowsPriceMap';
import { useFitToViewport } from '../../hooks/useFitToViewport';
import AssetPickerModal from './AssetPickerModal';
import PortfolioFundPicker, { defaultPortfolioTickers } from './PortfolioFundPicker';
import SegmentedControl from '../SegmentedControl';
import ChartActionsMenu from '../ChartActionsMenu';
import ChartCaptureButton from '../export/ChartCaptureButton';
import ChartSettings from '../chart/ChartSettings';
import { usePersistedState, usePersistedSet } from '../../hooks/usePersistedState';

type Metric = 'amount' | 'weight';

// Период графика — окно последних N месяцев. «Всё» = вся доступная история
// (левый пустой хвост всё равно обрезает сам CompanyFlowsHistogram).
type Period = '1y' | '3y' | 'all';
const PERIOD_LABELS: Record<Period, string> = { '1y': '1 год', '3y': '3 года', 'all': 'Всё' };
const CF_PERIODS: Period[] = ['1y', '3y', 'all'];
const PERIOD_MONTHS: Record<Period, number | null> = { '1y': 12, '3y': 36, 'all': null };

// Режим отображения: гистограмма потоков / сделки на графике цены.
type ChartMode = 'hist' | 'map';
const MODE_LABELS: Record<ChartMode, string> = { hist: 'Гистограмма', map: 'Карта сделок' };
const CF_MODES: ChartMode[] = ['hist', 'map'];

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

// Дефолт-выбор фондов для свежей бумаги — тот же, что в «Общем портфеле»: все
// держатели бумаги, кроме индексных фондов. Индексные почти идентичны друг другу,
// их движения это ребалансировка вслед за индексом, а не решения управляющих.
// Тумблер «Без индексных фондов» в пикере возвращает их обратно.
// Если индексных держателей нет вовсе — отдаём пусто (= «все»), чтобы не
// изображать частичный выбор там, где выбраны все.
function defaultFunds(funds: FundWithHistory[]): Set<string> {
    const tickers = defaultPortfolioTickers(funds);
    if (tickers.length === funds.length) return new Set();
    return new Set(tickers);
}

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
}

export default function CompanyFlowsTab({ presetAsset, onPresetConsumed, showChartActions = false }: CompanyFlowsTabProps = {}) {
    // Высота графика «под экран» — anchor на обёртке чарта (как в «Деньги в фондах»).
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const chartHeight = useFitToViewport(chartAnchorRef, { min: 360, max: 720, bottomBuffer: 64 });
    const [assets, setAssets] = useState<FundTradeAsset[]>([]);
    const [assetsLoading, setAssetsLoading] = useState(true);
    const [assetsError, setAssetsError] = useState<string | null>(null);
    // Карточки фондов (СЧА, доходность, подкатегория) — их показывает пикер
    // фондов, тот же, что в «Общем портфеле». Универсум совпадает: и /funds, и
    // /company-flows отдают только whitelist-фонды акций. Ошибку глотаем: без
    // метаданных пикер всё равно соберётся из flows.funds (см. pickerFunds).
    const [fundsMeta, setFundsMeta] = useState<FundWithHistory[]>([]);

    const [selectedKey, setSelectedKey] = useState<string | null>(null);
    const [flows, setFlows] = useState<CompanyFlowsResponse | null>(null);
    const [flowsLoading, setFlowsLoading] = useState(false);
    const [flowsError, setFlowsError] = useState<string | null>(null);

    // ITEM 3 — открыта ли модалка выбора бумаги.
    const [pickerOpen, setPickerOpen] = useState(false);

    // ITEM 2 — выбранные КОНКРЕТНЫЕ фонды (ключ = ticker). Пусто = все фонды.
    // Персистится и переживает смену бумаги/перезагрузку. До первого ручного
    // выбора действует авто-дефолт (все держатели, кроме индексных — пере-выбор
    // на каждую бумагу, см. эффект ниже); как только пользователь сам трогает
    // пикер, его выбор замораживается за ним (включая «Все фонды» = пустой Set).
    const [selectedFunds, setSelectedFunds] = usePersistedSet<string>('frame:companyflows:funds');
    const [fundsTouched, setFundsTouched] = usePersistedState<boolean>('frame:companyflows:funds-touched', false);
    // Ручной выбор в пикере: помечаем «тронуто» и сохраняем набор (персист-сеттер
    // сам пишет в localStorage). Авто-дефолт в эффекте зовёт setSelectedFunds
    // напрямую и «тронуто» НЕ ставит.
    const handleFundsChange = useCallback((next: Set<string>) => {
        setFundsTouched(true);
        setSelectedFunds(next);
    }, [setFundsTouched, setSelectedFunds]);

    // metric toggle — default ₽ (amount).
    const [metric] = useState<Metric>('amount');

    // Период графика (окно последних N месяцев). Персист между сессиями,
    // дефолт «Всё» — сохраняем прежнее поведение (показывали всю историю).
    const [period, setPeriod] = usePersistedState<Period>('frame:companyflows:period', 'all');

    // Режим отображения (гистограмма / карта сделок). Персист между сессиями.
    const [mode, setMode] = usePersistedState<ChartMode>('frame:companyflows:mode', 'hist');

    // Недельная цена для «Карты сделок» — грузим лениво, только в режиме map.
    // priceError: NO_PRICE_HISTORY (404 / нет тикера) → empty-state в чарте,
    // прочее → красная плашка (как flowsError).
    const [price, setPrice] = useState<CompanyPriceWeeklyResponse | null>(null);
    const [priceLoading, setPriceLoading] = useState(false);
    const [priceError, setPriceError] = useState<string | null>(null);

    // Загрузка списка бумаг → выбрать первую (топ по funds_count), если нет
    // pending-preset (presetAsset выбирается отдельным эффектом и имеет приоритет).
    useEffect(() => {
        let cancelled = false;
        setAssetsLoading(true);
        listFundTradeAssets()
            .then(resp => {
                if (cancelled) return;
                setAssets(resp.assets);
                if (resp.assets.length > 0 && !presetAsset) setSelectedKey(resp.assets[0].key);
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

    // Цена для «Карты сделок»: грузим при входе в режим / смене бумаги. Кэш —
    // сам price (тикер совпал → не перезапрашиваем). Нет тикера (облигация,
    // ОФЗ, денежный рынок) → сразу «нет истории», без похода на бэкенд.
    useEffect(() => {
        if (mode !== 'map') return;
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

    // Авто-дефолт набора фондов — пока пользователь сам не выбирал. Пере-выбор на
    // каждую бумагу (у каждой свои держатели) и повторно, когда доедут карточки
    // фондов: без них не видно, кто индексный. Тронул пикер → его выбор переживает
    // смену бумаги, дефолт больше не навязываем.
    useEffect(() => {
        if (fundsTouched || pickerFunds.length === 0) return;
        setSelectedFunds(defaultFunds(pickerFunds));
    }, [pickerFunds, fundsTouched, setSelectedFunds]);

    // Персист-набор общий на все бумаги, поэтому в нём остаются тикеры фондов,
    // которые ЭТУ бумагу не держат. Работаем с пересечением: пикер не пишет
    // «5 из 3 фондов», а бумага, по которой не выбран ни один сохранённый фонд,
    // показывает всех держателей, а не пустой график.
    const effectiveFunds = useMemo(() => {
        if (selectedFunds.size === 0 || !flows) return new Set<string>();
        const holders = new Set(flows.funds.map(f => f.ticker));
        return new Set([...selectedFunds].filter(t => holders.has(t)));
    }, [selectedFunds, flows]);

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
            .filter(x => effectiveFunds.size === 0 || effectiveFunds.has(x.ticker))
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

    // Триггер entrance-волны (бары / кругляши): перезапуск при смене бумаги,
    // набора фондов, периода ИЛИ режима — новое окно проигрывает каскад заново.
    const animTrigger = `${selectedAsset?.key ?? ''}|${[...effectiveFunds].sort().join(',')}|${period}|${mode}`;

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

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-4)' }}>
            {/* Контролы: таблетка поиска бумаги (стиль «Сезонности») + фильтр фондов.
                Раскладка кнопок повторяет «Деньги в фондах» — горизонтальный ряд. */}
            <div
                style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    alignItems: 'center',
                    gap: 'var(--sp-2)',
                }}
            >
                {/* Таблетка бумаги — widget-flat (icon + имя + тикер + ▾),
                    1-в-1 как селектор актива на «Сезонности» (под именем — тикер).
                    Открывает строковый поиск (AssetPickerModal — то же оформление,
                    что и InstrumentSearchModal). */}
                <button
                    type="button"
                    onClick={() => setPickerOpen(true)}
                    title={selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined}
                    className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
                    style={{
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
                        ? <AssetMark name={selectedAsset.asset_name} isin={selectedAsset.isin} size={28} />
                        : <span style={{ width: 28, height: 28, flexShrink: 0 }} />}
                    <div className="flex-1 text-left" style={{ minWidth: 0 }}>
                        <div
                            className="font-medium"
                            style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                        >
                            {selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : 'Выберите бумагу'}
                        </div>
                        {selectedAsset && (
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
                />

                {/* Период — окно последних N месяцев (1 год / 3 года / Всё). */}
                <SegmentedControl<Period>
                    options={CF_PERIODS.map(p => ({ key: p, label: PERIOD_LABELS[p] }))}
                    value={period}
                    onChange={setPeriod}
                />

                {/* Режим: гистограмма потоков / сделки на графике цены. */}
                <SegmentedControl<ChartMode>
                    options={CF_MODES.map(m => ({ key: m, label: MODE_LABELS[m] }))}
                    value={mode}
                    onChange={setMode}
                />

                {/* Скриншот + Настройки — kebab «⋮» в углу графика (как в «Деньги в
                    фондах»). JSX живёт тут, рядом со state, а DOM через portal уезжает
                    в chartAnchorRef (position:relative). */}
                {showChartActions && (
                    <ChartActionsMenu containerRef={chartAnchorRef}>
                        <ChartCaptureButton
                            getTargetElement={() => chartAnchorRef.current}
                            filename={`frame-company-flows-${mode}-${selectedTicker ?? selectedAsset?.key ?? 'asset'}-${period}`}
                            metadata={{
                                title: 'Сделки фондов',
                                asset: selectedAsset ? fundAssetName(selectedAsset.asset_name, selectedAsset.isin) : undefined,
                                ticker: selectedTicker,
                                details: [
                                    PERIOD_LABELS[period],
                                    effectiveFunds.size > 0
                                        ? `${effectiveFunds.size} ${pluralFunds(effectiveFunds.size)}`
                                        : 'Все фонды',
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

            {flowsError && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {flowsError}
                </div>
            )}
            {/* Сетевая ошибка загрузки цены (NO_PRICE_HISTORY — не ошибка, а
                empty-state внутри самого чарта карты). */}
            {mode === 'map' && priceError && priceError !== 'NO_PRICE_HISTORY' && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {priceError}
                </div>
            )}

            {/* Гистограмма: чистый поток по бумаге 1-в-1 как «Деньги в фондах».
                Карта сделок: недельная цена + кругляши месячных нетто. Оба чарта
                получают одинаковые months/series — режимы синхронны по окну. */}
            {/* position:relative — host для portal'а kebab-меню (ChartActionsMenu
                позиционируется absolute относительно этой обёртки). */}
            <div ref={chartAnchorRef} style={{ position: 'relative' }}>
                {mode === 'hist' ? (
                    <CompanyFlowsHistogram
                        months={visibleMonths}
                        series={visibleSeries}
                        height={chartHeight}
                        loading={flowsLoading}
                        noFundsSelected={noFundsSelected}
                        animTrigger={animTrigger}
                    />
                ) : (
                    <CompanyFlowsPriceMap
                        months={visibleMonths}
                        series={visibleSeries}
                        weeks={price && price.ticker === selectedTicker ? price.weeks : []}
                        closes={price && price.ticker === selectedTicker ? price.closes : []}
                        height={chartHeight}
                        loading={flowsLoading || priceLoading}
                        noFundsSelected={noFundsSelected}
                        priceMissing={!priceLoading && (priceError === 'NO_PRICE_HISTORY')}
                        animTrigger={animTrigger}
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
