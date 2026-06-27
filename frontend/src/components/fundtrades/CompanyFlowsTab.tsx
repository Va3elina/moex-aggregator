/**
 * CompanyFlowsTab — раздел «Потоки по компании».
 *
 * Выбор бумаги (таблетка-поиск в стиле «Сезонности») → её помесячные потоки
 * (Δ стоимости позиции) по фондам, что её держат. Один чарт —
 * CompanyFlowsHistogram, оформленный 1-в-1 как «Деньги в фондах»: бары рисуют
 * ЧИСТЫЙ поток (сумму по выбранным фондам, приток зелёный / отток красный), а
 * тултип раскрывает разбивку — какой фонд внёс наибольший вклад в движение.
 *
 * Цвет фонда: UK_LOGOS[String(uk_id)]?.bg, иначе DONUT_COLORS[idx % len].
 * Значения приходят в ₽ → CompanyFlowsHistogram переводит в млн (÷1e6).
 *
 * Контрактные импорты из services/api: listFundTradeAssets, getCompanyFlows,
 * типы FundTradeAsset, CompanyFlowsResponse. Их добавляет бэкенд-агент по
 * общему контракту — здесь импортируем строго по контрактным именам.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, TrendingUp } from 'lucide-react';
import { UK_LOGOS, DONUT_COLORS, resolveFundTicker, fundAssetName, fundAssetColor } from '../../config/fundConfig';
import {
    listFundTradeAssets,
    getCompanyFlows,
    type FundTradeAsset,
    type CompanyFlowsResponse,
} from '../../services/api';
import InstrumentIcon from '../InstrumentIcon';
import CompanyFlowsHistogram, { type CompanyFlowsSeries } from './CompanyFlowsHistogram';
import { useFitToViewport } from '../../hooks/useFitToViewport';
import AssetPickerModal from './AssetPickerModal';
import FundPicker, { type FundPickerFund } from './FundPicker';

type Metric = 'amount' | 'weight';

// ITEM 2 — сколько фондов выбрать по умолчанию для свежей бумаги:
// top-N по суммарному |потоку| (см. computeTopFunds). Пусто-выбор = все фонды.
const DEFAULT_FUND_COUNT = 3;

// ITEM 4b/5 — логотип бумаги: резолвим по ISIN в каноничный тикер (как в
// Сезонности) и рендерим через InstrumentIcon (STOCK_LOGO_OVERRIDE → стикерпак →
// /logos/<тикер>.png). Нет тикера (облигация/ОФЗ) → цветная точка.
function AssetMark({ name, isin, size = 22 }: { name: string; isin?: string | null; size?: number }) {
    const ticker = resolveFundTicker(name, isin);
    if (ticker) return <InstrumentIcon sectype={ticker} size={size} rounded="full" />;
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

// ITEM 2 — дефолт-выбор фондов для свежей бумаги: тикеры N фондов с наибольшим
// суммарным |потоком| (Σ|values|) по этой бумаге. Возвращает Set тикеров.
// Если фондов ≤ N — берём все (для FundPicker пустой Set = «все», поэтому при
// ≤ N выгоднее вернуть пусто и не плодить «частичный» выбор → даём пусто).
function computeTopFunds(
    funds: { ticker: string; values: (number | null)[] }[],
    n: number,
): Set<string> {
    if (funds.length <= n) return new Set();
    const scored = funds.map(f => ({
        ticker: f.ticker,
        score: f.values.reduce<number>((acc, v) => acc + (v == null ? 0 : Math.abs(v)), 0),
    }));
    scored.sort((a, b) => b.score - a.score);
    return new Set(scored.slice(0, n).map(s => s.ticker));
}

// ─────────────────────────────────────────────────────────────────────────────
// Главный компонент
// ─────────────────────────────────────────────────────────────────────────────
// ITEM 2 (cross-tab) — предвыбор бумаги из movers («Покупки фондов»).
export interface CompanyFlowsTabProps {
    /** Если задан — выбрать эту бумагу (по isin || asset_name) в селекторе. */
    presetAsset?: { asset_name: string; isin: string | null } | null;
    /** Дёрнуть после применения presetAsset (родитель сбросит state). */
    onPresetConsumed?: () => void;
}

export default function CompanyFlowsTab({ presetAsset, onPresetConsumed }: CompanyFlowsTabProps = {}) {
    // Высота графика «под экран» — anchor на обёртке чарта (как в «Деньги в фондах»).
    const chartAnchorRef = useRef<HTMLDivElement>(null);
    const chartHeight = useFitToViewport(chartAnchorRef, { min: 320, max: 560, bottomBuffer: 120 });
    const [assets, setAssets] = useState<FundTradeAsset[]>([]);
    const [assetsLoading, setAssetsLoading] = useState(true);
    const [assetsError, setAssetsError] = useState<string | null>(null);

    const [selectedKey, setSelectedKey] = useState<string | null>(null);
    const [flows, setFlows] = useState<CompanyFlowsResponse | null>(null);
    const [flowsLoading, setFlowsLoading] = useState(false);
    const [flowsError, setFlowsError] = useState<string | null>(null);

    // ITEM 3 — открыта ли модалка выбора бумаги.
    const [pickerOpen, setPickerOpen] = useState(false);

    // ITEM 2 — выбранные КОНКРЕТНЫЕ фонды (ключ = ticker). Пусто = все фонды.
    // Дефолт пересчитывается на смену бумаги (см. эффект ниже): top-3 по |потоку|.
    const [selectedFunds, setSelectedFunds] = useState<Set<string>>(() => new Set());

    // metric toggle — default ₽ (amount).
    const [metric] = useState<Metric>('amount');

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
                // ITEM 2 — на смену бумаги пере-выбираем дефолт: top-3 фонда по
                // суммарному |потоку|. ≤3 фондов → пусто (= все, см. computeTopFunds).
                setSelectedFunds(computeTopFunds(resp.funds, DEFAULT_FUND_COUNT));
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

    // ITEM 2 — список фондов для FundPicker (сгруппируется по УК внутри пикера).
    // uk-имя не передаём — FundPicker берёт его из UK_LOGOS[uk_id] (канон).
    const fundPickerFunds: FundPickerFund[] = useMemo(() => {
        if (!flows) return [];
        return flows.funds.map(f => ({
            ticker: f.ticker,
            name: f.fund_name,
            uk_id: f.uk_id,
        }));
    }, [flows]);

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
            .filter(x => selectedFunds.size === 0 || selectedFunds.has(x.ticker))
            .map(x => x.series);
    }, [flows, selectedFunds]);

    // Все фонды сняты пользователем (но у бумаги фонды есть) — empty-state.
    const noFundsSelected = !!flows && flows.funds.length > 0 && fundSeries.length === 0;

    // Триггер entrance-волны баров: перезапуск при смене бумаги ИЛИ набора фондов.
    const animTrigger = `${selectedAsset?.key ?? ''}|${[...selectedFunds].sort().join(',')}`;

    // ── Рендер ──
    if (assetsLoading) {
        return (
            <div className="flex items-center justify-center" style={{ padding: 'var(--sp-10)' }}>
                <div className="flex flex-col items-center" style={{ gap: 'var(--sp-3)' }}>
                    <div
                        className="w-8 h-8 border-2 border-t-transparent rounded-full animate-spin"
                        style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }}
                    />
                    <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-base)' }}>
                        Загрузка бумаг…
                    </span>
                </div>
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

                <FundPicker
                    funds={fundPickerFunds}
                    mode="multi"
                    selected={selectedFunds}
                    onChange={setSelectedFunds}
                />
            </div>

            {flowsError && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {flowsError}
                </div>
            )}

            {/* Чистый поток по бумаге — гистограмма 1-в-1 как «Деньги в фондах».
                Бары = сумма по выбранным фондам (приток зелёный / отток красный),
                тултип раскрывает вклад каждого фонда. */}
            <div ref={chartAnchorRef}>
                <CompanyFlowsHistogram
                    months={flows?.months ?? []}
                    series={fundSeries}
                    height={chartHeight}
                    loading={flowsLoading}
                    noFundsSelected={noFundsSelected}
                    animTrigger={animTrigger}
                />
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
