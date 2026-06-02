/**
 * CompanyFlowsTab — раздел «Потоки по компании».
 *
 * Выбор бумаги (searchable селектор) → её помесячные потоки (Δ стоимости позиции)
 * по ВСЕМ фондам, что её держат. Два чарта:
 *   1. «Flow по фондам (млрд ₽)» — divergent-stacked по фондам (легенда = имена).
 *   2. «Суммарный Flow (млрд ₽)» — одна серия, окраска по знаку.
 *
 * Цвет фонда: UK_LOGOS[String(uk_id)]?.bg, иначе DONUT_COLORS[idx % len].
 * Значения приходят в ₽ → делим на 1e9 (млрд) во formatValue StackedFlowBars.
 *
 * Контрактные импорты из services/api: listFundTradeAssets, getCompanyFlows,
 * типы FundTradeAsset, CompanyFlowsResponse. Их добавляет бэкенд-агент по
 * общему контракту — здесь импортируем строго по контрактным именам.
 */
import { useEffect, useMemo, useState } from 'react';
import { ChevronDown, TrendingUp } from 'lucide-react';
import { UK_LOGOS, DONUT_COLORS, assetTicker, assetColor } from '../../config/fundConfig';
import {
    listFundTradeAssets,
    getCompanyFlows,
    type FundTradeAsset,
    type CompanyFlowsResponse,
} from '../../services/api';
import TickerLogo from '../TickerLogo';
import ChartLegend from '../chart/ChartLegend';
import StackedFlowBars, { type StackedSeries } from './StackedFlowBars';
import AssetPickerModal from './AssetPickerModal';
import FundPicker, { type FundPickerFund } from './FundPicker';

type Metric = 'amount' | 'weight';

// ITEM 2 — сколько фондов выбрать по умолчанию для свежей бумаги:
// top-N по суммарному |потоку| (см. computeTopFunds). Пусто-выбор = все фонды.
const DEFAULT_FUND_COUNT = 3;

// ITEM 4b/5 — логотип бумаги в опции селектора: спрайт по тикеру, иначе
// цветная точка (assetColor), иначе нейтральная точка.
function AssetMark({ name, size = 22 }: { name: string; size?: number }) {
    const ticker = assetTicker(name);
    if (ticker) return <TickerLogo ticker={ticker} size={size} rounded="full" />;
    const dot = assetColor(name) ?? 'var(--text-muted)';
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

    // ITEM 2 — фонды, попадающие в чарты: фильтр по выбранным тикерам (пусто = все).
    // Цвет фонда привязан к индексу в ПОЛНОМ списке (стабилен при фильтрации).
    const visibleSeries: StackedSeries[] = useMemo(() => {
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
                    } as StackedSeries,
                };
            })
            .filter(x => selectedFunds.size === 0 || selectedFunds.has(x.ticker))
            .map(x => x.series);
    }, [flows, selectedFunds]);

    // Серии для чарта «Flow по фондам».
    const fundSeries = visibleSeries;

    // Серия для «Суммарный Flow» — сумма по ВИДИМЫМ фондам (учёт фильтра фондов).
    const totalSeries: StackedSeries[] = useMemo(() => {
        if (!flows) return [];
        const monthsLen = flows.months.length;
        const sum: (number | null)[] = new Array(monthsLen).fill(null);
        for (const s of visibleSeries) {
            for (let i = 0; i < monthsLen; i++) {
                const v = s.values[i];
                if (v == null) continue;
                sum[i] = (sum[i] ?? 0) + v;
            }
        }
        return [{ label: 'Суммарный поток', color: '', values: sum }];
    }, [flows, visibleSeries]);

    const legendItems = useMemo(
        () => fundSeries.map(s => ({ color: s.color, label: s.label })),
        [fundSeries],
    );

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
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-5)' }}>
            {/* Заголовок + селектор бумаги */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                <h2
                    className="font-bold text-theme-primary"
                    style={{ fontSize: 'var(--fs-xl)', margin: 0 }}
                >
                    Потоки по компании
                </h2>
                {/* ITEM 3 — кнопка-триггер бумаги (лого + имя + счётчик) → AssetPickerModal.
                    ITEM 2 — FundPicker (multi) по конкретным фондам текущей бумаги. */}
                <div
                    style={{
                        marginTop: 'var(--sp-1)',
                        display: 'flex',
                        flexWrap: 'wrap',
                        alignItems: 'center',
                        gap: 'var(--sp-2)',
                    }}
                >
                    <button
                        type="button"
                        onClick={() => setPickerOpen(true)}
                        className="editorial-press flex items-center font-semibold rounded-full"
                        style={{
                            backgroundColor: 'var(--bg-secondary)',
                            color: 'var(--text-primary)',
                            border: '2px solid var(--text-primary)',
                            minWidth: 240,
                            maxWidth: '100%',
                            fontSize: 'var(--fs-sm)',
                            padding: 'var(--sp-2) var(--sp-4)',
                            gap: 'var(--sp-2)',
                            cursor: 'pointer',
                        }}
                    >
                        {selectedAsset && <AssetMark name={selectedAsset.asset_name} />}
                        {selectedAsset ? (
                            <>
                                <span
                                    className="flex-1 text-left"
                                    title={selectedAsset.asset_name}
                                    style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                                >
                                    {selectedAsset.asset_name}
                                </span>
                                <span
                                    className="tabular-nums flex-shrink-0"
                                    style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)' }}
                                >
                                    {selectedAsset.funds_count} {pluralFunds(selectedAsset.funds_count)}
                                </span>
                            </>
                        ) : (
                            <span className="flex-1 text-left truncate">Выберите бумагу</span>
                        )}
                        <ChevronDown size={16} style={{ flexShrink: 0 }} />
                    </button>

                    <FundPicker
                        funds={fundPickerFunds}
                        mode="multi"
                        selected={selectedFunds}
                        onChange={setSelectedFunds}
                    />
                </div>
            </div>

            {flowsError && (
                <div
                    className="rounded-2xl border border-theme"
                    style={{ padding: 'var(--sp-4)', background: 'var(--bg-secondary)', color: 'var(--funds-flow-negative)' }}
                >
                    {flowsError}
                </div>
            )}

            {/* Чарт 1: Flow по фондам (stacked) */}
            <section style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
                <div className="flex items-center justify-between" style={{ gap: 'var(--sp-3)' }}>
                    <h3 className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-md)', margin: 0 }}>
                        Flow по фондам (млрд ₽)
                    </h3>
                    {flowsLoading && (
                        <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
                            <div
                                className="w-4 h-4 border-2 border-t-transparent rounded-full animate-spin"
                                style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }}
                            />
                            <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-xs)' }}>
                                Обновление…
                            </span>
                        </div>
                    )}
                </div>

                {legendItems.length > 0 && (
                    <ChartLegend
                        items={legendItems}
                        fontWeight={600}
                        gap={16}
                        style={{ color: 'var(--text-primary)' }}
                    />
                )}

                {/* ITEM 2 — фильтр фондов спрятал все серии */}
                {flows && flows.funds.length > 0 && fundSeries.length === 0 && (
                    <div
                        className="text-theme-secondary"
                        style={{ fontSize: 'var(--fs-xs)', padding: 'var(--sp-2) 0' }}
                    >
                        Не выбрано ни одного фонда — выберите фонды или «Все фонды».
                    </div>
                )}

                <StackedFlowBars months={flows?.months ?? []} series={fundSeries} height={340} />
            </section>

            {/* Чарт 2: Суммарный Flow */}
            <section style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
                <h3 className="font-semibold text-theme-primary" style={{ fontSize: 'var(--fs-md)', margin: 0 }}>
                    Суммарный Flow (млрд ₽)
                </h3>
                <StackedFlowBars
                    months={flows?.months ?? []}
                    series={totalSeries}
                    height={280}
                    signColorForSingle
                />
            </section>

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
