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
import { useEffect, useMemo, useRef, useState } from 'react';
import { Search, ChevronDown, TrendingUp } from 'lucide-react';
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
import UkMultiSelect, { type UkOption } from './UkMultiSelect';

type Metric = 'amount' | 'weight';

// ITEM 4a — default УК: Первая (34), Альфа-Капитал (5), Т-Капитал (3597).
// НЕ Авторские (record/geroi). uk_id'ы как строки (ключ селекта = String(uk_id)).
const DEFAULT_UK_KEYS = ['34', '5', '3597'];

// ITEM 4a — фонды без числового uk_id (Авторские и пр.) группируются под этим
// ключом. В default не входит → «НЕ Авторские» выполняется само собой.
const NULL_UK_KEY = '__no_uk__';

// Ключ УК для фильтрации/опций: String(uk_id) либо NULL_UK_KEY.
function ukKeyOf(ukId: string | number | null | undefined): string {
    return ukId == null ? NULL_UK_KEY : String(ukId);
}

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

// ─────────────────────────────────────────────────────────────────────────────
// Searchable селектор бумаги — editorial pill + popup с текстовым фильтром.
// Self-contained (shared Dropdown не поддерживает поиск).
// ─────────────────────────────────────────────────────────────────────────────
// ITEM 5 — last4 ISIN: различитель для идентичных коротких имён.
function isinSuffix(isin: string | null): string {
    return isin ? isin.slice(-4) : '';
}

function AssetSelector({
    assets,
    selectedKey,
    onSelect,
    dupNames,
}: {
    assets: FundTradeAsset[];
    selectedKey: string | null;
    onSelect: (a: FundTradeAsset) => void;
    dupNames: Set<string>;
}) {
    const [open, setOpen] = useState(false);
    const [query, setQuery] = useState('');
    const wrapRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const selected = useMemo(
        () => assets.find(a => a.key === selectedKey) ?? null,
        [assets, selectedKey],
    );

    // Закрытие по клику вне + ESC.
    useEffect(() => {
        if (!open) return;
        const onClick = (e: MouseEvent) => {
            if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
        };
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setOpen(false);
        };
        document.addEventListener('mousedown', onClick);
        document.addEventListener('keydown', onKey);
        return () => {
            document.removeEventListener('mousedown', onClick);
            document.removeEventListener('keydown', onKey);
        };
    }, [open]);

    // Фокус на поле ввода при открытии.
    useEffect(() => {
        if (open) inputRef.current?.focus();
        else setQuery('');
    }, [open]);

    const filtered = useMemo(() => {
        const q = query.trim().toLowerCase();
        if (!q) return assets;
        return assets.filter(
            a =>
                a.asset_name.toLowerCase().includes(q) ||
                (a.isin ?? '').toLowerCase().includes(q),
        );
    }, [assets, query]);

    return (
        <div ref={wrapRef} className="frame-dropdown relative inline-block">
            <button
                type="button"
                onClick={() => setOpen(o => !o)}
                className="frame-dropdown-trigger flex items-center font-semibold rounded-full"
                style={{
                    backgroundColor: 'var(--bg-secondary)',
                    color: 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    minWidth: 260,
                    maxWidth: '100%',
                    fontSize: 'var(--fs-sm)',
                    padding: 'var(--sp-2) var(--sp-4)',
                    gap: 'var(--sp-2)',
                    transition: 'transform 0.15s ease, box-shadow 0.15s ease',
                }}
            >
                {selected && <AssetMark name={selected.asset_name} />}
                {selected ? (
                    <>
                        {/* ITEM 5 — ellipsis 1 строка + нативный title с полным именем */}
                        <span
                            className="flex-1 text-left"
                            title={selected.asset_name}
                            style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                        >
                            {selected.asset_name}
                        </span>
                        <span
                            className="tabular-nums flex-shrink-0"
                            style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)' }}
                        >
                            {selected.funds_count} {pluralFunds(selected.funds_count)}
                        </span>
                    </>
                ) : (
                    <span className="flex-1 text-left truncate">Выберите бумагу</span>
                )}
                <ChevronDown
                    size={16}
                    style={{
                        transition: 'transform 0.2s ease',
                        transform: open ? 'rotate(180deg)' : 'rotate(0)',
                        flexShrink: 0,
                    }}
                />
            </button>

            {open && (
                <div
                    className="frame-dropdown-menu absolute z-50 mt-1 rounded-xl"
                    style={{
                        backgroundColor: 'var(--bg-secondary)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
                        minWidth: '100%',
                        width: 340,
                        maxWidth: '90vw',
                    }}
                >
                    {/* Поле поиска */}
                    <div
                        className="flex items-center gap-2"
                        style={{
                            padding: 'var(--sp-2) var(--sp-3)',
                            borderBottom: '1px solid var(--border-color)',
                        }}
                    >
                        <Search size={15} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
                        <input
                            ref={inputRef}
                            type="text"
                            value={query}
                            onChange={e => setQuery(e.target.value)}
                            placeholder="Поиск по названию или ISIN…"
                            className="flex-1 bg-transparent outline-none"
                            style={{
                                color: 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                            }}
                        />
                    </div>

                    {/* Список опций */}
                    <div style={{ maxHeight: '50vh', overflowY: 'auto', padding: '4px 0' }}>
                        {filtered.length === 0 ? (
                            <div
                                className="text-theme-secondary"
                                style={{ padding: 'var(--sp-3)', fontSize: 'var(--fs-sm)', textAlign: 'center' }}
                            >
                                Ничего не найдено
                            </div>
                        ) : (
                            filtered.map(a => {
                                const active = a.key === selectedKey;
                                // ITEM 5 — суффикс last4 ISIN только для бумаг с
                                // идентичным коротким именем (различитель).
                                const suffix = dupNames.has(a.asset_name) ? isinSuffix(a.isin) : '';
                                return (
                                    <button
                                        key={a.key}
                                        type="button"
                                        onClick={() => {
                                            onSelect(a);
                                            setOpen(false);
                                        }}
                                        className="frame-dropdown-item w-full text-left flex items-center"
                                        style={{
                                            margin: '4px 6px',
                                            padding: '8px 12px',
                                            width: 'calc(100% - 12px)',
                                            borderRadius: 999,
                                            fontWeight: active ? 800 : 600,
                                            color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                            backgroundColor: active ? 'var(--accent)' : 'transparent',
                                            border: active ? '2px solid var(--text-primary)' : '2px solid transparent',
                                            boxShadow: active
                                                ? 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))'
                                                : 'none',
                                            cursor: 'pointer',
                                            gap: 'var(--sp-2)',
                                            transition: 'background-color 0.12s ease, color 0.12s ease',
                                        }}
                                    >
                                        <AssetMark name={a.asset_name} />
                                        {/* ITEM 5 — ellipsis 1 строка + title */}
                                        <span
                                            className="flex-1"
                                            title={a.asset_name}
                                            style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                                        >
                                            {a.asset_name}
                                            {suffix && (
                                                <span
                                                    className="tabular-nums"
                                                    style={{
                                                        marginLeft: 6,
                                                        fontSize: 'var(--fs-2xs)',
                                                        color: active ? 'var(--text-inverse)' : 'var(--text-muted)',
                                                        opacity: active ? 0.8 : 1,
                                                    }}
                                                >
                                                    …{suffix}
                                                </span>
                                            )}
                                        </span>
                                        <span
                                            className="tabular-nums flex-shrink-0"
                                            style={{
                                                fontSize: 'var(--fs-2xs)',
                                                color: active ? 'var(--text-inverse)' : 'var(--text-muted)',
                                                opacity: active ? 0.9 : 1,
                                            }}
                                        >
                                            {a.funds_count} {pluralFunds(a.funds_count)}
                                        </span>
                                    </button>
                                );
                            })
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

function pluralFunds(n: number): string {
    const mod10 = n % 10;
    const mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return 'фонд';
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'фонда';
    return 'фондов';
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

    // ITEM 4a — выбранные УК (ключ = String(uk_id) | NULL_UK_KEY). Пусто = все.
    const [selectedUks, setSelectedUks] = useState<Set<string>>(() => new Set(DEFAULT_UK_KEYS));

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

    // ITEM 5 — короткие имена, встречающиеся у >1 бумаги с разным ISIN: им нужен
    // различитель (суффикс last4 ISIN) в селекторе.
    const dupNames = useMemo(() => {
        const counts = new Map<string, number>();
        for (const a of assets) counts.set(a.asset_name, (counts.get(a.asset_name) ?? 0) + 1);
        const dup = new Set<string>();
        for (const [name, n] of counts) if (n > 1) dup.add(name);
        return dup;
    }, [assets]);

    // ITEM 4a — список УК для мультиселекта: из фондов, держащих текущую бумагу.
    // Дедуп по ukKeyOf(uk_id); имя — из UK_LOGOS либо «Прочие УК» для безымянных.
    const ukOptions: UkOption[] = useMemo(() => {
        if (!flows) return [];
        const seen = new Map<string, UkOption>();
        for (const f of flows.funds) {
            const key = ukKeyOf(f.uk_id);
            if (seen.has(key)) continue;
            const name = key === NULL_UK_KEY
                ? 'Прочие УК'
                : (UK_LOGOS[key]?.name ?? `УК ${key}`);
            seen.set(key, { key, name, uk_id: f.uk_id ?? undefined });
        }
        return Array.from(seen.values());
    }, [flows]);

    // ITEM 4a — фонды, попадающие в чарты: фильтр по выбранным УК (пусто = все).
    // Цвет фонда привязан к индексу в ПОЛНОМ списке (стабилен при фильтрации).
    const visibleSeries: StackedSeries[] = useMemo(() => {
        if (!flows) return [];
        return flows.funds
            .map((f, idx) => {
                const ukColor = f.uk_id != null ? UK_LOGOS[String(f.uk_id)]?.bg : undefined;
                return {
                    ukKey: ukKeyOf(f.uk_id),
                    series: {
                        label: f.fund_name,
                        color: ukColor ?? DONUT_COLORS[idx % DONUT_COLORS.length],
                        values: f.values,
                    } as StackedSeries,
                };
            })
            .filter(x => selectedUks.size === 0 || selectedUks.has(x.ukKey))
            .map(x => x.series);
    }, [flows, selectedUks]);

    // Серии для чарта «Flow по фондам».
    const fundSeries = visibleSeries;

    // Серия для «Суммарный Flow» — сумма по ВИДИМЫМ фондам (учёт UK-фильтра).
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
                {/* ITEM 4 — селектор бумаги (с лого) + мультиселект УК-фондов */}
                <div
                    style={{
                        marginTop: 'var(--sp-1)',
                        display: 'flex',
                        flexWrap: 'wrap',
                        alignItems: 'center',
                        gap: 'var(--sp-2)',
                    }}
                >
                    <AssetSelector
                        assets={assets}
                        selectedKey={selectedKey}
                        onSelect={a => setSelectedKey(a.key)}
                        dupNames={dupNames}
                    />
                    <UkMultiSelect
                        options={ukOptions}
                        selected={selectedUks}
                        onChange={setSelectedUks}
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

                {/* ITEM 4a — UK-фильтр спрятал все фонды, держащие бумагу */}
                {flows && flows.funds.length > 0 && fundSeries.length === 0 && (
                    <div
                        className="text-theme-secondary"
                        style={{ fontSize: 'var(--fs-xs)', padding: 'var(--sp-2) 0' }}
                    >
                        Эту бумагу не держат фонды выбранных УК — расширьте фильтр УК.
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
        </div>
    );
}
