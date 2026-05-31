/**
 * FundTradesPage — отслеживание покупок/продаж в БПИФах.
 *
 * Архитектура:
 *   - Pro-only фича (tier-gated через useCommonFeatures().fund_trades_access).
 *   - Три таба: «Состав фондов» (карточки) + «Покупки фондов» (консенсус-
 *     гистограмма across фондов) + «Обзор снапшота» (per-fund помесячно).
 *   - При клике на фонд → детальный diff с current_holdings и изменениями.
 *
 * Источники данных: backend `/api/fund-trades/*`. Snapshot истории строится
 * скриптом `Funds/fetch_funds_realtime.py` который cron-аем дёргает раз в день.
 * Cbonds = monthly, ВИМ-парсер (WIP) = daily.
 *
 * Editorial design:
 *   - Card-based layout с иконками категорий
 *   - Diff показывается цветом: accumulated=success, reduced=danger,
 *     new=accent, sold_out=muted
 *   - Шаг данных — 1 снапшот в месяц; сравнение всегда «месяц vs предыдущий».
 */
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
    ArrowUpRight,
    ArrowDownRight,
    PlusCircle,
    XCircle,
    Lock,
    Sparkles,
    Wallet,
    TrendingUp,
    TrendingDown,
    Calendar,
    Activity,
} from 'lucide-react';
import {
    listFundsWithHistory,
    getFundTradesDetail,
    getFundTradesMovers,
    getFundSnapshots,
    getFundSnapshotReview,
    getAssetHistory,
    type FundTradesPeriod,
    type FundWithHistory,
    type FundTradesDetail,
    type FundTradesMovers,
    type FundTradeChangeType,
    type FundSnapshotsList,
    type FundSnapshotReview,
    type FundDiffRow,
    type AssetHistory,
} from '../services/api';
import { useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useAuth } from '../contexts/AuthContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import PageHeader from '../components/PageHeader';
import Dropdown from '../components/Dropdown';
import { UK_LOGOS } from '../config/fundConfig';

type Tab = 'funds' | 'movers' | 'snapshots';

const CATEGORY_LABEL: Record<string, string> = {
    stocks: 'Акции',
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
                    Что покупают фонды
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
                                featureName: 'Что покупают фонды',
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
// Fund Detail Modal — diff с current_holdings
// ════════════════════════════════════════════════════════════════════

function changeColor(type: FundTradeChangeType): string {
    switch (type) {
        case 'accumulated': return 'var(--success, #2dd478)';
        case 'reduced': return 'var(--danger, #ef4444)';
        case 'new': return 'var(--accent)';
        case 'sold_out': return 'var(--text-muted)';
        default: return 'var(--text-secondary)';
    }
}

function ChangeIcon({ type }: { type: FundTradeChangeType }) {
    const color = changeColor(type);
    const props = { size: 14, color, strokeWidth: 2.5 };
    switch (type) {
        case 'accumulated': return <ArrowUpRight {...props} />;
        case 'reduced': return <ArrowDownRight {...props} />;
        case 'new': return <PlusCircle {...props} />;
        case 'sold_out': return <XCircle {...props} />;
        default: return null;
    }
}

function FundDetailModal({
    ticker,
    period,
    onClose,
}: {
    ticker: string;
    period: FundTradesPeriod;
    onClose: () => void;
}) {
    const [data, setData] = useState<FundTradesDetail | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);
        getFundTradesDetail(ticker, period)
            .then((d) => { if (!cancelled) setData(d); })
            .catch((e: Error) => { if (!cancelled) setError(e.message); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [ticker, period]);

    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'rgba(0, 0, 0, 0.5)',
                zIndex: 1000,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: 16,
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 14,
                    width: '100%',
                    maxWidth: 900,
                    maxHeight: '85vh',
                    overflow: 'auto',
                    boxShadow: '0 10px 40px rgba(0,0,0,0.3)',
                }}
            >
                {/* Header */}
                <div
                    style={{
                        padding: '16px 20px',
                        borderBottom: '1px solid var(--border-color)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        position: 'sticky',
                        top: 0,
                        background: 'var(--bg-primary)',
                        zIndex: 1,
                    }}
                >
                    <h2
                        style={{
                            margin: 0,
                            fontSize: 'var(--fs-lg)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                        }}
                    >
                        {data?.fund.ticker || ticker}
                        {data?.fund.name && (
                            <span
                                style={{
                                    marginLeft: 10,
                                    fontWeight: 400,
                                    fontSize: 'var(--fs-sm)',
                                    color: 'var(--text-secondary)',
                                }}
                            >
                                {data.fund.name}
                            </span>
                        )}
                    </h2>
                    <button
                        onClick={onClose}
                        style={{
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text-secondary)',
                            cursor: 'pointer',
                            fontSize: 20,
                            padding: 4,
                        }}
                    >
                        ✕
                    </button>
                </div>

                <div style={{ padding: 20 }}>
                    {loading && <div style={{ color: 'var(--text-muted)' }}>Загружаем…</div>}
                    {error && <div style={{ color: 'var(--danger, #ef4444)' }}>{error}</div>}
                    {data && !loading && (
                        <>
                            {/* Snapshot dates */}
                            <div
                                style={{
                                    display: 'flex',
                                    gap: 12,
                                    marginBottom: 20,
                                    flexWrap: 'wrap',
                                    fontSize: 'var(--fs-sm)',
                                    color: 'var(--text-secondary)',
                                }}
                            >
                                <span>
                                    <Calendar size={12} style={{ verticalAlign: '-1px' }} /> Текущий:{' '}
                                    <strong style={{ color: 'var(--text-primary)' }}>
                                        {data.current_snapshot_date || '—'}
                                    </strong>
                                </span>
                                <span>
                                    Предыдущий:{' '}
                                    <strong style={{ color: 'var(--text-primary)' }}>
                                        {data.previous_snapshot_date || '—'}
                                    </strong>
                                </span>
                            </div>

                            {/* Summary stats */}
                            {data.previous_snapshot_date && (
                                <div
                                    style={{
                                        display: 'grid',
                                        gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
                                        gap: 10,
                                        marginBottom: 24,
                                    }}
                                >
                                    {[
                                        { label: 'Новые', count: data.summary.new, type: 'new' as const },
                                        { label: 'Накоплены', count: data.summary.accumulated, type: 'accumulated' as const },
                                        { label: 'Сокращены', count: data.summary.reduced, type: 'reduced' as const },
                                        { label: 'Проданы', count: data.summary.sold_out, type: 'sold_out' as const },
                                    ].map(({ label, count, type }) => (
                                        <div
                                            key={label}
                                            style={{
                                                padding: 12,
                                                background: 'var(--bg-secondary)',
                                                borderRadius: 8,
                                                border: '1px solid var(--border-color)',
                                            }}
                                        >
                                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                                                <ChangeIcon type={type} />
                                                <span
                                                    style={{
                                                        fontSize: 'var(--fs-2xs)',
                                                        color: 'var(--text-muted)',
                                                        textTransform: 'uppercase',
                                                        letterSpacing: '0.04em',
                                                        fontWeight: 600,
                                                    }}
                                                >
                                                    {label}
                                                </span>
                                            </div>
                                            <div
                                                style={{
                                                    fontSize: 'var(--fs-xl)',
                                                    fontWeight: 800,
                                                    color: changeColor(type),
                                                }}
                                            >
                                                {count}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            )}

                            {/* Diff table */}
                            {data.diff.length > 0 && (
                                <>
                                    <h3
                                        style={{
                                            fontSize: 'var(--fs-md)',
                                            fontWeight: 700,
                                            color: 'var(--text-primary)',
                                            marginBottom: 10,
                                        }}
                                    >
                                        Изменения позиций
                                    </h3>
                                    <DiffTable diff={data.diff} />
                                </>
                            )}

                            {/* Current holdings */}
                            {data.current_holdings.length > 0 && (
                                <>
                                    <h3
                                        style={{
                                            fontSize: 'var(--fs-md)',
                                            fontWeight: 700,
                                            color: 'var(--text-primary)',
                                            marginTop: 24,
                                            marginBottom: 10,
                                        }}
                                    >
                                        Текущий состав (топ-30)
                                    </h3>
                                    <table
                                        style={{
                                            width: '100%',
                                            borderCollapse: 'collapse',
                                            fontSize: 'var(--fs-sm)',
                                        }}
                                    >
                                        <thead>
                                            <tr>
                                                <th style={{
                                                    textAlign: 'left',
                                                    padding: '8px 12px',
                                                    color: 'var(--text-muted)',
                                                    fontSize: 'var(--fs-2xs)',
                                                    textTransform: 'uppercase',
                                                    letterSpacing: '0.05em',
                                                    fontWeight: 700,
                                                    borderBottom: '2px solid var(--text-primary)',
                                                }}>Актив</th>
                                                <th style={{
                                                    textAlign: 'right',
                                                    padding: '8px 12px',
                                                    color: 'var(--text-muted)',
                                                    fontSize: 'var(--fs-2xs)',
                                                    textTransform: 'uppercase',
                                                    letterSpacing: '0.05em',
                                                    fontWeight: 700,
                                                    borderBottom: '2px solid var(--text-primary)',
                                                }}>Доля, %</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {data.current_holdings.slice(0, 30).map((h) => (
                                                <tr key={h.asset_name}>
                                                    <td style={{
                                                        padding: '8px 12px',
                                                        borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
                                                        color: 'var(--text-primary)',
                                                    }}>
                                                        {h.asset_name}
                                                    </td>
                                                    <td style={{
                                                        padding: '8px 12px',
                                                        textAlign: 'right',
                                                        borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
                                                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                                        color: 'var(--text-primary)',
                                                        fontWeight: 600,
                                                    }}>
                                                        {h.weight !== null ? h.weight.toFixed(2) : '—'}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function DiffTable({ diff }: { diff: FundTradesDetail['diff'] }) {
    const labelByType: Record<FundTradeChangeType, string> = {
        new: 'НОВЫЙ',
        sold_out: 'ПРОДАН',
        accumulated: 'НАКОПЛЕН',
        reduced: 'СОКРАЩЁН',
        unchanged: '—',
    };
    return (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--fs-sm)' }}>
            <thead>
                <tr>
                    {['Актив', 'Тип', 'Δ %', 'Было', 'Стало'].map((h) => (
                        <th
                            key={h}
                            style={{
                                textAlign: 'left',
                                padding: '8px 12px',
                                color: 'var(--text-muted)',
                                fontSize: 'var(--fs-2xs)',
                                textTransform: 'uppercase',
                                letterSpacing: '0.05em',
                                fontWeight: 700,
                                borderBottom: '2px solid var(--text-primary)',
                            }}
                        >
                            {h}
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                {diff.map((d) => (
                    <tr key={d.asset_name}>
                        <td style={cellStyle()}>{d.asset_name}</td>
                        <td style={cellStyle()}>
                            <span
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 4,
                                    padding: '2px 8px',
                                    background: `color-mix(in srgb, ${changeColor(d.change_type)} 14%, transparent)`,
                                    color: changeColor(d.change_type),
                                    fontSize: 'var(--fs-2xs)',
                                    fontWeight: 700,
                                    borderRadius: 4,
                                    letterSpacing: '0.04em',
                                }}
                            >
                                <ChangeIcon type={d.change_type} />
                                {labelByType[d.change_type]}
                            </span>
                        </td>
                        <td style={{ ...cellStyle(), fontFamily: 'ui-monospace, monospace', fontWeight: 700, color: changeColor(d.change_type) }}>
                            {d.delta_weight !== null
                                ? `${d.delta_weight > 0 ? '+' : ''}${d.delta_weight.toFixed(2)}`
                                : '—'}
                        </td>
                        <td style={{ ...cellStyle(), fontFamily: 'ui-monospace, monospace', color: 'var(--text-muted)' }}>
                            {d.previous_weight !== null ? d.previous_weight.toFixed(2) : '—'}
                        </td>
                        <td style={{ ...cellStyle(), fontFamily: 'ui-monospace, monospace', color: 'var(--text-primary)', fontWeight: 600 }}>
                            {d.current_weight !== null ? d.current_weight.toFixed(2) : '—'}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

function cellStyle(): React.CSSProperties {
    return {
        padding: '8px 12px',
        borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
        color: 'var(--text-primary)',
    };
}

// ════════════════════════════════════════════════════════════════════
// Main page
// ════════════════════════════════════════════════════════════════════

export default function FundTradesPage() {
    const common = useCommonFeatures();
    const [tab, setTab] = useState<Tab>('funds');
    // Шаг данных — 1 снапшот/месяц. Период фиксирован '1m' (месяц vs предыдущий);
    // селектор месяца появится в Заходе 2 (нужен backend as_of/available_months).
    const [period] = useState<FundTradesPeriod>('1m');
    const [selectedTicker, setSelectedTicker] = useState<string | null>(null);

    const [funds, setFunds] = useState<FundWithHistory[]>([]);
    const [movers, setMovers] = useState<FundTradesMovers | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    // Заход 2: управление табом «Покупки фондов».
    const [asOf, setAsOf] = useState<string | undefined>(undefined);    // выбранный месяц (undefined = последний)
    const [manager, setManager] = useState<string>('');                  // фильтр по УК ('' = все)
    const [metric, setMetric] = useState<'weight' | 'amount'>('weight'); // % веса | объём ₽

    // Load funds list (один раз).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        setLoading(true);
        listFundsWithHistory()
            .then((r) => setFunds(r.funds))
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [common.fund_trades_access]);

    // Load movers when tab=movers (или меняются параметры: месяц/УК/метрика).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        if (tab !== 'movers') return;
        setLoading(true);
        getFundTradesMovers(period, { asOf, manager: manager || undefined, sort: metric })
            .then(setMovers)
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [tab, period, asOf, manager, metric, common.fund_trades_access]);

    const fundsByCategory = useMemo(() => {
        const groups: Record<string, FundWithHistory[]> = {};
        for (const f of funds) {
            const key = f.category || 'other';
            if (!groups[key]) groups[key] = [];
            groups[key].push(f);
        }
        return groups;
    }, [funds]);

    // УК для фильтра «Покупок фондов» — только stock-фонды.
    const managers = useMemo(() => {
        const set = new Set<string>();
        for (const f of funds) {
            if (f.category === 'stocks' && f.uk) set.add(f.uk);
        }
        return Array.from(set).sort();
    }, [funds]);

    if (!common.fund_trades_access) {
        return <LockedView />;
    }

    return (
        <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            {/* Header — единый PageHeader как у всех индикаторов
                (иконка стилизуется через .page-header-icon → выравнивание как везде) */}
            <PageHeader
                icon={Wallet}
                title="Что покупают фонды"
                subtitle="Состав портфелей крупных фондов акций — что управляющие компании накапливают и распродают"
            />

            {/* Beta banner — пока показываем только 6 ВИМ-фондов */}
            <div
                style={{
                    padding: '12px 16px',
                    marginBottom: 20,
                    background: 'color-mix(in srgb, var(--warning, #f59e0b) 8%, var(--bg-secondary))',
                    border: '1.5px solid color-mix(in srgb, var(--warning, #f59e0b) 30%, transparent)',
                    borderRadius: 10,
                    display: 'flex',
                    alignItems: 'flex-start',
                    gap: 10,
                }}
            >
                <span
                    style={{
                        padding: '2px 8px',
                        background: 'var(--warning, #f59e0b)',
                        color: 'var(--text-inverse)',
                        borderRadius: 4,
                        fontSize: 'var(--fs-2xs)',
                        fontWeight: 700,
                        textTransform: 'uppercase',
                        letterSpacing: '0.04em',
                        flexShrink: 0,
                    }}
                >
                    Beta
                </span>
                <div style={{ flex: 1, fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                    Отслеживаем <strong style={{ color: 'var(--text-primary)' }}>фонды акций</strong> УК
                    Первая, Т-Капитал, ВИМ и Альфа. Состав — строго из официальных Справок о
                    стоимости чистых активов (форма ЦБ № 0420502, точные позиции от самих УК).
                    Облигационные фонды скрыты — методология в работе.
                </div>
            </div>

            {/* Tabs */}
            <div
                style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 12,
                    alignItems: 'center',
                    marginBottom: 20,
                    paddingBottom: 16,
                    borderBottom: '1px solid var(--border-color)',
                }}
            >
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {([
                        { id: 'funds' as const, label: 'Состав фондов', icon: Wallet },
                        { id: 'movers' as const, label: 'Покупки фондов', icon: TrendingUp },
                        { id: 'snapshots' as const, label: 'Обзор снапшота', icon: Activity },
                    ]).map((t) => {
                        const Icon = t.icon;
                        const active = tab === t.id;
                        return (
                            <button
                                key={t.id}
                                onClick={() => setTab(t.id)}
                                className="editorial-press"
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 6,
                                    padding: '8px 16px',
                                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                    border: '2px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: active ? 700 : 600,
                                    cursor: 'pointer',
                                    boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
                                }}
                            >
                                <Icon size={14} />
                                {t.label}
                            </button>
                        );
                    })}
                </div>
            </div>

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

            {/* Tab content */}
            {tab === 'funds' && (
                <>
                    {loading && funds.length === 0 && (
                        <div style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Загружаем фонды…</div>
                    )}
                    {!loading && funds.length === 0 && !error && (
                        <EmptyState message="Фонды не найдены." />
                    )}
                    {Object.entries(fundsByCategory).map(([cat, list]) => (
                        <div key={cat} style={{ marginBottom: 28 }}>
                            <h2
                                style={{
                                    fontSize: 'var(--fs-md)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 10,
                                    textTransform: 'uppercase',
                                    letterSpacing: '0.04em',
                                }}
                            >
                                {CATEGORY_LABEL[cat] || cat} <span style={{ color: 'var(--text-muted)', fontWeight: 500 }}>· {list.length}</span>
                            </h2>
                            <div
                                style={{
                                    display: 'grid',
                                    gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))',
                                    gap: 10,
                                }}
                            >
                                {list.map((f) => {
                                    const uk = f.uk_id != null ? UK_LOGOS[String(f.uk_id)] : null;
                                    return (
                                    <button
                                        key={f.fund_id}
                                        onClick={() => setSelectedTicker(f.ticker)}
                                        style={{
                                            padding: 14,
                                            background: 'var(--bg-secondary)',
                                            border: '1.5px solid var(--border-color)',
                                            borderRadius: 10,
                                            textAlign: 'left',
                                            cursor: 'pointer',
                                            transition: 'border-color 120ms, transform 80ms',
                                        }}
                                        onMouseEnter={(e) => {
                                            e.currentTarget.style.borderColor = 'var(--accent)';
                                        }}
                                        onMouseLeave={(e) => {
                                            e.currentTarget.style.borderColor = 'var(--border-color)';
                                        }}
                                    >
                                        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                                            {uk && (
                                                <div
                                                    title={uk.name}
                                                    style={{
                                                        width: 40,
                                                        height: 40,
                                                        borderRadius: '50%',
                                                        display: 'flex',
                                                        alignItems: 'center',
                                                        justifyContent: 'center',
                                                        flexShrink: 0,
                                                        fontWeight: 900,
                                                        fontSize: 'var(--fs-lg)',
                                                        backgroundColor: uk.bg,
                                                        color: uk.color,
                                                    }}
                                                >
                                                    {uk.letter}
                                                </div>
                                            )}
                                            <div style={{ flex: 1, minWidth: 0 }}>
                                                <div
                                                    style={{
                                                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                                        fontSize: 'var(--fs-md)',
                                                        fontWeight: 800,
                                                        color: 'var(--text-primary)',
                                                        marginBottom: 4,
                                                    }}
                                                >
                                                    {f.ticker}
                                                </div>
                                                <div
                                                    style={{
                                                        fontSize: 'var(--fs-xs)',
                                                        color: 'var(--text-secondary)',
                                                        marginBottom: 8,
                                                        lineHeight: 1.4,
                                                        display: '-webkit-box',
                                                        WebkitLineClamp: 2,
                                                        WebkitBoxOrient: 'vertical',
                                                        overflow: 'hidden',
                                                    }}
                                                >
                                                    {f.name}
                                                </div>
                                                <div
                                                    style={{
                                                        display: 'flex',
                                                        justifyContent: 'space-between',
                                                        fontSize: 'var(--fs-2xs)',
                                                        color: 'var(--text-muted)',
                                                    }}
                                                >
                                                    <span>{f.last_snapshot_date || '—'}</span>
                                                    <span>{f.snapshot_count} snap.</span>
                                                </div>
                                            </div>
                                        </div>
                                    </button>
                                    );
                                })}
                            </div>
                        </div>
                    ))}
                </>
            )}

            {tab === 'movers' && (
                <>
                    {/* Контролы: месяц · УК · метрика (% веса / объём ₽) */}
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center', marginBottom: 16 }}>
                        {movers && movers.available_months.length > 0 && (
                            <Dropdown<string>
                                options={movers.available_months.map((m) => ({ key: m, label: formatMonthYear(m) }))}
                                value={asOf ?? movers.available_months[0]}
                                onChange={setAsOf}
                                minWidth={150}
                            />
                        )}
                        {managers.length > 0 && (
                            <Dropdown<string>
                                options={[{ key: '', label: 'Все УК' }, ...managers.map((uk) => ({ key: uk, label: uk }))]}
                                value={manager}
                                onChange={setManager}
                                minWidth={140}
                            />
                        )}
                        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4, padding: 4, background: 'var(--bg-secondary)', borderRadius: 8 }}>
                            {([['weight', '% веса'], ['amount', 'Объём ₽']] as const).map(([key, lbl]) => (
                                <button
                                    key={key}
                                    onClick={() => setMetric(key)}
                                    style={{
                                        padding: '4px 12px',
                                        background: metric === key ? 'var(--bg-primary)' : 'transparent',
                                        color: metric === key ? 'var(--text-primary)' : 'var(--text-secondary)',
                                        border: metric === key
                                            ? '1px solid color-mix(in srgb, var(--text-primary) 18%, transparent)'
                                            : '1px solid transparent',
                                        borderRadius: 6,
                                        fontSize: 'var(--fs-xs)',
                                        fontWeight: metric === key ? 700 : 600,
                                        cursor: 'pointer',
                                    }}
                                >
                                    {lbl}
                                </button>
                            ))}
                        </div>
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
                                items={movers.top_accumulated}
                                empty="Накоплений нет"
                                metric={metric}
                            />
                            <MoversColumn
                                title="Топ-распродажа"
                                icon={TrendingDown}
                                color="var(--danger, #ef4444)"
                                items={movers.top_reduced}
                                empty="Распродаж нет"
                                negative
                                metric={metric}
                            />
                        </div>
                    )}
                    {movers
                        && movers.top_accumulated.length === 0
                        && movers.top_reduced.length === 0 && (
                        <EmptyState message="Нет заметных движений между этими месяцами." />
                    )}
                </>
            )}

            {tab === 'snapshots' && <SnapshotReviewTab />}

            {selectedTicker && (
                <FundDetailModal
                    ticker={selectedTicker}
                    period={period}
                    onClose={() => setSelectedTicker(null)}
                />
            )}
        </div>
    );
}

// ════════════════════════════════════════════════════════════════════
// Snapshot Review Tab — обзор каждого снапшота: купил/продал/новые/полностью вышел
// в editorial-стиле «Секторов дня». Сравниваем с предыдущим снапшотом.
// ════════════════════════════════════════════════════════════════════

// SNAPSHOT_TICKERS подгружается динамически из API /funds — туда попадают
// все фонды из whitelist с хотя бы 1 snapshot в fund_holdings_history.

function formatRubShort(amount: number | null): string {
    if (amount === null || amount === undefined) return '—';
    const abs = Math.abs(amount);
    if (abs >= 1e9) return `${(amount / 1e9).toFixed(2)} млрд ₽`;
    if (abs >= 1e6) return `${(amount / 1e6).toFixed(1)} млн ₽`;
    if (abs >= 1e3) return `${(amount / 1e3).toFixed(0)} тыс ₽`;
    return `${amount.toFixed(0)} ₽`;
}

function formatShares(positions: number | null): string {
    if (positions === null || positions === undefined) return '—';
    return positions.toLocaleString('ru-RU');
}

function formatSnapshotDate(iso: string): string {
    // 2026-04-30 → "30 апр 2026"
    const d = new Date(iso);
    const months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
    return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

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
}: {
    label: string;
    subLabel?: string;
    amount: number;
    maxAbs: number;
    isPositive: boolean;
    onClick?: () => void;
}) {
    const widthPct = maxAbs > 0 ? Math.max(2, Math.abs(amount) / maxAbs * 100) : 2;
    const color = isPositive ? 'var(--mood-green, #4a9959)' : 'var(--mood-red, #b85645)';

    return (
        <div
            onClick={onClick}
            style={{
                display: 'grid',
                gridTemplateColumns: '180px 1fr 140px',
                alignItems: 'center',
                gap: 12,
                padding: '8px 0',
                cursor: onClick ? 'pointer' : 'default',
                borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.06))',
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
                        width: `${widthPct}%`,
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
                {isPositive ? '+' : '−'}{formatRubShort(Math.abs(amount))}
            </div>
        </div>
    );
}

function SnapshotReviewTab() {
    const [availableFunds, setAvailableFunds] = useState<FundWithHistory[]>([]);
    const [ticker, setTicker] = useState<string>('EQMX');
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
        getFundSnapshots(ticker)
            .then((data) => {
                if (cancel) return;
                setSnapshotsList(data);
                // Default = latest snapshot
                if (data.snapshots.length > 0) {
                    setSelectedDate(data.snapshots[0].snapshot_date);
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

    // Группировка фондов по УК для dropdown
    const fundsByUk = useMemo(() => {
        const groups: Record<string, FundWithHistory[]> = {};
        for (const f of availableFunds) {
            const uk = f.uk || 'Прочие';
            if (!groups[uk]) groups[uk] = [];
            groups[uk].push(f);
        }
        return groups;
    }, [availableFunds]);

    const selectedFund = availableFunds.find(f => f.ticker === ticker);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
            {/* Ticker selector — dropdown с группировкой по УК */}
            <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
                <label style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                    Фонд:
                </label>
                <Dropdown<string>
                    options={Object.entries(fundsByUk).flatMap(([, funds]) =>
                        funds.map((f) => ({ key: f.ticker, label: `${f.ticker} — ${f.name}` }))
                    )}
                    value={ticker}
                    onChange={setTicker}
                    minWidth={280}
                    menuMaxWidth={480}
                />
                {selectedFund && (
                    <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-tertiary)' }}>
                        {selectedFund.uk || 'УК неизвестна'} · {selectedFund.category || ''}
                    </span>
                )}
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
                                title={`${s.snapshot_date} · ${s.asset_count} активов`}
                                className="editorial-press"
                                style={{
                                    padding: '6px 14px',
                                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: active ? 'var(--text-inverse)' : 'var(--text-secondary)',
                                    border: '2px solid var(--text-primary)',
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

            {/* Review sections */}
            {!loading && review && (
                <SnapshotReviewBody
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
                    {' · '}{review.totals.current_assets} активов
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
                            <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>{h.asset_name}</span>
                            <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', fontVariantNumeric: 'tabular-nums', flexShrink: 0 }}>
                                {h.weight != null ? `${h.weight.toFixed(2)}%` : '—'}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {/* ДОКУПИЛ */}
            {review.added.length > 0 && (
                <SnapshotSection
                    title="ДОКУПИЛ"
                    count={review.added.length}
                    totalRub={review.totals.total_added_rub}
                    items={review.added}
                    maxAbs={maxAbsAmount}
                    isPositive={true}
                    valueGetter={(r) => r.delta_amount_rub || 0}
                    subLabelGetter={(r) =>
                        `+${formatShares(r.delta_positions || 0)} шт` +
                        (r.curr_weight !== null ? ` · ${r.curr_weight.toFixed(2)}%` : '')
                    }
                    onItemClick={onRowClick}
                />
            )}

            {/* ПРОДАЛ */}
            {review.reduced.length > 0 && (
                <SnapshotSection
                    title="ПРОДАЛ"
                    count={review.reduced.length}
                    totalRub={Math.abs(review.totals.total_reduced_rub)}
                    items={review.reduced}
                    maxAbs={maxAbsAmount}
                    isPositive={false}
                    valueGetter={(r) => r.delta_amount_rub || 0}
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
                    totalRub={review.totals.total_new_rub}
                    items={review.new}
                    maxAbs={maxAbsAmount}
                    isPositive={true}
                    valueGetter={(r) => r.curr_amount_rub || 0}
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
                    totalRub={review.totals.total_sold_out_rub}
                    items={review.sold_out}
                    maxAbs={maxAbsAmount}
                    isPositive={false}
                    valueGetter={(r) => -(r.prev_amount_rub || 0)}
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
    totalRub,
    items,
    maxAbs,
    isPositive,
    valueGetter,
    subLabelGetter,
    onItemClick,
}: {
    title: string;
    count: number;
    totalRub: number;
    items: FundDiffRow[];
    maxAbs: number;
    isPositive: boolean;
    valueGetter: (r: FundDiffRow) => number;
    subLabelGetter: (r: FundDiffRow) => string;
    onItemClick?: (r: FundDiffRow) => void;
}) {
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
                    {isPositive ? '+' : '−'}{formatRubShort(Math.abs(totalRub))}
                </span>
            </div>
            <div>
                {items.map((r) => (
                    <EditorialBar
                        key={`${r.asset_name}-${r.isin || ''}`}
                        label={r.asset_name}
                        subLabel={subLabelGetter(r)}
                        amount={valueGetter(r)}
                        maxAbs={maxAbs}
                        isPositive={isPositive}
                        onClick={onItemClick ? () => onItemClick(r) : undefined}
                    />
                ))}
            </div>
        </div>
    );
}

// ════════════════════════════════════════════════════════════════════
// Asset History Modal — drill-down: график positions по одной позиции в одном фонде
// ════════════════════════════════════════════════════════════════════

function AssetHistoryModal({
    ticker,
    asset_name,
    isin,
    onClose,
}: {
    ticker: string;
    asset_name: string;
    isin: string | null;
    onClose: () => void;
}) {
    const [data, setData] = useState<AssetHistory | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancel = false;
        setLoading(true);
        setError(null);
        getAssetHistory(ticker, isin ? { isin } : { assetName: asset_name })
            .then((d) => !cancel && setData(d))
            .catch((e) => !cancel && setError(e.message))
            .finally(() => !cancel && setLoading(false));
        return () => { cancel = true; };
    }, [ticker, asset_name, isin]);

    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                zIndex: 1000, padding: 16,
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-primary)',
                    maxWidth: 920, width: '100%', maxHeight: '92vh', overflow: 'auto',
                    border: '1.5px solid var(--text-primary)',
                    padding: '24px 28px',
                    boxShadow: '0 16px 60px rgba(0,0,0,0.3)',
                }}
            >
                {/* Header */}
                <div style={{
                    display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
                    marginBottom: 20, paddingBottom: 16,
                    borderBottom: '1.5px solid var(--text-primary)',
                }}>
                    <div>
                        <h3 style={{
                            fontFamily: 'var(--font-serif, Georgia, serif)',
                            fontSize: 'var(--fs-2xl)', margin: 0, marginBottom: 4,
                            color: 'var(--text-primary)',
                        }}>
                            {asset_name}
                        </h3>
                        <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-tertiary)' }}>
                            {ticker} {isin && <span style={{ marginLeft: 8 }}>· {isin}</span>}
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        style={{
                            border: 'none', background: 'transparent',
                            fontSize: 24, cursor: 'pointer', color: 'var(--text-tertiary)',
                            padding: '4px 8px',
                        }}
                        aria-label="Закрыть"
                    >
                        ×
                    </button>
                </div>

                {loading && (
                    <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>
                        Загрузка истории...
                    </div>
                )}
                {error && (
                    <div style={{ padding: 16, color: 'var(--mood-red)' }}>{error}</div>
                )}

                {data && !loading && <AssetHistoryContent data={data} />}
            </div>
        </div>
    );
}

function AssetHistoryContent({ data }: { data: AssetHistory }) {
    // Bounds для графика positions
    const maxPos = Math.max(...data.timeline.map(p => p.positions || 0));
    const minPos = Math.min(...data.timeline.filter(p => p.positions !== null).map(p => p.positions!));
    const range = maxPos - minPos || 1;

    // SVG chart dimensions
    const W = 800;
    const H = 260;
    const padL = 80;
    const padR = 20;
    const padT = 20;
    const padB = 40;
    const innerW = W - padL - padR;
    const innerH = H - padT - padB;

    const points = data.timeline.filter(p => p.positions !== null);
    const xStep = points.length > 1 ? innerW / (points.length - 1) : 0;

    const linePath = points.map((p, i) => {
        const x = padL + i * xStep;
        const y = padT + innerH - ((p.positions! - minPos) / range) * innerH;
        return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
    }).join(' ');

    // Y-axis ticks (3-5 значений)
    const yTicks = 4;
    const tickValues = Array.from({ length: yTicks + 1 }, (_, i) => minPos + (range / yTicks) * i);

    // Доп. цвета: сравнить первое и последнее
    const firstPos = points[0]?.positions || 0;
    const lastPos = points[points.length - 1]?.positions || 0;
    const totalDelta = lastPos - firstPos;
    const totalDeltaColor = totalDelta >= 0 ? 'var(--mood-green)' : 'var(--mood-red)';

    return (
        <>
            {/* Summary */}
            <div style={{
                display: 'flex', gap: 32, flexWrap: 'wrap',
                marginBottom: 20, padding: '12px 0',
            }}>
                <SummaryStat
                    label="ПЕРВЫЙ СНАПШОТ"
                    value={formatSnapshotDate(data.first_seen)}
                    sub={`${formatShares(firstPos)} шт`}
                />
                <SummaryStat
                    label="ПОСЛЕДНИЙ СНАПШОТ"
                    value={formatSnapshotDate(data.last_seen)}
                    sub={`${formatShares(lastPos)} шт`}
                />
                <SummaryStat
                    label="ИЗМЕНЕНИЕ"
                    value={`${totalDelta >= 0 ? '+' : ''}${formatShares(totalDelta)} шт`}
                    sub={`за ${data.snapshots_count} снапшота${data.snapshots_count > 1 ? 'ов' : ''}`}
                    color={totalDeltaColor}
                />
                <SummaryStat
                    label="ТЕКУЩАЯ ДОЛЯ"
                    value={points[points.length - 1]?.weight !== null
                        ? `${points[points.length - 1].weight!.toFixed(2)}%`
                        : '—'}
                    sub={formatRubShort(points[points.length - 1]?.amount_rub || null)}
                />
            </div>

            {/* SVG Chart */}
            <div style={{
                background: 'var(--bg-secondary, rgba(0,0,0,0.02))',
                padding: '12px 0', borderRadius: 4, marginBottom: 24,
            }}>
                <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: 'block' }}>
                    {/* Y-axis grid + labels */}
                    {tickValues.map((tv, i) => {
                        const y = padT + innerH - ((tv - minPos) / range) * innerH;
                        return (
                            <g key={i}>
                                <line
                                    x1={padL} y1={y} x2={W - padR} y2={y}
                                    stroke="var(--border-soft, rgba(0,0,0,0.08))"
                                    strokeDasharray="2,4"
                                />
                                <text
                                    x={padL - 8} y={y + 4}
                                    textAnchor="end" fontSize="11"
                                    fill="var(--text-tertiary)"
                                    fontFamily="ui-monospace, SFMono-Regular, monospace"
                                >
                                    {Math.round(tv).toLocaleString('ru-RU')}
                                </text>
                            </g>
                        );
                    })}

                    {/* X-axis labels (first, middle, last) */}
                    {points.length > 0 && [0, Math.floor(points.length / 2), points.length - 1].map((idx) => {
                        const x = padL + idx * xStep;
                        return (
                            <text
                                key={idx}
                                x={x} y={H - padB / 2 + 12}
                                textAnchor="middle" fontSize="11"
                                fill="var(--text-tertiary)"
                            >
                                {formatSnapshotDate(points[idx].snapshot_date)}
                            </text>
                        );
                    })}

                    {/* Line */}
                    <path d={linePath} fill="none" stroke="var(--text-primary)" strokeWidth="2" />

                    {/* Dots — buy/sell highlights */}
                    {points.map((p, i) => {
                        const x = padL + i * xStep;
                        const y = padT + innerH - ((p.positions! - minPos) / range) * innerH;
                        const delta = p.delta_positions;
                        const isBuy = delta !== null && delta > 0;
                        const isSell = delta !== null && delta < 0;
                        const color = isBuy ? 'var(--mood-green)' : isSell ? 'var(--mood-red)' : 'var(--text-primary)';
                        const r = isBuy || isSell ? 4 : 2.5;
                        return (
                            <circle
                                key={p.snapshot_date}
                                cx={x} cy={y} r={r}
                                fill={color}
                                stroke="var(--bg-primary)" strokeWidth="1.5"
                            >
                                <title>
                                    {formatSnapshotDate(p.snapshot_date)}: {formatShares(p.positions)} шт
                                    {delta ? `\nΔ ${delta >= 0 ? '+' : ''}${formatShares(delta)} шт` : ''}
                                    {p.delta_amount_rub ? `\n${p.delta_amount_rub >= 0 ? '+' : '−'}${formatRubShort(Math.abs(p.delta_amount_rub))}` : ''}
                                </title>
                            </circle>
                        );
                    })}
                </svg>
            </div>

            {/* Table of all snapshots */}
            <div>
                <div style={{
                    fontSize: 'var(--fs-xs)', fontWeight: 700,
                    letterSpacing: '0.08em', marginBottom: 8,
                    paddingBottom: 4, borderBottom: '1.5px solid var(--text-primary)',
                }}>
                    ВСЕ СНАПШОТЫ
                </div>
                <table style={{
                    width: '100%', borderCollapse: 'collapse',
                    fontSize: 'var(--fs-sm)', fontVariantNumeric: 'tabular-nums',
                }}>
                    <thead>
                        <tr style={{ color: 'var(--text-tertiary)' }}>
                            <th style={{ textAlign: 'left', padding: '6px 8px', fontWeight: 500 }}>Дата</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Штук</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Δ</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>На сумму</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Цена</th>
                            <th style={{ textAlign: 'right', padding: '6px 8px', fontWeight: 500 }}>Доля</th>
                        </tr>
                    </thead>
                    <tbody>
                        {[...data.timeline].reverse().map((p) => {
                            const dColor = !p.delta_positions ? 'var(--text-tertiary)'
                                : p.delta_positions > 0 ? 'var(--mood-green)' : 'var(--mood-red)';
                            return (
                                <tr key={p.snapshot_date} style={{ borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.05))' }}>
                                    <td style={{ padding: '6px 8px' }}>{formatSnapshotDate(p.snapshot_date)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>{formatShares(p.positions)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right', color: dColor, fontWeight: 600 }}>
                                        {p.delta_positions === null ? '—'
                                            : p.delta_positions === 0 ? '0'
                                            : `${p.delta_positions > 0 ? '+' : ''}${formatShares(p.delta_positions)}`}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {formatRubShort(p.amount_rub)}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {p.price_rub !== null ? `${p.price_rub.toFixed(2)} ₽` : '—'}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {p.weight !== null ? `${p.weight.toFixed(2)}%` : '—'}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </>
    );
}

function SummaryStat({
    label, value, sub, color,
}: {
    label: string;
    value: string;
    sub?: string;
    color?: string;
}) {
    return (
        <div>
            <div style={{
                fontSize: 'var(--fs-xs)', fontWeight: 700,
                letterSpacing: '0.08em', color: 'var(--text-tertiary)',
                marginBottom: 4,
            }}>
                {label}
            </div>
            <div style={{
                fontSize: 'var(--fs-lg)', fontWeight: 700,
                fontVariantNumeric: 'tabular-nums',
                color: color || 'var(--text-primary)',
            }}>
                {value}
            </div>
            {sub && (
                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-tertiary)', marginTop: 2 }}>
                    {sub}
                </div>
            )}
        </div>
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
}: {
    title: string;
    icon: typeof TrendingUp;
    color: string;
    items: FundTradesMovers['top_accumulated'];
    empty: string;
    negative?: boolean;
    metric: 'weight' | 'amount';
}) {
    // Значение по выбранной метрике: % веса (Δвеса) или объём ₽ (Δсуммы).
    const valOf = (m: FundTradesMovers['top_accumulated'][number]) =>
        metric === 'amount' ? m.total_delta_amount : m.total_delta_weight;
    const fmtVal = (v: number) => metric === 'amount'
        ? `${v > 0 ? '+' : '−'}${formatRubShort(Math.abs(v))}`
        : `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
    // Гистограмма: ширина бара ∝ |значение| относительно максимума в колонке.
    const maxAbs = Math.max(...items.map((m) => Math.abs(valOf(m))), 0.0001);
    return (
        <div
            style={{
                background: 'var(--bg-secondary)',
                border: '1.5px solid var(--border-color)',
                borderRadius: 12,
                padding: 16,
            }}
        >
            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    marginBottom: 12,
                    paddingBottom: 10,
                    borderBottom: `1px solid ${color}`,
                }}
            >
                <Icon size={18} color={color} strokeWidth={2.2} />
                <h3
                    style={{
                        fontSize: 'var(--fs-md)',
                        fontWeight: 800,
                        color,
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
                        return (
                        <div
                            key={m.akey}
                            style={{
                                padding: '9px 0',
                                borderBottom: i === items.length - 1
                                    ? 'none'
                                    : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                            }}
                        >
                            {/* Верх: ранг + имя + значение (% веса или ₽) */}
                            <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
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
                                <span
                                    style={{
                                        flex: 1,
                                        minWidth: 0,
                                        fontSize: 'var(--fs-sm)',
                                        color: 'var(--text-primary)',
                                        fontWeight: 600,
                                        lineHeight: 1.3,
                                        overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap',
                                    }}
                                >
                                    {m.asset_name}
                                </span>
                                <span
                                    style={{
                                        fontFamily: 'ui-monospace, "SF Mono", monospace',
                                        fontSize: 'var(--fs-sm)',
                                        fontWeight: 800,
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
                                    <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 3 }} />
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
