/**
 * FundTradesPage — отслеживание покупок/продаж в БПИФах.
 *
 * Архитектура:
 *   - Pro-only фича (tier-gated через useCommonFeatures().fund_trades_access).
 *   - Два таба: «По фондам» (карточки) + «Топ движений» (агрегаты).
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
 *   - Period picker (1m/3m/6m/1y) сверху страницы — applies к обоим табам
 */
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
    ArrowLeft,
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
    type FundTradesPeriod,
    type FundWithHistory,
    type FundTradesDetail,
    type FundTradesMovers,
    type FundTradeChangeType,
} from '../services/api';
import { useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useAuth } from '../contexts/AuthContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';

type Tab = 'funds' | 'movers';

const CATEGORY_LABEL: Record<string, string> = {
    stocks: 'Акции',
    bonds: 'Облигации',
    money_market: 'Денежный рынок',
    gold: 'Золото',
};

const PERIODS: { id: FundTradesPeriod; label: string }[] = [
    { id: '1m', label: '1 мес' },
    { id: '3m', label: '3 мес' },
    { id: '6m', label: '6 мес' },
    { id: '1y', label: '1 год' },
];

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
                    Отслеживайте куда направляются деньги крупных БПИФов: какие акции,
                    облигации и инструменты управляющие компании накапливают, а что
                    распродают.
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
                            {data.previous_snapshot_date ? (
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
                            ) : (
                                <div
                                    style={{
                                        padding: 14,
                                        marginBottom: 20,
                                        background: 'color-mix(in srgb, var(--accent) 8%, var(--bg-secondary))',
                                        border: '1px solid color-mix(in srgb, var(--accent) 25%, transparent)',
                                        borderRadius: 8,
                                        fontSize: 'var(--fs-sm)',
                                        color: 'var(--text-secondary)',
                                    }}
                                >
                                    <strong style={{ color: 'var(--text-primary)' }}>
                                        Недостаточно истории
                                    </strong>{' '}
                                    — для расчёта дельт нужно ≥2 snapshot'а. Подождите следующего обновления.
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
    const [period, setPeriod] = useState<FundTradesPeriod>('1m');
    const [selectedTicker, setSelectedTicker] = useState<string | null>(null);

    const [funds, setFunds] = useState<FundWithHistory[]>([]);
    const [movers, setMovers] = useState<FundTradesMovers | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Load funds list (один раз).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        setLoading(true);
        listFundsWithHistory()
            .then((r) => setFunds(r.funds))
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [common.fund_trades_access]);

    // Load movers when tab=movers (или меняется period).
    useEffect(() => {
        if (!common.fund_trades_access) return;
        if (tab !== 'movers') return;
        setLoading(true);
        getFundTradesMovers(period)
            .then(setMovers)
            .catch((e: Error) => setError(e.message))
            .finally(() => setLoading(false));
    }, [tab, period, common.fund_trades_access]);

    const fundsByCategory = useMemo(() => {
        const groups: Record<string, FundWithHistory[]> = {};
        for (const f of funds) {
            const key = f.category || 'other';
            if (!groups[key]) groups[key] = [];
            groups[key].push(f);
        }
        return groups;
    }, [funds]);

    if (!common.fund_trades_access) {
        return <LockedView />;
    }

    return (
        <div className="max-w-5xl mx-auto px-4 md:px-6 py-6 md:py-8">
            <Link
                to="/"
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    color: 'var(--text-secondary)',
                    fontSize: 'var(--fs-sm)',
                    marginBottom: 16,
                    textDecoration: 'none',
                }}
            >
                <ArrowLeft size={14} />На главную
            </Link>

            {/* Header */}
            <header className="flex items-center gap-4 mb-6">
                <div
                    style={{
                        width: 48,
                        height: 48,
                        borderRadius: 10,
                        background: 'color-mix(in srgb, var(--accent) 14%, transparent)',
                        color: 'var(--accent)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}
                >
                    <Wallet size={24} strokeWidth={1.8} />
                </div>
                <div>
                    <h1
                        style={{
                            fontSize: 'var(--fs-3xl)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                            letterSpacing: '-0.015em',
                            margin: 0,
                        }}
                    >
                        Что покупают фонды
                    </h1>
                    <p
                        style={{
                            fontSize: 'var(--fs-sm)',
                            color: 'var(--text-secondary)',
                            margin: '4px 0 0',
                        }}
                    >
                        Состав портфелей крупных БПИФов — что управляющие компании
                        накапливают и распродают
                    </p>
                </div>
            </header>

            {/* Tabs + Period picker */}
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
                <div style={{ display: 'flex', gap: 6 }}>
                    {([
                        { id: 'funds' as const, label: 'По фондам', icon: Wallet },
                        { id: 'movers' as const, label: 'Топ движений', icon: Activity },
                    ]).map((t) => {
                        const Icon = t.icon;
                        const active = tab === t.id;
                        return (
                            <button
                                key={t.id}
                                onClick={() => setTab(t.id)}
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 6,
                                    padding: '8px 16px',
                                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: active ? 700 : 600,
                                    cursor: 'pointer',
                                }}
                            >
                                <Icon size={14} />
                                {t.label}
                            </button>
                        );
                    })}
                </div>
                <div style={{ marginLeft: 'auto', display: 'flex', gap: 4, padding: 4, background: 'var(--bg-secondary)', borderRadius: 8 }}>
                    {PERIODS.map((p) => (
                        <button
                            key={p.id}
                            onClick={() => setPeriod(p.id)}
                            style={{
                                padding: '4px 10px',
                                background: period === p.id ? 'var(--bg-primary)' : 'transparent',
                                color: period === p.id ? 'var(--text-primary)' : 'var(--text-secondary)',
                                border: period === p.id
                                    ? '1px solid color-mix(in srgb, var(--text-primary) 18%, transparent)'
                                    : '1px solid transparent',
                                borderRadius: 6,
                                fontSize: 'var(--fs-xs)',
                                fontWeight: period === p.id ? 700 : 600,
                                cursor: 'pointer',
                            }}
                        >
                            {p.label}
                        </button>
                    ))}
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
                        <EmptyState />
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
                                {list.map((f) => (
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
                                    </button>
                                ))}
                            </div>
                        </div>
                    ))}
                </>
            )}

            {tab === 'movers' && (
                <>
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
                            />
                            <MoversColumn
                                title="Топ-распродажа"
                                icon={TrendingDown}
                                color="var(--danger, #ef4444)"
                                items={movers.top_reduced}
                                empty="Распродаж нет"
                                negative
                            />
                        </div>
                    )}
                    {movers
                        && movers.top_accumulated.length === 0
                        && movers.top_reduced.length === 0 && (
                        <EmptyState />
                    )}
                </>
            )}

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

function MoversColumn({
    title,
    icon: Icon,
    color,
    items,
    empty,
    negative,
}: {
    title: string;
    icon: typeof TrendingUp;
    color: string;
    items: FundTradesMovers['top_accumulated'];
    empty: string;
    negative?: boolean;
}) {
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
                    {items.map((m, i) => (
                        <div
                            key={m.asset_name}
                            style={{
                                display: 'grid',
                                gridTemplateColumns: '24px 1fr auto',
                                gap: 10,
                                alignItems: 'center',
                                padding: '8px 0',
                                borderBottom: i === items.length - 1
                                    ? 'none'
                                    : '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                            }}
                        >
                            <span
                                style={{
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-muted)',
                                    fontFamily: 'ui-monospace, monospace',
                                }}
                            >
                                {i + 1}.
                            </span>
                            <div>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-sm)',
                                        color: 'var(--text-primary)',
                                        fontWeight: 600,
                                        lineHeight: 1.3,
                                    }}
                                >
                                    {m.asset_name}
                                </div>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-2xs)',
                                        color: 'var(--text-muted)',
                                        marginTop: 2,
                                    }}
                                >
                                    {negative
                                        ? `${m.funds_selling} продают`
                                        : `${m.funds_buying} покупают`}
                                </div>
                            </div>
                            <span
                                style={{
                                    fontFamily: 'ui-monospace, "SF Mono", monospace',
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 800,
                                    color,
                                }}
                            >
                                {m.total_delta_weight > 0 ? '+' : ''}
                                {m.total_delta_weight.toFixed(2)}%
                            </span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

function EmptyState() {
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
            <Activity size={32} style={{ color: 'var(--text-muted)', margin: '0 auto 12px' }} />
            <p style={{ fontSize: 'var(--fs-md)', fontWeight: 700, color: 'var(--text-primary)', marginBottom: 6 }}>
                История пока копится
            </p>
            <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', maxWidth: 460, margin: '0 auto', lineHeight: 1.5 }}>
                Для расчёта движений нужны минимум 2 snapshot'а от каждой УК. Cbonds публикует данные раз в месяц с лагом
                10-30 дней — первые реальные дельты появятся через месяц.
            </p>
        </div>
    );
}
