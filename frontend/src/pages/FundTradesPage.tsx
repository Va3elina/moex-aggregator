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
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
    Lock,
    Sparkles,
    Wallet,
    TrendingUp,
    TrendingDown,
    Calendar,
    Activity,
    ArrowLeftRight,
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
import SimpleChart, { type ChartAnnotation } from '../components/SimpleChart';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import { UK_LOGOS, DONUT_COLORS } from '../config/fundConfig';
import Donut from '../components/funds/Donut';
import CompanyFlowsTab from '../components/fundtrades/CompanyFlowsTab';

type Tab = 'funds' | 'movers' | 'snapshots' | 'company';

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
// Fund Detail Modal — объём + график доходности + donut состава
// ════════════════════════════════════════════════════════════════════

function FundDetailModal({
    ticker,
    period,
    listFund,
    onClose,
}: {
    ticker: string;
    period: FundTradesPeriod;
    listFund?: FundWithHistory | null;
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
                            {/* (2) Объём — полная СЧА (AUM) крупно */}
                            <div style={{ marginBottom: 20 }}>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-2xs)',
                                        color: 'var(--text-muted)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.06em',
                                        fontWeight: 700,
                                        marginBottom: 2,
                                    }}
                                >
                                    Объём (СЧА)
                                </div>
                                <div
                                    style={{
                                        fontSize: 'var(--fs-2xl)',
                                        fontWeight: 800,
                                        fontVariantNumeric: 'tabular-nums',
                                        color: 'var(--text-primary)',
                                        lineHeight: 1.1,
                                    }}
                                >
                                    {listFund?.nav_rub != null ? formatRubShort(listFund.nav_rub) : '—'}
                                </div>
                                {data.current_snapshot_date && (
                                    <div
                                        style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: 5,
                                            marginTop: 6,
                                            fontSize: 'var(--fs-2xs)',
                                            color: 'var(--text-muted)',
                                        }}
                                    >
                                        <Calendar size={12} style={{ flexShrink: 0 }} />
                                        Состав на {data.current_snapshot_date}
                                    </div>
                                )}
                            </div>

                            {/* (3) График доходности (СЧА на пай) + плашки returns */}
                            {(() => {
                                const perf = data.performance;
                                const ret = perf?.returns ?? listFund?.returns ?? null;
                                const chartData = (perf?.timeline ?? [])
                                    .filter((p) => p.pay != null)
                                    .map((p) => ({ time: p.date, value: p.pay }));
                                return (
                                    <div style={{ marginBottom: 24 }}>
                                        <h3
                                            style={{
                                                fontSize: 'var(--fs-md)',
                                                fontWeight: 700,
                                                color: 'var(--text-primary)',
                                                marginBottom: 10,
                                            }}
                                        >
                                            Доходность пая
                                        </h3>
                                        {chartData.length > 1 ? (
                                            <SimpleChart
                                                data={chartData}
                                                height={300}
                                                primaryLabel="СЧА на пай, ₽"
                                                legendPosition="top"
                                                formatValue={(v) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`}
                                                formatPrimaryAxis={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: v >= 100 ? 0 : 2 })}
                                                formatTime={formatMonthYearShort}
                                                tooltipDateFormat={formatMonthYearShort}
                                                clampEdgeLabels
                                                showValueHeader={false}
                                                showDownloadButton={false}
                                            />
                                        ) : (
                                            <div
                                                style={{
                                                    padding: '24px 16px',
                                                    textAlign: 'center',
                                                    color: 'var(--text-muted)',
                                                    fontSize: 'var(--fs-sm)',
                                                    border: '1px solid var(--border-color)',
                                                    borderRadius: 10,
                                                    background: 'var(--bg-secondary)',
                                                }}
                                            >
                                                Недостаточно истории для графика доходности
                                            </div>
                                        )}

                                        {/* Плашки returns 1м/3м/6м/1г */}
                                        <div
                                            style={{
                                                display: 'grid',
                                                gridTemplateColumns: 'repeat(auto-fit, minmax(72px, 1fr))',
                                                gap: 8,
                                                marginTop: 12,
                                            }}
                                        >
                                            {[
                                                { label: '1 мес', v: ret?.m1 },
                                                { label: '3 мес', v: ret?.m3 },
                                                { label: '6 мес', v: ret?.m6 },
                                                { label: '1 год', v: ret?.y1 },
                                            ].map(({ label, v }) => (
                                                <div
                                                    key={label}
                                                    style={{
                                                        padding: '10px 12px',
                                                        background: 'var(--bg-secondary)',
                                                        borderRadius: 8,
                                                        border: '1px solid var(--border-color)',
                                                    }}
                                                >
                                                    <div
                                                        style={{
                                                            fontSize: 'var(--fs-2xs)',
                                                            color: 'var(--text-muted)',
                                                            textTransform: 'uppercase',
                                                            letterSpacing: '0.04em',
                                                            fontWeight: 600,
                                                            marginBottom: 3,
                                                        }}
                                                    >
                                                        {label}
                                                    </div>
                                                    <div
                                                        style={{
                                                            fontSize: 'var(--fs-md)',
                                                            fontWeight: 800,
                                                            fontVariantNumeric: 'tabular-nums',
                                                            color: returnColor(v),
                                                        }}
                                                    >
                                                        {formatReturnPct(v)}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                        {listFund?.has_distributions && (
                                            <div style={{ marginTop: 10, fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', lineHeight: 1.4 }}>
                                                Доходность — полная, с&nbsp;учётом выплат дохода (по&nbsp;данным Cbonds).
                                                График показывает цену пая — она снижается в&nbsp;даты выплат.
                                            </div>
                                        )}
                                    </div>
                                );
                            })()}

                            {/* (4) Donut состава + список топ-позиций */}
                            <h3
                                style={{
                                    fontSize: 'var(--fs-md)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 12,
                                }}
                            >
                                Состав фонда
                            </h3>
                            {data.current_holdings.length > 0 ? (
                                <div
                                    style={{
                                        display: 'flex',
                                        gap: 24,
                                        flexWrap: 'wrap',
                                        alignItems: 'flex-start',
                                    }}
                                >
                                    <div style={{ flexShrink: 0, margin: '0 auto', lineHeight: 0 }}>
                                        <Donut
                                            holdings={data.current_holdings.map((h) => ({
                                                name: h.asset_name,
                                                weight: (h.weight ?? 0) / 100,
                                            }))}
                                            size={180}
                                        />
                                    </div>
                                    <div style={{ flex: 1, minWidth: 240 }}>
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
                                                {data.current_holdings.slice(0, 30).map((h, i) => (
                                                    <tr key={h.asset_name}>
                                                        <td style={{
                                                            padding: '7px 12px',
                                                            borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)',
                                                            color: 'var(--text-primary)',
                                                        }}>
                                                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                                                                <span
                                                                    style={{
                                                                        width: 8,
                                                                        height: 8,
                                                                        borderRadius: '50%',
                                                                        flexShrink: 0,
                                                                        backgroundColor: i < 10
                                                                            ? DONUT_COLORS[i % DONUT_COLORS.length]
                                                                            : 'var(--text-muted)',
                                                                    }}
                                                                />
                                                                {h.asset_name}
                                                            </span>
                                                        </td>
                                                        <td style={{
                                                            padding: '7px 12px',
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
                                    </div>
                                </div>
                            ) : (
                                <div
                                    style={{
                                        padding: '24px 16px',
                                        textAlign: 'center',
                                        color: 'var(--text-muted)',
                                        fontSize: 'var(--fs-sm)',
                                    }}
                                >
                                    Состав не публикуется
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
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
    // Сортировка карточек «Состав фондов»: по доходности 1г (default) / объёму / имени.
    const [fundSort, setFundSort] = useState<FundSortKey>('return');

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
            // Авторские (блогерские) фонды — отдельной группой, остальные по категории
            const key = f.subcategory === 'Авторские' ? 'Авторские' : (f.category || 'other');
            if (!groups[key]) groups[key] = [];
            groups[key].push(f);
        }
        // Сортировка внутри группы. null'ы (нет данных) всегда в хвост.
        const cmp = (a: FundWithHistory, b: FundWithHistory): number => {
            if (fundSort === 'name') return a.ticker.localeCompare(b.ticker);
            const av = fundSort === 'return' ? a.returns?.y1 ?? null : a.nav_rub;
            const bv = fundSort === 'return' ? b.returns?.y1 ?? null : b.nav_rub;
            if (av === null && bv === null) return a.ticker.localeCompare(b.ticker);
            if (av === null) return 1;
            if (bv === null) return -1;
            return bv - av; // DESC
        };
        for (const k of Object.keys(groups)) groups[k] = [...groups[k]].sort(cmp);
        return groups;
    }, [funds, fundSort]);

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
                    {' '}<strong style={{ color: 'var(--text-primary)' }}>Методология в тестировании
                    и может измениться.</strong>
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
                        { id: 'company' as const, label: 'Потоки по компании', icon: ArrowLeftRight },
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
                    {/* Контрол сортировки карточек */}
                    {funds.length > 0 && (
                        <div
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 8,
                                flexWrap: 'wrap',
                                marginBottom: 18,
                            }}
                        >
                            <span
                                style={{
                                    fontSize: 'var(--fs-2xs)',
                                    fontWeight: 700,
                                    textTransform: 'uppercase',
                                    letterSpacing: '0.06em',
                                    color: 'var(--text-muted)',
                                }}
                            >
                                Сортировка
                            </span>
                            {([
                                { id: 'return' as const, label: 'Доходность 1г' },
                                { id: 'volume' as const, label: 'Объём' },
                                { id: 'name' as const, label: 'Имя' },
                            ]).map((s) => {
                                const active = fundSort === s.id;
                                return (
                                    <button
                                        key={s.id}
                                        onClick={() => setFundSort(s.id)}
                                        className="editorial-press"
                                        style={{
                                            padding: '6px 14px',
                                            background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                            color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                            border: '2px solid var(--text-primary)',
                                            borderRadius: 999,
                                            fontSize: 'var(--fs-xs)',
                                            fontWeight: active ? 700 : 600,
                                            cursor: 'pointer',
                                            boxShadow: active ? '3px 3px 0 var(--text-primary)' : 'none',
                                        }}
                                    >
                                        {s.label}
                                    </button>
                                );
                            })}
                        </div>
                    )}
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
                                        {/* Header: УК-аватар + тикер + имя */}
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
                                                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                                        fontSize: 'var(--fs-md)',
                                                        fontWeight: 800,
                                                        color: 'var(--text-primary)',
                                                        marginBottom: 2,
                                                    }}
                                                >
                                                    {f.ticker}
                                                </div>
                                                <div
                                                    style={{
                                                        fontSize: 'var(--fs-xs)',
                                                        color: 'var(--text-secondary)',
                                                        lineHeight: 1.4,
                                                        display: '-webkit-box',
                                                        WebkitLineClamp: 2,
                                                        WebkitBoxOrient: 'vertical',
                                                        overflow: 'hidden',
                                                    }}
                                                >
                                                    {f.name}
                                                </div>
                                            </div>
                                        </div>

                                        {/* Body: donut состава + топ-5 позиций */}
                                        <div
                                            style={{
                                                display: 'flex',
                                                gap: 12,
                                                alignItems: 'center',
                                                marginTop: 12,
                                            }}
                                        >
                                            {f.top_holdings && f.top_holdings.length > 0 ? (
                                                <div style={{ flexShrink: 0, lineHeight: 0 }}>
                                                    <Donut
                                                        holdings={(() => {
                                                            // top_holdings = только топ-10 (бэкенд обрезает);
                                                            // weight в процентах. Добавляем «Прочее» = 100 − Σтоп,
                                                            // иначе донат нормализует топ-10 как 100% и завышает
                                                            // концентрацию фонда.
                                                            const sum = f.top_holdings.reduce((s, h) => s + (h.weight || 0), 0);
                                                            const other = 100 - sum;
                                                            return other > 1
                                                                ? [...f.top_holdings, { name: 'Прочее', weight: other }]
                                                                : f.top_holdings;
                                                        })()}
                                                        size={84}
                                                        outerRadius={72}
                                                        innerRadius={46}
                                                        showCenterText={false}
                                                    />
                                                </div>
                                            ) : (
                                                <div
                                                    style={{
                                                        width: 84,
                                                        height: 84,
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
                                            <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 3 }}>
                                                {(f.top_holdings ?? []).slice(0, 5).map((h, i) => (
                                                    <div
                                                        key={h.name + i}
                                                        style={{
                                                            display: 'flex',
                                                            alignItems: 'center',
                                                            gap: 6,
                                                            fontSize: 'var(--fs-2xs)',
                                                        }}
                                                    >
                                                        <span
                                                            style={{
                                                                width: 7,
                                                                height: 7,
                                                                borderRadius: '50%',
                                                                flexShrink: 0,
                                                                backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length],
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
                                                            }}
                                                        >
                                                            {h.name}
                                                        </span>
                                                        <span
                                                            style={{
                                                                flexShrink: 0,
                                                                fontVariantNumeric: 'tabular-nums',
                                                                fontWeight: 600,
                                                                color: 'var(--text-primary)',
                                                            }}
                                                        >
                                                            {h.weight.toFixed(1)}%
                                                        </span>
                                                    </div>
                                                ))}
                                                {(!f.top_holdings || f.top_holdings.length === 0) && (
                                                    <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>
                                                        Состав не публикуется
                                                    </span>
                                                )}
                                            </div>
                                        </div>

                                        {/* Footer: доходность 1г + meta */}
                                        <div
                                            style={{
                                                display: 'flex',
                                                alignItems: 'flex-end',
                                                justifyContent: 'space-between',
                                                marginTop: 12,
                                                paddingTop: 10,
                                                borderTop: '1px solid var(--border-color)',
                                            }}
                                        >
                                            <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                                                <span
                                                    style={{
                                                        fontSize: 'var(--fs-lg)',
                                                        fontWeight: 800,
                                                        fontVariantNumeric: 'tabular-nums',
                                                        color: returnColor(f.returns?.y1),
                                                        lineHeight: 1.1,
                                                    }}
                                                >
                                                    {formatReturnPct(f.returns?.y1)}
                                                </span>
                                                <span
                                                    style={{
                                                        fontSize: 'var(--fs-2xs)',
                                                        color: 'var(--text-muted)',
                                                        textTransform: 'uppercase',
                                                        letterSpacing: '0.04em',
                                                    }}
                                                >
                                                    1 год
                                                </span>
                                            </div>
                                            <span
                                                style={{
                                                    fontSize: 'var(--fs-2xs)',
                                                    color: 'var(--text-muted)',
                                                    fontVariantNumeric: 'tabular-nums',
                                                }}
                                            >
                                                {f.nav_rub != null ? formatRubShort(f.nav_rub) : (f.last_snapshot_date || '—')}
                                            </span>
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
                        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
                            {([['weight', '% веса'], ['amount', 'Объём ₽']] as const).map(([key, lbl]) => {
                                const on = metric === key;
                                return (
                                    <button
                                        key={key}
                                        onClick={() => setMetric(key)}
                                        className="editorial-press"
                                        style={{
                                            padding: '6px 14px',
                                            background: on ? 'var(--accent)' : 'var(--bg-secondary)',
                                            color: on ? 'var(--text-inverse)' : 'var(--text-primary)',
                                            border: '2px solid var(--text-primary)',
                                            borderRadius: 999,
                                            fontSize: 'var(--fs-xs)',
                                            fontWeight: on ? 700 : 600,
                                            cursor: 'pointer',
                                            boxShadow: on ? '3px 3px 0 var(--text-primary)' : 'none',
                                        }}
                                    >
                                        {lbl}
                                    </button>
                                );
                            })}
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

            {tab === 'company' && <CompanyFlowsTab />}

            {tab === 'snapshots' && <SnapshotReviewTab />}

            {selectedTicker && (
                <FundDetailModal
                    ticker={selectedTicker}
                    period={period}
                    listFund={funds.find((f) => f.ticker === selectedTicker) ?? null}
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

// Доходность в %: «+12.3%» / «−4.1%» / «—». Знак «−» — типографский минус.
function formatReturnPct(v: number | null | undefined): string {
    if (v === null || v === undefined) return '—';
    const sign = v > 0 ? '+' : v < 0 ? '−' : '';
    return `${sign}${Math.abs(v).toFixed(1)}%`;
}

// Цвет доходности: зелёный для роста, красный для падения, нейтральный для 0/—.
function returnColor(v: number | null | undefined): string {
    if (v === null || v === undefined || v === 0) return 'var(--text-muted)';
    return v > 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)';
}

// Ключ сортировки карточек «Состав фондов».
type FundSortKey = 'return' | 'volume' | 'name';

// Компактный формат для меток Y-оси графика — полные числа ("5 683 220")
// не влезают слева и клиппятся, поэтому на оси даём "5,7 млн" / "339 тыс".
// В тултипе остаётся полное число (formatValue).
function formatSharesCompact(positions: number): string {
    const abs = Math.abs(positions);
    if (abs >= 1e9) return `${(positions / 1e9).toLocaleString('ru-RU', { maximumFractionDigits: 1 })} млрд`;
    if (abs >= 1e6) return `${(positions / 1e6).toLocaleString('ru-RU', { maximumFractionDigits: 1 })} млн`;
    if (abs >= 1e3) return `${(positions / 1e3).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} тыс`;
    return Math.round(positions).toLocaleString('ru-RU');
}

// Детект сплита акций в истории позиций + back-adjustment (как экспирация/ролловер
// фьючерсов на OI). Сплит: количество ×R, цена ÷R, СТОИМОСТЬ непрерывна (amount ≈ const)
// — именно это отличает сплит от реальной покупки (там сумма растёт ~×R). Без коррекции
// один снапшот после сплита «выстреливает» в N× и сплющивает историю. Возвращаем
// множитель на дату (история домножается до текущего масштаба, последние = raw) +
// маркеры «Сплит» для SimpleChart.
// Стандартные коэффициенты сплита. Берём ближайший (в лог-шкале) к наблюдаемому,
// а НЕ сырое отношение количеств — иначе реальная торговля на границе сплита
// «съедается» в ноль (множитель ровно подгонял бы pre к post). T 1:10: наблюдаем
// ~10.13 (шум от продажи фонда + движения цены) → берём 10, и Δ показывает реальный сдвиг.
const SPLIT_RATIOS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 100, 150, 200, 500, 1000];
function nearestSplitRatio(observed: number): number {
    const cands = observed >= 1 ? SPLIT_RATIOS : SPLIT_RATIOS.map((r) => 1 / r);
    let best = cands[0], bestD = Infinity;
    for (const c of cands) {
        const d = Math.abs(Math.log(observed) - Math.log(c));
        if (d < bestD) { bestD = d; best = c; }
    }
    return best;
}

function splitAdjustPositions(
    timeline: AssetHistory['timeline'],
): { factorByDate: Map<string, number>; annotations: ChartAnnotation[] } {
    const chrono = [...timeline].sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
    const factorByDate = new Map<string, number>();
    const splits: { date: string; ratio: number }[] = [];
    const splitRatio = (a: AssetHistory['timeline'][number], b: AssetHistory['timeline'][number]): number => {
        if (!a.positions || !b.positions || !a.price_rub || !b.price_rub) return 0;
        const posR = b.positions / a.positions;
        const priceR = a.price_rub / b.price_rub;
        const amtR = a.amount_rub && b.amount_rub ? b.amount_rub / a.amount_rub : 1;
        // value непрерывна + кол-во и цена двигаются согласованно (posR ≈ priceR)
        const ok = Math.abs(amtR - 1) < 0.4 && Math.abs(posR / priceR - 1) < 0.4;
        // Множитель = ближайший стандартный коэффициент к geomean(posR,priceR)
        // (geomean балансирует шум от торговли фонда и движения цены).
        if (ok && posR > 1.8 && priceR > 1.8) return nearestSplitRatio(Math.sqrt(posR * priceR));   // прямой
        if (ok && posR < 0.55 && priceR < 0.55) return nearestSplitRatio(Math.sqrt(posR * priceR)); // обратный
        return 0;
    };
    let factor = 1;
    for (let i = chrono.length - 1; i >= 0; i--) {
        factorByDate.set(chrono[i].snapshot_date, factor);
        if (i > 0) {
            const R = splitRatio(chrono[i - 1], chrono[i]);
            if (R) { splits.push({ date: chrono[i].snapshot_date, ratio: R }); factor *= R; }
        }
    }
    const annotations: ChartAnnotation[] = splits.map((s) => {
        const n = s.ratio >= 1 ? Math.round(s.ratio) : Math.round(1 / s.ratio);
        return {
            time: s.date,
            label: 'Сплит',
            description: `Сплит акций ~1:${n}${s.ratio < 1 ? ' (обратный)' : ''} · ${formatMonthYearShort(s.date)}. Кол-во и цена в графике/таблице скорректированы под сплит.`,
            color: 'var(--accent)',
            textColor: 'var(--text-inverse)',
        };
    });
    return { factorByDate, annotations };
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

// "2025-08-29" → "авг 2025" — компактный месяц+год для оси и тултипа графика.
function formatMonthYearShort(iso: string): string {
    const d = new Date(iso);
    const mm = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
    return `${mm[d.getMonth()]} ${d.getFullYear()}`;
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
}: {
    label: string;
    subLabel?: string;
    amount: number;
    maxAbs: number;
    isPositive: boolean;
    onClick?: () => void;
    formatValue?: (absValue: number) => string;
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
                {isPositive ? '+' : '−'}{fmt(Math.abs(amount))}
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
    // Переключатель метрики (как в «Покупки фондов»): сортировка/бары по объёму ₽ или по доле.
    const [metric, setMetric] = useState<'amount' | 'weight'>('amount');
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

            {/* Переключатель метрики (как в «Покупки фондов») */}
            {review.previous_snapshot_date && (
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: -16, marginBottom: -8 }}>
                    {([['amount', 'Объём ₽'], ['weight', '% веса']] as const).map(([key, lbl]) => {
                        const on = metric === key;
                        return (
                            <button
                                key={key}
                                onClick={() => setMetric(key)}
                                className="editorial-press"
                                style={{
                                    padding: '6px 14px',
                                    background: on ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: on ? 'var(--text-inverse)' : 'var(--text-primary)',
                                    border: '2px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-xs)',
                                    fontWeight: on ? 700 : 600,
                                    cursor: 'pointer',
                                    boxShadow: on ? '3px 3px 0 var(--text-primary)' : 'none',
                                }}
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
                    total={isW ? sumBy(addedItems, wDelta) : review.totals.total_added_rub}
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
                    total={isW ? Math.abs(sumBy(reducedItems, wDelta)) : Math.abs(review.totals.total_reduced_rub)}
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
                    total={isW ? sumBy(review.new, wNew) : review.totals.total_new_rub}
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
                    total={isW ? Math.abs(sumBy(review.sold_out, wSold)) : review.totals.total_sold_out_rub}
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
                {(expanded ? items : items.slice(0, 3)).map((r) => (
                    <EditorialBar
                        key={`${r.asset_name}-${r.isin || ''}`}
                        label={r.asset_name}
                        subLabel={subLabelGetter(r)}
                        amount={valueGetter(r)}
                        maxAbs={maxAbs}
                        isPositive={isPositive}
                        onClick={onItemClick ? () => onItemClick(r) : undefined}
                        formatValue={formatValue}
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
                    maxWidth: 1240, width: '100%', maxHeight: '92vh', overflow: 'auto',
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

                {data && !loading && <AssetHistoryContent data={data} assetName={asset_name} ticker={ticker} />}
            </div>
        </div>
    );
}

function AssetHistoryContent({ data, assetName, ticker }: { data: AssetHistory; assetName: string; ticker: string }) {
    const chartAnchorRef = useRef<HTMLDivElement>(null);

    // Сплит-коррекция: непрерывная серия (история домножена на множитель сплита) +
    // маркеры «Сплит». Снимает «один снапшот = пик, остальное плоское».
    const { factorByDate, annotations } = useMemo(() => splitAdjustPositions(data.timeline), [data.timeline]);
    const adjPos = (p: AssetHistory['timeline'][number]) =>
        p.positions == null ? null : Math.round(p.positions * (factorByDate.get(p.snapshot_date) ?? 1));
    // adjusted positions/delta/price по дате (delta — в хронологии; цена = raw/множитель,
    // сумма/вес остаются raw → adj_pos × adj_price = amount сходится).
    const adjByDate = useMemo(() => {
        const chrono = [...data.timeline].sort((a, b) => a.snapshot_date.localeCompare(b.snapshot_date));
        const m = new Map<string, { pos: number | null; delta: number | null; price: number | null }>();
        let prev: number | null = null;
        for (const p of chrono) {
            const f = factorByDate.get(p.snapshot_date) ?? 1;
            const pos = p.positions == null ? null : Math.round(p.positions * f);
            const price = p.price_rub == null ? null : p.price_rub / f;
            const delta = prev == null || pos == null ? null : pos - prev;
            m.set(p.snapshot_date, { pos, delta, price });
            if (pos != null) prev = pos;
        }
        return m;
    }, [data.timeline, factorByDate]);

    const points = data.timeline.filter(p => p.positions !== null);
    const firstPos = adjPos(points[0]) ?? 0;
    const lastPos = adjPos(points[points.length - 1]) ?? 0;
    const totalDelta = lastPos - firstPos;
    const totalDeltaColor = totalDelta >= 0 ? 'var(--mood-green)' : 'var(--mood-red)';

    // Данные для SimpleChart: скорректированные позиции (штуки) по снапшотам.
    const chartData = points.map(p => ({ time: p.snapshot_date, value: adjPos(p)! }));

    // Сортировка таблицы «Все снапшоты» — кликабельные колонки.
    const [snapSort, setSnapSort] = useState<'date' | 'positions' | 'delta' | 'amount' | 'price' | 'weight'>('date');
    const [snapDir, setSnapDir] = useState<'asc' | 'desc'>('desc');
    const snapColumns = [
        { key: 'date', label: 'Дата', align: 'left' },
        { key: 'positions', label: 'Штук', align: 'right' },
        { key: 'delta', label: 'Δ', align: 'right' },
        { key: 'amount', label: 'На сумму', align: 'right' },
        { key: 'price', label: 'Цена', align: 'right' },
        { key: 'weight', label: 'Доля', align: 'right' },
    ] as const;
    const sortedTimeline = useMemo(() => {
        const num = (v: number | null) => (v == null ? -Infinity : v);
        const val = (p: AssetHistory['timeline'][number]): string | number => {
            const adj = adjByDate.get(p.snapshot_date);
            switch (snapSort) {
                case 'date': return p.snapshot_date;
                case 'positions': return num(adj?.pos ?? p.positions);
                case 'delta': return num(adj?.delta ?? p.delta_positions);
                case 'amount': return num(p.amount_rub);
                case 'price': return num(adj?.price ?? p.price_rub);
                case 'weight': return num(p.weight);
            }
        };
        return [...data.timeline].sort((a, b) => {
            const av = val(a), bv = val(b);
            const cmp = typeof av === 'string' ? av.localeCompare(bv as string) : (av as number) - (bv as number);
            return snapDir === 'asc' ? cmp : -cmp;
        });
    }, [data.timeline, snapSort, snapDir, adjByDate]);
    const onSnapSort = (key: typeof snapSort) => {
        if (snapSort === key) setSnapDir((d) => (d === 'asc' ? 'desc' : 'asc'));
        else { setSnapSort(key); setSnapDir('desc'); }
    };

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

            {/* График позиций — SimpleChart (интерактивный, accent-линия + hover/crosshair) */}
            {/* Один контейнер — родной SimpleChart (rounded-2xl border bg-primary).
                chartAnchorRef — голая обёртка-цель для html2canvas (без своей рамки,
                иначе двойной контейнер). Камера-экспорт абсолютом ВНУТРИ угла
                контейнера (top/right 16 == SimpleChart top-4/right-4), снаружи
                chartAnchorRef → в snapshot не попадёт. */}
            <div style={{ position: 'relative', marginBottom: 24 }}>
                <div ref={chartAnchorRef}>
                    <SimpleChart
                        data={chartData}
                        height={470}
                        primaryLabel={`${assetName}, шт`}
                        legendPosition="top"
                        formatValue={(v) => formatShares(Math.round(v))}
                        formatPrimaryAxis={formatSharesCompact}
                        formatTime={formatMonthYearShort}
                        tooltipDateFormat={formatMonthYearShort}
                        clampEdgeLabels
                        showValueHeader={false}
                        showDownloadButton={false}
                        annotations={annotations}
                    />
                </div>
                <div data-export-ignore="true" style={{ position: 'absolute', top: 16, right: 16, zIndex: 3 }}>
                    <ChartCaptureButton
                        getTargetElement={() => chartAnchorRef.current}
                        filename={`frame-fund-${ticker}`}
                        metadata={{
                            title: assetName,
                            asset: ticker,
                            details: ['Позиции по снапшотам, шт'],
                        }}
                    />
                </div>
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
                        <tr>
                            {snapColumns.map((c) => {
                                const active = snapSort === c.key;
                                return (
                                    <th
                                        key={c.key}
                                        onClick={() => onSnapSort(c.key)}
                                        style={{
                                            textAlign: c.align,
                                            padding: '6px 8px',
                                            fontWeight: 600,
                                            cursor: 'pointer',
                                            userSelect: 'none',
                                            whiteSpace: 'nowrap',
                                            color: active ? 'var(--text-primary)' : 'var(--text-tertiary)',
                                        }}
                                    >
                                        {c.label}
                                        <span style={{ opacity: active ? 1 : 0.35, marginLeft: 3 }}>
                                            {active ? (snapDir === 'asc' ? '▲' : '▼') : '⇅'}
                                        </span>
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {sortedTimeline.map((p) => {
                            // Скорректированные под сплит значения (raw для сумм/веса).
                            const adj = adjByDate.get(p.snapshot_date);
                            const apos = adj?.pos ?? p.positions;
                            const adelta = adj?.delta ?? p.delta_positions;
                            const aprice = adj?.price ?? p.price_rub;
                            const dColor = !adelta ? 'var(--text-tertiary)'
                                : adelta > 0 ? 'var(--mood-green)' : 'var(--mood-red)';
                            return (
                                <tr key={p.snapshot_date} style={{ borderBottom: '1px solid var(--border-soft, rgba(0,0,0,0.05))' }}>
                                    <td style={{ padding: '6px 8px' }}>{formatSnapshotDate(p.snapshot_date)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>{formatShares(apos)}</td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right', color: dColor, fontWeight: 600 }}>
                                        {adelta === null ? '—'
                                            : adelta === 0 ? '0'
                                            : `${adelta > 0 ? '+' : ''}${formatShares(adelta)}`}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {formatRubShort(p.amount_rub)}
                                    </td>
                                    <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                                        {aprice !== null ? `${aprice.toFixed(2)} ₽` : '—'}
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
