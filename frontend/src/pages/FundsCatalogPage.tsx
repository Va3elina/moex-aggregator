import { useEffect, useState, useMemo } from 'react';
import { TrendingUp, TrendingDown, LayoutGrid } from 'lucide-react';
import { getFundsCatalog, getFundHoldings, type CatalogFund, type FundHoldingsResponse } from '../services/api';
import { UK_LOGOS, CATEGORY_LABELS, DONUT_COLORS } from '../config/fundConfig';

// Мини donut chart component
function MiniDonut({ holdings, size = 72 }: { holdings: { name: string; weight: number }[]; size?: number }) {
    if (!holdings.length) return null;
    const r = size / 2, outerR = r - 2, innerR = r * 0.55;
    let cumAngle = -90;
    const total = holdings.reduce((s, h) => s + h.weight, 0);

    return (
        <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size}>
            {holdings.map((h, i) => {
                const angle = (h.weight / total) * 360;
                const startRad = (cumAngle * Math.PI) / 180;
                const endRad = ((cumAngle + angle) * Math.PI) / 180;
                cumAngle += angle;
                const x1 = r + outerR * Math.cos(startRad), y1 = r + outerR * Math.sin(startRad);
                const x2 = r + outerR * Math.cos(endRad), y2 = r + outerR * Math.sin(endRad);
                const ix1 = r + innerR * Math.cos(endRad), iy1 = r + innerR * Math.sin(endRad);
                const ix2 = r + innerR * Math.cos(startRad), iy2 = r + innerR * Math.sin(startRad);
                const large = angle > 180 ? 1 : 0;
                const d = `M ${x1} ${y1} A ${outerR} ${outerR} 0 ${large} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${innerR} ${innerR} 0 ${large} 0 ${ix2} ${iy2} Z`;
                return <path key={i} d={d} fill={DONUT_COLORS[i % DONUT_COLORS.length]} stroke="#121523" strokeWidth="1" />;
            })}
        </svg>
    );
}

export default function FundsCatalogPage() {
    const [funds, setFunds] = useState<CatalogFund[]>([]);
    const [loading, setLoading] = useState(true);
    const [categoryFilter, setCategoryFilter] = useState<string>('all');
    const [ukFilter, setUkFilter] = useState<string>('all');
    const [search] = useState('');
    const [expandedFund, setExpandedFund] = useState<number | null>(null);
    const [expandedHoldings, setExpandedHoldings] = useState<FundHoldingsResponse | null>(null);

    useEffect(() => {
        getFundsCatalog()
            .then(data => setFunds(data.funds))
            .catch(console.error)
            .finally(() => setLoading(false));
    }, []);

    const filteredFunds = useMemo(() => {
        return funds.filter(f => {
            // Скрываем фонды без состава и золото
            if (f.holdings_count === 0) return false;
            if (f.category === 'gold') return false;
            if (categoryFilter !== 'all' && f.category !== categoryFilter) return false;
            if (ukFilter !== 'all' && f.uk_id !== ukFilter) return false;
            if (search) {
                const q = search.toLowerCase();
                if (!f.ticker.toLowerCase().includes(q) && !f.name.toLowerCase().includes(q)) return false;
            }
            return true;
        });
    }, [funds, categoryFilter, ukFilter, search]);

    // Уникальные УК для фильтра
    const ukIds = useMemo(() => {
        const set = new Set<string>();
        funds.forEach(f => f.uk_id && set.add(f.uk_id));
        return Array.from(set).sort();
    }, [funds]);

    const handleExpandFund = async (fundId: number) => {
        if (expandedFund === fundId) {
            setExpandedFund(null);
            return;
        }
        setExpandedFund(fundId);
        try {
            const data = await getFundHoldings(fundId);
            setExpandedHoldings(data);
        } catch {
            setExpandedHoldings({ fund_id: fundId, holdings: [] });
        }
    };

    const formatNav = (nav: number | null) => {
        if (!nav) return '—';
        if (nav >= 1e12) return `${(nav / 1e12).toFixed(1)} трлн ₽`;
        if (nav >= 1e9) return `${(nav / 1e9).toFixed(1)} млрд ₽`;
        return `${(nav / 1e6).toFixed(0)} млн ₽`;
    };

    const ReturnBadge = ({ value, label }: { value: number | null; label: string }) => {
        if (value === null) return null;
        const isPositive = value >= 0;
        return (
            <div className={`flex items-center gap-1 text-xs font-mono ${isPositive ? 'text-[#2EE59D]' : 'text-[#FF4D4D]'}`}>
                {isPositive ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                <span>{isPositive ? '+' : ''}{value.toFixed(1)}%</span>
                <span className="text-theme-secondary">{label}</span>
            </div>
        );
    };

    return (
        <div className="max-w-7xl mx-auto px-4 py-6 min-h-screen">
            {/* Header */}
            <div className="flex items-center gap-3 mb-6">
                <div className="w-12 h-12 bg-gradient-to-br from-[#6366f1] to-[#8B5CF6] rounded-xl flex items-center justify-center">
                    <LayoutGrid size={24} className="text-white" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold">Каталог фондов</h1>
                    <p className="text-theme-secondary text-sm">Все фонды с составом и доходностью</p>
                </div>
            </div>

            {/* Filters */}
            <div className="flex flex-wrap items-center gap-3 mb-6">
                {/* Category */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setCategoryFilter('all')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${categoryFilter === 'all' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                    >Все</button>
                    {Object.entries(CATEGORY_LABELS).filter(([key]) => key !== 'gold').map(([key, label]) => (
                        <button
                            key={key}
                            onClick={() => setCategoryFilter(key)}
                            className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${categoryFilter === key ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                        >{label}</button>
                    ))}
                </div>

                {/* UK */}
                <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setUkFilter('all')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${ukFilter === 'all' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                    >Все УК</button>
                    {ukIds.map(id => {
                        const uk = UK_LOGOS[id];
                        return uk ? (
                            <button
                                key={id}
                                onClick={() => setUkFilter(id)}
                                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5 ${ukFilter === id ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                            >
                                <div className="w-4 h-4 rounded text-[8px] font-black flex items-center justify-center" style={{ backgroundColor: uk.bg, color: uk.color }}>{uk.letter}</div>
                                {uk.name}
                            </button>
                        ) : null;
                    })}
                </div>

            </div>

            {/* Stats */}
            <div className="text-sm text-theme-secondary mb-4">
                {filteredFunds.length} из {funds.length} фондов
            </div>

            {/* Loading */}
            {loading && (
                <div className="flex justify-center py-20">
                    <div className="w-8 h-8 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                </div>
            )}

            {/* Grid */}
            {!loading && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {filteredFunds.map(fund => {
                        const uk = fund.uk_id ? UK_LOGOS[fund.uk_id] : null;
                        const isExpanded = expandedFund === fund.fund_id;
                        const topHoldings = fund.top_holdings || [];

                        return (
                            <div
                                key={fund.fund_id}
                                className={`bg-theme-secondary rounded-2xl border transition-all cursor-pointer hover:border-[#6366f1]/50 ${isExpanded ? 'border-[#6366f1] ring-1 ring-[#6366f1]/20' : 'border-theme'}`}
                                onClick={() => handleExpandFund(fund.fund_id)}
                            >
                                {/* Header: compact — logo + ticker + name */}
                                <div className="px-4 pt-4 pb-2 flex items-center gap-2.5">
                                    {uk && (
                                        <div className="w-8 h-8 rounded-lg flex items-center justify-center font-black text-xs flex-shrink-0"
                                            style={{ backgroundColor: uk.bg, color: uk.color }}>
                                            {uk.letter}
                                        </div>
                                    )}
                                    <div className="min-w-0 flex-1">
                                        <div className="font-bold text-sm">{fund.ticker}</div>
                                        <div className="text-[11px] text-theme-secondary truncate">{fund.name}</div>
                                    </div>
                                    <div className="text-right flex-shrink-0">
                                        <div className="text-sm font-mono font-bold">{formatNav(fund.last_nav)}</div>
                                        <ReturnBadge value={fund.return_1y} label="1г" />
                                    </div>
                                </div>

                                {/* Large donut — центральный элемент */}
                                <div className="flex justify-center py-2">
                                    <MiniDonut holdings={topHoldings} size={160} />
                                </div>

                                {/* Top 5 legend */}
                                <div className="px-4 pb-3 space-y-1">
                                    {topHoldings.slice(0, 5).map((h, i) => (
                                        <div key={i} className="flex items-center gap-2 text-xs">
                                            <div className="w-2 h-2 rounded-sm flex-shrink-0" style={{ backgroundColor: DONUT_COLORS[i] }} />
                                            <span className="truncate flex-1 text-theme-secondary">{h.name}</span>
                                            <span className="font-mono flex-shrink-0">{(h.weight * 100).toFixed(1)}%</span>
                                        </div>
                                    ))}
                                </div>

                                {/* Expanded: full holdings list */}
                                {isExpanded && expandedHoldings && expandedHoldings.holdings.length > 0 && (
                                    <div className="border-t border-theme px-4 py-3">
                                        <div className="text-xs font-semibold text-theme-secondary mb-2">
                                            Полный состав ({expandedHoldings.holdings.length})
                                        </div>
                                        <div className="space-y-1 max-h-64 overflow-y-auto pr-1">
                                            {expandedHoldings.holdings.map((h, i) => (
                                                <div key={i} className="flex items-center gap-2 text-xs">
                                                    <div className="w-2 h-2 rounded-sm flex-shrink-0" style={{ backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length] }} />
                                                    <span className="truncate flex-1">{h.name}</span>
                                                    <span className="font-mono text-theme-secondary">{(h.weight * 100).toFixed(1)}%</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
