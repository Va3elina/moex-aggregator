import { useEffect, useState, useMemo } from 'react';
import { LayoutGrid } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { getFundsCatalog, getFundHoldings, type CatalogFund, type FundHoldingsResponse } from '../services/api';
import { UK_LOGOS, CATEGORY_LABELS, DONUT_COLORS } from '../config/fundConfig';

// Интерактивный donut chart с hover
function InteractiveDonut({ holdings, size = 160 }: { holdings: { name: string; weight: number }[]; size?: number }) {
    const [hovered, setHovered] = useState<number | null>(null);
    if (!holdings.length) return null;

    // Фильтруем нулевые и SNGSP
    const filtered = holdings.filter(h => h.weight > 0 && !h.name.includes('акция прив'));
    if (!filtered.length) return null;

    const r = size / 2, outerR = r - 2, innerR = r * 0.5;
    let cumAngle = -90;
    const total = filtered.reduce((s, h) => s + h.weight, 0);

    const segments = filtered.map((h, i) => {
        const angle = (h.weight / total) * 360;
        const startRad = (cumAngle * Math.PI) / 180;
        const endRad = ((cumAngle + angle) * Math.PI) / 180;
        cumAngle += angle;
        const oR = hovered === i ? outerR + 4 : outerR;
        const iR = hovered === i ? innerR - 2 : innerR;
        const x1 = r + oR * Math.cos(startRad), y1 = r + oR * Math.sin(startRad);
        const x2 = r + oR * Math.cos(endRad), y2 = r + oR * Math.sin(endRad);
        const ix1 = r + iR * Math.cos(endRad), iy1 = r + iR * Math.sin(endRad);
        const ix2 = r + iR * Math.cos(startRad), iy2 = r + iR * Math.sin(startRad);
        const large = angle > 180 ? 1 : 0;
        const d = `M ${x1} ${y1} A ${oR} ${oR} 0 ${large} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${iR} ${iR} 0 ${large} 0 ${ix2} ${iy2} Z`;
        return { d, color: DONUT_COLORS[i % DONUT_COLORS.length], name: h.name, weight: h.weight, index: i };
    });

    return (
        <div className="relative" style={{ width: size + 8, height: size + 8 }}>
            <svg viewBox={`-4 -4 ${size + 8} ${size + 8}`} width={size + 8} height={size + 8}>
                {segments.map((seg) => (
                    <path
                        key={seg.index}
                        d={seg.d}
                        fill={seg.color}
                        stroke="#121523"
                        strokeWidth="1"
                        opacity={hovered === null || hovered === seg.index ? 1 : 0.4}
                        onMouseEnter={() => setHovered(seg.index)}
                        onMouseLeave={() => setHovered(null)}
                        className="transition-opacity duration-150 cursor-pointer"
                    />
                ))}
            </svg>
            {/* Hover tooltip в центре donut */}
            {hovered !== null && (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                    <div className="text-center px-2">
                        <div className="text-xs font-semibold truncate max-w-[100px]">{segments[hovered].name.replace(', акция об.', '')}</div>
                        <div className="text-sm font-mono font-bold" style={{ color: segments[hovered].color }}>
                            {(segments[hovered].weight * 100).toFixed(1)}%
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default function FundsCatalogPage() {
    const [funds, setFunds] = useState<CatalogFund[]>([]);
    const [loading, setLoading] = useState(true);
    const [categoryFilter, setCategoryFilter] = useState<string>('stocks');
    const [ukFilter, setUkFilter] = useState<string>('all');
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
            if (f.holdings_count === 0) return false;
            if (f.category === 'gold' || f.category === 'money_market') return false;
            if (categoryFilter !== 'all' && f.category !== categoryFilter) return false;
            if (ukFilter !== 'all' && f.uk_id !== ukFilter) return false;
            return true;
        });
    }, [funds, categoryFilter, ukFilter]);

    const ukIds = useMemo(() => {
        const set = new Set<string>();
        funds.forEach(f => f.uk_id && f.category !== 'gold' && f.category !== 'money_market' && set.add(f.uk_id));
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

    return (
        <div className="max-w-7xl mx-auto px-4 md:px-6 py-6 md:py-8 min-h-screen">
            <PageHeader
                icon={LayoutGrid}
                title="Состав фондов"
                subtitle="Структура портфелей фондов акций и облигаций"
            />

            {/* Filters */}
            <div className="flex flex-wrap items-center gap-3 mb-6">
                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                    <button
                        onClick={() => setCategoryFilter('all')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${categoryFilter === 'all' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                    >Все</button>
                    {Object.entries(CATEGORY_LABELS).filter(([key]) => key !== 'gold' && key !== 'money_market').map(([key, label]) => (
                        <button
                            key={key}
                            onClick={() => setCategoryFilter(key)}
                            className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${categoryFilter === key ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'}`}
                        >{label}</button>
                    ))}
                </div>

                <div className="btn-group-scroll gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
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

            {loading && (
                <div className="flex justify-center py-20">
                    <div className="w-8 h-8 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
                </div>
            )}

            {!loading && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {filteredFunds.map(fund => {
                        const uk = fund.uk_id ? UK_LOGOS[fund.uk_id] : null;
                        const isExpanded = expandedFund === fund.fund_id;
                        const topHoldings = (fund.top_holdings || []).filter(h => h.weight > 0 && !h.name.includes('акция прив'));

                        return (
                            <div
                                key={fund.fund_id}
                                className={`bg-theme-secondary rounded-2xl border transition-all cursor-pointer hover:border-[#6366f1]/50 ${isExpanded ? 'border-[#6366f1] ring-1 ring-[#6366f1]/20' : 'border-theme'}`}
                                onClick={() => handleExpandFund(fund.fund_id)}
                            >
                                {/* Header */}
                                <div className="px-4 pt-4 pb-2 flex items-center gap-2.5">
                                    {uk && (
                                        <div className="w-8 h-8 rounded-full flex items-center justify-center font-black text-xs flex-shrink-0"
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
                                    </div>
                                </div>

                                {/* Interactive donut */}
                                <div className="flex justify-center py-2">
                                    <InteractiveDonut holdings={topHoldings} size={160} />
                                </div>

                                {/* Holdings list — топ-5 или полный */}
                                <div className="px-4 pb-3 space-y-1">
                                    {(() => {
                                        const list = isExpanded && expandedHoldings
                                            ? expandedHoldings.holdings.filter(h => h.weight > 0 && !h.name.includes('акция прив'))
                                            : topHoldings.slice(0, 5);
                                        return (
                                            <div className={`space-y-1 ${isExpanded ? 'max-h-72 overflow-y-auto pr-1' : ''}`}>
                                                {list.map((h, i) => (
                                                    <div key={i} className="flex items-center gap-2 text-xs">
                                                        <div className="w-2 h-2 rounded-sm flex-shrink-0" style={{ backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length] }} />
                                                        <span className="truncate flex-1 text-theme-secondary">{h.name}</span>
                                                        <span className="font-mono flex-shrink-0">{(h.weight * 100).toFixed(1)}%</span>
                                                    </div>
                                                ))}
                                            </div>
                                        );
                                    })()}
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
