import { X } from 'lucide-react';
import { DONUT_COLORS, resolveFundLogo } from '../../config/fundConfig';
import type { FundInfo, FundHoldingsResponse } from '../../services/api';

interface FundCardModalProps {
    selectedFund: FundInfo;
    fundHoldings: FundHoldingsResponse | null;
    holdingsLoading: boolean;
    onClose: () => void;
}

export default function FundCardModal({ selectedFund, fundHoldings, holdingsLoading, onClose }: FundCardModalProps) {
    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60" onClick={onClose}>
            {/* Editorial paper-style modal: убран backdrop-blur (glass anti-pattern),
                bg-white/[0.08] заменён на theme-aware bg-theme-primary, border-white/15
                на 1.5px ink outline + hard-shadow. На обеих темах смотрится как лист
                бумаги, не как полупрозрачное стекло. */}
            <div
                className="bg-theme-primary rounded-2xl shadow-md w-full max-w-md mx-4 max-h-[80vh] overflow-hidden"
                style={{ border: '1.5px solid var(--text-primary)', boxShadow: '4px 4px 0 var(--text-primary)' }}
                onClick={e => e.stopPropagation()}
            >
                {/* Header */}
                <div className="flex items-center justify-between p-5" style={{ borderBottom: '1px solid var(--border-color)' }}>
                    <div className="flex items-center gap-3">
                        {(() => {
                            const uk = resolveFundLogo(selectedFund.ticker, selectedFund.uk_id);
                            if (!uk) return null;
                            return (
                                <div className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 font-black text-xl overflow-hidden"
                                    style={{ backgroundColor: uk.img ? undefined : uk.bg, color: uk.color }}>
                                    {uk.img
                                        ? <img src={uk.img} alt={uk.name} className="w-full h-full object-cover" />
                                        : uk.letter}
                                </div>
                            );
                        })()}
                        <div>
                            <div className="font-bold text-lg">{selectedFund.ticker}</div>
                            <div className="text-sm text-theme-secondary">{selectedFund.name}</div>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-1 rounded-lg transition-colors hover:bg-theme-secondary">
                        <X size={20} />
                    </button>
                </div>

                {/* Info */}
                <div className="px-5 py-3 border-b border-white/10 bg-white/[0.03] grid grid-cols-2 gap-3 text-sm">
                    {selectedFund.subcategory && (
                        <div>
                            <span className="text-theme-secondary">Тип: </span>
                            <span className="font-medium">{selectedFund.subcategory}</span>
                        </div>
                    )}
                    <div>
                        <span className="text-theme-secondary">СЧА: </span>
                        <span className="font-mono font-bold text-[#2EE59D]">
                            {(() => {
                                const last = selectedFund.data[selectedFund.data.length - 1];
                                return last?.nav ? `${(last.nav / 1e9).toFixed(2)} млрд ₽` : '—';
                            })()}
                        </span>
                    </div>
                </div>

                {/* Holdings — donut chart + list */}
                <div className="px-5 py-3 overflow-y-auto" style={{ maxHeight: 'calc(80vh - 180px)' }}>
                    <div className="text-sm font-semibold text-theme-secondary mb-3">Состав фонда</div>
                    {holdingsLoading ? (
                        <div className="flex justify-center py-8">
                            <div className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
                        </div>
                    ) : fundHoldings?.holdings?.length ? (
                        <div>
                            {/* Donut Chart */}
                            <div className="flex justify-center mb-4">
                                <svg viewBox="0 0 200 200" width="180" height="180">
                                    {(() => {
                                        const top = fundHoldings.holdings.slice(0, 10);
                                        const otherWeight = fundHoldings.holdings.slice(10).reduce((s, h) => s + h.weight, 0);
                                        const items = otherWeight > 0 ? [...top, { name: 'Прочее', weight: otherWeight }] : top;
                                        const total = items.reduce((s, h) => s + h.weight, 0);
                                        let cumAngle = -90;
                                        const cx = 100, cy = 100, r = 70, ir = 45;
                                        return items.map((h, i) => {
                                            const angle = (h.weight / total) * 360;
                                            const startRad = (cumAngle * Math.PI) / 180;
                                            const endRad = ((cumAngle + angle) * Math.PI) / 180;
                                            cumAngle += angle;
                                            const x1 = cx + r * Math.cos(startRad), y1 = cy + r * Math.sin(startRad);
                                            const x2 = cx + r * Math.cos(endRad), y2 = cy + r * Math.sin(endRad);
                                            const ix1 = cx + ir * Math.cos(endRad), iy1 = cy + ir * Math.sin(endRad);
                                            const ix2 = cx + ir * Math.cos(startRad), iy2 = cy + ir * Math.sin(startRad);
                                            const largeArc = angle > 180 ? 1 : 0;
                                            const d = `M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${ir} ${ir} 0 ${largeArc} 0 ${ix2} ${iy2} Z`;
                                            return <path key={i} d={d} fill={DONUT_COLORS[i % DONUT_COLORS.length]} stroke="var(--bg-primary)" strokeWidth="1.5" />;
                                        });
                                    })()}
                                    <text x="100" y="96" textAnchor="middle" fill="white" fontSize="14" fontWeight="bold">
                                        {fundHoldings.holdings.length}
                                    </text>
                                    <text x="100" y="112" textAnchor="middle" fill="#9CA3B8" fontSize="10">
                                        позиций
                                    </text>
                                </svg>
                            </div>
                            {/* Legend */}
                            <div className="space-y-1.5">
                                {(() => {
                                    const top = fundHoldings.holdings.slice(0, 10);
                                    const otherWeight = fundHoldings.holdings.slice(10).reduce((s, h) => s + h.weight, 0);
                                    const items = otherWeight > 0 ? [...top, { name: `Прочее (${fundHoldings.holdings.length - 10})`, weight: otherWeight }] : top;
                                    return items.map((h, i) => (
                                        <div key={i} className="flex items-center gap-2 text-sm">
                                            <div className="legend-dot" style={{ backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length] }} />
                                            <span className="truncate flex-1">{h.name}</span>
                                            <span className="font-mono text-theme-secondary flex-shrink-0">{(h.weight * 100).toFixed(1)}%</span>
                                        </div>
                                    ));
                                })()}
                            </div>
                        </div>
                    ) : (
                        <div className="text-center text-theme-secondary py-8 text-sm">
                            Состав не публикуется
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
