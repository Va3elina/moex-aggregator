import { X } from 'lucide-react';
import { UK_LOGOS } from '../../config/fundConfig';
import type { FundInfo, FundHoldingsResponse } from '../../services/api';

interface FundCardModalProps {
    selectedFund: FundInfo;
    fundHoldings: FundHoldingsResponse | null;
    holdingsLoading: boolean;
    onClose: () => void;
}

const DONUT_COLORS = ['#6366f1','#2EE59D','#4DA3FF','#FF4D4D','#FFB020','#00D9FF','#9D4DFF','#FF6B9D','#FCD34D','#14B8A6','#F97316','#818CF8'];

export default function FundCardModal({ selectedFund, fundHoldings, holdingsLoading, onClose }: FundCardModalProps) {
    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-md" onClick={onClose}>
            <div className="bg-white/[0.08] backdrop-blur-xl rounded-2xl border border-white/15 shadow-2xl w-full max-w-md mx-4 max-h-[80vh] overflow-hidden" style={{ boxShadow: '0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1)' }} onClick={e => e.stopPropagation()}>
                {/* Header */}
                <div className="flex items-center justify-between p-5 border-b border-white/10">
                    <div className="flex items-center gap-3">
                        {selectedFund.uk_id && UK_LOGOS[selectedFund.uk_id] && (
                            <div className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 font-black text-xl"
                                style={{ backgroundColor: UK_LOGOS[selectedFund.uk_id].bg, color: UK_LOGOS[selectedFund.uk_id].color }}>
                                {UK_LOGOS[selectedFund.uk_id].letter}
                            </div>
                        )}
                        <div>
                            <div className="font-bold text-lg">{selectedFund.ticker}</div>
                            <div className="text-sm text-theme-secondary">{selectedFund.name}</div>
                        </div>
                    </div>
                    <button onClick={onClose} className="p-1 rounded-lg hover:bg-white/10 transition-colors">
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
                            <div className="w-6 h-6 border-2 border-[#6366f1] border-t-transparent rounded-full animate-spin" />
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
                                            return <path key={i} d={d} fill={DONUT_COLORS[i % DONUT_COLORS.length]} stroke="#1A1F2E" strokeWidth="1.5" />;
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
                                            <div className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: DONUT_COLORS[i % DONUT_COLORS.length] }} />
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
