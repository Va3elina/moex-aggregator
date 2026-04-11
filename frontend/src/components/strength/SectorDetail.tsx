import { ArrowUp, ArrowDown, Filter } from 'lucide-react';
import type { BreadthStock } from '../../services/api';

interface SectorDetailProps {
    sectorNames: string[];
    sectorCounts: Record<string, number>;
    selectedSector: string;
    onSelectSector: (sector: string) => void;
    filteredStocks: BreadthStock[];
    emaPeriod: number;
}

export default function SectorDetail({
    sectorNames,
    sectorCounts,
    selectedSector,
    onSelectSector,
    filteredStocks,
    emaPeriod,
}: SectorDetailProps) {
    return (
        <div className="widget p-4 md:p-6">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-4">
                <h3 className="text-lg font-semibold text-theme-primary">
                    Детализация по акциям
                </h3>

                {/* Sector filter */}
                <div className="flex flex-wrap items-center gap-2">
                    <Filter size={16} className="text-theme-muted" />
                    {sectorNames
                        .filter(sector => (sectorCounts[sector] ?? 0) > 0)
                        .map((sector) => (
                            <button
                                key={sector}
                                onClick={() => onSelectSector(sector)}
                                className={`px-3 py-1 text-xs font-medium rounded-full transition-colors ${selectedSector === sector
                                    ? 'bg-white/10 text-theme-primary border border-theme'
                                    : 'text-theme-secondary hover:text-theme-primary'
                                    }`}
                            >
                                {sector}
                                {sector !== 'Все' && (
                                    <span className="ml-1 opacity-50">({sectorCounts[sector]})</span>
                                )}
                            </button>
                        ))}
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full text-sm">
                    <thead>
                        <tr className="text-theme-muted border-b border-theme">
                            <th className="text-left py-3 px-2">Тикер</th>
                            <th className="text-right py-3 px-2">Цена</th>
                            <th className="text-right py-3 px-2">EMA{emaPeriod}</th>
                            <th className="text-right py-3 px-2">Отклонение</th>
                            <th className="text-center py-3 px-2">Статус</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filteredStocks.map((stock) => (
                            <tr key={stock.ticker} className="border-b border-theme/50 hover:bg-white/5">
                                <td className="py-3 px-2 font-medium text-theme-primary">
                                    {stock.ticker}
                                </td>
                                <td className="py-3 px-2 text-right text-theme-primary">
                                    {stock.price.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}
                                </td>
                                <td className="py-3 px-2 text-right text-theme-secondary">
                                    {stock.ema.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}
                                </td>
                                <td className={`py-3 px-2 text-right font-medium ${stock.diff_percent >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                                    <span className="flex items-center justify-end gap-1">
                                        {stock.diff_percent >= 0 ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
                                        {Math.abs(stock.diff_percent).toFixed(2)}%
                                    </span>
                                </td>
                                <td className="py-3 px-2 text-center">
                                    <span className={`inline-block px-2 py-1 rounded-full text-xs font-medium ${stock.is_above
                                        ? 'bg-emerald-500/20 text-emerald-400'
                                        : 'bg-red-500/20 text-red-400'
                                        }`}>
                                        {stock.is_above ? 'Выше' : 'Ниже'}
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>

                {filteredStocks.length === 0 && (
                    <div className="py-8 text-center text-theme-muted">
                        Нет акций в выбранном секторе
                    </div>
                )}
            </div>
        </div>
    );
}
