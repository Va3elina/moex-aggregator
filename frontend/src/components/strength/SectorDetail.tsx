import { ArrowUp, ArrowDown, Filter } from 'lucide-react';
import type { BreadthStock } from '../../services/api';
import { formatNumber } from '../../utils/formatNumber';

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
        <div
            className="rounded-2xl mt-6 md:mt-8 overflow-hidden"
            style={{
                background: 'var(--bg-secondary)',  // белый panel — отделить от paper-страницы
                border: '1.5px solid var(--text-primary)',
                boxShadow: '5px 5px 0 var(--text-primary)',  // editorial hard shadow
                padding: 'var(--sp-5)',
            }}
        >
            <div className="flex flex-row items-center justify-between flex-wrap mb-4" style={{ gap: 'var(--sp-3)' }}>
                <h3 className="font-semibold text-theme-primary whitespace-nowrap" style={{ fontSize: 'var(--fs-lg)' }}>
                    Детализация по акциям
                </h3>

                {/* Sector filter — editorial-press chip-pills */}
                <div className="flex flex-wrap items-center" style={{ gap: 'var(--sp-2)' }}>
                    <Filter size={16} className="text-theme-muted" />
                    {sectorNames
                        .filter(sector => (sectorCounts[sector] ?? 0) > 0)
                        .map((sector) => {
                            const isActive = selectedSector === sector;
                            return (
                                <button
                                    key={sector}
                                    onClick={() => onSelectSector(sector)}
                                    className="editorial-press font-semibold rounded-full"
                                    style={{
                                        backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                                        color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                                        border: '1.5px solid var(--text-primary)',
                                        boxShadow: isActive ? 'var(--shadow-hard-chip)' : undefined,
                                        fontSize: 'var(--fs-2xs)',
                                        padding: 'calc(var(--sp-1)) var(--sp-3)',
                                    }}
                                >
                                    {sector}
                                    {sector !== 'Все' && (
                                        <span className="ml-1 opacity-60">({sectorCounts[sector]})</span>
                                    )}
                                </button>
                            );
                        })}
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full" style={{ fontSize: 'var(--fs-sm)' }}>
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
                            <tr key={stock.ticker} className="border-b border-theme/50 transition-colors hover:bg-white/5">
                                <td className="py-3 px-2 font-medium text-theme-primary">
                                    {stock.ticker}
                                </td>
                                <td className="py-3 px-2 text-right text-theme-primary">
                                    {formatNumber(stock.price, 2)}
                                </td>
                                <td className="py-3 px-2 text-right text-theme-secondary">
                                    {formatNumber(stock.ema, 2)}
                                </td>
                                <td className="py-3 px-2 text-right font-medium" style={{ color: stock.diff_percent >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)' }}>
                                    <span className="flex items-center justify-end gap-1">
                                        {stock.diff_percent >= 0 ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
                                        {Math.abs(stock.diff_percent).toFixed(2)}%
                                    </span>
                                </td>
                                <td className="py-3 px-2 text-center">
                                    <span
                                        className="inline-block rounded-full font-medium"
                                        style={{
                                            padding: 'calc(var(--sp-1)) var(--sp-2)',
                                            fontSize: 'var(--fs-2xs)',
                                            color: stock.is_above ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
                                            backgroundColor: stock.is_above
                                                ? 'color-mix(in srgb, var(--funds-flow-positive) 18%, transparent)'
                                                : 'color-mix(in srgb, var(--funds-flow-negative) 18%, transparent)',
                                        }}
                                    >
                                        {stock.is_above ? 'Выше' : 'Ниже'}
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>

                {filteredStocks.length === 0 && (
                    <div className="py-8 text-center text-theme-muted" style={{ fontSize: 'var(--fs-sm)' }}>
                        Нет акций в выбранном секторе
                    </div>
                )}
            </div>
        </div>
    );
}
