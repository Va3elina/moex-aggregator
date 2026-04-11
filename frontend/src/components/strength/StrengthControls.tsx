import { LineChart, BarChart2, Lock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { isPeriodAllowed } from '../../config/accessControl';

type Period = '6m' | '1y' | '2y' | '5y' | 'all';
type ChartMode = 'line' | 'histogram';

const PERIOD_LABELS: Record<Period, string> = {
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    '5y': '5Л',
    'all': 'Всё'
};

interface StrengthControlsProps {
    period: Period;
    onPeriodChange: (period: Period) => void;
    chartMode: ChartMode;
    onChartModeChange: (mode: ChartMode) => void;
    universeBase: 'all' | 'imoex';
    onUniverseBaseChange: (base: 'all' | 'imoex') => void;
    currency: 'rub' | 'usd';
    onCurrencyChange: (currency: 'rub' | 'usd') => void;
    showPrice: boolean;
    onShowPriceChange: (show: boolean) => void;
    stocksAbove: number;
    stocksTotal: number;
    classInfo: { label: string; color: string; bg: string };
    hasCurrent: boolean;
}

export default function StrengthControls({
    period,
    onPeriodChange,
    chartMode,
    onChartModeChange,
    universeBase,
    onUniverseBaseChange,
    currency,
    onCurrencyChange,
    showPrice,
    onShowPriceChange,
    stocksAbove,
    stocksTotal,
    classInfo,
    hasCurrent,
}: StrengthControlsProps) {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();

    return (
        <div className="flex items-center gap-4 mb-6 flex-wrap">
            {/* Period */}
            <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                {(Object.entries(PERIOD_LABELS) as [Period, string][]).map(([key, label]) => {
                    const allowed = isPeriodAllowed(key, isAuthenticated);
                    return (
                        <button
                            key={key}
                            onClick={() => {
                                if (!allowed) { navigate('/login'); return; }
                                onPeriodChange(key);
                            }}
                            title={!allowed ? 'Войдите для доступа' : undefined}
                            className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                                !allowed
                                    ? 'text-theme-muted cursor-not-allowed opacity-50'
                                    : period === key
                                        ? 'btn-control active'
                                        : 'text-theme-secondary hover:text-theme-primary'
                            }`}
                        >
                            {label}
                            {!allowed && <Lock className="inline-block ml-0.5 w-3 h-3" />}
                        </button>
                    );
                })}
            </div>

            {/* Chart type breadth */}
            <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                <button
                    onClick={() => onChartModeChange('line')}
                    className={`p-2 rounded-lg transition-colors ${chartMode === 'line' ? 'bg-white/10' : ''}`}
                    title="Линия"
                >
                    <LineChart size={18} className={chartMode === 'line' ? 'text-theme-accent' : 'text-theme-secondary'} />
                </button>
                <button
                    onClick={() => onChartModeChange('histogram')}
                    className={`p-2 rounded-lg transition-colors ${chartMode === 'histogram' ? 'bg-white/10' : ''}`}
                    title="Гистограмма"
                >
                    <BarChart2 size={18} className={chartMode === 'histogram' ? 'text-theme-accent' : 'text-theme-secondary'} />
                </button>
            </div>

            {/* Universe: IMOEX / all stocks */}
            <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                <button
                    onClick={() => onUniverseBaseChange('imoex')}
                    className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                        universeBase === 'imoex' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                    }`}
                >
                    Индекс IMOEX
                </button>
                <button
                    onClick={() => onUniverseBaseChange('all')}
                    className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
                        universeBase === 'all' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                    }`}
                >
                    Все акции
                </button>
            </div>

            {/* Currency */}
            <div className="flex items-center gap-1 bg-theme-secondary rounded-xl border border-theme p-1">
                <button
                    onClick={() => onCurrencyChange('rub')}
                    className={`px-3 py-1.5 text-sm font-bold rounded-lg transition-colors ${
                        currency === 'rub' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                    }`}
                >
                    ₽
                </button>
                <button
                    onClick={() => onCurrencyChange('usd')}
                    className={`px-3 py-1.5 text-sm font-bold rounded-lg transition-colors ${
                        currency === 'usd' ? 'btn-control active' : 'text-theme-secondary hover:text-theme-primary'
                    }`}
                >
                    $
                </button>
            </div>

            {/* Show IMOEX */}
            <button
                onClick={() => onShowPriceChange(!showPrice)}
                className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors border ${showPrice ? 'bg-indigo-500/20 border-indigo-500/50 text-indigo-400' : 'border-theme text-theme-secondary'
                    }`}
            >
                {currency === 'usd' ? 'RTS' : 'IMOEX'}
            </button>

            {/* Status + counter */}
            {hasCurrent && (
                <div className="flex items-center gap-3 sm:ml-auto">
                    <span className="text-sm text-theme-secondary">
                        <span className="font-bold text-theme-primary">{stocksAbove}</span>/{stocksTotal} выше EMA
                    </span>
                    <div className={`px-3 py-1 rounded-full ${classInfo.bg}`}>
                        <span className={`text-xs font-medium ${classInfo.color}`}>
                            {classInfo.label}
                        </span>
                    </div>
                </div>
            )}
        </div>
    );
}
