import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { isPeriodAllowed } from '../../config/accessControl';
import Dropdown, { type DropdownOption } from '../Dropdown';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

type Period = '6m' | '1y' | '2y' | '5y' | 'all';
type ChartMode = 'line' | 'histogram';
type EmaPeriod = 50 | 100 | 200;

const PERIOD_LABELS: Record<Period, string> = {
    '6m': '6М',
    '1y': '1Г',
    '2y': '2Г',
    '5y': '5Л',
    'all': 'Всё',
};

const EMA_OPTIONS: EmaPeriod[] = [50, 100, 200];

interface StrengthControlsProps {
    period: Period;
    onPeriodChange: (period: Period) => void;
    chartMode: ChartMode;
    onChartModeChange: (mode: ChartMode) => void;
    universeBase: 'all' | 'imoex';
    onUniverseBaseChange: (base: 'all' | 'imoex') => void;
    currency: 'rub' | 'usd';
    onCurrencyChange: (currency: 'rub' | 'usd') => void;
    emaPeriod: EmaPeriod;
    onEmaPeriodChange: (ema: EmaPeriod) => void;
    showPrice: boolean;
    onShowPriceChange: (show: boolean) => void;
    stocksAbove: number;
    stocksTotal: number;
    classInfo: { label: string; color: string; bg: string };
    hasCurrent: boolean;
    /** Trailing slot — rendered после classification chip (ml-auto group).
     *  Используется для ChartCaptureButton чтобы он был на одной строке. */
    trailingSlot?: React.ReactNode;
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
    emaPeriod,
    onEmaPeriodChange,
    showPrice,
    onShowPriceChange,
    stocksAbove,
    stocksTotal,
    classInfo,
    hasCurrent,
    trailingSlot,
}: StrengthControlsProps) {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const strengthAccess = useTierAccess('strength');
    const { showUpgrade } = useUpgradePrompt();

    const universeAllLocked = !strengthAccess.isLoading && !strengthAccess.canUseUniverse('all');
    const usdLocked = !strengthAccess.isLoading && !strengthAccess.canUseUniverse('imoex_usd');

    return (
        <div className="flex items-center flex-wrap mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
            {/* Period */}
            <Dropdown<Period>
                options={(Object.keys(PERIOD_LABELS) as Period[]).map((p): DropdownOption<Period> => ({
                    key: p,
                    label: PERIOD_LABELS[p],
                    locked: !isPeriodAllowed(p, isAuthenticated),
                }))}
                value={period}
                onChange={(p) => {
                    if (!isPeriodAllowed(p, isAuthenticated)) { navigate('/login'); return; }
                    onPeriodChange(p);
                }}
            />

            {/* Chart mode (line / histogram) */}
            <Dropdown<ChartMode>
                options={[
                    { key: 'line', label: 'Линия' },
                    { key: 'histogram', label: 'Гистограмма' },
                ]}
                value={chartMode}
                onChange={onChartModeChange}
            />

            {/* Universe: IMOEX / 100 акций.
                Раньше label был 'Все акции' — переименовано чтобы точнее
                отражать что universe = 100 ликвидных акций (не реально все). */}
            <Dropdown<'imoex' | 'all'>
                options={[
                    { key: 'imoex', label: currency === 'usd' ? 'Индекс RTSI' : 'Индекс IMOEX' },
                    { key: 'all', label: '100 акций', locked: universeAllLocked },
                ]}
                value={universeBase}
                onChange={onUniverseBaseChange}
                onLockedClick={() => {
                    const tier = strengthAccess.requiredTierFor({ universe: 'all' });
                    if (tier) {
                        showUpgrade({
                            tier,
                            featureName: 'вселенная «100 акций»',
                            indicator: 'strength',
                        });
                    }
                }}
            />

            {/* Currency */}
            <Dropdown<'rub' | 'usd'>
                options={[
                    { key: 'rub', label: '₽ Рубль' },
                    { key: 'usd', label: '$ Доллар', locked: usdLocked },
                ]}
                value={currency}
                onChange={onCurrencyChange}
                onLockedClick={() => {
                    const tier = strengthAccess.requiredTierFor({ universe: 'imoex_usd' });
                    if (tier) {
                        showUpgrade({
                            tier,
                            featureName: 'долларовый режим',
                            indicator: 'strength',
                        });
                    }
                }}
            />

            {/* EMA period */}
            <Dropdown<string>
                options={EMA_OPTIONS.map((p): DropdownOption<string> => ({
                    key: String(p),
                    label: `EMA${p}`,
                }))}
                value={String(emaPeriod)}
                onChange={(k) => onEmaPeriodChange(Number(k) as EmaPeriod)}
            />

            {/* Тоггл показа верхнего графика — editorial-press chip.
                Метка = индекс, который рисуется сверху (IMOEX в ₽ / RTS в $).
                Верхний график — всегда индекс и НЕ зависит от вселенной
                breadth-метрики (universeBase), поэтому метка считается только
                по валюте. Не возвращать ветку на universeBase. */}
            <button
                onClick={() => onShowPriceChange(!showPrice)}
                className="editorial-press font-semibold rounded-full"
                style={{
                    backgroundColor: showPrice ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: showPrice ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: showPrice ? 'var(--shadow-hard-chip)' : undefined,
                    fontSize: 'var(--fs-sm)',
                    padding: 'var(--sp-2) var(--sp-4)',
                }}
            >
                {currency === 'usd' ? 'RTS' : 'IMOEX'}
            </button>

            {/* Status + counter.
                visibility: hidden пока нет данных — чтобы место было зарезервировано
                и главный контейнер НЕ сдвигался вниз когда данные прилетят. */}
            <div className="flex items-center sm:ml-auto" style={{ gap: 'var(--sp-3)', visibility: hasCurrent ? 'visible' : 'hidden' }}>
                <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)' }}>
                    <span className="font-bold text-theme-primary">{stocksAbove}</span>/{stocksTotal} выше EMA
                </span>
                <div className="rounded-full" style={{ padding: 'calc(var(--sp-1)) var(--sp-3)', background: classInfo.bg }}>
                    <span className="font-medium" style={{ fontSize: 'var(--fs-2xs)', color: classInfo.color }}>
                        {classInfo.label}
                    </span>
                </div>
            </div>
            {trailingSlot}
        </div>
    );
}
