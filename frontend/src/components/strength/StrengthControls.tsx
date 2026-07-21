import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { isPeriodAllowed } from '../../config/accessControl';
import SegmentedControl from '../SegmentedControl';
import Dropdown from '../Dropdown';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import DollarStaleHint from './DollarStaleHint';

type Period = '1y' | '5y' | '10y' | '20y' | 'all';
type EmaPeriod = 20 | 50 | 100 | 200;

const PERIOD_LABELS: Record<Period, string> = {
    '1y': '1Г',
    '5y': '5Л',
    '10y': '10Л',
    '20y': '20Л',
    'all': 'Всё',
};

const EMA_OPTIONS: EmaPeriod[] = [20, 50, 100, 200];

interface StrengthControlsProps {
    period: Period;
    onPeriodChange: (period: Period) => void;
    universeBase: 'all' | 'imoex';
    onUniverseBaseChange: (base: 'all' | 'imoex') => void;
    currency: 'rub' | 'usd';
    onCurrencyChange: (currency: 'rub' | 'usd') => void;
    /** USD-режим: долларовый ряд отстал → показать маркер «доллар не обновляется». */
    dollarStale?: boolean;
    emaPeriod: EmaPeriod;
    onEmaPeriodChange: (ema: EmaPeriod) => void;
    /** Trailing slot — экшены графика (kebab) через portal. */
    trailingSlot?: React.ReactNode;
}

export default function StrengthControls({
    period,
    onPeriodChange,
    universeBase,
    onUniverseBaseChange,
    currency,
    onCurrencyChange,
    dollarStale,
    emaPeriod,
    onEmaPeriodChange,
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
            {/* Universe: IMOEX / 100 акций.
                Раньше label был 'Все акции' — переименовано чтобы точнее
                отражать что universe = 100 ликвидных акций (не реально все). */}
            <SegmentedControl<'imoex' | 'all'>
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

            {/* Currency — только символ валюты. На выходных и в нерабочие для РТС
                дни на угол кнопки $ накладывается красный маркер «доллар не
                обновляется» (по клику — поповер с пояснением). */}
            <div style={{ position: 'relative', display: 'inline-flex' }}>
                <SegmentedControl<'rub' | 'usd'>
                    options={[
                        { key: 'rub', label: '₽' },
                        { key: 'usd', label: '$', locked: usdLocked },
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
                {dollarStale && (
                    <DollarStaleHint style={{ position: 'absolute', top: -8, right: -8, zIndex: 5 }} />
                )}
            </div>

            {/* EMA period — выпадающий список (вниз). Четыре варианта 20/50/100/200
                не помещались бы в ряд сегментами, поэтому свернули в Dropdown. */}
            <Dropdown<string>
                options={EMA_OPTIONS.map((p) => ({
                    key: String(p),
                    label: `EMA ${p}`,
                }))}
                value={String(emaPeriod)}
                onChange={(k) => onEmaPeriodChange(Number(k) as EmaPeriod)}
            />

            {/* Период — горизонтальный ряд, перенесён в конец селекторов */}
            <SegmentedControl<Period>
                options={(Object.keys(PERIOD_LABELS) as Period[]).map((p) => ({
                    key: p,
                    label: PERIOD_LABELS[p],
                    // tier-замок по ПЕР-ИНДИКАТОРНОМУ canUsePeriod (бэковый
                    // max_history_days strength), а не глобальному GUEST_MAX='1y'.
                    locked: strengthAccess.isLoading ? false : !strengthAccess.canUsePeriod(p),
                }))}
                value={period}
                onChange={onPeriodChange}
                onLockedClick={(p) => {
                    // Tier-блокировка → upgrade modal; иначе legacy guest gate → /login.
                    if (!strengthAccess.canUsePeriod(p)) {
                        const tier = strengthAccess.requiredTierFor({ period: p });
                        if (tier) {
                            showUpgrade({ tier, featureName: `период «${PERIOD_LABELS[p]}»`, indicator: 'strength' });
                            return;
                        }
                    }
                    if (!isPeriodAllowed(p, isAuthenticated)) navigate('/login');
                }}
            />

            {/* Индекс-линия и режим линия/гистограмма — в модалке «Слои»
                (LayersButton в trailingSlot на странице). */}
            {trailingSlot}
        </div>
    );
}
