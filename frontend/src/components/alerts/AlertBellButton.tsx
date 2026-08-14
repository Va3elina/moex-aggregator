/**
 * AlertBellButton — кнопка-колокол рядом с камерой (ChartCaptureButton) в controls-row индикатора.
 * Открывает CreateAlertModal для текущего актива. Стиль 1:1 с ChartCaptureButton
 * (44×44 pill, bg-secondary, 2px border, editorial-press).
 */
import { useState } from 'react';
import { AlarmClock, Lock } from 'lucide-react';
import CreateAlertModal, { type AlertMetricOption } from './CreateAlertModal';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

interface Props {
    indicator: string;            // 'open_interest' | ...
    asset: string;                // sectype: 'SR','Si'...
    assetName?: string;
    metrics: AlertMetricOption[]; // доступные метрики для этого индикатора
    className?: string;
}

export default function AlertBellButton({ indicator, asset, assetName, metrics, className = '' }: Props) {
    const [open, setOpen] = useState(false);
    const quota = useCommonFeatures().telegram_alerts_quota;  // 0 / число / null(∞)
    const { showUpgrade } = useUpgradePrompt();
    const locked = quota === 0;  // Free/guest — алерты на Basic+

    const handleClick = () => {
        if (locked) {
            // Замочек уже виден; клик объясняет почему и ведёт на апгрейд.
            showUpgrade({ tier: 'basic', featureName: 'Уведомления в мессенджере', indicator: 'alerts' });
            return;
        }
        setOpen(true);
    };

    return (
        <>
            <span className={`relative inline-flex ${className}`} style={{ flex: '0 0 auto' }}>
                <button
                    type="button"
                    data-export-ignore="true"
                    onClick={handleClick}
                    className="editorial-press rounded-full inline-flex items-center justify-center"
                    style={{
                        backgroundColor: 'var(--bg-secondary)',
                        border: '2px solid var(--text-primary)',
                        color: 'var(--text-primary)',
                        width: 44,
                        height: 44,
                    }}
                    aria-label={locked ? 'Уведомления доступны на тарифе Basic и Pro' : 'Создать уведомление'}
                    title={locked
                        ? 'Уведомления в мессенджере доступны на тарифе Basic и Pro. Нажмите, чтобы улучшить.'
                        : 'Создать уведомление в мессенджере'}
                >
                    <AlarmClock size={22} />
                </button>
                {locked && (
                    <span
                        aria-hidden="true"
                        data-export-ignore="true"
                        style={{
                            position: 'absolute', right: -3, bottom: -3,
                            width: 18, height: 18, borderRadius: '50%',
                            background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            color: 'var(--text-primary)', pointerEvents: 'none',
                        }}
                    >
                        <Lock size={10} strokeWidth={2.6} />
                    </span>
                )}
            </span>
            {open && (
                <CreateAlertModal
                    indicator={indicator}
                    asset={asset}
                    assetName={assetName}
                    metrics={metrics}
                    onClose={() => setOpen(false)}
                />
            )}
        </>
    );
}
