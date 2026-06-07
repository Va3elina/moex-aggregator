/**
 * AlertBellButton — 🔔 рядом с 📷 (ChartCaptureButton) в controls-row индикатора.
 * Открывает CreateAlertModal для текущего актива. Стиль 1:1 с ChartCaptureButton
 * (44×44 pill, bg-secondary, 2px border, editorial-press).
 */
import { useState } from 'react';
import { Bell } from 'lucide-react';
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

    const handleClick = () => {
        if (quota === 0) {
            // Free/guest — алерты на Basic+; ведём на апгрейд вместо формы.
            showUpgrade({ tier: 'basic', featureName: 'Telegram-алерты', indicator: 'alerts' });
            return;
        }
        setOpen(true);
    };

    return (
        <>
            <button
                type="button"
                data-export-ignore="true"
                onClick={handleClick}
                className={`editorial-press rounded-full inline-flex items-center justify-center ${className}`}
                style={{
                    backgroundColor: 'var(--bg-secondary)',
                    border: '2px solid var(--text-primary)',
                    color: 'var(--text-primary)',
                    width: 44,
                    height: 44,
                }}
                aria-label="Создать алерт"
                title="Создать алерт в Telegram"
            >
                <Bell size={22} />
            </button>
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
