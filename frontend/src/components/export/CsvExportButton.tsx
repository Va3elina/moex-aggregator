/**
 * CsvExportButton — кнопка-pill для скачивания CSV с данными индикатора.
 *
 * Размещается рядом с ChartCaptureButton (Camera) на page-toolbar'е каждого
 * индикатора. Стилистически идентична — 44px pill, paper bg, theme-primary
 * outline.
 *
 * Tier-gating:
 *   - Pro/admin: download срабатывает.
 *   - Free/Basic: при клике показывается UpgradeModal с targetTier='pro'.
 *
 * Auth: используем apiFetch (через AuthContext автоматически добавит
 * Authorization header). Если backend возвращает 403 (например токен
 * протух) — показываем upgrade modal как fallback.
 */
import { Download } from 'lucide-react';
import { apiFetch } from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { useAnalytics } from '../../contexts/AnalyticsContext';

interface Props {
    /** URL endpoint — относительный или абсолютный. Например /api/export/heatmap.csv */
    url: string;
    /** Имя файла для browser download. Backend задаёт через Content-Disposition,
     *  но для UX consistency дублируем здесь. */
    filename: string;
    /** Indicator ID для UpgradeModal context. */
    indicator: string;
    /** Подсказка title (tooltip). По умолчанию "Скачать CSV". */
    title?: string;
    /** Дополнительные классы. */
    className?: string;
}

export default function CsvExportButton({
    url,
    filename,
    indicator,
    title = 'Скачать CSV',
    className = '',
}: Props) {
    const common = useCommonFeatures();
    const { showUpgrade } = useUpgradePrompt();
    const { track } = useAnalytics();

    const handleClick = async () => {
        // Pre-gate: если tier не Pro — открываем upgrade modal сразу без запроса.
        if (!common.csv_export) {
            showUpgrade({
                tier: 'pro',
                featureName: 'экспорт в CSV',
                indicator,
            });
            return;
        }

        // Используем существующий 'chart_export' event-type с extra payload —
        // не плодим новый enum-вариант ради subdivision.
        track('chart_export', { indicator, format: 'csv' });

        try {
            const resp = await apiFetch(url);
            if (resp.status === 403) {
                // Backup case: токен протух / matrix не обновилась.
                showUpgrade({ tier: 'pro', featureName: 'экспорт в CSV', indicator });
                return;
            }
            if (!resp.ok) {
                // 404 / 400 / 500 — показать alert, не оверкомпенсировать.
                // eslint-disable-next-line no-alert
                alert(`Не удалось скачать CSV (статус ${resp.status})`);
                return;
            }

            const blob = await resp.blob();
            // Trigger browser download через временный anchor.
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            // Cleanup blob — иначе Safari/Firefox держат memory.
            setTimeout(() => URL.revokeObjectURL(blobUrl), 1000);
        } catch (err) {
            console.error('[CsvExportButton] download failed:', err);
            // eslint-disable-next-line no-alert
            alert('Ошибка скачивания. Проверьте подключение и попробуйте ещё раз.');
        }
    };

    return (
        <button
            type="button"
            onClick={handleClick}
            data-export-ignore="true"
            className={`editorial-press rounded-full inline-flex items-center justify-center ${className}`}
            style={{
                backgroundColor: 'var(--bg-secondary)',
                border: '2px solid var(--text-primary)',
                color: 'var(--text-primary)',
                width: 44,
                height: 44,
            }}
            aria-label={title}
            title={title}
        >
            <Download size={22} />
        </button>
    );
}
