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
import { Download, Lock } from 'lucide-react';
import { apiFetch } from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { useAnalytics } from '../../contexts/AnalyticsContext';

interface Props {
    /** URL endpoint. Может быть строкой ИЛИ getter-функцией для lazy evaluation.
     *  Getter полезен когда URL зависит от текущего state страницы (period,
     *  ticker, category) — компонент не re-render'ится при каждом изменении
     *  state'а, но при клике берёт актуальное значение. */
    url: string | (() => string);
    /** Имя файла для browser download. Также может быть getter'ом для lazy state. */
    filename: string | (() => string);
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

    const locked = !common.csv_export;

    const handleClick = async () => {
        // Pre-gate: если tier не Pro — открываем upgrade modal сразу без запроса.
        if (locked) {
            showUpgrade({
                tier: 'pro',
                featureName: 'экспорт в CSV',
                indicator,
            });
            return;
        }

        // Lazy resolve URL/filename — на момент клика, не render'а.
        const resolvedUrl = typeof url === 'function' ? url() : url;
        const resolvedFilename = typeof filename === 'function' ? filename() : filename;

        // Используем существующий 'chart_export' event-type с extra payload —
        // не плодим новый enum-вариант ради subdivision.
        track('chart_export', { indicator, format: 'csv' });

        try {
            const resp = await apiFetch(resolvedUrl);
            if (resp.status === 403) {
                showUpgrade({ tier: 'pro', featureName: 'экспорт в CSV', indicator });
                return;
            }
            if (!resp.ok) {
                // eslint-disable-next-line no-alert
                alert(`Не удалось скачать CSV (статус ${resp.status})`);
                return;
            }

            const blob = await resp.blob();
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = resolvedFilename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
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
                opacity: locked ? 0.65 : 1,
                position: 'relative',
            }}
            aria-label={locked ? `${title} (Pro)` : title}
            title={locked ? `${title} — доступно на тарифе Pro` : title}
        >
            <Download size={22} />
            {locked && (
                // Lock-badge в правом-верхнем углу. Тот же паттерн что у
                // locked-chip'ов в Buffett/Strength sheet'ах.
                <span
                    style={{
                        position: 'absolute',
                        top: -4,
                        right: -4,
                        width: 18,
                        height: 18,
                        borderRadius: '50%',
                        background: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        border: '1.5px solid var(--text-primary)',
                    }}
                    aria-hidden="true"
                >
                    <Lock size={10} strokeWidth={2.5} />
                </span>
            )}
        </button>
    );
}
