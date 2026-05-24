/**
 * CsvExportButton — кнопка-pill для скачивания данных индикатора.
 *
 * Click flow:
 *   - Free/Basic → UpgradeModal (pre-gate, без сетевого запроса).
 *   - Pro/admin → открывает CsvExportModal где юзер выбирает layers
 *     (исходные/индикатор/агрегаты) и тогда download.
 *
 * Без модалки (legacy режим): если передан url+filename (legacy props),
 * клик сразу скачивает — для простых случаев / обратной совместимости.
 *
 * Lock badge — accent-кружок в правом-верхнем углу для Free/Basic.
 */
import { useState, lazy, Suspense } from 'react';
import { Download, Lock } from 'lucide-react';
import { apiFetch } from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import type { CsvExportConfig } from './CsvExportModal';

// Lazy-load модалки — chunk выделяется отдельно.
const CsvExportModal = lazy(() => import('./CsvExportModal'));

interface PropsWithConfig {
    /** Config-driven вариант: открывает модалку с выбором layers. */
    config: () => CsvExportConfig;
    /** Indicator ID для UpgradeModal context. */
    indicator: string;
    title?: string;
    className?: string;
    url?: never;
    filename?: never;
}

interface PropsLegacy {
    /** Legacy вариант: прямое скачивание без модалки. */
    url: string | (() => string);
    filename: string | (() => string);
    indicator: string;
    title?: string;
    className?: string;
    config?: never;
}

type Props = PropsWithConfig | PropsLegacy;

export default function CsvExportButton(props: Props) {
    const common = useCommonFeatures();
    const { showUpgrade } = useUpgradePrompt();
    const [modalOpen, setModalOpen] = useState(false);
    const [resolvedConfig, setResolvedConfig] = useState<CsvExportConfig | null>(null);

    const locked = !common.csv_export;
    const title = props.title ?? 'Скачать CSV';
    const indicator = props.indicator;

    const handleClick = async () => {
        if (locked) {
            showUpgrade({
                tier: 'pro',
                featureName: 'экспорт в CSV',
                indicator,
            });
            return;
        }

        // Config-mode: open modal with layer selection.
        if ('config' in props && props.config) {
            setResolvedConfig(props.config());
            setModalOpen(true);
            return;
        }

        // Legacy: direct download.
        const legacyProps = props as PropsLegacy;
        const resolvedUrl = typeof legacyProps.url === 'function' ? legacyProps.url() : legacyProps.url;
        const resolvedFilename = typeof legacyProps.filename === 'function' ? legacyProps.filename() : legacyProps.filename;

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
        <>
            <button
                type="button"
                onClick={handleClick}
                data-export-ignore="true"
                className={`editorial-press rounded-full inline-flex items-center justify-center ${props.className ?? ''}`}
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
            {modalOpen && resolvedConfig && (
                <Suspense fallback={null}>
                    <CsvExportModal
                        config={resolvedConfig}
                        onClose={() => {
                            setModalOpen(false);
                            setResolvedConfig(null);
                        }}
                    />
                </Suspense>
            )}
        </>
    );
}
