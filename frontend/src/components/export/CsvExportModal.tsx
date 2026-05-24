/**
 * CsvExportModal — модалка выбора что включить в CSV-экспорт.
 *
 * Открывается из CsvExportButton (если у юзера Pro). Юзер видит:
 *   1. Текущие параметры индикатора (period, ticker, mode etc.) — readonly,
 *      показывает что попадёт в выгрузку.
 *   2. Список слоёв (layers) — checkbox'ы. Выбрал > 1 → backend отдаст ZIP
 *      с отдельными CSV.
 *   3. Format selector (пока только CSV, заглушка для будущего XLSX/JSON).
 *   4. Кнопки: Скачать / Отмена.
 *
 * Config-driven: страница декларирует свой CsvExportConfig (layers, params,
 * URL builder) и передаёт в CsvExportButton, который пробрасывает в модалку.
 */
import { useState, useEffect } from 'react';
import { X, Download, FileText, FileArchive } from 'lucide-react';
import { apiFetch } from '../../services/api';
import { useAnalytics } from '../../contexts/AnalyticsContext';

export interface CsvLayer {
    id: string;
    label: string;
    description: string;
    defaultSelected?: boolean;
}

export interface CsvParamSummary {
    label: string;
    value: string;
}

export interface CsvExportConfig {
    /** Indicator ID для analytics + UpgradeModal. */
    indicator: string;
    /** Заголовок модалки — "Экспорт: Сезонность SBER". */
    title: string;
    /** Список слоёв что юзер может выбрать. Если 1 — скрываем секцию. */
    layers: CsvLayer[];
    /** Параметры текущего UI-состояния — readonly, информация для юзера. */
    params: CsvParamSummary[];
    /** Builder URL'а на основе выбранных layer ID. */
    buildUrl: (layerIds: string[]) => string;
    /** Builder имени файла на основе выбранных layer ID. */
    buildFilename: (layerIds: string[]) => string;
}

interface Props {
    config: CsvExportConfig;
    onClose: () => void;
}

export default function CsvExportModal({ config, onClose }: Props) {
    const { track } = useAnalytics();

    // Default selection: defaultSelected=true → on; иначе если только 1 layer → on.
    const [selected, setSelected] = useState<Set<string>>(() => {
        const initial = new Set<string>();
        for (const l of config.layers) {
            if (l.defaultSelected || config.layers.length === 1) {
                initial.add(l.id);
            }
        }
        // Если ни один не дефолтный — выбираем первый.
        if (initial.size === 0 && config.layers.length > 0) {
            initial.add(config.layers[0].id);
        }
        return initial;
    });
    const [downloading, setDownloading] = useState(false);

    // Esc — close. Body scroll lock.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && !downloading) onClose();
        };
        document.addEventListener('keydown', onKey);
        const prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.removeEventListener('keydown', onKey);
            document.body.style.overflow = prevOverflow;
        };
    }, [onClose, downloading]);

    const toggleLayer = (id: string) => {
        setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(id)) {
                // Не позволяем снять последний — должен быть хотя бы 1 layer.
                if (next.size > 1) next.delete(id);
            } else {
                next.add(id);
            }
            return next;
        });
    };

    const handleDownload = async () => {
        if (selected.size === 0) return;
        const layerIds = Array.from(selected);
        const url = config.buildUrl(layerIds);
        const filename = config.buildFilename(layerIds);

        track('chart_export', {
            indicator: config.indicator,
            format: layerIds.length > 1 ? 'zip' : 'csv',
            layers: layerIds.join(','),
        });

        setDownloading(true);
        try {
            const resp = await apiFetch(url);
            if (!resp.ok) {
                // eslint-disable-next-line no-alert
                alert(`Не удалось скачать (статус ${resp.status})`);
                return;
            }
            const blob = await resp.blob();
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            setTimeout(() => URL.revokeObjectURL(blobUrl), 1000);
            onClose();
        } catch (err) {
            console.error('[CsvExportModal] download failed:', err);
            // eslint-disable-next-line no-alert
            alert('Ошибка скачивания. Проверьте подключение и попробуйте ещё раз.');
        } finally {
            setDownloading(false);
        }
    };

    const isZip = selected.size > 1;
    const hideLayersSection = config.layers.length <= 1;

    return (
        <div
            // Backdrop
            onClick={(e) => {
                if (e.target === e.currentTarget && !downloading) onClose();
            }}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'color-mix(in srgb, var(--text-primary) 50%, transparent)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                zIndex: 1000,
                padding: 16,
            }}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="csv-export-title"
                style={{
                    background: 'var(--bg-primary)',
                    border: '2px solid var(--text-primary)',
                    borderRadius: 16,
                    boxShadow: '6px 6px 0 var(--text-primary)',
                    width: '100%',
                    maxWidth: 540,
                    maxHeight: '90vh',
                    overflowY: 'auto',
                }}
            >
                {/* Header */}
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        padding: '16px 20px',
                        borderBottom: '1.5px solid var(--text-primary)',
                    }}
                >
                    <h2
                        id="csv-export-title"
                        style={{
                            fontSize: 'var(--fs-lg)',
                            fontWeight: 800,
                            color: 'var(--text-primary)',
                            margin: 0,
                        }}
                    >
                        {config.title}
                    </h2>
                    <button
                        onClick={onClose}
                        disabled={downloading}
                        aria-label="Закрыть"
                        style={{
                            background: 'transparent',
                            border: 'none',
                            cursor: downloading ? 'wait' : 'pointer',
                            color: 'var(--text-primary)',
                            padding: 4,
                        }}
                    >
                        <X size={20} />
                    </button>
                </div>

                {/* Layers selection */}
                {!hideLayersSection && (
                    <div style={{ padding: '16px 20px', borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 15%, transparent)' }}>
                        <div
                            style={{
                                fontSize: 11,
                                fontWeight: 800,
                                color: 'var(--text-secondary)',
                                textTransform: 'uppercase',
                                letterSpacing: '0.06em',
                                marginBottom: 10,
                            }}
                        >
                            Что включить
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {config.layers.map((layer) => {
                                const checked = selected.has(layer.id);
                                return (
                                    <label
                                        key={layer.id}
                                        style={{
                                            display: 'flex',
                                            alignItems: 'flex-start',
                                            gap: 10,
                                            padding: '10px 12px',
                                            background: checked ? 'color-mix(in srgb, var(--accent) 10%, var(--bg-secondary))' : 'var(--bg-secondary)',
                                            border: '1.5px solid var(--text-primary)',
                                            borderRadius: 10,
                                            cursor: 'pointer',
                                        }}
                                    >
                                        <input
                                            type="checkbox"
                                            checked={checked}
                                            onChange={() => toggleLayer(layer.id)}
                                            style={{
                                                marginTop: 2,
                                                accentColor: 'var(--accent)',
                                                cursor: 'pointer',
                                            }}
                                        />
                                        <span style={{ flex: 1 }}>
                                            <div style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>
                                                {layer.label}
                                            </div>
                                            <div
                                                style={{
                                                    fontSize: 'var(--fs-xs)',
                                                    color: 'var(--text-muted)',
                                                    marginTop: 2,
                                                    lineHeight: 1.4,
                                                }}
                                            >
                                                {layer.description}
                                            </div>
                                        </span>
                                    </label>
                                );
                            })}
                        </div>
                    </div>
                )}

                {/* Params summary */}
                {config.params.length > 0 && (
                    <div style={{ padding: '16px 20px', borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 15%, transparent)' }}>
                        <div
                            style={{
                                fontSize: 11,
                                fontWeight: 800,
                                color: 'var(--text-secondary)',
                                textTransform: 'uppercase',
                                letterSpacing: '0.06em',
                                marginBottom: 10,
                            }}
                        >
                            Параметры экспорта
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                            {config.params.map((p) => (
                                <div
                                    key={p.label}
                                    style={{ display: 'flex', justifyContent: 'space-between', gap: 12, fontSize: 'var(--fs-sm)' }}
                                >
                                    <span style={{ color: 'var(--text-muted)' }}>{p.label}</span>
                                    <span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{p.value}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                )}

                {/* Format hint */}
                <div style={{ padding: '14px 20px', borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 15%, transparent)' }}>
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 10,
                            fontSize: 'var(--fs-sm)',
                            color: 'var(--text-secondary)',
                        }}
                    >
                        {isZip ? (
                            <>
                                <FileArchive size={16} />
                                <span>Формат: ZIP-архив ({selected.size} CSV файлов)</span>
                            </>
                        ) : (
                            <>
                                <FileText size={16} />
                                <span>Формат: CSV (UTF-8, открывается в Excel/Numbers)</span>
                            </>
                        )}
                    </div>
                </div>

                {/* Actions */}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, padding: '14px 20px' }}>
                    <button
                        onClick={onClose}
                        disabled={downloading}
                        style={{
                            padding: '8px 18px',
                            background: 'var(--bg-secondary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            color: 'var(--text-primary)',
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            cursor: downloading ? 'wait' : 'pointer',
                        }}
                    >
                        Отмена
                    </button>
                    <button
                        onClick={handleDownload}
                        disabled={downloading || selected.size === 0}
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '8px 18px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            cursor: downloading ? 'wait' : 'pointer',
                            boxShadow: 'var(--shadow-hard-chip)',
                        }}
                    >
                        <Download size={16} />
                        {downloading ? 'Скачиваем…' : 'Скачать'}
                    </button>
                </div>
            </div>
        </div>
    );
}
