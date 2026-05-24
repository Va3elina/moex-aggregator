/**
 * CsvExportModal — конфигуратор CSV-экспорта.
 *
 * Расширенная версия: помимо layer-чекбоксов даёт юзеру задать ПАРАМЕТРЫ
 * самого экспорта (тикеры, период, режим etc.). Defaults = current UI state,
 * но юзер может override / добавить дополнительные значения.
 *
 * При multi-select (несколько тикеров, несколько режимов) backend упаковывает
 * результат в ZIP — каждый CSV per combination.
 *
 * Config-driven — каждая страница декларирует layers + selectors + builders.
 */
import { useState, useEffect, useMemo, lazy, Suspense } from 'react';
import { X, Download, FileText, FileArchive, Check, Plus } from 'lucide-react';
import { apiFetch } from '../../services/api';
import { useAnalytics } from '../../contexts/AnalyticsContext';

const MultiInstrumentSearchModal = lazy(() => import('./MultiInstrumentSearchModal'));

export interface CsvLayer {
    id: string;
    label: string;
    description: string;
    defaultSelected?: boolean;
}

export interface CsvSelectOption {
    value: string;
    label: string;
}

/** Один из 4 типов селекторов в модалке. */
export type CsvSelector =
    | {
        kind: 'select';
        id: string;
        label: string;
        options: CsvSelectOption[];
        default: string;
    }
    | {
        kind: 'multiselect';
        id: string;
        label: string;
        options: CsvSelectOption[];
        default: string[];
        hint?: string;
    }
    | {
        kind: 'number';
        id: string;
        label: string;
        default: number;
        min?: number;
        max?: number;
        suffix?: string;
    }
    | {
        // Picker — открывает MultiInstrumentSearchModal для выбора любого
        // инструмента из БД (550 шт). В отличие от multiselect (фиксированный
        // список) — даёт неограниченный выбор + поиск + категории.
        kind: 'instrument-picker';
        id: string;
        label: string;
        default: string[];
        hint?: string;
        filterType?: 'stock' | 'futures' | 'no-futures';
        pickerTitle?: string;
        /** Лимит — защита backend от monster-ZIP. По default 6. */
        maxItems?: number;
    }
    | {
        // Period — toggle между preset'ами (1M/3M/6M/1Y/...) и custom date range.
        // Default = preset (для UX простоты), юзер может переключиться в "Точные
        // даты" с двумя date input'ами.
        // Value хранится как объект { type: 'preset' | 'range', ... }, в URL
        // конвертируется в days=N или from=...&to=... через buildUrl callback.
        kind: 'period';
        id: string;
        label: string;
        presets: { value: string; label: string; days: number }[];
        default: PeriodValue;
    };

export type PeriodValue =
    | { type: 'preset'; value: string }
    | { type: 'range'; from: string; to: string };

/** Все типы values в записи селекторов. */
export type CsvSelectorValue = string | string[] | number | PeriodValue;

export interface CsvExportConfig {
    indicator: string;
    title: string;
    layers: CsvLayer[];
    /** Параметры с возможностью override. Default = current UI state. */
    selectors: CsvSelector[];
    /** URL builder — получает выбранные layers + текущие selector values. */
    buildUrl: (
        layers: string[],
        values: Record<string, CsvSelectorValue>,
    ) => string;
    /** Filename builder — то же сигнатура. */
    buildFilename: (
        layers: string[],
        values: Record<string, CsvSelectorValue>,
    ) => string;
}

interface Props {
    config: CsvExportConfig;
    onClose: () => void;
}

export default function CsvExportModal({ config, onClose }: Props) {
    const { track } = useAnalytics();

    // ──── Layer selection state ────
    const [selectedLayers, setSelectedLayers] = useState<Set<string>>(() => {
        const initial = new Set<string>();
        for (const l of config.layers) {
            if (l.defaultSelected || config.layers.length === 1) {
                initial.add(l.id);
            }
        }
        if (initial.size === 0 && config.layers.length > 0) {
            initial.add(config.layers[0].id);
        }
        return initial;
    });

    // ──── Selectors values state ────
    const [values, setValues] = useState<Record<string, CsvSelectorValue>>(() => {
        const init: Record<string, CsvSelectorValue> = {};
        for (const s of config.selectors) init[s.id] = s.default;
        return init;
    });

    const [downloading, setDownloading] = useState(false);
    const [format, setFormat] = useState<'csv' | 'xlsx'>('csv');

    // Esc + body scroll lock.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && !downloading) onClose();
        };
        document.addEventListener('keydown', onKey);
        const prev = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.removeEventListener('keydown', onKey);
            document.body.style.overflow = prev;
        };
    }, [onClose, downloading]);

    const toggleLayer = (id: string) => {
        setSelectedLayers((prev) => {
            const next = new Set(prev);
            if (next.has(id)) {
                if (next.size > 1) next.delete(id);
            } else {
                next.add(id);
            }
            return next;
        });
    };

    // Сколько combinations всего → если ≥2, будет ZIP.
    const totalCombinations = useMemo(() => {
        let count = selectedLayers.size;
        for (const s of config.selectors) {
            if (s.kind === 'multiselect' || s.kind === 'instrument-picker') {
                const v = values[s.id];
                if (Array.isArray(v)) count *= Math.max(1, v.length);
            }
        }
        return count;
    }, [selectedLayers, values, config.selectors]);

    const isZip = totalCombinations > 1;

    const handleDownload = async () => {
        if (selectedLayers.size === 0) return;
        const layerIds = Array.from(selectedLayers);
        // Add format to URL — backend выбирает CSV/ZIP/XLSX по этому param'у.
        let url = config.buildUrl(layerIds, values);
        url += (url.includes('?') ? '&' : '?') + `fmt=${format}`;

        // Filename: для XLSX заменяем .csv/.zip → .xlsx.
        let filename = config.buildFilename(layerIds, values);
        if (format === 'xlsx') {
            filename = filename.replace(/\.(csv|zip)$/, '.xlsx');
            if (!filename.endsWith('.xlsx')) filename += '.xlsx';
        }

        track('chart_export', {
            indicator: config.indicator,
            format,
            layers: layerIds.join(','),
            combinations: totalCombinations,
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

    return (
        <div
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
                    maxWidth: 580,
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
                        position: 'sticky',
                        top: 0,
                        background: 'var(--bg-primary)',
                        zIndex: 1,
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

                {/* Layers */}
                {config.layers.length > 1 && (
                    <Section title="Что включить">
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {config.layers.map((layer) => {
                                const checked = selectedLayers.has(layer.id);
                                return (
                                    <label
                                        key={layer.id}
                                        style={{
                                            display: 'flex',
                                            alignItems: 'flex-start',
                                            gap: 10,
                                            padding: '10px 12px',
                                            background: checked
                                                ? 'color-mix(in srgb, var(--accent) 10%, var(--bg-secondary))'
                                                : 'var(--bg-secondary)',
                                            border: '1.5px solid var(--text-primary)',
                                            borderRadius: 10,
                                            cursor: 'pointer',
                                        }}
                                    >
                                        <input
                                            type="checkbox"
                                            checked={checked}
                                            onChange={() => toggleLayer(layer.id)}
                                            style={{ marginTop: 2, accentColor: 'var(--accent)', cursor: 'pointer' }}
                                        />
                                        <span style={{ flex: 1 }}>
                                            <div style={{ fontWeight: 700, fontSize: 'var(--fs-sm)' }}>{layer.label}</div>
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
                    </Section>
                )}

                {/* Selectors — параметры экспорта */}
                {config.selectors.length > 0 && (
                    <Section title="Параметры экспорта">
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                            {config.selectors.map((sel) => (
                                <SelectorControl
                                    key={sel.id}
                                    selector={sel}
                                    value={values[sel.id]}
                                    onChange={(v) => setValues((prev) => ({ ...prev, [sel.id]: v }))}
                                />
                            ))}
                        </div>
                    </Section>
                )}

                {/* Format selector — radio: CSV vs XLSX */}
                <Section title="Формат файла">
                    <div style={{ display: 'flex', gap: 8 }}>
                        <FormatButton
                            active={format === 'csv'}
                            onClick={() => setFormat('csv')}
                            icon={isZip ? <FileArchive size={14} /> : <FileText size={14} />}
                            label={isZip ? `ZIP · ${totalCombinations} CSV` : 'CSV'}
                            description="UTF-8 · открывается в Excel/Numbers/любом редакторе"
                        />
                        <FormatButton
                            active={format === 'xlsx'}
                            onClick={() => setFormat('xlsx')}
                            icon={<FileText size={14} />}
                            label="XLSX"
                            description={
                                totalCombinations > 1
                                    ? `Excel · ${totalCombinations} sheet'ов в одном файле`
                                    : 'Excel · нативный формат'
                            }
                        />
                    </div>
                </Section>

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
                        disabled={downloading || selectedLayers.size === 0}
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

// ────────────────────────────────────────────────────────────────────
// Subcomponents
// ────────────────────────────────────────────────────────────────────

function FormatButton({
    active,
    onClick,
    icon,
    label,
    description,
}: {
    active: boolean;
    onClick: () => void;
    icon: React.ReactNode;
    label: string;
    description: string;
}) {
    return (
        <button
            type="button"
            onClick={onClick}
            style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'flex-start',
                gap: 4,
                padding: '10px 14px',
                background: active
                    ? 'color-mix(in srgb, var(--accent) 12%, var(--bg-secondary))'
                    : 'var(--bg-secondary)',
                border: `1.5px solid ${active ? 'var(--accent)' : 'var(--text-primary)'}`,
                borderRadius: 10,
                cursor: 'pointer',
                textAlign: 'left',
            }}
        >
            <span
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    fontWeight: 700,
                    fontSize: 'var(--fs-sm)',
                    color: 'var(--text-primary)',
                }}
            >
                {icon}
                {label}
            </span>
            <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', lineHeight: 1.3 }}>
                {description}
            </span>
        </button>
    );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
    return (
        <div
            style={{
                padding: '16px 20px',
                borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 15%, transparent)',
            }}
        >
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
                {title}
            </div>
            {children}
        </div>
    );
}

interface SelectorControlProps {
    selector: CsvSelector;
    value: CsvSelectorValue | undefined;
    onChange: (v: CsvSelectorValue) => void;
}

function SelectorControl({ selector, value, onChange }: SelectorControlProps) {
    if (selector.kind === 'select') {
        return (
            <div>
                <label
                    style={{
                        display: 'block',
                        fontSize: 'var(--fs-xs)',
                        fontWeight: 700,
                        color: 'var(--text-secondary)',
                        marginBottom: 6,
                    }}
                >
                    {selector.label}
                </label>
                <select
                    value={(value as string) ?? selector.default}
                    onChange={(e) => onChange(e.target.value)}
                    style={{
                        width: '100%',
                        padding: '8px 12px',
                        background: 'var(--bg-secondary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 10,
                        color: 'var(--text-primary)',
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 600,
                    }}
                >
                    {selector.options.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                            {opt.label}
                        </option>
                    ))}
                </select>
            </div>
        );
    }

    if (selector.kind === 'multiselect') {
        const current = Array.isArray(value) ? value : selector.default;
        const toggle = (v: string) => {
            const next = current.includes(v) ? current.filter((x) => x !== v) : [...current, v];
            onChange(next);
        };
        return (
            <div>
                <label
                    style={{
                        display: 'block',
                        fontSize: 'var(--fs-xs)',
                        fontWeight: 700,
                        color: 'var(--text-secondary)',
                        marginBottom: 6,
                    }}
                >
                    {selector.label}
                </label>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {selector.options.map((opt) => {
                        const selected = current.includes(opt.value);
                        return (
                            <button
                                key={opt.value}
                                type="button"
                                onClick={() => toggle(opt.value)}
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 4,
                                    padding: '6px 12px',
                                    background: selected ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: selected ? 'var(--text-inverse)' : 'var(--text-primary)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 600,
                                    cursor: 'pointer',
                                }}
                            >
                                {selected && <Check size={12} strokeWidth={3} />}
                                {opt.label}
                            </button>
                        );
                    })}
                </div>
                {selector.hint && (
                    <div
                        style={{
                            marginTop: 6,
                            fontSize: 'var(--fs-2xs)',
                            color: 'var(--text-muted)',
                            lineHeight: 1.4,
                        }}
                    >
                        {selector.hint}
                    </div>
                )}
            </div>
        );
    }

    if (selector.kind === 'instrument-picker') {
        const current = Array.isArray(value) ? (value as string[]) : selector.default;
        return (
            <InstrumentPickerControl
                selector={selector}
                value={current}
                onChange={(v) => onChange(v)}
            />
        );
    }

    if (selector.kind === 'period') {
        const current = (value as PeriodValue | undefined) ?? selector.default;
        return (
            <PeriodControl
                selector={selector}
                value={current}
                onChange={(v) => onChange(v)}
            />
        );
    }

    // number
    return (
        <div>
            <label
                style={{
                    display: 'block',
                    fontSize: 'var(--fs-xs)',
                    fontWeight: 700,
                    color: 'var(--text-secondary)',
                    marginBottom: 6,
                }}
            >
                {selector.label}
            </label>
            <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                <input
                    type="number"
                    value={(value as number) ?? selector.default}
                    min={selector.min}
                    max={selector.max}
                    onChange={(e) => onChange(Number(e.target.value))}
                    style={{
                        width: 140,
                        padding: '8px 12px',
                        background: 'var(--bg-secondary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 10,
                        color: 'var(--text-primary)',
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 600,
                    }}
                />
                {selector.suffix && (
                    <span style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-muted)' }}>
                        {selector.suffix}
                    </span>
                )}
            </div>
        </div>
    );
}

// ────────────────────────────────────────────────────────────────────
// InstrumentPickerControl — для 'instrument-picker' kind.
// Chip-row выбранных + "+ Добавить" button → открывает picker modal.
// ────────────────────────────────────────────────────────────────────

interface InstrumentPickerProps {
    selector: Extract<CsvSelector, { kind: 'instrument-picker' }>;
    value: string[];
    onChange: (v: string[]) => void;
}

function InstrumentPickerControl({ selector, value, onChange }: InstrumentPickerProps) {
    const [pickerOpen, setPickerOpen] = useState(false);

    const removeTicker = (ticker: string) => {
        onChange(value.filter((t) => t !== ticker));
    };

    return (
        <div>
            <label
                style={{
                    display: 'block',
                    fontSize: 'var(--fs-xs)',
                    fontWeight: 700,
                    color: 'var(--text-secondary)',
                    marginBottom: 6,
                }}
            >
                {selector.label}
            </label>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, alignItems: 'center' }}>
                {value.map((ticker) => (
                    <span
                        key={ticker}
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 4,
                            padding: '4px 8px 4px 12px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 600,
                        }}
                    >
                        {ticker}
                        <button
                            type="button"
                            onClick={() => removeTicker(ticker)}
                            aria-label={`Убрать ${ticker}`}
                            style={{
                                background: 'transparent',
                                border: 'none',
                                color: 'inherit',
                                cursor: 'pointer',
                                padding: 0,
                                display: 'inline-flex',
                                opacity: 0.85,
                            }}
                        >
                            <X size={12} strokeWidth={2.5} />
                        </button>
                    </span>
                ))}
                <button
                    type="button"
                    onClick={() => setPickerOpen(true)}
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                        padding: '6px 12px',
                        background: 'var(--bg-secondary)',
                        color: 'var(--text-primary)',
                        border: '1.5px dashed var(--text-primary)',
                        borderRadius: 999,
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 600,
                        cursor: 'pointer',
                    }}
                >
                    <Plus size={12} strokeWidth={2.5} />
                    {value.length === 0 ? 'Выбрать' : 'Изменить'}
                </button>
            </div>
            {selector.hint && (
                <div
                    style={{
                        marginTop: 6,
                        fontSize: 'var(--fs-2xs)',
                        color: 'var(--text-muted)',
                        lineHeight: 1.4,
                    }}
                >
                    {selector.hint}
                </div>
            )}
            {pickerOpen && (
                <Suspense fallback={null}>
                    <MultiInstrumentSearchModal
                        initial={value}
                        filterType={selector.filterType}
                        title={selector.pickerTitle ?? 'Выберите инструменты'}
                        maxItems={selector.maxItems ?? 6}
                        onConfirm={(tickers) => {
                            onChange(tickers);
                            setPickerOpen(false);
                        }}
                        onClose={() => setPickerOpen(false)}
                    />
                </Suspense>
            )}
        </div>
    );
}

// ────────────────────────────────────────────────────────────────────
// PeriodControl — toggle между preset (1M/3M/6M/1Y/2Y/All) и custom date range.
// ────────────────────────────────────────────────────────────────────

interface PeriodControlProps {
    selector: Extract<CsvSelector, { kind: 'period' }>;
    value: PeriodValue;
    onChange: (v: PeriodValue) => void;
}

function PeriodControl({ selector, value, onChange }: PeriodControlProps) {
    const isRange = value.type === 'range';

    // Today as default `to`, 1 year ago as `from` для smooth UX когда юзер
    // первый раз переключается в Range mode.
    const defaultRange = useMemo(() => {
        const today = new Date().toISOString().slice(0, 10);
        const yearAgo = new Date(Date.now() - 365 * 86400_000).toISOString().slice(0, 10);
        return { type: 'range' as const, from: yearAgo, to: today };
    }, []);

    return (
        <div>
            <label
                style={{
                    display: 'block',
                    fontSize: 'var(--fs-xs)',
                    fontWeight: 700,
                    color: 'var(--text-secondary)',
                    marginBottom: 6,
                }}
            >
                {selector.label}
            </label>

            {/* Tabs: Period preset vs Date range */}
            <div style={{ display: 'flex', gap: 4, marginBottom: 10 }}>
                <button
                    type="button"
                    onClick={() =>
                        onChange({
                            type: 'preset',
                            value: selector.presets[selector.presets.length - 2]?.value ?? selector.presets[0].value,
                        })
                    }
                    style={tabStyle(!isRange)}
                >
                    Период
                </button>
                <button
                    type="button"
                    onClick={() => onChange(isRange ? value : defaultRange)}
                    style={tabStyle(isRange)}
                >
                    Даты
                </button>
            </div>

            {/* Preset chips */}
            {value.type === 'preset' && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {selector.presets.map((p) => {
                        const active = value.value === p.value;
                        return (
                            <button
                                key={p.value}
                                type="button"
                                onClick={() => onChange({ type: 'preset', value: p.value })}
                                style={{
                                    padding: '6px 12px',
                                    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
                                    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
                                    border: '1.5px solid var(--text-primary)',
                                    borderRadius: 999,
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 600,
                                    cursor: 'pointer',
                                }}
                            >
                                {p.label}
                            </button>
                        );
                    })}
                </div>
            )}

            {/* Date range inputs */}
            {value.type === 'range' && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
                    <DateInput
                        label="С"
                        value={value.from}
                        onChange={(d) => onChange({ type: 'range', from: d, to: value.to })}
                    />
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                    <DateInput
                        label="По"
                        value={value.to}
                        onChange={(d) => onChange({ type: 'range', from: value.from, to: d })}
                    />
                </div>
            )}
        </div>
    );
}

function tabStyle(active: boolean): React.CSSProperties {
    return {
        padding: '4px 12px',
        background: active ? 'var(--text-primary)' : 'transparent',
        color: active ? 'var(--bg-primary)' : 'var(--text-primary)',
        border: '1.5px solid var(--text-primary)',
        borderRadius: 8,
        fontSize: 'var(--fs-xs)',
        fontWeight: 700,
        cursor: 'pointer',
    };
}

function DateInput({
    label,
    value,
    onChange,
}: {
    label: string;
    value: string;
    onChange: (v: string) => void;
}) {
    return (
        <span style={{ display: 'inline-flex', flexDirection: 'column', gap: 2 }}>
            <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>{label}</span>
            <input
                type="date"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                style={{
                    padding: '6px 10px',
                    background: 'var(--bg-secondary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 8,
                    color: 'var(--text-primary)',
                    fontSize: 'var(--fs-sm)',
                    fontWeight: 600,
                }}
            />
        </span>
    );
}
