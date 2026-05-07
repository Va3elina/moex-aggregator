/**
 * AnnotationToolbar — UI для аннотации:
 *   ✏️ Pen toggle (active state) | 🎨 5 color swatches | stroke width 3 sizes
 *   | ↶ Undo | ↷ Redo | 🗑 Clear
 *
 * Editorial-style: paper bg + 1.5px outline + accent для active states.
 * Mobile: flex-wraps по ширине, swatches и stroke-buttons остаются compact.
 *
 * Phase 4 будет: + Line / Arrow / Circle / Rectangle / Text + custom color.
 */

import { Pencil, RotateCcw, RotateCw, Trash2 } from 'lucide-react';
import type { AnnotationTool } from './AnnotationCanvas';

/** 5 preset colors — editorial palette. Theme-aware через CSS vars где возможно. */
export const COLOR_PRESETS = [
    { key: 'accent', value: 'var(--accent)', label: 'Акцент' },
    { key: 'primary', value: 'var(--text-primary)', label: 'Основной' },
    { key: 'red', value: '#E63946', label: 'Красный' },
    { key: 'green', value: '#06A77D', label: 'Зелёный' },
    { key: 'blue', value: '#1D7AB8', label: 'Синий' },
];

/** Stroke width presets — 3 size discrete values vs slider (Phase 4). */
export const STROKE_PRESETS = [
    { key: 'thin', value: 2, label: 'Тонкая' },
    { key: 'medium', value: 4, label: 'Средняя' },
    { key: 'thick', value: 8, label: 'Толстая' },
];

interface Props {
    tool: AnnotationTool;
    color: string;
    strokeWidth: number;
    canUndo: boolean;
    canRedo: boolean;
    canClear: boolean;
    onToolChange: (tool: AnnotationTool) => void;
    onColorChange: (color: string) => void;
    onStrokeWidthChange: (width: number) => void;
    onUndo: () => void;
    onRedo: () => void;
    onClear: () => void;
}

export default function AnnotationToolbar({
    tool,
    color,
    strokeWidth,
    canUndo,
    canRedo,
    canClear,
    onToolChange,
    onColorChange,
    onStrokeWidthChange,
    onUndo,
    onRedo,
    onClear,
}: Props) {
    const buttonStyle = {
        backgroundColor: 'var(--bg-primary)',
        border: '1.5px solid var(--text-primary)',
        color: 'var(--text-primary)',
    };

    const activeButtonStyle = {
        backgroundColor: 'var(--accent)',
        border: '1.5px solid var(--text-primary)',
        color: '#FFFFFF',
        boxShadow: 'var(--shadow-hard-chip)',
    };

    return (
        <div
            className="flex flex-wrap items-center"
            style={{ gap: 'var(--sp-2)', padding: 'var(--sp-2) var(--sp-3)' }}
        >
            {/* Pen toggle */}
            <button
                onClick={() => onToolChange('pen')}
                className="editorial-press rounded-lg p-2 inline-flex items-center justify-center"
                style={tool === 'pen' ? activeButtonStyle : buttonStyle}
                aria-label="Карандаш"
                title="Карандаш"
            >
                <Pencil size={16} />
            </button>

            {/* Color swatches */}
            <div
                className="flex items-center"
                style={{
                    gap: 'var(--sp-1)',
                    padding: '4px 6px',
                    borderRadius: 8,
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'var(--bg-primary)',
                }}
            >
                {COLOR_PRESETS.map((preset) => {
                    const isActive = color === preset.value;
                    return (
                        <button
                            key={preset.key}
                            onClick={() => onColorChange(preset.value)}
                            className="editorial-press rounded-full"
                            style={{
                                width: 22,
                                height: 22,
                                backgroundColor: preset.value,
                                border: isActive
                                    ? '2px solid var(--text-primary)'
                                    : '1px solid var(--text-primary)',
                                outline: isActive ? '2px solid var(--accent)' : 'none',
                                outlineOffset: 1,
                                cursor: 'pointer',
                            }}
                            aria-label={`Цвет: ${preset.label}`}
                            title={preset.label}
                        />
                    );
                })}
            </div>

            {/* Stroke width — три discrete preset */}
            <div
                className="flex items-center"
                style={{
                    gap: 'var(--sp-1)',
                    padding: '4px 6px',
                    borderRadius: 8,
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'var(--bg-primary)',
                }}
            >
                {STROKE_PRESETS.map((preset) => {
                    const isActive = strokeWidth === preset.value;
                    return (
                        <button
                            key={preset.key}
                            onClick={() => onStrokeWidthChange(preset.value)}
                            className="editorial-press inline-flex items-center justify-center rounded"
                            style={{
                                width: 26,
                                height: 22,
                                backgroundColor: isActive ? 'var(--accent)' : 'transparent',
                                border: '1px solid var(--text-primary)',
                            }}
                            aria-label={preset.label}
                            title={preset.label}
                        >
                            <span
                                style={{
                                    display: 'block',
                                    width: 14,
                                    height: preset.value,
                                    backgroundColor: isActive ? '#FFFFFF' : 'var(--text-primary)',
                                    borderRadius: preset.value / 2,
                                }}
                            />
                        </button>
                    );
                })}
            </div>

            {/* Spacer pushes undo/redo/clear to right.
                На mobile (wrap) — кнопки уйдут на следующую строку, что ОК. */}
            <div className="flex-1" style={{ minWidth: 'var(--sp-2)' }} />

            {/* Undo / Redo / Clear */}
            <button
                onClick={onUndo}
                disabled={!canUndo}
                className="editorial-press rounded-lg p-2 inline-flex items-center justify-center disabled:opacity-40 disabled:cursor-not-allowed"
                style={buttonStyle}
                aria-label="Отменить"
                title="Отменить (последнее действие)"
            >
                <RotateCcw size={16} />
            </button>
            <button
                onClick={onRedo}
                disabled={!canRedo}
                className="editorial-press rounded-lg p-2 inline-flex items-center justify-center disabled:opacity-40 disabled:cursor-not-allowed"
                style={buttonStyle}
                aria-label="Вернуть"
                title="Вернуть"
            >
                <RotateCw size={16} />
            </button>
            <button
                onClick={onClear}
                disabled={!canClear}
                className="editorial-press rounded-lg p-2 inline-flex items-center justify-center disabled:opacity-40 disabled:cursor-not-allowed"
                style={buttonStyle}
                aria-label="Очистить всю разметку"
                title="Очистить всю разметку"
            >
                <Trash2 size={16} />
            </button>
        </div>
    );
}
