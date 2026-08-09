/**
 * ExportModal — оркестратор state machine для capture → preview → annotate → download.
 *
 * State transitions (Phase 3):
 *   [capturing] → [preview] | [error]
 *   [preview] → [annotating] | [downloading] | close
 *   [annotating] → [downloading] | close (drawings будут потеряны при close)
 *   [downloading] → [preview] (после успешного download — user может скачать снова)
 *   [error] → close
 *
 * Lifecycle:
 *   - Mount → start capture
 *   - Unmount → abort in-flight capture, dispose fabric canvas (через AnnotationCanvas cleanup)
 *   - ESC / click outside / × → onClose() (с confirm если есть annotations)
 *
 * Portal-rendered в document.body чтобы избежать z-index issues с charts.
 *
 * Annotation flow:
 *   - Linear path для MVP: preview → annotating → download. "Назад к preview"
 *     не поддерживается (drawings терялись бы при unmount fabric).
 *   - Phase 4 добавит back-navigation с preserve drawings (display:none toggle).
 */

import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Download, Loader2, AlertCircle, Pencil, Copy, Check } from 'lucide-react';
import type { ExportModalState, ExportMetadata } from './types';
import { captureChart } from './captureChart';
import { composeFramedCanvas } from './composeFramedCanvas';
import { downloadCanvas, copyCanvasToClipboard } from './downloadCanvas';
import ChartPreview from './ChartPreview';
import AnnotationCanvas, {
    type AnnotationCanvasHandle,
    type AnnotationTool,
} from './AnnotationCanvas';
import AnnotationToolbar, { COLOR_PRESETS, STROKE_PRESETS } from './AnnotationToolbar';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { usePortalTheme } from '../../hooks/usePortalTheme';

/** Ждать N кадров подряд (не N независимых rAF-промисов запущенных разом —
 *  каждый следующий rAF планируется ТОЛЬКО после того, как разрешился предыдущий).
 *  Один кадр гарантирует лишь то, что колбэк выполнится ДО следующей отрисовки —
 *  сам РЕЗУЛЬТАТ синхронной DOM/canvas-мутации может докраситься только к кадру
 *  ПОСЛЕ этого. См. использование в mount-эффекте ниже. */
function waitFrames(n: number): Promise<void> {
    return new Promise((resolve) => {
        const step = (left: number) => {
            if (left <= 0) { resolve(); return; }
            requestAnimationFrame(() => step(left - 1));
        };
        step(n);
    });
}

/** Песочница (/sandbox) и эмбеды (/embed/*) — там шапка кадра масштабируется
 *  под ширину снимка (правка «текст очень мелкий на фото»). На сайте она
 *  остаётся прежнего кегля, иначе название индикатора в левом верхнем углу
 *  раздувается в полтора-два раза. */
function usesWidthScaledFrame(): boolean {
    if (typeof window === 'undefined') return false;
    const p = window.location.pathname;
    return p === '/sandbox' || p.startsWith('/sandbox/') || p.startsWith('/embed/');
}

interface Props {
    targetElement: HTMLElement;
    filename: string;
    metadata?: ExportMetadata;
    /** CSS property→value pairs to apply to targetElement BEFORE capture, restored AFTER.
     *  Используется для transient layout adjustments (e.g. --chart-pad-left override
     *  чтобы chart expanded в empty space только в exported PNG, без affecting live). */
    exportStyles?: Record<string, string>;
    /** Синхронный форс-хук перед captureChart — напр. LwChart.syncBeforeCapture(w,h):
     *  движки (lightweight-charts) держат внутренний размер через ResizeObserver →
     *  React state → рендер, асинхронно в несколько шагов; если html2canvas снимет
     *  скриншот внутри этого окна рассинхрона, контент (особенно рисунки поверх
     *  графика) окажется смещён/обрезан/пропадёт. Вызывается ПОСЛЕ exportStyles,
     *  тоже с ожиданием rAF перед captureChart. */
    beforeCapture?: () => void;
    /** Парный откат beforeCapture — напр. LwChart.restoreAfterCapture(): возврат
     *  временно увеличенного под captureFontScale font-size движка обратно к
     *  базовому. Вызывается СРАЗУ после captureChart (успех или ошибка) — live
     *  график на странице не должен остаться с раздутым шрифтом. */
    afterCapture?: () => void;
    onClose: () => void;
}

export default function ExportModal({ targetElement, filename, metadata, exportStyles, beforeCapture, afterCapture, onClose }: Props) {
    const [state, setState] = useState<ExportModalState>({ phase: 'capturing' });
    const abortRef = useRef<AbortController | null>(null);

    // Tier features — Free → watermark on export.
    const commonFeatures = useCommonFeatures();

    // Модалка уходит порталом в body — вне .sb-panel, на которой в песочнице
    // висит собственный data-theme панели. Без явного атрибута окно красилось бы
    // темой оболочки: светлая модалка поверх тёмной панели. Вне песочницы тема
    // поддерева и корневая совпадают, и хук ничего не добавляет.
    const portalTheme = usePortalTheme();

    // Annotation state — изолировано от phase, чтобы tool/color не сбрасывались
    // при downloading → preview transitions.
    const [tool, setTool] = useState<AnnotationTool>('pen');
    // Default red — наиболее universal annotation color (visible на любой palette).
    // accent (orange) был первым но user попросил red default.
    const [color, setColor] = useState<string>(
        COLOR_PRESETS.find(p => p.key === 'red')?.value ?? COLOR_PRESETS[0].value,
    );
    const [strokeWidth, setStrokeWidth] = useState<number>(STROKE_PRESETS[1].value);
    const annotationRef = useRef<AnnotationCanvasHandle>(null);
    // Force-update toolbar disabled-state когда меняется history (undo/redo/clear).
    // Не используем object count прямо — fabric meняет его внутренне, реагируем
    // через onHistoryChange callback который бампает counter.
    const [historyTick, setHistoryTick] = useState(0);
    const onHistoryChange = () => setHistoryTick((t) => t + 1);

    // Статус копирования в буфер — отдельно от phase (операция быстрая, без spinner-фазы).
    const [copyStatus, setCopyStatus] = useState<'idle' | 'copying' | 'copied' | 'error'>('idle');

    // Mount: start capture
    useEffect(() => {
        const ac = new AbortController();
        abortRef.current = ac;

        (async () => {
            // Apply transient export styles (CSS var overrides) на target.
            // Browser reflows на следующем frame → ждём rAF → captureChart видит
            // новый layout → восстанавливаем старые значения после capture.
            const oldStyles = new Map<string, string>();
            if (exportStyles) {
                for (const [prop, val] of Object.entries(exportStyles)) {
                    oldStyles.set(prop, targetElement.style.getPropertyValue(prop));
                    targetElement.style.setProperty(prop, val);
                }
                await waitFrames(2);
            }

            // Форс-синк движка/рисунков под РЕАЛЬНЫЙ текущий размер targetElement —
            // убирает асинхронный лаг ResizeObserver→state→рендер (см. beforeCapture
            // doc выше). ДВА rAF, не один: syncBeforeCapture в LwChart/LwChartPanes
            // теперь ещё и applyOptions({layout:{fontSize}}) (captureFontScale) —
            // lightweight-charts красит canvas асинхронно через свой внутренний
            // rAF-scheduler, и один кадр иногда не успевал долиться до html2canvas:
            // осевая панель уже получала новую ширину под больший шрифт, а сам
            // plot-canvas — ещё старую, экспорт получался с разъехавшимися
            // осью/сеткой (осевые цифры "сползали" в нижнюю половину кадра).
            // Поймано вживую с частотой ~1 из 3 попыток на реальных данных.
            if (beforeCapture) {
                beforeCapture();
                await waitFrames(2);
            }

            try {
                let raw: HTMLCanvasElement;
                try {
                    raw = await captureChart(targetElement, ac.signal);
                } finally {
                    // Откат ДО любых await ниже — живой график не должен ждать
                    // весь остаток pipeline с раздутым (под captureFontScale)
                    // шрифтом дольше, чем нужно самому captureChart.
                    afterCapture?.();
                }

                // Restore exportStyles immediately после capture (live page back to normal)
                if (exportStyles) {
                    for (const [prop, oldVal] of oldStyles) {
                        if (oldVal) targetElement.style.setProperty(prop, oldVal);
                        else targetElement.style.removeProperty(prop);
                    }
                }

                // Wait for fonts so canvas2D drawText использует Inter
                // (а не fallback) — header/footer text должен match chart.
                if (typeof document !== 'undefined' && 'fonts' in document) {
                    await document.fonts.ready;
                }

                // Цвета кадра снимаем с САМОГО снимаемого элемента, а не с <html>.
                // В песочнице у каждой панели свой data-theme на .sb-panel, а
                // корень несёт тему оболочки: читая переменные от корня, мы
                // рисовали светлые поля/шапку/подвал вокруг тёмного графика (и
                // наоборот). Кастомные свойства наследуются, поэтому обычного
                // getComputedStyle(targetElement) достаточно — вне песочницы он
                // вернёт ровно те же значения, что и корень.
                const computed = getComputedStyle(targetElement ?? document.documentElement);
                const bgColor = computed.getPropertyValue('--bg-primary').trim() || '#0E0E10';
                const textColor = computed.getPropertyValue('--text-primary').trim() || '#0A0A0A';
                const textSecondary = computed.getPropertyValue('--text-secondary').trim() || '#6B6B6B';
                const accent = computed.getPropertyValue('--accent').trim() || '#FF5C2B';

                const dpr = typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1;

                const framed = composeFramedCanvas(raw, {
                    background: bgColor,
                    textColor,
                    textSecondaryColor: textSecondary,
                    accentColor: accent,
                    dpr,
                    metadata,
                    watermark: commonFeatures.watermark_on_export,
                    scaleWithWidth: usesWidthScaledFrame(),
                });

                if (ac.signal.aborted) return;
                setState({ phase: 'preview', canvas: framed });
            } catch (err) {
                if (ac.signal.aborted) return;
                // Восстановить export styles даже при ошибке (защита от
                // "застрявшего" override на live page после crash в pipeline)
                if (exportStyles) {
                    for (const [prop, oldVal] of oldStyles) {
                        if (oldVal) targetElement.style.setProperty(prop, oldVal);
                        else targetElement.style.removeProperty(prop);
                    }
                }
                const msg = err instanceof Error ? err.message : 'Не удалось снять график';
                setState({ phase: 'error', message: msg });
            }
        })();

        return () => {
            ac.abort();
        };
    }, [targetElement, commonFeatures.watermark_on_export]);

    // ESC to close. Если есть annotations — confirm (защита от случайного потерь).
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') tryClose();
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [state, historyTick]);

    /** Close с confirm если в annotating и есть drawings */
    const tryClose = () => {
        if (state.phase === 'annotating') {
            const count = annotationRef.current?.getObjectsCount() ?? 0;
            if (count > 0) {
                if (!window.confirm('Закрыть окно? Разметка будет потеряна.')) return;
            }
        }
        onClose();
    };

    /** Скачать без разметки (из preview) */
    const handleDownloadPreview = async () => {
        if (state.phase !== 'preview') return;
        const canvasToDownload = state.canvas;
        setState({ phase: 'downloading' });
        try {
            await downloadCanvas(canvasToDownload, filename, 'png');
            setState({ phase: 'preview', canvas: canvasToDownload });
        } catch (err) {
            const msg = err instanceof Error ? err.message : 'Не удалось скачать файл';
            setState({ phase: 'error', message: msg });
        }
    };

    /** Скачать с разметкой (из annotating) — composite через fabric */
    const handleDownloadAnnotated = async () => {
        if (state.phase !== 'annotating') return;
        const composite = annotationRef.current?.exportToCanvas();
        if (!composite) {
            setState({ phase: 'error', message: 'Не удалось собрать разметку' });
            return;
        }
        const original = state.canvas;
        setState({ phase: 'downloading' });
        try {
            await downloadCanvas(composite, `${filename}-annotated`, 'png');
            // После download возвращаемся в annotating чтобы user мог редактировать
            // дальше (не сбрасывая drawings). К сожалению annotating требует canvas
            // в state — берём из original.
            setState({ phase: 'annotating', canvas: original });
        } catch (err) {
            const msg = err instanceof Error ? err.message : 'Не удалось скачать файл';
            setState({ phase: 'error', message: msg });
        }
    };

    /** Скопировать изображение в буфер обмена. preview → без разметки,
     *  annotating → composite с разметкой (как и download). Phase не меняем. */
    const handleCopy = async () => {
        let canvas: HTMLCanvasElement | null = null;
        if (state.phase === 'preview') canvas = state.canvas;
        else if (state.phase === 'annotating') canvas = annotationRef.current?.exportToCanvas() ?? null;
        if (!canvas) return;
        setCopyStatus('copying');
        try {
            await copyCanvasToClipboard(canvas);
            setCopyStatus('copied');
            setTimeout(() => setCopyStatus('idle'), 2000);
        } catch {
            setCopyStatus('error');
            setTimeout(() => setCopyStatus('idle'), 3000);
        }
    };

    const handleStartAnnotation = () => {
        if (state.phase !== 'preview') return;
        setState({ phase: 'annotating', canvas: state.canvas });
    };

    const handleClear = () => {
        const count = annotationRef.current?.getObjectsCount() ?? 0;
        if (count === 0) return;
        if (!window.confirm('Удалить всю разметку?')) return;
        annotationRef.current?.clear();
    };

    const objectsCount = annotationRef.current?.getObjectsCount() ?? 0;
    const canUndo = annotationRef.current?.canUndo() ?? false;
    const canRedo = annotationRef.current?.canRedo() ?? false;
    // historyTick — dependency для re-render toolbar disabled-state. Используется
    // imperatively (objectsCount/canUndo/canRedo читаются в render), поэтому ESLint
    // не видит её — оставляем явный void чтобы линтер не выпилил.
    void historyTick;

    return createPortal(
        <div
            {...portalTheme}
            className="fixed inset-0 z-[2000] flex items-center justify-center p-4"
            // color из хука (портал в body наследовал бы текст темы ОБОЛОЧКИ)
            // приезжает только в песочнице; свой backgroundColor держим всегда.
            style={{ backgroundColor: 'rgba(0, 0, 0, 0.65)', ...portalTheme.style }}
            onClick={tryClose}
            role="dialog"
            aria-modal="true"
        >
            <div
                className="rounded-2xl shadow-md flex flex-col"
                style={{
                    backgroundColor: 'var(--bg-primary)',
                    border: '1.5px solid var(--text-primary)',
                    boxShadow: '4px 4px 0 var(--text-primary)',
                    width: 'min(1400px, 100%)',
                    height: 'min(880px, 100%)',
                    maxHeight: '94vh',
                }}
                onClick={(e) => e.stopPropagation()}
            >
                {/* Header */}
                <div
                    className="flex items-center justify-between px-5 py-3"
                    style={{ borderBottom: '1px solid var(--border-color)' }}
                >
                    <h2
                        className="font-bold"
                        style={{
                            fontSize: 'var(--fs-lg)',
                            color: 'var(--text-primary)',
                            letterSpacing: '-0.01em',
                        }}
                    >
                        {state.phase === 'annotating' ? 'Разметка графика' : 'Экспорт графика'}
                    </h2>
                    <button
                        onClick={tryClose}
                        className="editorial-press p-2 rounded-lg"
                        style={{
                            backgroundColor: 'var(--bg-primary)',
                            border: '1.5px solid var(--text-primary)',
                            color: 'var(--text-primary)',
                        }}
                        aria-label="Закрыть"
                    >
                        <X size={18} />
                    </button>
                </div>

                {/* Annotation toolbar — show only in annotating phase */}
                {state.phase === 'annotating' && (
                    <div
                        style={{
                            borderBottom: '1px solid var(--border-color)',
                            backgroundColor: 'var(--bg-secondary)',
                        }}
                    >
                        <AnnotationToolbar
                            tool={tool}
                            color={color}
                            strokeWidth={strokeWidth}
                            canUndo={canUndo}
                            canRedo={canRedo}
                            canClear={objectsCount > 0}
                            onToolChange={setTool}
                            onColorChange={setColor}
                            onStrokeWidthChange={setStrokeWidth}
                            onUndo={() => annotationRef.current?.undo()}
                            onRedo={() => annotationRef.current?.redo()}
                            onClear={handleClear}
                        />
                    </div>
                )}

                {/* Body — phase-dependent */}
                <div className="flex-1 flex flex-col p-5 min-h-0" style={{ gap: 'var(--sp-3)' }}>
                    {state.phase === 'capturing' && (
                        <div className="flex-1 flex flex-col items-center justify-center gap-3">
                            <Loader2
                                className="animate-spin"
                                size={36}
                                style={{ color: 'var(--accent)' }}
                            />
                            <span
                                className="text-theme-secondary"
                                style={{ fontSize: 'var(--fs-sm)' }}
                            >
                                Снимаем график…
                            </span>
                        </div>
                    )}

                    {state.phase === 'preview' && <ChartPreview canvas={state.canvas} />}

                    {state.phase === 'annotating' && (
                        <AnnotationCanvas
                            ref={annotationRef}
                            background={state.canvas}
                            tool={tool}
                            color={color}
                            strokeWidth={strokeWidth}
                            onHistoryChange={onHistoryChange}
                            // После создания фигуры — auto-switch в select.
                            // Пользователь сразу видит handles на новой фигуре
                            // и может drag/resize/rotate без переключения tool.
                            onShapeCreated={() => setTool('select')}
                        />
                    )}

                    {state.phase === 'downloading' && (
                        <div className="flex-1 flex flex-col items-center justify-center gap-3">
                            <Loader2
                                className="animate-spin"
                                size={36}
                                style={{ color: 'var(--accent)' }}
                            />
                            <span
                                className="text-theme-secondary"
                                style={{ fontSize: 'var(--fs-sm)' }}
                            >
                                Скачиваем…
                            </span>
                        </div>
                    )}

                    {state.phase === 'error' && (
                        <div className="flex-1 flex flex-col items-center justify-center gap-3">
                            <AlertCircle size={36} style={{ color: 'var(--funds-flow-negative)' }} />
                            <span
                                className="text-theme-primary font-semibold"
                                style={{ fontSize: 'var(--fs-base)' }}
                            >
                                Ошибка
                            </span>
                            <span
                                className="text-theme-secondary text-center max-w-md"
                                style={{ fontSize: 'var(--fs-sm)' }}
                            >
                                {state.message}
                            </span>
                        </div>
                    )}
                </div>

                {/* Footer — toolbar (phase-dependent) */}
                <div
                    className="flex items-center justify-end px-5 py-3"
                    style={{ borderTop: '1px solid var(--border-color)', gap: 'var(--sp-2)' }}
                >
                    <button
                        onClick={tryClose}
                        className="editorial-press rounded-lg px-4 py-2 font-semibold"
                        style={{
                            backgroundColor: 'var(--bg-primary)',
                            border: '1.5px solid var(--text-primary)',
                            color: 'var(--text-primary)',
                            fontSize: 'var(--fs-sm)',
                        }}
                    >
                        Отмена
                    </button>

                    {/* "Рисовать" — только в preview */}
                    {state.phase === 'preview' && (
                        <button
                            onClick={handleStartAnnotation}
                            className="editorial-press rounded-lg px-4 py-2 font-semibold flex items-center gap-2"
                            style={{
                                backgroundColor: 'var(--bg-primary)',
                                border: '1.5px solid var(--text-primary)',
                                color: 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                            }}
                        >
                            <Pencil size={16} />
                            Рисовать
                        </button>
                    )}

                    {/* "Скопировать" — копирует изображение в буфер обмена (preview/annotating) */}
                    {(state.phase === 'preview' || state.phase === 'annotating') && (
                        <button
                            onClick={handleCopy}
                            disabled={copyStatus === 'copying'}
                            className="editorial-press rounded-lg px-4 py-2 font-semibold flex items-center gap-2"
                            style={{
                                backgroundColor: 'var(--bg-primary)',
                                border: '1.5px solid var(--text-primary)',
                                color: copyStatus === 'error' ? 'var(--danger, #dc2626)' : 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                            }}
                            title="Скопировать изображение в буфер обмена"
                        >
                            {copyStatus === 'copied' ? <Check size={16} /> : <Copy size={16} />}
                            {copyStatus === 'copied' ? 'Скопировано'
                                : copyStatus === 'copying' ? 'Копирую…'
                                : copyStatus === 'error' ? 'Не вышло'
                                : 'Скопировать'}
                        </button>
                    )}

                    {/* "Скачать" — preview/annotating, разные handlers */}
                    {(state.phase === 'preview' || state.phase === 'annotating') && (
                        <button
                            onClick={
                                state.phase === 'annotating'
                                    ? handleDownloadAnnotated
                                    : handleDownloadPreview
                            }
                            className="editorial-press rounded-lg px-4 py-2 font-semibold flex items-center gap-2"
                            style={{
                                backgroundColor: 'var(--accent)',
                                border: '1.5px solid var(--text-primary)',
                                // #fff hardcoded: bg=accent (pumpkin) — не зависит от темы
                                color: '#fff',
                                fontSize: 'var(--fs-sm)',
                            }}
                        >
                            <Download size={16} />
                            {state.phase === 'annotating' ? 'Скачать с разметкой' : 'Скачать'}
                        </button>
                    )}

                    {/* downloading/error/capturing — disabled placeholder для visual consistency */}
                    {(state.phase === 'downloading' ||
                        state.phase === 'error' ||
                        state.phase === 'capturing') && (
                        <button
                            disabled
                            className="rounded-lg px-4 py-2 font-semibold flex items-center gap-2 opacity-50 cursor-not-allowed"
                            style={{
                                backgroundColor: 'var(--accent)',
                                border: '1.5px solid var(--text-primary)',
                                // #fff hardcoded: bg=accent (pumpkin) — не зависит от темы
                                color: '#fff',
                                fontSize: 'var(--fs-sm)',
                            }}
                        >
                            <Download size={16} />
                            Скачать
                        </button>
                    )}
                </div>
            </div>
        </div>,
        document.body,
    );
}
