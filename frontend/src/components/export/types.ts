/**
 * Shared types для chart export feature.
 *
 * Architecture:
 *   ChartCaptureButton  — кнопка-trigger в углу chart-card
 *   ExportModal         — modal-контейнер, оркестратор state machine
 *   ChartPreview        — рендер framed canvas в modal
 *   composeFramedCanvas — utility: raw chart canvas → framed 16:9 canvas
 *   captureChart        — utility: HTMLElement → raw canvas (html2canvas wrapper)
 *   downloadCanvas      — utility: canvas → PNG download
 *
 * Lazy-loaded boundaries:
 *   html2canvas  — dynamic import при первом capture
 *   fabric.js    — dynamic import при первом "Рисовать" (Phase 3)
 */

/** Метаданные для имени файла экспорта */
export interface ExportMeta {
    /** Имя файла без extension (e.g. "frame-oi-sber-5m") */
    filename: string;
}

/** Состояния ExportModal — discriminated union предотвращает invalid combos */
export type ExportModalState =
    | { phase: 'capturing' }
    | { phase: 'preview'; canvas: HTMLCanvasElement }
    | { phase: 'annotating'; canvas: HTMLCanvasElement }
    | { phase: 'downloading' }
    | { phase: 'error'; message: string };

/** Опции для composeFramedCanvas */
export interface FrameOptions {
    /** Background color (theme-aware) */
    background: string;
    /** Padding по краям в pixels (default 48) */
    padding?: number;
    /** Target aspect ratio (default 16/9) */
    aspectRatio?: number;
}
