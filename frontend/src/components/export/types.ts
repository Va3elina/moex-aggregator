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

/** Метаданные для рендера header/footer экспорт-фрейма. */
export interface ExportMetadata {
    /** Заголовок (имя индикатора), e.g. "Открытый интерес" */
    title: string;
    /** Имя актива/инструмента, e.g. "Сбербанк" */
    asset?: string;
    /** Тикер актива, e.g. "SR" */
    ticker?: string;
    /** Period / timeframe / detail tags, e.g. ["1 час", "6 месяцев", "Чистая позиция"] */
    details?: string[];
}

/** Опции для composeFramedCanvas */
export interface FrameOptions {
    /** Background color (theme-aware) */
    background: string;
    /** Color для текста header/footer (theme-aware) */
    textColor?: string;
    /** Цвет вторичного текста (subtitle/footer) */
    textSecondaryColor?: string;
    /** Color для accent элементов (border, ticker badge) */
    accentColor?: string;
    /** Padding по краям в pixels (default 48) */
    padding?: number;
    /** Target aspect ratio. Если задан metadata — игнорируется (frame растёт по высоте) */
    aspectRatio?: number;
    /** Метаданные для header/footer. Если null — фрейм без header/footer. */
    metadata?: ExportMetadata;
}
