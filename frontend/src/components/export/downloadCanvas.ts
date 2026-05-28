/**
 * downloadCanvas — конвертирует canvas в PNG/JPEG blob и инициирует download
 * через скрытый <a download> trick.
 *
 * Lifecycle:
 *   1. canvas.toBlob() — async, дешевле чем toDataURL для больших canvas
 *   2. URL.createObjectURL(blob) — создаёт временный URL
 *   3. <a download href={url}> + click() — браузер инициирует save dialog
 *   4. URL.revokeObjectURL — cleanup, иначе memory leak
 *
 * Поддерживает PNG (lossless, ~500KB) и JPEG (smaller ~200KB, для Telegram).
 */

export type ExportFormat = 'png' | 'jpeg';

export async function downloadCanvas(
    canvas: HTMLCanvasElement,
    filename: string,
    format: ExportFormat = 'png',
    quality: number = 0.92,
): Promise<void> {
    const mime = format === 'jpeg' ? 'image/jpeg' : 'image/png';
    const ext = format === 'jpeg' ? 'jpg' : 'png';

    // toBlob is async — не блокирует main thread на больших canvas (3-5MB).
    const blob: Blob | null = await new Promise((resolve) => {
        canvas.toBlob(
            (b) => resolve(b),
            mime,
            format === 'jpeg' ? quality : undefined,
        );
    });

    if (!blob) {
        throw new Error('Failed to create blob from canvas');
    }

    const url = URL.createObjectURL(blob);

    try {
        const a = document.createElement('a');
        a.href = url;
        a.download = `${filename}.${ext}`;
        // Append to body — workaround для Firefox чтобы click() работал.
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    } finally {
        // Cleanup URL после короткого delay (браузер должен успеть начать save).
        // Без revoke — memory leak растёт с каждым downloads.
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
}

/**
 * copyCanvasToClipboard — копирует canvas как PNG в системный буфер обмена,
 * чтобы юзер мог сразу вставить картинку (Ctrl/Cmd+V) в Telegram, чат и т.д.
 *
 * Требования: HTTPS + клик (user gesture). Бросает Error при неподдержке
 * (старый браузер / Firefox без image-write) или отказе — вызывающий
 * показывает fallback (кнопка «Скачать» рядом всегда доступна).
 *
 * ВАЖНО: clipboard.write вызывается синхронно в рамках жеста, а blob
 * передаётся как Promise внутрь ClipboardItem — иначе Safari считает
 * user-gesture протухшим после await toBlob(). Chrome принимает оба варианта.
 */
export async function copyCanvasToClipboard(canvas: HTMLCanvasElement): Promise<void> {
    if (typeof ClipboardItem === 'undefined' || !navigator.clipboard?.write) {
        throw new Error('Браузер не поддерживает копирование изображений');
    }

    const blobPromise = new Promise<Blob>((resolve, reject) => {
        canvas.toBlob(
            (b) => (b ? resolve(b) : reject(new Error('Не удалось создать изображение'))),
            'image/png',
        );
    });

    await navigator.clipboard.write([new ClipboardItem({ 'image/png': blobPromise })]);
}
