/**
 * tierError — единый разбор tier-related 403 в catch-блоках индикаторов.
 *
 * Бэкенд при отказе по тарифу шлёт сообщение, содержащее «… на тарифе …» или
 * «… недоступ…». Раньше каждая страница (6 desktop + 6 mobile) повторяла один и
 * тот же if-блок: распарсить msg → определить tier → showUpgrade(). Этот модуль
 * выносит распознавание и вызов showUpgrade в одно место, СОХРАНЯЯ при этом все
 * per-place различия (featureName, выбор tier, side-effects) через опции.
 *
 * ВАЖНО: helper НЕ трогает error-state и НЕ логирует — это остаётся в caller'е.
 * `handleTierError` возвращает boolean (был ли tier-error), чтобы caller сам
 * решал, что делать с non-tier ошибкой (setError / console.error / ничего).
 * `onTier` — опциональный колбэк для side-effect на tier-hit (напр. setError(null)).
 */

export type UpgradeTier = 'basic' | 'pro';

type ShowUpgrade = (props: { tier: UpgradeTier; featureName?: string; indicator?: string }) => void;

function extractMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
}

/** true, если ошибка — отказ по тарифу (а не сетевая/500/прочая). */
export function isTierError(err: unknown): boolean {
    const msg = extractMessage(err);
    return msg.includes('тарифе') || msg.includes('недоступ');
}

/** Дефолтный резолвер: «Pro» в тексте → 'pro', иначе 'basic'. */
function defaultTierResolver(msg: string): UpgradeTier {
    return msg.includes('Pro') ? 'pro' : 'basic';
}

/**
 * Резолвер для Open Interest: 5-минутный таймфрейм И платные активы требуют Pro.
 * Эквивалент прежнему `msg.includes('5мин') || msg.includes('Pro') ? 'pro' : 'basic'`.
 */
export function oiTierResolver(msg: string): UpgradeTier {
    return msg.includes('5мин') || msg.includes('Pro') ? 'pro' : 'basic';
}

interface HandleTierErrorOpts {
    /** showUpgrade из useUpgradePrompt() — helper только вызывает его. */
    showUpgrade: ShowUpgrade;
    /** Контекст-строка индикатора для модалки ('buffett', 'open_interest', …). */
    indicator: string;
    /**
     * Имя фичи в модалке. Строка — статичная; функция — вычисляется из msg
     * (напр. OI: `(msg) => msg.replace(/^.*?: /, '')` достаёт причину из текста).
     */
    featureName: string | ((msg: string) => string);
    /** Нестандартный выбор tier (по умолчанию defaultTierResolver). */
    tierResolver?: (msg: string) => UpgradeTier;
    /** Side-effect на tier-hit (напр. `() => setError(null)`). Опционален. */
    onTier?: () => void;
}

/**
 * Если err — tier-error: вычисляет tier + featureName, зовёт showUpgrade,
 * запускает onTier (если задан) и возвращает true. Иначе возвращает false
 * (caller обрабатывает non-tier ошибку сам). НЕ логирует и НЕ трогает error-state.
 */
export function handleTierError(err: unknown, opts: HandleTierErrorOpts): boolean {
    const msg = extractMessage(err);
    if (!(msg.includes('тарифе') || msg.includes('недоступ'))) return false;
    const tier = (opts.tierResolver ?? defaultTierResolver)(msg);
    const featureName = typeof opts.featureName === 'function' ? opts.featureName(msg) : opts.featureName;
    opts.showUpgrade({ tier, featureName, indicator: opts.indicator });
    opts.onTier?.();
    return true;
}
