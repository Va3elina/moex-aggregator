/**
 * featurePreviews — что показать в окне апселла для конкретного замка.
 *
 * Правило окна (2026-11): вместо «Доступно на тарифе X» показываем саму
 * функцию короткой анимацией, одну фразу о пользе, спокойное сравнение
 * «ваш тариф ✕ → нужный ✓» и кнопку-решение («Открыть данные по юрлицам»).
 *
 * Сцену подбираем сами по (indicator, featureName) — так все места вызова
 * showUpgrade() получают новое окно без правок.
 *
 * Отдельные ветки — ТОЛЬКО для замков, которые реально есть в матрице
 * api/billing/features.py (сверено 2026-11-27):
 *   Pro:   Терминал (роль), экспорт CSV, расширение для терминала (require_pro);
 *   Basic: ОИ — юрлица и число трейдеров; сигналы ОИ в ленте колокола
 *          (free видит скринер с задержкой на день); уведомления в Telegram;
 *          «Деньги в фондах» — выбор фондов; «Сделки фондов» — свежий срез,
 *          выбор фондов, свой период; сезонность — фильтры «Без выбросов»
 *          и «Без дивидендных гэпов».
 * Периоды, таймфреймы, активы, режимы и т.п. сейчас открыты всем — у них
 * своих текстов нет: если такой замок вернут, окно покажет живой график
 * индикатора с чипом функции и нейтральную фразу. Появился новый замок —
 * добавь ветку здесь и строку в шапке.
 */
import type { SceneSpec } from './previewKit';

/** Явный ключ сцены, когда по indicator её не угадать (Терминал — не индикатор). */
export type FeaturePreviewKey = 'terminal';

export interface UpsellArgs {
    featureName?: string;
    indicator?: string;
    preview?: FeaturePreviewKey;
}

export interface ResolvedPreview {
    spec: SceneSpec;
    /** Заголовок окна — что это за инструмент. */
    title: string;
    /** Одна фраза о пользе. */
    text: string;
    /** Кнопка-решение. */
    cta: string;
}

type T = (key: string, opts?: Record<string, unknown>) => string;

const lowerFirst = (s: string) => (s ? s[0].toLowerCase() + s.slice(1) : s);
const upperFirst = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : s);

interface IndicatorLook { name: string; arch: SceneSpec['arch']; seed: number; sample?: string }

function lookOf(indicator: string, t: T): IndicatorLook | null {
    switch (indicator) {
        case 'open_interest': return { name: t('Открытые позиции'), arch: 'line', seed: 3, sample: 'Si' };
        case 'funds_money': return { name: t('Деньги в фондах'), arch: 'bars', seed: 5 };
        case 'fund_trades': return { name: t('Сделки фондов'), arch: 'bars', seed: 7 };
        case 'strength': return { name: t('Сила рынка'), arch: 'line', seed: 9 };
        case 'buffett': return { name: t('Индикатор Баффетта'), arch: 'line', seed: 2 };
        case 'seasonality': return { name: t('Сезонность'), arch: 'bars', seed: 4, sample: 'IMOEX' };
        case 'cbr_flows': return { name: t('Потоки капитала'), arch: 'bars', seed: 6 };
        default: return null;
    }
}

export function resolvePreview(a: UpsellArgs, t: T): ResolvedPreview {
    const f = (a.featureName ?? '').trim();
    const fl = f.toLowerCase();
    const ind = a.indicator ?? '';
    const openFeature = f
        ? t('Открыть {{feature}}', { feature: lowerFirst(f) })
        : t('Открыть функцию');

    /* ── отдельные сцены ── */
    if (a.preview === 'terminal') {
        return {
            spec: { kind: 'terminal' },
            title: t('Терминал'),
            text: t('Располагайте много виджетов на одном экране и используйте дополнительные индикаторы с расширенными настройками.'),
            cta: t('Получить доступ к Терминалу'),
        };
    }
    if (/расширени|extension/.test(fl)) {
        return {
            spec: { kind: 'extension', header: t('Терминал брокера') },
            title: t('Расширение для терминала'),
            text: t('Индикаторы Фрейма в терминале Т-Инвестиций: панель встраивается в раскладку рядом с графиком и стаканом.'),
            cta: t('Подключить расширение'),
        };
    }
    if (/csv/.test(fl)) {
        const look = lookOf(ind, t);
        return {
            spec: { kind: 'csv', header: look ? look.name : t('Данные индикатора') },
            title: t('Экспорт в CSV'),
            text: t('Выгружайте данные индикатора в CSV и считайте их в Excel или Python.'),
            cta: t('Выгружать в CSV'),
        };
    }
    if (ind === 'oi_screener') {
        return {
            spec: { kind: 'screener', header: t('Сигналы по открытым позициям') },
            title: t('Сигналы по открытым позициям'),
            text: t('Сигналы по открытым позициям приходят в реальном времени, а не с задержкой на день.'),
            cta: t('Открыть сигналы в реальном времени'),
        };
    }
    if (ind === 'alerts') {
        return {
            spec: { kind: 'alerts', header: t('Ваш график') },
            title: t('Уведомления'),
            text: t('Сигнал придёт в Telegram, когда график дойдёт до заданного вами уровня.'),
            cta: t('Включить уведомления'),
        };
    }

    /* ── индикатор + клик по закрытой функции ── */
    const look = lookOf(ind, t);
    const spec: SceneSpec = {
        kind: 'chart',
        arch: look?.arch ?? 'line',
        seed: look?.seed ?? 3,
        header: look ? (look.sample ? `${look.name} · ${look.sample}` : look.name) : (f ? upperFirst(f) : ''),
    };
    const title = look?.name ?? (f ? upperFirst(f) : t('Расширенные данные'));
    const withChips = (base: string, target: string, text: string, cta = openFeature): ResolvedPreview => {
        spec.chips = [base, target];
        return { spec, title, text, cta };
    };

    if (ind === 'open_interest' && /юрлиц/.test(fl)) {
        return withChips(t('Физлица'), t('Юрлица'),
            t('Позиции юрлиц показывают, куда ставят крупные участники рынка.'));
    }
    if (ind === 'open_interest' && /трейдер/.test(fl)) {
        return withChips(t('Позиции'), t('Число трейдеров'),
            t('Число трейдеров показывает, сколько участников стоит за позициями.'));
    }
    // Фонды — свои сцены по настоящему интерфейсу (FundsScenes.tsx), без чипов.
    if (ind === 'funds_money' && /выбор фондов/.test(fl)) {
        return {
            spec: { kind: 'fundpick', header: t('Деньги в фондах') },
            title,
            text: t('Соберите свой набор фондов внутри категории — график покажет деньги только в них.'),
            cta: openFeature,
        };
    }
    if (ind === 'fund_trades') {
        const fresh = /свеж/.test(fl), picker = /выбор фондов/.test(fl), range = /свой период/.test(fl);
        return {
            spec: {
                kind: 'fundtrades',
                header: t('Сделки фондов · Общий портфель'),
                variant: fresh ? 'fresh' : picker ? 'picker' : range ? 'range' : undefined,
            },
            title,
            text: fresh ? t('Свежий срез показывает сделки фондов за последний раскрытый месяц, без задержки.')
                : picker ? t('Соберите свой набор фондов и смотрите их общий портфель и сделки по бумагам.')
                : range ? t('Сравните сделки фондов за любой диапазон месяцев, а не только за 1М, 6М или 1Г.')
                : t('Больше данных и настроек этого индикатора.'),
            cta: openFeature,
        };
    }
    if (ind === 'seasonality' && /выброс/.test(fl)) {
        return withChips(t('Среднее'), t('Без выбросов'),
            t('Расчёт по медиане вместо среднего: единичные аномальные годы не искажают сезонность.'));
    }
    if (ind === 'seasonality' && /дивиденд/.test(fl)) {
        return withChips(t('Все дни'), t('Без дивидендных гэпов'),
            t('Дивидендные гэпы убираются из расчёта и не искажают сезонность.'));
    }

    // Замок без своей ветки (сейчас таких в матрице нет): живой график + чип функции.
    if (f) spec.chips = [f.length > 24 ? upperFirst(f.slice(0, 23)) + '…' : upperFirst(f)];
    return { spec, title, text: t('Больше данных и настроек этого индикатора.'), cta: openFeature };
}
