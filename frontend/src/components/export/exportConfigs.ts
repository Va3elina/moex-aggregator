/**
 * exportConfigs — конфиги CSV/XLSX-экспорта индикаторов, общие для ПК и мобилки.
 *
 * До 02.09.2026 каждая десктопная страница держала литерал конфига прямо в
 * JSX кнопки CsvExportButton. С выводом экспорта на мобилку (строка в шите
 * «Опции» + MobileCsvExportSheet) литералы вынесены сюда, чтобы слои,
 * селекторы и URL не разъезжались между двумя версиями страницы. Страница
 * передаёт ТОЛЬКО текущее состояние UI (дефолты селекторов) — всё остальное
 * живёт здесь.
 *
 * Значения периодов — ключи DEFAULT_DAYS_MAP в utils/csvPeriod.ts.
 */
import type { CsvExportConfig, CsvSelectOption } from './CsvExportModal';
import { periodToQuery } from '../../utils/csvPeriod';
import { getCategoryShortLabel } from '../cbr/cbrCategoryInfo';

// ────────────────────────────────────────────────────────────────────
// Индикатор Баффета
// ────────────────────────────────────────────────────────────────────
export function buildBuffettExportConfig(ui: {
    viewMode: string;
    period: string;
    timeframe: string;
}): CsvExportConfig {
    const { viewMode, period, timeframe } = ui;
    return {
        indicator: 'buffett',
        title: 'Экспорт: Индикатор Баффета',
        layers: [{
            id: 'main',
            label: 'Данные индикатора',
            description: 'date, market_cap, knex (gdp_ttm или m2), ratio',
            defaultSelected: true,
        }],
        // Unified порядок: режимы → период → таймфрейм.
        selectors: [
            {
                kind: 'multiselect',
                id: 'modes',
                label: 'Режим расчёта',
                default: [viewMode],
                hint: 'Несколько → ZIP с CSV per режим',
                options: [
                    { value: 'cap-gdp', label: 'Кап / ВВП' },
                    { value: 'cap-m2', label: 'Кап / M2' },
                ],
            },
            {
                kind: 'period',
                id: 'period',
                label: 'Период',
                default: { type: 'preset', value: period },
                presets: [
                    { value: '1y', label: '1Г', days: 365 },
                    { value: '5y', label: '5Л', days: 1825 },
                    { value: '10y', label: '10Л', days: 3650 },
                    { value: '20y', label: '20Л', days: 7300 },
                    { value: 'all', label: 'Всё', days: 15000 },
                ],
            },
            {
                kind: 'select',
                id: 'timeframe',
                label: 'Таймфрейм',
                default: timeframe,
                options: [
                    { value: '1d', label: 'Дневной' },
                    { value: '1w', label: 'Недельный' },
                    { value: '1m', label: 'Месячный' },
                ],
            },
        ],
        buildUrl: (_layers, vals) => {
            const modes = (vals.modes as string[] ?? [viewMode]).join(',');
            const tf = (vals.timeframe as string) ?? timeframe;
            const periodParam = periodToQuery(vals.period, 3650);
            return `/api/export/buffett.csv?mode=${modes}&timeframe=${tf}&${periodParam}`;
        },
        buildFilename: (_layers, vals) => {
            const modes = (vals.modes as string[] ?? [viewMode]);
            const tf = (vals.timeframe as string) ?? timeframe;
            return modes.length === 1
                ? `buffett_${modes[0]}_${tf}.csv`
                : `buffett_${Date.now()}.zip`;
        },
    };
}

// ────────────────────────────────────────────────────────────────────
// Поток капитала (ОРФР ЦБ)
// ────────────────────────────────────────────────────────────────────

/** Категории участников в выгрузке — подписи через getCategoryShortLabel
 *  (единый источник правды с легендой графика, чтобы не разъезжались). */
const CBR_EXPORT_CATEGORIES = [
    'Физические лица',
    'СЗКО',
    'Прочие Банки',
    'Нерезиденты',
    'НФО',
    'Нефинансовые организации',
    'Доверительное управление',
    'Банк России',
    'Российские кредитные организации',
    'Клиенты российских кредитных организаций',
];

export function buildCbrFlowsExportConfig(ui: {
    type: string;
    period: string;
    /** Табы типов инструмента страницы (ключ + подпись) → селектор «Тип инструмента». */
    instrumentOptions: CsvSelectOption[];
}): CsvExportConfig {
    const { type, period, instrumentOptions } = ui;
    return {
        indicator: 'cbr_flows',
        title: 'Экспорт: Поток капитала',
        layers: [{
            id: 'flows',
            label: 'Потоки ОРФР',
            description: 'period_year, label, category, value (млрд ₽)',
            defaultSelected: true,
        }],
        // Unified порядок: тип инструмента (актив-эквивалент) →
        // категории (mode-эквивалент) → период.
        selectors: [
            {
                kind: 'multiselect',
                id: 'instruments',
                label: 'Тип инструмента',
                default: [type],
                hint: 'Несколько → ZIP с CSV per тип',
                options: instrumentOptions,
            },
            {
                kind: 'multiselect',
                id: 'categories',
                label: 'Категории участников',
                default: [], // пустой = все категории (backend не фильтрует)
                hint: 'Пусто = все категории',
                options: CBR_EXPORT_CATEGORIES.map((value) => ({ value, label: getCategoryShortLabel(value) })),
            },
            {
                kind: 'period',
                id: 'period',
                label: 'Период',
                default: { type: 'preset', value: period },
                presets: [
                    { value: '1y', label: '1Г', days: 365 },
                    { value: '3y', label: '3Г', days: 1095 },
                    { value: '5y', label: '5Л', days: 1825 },
                    { value: 'all', label: 'Всё', days: 11000 },
                ],
            },
        ],
        buildUrl: (_layers, vals) => {
            const insts = (vals.instruments as string[] ?? [type]).join(',');
            const cats = (vals.categories as string[] ?? []);
            const catsParam = cats.length > 0 ? `&categories=${encodeURIComponent(cats.join(','))}` : '';
            // ORFR хранит данные по годам, так что period конвертируется в `years`.
            let yearsParam: string;
            const pv = vals.period;
            if (pv && typeof pv === 'object' && (pv as { type?: string }).type === 'range') {
                // Range — берём years = diff в годах.
                const r = pv as { type: 'range'; from: string; to: string };
                const fromY = parseInt(r.from.slice(0, 4), 10);
                const toY = parseInt(r.to.slice(0, 4), 10);
                yearsParam = `&years=${Math.max(1, toY - fromY + 1)}`;
            } else {
                // Preset → days → years.
                const days = parseInt(periodToQuery(pv, 365).replace('days=', ''), 10);
                yearsParam = `&years=${Math.max(1, Math.round(days / 365))}`;
            }
            return `/api/export/cbr-flows.csv?instrument=${insts}${yearsParam}${catsParam}`;
        },
        buildFilename: () => `cbr_flows_${Date.now()}.zip`,
    };
}

// ────────────────────────────────────────────────────────────────────
// Открытые позиции
// ────────────────────────────────────────────────────────────────────
export function buildOiExportConfig(ui: {
    instrument: string;
    /** Имя актива для заголовка; пустое (актив из URL ещё не разрезолвлен) → тикер. */
    instrumentName: string;
    clgroup: string;
    interval: number;
    period: string;
}): CsvExportConfig {
    const { instrument, instrumentName, clgroup, interval, period } = ui;
    const periodDays: Record<string, number> = {
        '1d': 2, '1w': 7, '1m': 30, '3m': 90, '6m': 180,
        '1y': 365, '2y': 730, '5y': 1825, 'all': 7000,
    };
    return {
        indicator: 'open_interest',
        title: `Экспорт: Открытые позиции · ${instrumentName || instrument}`,
        layers: [{
            id: 'oi',
            label: 'История позиций',
            description: 'trade_date, trade_time, open_interest, pos_long/short, число участников',
            defaultSelected: true,
        }],
        selectors: [
            {
                kind: 'instrument-picker',
                id: 'instruments',
                label: 'Инструменты (фьючерсы)',
                default: [instrument],
                filterType: 'futures',
                pickerTitle: 'Выберите фьючерсы для экспорта',
                hint: 'Несколько → ZIP с CSV per инструмент',
            },
            {
                kind: 'multiselect',
                id: 'clgroups',
                label: 'Категория участников',
                default: [clgroup],
                hint: 'Оба → 2 CSV в ZIP',
                options: [
                    { value: 'YUR', label: 'Юрлица' },
                    { value: 'FIZ', label: 'Физлица' },
                ],
            },
            {
                kind: 'multiselect',
                id: 'intervals',
                label: 'Таймфрейм',
                default: [String(interval)],
                hint: 'Несколько → ZIP с CSV per таймфрейм',
                options: [
                    { value: '5', label: '5 мин' },
                    { value: '60', label: '1 час' },
                    { value: '24', label: '1 день' },
                ],
            },
            {
                kind: 'period',
                id: 'period',
                label: 'Период',
                default: { type: 'preset', value: period },
                presets: [
                    { value: '1m', label: '1М', days: 30 },
                    { value: '1y', label: '1Г', days: 365 },
                    { value: 'all', label: 'Всё', days: 7000 },
                ],
            },
        ],
        buildUrl: (_layers, vals) => {
            const insts = (vals.instruments as string[] ?? [instrument]).join(',');
            const cls = (vals.clgroups as string[] ?? [clgroup]).join(',');
            const ints = (vals.intervals as string[] ?? [String(interval)]).join(',');
            const periodParam = periodToQuery(vals.period, periodDays[period] ?? 365);
            return `/api/export/oi.csv?instrument=${encodeURIComponent(insts)}&clgroup=${cls}&interval=${ints}&${periodParam}`;
        },
        buildFilename: () => `oi_${Date.now()}.zip`,
    };
}

// ────────────────────────────────────────────────────────────────────
// Деньги в фондах
// ────────────────────────────────────────────────────────────────────
export function buildFundsMoneyExportConfig(ui: {
    category: string;
    period: string;
    /** Фонды, видимые сейчас на графике (доступные и не скрытые) — дефолт,
     *  если юзер не выбрал фонды пикером. */
    visibleTickers: string[];
    categoryOptions: CsvSelectOption[];
}): CsvExportConfig {
    const { category, period, visibleTickers, categoryOptions } = ui;
    const periodDays: Record<string, number> = {
        '1m': 30, '3m': 90, '6m': 180, '1y': 365,
        '2y': 730, '3y': 1095, 'all': 7000,
    };
    return {
        indicator: 'funds_money',
        title: 'Экспорт: Деньги в фондах',
        layers: [{
            id: 'nav',
            label: 'История СЧА фондов',
            description: 'Daily NAV per fund по выбранной категории',
            defaultSelected: true,
        }],
        // Unified порядок: фонды (актив) → категории (mode) → период.
        selectors: [
            {
                kind: 'instrument-picker',
                id: 'funds',
                label: 'Фонды (опционально)',
                default: [],
                source: 'funds',
                pickerTitle: 'Выберите фонды',
                hint: 'Пусто = все фонды выбранных категорий. Иначе — только эти.',
            },
            {
                kind: 'multiselect',
                id: 'categories',
                label: 'Категории',
                default: [category],
                hint: 'Несколько → ZIP с CSV per категория',
                options: categoryOptions,
            },
            {
                kind: 'period',
                id: 'period',
                label: 'Период',
                default: { type: 'preset', value: period },
                presets: [
                    { value: '1m', label: '1М', days: 30 },
                    { value: '1y', label: '1Г', days: 365 },
                    { value: '3y', label: '3Г', days: 1095 },
                    { value: 'all', label: 'Всё', days: 7000 },
                ],
            },
        ],
        buildUrl: (_layers, vals) => {
            const cats = (vals.categories as string[] ?? [category]).join(',');
            const periodParam = periodToQuery(vals.period, periodDays[period] ?? 365);
            // Picker override — если юзер выбрал → этими фондами; иначе видимые на графике.
            const pickedFunds = (vals.funds as string[] ?? []);
            const effectiveFundsList = pickedFunds.length > 0 ? pickedFunds : visibleTickers;
            const fundsParam = effectiveFundsList.length > 0
                ? `&funds=${encodeURIComponent(effectiveFundsList.join(','))}`
                : '';
            return `/api/export/funds-money.csv?category=${cats}&${periodParam}${fundsParam}`;
        },
        buildFilename: () => `funds_${Date.now()}.zip`,
    };
}

// ────────────────────────────────────────────────────────────────────
// Сила рынка
// ────────────────────────────────────────────────────────────────────
export function buildStrengthExportConfig(ui: {
    emaPeriod: number;
    universe: string;
    period: string;
    /** Дней в текущем периоде UI — fallback, если preset не распознан. */
    periodDays: number;
}): CsvExportConfig {
    const { emaPeriod, universe, period, periodDays } = ui;
    return {
        indicator: 'strength',
        title: 'Экспорт: Сила рынка',
        layers: [
            { id: 'history', label: 'История Силы рынка',
              description: 'Timeseries % акций выше EMA',
              defaultSelected: true },
            { id: 'stocks', label: 'Снапшот акций',
              description: 'Текущие цены + изменения по всем акциям' },
        ],
        selectors: [
            {
                kind: 'multiselect',
                id: 'emas',
                label: 'EMA периоды',
                default: [String(emaPeriod)],
                hint: 'Несколько EMA → ZIP с CSV per EMA',
                options: [
                    { value: '20', label: 'EMA 20' },
                    { value: '50', label: 'EMA 50' },
                    { value: '100', label: 'EMA 100' },
                    { value: '200', label: 'EMA 200' },
                ],
            },
            {
                kind: 'multiselect',
                id: 'universes',
                label: 'Вселенные',
                default: [universe],
                hint: 'Несколько → ZIP с CSV per universe',
                options: [
                    { value: 'imoex', label: 'IMOEX' },
                    { value: 'all', label: 'Все акции' },
                    { value: 'imoex_usd', label: 'IMOEX (USD)' },
                    { value: 'all_usd', label: 'Все (USD)' },
                ],
            },
            {
                kind: 'period',
                id: 'period',
                label: 'Период',
                default: { type: 'preset', value: period },
                presets: [
                    { value: '1y', label: '1Г', days: 365 },
                    { value: '5y', label: '5Л', days: 1825 },
                    { value: '10y', label: '10Л', days: 3650 },
                    { value: '20y', label: '20Л', days: 7300 },
                    { value: 'all', label: 'Всё', days: 7000 },
                ],
            },
        ],
        buildUrl: (layers, vals) => {
            const emas = (vals.emas as string[] ?? [String(emaPeriod)]).join(',');
            const universes = (vals.universes as string[] ?? [universe]).join(',');
            const periodParam = periodToQuery(vals.period, periodDays);
            return `/api/export/breadth.csv?ema=${emas}&universe=${universes}&${periodParam}&layers=${layers.join(',')}`;
        },
        buildFilename: () => `strength_${Date.now()}.zip`,
    };
}

// ────────────────────────────────────────────────────────────────────
// Сезонность
// ────────────────────────────────────────────────────────────────────
export function buildSeasonalityExportConfig(ui: {
    ticker: string;
    name: string;
}): CsvExportConfig {
    const { ticker, name } = ui;
    return {
        indicator: 'seasonality',
        title: `Экспорт: Сезонность · ${name}`,
        layers: [
            { id: 'daily', label: 'Дневные свечи',
              description: 'OHLCV + change_pct + декомпозиция (year/month/weekday)',
              defaultSelected: true },
            { id: 'weekday_avg', label: 'Средняя по дню недели',
              description: 'Avg change_pct по Пн-Вс + stdev + размер выборки' },
            { id: 'monthly_avg', label: 'Средняя по месяцам',
              description: 'Avg change_pct по Янв-Дек (классическая сезонность)' },
            { id: 'monthday_avg', label: 'Средняя по дню месяца',
              description: 'Avg change_pct по 1-31 числу — turn-of-month' },
        ],
        selectors: [
            {
                kind: 'instrument-picker',
                id: 'tickers',
                label: 'Тикеры (можно несколько)',
                default: [ticker],
                filterType: 'stock',
                pickerTitle: 'Выберите акции для экспорта',
                hint: 'Несколько → ZIP с отдельным CSV per ticker × layer',
            },
        ],
        buildUrl: (layers, vals) => {
            const tickers = (vals.tickers as string[]) ?? [ticker];
            return `/api/export/seasonality.csv?ticker=${encodeURIComponent(tickers.join(','))}&layers=${layers.join(',')}`;
        },
        buildFilename: (layers, vals) => {
            const tickers = (vals.tickers as string[]) ?? [ticker];
            if (tickers.length === 1 && layers.length === 1) {
                return `seasonality_${tickers[0]}_${layers[0]}.csv`;
            }
            return `seasonality_${Date.now()}.zip`;
        },
    };
}
