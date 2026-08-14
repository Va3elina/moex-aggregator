/**
 * ApiDocsPage — интерактивная документация по REST API Фрейма.
 *
 * UX по образцу Stripe/Postman:
 *   - Sticky sidebar с TOC и подсветкой активной секции (IntersectionObserver).
 *   - API-key bar сверху — введи один раз, используется во всех примерах.
 *   - Две колонки на endpoint'ах: слева docs+params, справа code+try-it.
 *   - Language tabs (curl/Python/JS) — переключение синхронизировано
 *     на всю страницу.
 *   - "Попробовать" — реальный fetch с пользовательским ключом, ответ
 *     рендерится с status-badge и временем выполнения.
 *
 * Доступ к API — Pro tier (создание ключей через /profile).
 * Сама страница публичная — гость может читать документацию,
 * но без ключа ничего не получится протестировать.
 */
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
    Key,
    ArrowLeft,
    Copy,
    Check,
    Play,
    AlertCircle,
    Eye,
    EyeOff,
    Trash2,
    Lock,
    Sparkles,
} from 'lucide-react';
import { useCommonFeatures } from '../contexts/TierFeaturesContext';
import { useAuth } from '../contexts/AuthContext';

// Базовый URL для примеров — берём origin страницы, так юзер
// автоматически видит https://таймфрейм.рф/api/v1/... вместо абстрактного <BASE>.
const BASE_URL =
    typeof window !== 'undefined'
        ? window.location.origin
        : 'https://framedata.ru';

const API_KEY_STORAGE_KEY = 'frame_api_docs_key';

// ════════════════════════════════════════════════════════════════════
// Типы и константы
// ════════════════════════════════════════════════════════════════════

type Lang = 'curl' | 'python' | 'javascript';

interface ParamDef {
    name: string;
    type: 'string' | 'int';
    required: boolean;
    description: string;
    /** Дефолтное значение в форме "Попробовать". Пусто = не подставлять. */
    defaultValue?: string;
    /** Если задан — рендерится <select> вместо <input>. */
    enum?: string[];
}

interface EndpointDef {
    id: string;
    method: 'GET';
    path: string;
    title: string;
    description: string;
    params: ParamDef[];
    exampleResponse: string;
}

// Группы endpoint'ов для навигации — отображаются как секции в sidebar.
interface EndpointGroup {
    title: string;
    endpoints: EndpointDef[];
}

const ENDPOINT_GROUPS: EndpointGroup[] = [
    {
        title: 'Снимки рынка',
        endpoints: [
            {
                id: 'heatmap',
                method: 'GET',
                path: '/api/v1/public/heatmap',
                title: 'Карта рынка',
                description:
                    'Снапшот всех акций: цена, изменение за разные периоды (день/неделя/месяц/год), оборот и капитализация. Источник — материализованное представление, обновляется ежедневно.',
                params: [],
                exampleResponse: `{
  "data": [
    {
      "sec_id": "SBER",
      "name": "Сбербанк",
      "sector": "Финансы",
      "price": 318.45,
      "prev_close": 315.20,
      "change_1d": 1.03,
      "change_1w": 2.15,
      "change_1m": -0.85,
      "change_1y": 22.4,
      "volume_1d": 28453210,
      "value_1d": 9056782345,
      "market_cap": 6850000000000
    }
  ],
  "meta": {
    "count": 235,
    "source": "MOEX (via Frame)",
    "as_of": "2026-05-24T08:15:32.451Z"
  }
}`,
            },
            {
                id: 'stock-quote',
                method: 'GET',
                path: '/api/v1/public/stocks/{ticker}/quote',
                title: 'Котировка одной акции',
                description:
                    'Текущая цена + изменение за периоды по одному тикеру. Подмножество `/heatmap` отфильтрованное на сервере — экономит трафик для ботов которые тащат цену по одной бумаге.',
                params: [
                    {
                        name: 'ticker',
                        type: 'string',
                        required: true,
                        description: 'Тикер акции (path-параметр), например `SBER`.',
                        defaultValue: 'SBER',
                    },
                ],
                exampleResponse: `{
  "data": {
    "sec_id": "SBER",
    "name": "Сбербанк",
    "sector": "Финансы",
    "price": 318.45,
    "prev_close": 315.20,
    "change_1d": 1.03,
    "change_1w": 2.15,
    "change_1m": -0.85,
    "change_1y": 22.4,
    "volume_1d": 28453210,
    "value_1d": 9056782345,
    "market_cap": 6850000000000
  },
  "meta": {
    "ticker": "SBER",
    "source": "MOEX (via Frame)",
    "as_of": "2026-05-24T08:15:32.451Z"
  }
}`,
            },
            {
                id: 'instruments',
                method: 'GET',
                path: '/api/v1/public/instruments',
                title: 'Список инструментов',
                description:
                    'Все инструменты доступные на Фрейме — акции и фьючерсы с тикерами, sectype и торговыми группами MOEX.',
                params: [
                    {
                        name: 'type',
                        type: 'string',
                        required: false,
                        description: 'Фильтр: `stock` (акции) или `futures` (фьючерсы). По умолчанию — все.',
                        enum: ['stock', 'futures'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"sec_id": "SBER", "sectype": "SBER", "name": "Сбербанк", "type": "stock", "group": "TQBR"},
    {"sec_id": "GAZP", "sectype": "GAZP", "name": "Газпром", "type": "stock", "group": "TQBR"},
    {"sec_id": "SR-6.26", "sectype": "SR", "name": "Сбербанк фьючерс", "type": "futures", "group": "RTSI"}
  ],
  "count": 287
}`,
            },
        ],
    },
    {
        title: 'Сила рынка',
        endpoints: [
            {
                id: 'breadth-current',
                method: 'GET',
                path: '/api/v1/public/breadth/current',
                title: 'Сила рынка — текущий снимок',
                description:
                    'Процент акций торгующихся выше выбранной EMA. Классический индикатор широты рынка — выше 70% = перегрев, ниже 30% = перепроданность.',
                params: [
                    {
                        name: 'ema',
                        type: 'int',
                        required: false,
                        description: 'Период EMA (от 10 до 500). По умолчанию 200.',
                        defaultValue: '200',
                    },
                    {
                        name: 'universe',
                        type: 'string',
                        required: false,
                        description: 'Вселенная акций: `imoex` (50 топ), `all` (все), `imoex_usd` или `all_usd` (в долларах).',
                        defaultValue: 'imoex',
                        enum: ['imoex', 'all', 'imoex_usd', 'all_usd'],
                    },
                ],
                exampleResponse: `{
  "data": {
    "trade_date": "2026-05-23",
    "percent_above": 54.2,
    "count_above": 26,
    "count_total": 48
  },
  "ema_period": 200,
  "universe": "imoex"
}`,
            },
            {
                id: 'breadth-history',
                method: 'GET',
                path: '/api/v1/public/breadth/history',
                title: 'Сила рынка — история',
                description:
                    'Дневной timeseries Силы рынка за указанный период. Каждая точка — % акций выше EMA на эту дату.',
                params: [
                    {
                        name: 'ema',
                        type: 'int',
                        required: false,
                        description: 'Период EMA (10–500). По умолчанию 200.',
                        defaultValue: '200',
                    },
                    {
                        name: 'universe',
                        type: 'string',
                        required: false,
                        description: 'Вселенная: `imoex`, `all`, `imoex_usd`, `all_usd`.',
                        defaultValue: 'imoex',
                        enum: ['imoex', 'all', 'imoex_usd', 'all_usd'],
                    },
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (1–15000). По умолчанию 365.',
                        defaultValue: '365',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv` — для удобного импорта в pandas / Excel.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2025-05-24", "percent_above": 48.2, "count_above": 23, "count_total": 48},
    {"trade_date": "2025-05-25", "percent_above": 50.0, "count_above": 24, "count_total": 48}
  ],
  "ema_period": 200,
  "universe": "imoex",
  "count": 365
}`,
            },
        ],
    },
    {
        title: 'Открытые позиции',
        endpoints: [
            {
                id: 'oi-current',
                method: 'GET',
                path: '/api/v1/public/oi/current',
                title: 'ОИ — последняя точка',
                description:
                    'Текущие открытые позиции по фьючерсу с разбивкой long/short по клиентским группам — физические или юридические лица.',
                params: [
                    {
                        name: 'instrument',
                        type: 'string',
                        required: true,
                        description: 'Sectype фьючерса (например `SR` для Сбербанка, `BR` для Brent).',
                        defaultValue: 'SR',
                    },
                    {
                        name: 'clgroup',
                        type: 'string',
                        required: false,
                        description: '`YUR` (юрлица) или `FIZ` (физлица). По умолчанию `YUR`.',
                        defaultValue: 'YUR',
                        enum: ['YUR', 'FIZ'],
                    },
                ],
                exampleResponse: `{
  "data": {
    "tradedate": "2026-05-23",
    "tradetime": "23:59:00",
    "open_interest": 184523,
    "pos_long": 89234,
    "pos_short": 95289,
    "pos_long_num": 4521,
    "pos_short_num": 4123
  },
  "instrument": "SR",
  "clgroup": "YUR"
}`,
            },
            {
                id: 'oi-history',
                method: 'GET',
                path: '/api/v1/public/oi/history',
                title: 'ОИ — история',
                description:
                    'Дневная история ОИ по фьючерсу за указанный период. Только daily (interval=24) — intraday данные слишком тяжёлые для programmatic access.',
                params: [
                    {
                        name: 'instrument',
                        type: 'string',
                        required: true,
                        description: 'Sectype фьючерса (`SR`, `GZ`, `MX`, `BR`, ...).',
                        defaultValue: 'SR',
                    },
                    {
                        name: 'clgroup',
                        type: 'string',
                        required: false,
                        description: '`YUR` или `FIZ`. По умолчанию `YUR`.',
                        defaultValue: 'YUR',
                        enum: ['YUR', 'FIZ'],
                    },
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (1–3650). По умолчанию 365.',
                        defaultValue: '365',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2025-05-24", "trade_time": "23:59:00", "open_interest": 175820, "pos_long": 84210, "pos_short": 91610, "pos_long_num": 4321, "pos_short_num": 3987},
    {"trade_date": "2025-05-25", "trade_time": "23:59:00", "open_interest": 178240, "pos_long": 85432, "pos_short": 92808, "pos_long_num": 4380, "pos_short_num": 4011}
  ],
  "instrument": "SR",
  "clgroup": "YUR",
  "count": 250
}`,
            },
        ],
    },
    {
        title: 'Свечи и сезонность',
        endpoints: [
            {
                id: 'seasonality',
                method: 'GET',
                path: '/api/v1/public/seasonality',
                title: 'Daily свечи акций',
                description:
                    'Daily-свечи для тикера за указанный период. Полезно для самостоятельного расчёта сезонности, бэктестов и аналитики.',
                params: [
                    {
                        name: 'ticker',
                        type: 'string',
                        required: true,
                        description: 'Тикер акции (например `SBER`, `GAZP`, `LKOH`).',
                        defaultValue: 'SBER',
                    },
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (30–15000). По умолчанию 3650 (10 лет).',
                        defaultValue: '3650',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2016-05-24", "open": 145.5, "high": 147.2, "low": 144.8, "close": 146.1, "volume": 12345678},
    {"trade_date": "2016-05-25", "open": 146.1, "high": 148.5, "low": 145.9, "close": 148.0, "volume": 14523890}
  ],
  "ticker": "SBER",
  "count": 2503
}`,
            },
        ],
    },
    {
        title: 'Индикатор Баффетта',
        endpoints: [
            {
                id: 'buffett-cap-gdp',
                method: 'GET',
                path: '/api/v1/public/buffett/cap-gdp',
                title: 'Кап / ВВП',
                description:
                    'Рыночная капитализация MOEX / ВВП TTM × 100. Классический Buffett-индикатор. Cap — данные MOEX, GDP TTM — сумма последних 4 кварталов Росстата с forward-fill.',
                params: [
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (30–15000). По умолчанию 3650 (10 лет).',
                        defaultValue: '3650',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2020-05-24", "market_cap": 35200, "gdp_ttm": 109800, "buffett_ratio_pct": 32.0586},
    {"trade_date": "2020-05-25", "market_cap": 35450, "gdp_ttm": 109800, "buffett_ratio_pct": 32.2861}
  ],
  "meta": {
    "indicator": "cap-gdp",
    "unit": "ratio_pct",
    "source_cap": "MOEX",
    "source_gdp": "Росстат (TTM forward-fill)",
    "count": 2503
  }
}`,
            },
            {
                id: 'buffett-cap-m2',
                method: 'GET',
                path: '/api/v1/public/buffett/cap-m2',
                title: 'Кап / M2',
                description:
                    'Отношение рыночной капитализации к денежной массе M2 (ЦБ РФ, monthly forward-fill). Альтернативная версия Buffett-индикатора для рынков с быстро растущей денежной массой.',
                params: [
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (30–15000). По умолчанию 3650.',
                        defaultValue: '3650',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2020-05-24", "market_cap": 35200, "m2": 53800, "cap_m2_ratio": 0.654275},
    {"trade_date": "2020-05-25", "market_cap": 35450, "m2": 53800, "cap_m2_ratio": 0.658921}
  ],
  "meta": {
    "indicator": "cap-m2",
    "unit": "ratio",
    "source_cap": "MOEX",
    "source_m2": "ЦБ РФ (monthly forward-fill)",
    "count": 2503
  }
}`,
            },
        ],
    },
    {
        title: 'Фонды (БПИФ)',
        endpoints: [
            {
                id: 'funds-categories',
                method: 'GET',
                path: '/api/v1/public/funds/categories',
                title: 'Все БПИФы — каталог',
                description:
                    'Список всех БПИФов которые отслеживает Фрейм, с категорией (денежный рынок, акции, облигации, золото) и подкатегорией.',
                params: [],
                exampleResponse: `{
  "data": [
    {"ticker": "LQDT", "name": "Ликвидность", "category": "money_market", "subcategory": null},
    {"ticker": "SBMM", "name": "Сберегательный", "category": "money_market", "subcategory": null},
    {"ticker": "TMOS", "name": "Тинькофф iMOEX", "category": "stocks", "subcategory": null}
  ],
  "count": 42
}`,
            },
            {
                id: 'fund-detail',
                method: 'GET',
                path: '/api/v1/public/funds/{ticker}',
                title: 'Один фонд — детали',
                description:
                    'Метаданные фонда + текущая СЧА и цена пая + доходность за 1м/3м/6м/1г + топ-5 holdings. Если ticker не найден — 404.',
                params: [
                    {
                        name: 'ticker',
                        type: 'string',
                        required: true,
                        description: 'Тикер фонда (path-параметр), например `LQDT`, `SBMM`, `TMOS`.',
                        defaultValue: 'LQDT',
                    },
                ],
                exampleResponse: `{
  "data": {
    "fund_id": 12,
    "ticker": "LQDT",
    "name": "Ликвидность",
    "category": "money_market",
    "subcategory": null,
    "uk_id": "VIM",
    "last_nav": 285340000000.0,
    "last_pay": 1.7821,
    "last_date": "2026-05-23",
    "return_1m": 1.21,
    "return_3m": 3.65,
    "return_6m": 7.42,
    "return_1y": 14.85,
    "top_holdings": [
      {"name": "Депозиты на RUSFAR", "weight": 99.5},
      {"name": "Денежные средства", "weight": 0.5}
    ]
  }
}`,
            },
            {
                id: 'fund-history',
                method: 'GET',
                path: '/api/v1/public/funds/{ticker}/history',
                title: 'Один фонд — daily NAV/pay',
                description:
                    'Дневная история СЧА (nav) и цены пая (pay). На основе этого можно посчитать доходность, sharpe, drawdown, любые кастомные метрики и притоки/оттоки (через изменение СЧА с учётом цены пая).',
                params: [
                    {
                        name: 'ticker',
                        type: 'string',
                        required: true,
                        description: 'Тикер фонда (path-параметр).',
                        defaultValue: 'LQDT',
                    },
                    {
                        name: 'days',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в днях (1–3650). По умолчанию 365.',
                        defaultValue: '365',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"trade_date": "2025-05-24", "nav": 248530000000.0, "pay": 1.5510},
    {"trade_date": "2025-05-25", "nav": 249120000000.0, "pay": 1.5528}
  ],
  "ticker": "LQDT",
  "count": 250
}`,
            },
        ],
    },
    {
        title: 'Поток капитала',
        endpoints: [
            {
                id: 'cbr-flows',
                method: 'GET',
                path: '/api/v1/public/cbr-flows',
                title: 'ОРФР — потоки участников',
                description:
                    'Данные ЦБ РФ по потокам биржевых торгов: кто покупает (физлица, банки, нерезиденты, фонды, …). Категории зависят от типа инструмента и периода — ЦБ изменяет состав отчётности.',
                params: [
                    {
                        name: 'type',
                        type: 'string',
                        required: false,
                        description: 'Тип инструмента: `stocks` (акции), `ofz` (ОФЗ), `fx` (валюты).',
                        defaultValue: 'stocks',
                        enum: ['stocks', 'ofz', 'fx'],
                    },
                    {
                        name: 'years',
                        type: 'int',
                        required: false,
                        description: 'Глубина истории в годах (1–30). По умолчанию 10.',
                        defaultValue: '10',
                    },
                    {
                        name: 'format',
                        type: 'string',
                        required: false,
                        description: '`json` (default) или `csv`.',
                        enum: ['json', 'csv'],
                    },
                ],
                exampleResponse: `{
  "data": [
    {"period_year": 2024, "period_label": "I квартал", "period_kind": "quarter", "period_end_date": "2024-03-31", "category": "retail", "value": 124.5},
    {"period_year": 2024, "period_label": "I квартал", "period_kind": "quarter", "period_end_date": "2024-03-31", "category": "banks", "value": -45.2}
  ],
  "instrument_type": "stocks",
  "years": 10,
  "unit": "млрд руб.",
  "count": 480
}`,
            },
        ],
    },
];

// Плоский массив всех endpoint'ов для backward-compat. Используется в EndpointBlock,
// но сейчас iterate'имся через ENDPOINT_GROUPS.
const ENDPOINTS: EndpointDef[] = ENDPOINT_GROUPS.flatMap((g) => g.endpoints);

// Все секции на странице — для IntersectionObserver active-tracking.
const SECTIONS = [
    { id: 'intro', label: 'Введение' },
    { id: 'auth', label: 'Аутентификация' },
    { id: 'limits', label: 'Лимиты' },
    { id: 'errors', label: 'Ошибки' },
    { id: 'tools', label: 'Инструменты' },
    { id: 'changelog', label: 'Changelog' },
    ...ENDPOINTS.map((e) => ({ id: e.id, label: e.title })),
];

// ════════════════════════════════════════════════════════════════════
// Утилиты
// ════════════════════════════════════════════════════════════════════

/** Парсит `текст` в JSX, заменяя `code` фрагменты на <code>. */
function renderWithBackticks(text: string): React.ReactNode {
    const parts = text.split(/`([^`]+)`/);
    return parts.map((part, i) =>
        i % 2 === 1 ? (
            <code key={i} className="api-inline-code">
                {part}
            </code>
        ) : (
            <Fragment key={i}>{part}</Fragment>
        )
    );
}

/** Построить URL запроса с подстановкой path-плейсхолдеров `{name}` и query-параметрами.
 *  Параметр становится path-параметром если path содержит `{paramName}` —
 *  в этом случае значение НЕ попадает в query string. */
function buildUrl(path: string, params: Record<string, string>): string {
    let url = path;
    const queryEntries: Array<[string, string]> = [];
    Object.entries(params).forEach(([k, v]) => {
        if (v.trim() === '') return;
        const placeholder = `{${k}}`;
        if (url.includes(placeholder)) {
            url = url.replace(placeholder, encodeURIComponent(v));
        } else {
            queryEntries.push([k, v]);
        }
    });
    // Если path-параметр не задан — оставляем placeholder, чтобы юзер увидел проблему.
    const qs = queryEntries
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
        .join('&');
    return `${BASE_URL}${url}${qs ? `?${qs}` : ''}`;
}

/** Генерация code-example для языка. */
function codeExample(lang: Lang, url: string, apiKey: string): string {
    const keyPlaceholder = apiKey || 'pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
    if (lang === 'curl') {
        return `curl -H "X-API-Key: ${keyPlaceholder}" \\
  "${url}"`;
    }
    if (lang === 'python') {
        return `import requests

API_KEY = "${keyPlaceholder}"
r = requests.get(
    "${url}",
    headers={"X-API-Key": API_KEY},
    timeout=30,
)
r.raise_for_status()
data = r.json()
print(data)`;
    }
    return `const API_KEY = "${keyPlaceholder}";

const res = await fetch(
    "${url}",
    { headers: { "X-API-Key": API_KEY } }
);
const data = await res.json();
console.log(data);`;
}

// ════════════════════════════════════════════════════════════════════
// Хуки
// ════════════════════════════════════════════════════════════════════

/** localStorage-bound state для API-ключа. */
function useApiKey(): readonly [string, (k: string) => void] {
    const [key, setKey] = useState<string>(() => {
        if (typeof window === 'undefined') return '';
        try {
            return localStorage.getItem(API_KEY_STORAGE_KEY) || '';
        } catch {
            return '';
        }
    });
    const save = useCallback((k: string) => {
        setKey(k);
        try {
            if (k) localStorage.setItem(API_KEY_STORAGE_KEY, k);
            else localStorage.removeItem(API_KEY_STORAGE_KEY);
        } catch {
            /* localStorage может быть disabled — игнор */
        }
    }, []);
    return [key, save] as const;
}

/** Активная секция по IntersectionObserver. */
function useActiveSection(ids: string[]): string {
    const [active, setActive] = useState<string>(ids[0] || '');
    // useRef — иначе при ре-рендере observer пересоздаётся каждый раз.
    const observerRef = useRef<IntersectionObserver | null>(null);
    useEffect(() => {
        observerRef.current?.disconnect();
        const obs = new IntersectionObserver(
            (entries) => {
                // Выбираем самый верхний пересекающийся элемент.
                const visible = entries
                    .filter((e) => e.isIntersecting)
                    .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
                if (visible.length > 0) {
                    setActive(visible[0].target.id);
                }
            },
            { rootMargin: '-20% 0% -60% 0%' }
        );
        ids.forEach((id) => {
            const el = document.getElementById(id);
            if (el) obs.observe(el);
        });
        observerRef.current = obs;
        return () => obs.disconnect();
    }, [ids]);
    return active;
}

// ════════════════════════════════════════════════════════════════════
// Компоненты
// ════════════════════════════════════════════════════════════════════

function CopyButton({ text }: { text: string }) {
    const [copied, setCopied] = useState(false);
    return (
        <button
            onClick={() => {
                navigator.clipboard?.writeText(text).then(() => {
                    setCopied(true);
                    setTimeout(() => setCopied(false), 1500);
                });
            }}
            title="Копировать"
            className="api-copy-btn"
            style={{ color: copied ? 'var(--success, #2dd478)' : 'var(--text-secondary)' }}
        >
            {copied ? <Check size={12} /> : <Copy size={12} />}
        </button>
    );
}

function CodeBlock({ code, language }: { code: string; language?: string }) {
    return (
        <div className="api-code-block">
            {language && <div className="api-code-lang">{language}</div>}
            <CopyButton text={code} />
            <pre className="api-code-pre">
                <code>{code}</code>
            </pre>
        </div>
    );
}

function MethodBadge({ method }: { method: string }) {
    const color =
        method === 'GET'
            ? 'var(--accent)'
            : method === 'POST'
              ? 'var(--success, #2dd478)'
              : method === 'DELETE'
                ? 'var(--danger, #ef4444)'
                : 'var(--text-secondary)';
    return (
        <span
            style={{
                display: 'inline-block',
                padding: '2px 8px',
                background: `color-mix(in srgb, ${color} 14%, transparent)`,
                color,
                fontSize: 'var(--fs-2xs)',
                fontWeight: 700,
                borderRadius: 4,
                letterSpacing: '0.04em',
                fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
            }}
        >
            {method}
        </span>
    );
}

function StatusBadge({ status }: { status: number }) {
    const color =
        status >= 200 && status < 300
            ? 'var(--success, #2dd478)'
            : status >= 400 && status < 500
              ? 'var(--warning, #f59e0b)'
              : 'var(--danger, #ef4444)';
    const label =
        status === 200
            ? '200 OK'
            : status === 400
              ? '400 Bad Request'
              : status === 401
                ? '401 Unauthorized'
                : status === 403
                  ? '403 Forbidden'
                  : status === 404
                    ? '404 Not Found'
                    : status === 429
                      ? '429 Too Many Requests'
                      : status >= 500
                        ? `${status} Server Error`
                        : status === 0
                          ? 'Network Error'
                          : String(status);
    return (
        <span
            style={{
                display: 'inline-block',
                padding: '3px 10px',
                background: `color-mix(in srgb, ${color} 14%, transparent)`,
                color,
                fontSize: 'var(--fs-2xs)',
                fontWeight: 700,
                borderRadius: 4,
                letterSpacing: '0.04em',
                fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
            }}
        >
            {label}
        </span>
    );
}

interface ApiKeyBarProps {
    apiKey: string;
    onChange: (k: string) => void;
}

function ApiKeyBar({ apiKey, onChange }: ApiKeyBarProps) {
    const [draft, setDraft] = useState(apiKey);
    const [showKey, setShowKey] = useState(false);
    useEffect(() => {
        setDraft(apiKey);
    }, [apiKey]);

    const isSaved = draft === apiKey && apiKey !== '';
    const hasUnsaved = draft !== apiKey;

    return (
        <div
            style={{
                position: 'sticky',
                top: 0,
                zIndex: 30,
                background: 'var(--bg-primary)',
                borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                padding: '12px 16px',
                marginBottom: 24,
            }}
        >
            <div
                style={{
                    maxWidth: 1200,
                    margin: '0 auto',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 10,
                    flexWrap: 'wrap',
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
                    <Key size={14} style={{ color: 'var(--text-secondary)' }} />
                    <span
                        style={{
                            fontSize: 'var(--fs-xs)',
                            color: 'var(--text-secondary)',
                            fontWeight: 600,
                        }}
                    >
                        API-ключ:
                    </span>
                </div>
                <input
                    type={showKey ? 'text' : 'password'}
                    value={draft}
                    onChange={(e) => setDraft(e.target.value)}
                    placeholder="pk_live_…"
                    style={{
                        flex: 1,
                        minWidth: 200,
                        padding: '6px 12px',
                        background: 'var(--bg-secondary)',
                        border: `1.5px solid ${
                            isSaved
                                ? 'color-mix(in srgb, var(--success, #2dd478) 50%, var(--text-primary))'
                                : 'color-mix(in srgb, var(--text-primary) 20%, transparent)'
                        }`,
                        borderRadius: 8,
                        color: 'var(--text-primary)',
                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                        fontSize: 'var(--fs-xs)',
                        outline: 'none',
                    }}
                />
                <button
                    onClick={() => setShowKey((v) => !v)}
                    title={showKey ? 'Скрыть' : 'Показать'}
                    style={{
                        padding: 6,
                        background: 'var(--bg-secondary)',
                        border: '1.5px solid color-mix(in srgb, var(--text-primary) 20%, transparent)',
                        borderRadius: 8,
                        cursor: 'pointer',
                        color: 'var(--text-secondary)',
                        display: 'inline-flex',
                        alignItems: 'center',
                    }}
                >
                    {showKey ? <EyeOff size={14} /> : <Eye size={14} />}
                </button>
                <button
                    onClick={() => onChange(draft)}
                    disabled={!hasUnsaved}
                    style={{
                        padding: '6px 14px',
                        background: hasUnsaved ? 'var(--accent)' : 'var(--bg-secondary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 999,
                        cursor: hasUnsaved ? 'pointer' : 'default',
                        color: hasUnsaved ? 'var(--text-inverse)' : 'var(--text-muted)',
                        fontSize: 'var(--fs-xs)',
                        fontWeight: 700,
                        opacity: hasUnsaved ? 1 : 0.6,
                    }}
                >
                    {isSaved ? 'Сохранён' : 'Сохранить'}
                </button>
                {apiKey && (
                    <button
                        onClick={() => {
                            setDraft('');
                            onChange('');
                        }}
                        title="Удалить из браузера"
                        style={{
                            padding: 6,
                            background: 'transparent',
                            border: '1.5px solid color-mix(in srgb, var(--text-primary) 20%, transparent)',
                            borderRadius: 8,
                            cursor: 'pointer',
                            color: 'var(--text-secondary)',
                            display: 'inline-flex',
                            alignItems: 'center',
                        }}
                    >
                        <Trash2 size={12} />
                    </button>
                )}
            </div>
            <p
                style={{
                    maxWidth: 1200,
                    margin: '6px auto 0',
                    fontSize: 'var(--fs-2xs)',
                    color: 'var(--text-muted)',
                    lineHeight: 1.4,
                }}
            >
                Сохраняется в браузере (localStorage). Используется в примерах и кнопке «Попробовать».{' '}
                Нет ключа? Создайте в{' '}
                <Link to="/profile" style={{ color: 'var(--accent)', textDecoration: 'underline' }}>
                    личном кабинете
                </Link>
                .
            </p>
        </div>
    );
}

interface SidebarProps {
    active: string;
}

function Sidebar({ active }: SidebarProps) {
    const groupHeading = (label: string, marginTop = 16) => (
        <div
            style={{
                fontSize: 'var(--fs-2xs)',
                color: 'var(--text-muted)',
                fontWeight: 700,
                textTransform: 'uppercase',
                letterSpacing: '0.05em',
                marginTop,
                marginBottom: 8,
            }}
        >
            {label}
        </div>
    );
    return (
        <nav className="api-sidebar" aria-label="Навигация по документации">
            {groupHeading('Документация', 0)}
            <a href="#intro" className={`api-toc-link ${active === 'intro' ? 'active' : ''}`}>
                Введение
            </a>
            <a href="#auth" className={`api-toc-link ${active === 'auth' ? 'active' : ''}`}>
                Аутентификация
            </a>
            <a href="#limits" className={`api-toc-link ${active === 'limits' ? 'active' : ''}`}>
                Лимиты
            </a>
            <a href="#errors" className={`api-toc-link ${active === 'errors' ? 'active' : ''}`}>
                Ошибки
            </a>
            <a href="#tools" className={`api-toc-link ${active === 'tools' ? 'active' : ''}`}>
                Инструменты
            </a>
            <a href="#changelog" className={`api-toc-link ${active === 'changelog' ? 'active' : ''}`}>
                Changelog
            </a>
            {ENDPOINT_GROUPS.map((group) => (
                <Fragment key={group.title}>
                    {groupHeading(group.title)}
                    {group.endpoints.map((ep) => (
                        <a
                            key={ep.id}
                            href={`#${ep.id}`}
                            className={`api-toc-link ${active === ep.id ? 'active' : ''}`}
                        >
                            {ep.title}
                        </a>
                    ))}
                </Fragment>
            ))}
        </nav>
    );
}

interface LangTabsProps {
    value: Lang;
    onChange: (l: Lang) => void;
}

function LangTabs({ value, onChange }: LangTabsProps) {
    const tabs: { id: Lang; label: string }[] = [
        { id: 'curl', label: 'cURL' },
        { id: 'python', label: 'Python' },
        { id: 'javascript', label: 'JavaScript' },
    ];
    return (
        <div
            style={{
                display: 'flex',
                gap: 4,
                marginBottom: 8,
                padding: 4,
                background: 'var(--bg-secondary)',
                borderRadius: 8,
                width: 'fit-content',
            }}
        >
            {tabs.map((t) => (
                <button
                    key={t.id}
                    onClick={() => onChange(t.id)}
                    style={{
                        padding: '4px 12px',
                        background: value === t.id ? 'var(--bg-primary)' : 'transparent',
                        color: value === t.id ? 'var(--text-primary)' : 'var(--text-secondary)',
                        border: value === t.id
                            ? '1px solid color-mix(in srgb, var(--text-primary) 18%, transparent)'
                            : '1px solid transparent',
                        borderRadius: 6,
                        cursor: 'pointer',
                        fontSize: 'var(--fs-xs)',
                        fontWeight: value === t.id ? 700 : 600,
                        fontFamily: 'inherit',
                    }}
                >
                    {t.label}
                </button>
            ))}
        </div>
    );
}

interface ResponseState {
    status: number;
    body: unknown;
    ms: number;
    error?: string;
}

interface EndpointBlockProps {
    ep: EndpointDef;
    apiKey: string;
    lang: Lang;
    onLangChange: (l: Lang) => void;
    /** True если у юзера НЕТ api_access (гость / free / basic). Включает lock на Try-кнопку. */
    locked: boolean;
}

function EndpointBlock({ ep, apiKey, lang, onLangChange, locked }: EndpointBlockProps) {
    // Локальный state параметров формы — keyed by param name.
    const initialParams = useMemo(() => {
        const obj: Record<string, string> = {};
        ep.params.forEach((p) => {
            obj[p.name] = p.defaultValue || '';
        });
        return obj;
    }, [ep.params]);
    const [params, setParams] = useState<Record<string, string>>(initialParams);
    const [response, setResponse] = useState<ResponseState | null>(null);
    const [loading, setLoading] = useState(false);

    const url = buildUrl(ep.path, params);
    const code = codeExample(lang, url, apiKey);

    const handleTry = async () => {
        if (!apiKey) {
            setResponse({
                status: 0,
                body: { detail: 'Введите API-ключ в верхней панели чтобы протестировать запрос.' },
                ms: 0,
                error: 'no-key',
            });
            return;
        }
        // Валидация required-параметров на клиенте — UX-приятнее чем 400 с сервера.
        const missing = ep.params.filter((p) => p.required && !params[p.name]?.trim());
        if (missing.length > 0) {
            setResponse({
                status: 0,
                body: { detail: `Заполните обязательные параметры: ${missing.map((p) => p.name).join(', ')}` },
                ms: 0,
                error: 'missing-params',
            });
            return;
        }
        setLoading(true);
        const start = performance.now();
        try {
            const r = await fetch(url, { headers: { 'X-API-Key': apiKey } });
            const ms = Math.round(performance.now() - start);
            const contentType = r.headers.get('content-type') || '';
            // CSV responses → парсим как text, показываем первые ~200 строк.
            // JSON responses → парсим как JSON (с graceful fallback).
            let body: unknown;
            if (contentType.includes('text/csv') || contentType.includes('text/plain')) {
                const text = await r.text();
                // Срезаем BOM если есть. Показываем raw CSV.
                const cleaned = text.replace(/^﻿/, '');
                body = { _csv: cleaned, _csv_lines: cleaned.split('\n').length };
            } else {
                body = await r.json().catch(() => ({ detail: 'Не удалось распарсить ответ как JSON' }));
            }
            setResponse({ status: r.status, body, ms });
        } catch (e) {
            setResponse({
                status: 0,
                body: { detail: (e as Error).message },
                ms: Math.round(performance.now() - start),
                error: 'network',
            });
        } finally {
            setLoading(false);
        }
    };

    // Текст ответа — pretty-printed JSON, CSV-первые строки, или example.
    let responseText: string;
    if (response) {
        // Special-case для CSV: показать raw text (не stringify).
        const csvBody = response.body as { _csv?: string; _csv_lines?: number } | null;
        if (csvBody && typeof csvBody === 'object' && '_csv' in csvBody) {
            const lines = csvBody._csv!.split('\n');
            const preview = lines.slice(0, 30).join('\n');
            responseText = preview + (lines.length > 30 ? `\n… (${lines.length - 30} more lines)` : '');
        } else {
            responseText = JSON.stringify(response.body, null, 2);
        }
    } else {
        responseText = ep.exampleResponse;
    }

    return (
        <section id={ep.id} className="api-endpoint">
            {/* Заголовок: method + path */}
            <div className="api-endpoint-header">
                <MethodBadge method={ep.method} />
                <code className="api-endpoint-path">{ep.path}</code>
            </div>
            <h2 className="api-endpoint-title">{ep.title}</h2>

            <div className="api-endpoint-grid">
                {/* ════════ Левая колонка: docs + params ════════ */}
                <div>
                    <p className="api-endpoint-desc">{renderWithBackticks(ep.description)}</p>

                    {ep.params.length > 0 && (
                        <>
                            <h3 className="api-section-h3">Параметры</h3>
                            <div className="api-params-table">
                                {ep.params.map((p) => (
                                    <div key={p.name} className="api-param-row">
                                        <div>
                                            <code className="api-param-name">{p.name}</code>
                                            {p.required && <span className="api-required">обязательный</span>}
                                            <span className="api-param-type">{p.type}</span>
                                        </div>
                                        <div className="api-param-desc">
                                            {renderWithBackticks(p.description)}
                                        </div>
                                    </div>
                                ))}
                            </div>

                            <h3 className="api-section-h3">Попробовать</h3>
                            <div className="api-try-form">
                                {ep.params.map((p) => (
                                    <label key={p.name} className="api-try-field">
                                        <span className="api-try-label">
                                            {p.name}
                                            {p.required && <span style={{ color: 'var(--danger, #ef4444)' }}> *</span>}
                                        </span>
                                        {p.enum ? (
                                            <select
                                                value={params[p.name] ?? ''}
                                                onChange={(e) =>
                                                    setParams({ ...params, [p.name]: e.target.value })
                                                }
                                                className="api-try-input"
                                            >
                                                <option value="">— не задавать —</option>
                                                {p.enum.map((opt) => (
                                                    <option key={opt} value={opt}>
                                                        {opt}
                                                    </option>
                                                ))}
                                            </select>
                                        ) : (
                                            <input
                                                type={p.type === 'int' ? 'number' : 'text'}
                                                value={params[p.name] ?? ''}
                                                onChange={(e) =>
                                                    setParams({ ...params, [p.name]: e.target.value })
                                                }
                                                placeholder={p.defaultValue || ''}
                                                className="api-try-input"
                                            />
                                        )}
                                    </label>
                                ))}
                            </div>
                        </>
                    )}

                    {locked ? (
                        <Link
                            to="/pricing"
                            className="api-try-btn"
                            style={{ textDecoration: 'none' }}
                        >
                            <Lock size={12} />
                            Доступно на Pro
                        </Link>
                    ) : (
                        <button
                            onClick={handleTry}
                            disabled={loading}
                            className="api-try-btn"
                        >
                            <Play size={12} />
                            {loading ? 'Отправляем…' : ep.params.length > 0 ? 'Отправить запрос' : 'Попробовать'}
                        </button>
                    )}
                    {!locked && !apiKey && (
                        <p className="api-try-hint">
                            <AlertCircle size={11} style={{ verticalAlign: '-1px' }} /> Введите ключ
                            в верхней панели, чтобы получить реальный ответ.
                        </p>
                    )}
                </div>

                {/* ════════ Правая колонка: code + response ════════ */}
                <div>
                    <LangTabs value={lang} onChange={onLangChange} />
                    <CodeBlock code={code} />

                    <h3 className="api-section-h3 api-response-h3">
                        Ответ
                        {response && (
                            <>
                                <StatusBadge status={response.status} />
                                <span className="api-response-time">{response.ms} мс</span>
                            </>
                        )}
                        {!response && (
                            <span style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-2xs)', fontWeight: 500 }}>
                                пример
                            </span>
                        )}
                    </h3>
                    <div className="api-code-block">
                        <CopyButton text={responseText} />
                        <pre
                            className="api-code-pre"
                            style={{ maxHeight: 360, overflow: 'auto' }}
                        >
                            <code>{responseText}</code>
                        </pre>
                    </div>
                </div>
            </div>
        </section>
    );
}

// ════════════════════════════════════════════════════════════════════
// Главный компонент
// ════════════════════════════════════════════════════════════════════

/**
 * Pro-CTA баннер для non-Pro юзеров.
 * Показывается сверху страницы — над ApiKeyBar. Объясняет что API
 * доступен только на тарифе Pro, со ссылкой на /pricing.
 */
function ProRequiredBanner() {
    return (
        <div
            style={{
                background: 'color-mix(in srgb, var(--accent) 12%, var(--bg-secondary))',
                borderBottom: '2px solid var(--accent)',
                padding: '14px 16px',
            }}
        >
            <div
                style={{
                    maxWidth: 1200,
                    margin: '0 auto',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 14,
                    flexWrap: 'wrap',
                }}
            >
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: 36,
                        height: 36,
                        background: 'var(--accent)',
                        borderRadius: 8,
                        color: 'var(--text-inverse)',
                        flexShrink: 0,
                    }}
                >
                    <Lock size={18} strokeWidth={2.2} />
                </div>
                <div style={{ flex: 1, minWidth: 220 }}>
                    <div
                        style={{
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            color: 'var(--text-primary)',
                            marginBottom: 2,
                        }}
                    >
                        API-доступ — функция тарифа Pro
                    </div>
                    <div
                        style={{
                            fontSize: 'var(--fs-xs)',
                            color: 'var(--text-secondary)',
                            lineHeight: 1.45,
                        }}
                    >
                        Документация открыта для всех — её можно читать, чтобы оценить возможности.
                        Но создавать ключи и делать запросы можно только на Pro.
                    </div>
                </div>
                <Link
                    to="/pricing"
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '8px 18px',
                        background: 'var(--accent)',
                        color: 'var(--text-inverse)',
                        textDecoration: 'none',
                        borderRadius: 999,
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 700,
                        border: '1.5px solid var(--text-primary)',
                        flexShrink: 0,
                    }}
                >
                    <Sparkles size={14} />
                    Перейти на Pro
                </Link>
            </div>
        </div>
    );
}

export default function ApiDocsPage() {
    const [apiKey, setApiKey] = useApiKey();
    const [lang, setLang] = useState<Lang>('curl');
    const sectionIds = useMemo(() => SECTIONS.map((s) => s.id), []);
    const active = useActiveSection(sectionIds);
    // Tier-gating: показываем CTA-баннер если у юзера нет api_access.
    // Не блокируем доступ к документации — она нужна гостям/free-юзерам
    // чтобы принять решение о покупке Pro.
    const common = useCommonFeatures();
    const { isAuthenticated } = useAuth();
    const showProBanner = !common.api_access;

    return (
        <>
            <style>{INLINE_STYLES}</style>

            {/* Pro-CTA сверху для не-Pro юзеров (гостей + free + basic) */}
            {showProBanner && <ProRequiredBanner />}

            {/* Sticky API-key bar — скрываем для гостей (бессмысленно вводить ключ) */}
            {isAuthenticated && <ApiKeyBar apiKey={apiKey} onChange={setApiKey} />}

            <div className="api-docs-layout">
                {/* Sidebar */}
                <aside className="api-sidebar-wrap">
                    <Sidebar active={active} />
                </aside>

                {/* Main content */}
                <main className="api-main">
                    <Link
                        to="/profile"
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 6,
                            color: 'var(--text-secondary)',
                            fontSize: 'var(--fs-sm)',
                            marginBottom: 16,
                            textDecoration: 'none',
                        }}
                    >
                        <ArrowLeft size={14} />К профилю
                    </Link>

                    {/* ════ Intro ════ */}
                    <section id="intro" className="api-section">
                        <header className="flex items-center gap-4 mb-6">
                            <div
                                style={{
                                    width: 48,
                                    height: 48,
                                    borderRadius: 8,
                                    background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
                                    color: 'var(--accent)',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                }}
                            >
                                <Key size={24} strokeWidth={1.8} />
                            </div>
                            <h1
                                style={{
                                    fontSize: 'var(--fs-3xl)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    letterSpacing: '-0.015em',
                                    margin: 0,
                                }}
                            >
                                API Фрейма
                            </h1>
                        </header>
                        <p
                            style={{
                                fontSize: 'var(--fs-base)',
                                color: 'var(--text-secondary)',
                                lineHeight: 1.6,
                            }}
                        >
                            Программный доступ к данным MOEX через REST API. Для торговых ботов,
                            скриптов аналитики и личных сводных панелей. Возвращает JSON, аутентификация
                            через персональный ключ <code className="api-inline-code">pk_live_…</code>.
                        </p>
                        <p
                            style={{
                                fontSize: 'var(--fs-sm)',
                                color: 'var(--text-muted)',
                                lineHeight: 1.6,
                                marginTop: 8,
                            }}
                        >
                            Доступ — тариф <strong style={{ color: 'var(--text-primary)' }}>Pro</strong>.
                            Создать ключ можно в{' '}
                            <Link to="/profile" style={{ color: 'var(--accent)', textDecoration: 'underline' }}>
                                личном кабинете
                            </Link>
                            .
                        </p>
                    </section>

                    {/* ════ Auth ════ */}
                    <section id="auth" className="api-section">
                        <h2 className="api-section-h2">Аутентификация</h2>
                        <p className="api-section-p">
                            Передавайте ключ в заголовке <code className="api-inline-code">X-API-Key</code>{' '}
                            или как <code className="api-inline-code">Authorization: Bearer</code>.
                            Ключи генерируются один раз — мы храним только их хеш, восстановить нельзя.
                        </p>
                        <CodeBlock
                            language="http"
                            code={`X-API-Key: pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# или

Authorization: Bearer pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`}
                        />
                        <p
                            style={{
                                fontSize: 'var(--fs-xs)',
                                color: 'var(--text-muted)',
                                marginTop: 8,
                            }}
                        >
                            Если ключ скомпрометирован — отзовите его в личном кабинете и создайте новый.
                            Максимум 10 активных ключей на аккаунт.
                        </p>

                        <h3 className="api-section-h3" style={{ marginTop: 20 }}>
                            Live и Test ключи
                        </h3>
                        <p className="api-section-p">
                            Два режима ключей по аналогии со Stripe:
                        </p>
                        <ul className="api-section-ul">
                            <li>
                                <code className="api-inline-code">pk_live_…</code> —{' '}
                                <strong style={{ color: 'var(--text-primary)' }}>production</strong>,
                                требует тариф Pro, все endpoints доступны.
                            </li>
                            <li>
                                <code className="api-inline-code">pk_test_…</code> —{' '}
                                <strong style={{ color: 'var(--text-primary)' }}>разработка</strong>,
                                бесплатно для любого юзера. Те же endpoints, тот же rate-limit, те же
                                данные. Идеально для локальной разработки, CI/CD pipelines, демо.
                            </li>
                        </ul>
                        <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', marginTop: 8 }}>
                            Префикс ключа сразу показывает режим — нельзя случайно «подсунуть»
                            test-ключ в production бот или наоборот.
                        </p>

                        <h3 className="api-section-h3" style={{ marginTop: 20 }}>
                            Фиксация версии API
                        </h3>
                        <p className="api-section-p">
                            По умолчанию ваш клиент получает <strong style={{ color: 'var(--text-primary)' }}>latest</strong>{' '}
                            версию. Чтобы зафиксироваться на конкретной версии и не зависеть от
                            будущих изменений — передавайте <code className="api-inline-code">Accept</code>{' '}
                            header:
                        </p>
                        <CodeBlock
                            language="http"
                            code={`Accept: application/vnd.frame.v1+json`}
                        />
                        <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', marginTop: 8 }}>
                            В каждом ответе есть <code className="api-inline-code">X-Frame-Version</code> —
                            фактическая версия которая отдала данные. Если вы запросили несуществующую
                            версию — получите <code className="api-inline-code">406 Not Acceptable</code>.
                        </p>
                    </section>

                    {/* ════ Limits ════ */}
                    <section id="limits" className="api-section">
                        <h2 className="api-section-h2">Лимиты</h2>
                        <ul className="api-section-ul">
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>60 запросов / минуту</strong>{' '}
                                на API-ключ. При превышении — <code className="api-inline-code">429 Too Many Requests</code>{' '}
                                с заголовком <code className="api-inline-code">Retry-After</code>{' '}
                                (секунды до сброса окна).
                            </li>
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>30 запросов / секунду</strong>{' '}
                                на IP — общий nginx-лимит для всего сайта.
                            </li>
                            <li>Все ответы сжаты gzip — экономия трафика до 80%.</li>
                        </ul>
                        <p className="api-section-p" style={{ marginTop: 12 }}>
                            <strong style={{ color: 'var(--text-primary)' }}>Headers в каждом успешном ответе:</strong>
                        </p>
                        <CodeBlock
                            language="http"
                            code={`X-RateLimit-Limit:        60                          # лимит за окно
X-RateLimit-Remaining:    47                          # сколько осталось
X-RateLimit-Reset:        1716548340                  # unix-ts когда сбросится
X-Subscription-Expires-At: 2026-12-31T23:59:59+03:00  # когда истекает Pro
X-Frame-Version:          v1                          # версия API в этом ответе
Cache-Control:            public, max-age=60          # как долго можно кэшировать`}
                        />
                        <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', marginTop: 4 }}>
                            Используйте <code className="api-inline-code">X-RateLimit-Remaining</code> для
                            backoff и <code className="api-inline-code">X-Subscription-Expires-At</code> чтобы
                            заблаговременно напоминать о продлении. <code className="api-inline-code">Cache-Control</code>
                            — клиент может кэшировать результат указанное число секунд.
                        </p>
                    </section>

                    {/* ════ Errors ════ */}
                    <section id="errors" className="api-section">
                        <h2 className="api-section-h2">Ошибки</h2>
                        <p className="api-section-p">
                            Все ошибки возвращаются в едином envelope-формате с HTTP-кодом и
                            детальным сообщением:
                        </p>
                        <CodeBlock
                            language="json"
                            code={`{
  "success": false,
  "error": {
    "code": 401,
    "message": "Invalid API key"
  }
}`}
                        />
                        <p className="api-section-p" style={{ marginTop: 8 }}>
                            Возможные коды:
                        </p>
                        <div className="api-errors-grid">
                            {[
                                { code: 400, label: 'некорректные параметры' },
                                { code: 401, label: 'ключ отсутствует, отозван или невалиден' },
                                { code: 403, label: 'у пользователя не Pro tier' },
                                { code: 404, label: 'данные не найдены' },
                                { code: 429, label: 'превышен лимит (Retry-After + RateLimit headers)' },
                                { code: 500, label: 'внутренняя ошибка сервера' },
                            ].map((row) => (
                                <div key={row.code} className="api-error-row">
                                    <StatusBadge status={row.code} />
                                    <span style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                                        {row.label}
                                    </span>
                                </div>
                            ))}
                        </div>
                    </section>

                    {/* ════ Tools / OpenAPI ════ */}
                    <section id="tools" className="api-section">
                        <h2 className="api-section-h2">Инструменты для интеграции</h2>
                        <p className="api-section-p">
                            Для удобной разработки клиентских приложений:
                        </p>
                        <ul className="api-section-ul">
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>OpenAPI 3.0 спека:</strong>{' '}
                                <a
                                    href="/api/v1/public/openapi.json"
                                    target="_blank"
                                    rel="noreferrer"
                                    style={{ color: 'var(--accent)', textDecoration: 'underline' }}
                                >
                                    /api/v1/public/openapi.json
                                </a>
                                {' '}— импортируйте в Postman, Insomnia, или сгенерируйте клиент
                                на любом языке через openapi-codegen.
                            </li>
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>CSV-формат:</strong>{' '}
                                для history-эндпоинтов (
                                <code className="api-inline-code">/breadth/history</code>,{' '}
                                <code className="api-inline-code">/oi/history</code>,{' '}
                                <code className="api-inline-code">/seasonality</code>,{' '}
                                <code className="api-inline-code">/buffett/*</code>,{' '}
                                <code className="api-inline-code">/funds/&#123;ticker&#125;/history</code>,{' '}
                                <code className="api-inline-code">/cbr-flows</code>) можно
                                запросить ответ как CSV, добавив <code className="api-inline-code">?format=csv</code>{' '}
                                — удобно для pandas / Excel.
                            </li>
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>Cursor-pagination:</strong>{' '}
                                для history-эндпоинтов добавьте{' '}
                                <code className="api-inline-code">?limit=1000</code> чтобы получать
                                ответ чанками. В ответе будет{' '}
                                <code className="api-inline-code">next_cursor</code> — передайте его как{' '}
                                <code className="api-inline-code">?after_date=&lt;cursor&gt;</code> чтобы
                                получить следующую страницу. Полезно для огромных периодов (10+ лет).
                            </li>
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>Health-check:</strong>{' '}
                                <code className="api-inline-code">GET /api/v1/public/health</code>{' '}
                                — без авторизации, для UptimeRobot / Pingdom.
                            </li>
                            <li>
                                <strong style={{ color: 'var(--text-primary)' }}>Usage stats:</strong>{' '}
                                смотрите в <Link to="/profile" style={{ color: 'var(--accent)', textDecoration: 'underline' }}>личном кабинете</Link>
                                {' '}— график запросов за последние 30 дней.
                            </li>
                        </ul>
                    </section>

                    {/* ════ Changelog ════ */}
                    <section id="changelog" className="api-section">
                        <h2 className="api-section-h2">История версий</h2>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                            <div>
                                <div style={{
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 4,
                                }}>
                                    v1.4 — 2026-05-24
                                </div>
                                <ul className="api-section-ul">
                                    <li>Test mode: <code className="api-inline-code">pk_test_</code> ключи доступны без Pro — для разработки и CI/CD</li>
                                    <li>API version pinning через <code className="api-inline-code">Accept: application/vnd.frame.v1+json</code></li>
                                    <li>Header <code className="api-inline-code">X-Frame-Version</code> в каждом ответе</li>
                                    <li>406 Not Acceptable если запросить несуществующую версию</li>
                                </ul>
                            </div>
                            <div>
                                <div style={{
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 4,
                                }}>
                                    v1.3 — 2026-05-24
                                </div>
                                <ul className="api-section-ul">
                                    <li>Header <code className="api-inline-code">X-Subscription-Expires-At</code> — для проактивных уведомлений</li>
                                    <li>Header <code className="api-inline-code">Cache-Control</code> с разумным max-age (60s/300s/86400s)</li>
                                    <li>Cursor pagination: <code className="api-inline-code">?after_date=YYYY-MM-DD&amp;limit=1000</code> + <code className="api-inline-code">next_cursor</code> в ответе</li>
                                    <li>Usage stats endpoint <code className="api-inline-code">/api/keys/usage</code> + chart в личном кабинете</li>
                                    <li>Pro-tier enforcement: после истечения подписки key возвращает 403 до возобновления</li>
                                </ul>
                            </div>
                            <div>
                                <div style={{
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 4,
                                }}>
                                    v1.2 — 2026-05-24
                                </div>
                                <ul className="api-section-ul">
                                    <li>Унифицированный envelope для ошибок: <code className="api-inline-code">{'{success, error: {code, message}}'}</code></li>
                                    <li>Headers <code className="api-inline-code">X-RateLimit-*</code> в каждом ответе</li>
                                    <li>Параметр <code className="api-inline-code">?format=csv</code> для history-эндпоинтов</li>
                                    <li>OpenAPI 3.0 спека на <code className="api-inline-code">/openapi.json</code></li>
                                    <li>Health-check эндпоинт без auth</li>
                                </ul>
                            </div>
                            <div>
                                <div style={{
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 4,
                                }}>
                                    v1.1 — 2026-05-24
                                </div>
                                <ul className="api-section-ul">
                                    <li>+8 эндпоинтов: <code className="api-inline-code">/stocks/&#123;ticker&#125;/quote</code>,{' '}
                                        <code className="api-inline-code">/breadth/history</code>,{' '}
                                        <code className="api-inline-code">/oi/history</code>,{' '}
                                        <code className="api-inline-code">/buffett/cap-gdp</code>,{' '}
                                        <code className="api-inline-code">/buffett/cap-m2</code>,{' '}
                                        <code className="api-inline-code">/funds/&#123;ticker&#125;</code>,{' '}
                                        <code className="api-inline-code">/funds/&#123;ticker&#125;/history</code>,{' '}
                                        <code className="api-inline-code">/cbr-flows</code>
                                    </li>
                                </ul>
                            </div>
                            <div>
                                <div style={{
                                    fontSize: 'var(--fs-sm)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    marginBottom: 4,
                                }}>
                                    v1.0 — 2026-05-23
                                </div>
                                <ul className="api-section-ul">
                                    <li>Релиз публичного API: <code className="api-inline-code">/heatmap</code>,{' '}
                                        <code className="api-inline-code">/breadth/current</code>,{' '}
                                        <code className="api-inline-code">/instruments</code>,{' '}
                                        <code className="api-inline-code">/oi/current</code>,{' '}
                                        <code className="api-inline-code">/funds/categories</code>,{' '}
                                        <code className="api-inline-code">/seasonality</code>
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </section>

                    {/* ════ Endpoints — сгруппированы для navigability ════ */}
                    {ENDPOINT_GROUPS.map((group) => (
                        <Fragment key={group.title}>
                            <h2
                                style={{
                                    marginTop: 56,
                                    marginBottom: 0,
                                    fontSize: 'var(--fs-2xl)',
                                    fontWeight: 700,
                                    color: 'var(--text-primary)',
                                    letterSpacing: '-0.01em',
                                }}
                            >
                                {group.title}
                            </h2>
                            {group.endpoints.map((ep) => (
                                <EndpointBlock
                                    key={ep.id}
                                    ep={ep}
                                    apiKey={apiKey}
                                    lang={lang}
                                    onLangChange={setLang}
                                    locked={!common.api_access}
                                />
                            ))}
                        </Fragment>
                    ))}

                    {/* ════ Footer ════ */}
                    <section
                        style={{
                            marginTop: 48,
                            paddingTop: 20,
                            borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                        }}
                    >
                        <p
                            style={{
                                fontSize: 'var(--fs-xs)',
                                color: 'var(--text-muted)',
                                lineHeight: 1.6,
                            }}
                        >
                            API находится в стабильной версии <code className="api-inline-code">v1</code>.
                            Breaking changes выпускаются в новой версии <code className="api-inline-code">v2</code>{' '}
                            с минимум 30-дневной депрекацией старой. Вопросы и feature requests — на{' '}
                            <a
                                href="mailto:frameinfo@mail.ru"
                                style={{ color: 'var(--accent)', textDecoration: 'underline' }}
                            >
                                frameinfo@mail.ru
                            </a>
                            .
                        </p>
                    </section>
                </main>
            </div>
        </>
    );
}

// ════════════════════════════════════════════════════════════════════
// Стили — inline через <style> чтобы не плодить .css-файлы
// ════════════════════════════════════════════════════════════════════

const INLINE_STYLES = `
.api-docs-layout {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 16px 64px;
    display: grid;
    grid-template-columns: 220px 1fr;
    gap: 32px;
}
@media (max-width: 900px) {
    .api-docs-layout {
        grid-template-columns: 1fr;
        gap: 0;
    }
}

.api-sidebar-wrap {
    position: sticky;
    top: 88px;
    align-self: start;
    max-height: calc(100vh - 100px);
    overflow-y: auto;
    padding-right: 8px;
}
@media (max-width: 900px) {
    .api-sidebar-wrap {
        display: none;
    }
}

.api-sidebar {
    display: flex;
    flex-direction: column;
    gap: 1px;
}
.api-toc-link {
    display: block;
    padding: 6px 10px;
    font-size: var(--fs-xs);
    color: var(--text-secondary);
    text-decoration: none;
    border-radius: 6px;
    border-left: 2px solid transparent;
    transition: background 120ms, color 120ms, border-color 120ms;
}
.api-toc-link:hover {
    background: var(--bg-secondary);
    color: var(--text-primary);
}
.api-toc-link.active {
    color: var(--accent);
    border-left-color: var(--accent);
    font-weight: 700;
    background: color-mix(in srgb, var(--accent) 8%, transparent);
}

.api-main {
    min-width: 0;
}

.api-section {
    margin-top: 32px;
    padding-top: 24px;
    border-top: 1px solid color-mix(in srgb, var(--text-primary) 10%, transparent);
}
.api-section:first-of-type {
    border-top: none;
    padding-top: 0;
    margin-top: 0;
}

.api-section-h2 {
    font-size: var(--fs-xl);
    font-weight: 700;
    color: var(--text-primary);
    margin: 0 0 8px;
}
.api-section-h3 {
    font-size: var(--fs-sm);
    font-weight: 700;
    color: var(--text-primary);
    margin: 16px 0 6px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    display: flex;
    align-items: center;
    gap: 8px;
}
.api-section-p {
    font-size: var(--fs-sm);
    color: var(--text-secondary);
    line-height: 1.55;
    margin: 0 0 8px;
}
.api-section-ul {
    font-size: var(--fs-sm);
    color: var(--text-secondary);
    line-height: 1.6;
    margin: 0;
    padding-left: 20px;
}
.api-section-ul li {
    margin-bottom: 4px;
}

.api-errors-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 8px;
    margin-top: 8px;
}
.api-error-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 10px;
    background: var(--bg-secondary);
    border-radius: 8px;
}

.api-endpoint {
    margin-top: 48px;
    padding-top: 28px;
    border-top: 1px solid color-mix(in srgb, var(--text-primary) 10%, transparent);
}

.api-endpoint-header {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    margin-bottom: 6px;
}
.api-endpoint-path {
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    font-size: var(--fs-sm);
    color: var(--text-primary);
    font-weight: 600;
}
.api-endpoint-title {
    font-size: var(--fs-xl);
    font-weight: 700;
    color: var(--text-primary);
    margin: 8px 0 16px;
}
.api-endpoint-desc {
    font-size: var(--fs-sm);
    color: var(--text-secondary);
    line-height: 1.55;
    margin: 0 0 12px;
}

.api-endpoint-grid {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.1fr);
    gap: 24px;
    margin-top: 12px;
}
@media (max-width: 900px) {
    .api-endpoint-grid {
        grid-template-columns: 1fr;
    }
}

.api-params-table {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 12px;
    background: var(--bg-secondary);
    border-radius: 8px;
}
.api-param-row {
    display: flex;
    flex-direction: column;
    gap: 2px;
}
.api-param-name {
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    color: var(--text-primary);
    font-weight: 700;
    font-size: var(--fs-sm);
}
.api-param-type {
    margin-left: 8px;
    font-size: var(--fs-2xs);
    color: var(--text-muted);
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
}
.api-required {
    margin-left: 8px;
    font-size: var(--fs-2xs);
    color: var(--danger, #ef4444);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
.api-param-desc {
    font-size: var(--fs-xs);
    color: var(--text-secondary);
    line-height: 1.5;
}

.api-try-form {
    display: flex;
    flex-direction: column;
    gap: 8px;
}
.api-try-field {
    display: flex;
    flex-direction: column;
    gap: 4px;
}
.api-try-label {
    font-size: var(--fs-xs);
    color: var(--text-secondary);
    font-weight: 600;
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
}
.api-try-input {
    padding: 6px 10px;
    background: var(--bg-secondary);
    border: 1.5px solid color-mix(in srgb, var(--text-primary) 18%, transparent);
    border-radius: 6px;
    color: var(--text-primary);
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    font-size: var(--fs-xs);
    outline: none;
}
.api-try-input:focus {
    border-color: var(--accent);
}

.api-try-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-top: 12px;
    padding: 8px 18px;
    background: var(--accent);
    color: var(--text-inverse);
    border: 1.5px solid var(--text-primary);
    border-radius: 999px;
    font-size: var(--fs-sm);
    font-weight: 700;
    cursor: pointer;
    box-shadow: var(--shadow-hard-chip, none);
    font-family: inherit;
}
.api-try-btn:disabled {
    opacity: 0.6;
    cursor: wait;
}

.api-try-hint {
    margin-top: 6px;
    font-size: var(--fs-2xs);
    color: var(--text-muted);
    display: flex;
    align-items: center;
    gap: 4px;
}

.api-response-h3 {
    justify-content: space-between;
}
.api-response-time {
    margin-left: auto;
    font-size: var(--fs-2xs);
    color: var(--text-muted);
    font-weight: 500;
    text-transform: none;
    letter-spacing: 0;
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
}

.api-code-block {
    position: relative;
    margin: 4px 0 8px;
    background: var(--bg-secondary);
    border: 1px solid color-mix(in srgb, var(--text-primary) 12%, transparent);
    border-radius: 8px;
    overflow: hidden;
}
.api-code-lang {
    position: absolute;
    top: 8px;
    right: 36px;
    font-size: var(--fs-2xs);
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-weight: 600;
}
.api-copy-btn {
    position: absolute;
    top: 6px;
    right: 6px;
    background: var(--bg-primary);
    border: 1px solid color-mix(in srgb, var(--text-primary) 18%, transparent);
    border-radius: 6px;
    padding: 4px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    z-index: 2;
}
.api-code-pre {
    margin: 0;
    padding: 14px 16px 14px 16px;
    padding-right: 56px;
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    font-size: var(--fs-xs);
    color: var(--text-primary);
    line-height: 1.55;
    overflow: auto;
    white-space: pre;
}

.api-inline-code {
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
    color: var(--text-primary);
    font-size: 0.92em;
    background: color-mix(in srgb, var(--text-primary) 7%, transparent);
    padding: 1px 5px;
    border-radius: 3px;
}
`;
