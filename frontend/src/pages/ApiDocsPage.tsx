/**
 * ApiDocsPage — публичная документация по REST API Фрейма.
 *
 * Доступ к API — Pro tier (создание ключей через /profile).
 * Страница сама по себе публичная — её могут читать гости (для оценки фичи).
 * Auth не требуется чтобы видеть docs, только чтобы создать ключ.
 *
 * Содержание:
 *   1. Intro + ссылка на /profile для создания ключа
 *   2. Authentication (X-API-Key header)
 *   3. Rate limits
 *   4. 6 endpoints с примерами на curl/Python/JS
 *   5. Error format
 */
import { Fragment, useState } from 'react';
import { Link } from 'react-router-dom';
import { Key, ArrowLeft, Copy, Check } from 'lucide-react';

// Базовый URL для примеров — берём origin страницы, так юзер
// автоматически видит https://таймфрейм.рф/api/v1/... вместо абстрактного <BASE>.
const BASE_URL = typeof window !== 'undefined'
    ? window.location.origin
    : 'https://xn--80aklbnczmv.xn--p1ai';

interface EndpointDef {
    id: string;
    method: string;
    path: string;
    title: string;
    description: string;
    params: { name: string; type: string; required: boolean; description: string }[];
    exampleResponse: string;
}

const ENDPOINTS: EndpointDef[] = [
    {
        id: 'heatmap',
        method: 'GET',
        path: '/api/v1/public/heatmap',
        title: 'Карта рынка',
        description: 'Снапшот всех акций: цена, изменение за разные периоды, капитализация. Источник — `mv_heatmap_stocks` (обновляется ежедневно).',
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
        id: 'breadth',
        method: 'GET',
        path: '/api/v1/public/breadth/current',
        title: 'Сила рынка — текущий снимок',
        description: 'Процент акций торгующихся выше выбранной EMA. Классический индикатор широты рынка.',
        params: [
            { name: 'ema', type: 'int', required: false, description: 'Период EMA (10–500). По умолчанию 200.' },
            { name: 'universe', type: 'string', required: false, description: '`imoex` / `all` / `imoex_usd` / `all_usd`. По умолчанию `imoex`.' },
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
        id: 'instruments',
        method: 'GET',
        path: '/api/v1/public/instruments',
        title: 'Список инструментов',
        description: 'Все инструменты доступные на Фрейме — акции и фьючерсы с тикерами и группами.',
        params: [
            { name: 'type', type: 'string', required: false, description: '`stock` или `futures`. По умолчанию все.' },
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
    {
        id: 'oi',
        method: 'GET',
        path: '/api/v1/public/oi/current',
        title: 'Открытый интерес — последняя точка',
        description: 'Текущий открытый интерес по фьючерсу с разбивкой long/short по физлицам или юрлицам.',
        params: [
            { name: 'instrument', type: 'string', required: true, description: 'Sectype фьючерса (например `SR` для Сбербанка).' },
            { name: 'clgroup', type: 'string', required: false, description: '`YUR` (юрлица) или `FIZ` (физлица). По умолчанию `YUR`.' },
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
        id: 'funds',
        method: 'GET',
        path: '/api/v1/public/funds/categories',
        title: 'БПИФы — категории и тикеры',
        description: 'Список всех БПИФов которые отслеживает Фрейм, с категориями и подкатегориями.',
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
        id: 'seasonality',
        method: 'GET',
        path: '/api/v1/public/seasonality',
        title: 'Сезонность — дневные свечи для расчёта',
        description: 'Daily candles для тикера за указанный период — для самостоятельного расчёта сезонности или других метрик.',
        params: [
            { name: 'ticker', type: 'string', required: true, description: 'Тикер акции (например `SBER`).' },
            { name: 'days', type: 'int', required: false, description: 'Глубина истории в днях (30–15000). По умолчанию 3650 (10 лет).' },
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
];

/** Парсит `текст` в JSX, заменяя `code` фрагменты на <code>. */
function renderWithBackticks(text: string): React.ReactNode {
    const parts = text.split(/`([^`]+)`/);
    return parts.map((part, i) =>
        i % 2 === 1 ? (
            <code
                key={i}
                style={{
                    fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                    color: 'var(--text-primary)',
                    fontSize: '0.92em',
                }}
            >
                {part}
            </code>
        ) : (
            <Fragment key={i}>{part}</Fragment>
        )
    );
}

function CodeBlock({ code, language }: { code: string; language?: string }) {
    const [copied, setCopied] = useState(false);
    const handleCopy = () => {
        navigator.clipboard?.writeText(code).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
        });
    };
    return (
        <div
            style={{
                position: 'relative',
                marginTop: 8,
                marginBottom: 8,
                background: 'var(--bg-secondary)',
                border: '1px solid color-mix(in srgb, var(--text-primary) 12%, transparent)',
                borderRadius: 8,
                overflow: 'hidden',
            }}
        >
            {language && (
                <div
                    style={{
                        position: 'absolute',
                        top: 8,
                        right: 36,
                        fontSize: 'var(--fs-2xs)',
                        color: 'var(--text-muted)',
                        textTransform: 'uppercase',
                        letterSpacing: '0.04em',
                        fontWeight: 600,
                    }}
                >
                    {language}
                </div>
            )}
            <button
                onClick={handleCopy}
                title="Копировать"
                style={{
                    position: 'absolute',
                    top: 6,
                    right: 6,
                    background: 'var(--bg-primary)',
                    border: '1px solid color-mix(in srgb, var(--text-primary) 18%, transparent)',
                    borderRadius: 6,
                    padding: 4,
                    cursor: 'pointer',
                    color: copied ? 'var(--success, #2dd478)' : 'var(--text-secondary)',
                    display: 'inline-flex',
                    alignItems: 'center',
                }}
            >
                {copied ? <Check size={12} /> : <Copy size={12} />}
            </button>
            <pre
                style={{
                    margin: 0,
                    padding: '14px 16px',
                    paddingRight: 56,
                    fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                    fontSize: 'var(--fs-xs)',
                    color: 'var(--text-primary)',
                    lineHeight: 1.55,
                    overflow: 'auto',
                    whiteSpace: 'pre',
                }}
            >
                <code>{code}</code>
            </pre>
        </div>
    );
}

function MethodBadge({ method }: { method: string }) {
    const color =
        method === 'GET' ? 'var(--accent)'
        : method === 'POST' ? 'var(--success, #2dd478)'
        : method === 'DELETE' ? 'var(--danger, #ef4444)'
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

function EndpointSection({ ep }: { ep: EndpointDef }) {
    const fullUrl = `${BASE_URL}${ep.path}`;
    const exampleParams =
        ep.params.length > 0
            ? '?' + ep.params
                  .filter((p) => p.required || p.name === 'ema' || p.name === 'ticker' || p.name === 'instrument')
                  .map((p) => {
                      if (p.name === 'ticker') return 'ticker=SBER';
                      if (p.name === 'instrument') return 'instrument=SR';
                      if (p.name === 'ema') return 'ema=200';
                      if (p.name === 'days') return 'days=3650';
                      if (p.name === 'universe') return 'universe=imoex';
                      if (p.name === 'clgroup') return 'clgroup=YUR';
                      if (p.name === 'type') return 'type=stock';
                      return `${p.name}=`;
                  })
                  .join('&')
            : '';
    const exampleUrl = `${fullUrl}${exampleParams}`;
    const curlExample = `curl -H "X-API-Key: pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \\
  "${exampleUrl}"`;
    const pythonExample = `import requests

API_KEY = "pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
r = requests.get(
    "${exampleUrl}",
    headers={"X-API-Key": API_KEY},
    timeout=30,
)
r.raise_for_status()
data = r.json()
print(data)`;
    const jsExample = `const API_KEY = "pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

const res = await fetch(
    "${exampleUrl}",
    { headers: { "X-API-Key": API_KEY } }
);
const data = await res.json();
console.log(data);`;

    return (
        <section
            id={ep.id}
            style={{
                marginTop: 40,
                paddingTop: 24,
                borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
            }}
        >
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 6 }}>
                <MethodBadge method={ep.method} />
                <code
                    style={{
                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--text-primary)',
                        fontWeight: 600,
                    }}
                >
                    {ep.path}
                </code>
            </div>
            <h2
                style={{
                    fontSize: 'var(--fs-xl)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    marginTop: 8,
                    marginBottom: 6,
                }}
            >
                {ep.title}
            </h2>
            <p
                style={{
                    fontSize: 'var(--fs-sm)',
                    color: 'var(--text-secondary)',
                    lineHeight: 1.55,
                    marginBottom: 12,
                }}
            >
                {renderWithBackticks(ep.description)}
            </p>

            {ep.params.length > 0 && (
                <div style={{ marginTop: 12 }}>
                    <h3
                        style={{
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            color: 'var(--text-primary)',
                            marginBottom: 6,
                            textTransform: 'uppercase',
                            letterSpacing: '0.04em',
                        }}
                    >
                        Параметры
                    </h3>
                    <div
                        style={{
                            display: 'grid',
                            gap: 6,
                            fontSize: 'var(--fs-sm)',
                            color: 'var(--text-secondary)',
                        }}
                    >
                        {ep.params.map((p) => (
                            <div key={p.name}>
                                <code
                                    style={{
                                        fontFamily: 'ui-monospace, "SF Mono", Menlo, monospace',
                                        color: 'var(--text-primary)',
                                        fontWeight: 600,
                                    }}
                                >
                                    {p.name}
                                </code>
                                {' '}
                                <span style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)' }}>
                                    {p.type}
                                    {p.required ? ' · обязательный' : ' · опциональный'}
                                </span>
                                {' — '}
                                {renderWithBackticks(p.description)}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            <h3
                style={{
                    fontSize: 'var(--fs-sm)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    marginTop: 16,
                    marginBottom: 4,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                }}
            >
                Примеры
            </h3>
            <CodeBlock code={curlExample} language="curl" />
            <CodeBlock code={pythonExample} language="python" />
            <CodeBlock code={jsExample} language="javascript" />

            <h3
                style={{
                    fontSize: 'var(--fs-sm)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    marginTop: 16,
                    marginBottom: 4,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                }}
            >
                Ответ
            </h3>
            <CodeBlock code={ep.exampleResponse} language="json" />
        </section>
    );
}

export default function ApiDocsPage() {
    return (
        <div className="max-w-3xl mx-auto px-4 md:px-6 py-6 md:py-8">
            <Link
                to="/profile"
                className="inline-flex items-center gap-2 text-sm mb-6"
                style={{ color: 'var(--text-secondary)' }}
            >
                <ArrowLeft size={14} />К профилю
            </Link>

            {/* Header */}
            <header className="flex items-center gap-4 mb-8">
                <div
                    className="flex items-center justify-center flex-shrink-0"
                    style={{
                        width: 48,
                        height: 48,
                        borderRadius: 8,
                        background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
                        color: 'var(--accent)',
                    }}
                >
                    <Key size={24} strokeWidth={1.8} />
                </div>
                <h1
                    className="text-2xl md:text-3xl font-bold"
                    style={{ color: 'var(--text-primary)', letterSpacing: '-0.015em' }}
                >
                    API Фрейма
                </h1>
            </header>

            {/* Intro */}
            <section style={{ marginBottom: 24 }}>
                <p
                    style={{
                        fontSize: 'var(--fs-base)',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.6,
                    }}
                >
                    Программный доступ к данным MOEX через REST API. Для торговых ботов,
                    скриптов аналитики и личных дашбордов. Возвращает JSON, аутентификация
                    через персональный ключ <code style={{ fontFamily: 'ui-monospace, monospace' }}>pk_live_…</code>.
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
                    </Link>.
                </p>
            </section>

            {/* Authentication */}
            <section
                style={{
                    marginTop: 24,
                    paddingTop: 20,
                    borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                }}
            >
                <h2
                    style={{
                        fontSize: 'var(--fs-xl)',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                        marginBottom: 8,
                    }}
                >
                    Аутентификация
                </h2>
                <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', lineHeight: 1.55 }}>
                    Передавайте ключ в заголовке <code style={{ fontFamily: 'ui-monospace, monospace' }}>X-API-Key</code>{' '}
                    или как <code style={{ fontFamily: 'ui-monospace, monospace' }}>Authorization: Bearer</code>.
                    Ключи генерируются один раз — мы храним только их хеш, восстановить нельзя.
                </p>
                <CodeBlock
                    language="http"
                    code={`X-API-Key: pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# или

Authorization: Bearer pk_live_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`}
                />
                <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', marginTop: 8 }}>
                    Если ключ скомпрометирован — отзовите его в личном кабинете и создайте новый.
                    Максимум 10 активных ключей на аккаунт.
                </p>
            </section>

            {/* Rate limits */}
            <section
                style={{
                    marginTop: 32,
                    paddingTop: 20,
                    borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                }}
            >
                <h2
                    style={{
                        fontSize: 'var(--fs-xl)',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                        marginBottom: 8,
                    }}
                >
                    Лимиты
                </h2>
                <ul
                    style={{
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.6,
                        margin: 0,
                        paddingLeft: 20,
                    }}
                >
                    <li>
                        <strong style={{ color: 'var(--text-primary)' }}>60 запросов / минуту</strong>{' '}
                        на API-ключ. При превышении — <code style={{ fontFamily: 'ui-monospace, monospace' }}>429
                        Too Many Requests</code> с заголовком <code style={{ fontFamily: 'ui-monospace, monospace' }}>Retry-After: 60</code>.
                    </li>
                    <li>
                        <strong style={{ color: 'var(--text-primary)' }}>30 запросов / секунду</strong> на IP —
                        общий лимит на уровне nginx, действует для всего сайта.
                    </li>
                    <li>
                        Все ответы сжаты gzip — экономия трафика до 80%.
                    </li>
                </ul>
            </section>

            {/* Errors */}
            <section
                style={{
                    marginTop: 32,
                    paddingTop: 20,
                    borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                }}
            >
                <h2
                    style={{
                        fontSize: 'var(--fs-xl)',
                        fontWeight: 700,
                        color: 'var(--text-primary)',
                        marginBottom: 8,
                    }}
                >
                    Ошибки
                </h2>
                <p style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', lineHeight: 1.55, marginBottom: 8 }}>
                    Все ошибки возвращаются в формате{' '}
                    <code style={{ fontFamily: 'ui-monospace, monospace' }}>{'{"detail": "..."}'}</code>{' '}
                    с соответствующим HTTP-кодом:
                </p>
                <ul
                    style={{
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.6,
                        margin: 0,
                        paddingLeft: 20,
                    }}
                >
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>400</code> — некорректные параметры
                    </li>
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>401</code> — ключ отсутствует, отозван или невалиден
                    </li>
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>403</code> — у пользователя не Pro tier
                    </li>
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>404</code> — данные не найдены
                    </li>
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>429</code> — превышен лимит (Retry-After в заголовке)
                    </li>
                    <li>
                        <code style={{ fontFamily: 'ui-monospace, monospace' }}>500</code> — внутренняя ошибка сервера
                    </li>
                </ul>
            </section>

            {/* Endpoints */}
            <h2
                style={{
                    fontSize: 'var(--fs-2xl)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    marginTop: 48,
                    marginBottom: 8,
                }}
            >
                Эндпоинты
            </h2>
            {ENDPOINTS.map((ep) => (
                <EndpointSection key={ep.id} ep={ep} />
            ))}

            {/* Footer note */}
            <section
                style={{
                    marginTop: 48,
                    paddingTop: 20,
                    borderTop: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)',
                }}
            >
                <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', lineHeight: 1.6 }}>
                    API находится в стабильной версии <code>v1</code>. Breaking changes выпускаются
                    в новой версии <code>v2</code> с минимум 30-дневным депрекацией старой.
                    Вопросы и feature requests — на{' '}
                    <a
                        href="mailto:frameinfo@mail.ru"
                        style={{ color: 'var(--accent)', textDecoration: 'underline' }}
                    >
                        frameinfo@mail.ru
                    </a>.
                </p>
            </section>
        </div>
    );
}
