"""
Tier × Indicator × Feature-flags — единая матрица доступа.

Источник истины для tier-based gating на бэкенде и frontend.
  - Backend импортирует напрямую (роутеры используют `get_indicator_limits()`)
  - Frontend получает через GET /api/billing/features при login → кэширует в AuthContext

При изменении этого файла:
  1. Перебилдить frontend (он подхватит матрицу через endpoint при login)
  2. Бэкенд cначала перезагружается (роутеры используют новые лимиты)

Любая новая Pro-фича добавляется здесь, не разбрасывается по коду.
"""
from __future__ import annotations
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════════
# Whitelist'ы крупных активов для Free (хардкод по спецификации 2026-05-20)
# ═══════════════════════════════════════════════════════════════════════════════

# Открытый интерес — 12 крупных фьючерсов.
# NB: с 2026-07 Free-тариф открыт для ВСЕХ активов OI (assets_whitelist=None),
# этот список больше не гейтит доступ. Оставлен для справки/возможного переиспользования.
FREE_OI_ASSETS: list[str] = [
    "SR",        # Сбербанк
    "GZ",        # Газпром
    "RN",        # Роснефть
    "LK",        # Лукойл
    "VB",        # ВТБ
    "GK",        # Норильский никель
    "MX",        # Индекс МосБиржи
    "IMOEXF",    # Индекс МосБиржи (вечный)
    "USDRUBF",   # USD/RUB вечный
    "CNYRUBF",   # CNY/RUB вечный
    "GD",        # Золото
    "BR",        # Brent
]

# Сезонность — 10 крупных акций (spot тикеры, не фьючерсные).
# NB: с 2026-08 Free-тариф открыт для ВСЕХ активов сезонности (assets_whitelist=None),
# этот список больше не гейтит доступ. Оставлен для справки.
FREE_SEASONALITY_ASSETS: list[str] = [
    "SBER",         # Сбербанк
    "GAZP",         # Газпром
    "ROSN",         # Роснефть
    "LKOH",         # Лукойл
    "GMKN",         # Норникель
    "VTBR",         # ВТБ
    "ALRS",         # Алроса
    "NVTK",         # Новатэк
    "IMOEX",        # Индекс
    "USD000UTSTOM", # USD/RUB
]

# Деньги в фондах — топ-2 в каждой из 4 категорий = 8 тикеров.
# NB: с 2026-07 Free-тариф открыт для ВСЕХ фондов (tickers_whitelist=None),
# этот список больше не гейтит доступ. Оставлен для справки.
FREE_FUNDS_TICKERS: list[str] = [
    # money_market
    "LQDT", "AKMM",
    # bonds
    "TOFZ", "SBLB",
    # stocks
    "TMOS", "SBMX",
    # gold
    "GOLD", "TGLD",
]


# ═══════════════════════════════════════════════════════════════════════════════
# Per-indicator feature-flags по tier
# ═══════════════════════════════════════════════════════════════════════════════

INDICATOR_FEATURES: dict[str, dict[str, dict]] = {

    # ───────────────────────────────────────────────────────────────
    # 1. Карта рынка (/heatmap)
    # ───────────────────────────────────────────────────────────────
    # NB: с 2026-08-10 карта полностью бесплатна — оба режима (IMOEX и «Все
    # акции») на всех тирах. Тиры оставлены отдельными строками, чтобы можно
    # было вернуть разграничение без перекройки структуры матрицы.
    "heatmap": {
        "free":  {"allowed_modes": ["imoex", "all"],  "max_history_days": None},
        "basic": {"allowed_modes": ["imoex", "all"],  "max_history_days": None},
        "pro":   {"allowed_modes": ["imoex", "all"],  "max_history_days": None},
    },

    # ───────────────────────────────────────────────────────────────
    # 2. Открытый интерес (/open-interest)
    # ───────────────────────────────────────────────────────────────
    # Пересмотр 2026-08-10 (владелец): сняты лимиты по таймфреймам, глубине
    # истории и настройкам среза — на всех тирах. За Basic остаются ровно два
    # среза: категория «Юрлица» (clgroup_yur) и показатель «Число трейдеров»
    # (metric_traders) + отсутствие задержки данных. Прежний булевый
    # settings_customizable (весь срез целиком) заменён этими двумя флагами.
    "open_interest": {
        "free": {
            "assets_whitelist": None,                # все активы открыты на Free (без whitelist'а)
            "allowed_intervals": [5, 24, 60],        # все ТФ, включая 5min
            "max_history_days": None,                # вся история
            "data_delay_hours": 24,                 # задержка 24 часа
            "clgroups": ["FIZ", "YUR"],              # backend отдаёт оба (нужно embed-виджету)
            "clgroup_yur": False,                    # срез «Юрлица» в UI — с Basic
            "metric_traders": False,                 # показатель «Число трейдеров» — с Basic
        },
        "basic": {
            "assets_whitelist": None,                # все 65
            "allowed_intervals": [5, 24, 60],
            "max_history_days": None,
            "data_delay_hours": 0,
            "clgroups": ["FIZ", "YUR"],
            "clgroup_yur": True,
            "metric_traders": True,
        },
        "pro": {
            "assets_whitelist": None,
            "allowed_intervals": [5, 24, 60],
            "max_history_days": None,
            "data_delay_hours": 0,
            "clgroups": ["FIZ", "YUR"],
            "clgroup_yur": True,
            "metric_traders": True,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 2b. Скринер сигналов ОИ (/oi, вкладка «Скринер сигналов» + embed)
    # ───────────────────────────────────────────────────────────────
    # ⚠️ РАЗВОРОТ РЕШЕНИЯ 2026-08-09 (владелец). Раньше лента была открыта всем
    # как «публичная агрегированная статистика, не премиум-данные» — эта логика
    # записана в docstring GET /api/oi/screener и намеренно там оставлена, чтобы
    # видеть, почему открывали. Теперь скринер закрыт для гостя и free: это
    # готовый торговый вывод (что именно и насколько резко сдвинулось сегодня по
    # всему рынку разом), а не сырой ряд, — и он же ядро Telegram-алертов.
    # Гейт булевый (`open`), а не whitelist/задержка: половина ленты бессмысленна,
    # а лента вчерашним днём — это уже не сигнал.
    "oi_screener": {
        "free":  {"open": False},
        "basic": {"open": True},
        "pro":   {"open": True},
    },

    # ───────────────────────────────────────────────────────────────
    # 3. Деньги в фондах (/funds-money)
    # ───────────────────────────────────────────────────────────────
    # fund_picker (2026-08-09) — выбор КОНКРЕТНЫХ фондов внутри категории
    # (таблетка «Фонды: …» → модалка со списком). Free/гость видит категорию
    # целиком, но собрать из неё свою подвыборку не может. Бэкенд игнорирует
    # `fund_ids` у таких тиров — иначе гейт обходится подстановкой параметра.
    "funds_money": {
        "free": {
            "tickers_whitelist": None,                 # все фонды открыты на Free (без whitelist'а)
            "allowed_timeframes": None,                # все ТФ, включая дневной (притоки-оттоки открыты)
            "max_history_days": None,                  # вся история, включая период «Всё» (2026-08-10)
            "fund_picker": False,                      # выбор подмножества фондов — с Basic
        },
        "basic": {
            "tickers_whitelist": None,
            "allowed_timeframes": None,                # все
            "max_history_days": None,
            "fund_picker": True,
        },
        "pro": {
            "tickers_whitelist": None,
            "allowed_timeframes": None,
            "max_history_days": None,
            "fund_picker": True,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # Сделки фондов (/fund-trades) — пилот delayed-data freemium.
    # snapshot_delay = на сколько снапшотов свежести назад видит тир (0 = свежий
    # срез). Free/гость = 1 (свежая месячная выборка «что фонды купили» — по
    # подписке); любой платный тир (Basic/Pro) = realtime. Первый раздел, на
    # котором обкатываем модель «видно всё, но с задержкой»; дальше — на весь проект.
    # ───────────────────────────────────────────────────────────────
    # portfolio_snapshot_delay (2026-08-09) — ОТДЕЛЬНАЯ задержка для вкладки
    # «Общий портфель»: там свежесть не режем никому (решение владельца), = 0.
    # Одним числом на индикатор разные ручки не различить, поэтому ключ второй,
    # а не общий: /portfolio и портфельный вызов /movers (scope=portfolio) читают
    # его, все остальные ручки раздела продолжают читать snapshot_delay. Оба
    # ключа энфорсит бэкенд (api/routers/fund_trades.py: _snapshot_cutoff(key=…)) —
    # фронт задержку не вычисляет, только рисует по возвращённому snapshot_cutoff.
    #
    # fund_picker (2026-08-09) — выбор КОНКРЕТНЫХ фондов в «Общем портфеле» и
    # «По бумаге». Free/гость смотрит набор целиком; собрать свой пул (одна УК,
    # только крупные, без индексных) — с Basic. Витрины гейт не касается.
    # company_snapshot_delay (2026-08-10) — ОТДЕЛЬНАЯ задержка для вкладки
    # «По бумаге» (потоки по компании): свежесть не режем никому (решение
    # владельца), = 0. Читают её /company-flows и /company-weights
    # (api/routers/fund_trades.py: _snapshot_cutoff(key=COMPANY_DELAY_KEY)),
    # остальные ручки раздела продолжают читать snapshot_delay.
    #
    # showcase_snapshot_delay (2026-08-10) — задержка ВИТРИНЫ (вкладка «Витрина»:
    # каталог фондов со СЧА, доходностью, составом + карточка фонда, которая
    # открывается кликом из него, и её drill-down по бумаге). Тоже 0 всем:
    # витрина — верхняя воронка раздела. Ключ читают /funds, /fund/{ticker},
    # /asset/{name}, /asset-history/{ticker} (SHOWCASE_DELAY_KEY). Карточку
    # включили в витрину сознательно: иначе список и карточка разъезжались бы
    # по датам снапшота. Общий snapshot_delay остался только у скрытых вкладок
    # «Движения»/«Снапшот» (/movers scope=movers, /snapshot(s)/{ticker}).
    #
    # custom_range (2026-08-10) — кнопка-календарь «свой период» (произвольный
    # диапазон месяцев вместо пресетов 1М/6М/1Г) в «Сделках» Общего портфеля и в
    # карточке фонда. Free/гость считает по пресетам; произвольный диапазон — с
    # Basic. Бэкенд гасит параметры from/to у таких тиров (/movers, /fund/{ticker}),
    # иначе гейт обходится правкой URL.
    "fund_trades": {
        "free":  {"snapshot_delay": 1, "portfolio_snapshot_delay": 0, "company_snapshot_delay": 0,
                  "showcase_snapshot_delay": 0, "fund_picker": False, "custom_range": False},
        "basic": {"snapshot_delay": 0, "portfolio_snapshot_delay": 0, "company_snapshot_delay": 0,
                  "showcase_snapshot_delay": 0, "fund_picker": True, "custom_range": True},
        "pro":   {"snapshot_delay": 0, "portfolio_snapshot_delay": 0, "company_snapshot_delay": 0,
                  "showcase_snapshot_delay": 0, "fund_picker": True, "custom_range": True},
    },

    # ───────────────────────────────────────────────────────────────
    # 4. Сила рынка (/strength)
    # ───────────────────────────────────────────────────────────────
    # Вселенные в БД breadth_history: all / all_usd / imoex / imoex_usd
    # NB: с 2026-08 индикатор полностью бесплатен — все вселенные, USD-режим и
    # вся история на всех тирах (гость мапится на free). Раньше free видел только
    # рублёвый IMOEX за год; сняли, т.к. «Сила рынка» — верхняя воронка, а не то,
    # за что платят. Тиры оставлены отдельными строками (не схлопнуты), чтобы
    # можно было вернуть разграничение без перекройки структуры матрицы.
    "strength": {
        "free": {
            "universes": ["all", "all_usd", "imoex", "imoex_usd"],
            "usd_mode": True,
            "max_history_days": None,
        },
        "basic": {
            "universes": ["all", "all_usd", "imoex", "imoex_usd"],
            "usd_mode": True,
            "max_history_days": None,
        },
        "pro": {
            "universes": ["all", "all_usd", "imoex", "imoex_usd"],
            "usd_mode": True,
            "max_history_days": None,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 5. Индикатор Баффетта (/buffett)
    # ───────────────────────────────────────────────────────────────
    # ⚠️ modes должен перечислять ВСЕ режимы, которые роутер передаёт в
    # enforce_tier_limits(mode=...) — включая те, которых нет в UI. Режим,
    # отсутствующий здесь, ловит `mode not in allowed` и отдаёт 403 всем тирам,
    # включая Pro. mcftr-m2 фронтом не используется (только API), но в списке
    # обязан быть — иначе эндпоинт умрёт целиком.
    "buffett": {
        # NB: с 2026-07 индикатор полностью бесплатен — все режимы, вся история,
        # кастомные диапазоны на всех тирах (гость мапится на free).
        "free":  {"modes": ["cap-gdp", "cap-m2", "mcftr-m2"], "max_history_days": None, "custom_ranges": True},
        "basic": {"modes": ["cap-gdp", "cap-m2", "mcftr-m2"], "max_history_days": None, "custom_ranges": True},
        "pro":   {"modes": ["cap-gdp", "cap-m2", "mcftr-m2"], "max_history_days": None, "custom_ranges": True},
    },

    # ───────────────────────────────────────────────────────────────
    # 6. Сезонность (/seasonality)
    # ───────────────────────────────────────────────────────────────
    # NB: с 2026-08 базовый индикатор бесплатен — все активы, все режимы
    # (включая intraday) и вся история на всех тирах (гость мапится на free).
    # Раньше free видел только «Годовую» по 10 крупным тикерам, а intraday был
    # Pro-only. Сняли: сезонность — верхняя воронка и SEO-вход, а не то, за что
    # платят. Тиры оставлены отдельными строками (не схлопнуты), чтобы можно
    # было вернуть разграничение без перекройки структуры матрицы. canUseMode
    # на фронте проверяется для chartType 'histogram' и для 4 режимов
    # гистограммы: intraday / weekday / monthday / monthly — при
    # allowed_modes=None он всегда true.
    #
    # 2026-08-24 (владелец): фильтры серий «Без выбросов» (agg_type=median) и
    # «Без дивидендных гэпов» (exclude_dividends) — с Basic. UI на free не
    # меняется: тумблеры видны, попытка включить открывает upgrade-модалку
    # (SeasonalityPage / MobileSeasonalityPage). Бэкенд не 403-ит, а тихо
    # гасит параметры до дефолтов (api/routers/seasonality.py) — как
    # custom_range в fund_trades, иначе гейт обходится правкой URL.
    #
    # Глубина истории у сезонности НЕ ограничена ни на одном тарифе — все видят
    # всю доступную историю (у бесплатных тикеров это ~19 лет, с 2007). Раньше
    # здесь лежало поле max_history_years (free=10), но enforce_tier_limits его
    # никогда не проверял — гейт был мёртвым, и фронт всегда слал iterations=9999.
    # Поле убрано осознанно (решение 2026-07-30): включать его сейчас означало бы
    # забрать у действующих free-юзеров половину глубины. Если понадобится
    # гейтить историю — придётся и добавить проверку в access_control, и заново
    # решить продуктовый вопрос, а не просто вернуть строчку в конфиг.
    "seasonality": {
        "free": {
            "assets_whitelist": None,   # все активы
            "allowed_modes": None,      # без ограничений — все режимы сезонности
            "filter_no_outliers": False,   # «Без выбросов» (median) — с Basic
            "filter_no_dividends": False,  # «Без дивидендных гэпов» — с Basic
        },
        "basic": {
            "assets_whitelist": None,
            "allowed_modes": None,
            "filter_no_outliers": True,
            "filter_no_dividends": True,
        },
        "pro": {
            "assets_whitelist": None,
            "allowed_modes": None,
            "filter_no_outliers": True,
            "filter_no_dividends": True,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 7. Потоки участников биржи (/cbr-flows)
    # ───────────────────────────────────────────────────────────────
    # NB: задержка данных снята для всех тиров 2026-08 (было 24ч на free/госте).
    # Данные ОРФР ЦБ — МЕСЯЧНЫЕ и приезжают ручным ингестом с лагом в недели,
    # так что «минус сутки» ничего не продавало, а бейдж «данные с задержкой»
    # выглядел враньём.
    # 2026-08-10 (владелец): снят и гейт на СОСТАВ участников — free/гость может
    # включать любые категории вручную (categories_whitelist=None,
    # category_filters_enabled=True). При этом ДЕФОЛТНАЯ видимость для первого
    # визита остаётся «розничным» срезом — это фронтовый дефолт
    # (frontend cbrDefaultVisibility.ts), не тарифный гейт: расширенные
    # категории не включаются сами, но доступны в пикере всем.
    "cbr_flows": {
        "free": {
            "data_delay_hours": 0,
            "max_history_days": None,            # период открыт полностью — все ТФ на free
            "category_filters_enabled": True,
            "categories_whitelist": None,        # все категории
        },
        "basic": {
            "data_delay_hours": 0,
            "max_history_days": None,
            "category_filters_enabled": True,
            "categories_whitelist": None,        # все категории
        },
        "pro": {
            "data_delay_hours": 0,
            "max_history_days": None,
            "category_filters_enabled": True,
            "categories_whitelist": None,        # все категории
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 8. Каталог фондов (/funds-catalog) — пока полностью бесплатный
    # ───────────────────────────────────────────────────────────────
    "funds_catalog": {
        "free":  {"open": True},
        "basic": {"open": True},
        "pro":   {"open": True},
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Общие фичи (не привязанные к индикатору)
# ═══════════════════════════════════════════════════════════════════════════════

import os
# KILL-SWITCH: публичный API + CSV-экспорт скрыты до официального запуска
# (конкуренты). Когда выключено (default) — csv_export/api_access=False даже
# для Pro, а роутеры /api/export, /api/keys, /api/v1/public не монтируются (см. main.py).
# Вернуть: env PUBLIC_API_CSV_ENABLED=1 + фронт config/features.ts API_CSV_ENABLED=true.
PUBLIC_API_CSV_ENABLED = os.getenv("PUBLIC_API_CSV_ENABLED", "").lower() in ("1", "true", "yes")

COMMON_FEATURES: dict[str, dict] = {
    "free": {
        "csv_export": False,            # Excel/CSV экспорт
        "api_access": False,            # /api/v1/public/* с ключами
        "fund_trades_access": True,     # /fund-trades — открыт для ВСЕХ тиров (что покупают/продают БПИФ)
        "telegram_alerts_quota": 0,     # лимит индивидуальных алертов
    },
    "basic": {
        "csv_export": False,
        "api_access": False,
        "fund_trades_access": True,
        "telegram_alerts_quota": 20,    # 20 алертов
    },
    "pro": {
        "csv_export": PUBLIC_API_CSV_ENABLED,   # kill-switch: скрыто до запуска
        "api_access": PUBLIC_API_CSV_ENABLED,   # kill-switch: скрыто до запуска
        "fund_trades_access": True,     # smart-money tracking — открыт для всех тиров (2026-07-05, было Pro-only)
        "telegram_alerts_quota": None,  # unlimited
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

VALID_TIERS = ("free", "basic", "pro")


def _normalize_tier(tier: str) -> str:
    """admin → pro доступ; guest → free; unknown → free."""
    if tier == "admin":
        return "pro"
    if tier in VALID_TIERS:
        return tier
    return "free"


def get_indicator_limits(tier: str, indicator: str) -> dict:
    """Лимиты для конкретного (tier, indicator).

    Используется в роутерах: `from api.billing.features import get_indicator_limits`.

    Если indicator не найден — пустой dict (нет ограничений).
    """
    tier = _normalize_tier(tier)
    return INDICATOR_FEATURES.get(indicator, {}).get(tier, {})


def get_common_features(tier: str) -> dict:
    """Общие фичи (watermark, csv, api, alerts) для tier."""
    return COMMON_FEATURES.get(_normalize_tier(tier), COMMON_FEATURES["free"])


def matrix_for_frontend() -> dict:
    """Полная матрица для отдачи через GET /api/billing/features.

    Frontend кэширует в AuthContext и используется во всех TierGate компонентах.
    """
    return {
        "indicators": INDICATOR_FEATURES,
        "common": COMMON_FEATURES,
        "valid_tiers": list(VALID_TIERS),
    }
