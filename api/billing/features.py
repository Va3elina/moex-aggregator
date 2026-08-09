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

# Сезонность — 10 крупных акций (spot тикеры, не фьючерсные)
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
    "heatmap": {
        "free":  {"allowed_modes": ["imoex"],         "max_history_days": None},
        "basic": {"allowed_modes": ["imoex", "all"],  "max_history_days": None},
        "pro":   {"allowed_modes": ["imoex", "all"],  "max_history_days": None},
    },

    # ───────────────────────────────────────────────────────────────
    # 2. Открытый интерес (/open-interest)
    # ───────────────────────────────────────────────────────────────
    "open_interest": {
        "free": {
            "assets_whitelist": None,                # все активы открыты на Free (без whitelist'а)
            "allowed_intervals": [24, 60],          # daily + hourly; 5min только на Pro
            "max_history_days": 365 * 5,            # 5 лет
            "data_delay_hours": 24,                 # задержка 24 часа
            "clgroups": ["FIZ", "YUR"],              # backend отдаёт оба (нужно embed-виджету)
            "settings_customizable": False,          # но в UI срез залочен на дефолт (FIZ · Объём · Чистая); смена → Basic
        },
        "basic": {
            "assets_whitelist": None,                # все 65
            "allowed_intervals": [24, 60],
            "max_history_days": 365 * 10,            # 10 лет
            "data_delay_hours": 0,
            "clgroups": ["FIZ", "YUR"],
            "settings_customizable": True,           # можно свободно менять срез
        },
        "pro": {
            "assets_whitelist": None,
            "allowed_intervals": [5, 24, 60],        # + 5min
            "max_history_days": None,                # вся история
            "data_delay_hours": 0,
            "clgroups": ["FIZ", "YUR"],
            "settings_customizable": True,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 3. Деньги в фондах (/funds-money)
    # ───────────────────────────────────────────────────────────────
    "funds_money": {
        "free": {
            "tickers_whitelist": None,                 # все фонды открыты на Free (без whitelist'а)
            "allowed_timeframes": None,                # все ТФ, включая дневной (притоки-оттоки открыты)
            "max_history_days": 365 * 3,               # 3 года (период «Всё» → Basic/Pro)
        },
        "basic": {
            "tickers_whitelist": None,
            "allowed_timeframes": None,                # все
            "max_history_days": None,
        },
        "pro": {
            "tickers_whitelist": None,
            "allowed_timeframes": None,
            "max_history_days": None,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # Сделки фондов (/fund-trades) — пилот delayed-data freemium.
    # snapshot_delay = на сколько снапшотов свежести назад видит тир (0 = свежий
    # срез). Free/гость = 1 (свежая месячная выборка «что фонды купили» — по
    # подписке); любой платный тир (Basic/Pro) = realtime. Первый раздел, на
    # котором обкатываем модель «видно всё, но с задержкой»; дальше — на весь проект.
    # ───────────────────────────────────────────────────────────────
    "fund_trades": {
        "free":  {"snapshot_delay": 1},
        "basic": {"snapshot_delay": 0},
        "pro":   {"snapshot_delay": 0},
    },

    # ───────────────────────────────────────────────────────────────
    # 4. Сила рынка (/strength)
    # ───────────────────────────────────────────────────────────────
    # Вселенные в БД breadth_history: all / all_usd / imoex / imoex_usd
    "strength": {
        "free": {
            "universes": ["imoex"],
            "usd_mode": False,
            "max_history_days": 365,                   # 1 год
        },
        "basic": {
            "universes": ["all", "all_usd", "imoex", "imoex_usd"],
            "usd_mode": True,
            "max_history_days": 365 * 10,
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
    # canUseMode на фронте проверяется для chartType 'histogram' и для 4
    # режимов гистограммы: intraday / weekday / monthday / monthly.
    #   Free  — только «Годовая» (histogram закрыт целиком).
    #   Basic — histogram + weekday/monthday/monthly (без intraday).
    #   Pro   — без ограничений (allowed_modes=None ⇒ canUseMode всегда true).
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
            "assets_whitelist": FREE_SEASONALITY_ASSETS,   # 10 крупных
            "allowed_modes": ["yearly"],
            "filter_no_outliers": False,
            "filter_no_dividends": False,
        },
        "basic": {
            "assets_whitelist": None,
            "allowed_modes": ["histogram", "weekday", "monthday", "monthly", "yearly"],  # без intraday
            "filter_no_outliers": False,
            "filter_no_dividends": False,
        },
        "pro": {
            "assets_whitelist": None,
            "allowed_modes": None,   # без ограничений — все режимы сезонности
            "filter_no_outliers": True,
            "filter_no_dividends": True,
        },
    },

    # ───────────────────────────────────────────────────────────────
    # 7. Потоки участников биржи (/cbr-flows)
    # ───────────────────────────────────────────────────────────────
    "cbr_flows": {
        "free": {
            "data_delay_hours": 24,
            "max_history_days": None,            # период открыт полностью — все ТФ на free
            "category_filters_enabled": False,   # категории нельзя скрывать вручную
            # Free видит только «розничный» срез участников — per-type, т.к.
            # набор категорий у акций/ОФЗ и валюты разный (напр. НФО закрыт на
            # акциях, но открыт на валюте). Остальных участников бэкенд вырезает
            # из данных и помечает locked → апселл на basic.
            "categories_whitelist": {
                "stocks": [
                    "Физические лица",
                    "Доверительное управление",
                    "Нерезиденты",
                ],
                "ofz": [
                    "Физические лица",
                    "Доверительное управление",
                    "Нерезиденты",
                ],
                "fx": [
                    "Физические лица",
                    "Клиенты российских кредитных организаций",
                    "НФО",
                    "Банк России",
                ],
            },
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
        "watermark_on_export": True,    # PNG с водяным знаком framedata.ru
        "csv_export": False,            # Excel/CSV экспорт
        "api_access": False,            # /api/v1/public/* с ключами
        "fund_trades_access": True,     # /fund-trades — открыт для ВСЕХ тиров (что покупают/продают БПИФ)
        "telegram_alerts_quota": 0,     # лимит индивидуальных алертов
    },
    "basic": {
        "watermark_on_export": False,
        "csv_export": False,
        "api_access": False,
        "fund_trades_access": True,
        "telegram_alerts_quota": 20,    # 20 алертов
    },
    "pro": {
        "watermark_on_export": False,
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
