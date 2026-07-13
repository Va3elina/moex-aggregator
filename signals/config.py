"""Configuration for signal-engine.

Загружается при импорте: тянет .env из корня проекта (как api/database.py).
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ─── Telegram ────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SIGNALS_CHANNEL_ID = int(os.getenv("SIGNALS_CHANNEL_ID", "0"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

# ─── Detection ──────────────────────────────────────────
LOOKBACK_DAYS = 60           # окно для rolling z-score
Z_THRESHOLD = 2.5            # |z| > этого → сигнал
COOLDOWN_HOURS = 24          # не чаще одного сигнала в сутки на (asset, signal_type)
MIN_HISTORY_DAYS = 30        # минимум точек для статистически значимого z

# ─── Render (matplotlib chart) ──────────────────────────
CHART_PERIOD_DAYS = 365      # период, отображаемый на графике
CHART_WIDTH = 1280
CHART_HEIGHT = 720
SITE_URL = "таймфрейм.рф"

# editorial-dark палитра (синхронизировано с frontend/src/index.css)
COLOR_BG = "#0a0a0a"         # paper-bg
COLOR_FG = "#f4f4f4"         # text primary
COLOR_MUTED = "#888"         # text muted
COLOR_GRID = "#1f1f1f"
COLOR_PRICE = "#f4f4f4"      # белая линия цены
COLOR_OI = "#FF5C2B"         # pumpkin accent — OI net positions
COLOR_MARKER = "#FF5C2B"     # красный кружок на последней точке

# ─── Assets ─────────────────────────────────────────────
# 65 тикеров фьючерсов MOEX с OI данными.
# Источник: OI/fetch_oi_5min_realtime.py:ALGOPACK_OI_TICKERS.
# Дублируем здесь чтобы не тянуть orchestrator-зависимости при импорте.
OI_ASSETS = [
    "CR", "CNYRUBF", "Si", "Eu", "IB", "VB", "USDRUBF", "GZ", "IMOEXF", "RB",
    "CC", "GL", "GLDRUBF", "NA", "NR", "ED", "GK", "SV", "SS", "X5",
    "MX", "MM", "NG", "GD", "SR", "SF", "GAZPF", "MN", "YD", "BR",
    "SE", "TN", "PT", "AF", "KC", "FF", "AL", "EURRUBF", "SBERF", "CE",
    "HS", "NK", "RI", "RL", "LK", "UC", "PD", "NM", "MC", "RM",
    "RN", "SP", "SN", "ME", "HY", "BM", "TT", "OJ", "MG", "W4",
    "DX", "CH", "MY", "VI", "AU",
]

# ─── Публичная лента аномалий (сайт: тосты + колокол) ───────────────────────
# Маркет-вайд скан (signals/anomaly_scan.py) гоняет ТЕ ЖЕ detector-функции, что и
# личные алерты (compute_position_atr / compute_fund_flow_atr), по всем активам и
# пишет аномалии ≥ порога в таблицу `anomalies` (scope='public'). Видимость НЕ
# делится по силе/тарифу — все видят одну ленту (решение Вадима), порог единый.
PUBLIC_RATIO_MIN = 3.0       # ×N к обычному дневному шагу — порог публичной аномалии
# Материальность фондов: страховка от шума мелких категорий («дырка→×14» на юане).
# Доля СЧА категории, ниже которой ATR считаем «замёрзшим» и аномалию не публикуем.
# 0 = выключено — стартуем без неё («пробуем ×3, смотрим объём»), включаем (напр.
# 0.0002), если мелкие категории зашумят.
PUBLIC_FUND_MIN_NAV_FRAC = 0.0

# ─── Content-пайплайн («завод постов») ──────────────────────────────────────
CALENDAR_LOOKAHEAD_DAYS = 14   # на сколько дней вперёд смотрит MOEX-календарь
CONTENT_WINDOW_HOURS = 6       # окно кросс-издательской конвергенции RSS
CONTENT_WORD_OVERLAP_MIN = 3   # минимум значимых слов пересечения между источниками
CONTENT_IMPORTANCE_MIN = 3     # порог значимости (Шаг А) для продолжения в pending
CONTENT_PENDING_DAYS = 5       # окно ожидания подтверждения данными (Шаг Б)
