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
