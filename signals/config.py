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
# Исходные 65 тикеров фьючерсов MOEX с OI данными (валюта/сырьё/индексы + часть
# акций). Источник: OI/fetch_oi_5min_realtime.py:ALGOPACK_OI_TICKERS. Дублируем
# здесь чтобы не тянуть orchestrator-зависимости при импорте.
#
# ⚠️ Найдено 2026-07-14: этот список — НЕ источник истины, им был всегда
# только open_interest.sectype (118 живых кодов на сегодня). ticker_futures_map
# (024/026) свежее и шире по акциям (54), но этот список — то, что реально
# сканирует anomaly_scan.py (публичная лента аномалий: тосты/колокол + весь
# content-пайплайн через неё). Расхождение = слепая зона для этих активов.
#
# Валюта/сырьё/индексы (CR/Si/Eu/GL/SV/BR/NG/PT/PD/KC/CC/W4/IMOEXF/RI/RM и т.п.)
# остаются — это НЕ пробел, это структурно другой класс активов, у которого нет
# stock_ticker и который НЕ может попасть в ticker_futures_map по конструкции
# схемы (там акция↔фьючерс).
#
# ⚠️ Уточнено 2026-07-14 (проверено вживую через Algopack API): крипто-фьючерсы
# на MOEX ЕСТЬ — IB (Биткоин/IBIT, был здесь с самого начала) и BT (Биткоин
# Индекс МосБиржи, добавлен сегодня — реальные дневные OI-данные шли в БД
# месяцами, но anomaly_scan.py их не видел, т.к. не было в этом списке).
# Настоящие литеральные контракты BTC/ETH/SOL и междунар. акции (Alibaba/AMD/
# ASML/SAP/...), недавно появившиеся на FORTS (проверено через ISS), — НЕ
# добавлены: Algopack (наш источник разбивки по держателям) прямыми запросами
# подтверждённо не даёт по ним данных вообще, добавление в список ничего не
# даст. Перепроверять периодически, если Algopack расширит покрытие.
OI_ASSETS = [
    "CR", "CNYRUBF", "Si", "Eu", "IB", "VB", "USDRUBF", "GZ", "IMOEXF", "RB",
    "CC", "GL", "GLDRUBF", "NA", "NR", "ED", "GK", "SV", "SS", "X5",
    "MX", "MM", "NG", "GD", "SR", "SF", "GAZPF", "MN", "YD", "BR",
    "SE", "TN", "PT", "AF", "KC", "FF", "AL", "EURRUBF", "SBERF", "CE",
    "HS", "NK", "RI", "RL", "LK", "UC", "PD", "NM", "MC", "RM",
    "RN", "SP", "SN", "ME", "HY", "BM", "TT", "OJ", "MG", "W4",
    "DX", "CH", "MY", "VI", "AU", "BT",
    # + 29 акций из ticker_futures_map, отсутствовавших здесь (миграция 024,
    # найдено 2026-07-14): PX(Полюс) + 28 daily-only акций.
    "PX", "AK", "AS", "BN", "BS", "CM", "FE", "FL", "FS", "HD", "IR", "KM",
    "LE", "MT", "NB", "PH", "PI", "PS", "RA", "RD", "RT", "SC", "SG", "SH",
    "SO", "TB", "UN", "VK", "TP",
    # + 3 акции найденные 2026-07-14 (аудит 024 их не видел — их не было и в
    # ЭТОМ списке, поэтому аудит по OI_ASSETS их не мог найти). NV(Новатэк)
    # теперь и в ticker_futures_map (026). IS(Артген)/MV(М.Видео) в
    # ticker_futures_map НЕ входят (нет строки-акции в instruments — честная
    # дыра, см. 026), но здесь нужны для публичной ленты аномалий/тостов —
    # detector работает по sectype напрямую, stock_ticker ему не нужен.
    "NV", "IS", "MV",
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

# Этап 9 — review-бот. Публикует через config.BOT_TOKEN (тот же бот, что и
# send_signal_post — у него нет активного getUpdates-поллера нигде, кроме
# content_review_bot.py, конфликта с alert_bot.py (ALERT_BOT_TOKEN, другой
# токен) нет). Карточки на ревью шлём в личку ADMIN_USER_ID.
# CONTENT_CHANNEL_ID — ТЕСТОВЫЙ канал на старте (не боевой @FrameTool) —
# Вадим создаёт канал, добавляет бота админом, даёт chat_id (обычно -100...).
CONTENT_CHANNEL_ID = int(os.getenv("CONTENT_CHANNEL_ID", "0"))
# users.id того же админа, что получает карточки — для reviewer_id (FK).
CONTENT_REVIEWER_USER_ID = int(os.getenv("CONTENT_REVIEWER_USER_ID", "0"))

# Этап 6 — MarketTwits (репост-хайп). Формула калибрована вживую 2026-07-13
# (лонгитюдный замер 20 постов, 9 раундов за 2 часа) — два чекпоинта, не
# постоянный поллинг. MTP_CHECKPOINT_*_MIN — окно допуска вокруг чекпоинта
# (крон ~раз в 15 мин, точного попадания в минуту не будет).
MARKETTWITS_CHANNEL = "markettwits"
MTP_CHECKPOINT_1_MIN = 15
MTP_CHECKPOINT_2_MIN = 90
MTP_CHECKPOINT_TOLERANCE_MIN = 20   # проверяем в окне [checkpoint, checkpoint+tolerance] —
                                     # шире кроновского интервала (~15 мин), чтобы не
                                     # промахнуться мимо окна между двумя тиками
MTP_HYPE_RATIO_MIN = 3.0            # ×N к медиане fwd_90 последних наблюдений — тот же
                                     # ATR-стиль порога, что и у OI/фондов (PUBLIC_RATIO_MIN)
MTP_BASELINE_WINDOW = 50            # сколько последних fwd_90-наблюдений берём для медианы

# Шаг Б′ — «аномалия сначала» (обратное направление, найдено 2026-07-14).
# Порог ВЫШЕ пассивной сверки (Шаг Б) — здесь мы САМИ идём искать повод,
# а не подтверждаем уже пришедшую новость, планка строже.
ANOMALY_FIRST_RATIO_MIN = 5.0
ANOMALY_FIRST_MIN_AGE_HOURS = 24     # дать шанс обычным каналам (RSS/MarketTwits) поймать раньше
ANOMALY_FIRST_MAX_AGE_DAYS = 4       # старше — уже не повод для свежего поста
ANOMALY_FIRST_NEWS_WINDOW_DAYS = 2   # окно поиска новости вокруг даты сигнала (±)
ANOMALY_FIRST_SOURCES_MIN = 2        # минимум независимых изданий для подтверждения
