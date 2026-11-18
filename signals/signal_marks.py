"""Значки силы рекордов в @framesignalbot (Вадим 18.11.2026): три ступени вместо красных кружков по числу окон.

  • «Вау» — рекорд за всё время → горящий огонь, со звуком;
  • «Сильно» — рекорд за 2, 3 или 5 лет → переливающийся оранжевый кружок, со звуком;
  • «Для понимания» — 3 месяца, полгода, год → без значка и без звука.

Кружки по числу окон никто не считает, а огонь на каждом рекорде обесценивается — поэтому знаков два, оба
анимированные и разные по форме. Значки — свой пак t.me/addemoji/frame_signals_by_framesignalbot (исходники
research/signal_emoji). Запасной эмодзи внутри <tg-emoji> — то, что видно в пуш-уведомлении и без Premium; он обязан
быть эмодзи, иначе Telegram отклоняет сообщение (ENTITY_TEXT_INVALID).
"""

import json
import os

FIRE = '<tg-emoji emoji-id="5470008294498150786">🔥</tg-emoji>'
PULSE = '<tg-emoji emoji-id="5472356988774034303">🟠</tg-emoji>'
# значки кнопок под сигналом (Вадим 18.11): анимированные шестерёнка и линия графика из того же пака
# (research/signal_emoji/make_buttons.py → pack.py add); у кнопки — icon_custom_emoji_id
GEAR_ID = "5472322972633051055"
# рост/падение в таблице истории (Вадим 18.11: «можно цветом пометить?» — цвета текста в Telegram нет, поэтому
# значки нашего пака роботов frame_robots: зелёная стрелка вверх, красная вниз)
UP = '<tg-emoji emoji-id="5319270099348140586">↗️</tg-emoji>'
DOWN = '<tg-emoji emoji-id="5316938486157192935">↘️</tg-emoji>'
CHART_ID = "5472159987214102346"

WOW = "за всё время"
STRONG = ("за 2 года", "за 3 года", "за 5 лет")


def tier(window: str) -> str:
    """wow / strong / info."""
    if window == WOW:
        return "wow"
    return "strong" if window in STRONG else "info"


def mark(window: str) -> str:
    """Значок перед заголовком сообщения (с пробелом) или пустая строка."""
    return {"wow": FIRE + " ", "strong": PULSE + " "}.get(tier(window), "")


def silent(window: str) -> bool:
    """Рядовые рекорды приходят без звука — не дёргают телефон по три раза в день."""
    return tier(window) == "info"


# Логотипы активов — свой пак t.me/addemoji/frame_logos_by_framesignalbot (Вадим 18.11). Картинка — как на сайте
# (InstrumentIcon); ключи «oi:<sectype>» — фьючерсы открытого интереса, «se:<код>» — активы сезонности. Список и
# номера значков — signal_logos.json (research/signal_emoji: make_logos.py → pack_logos.py). Нет логотипа — без значка.
LOGO_FALLBACK = "▫️"
_LOGOS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal_logos.json")
try:
    with open(_LOGOS_FILE, encoding="utf-8") as _fh:
        LOGOS: dict = json.load(_fh)
except FileNotFoundError:
    LOGOS = {}


def logo(key: str) -> str:
    """Логотип актива перед названием (с пробелом) или пустая строка."""
    eid = LOGOS.get(key)
    return f'<tg-emoji emoji-id="{eid}">{LOGO_FALLBACK}</tg-emoji> ' if eid else ""
