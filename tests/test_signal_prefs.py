"""Юнит-тесты настроек сигналов (signals/signal_prefs.py) и кнопок под сигналом (signals/framesignal._markup).

    pytest tests/test_signal_prefs.py
"""
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signals import framesignal as fs  # noqa: E402
from signals import signal_prefs as sp  # noqa: E402

DAY = dt.datetime(2026, 11, 18, 12, 0, tzinfo=sp.MSK)
NIGHT = dt.datetime(2026, 11, 18, 23, 30, tzinfo=sp.MSK)
REC = {"type": "oi_record", "asset": "oi:SR", "window": "за полгода", "market": "Акции", "mode": "positions"}


def test_no_prefs_sends_everything():
    assert sp.decide(None, REC, DAY) == "sound"
    assert sp.decide({}, REC, DAY) == "sound"
    assert sp.decide({"types": {}}, None, DAY) == "sound"


def test_off_hidden_and_type():
    assert sp.decide({"on": False}, REC, DAY) is None
    assert sp.decide({"types": {"oi_record": {"on": False}}}, REC, DAY) is None
    assert sp.decide({"hidden": ["oi:SR"]}, REC, DAY) is None
    assert sp.decide({"hidden": ["oi:SR"]}, {**REC, "asset": "oi:GZ"}, DAY) == "sound"


def test_hidden_per_type():
    p = {"types": {"oi_record": {"hidden": ["oi:SR"]}}}
    assert sp.decide(p, REC, DAY) is None
    assert sp.decide(p, {"type": "season", "asset": "oi:SR"}, DAY) == "sound"     # у сезонности своё


def test_min_window_markets_modes():
    p = {"types": {"oi_record": {"min": "1y"}}}
    assert sp.decide(p, REC, DAY) is None                                   # полгода < года
    assert sp.decide(p, {**REC, "window": "за 3 года"}, DAY) == "sound"
    assert sp.decide({"types": {"oi_record": {"markets": ["Валюта"]}}}, REC, DAY) is None
    assert sp.decide({"types": {"oi_record": {"modes": ["participants"]}}}, REC, DAY) is None


def test_quiet_hours_mute_not_drop():
    p = {"quiet": {"on": True, "from": "23:00", "to": "08:00"}}
    assert sp.decide(p, REC, NIGHT) == "silent"
    assert sp.decide(p, REC, DAY) == "sound"


def test_buttons():
    kb = json.loads(fs._markup({**REC, "name": "Сбербанк"}, "https://framedata.ru/oi?instrument=SR"))["inline_keyboard"][0]
    assert kb[0]["text"] == "Открыть график" and kb[0]["url"] == "https://framedata.ru/oi?instrument=SR"
    assert kb[0]["icon_custom_emoji_id"] and kb[1]["icon_custom_emoji_id"]          # анимированные значки пака
    assert kb[1]["text"] == "Настройки"
    assert kb[1]["web_app"]["url"].startswith("https://framedata.ru/tg?s=signals&from=oi_record&asset=oi%3ASR")
    assert fs._markup(None, None) is None


def test_rich_history_details_table():
    cap = ("<b>RL</b>\nЛонг\n602 чел.\n<blockquote expandable><b>История · 2 похожих случая</b>\n"
           "Дата · через день · через 2 нед\n21 янв 2026 · +1,0% · −6,9%\n25 фев 2025 · −4,2% · +3,5%\n"
           "В обычный день цена росла в 45% случаев</blockquote>")
    h = fs.rich_html(cap, photo=True)
    assert h.startswith('<img src="tg://photo?id=p0"/><p><b>RL</b><br/>Лонг<br/>602 чел.</p>')
    assert "<details><summary>История · 2 похожих случая</summary><table" in h and h.endswith("</details>")
    assert "<th>через 2 нед</th>" in h and "<footer>В обычный день" in h
    from signals import signal_marks
    assert f"{signal_marks.UP} +1,0%" in h and f"{signal_marks.DOWN} −6,9%" in h
    assert fs.rich_html("<b>x</b>\nбез истории", photo=True) is None


def test_price_after_no_invention():
    from signals import signal_history as sh
    px = [(dt.date(2020, 1, 9) + dt.timedelta(days=i), 100.0 + i) for i in range(60)]
    assert sh.price_after(px, dt.date(2014, 3, 3), 14) is None                 # событие раньше ряда цен
    assert sh.price_after(px, dt.date(2020, 1, 10), 14) is not None
    gap = px[:10] + px[30:]
    assert sh.price_after(gap, dt.date(2020, 1, 25), 5) is None                 # дыра в данных больше недели


def test_buttons_without_icons():
    kb = fs._markup({**REC, "name": "Сбербанк"}, "https://framedata.ru/oi")
    row = json.loads(fs._no_icons(kb))["inline_keyboard"][0]
    assert all("icon_custom_emoji_id" not in b for b in row) and len(row) == 2
