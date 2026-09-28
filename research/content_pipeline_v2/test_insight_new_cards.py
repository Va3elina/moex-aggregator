"""Новые карточки находок (28.09.2026, примеры для Вадима — не выкачено): позиция на минимуме, сделки фондов за месяц,
реакция наших данных на макро-новость. Без сети и базы: данные подставные.

python3 -m pytest research/content_pipeline_v2/test_insight_new_cards.py -q
"""
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals import insight_scan as ins  # noqa: E402
from signals.insights import cards  # noqa: E402


def test_low_leg_goes_to_minimum_card(monkeypatch):
    got = {}
    monkeypatch.setattr(cards, "positions_low_card", lambda sec, base, as_of: got.update(sec=sec, base=base) or {})
    cards.build_card({"kind": "positions", "sec": "USDRUBF", "leg": "long_low"}, "2026-09-15")
    assert got == {"sec": "USDRUBF", "base": "long"}, "«покупки физлиц — минимум» раньше падали KeyError long_low"


def test_new_cards_set_their_own_main_block():
    assert cards.focus_lines({"kind": "macro", "focus": ["новость: …", "цена: …"]}) == ["новость: …", "цена: …"]


def test_day_pick_takes_position_minimums(monkeypatch):
    day = pd.Timestamp("2026-09-15")
    item = {"date": day, "family": "позиции", "type": "рекорд_или_экстремум", "score": 2.6, "instrument": "Si",
            "facts": {"sec": "USDRUBF", "leg": "long_low"},
            "title": "USD/RUB (вечн): покупки физлиц (длинная сторона) 383 518 — минимум с 04.04.2025"}
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [])
    got = ins.pick([item], log=lambda *a: None, until=day)
    assert got and got[0]["spec"] == {"kind": "positions", "sec": "USDRUBF", "leg": "long_low"}


def _five_min(day0="2026-09-01", days=20, jump_at=None, jump=0.0):
    rows = []
    for d in pd.bdate_range(day0, periods=days):
        for k, t in enumerate(pd.date_range(d + pd.Timedelta(hours=10), d + pd.Timedelta(hours=18, minutes=45),
                                            freq="5min")):
            v = 100 * (1 + 0.001 * np.sin(k / 7))
            if jump_at is not None and t >= jump_at:
                v *= 1 + jump
            rows.append((t, v))
    return pd.DataFrame(rows, columns=["t", "close"])


def test_macro_window_measures_two_hours_and_day_end():
    ev = pd.Timestamp("2026-09-24 12:02")
    df = _five_min(jump_at=pd.Timestamp("2026-09-24 12:05"), jump=-0.02)
    w = cards._window(df, ev, "close")
    assert w["t0"] == pd.Timestamp("2026-09-24 12:05")
    assert w["v2"] / w["v0"] - 1 < -0.015, "падение после новости видно в двухчасовом окне"
    assert w["td"].normalize() == ev.normalize() and w["td"].hour == 18
    typ = cards._typical(df[df.t < ev], "close")
    assert typ is not None and typ < 0.005, "обычный двухчасовой ход — без скачка новости"


def test_macro_night_news_counts_the_opening_gap():
    ev = pd.Timestamp("2026-09-24 06:16")          # до открытия: «до» — закрытие прошлого вечера
    df = _five_min(jump_at=pd.Timestamp("2026-09-24 10:00"), jump=-0.01)
    w = cards._window(df, ev, "close")
    assert w["t0"] == pd.Timestamp("2026-09-24 10:00") and w["v2"] / w["v0"] - 1 < -0.005


def test_fund_trades_card_names_leader_change_and_oil(monkeypatch):
    mv = {"resolved_month": "2026-08-01",
          "top_accumulated": [{"akey": "RU0009024277", "asset_name": "ЛУКОЙЛ", "total_delta_amount": 1.56e9,
                               "funds_buying": 9, "funds_selling": 0}],
          "top_reduced": [{"akey": "RU000A108X38", "asset_name": "КЦ ИКС 5", "total_delta_amount": -0.93e9,
                           "funds_buying": 0, "funds_selling": 11}]}
    aug = {"num_funds": 16, "total_value_rub": 121.8e9, "holdings": [
        {"akey": "RU0009024277", "isin": "RU0009024277", "asset_name": "ЛУКОЙЛ", "value_rub": 10.57e9},
        {"akey": "RU0009029540", "isin": "RU0009029540", "asset_name": "Сбербанк", "value_rub": 10.46e9},
        {"akey": "RU0009033591", "isin": "RU0009033591", "asset_name": "Татнфт 3ао", "value_rub": 9.2e9},
        {"akey": "RU000A0DKVS5", "isin": "RU000A0DKVS5", "asset_name": "Новатэк ао", "value_rub": 8.52e9}]}
    jul = {"num_funds": 16, "holdings": [
        {"akey": "RU0009029540", "isin": "RU0009029540", "asset_name": "Сбербанк", "value_rub": 10.24e9},
        {"akey": "RU0009024277", "isin": "RU0009024277", "asset_name": "ЛУКОЙЛ", "value_rub": 8.82e9}]}
    older = {"holdings": [{"akey": "RU0009029540", "asset_name": "Сбербанк", "value_rub": 12e9}]}
    earliest = {"holdings": [{"akey": "RU0009024277", "asset_name": "ЛУКОЙЛ", "value_rub": 9e9}]}

    def api(path, **p):
        if path == "movers":
            return mv
        d = p.get("as_of")
        return aug if d == "2026-08-31" else jul if d == "2026-07-31" else earliest if d <= "2026-04-30" else older
    sectors = pd.DataFrame([("RU0009024277", "LKOH", "Нефть и газ", "Лукойл"),
                            ("RU0009029540", "SBER", "Финансы", "Сбербанк"),
                            ("RU0009033591", "TATN", "Нефть и газ", "Татнефть"),
                            ("RU000A0DKVS5", "NVTK", "Нефть и газ", "НОВАТЭК"),
                            ("RU000A108X38", "X5", "Потреб. сектор", "X5 Group")],
                           columns=["isin", "secid", "sector", "company"])
    monkeypatch.setattr(cards, "_ft_get", api)
    monkeypatch.setattr(cards, "_sql", lambda q, p: sectors)
    stk = pd.DataFrame({"LKOH": np.linspace(6000, 7000, 120)}, index=pd.bdate_range("2026-05-01", periods=120))
    monkeypatch.setattr(cards, "data", lambda: (None, {}, {}, {}, {}, stk, {}, None))
    c = cards.fund_trades_card("2026-09-22")
    text = cards.brief_text(c, focus=True)
    assert "больше всего купили Лукойл" in text and "больше всего продали X5 Group" in text, "имена компаний, не «КЦ ИКС 5»"
    assert "на первом месте Лукойл" in text and "впервые за 3 месяца" in text, "«впервые» — только со сроком"
    assert "Сбербанк был первым 3 месяца подряд и опустился на второе место" in text, "июль, июнь и май"
    assert "3 компании нефти и газа" in text and "#сделкифондов" in text
    assert "«в августе»" in text and "ДАТА ДАННЫХ: 31 августа" in text


# ── посты новых типов по позициям (Вадим 28.09: «трендовые посты нужны, новыми типами разбавить») ─────────────
def _pos_card(angles=()):
    return {"kind": "positions", "spec": {"sec": "MG", "leg": "short"}, "as_of": pd.Timestamp("2026-09-22"),
            "headline": "Шорт физлиц по фьючерсу на акции «Магнит» - 265 тыс. контрактов, исторический максимум",
            "facts": ["шорт физлиц по фьючерсу на акции «Магнит» - 265 тыс. контрактов на закрытие 22 сентября",
                      "итог толпы: ставка на падение в плюсе примерно на 5%"],
            "trend": ["главное - тренд, а не прошлый пик: за последние два месяца шорт физлиц растёт, а акции -15%",
                      "итого: когда такой тренд позиции разворачивался, акции через месяц были выше в 2 случаях из 4"],
            "after": [], "analogy": [], "price": ["акции «Магнит» - 1 600 ₽ на 22 сентября; за неделю -1%, за месяц -1%"],
            "context": ["число физлиц в шорте - 296 человек, за месяц -15%"], "limits": [], "chart_note": [],
            "hashtag": "#открытыепозиции", "angles": list(angles)}


CONC = {"type": "концентрация", "strength": 7.9, "line": "поворот - концентрация: шорт физлиц за месяц вырос в 6,7 раза, "
        "а число физлиц в шорте -15%; на одного в среднем в 7,9 раза больше"}


def test_trend_card_text_does_not_change():
    plain, with_angles = _pos_card(), _pos_card([CONC])
    assert cards.brief_text(plain, focus=True) == cards.brief_text(with_angles, focus=True), \
        "трендовый пост — как был: поворот в его карточку не выводится"


def test_angle_card_has_its_own_main_block():
    c = cards.angle_card(_pos_card([CONC]), "концентрация")
    text = cards.brief_text(c, focus=True)
    main = text.split("ГЛАВНОЕ - строй пост вокруг этого:")[1].split("\n\n")[0]
    assert "ТИП ПОСТА: КОНЦЕНТРАЦИЯ" in main and "узкий круг" in main and "итог толпы" in main
    assert "главное - тренд" not in main, "тренд — не главное у поста нового типа"
    assert c["spec"]["angle"] == "концентрация"


def test_day_pick_adds_new_types_on_top_of_three_trend_posts(monkeypatch):
    day = pd.Timestamp("2026-09-22")
    names = ["Магнит", "VK", "Газпром", "Норникель", "Сбер"]
    items = [{"date": day, "family": "позиции", "type": "рекорд_или_экстремум", "score": 10 - k, "instrument": n,
              "facts": {"sec": n, "leg": "short"}, "title": f"{n}: шорт физлиц — максимум"} for k, n in enumerate(names)]
    ang = {"Магнит": [CONC], "Сбер": [{"type": "повод", "strength": 9.0, "line": "повод дня: …"}],
           "VK": [{"type": "повод", "strength": 3.0, "line": "слабый повод"}]}
    monkeypatch.setattr(ins, "repeat_of", lambda spec, as_of: None)
    monkeypatch.setattr(ins.cards, "build_card", lambda spec, d: {"angles": ang.get(spec["sec"], [])})
    got = ins.pick(items, log=lambda *a: None, until=day)
    trend = [j["instrument"] for j in got if not j["spec"].get("angle")]
    new = {j["instrument"]: j["spec"]["angle"] for j in got if j["spec"].get("angle")}
    assert new == {"Магнит": "концентрация", "Сбер": "повод"}, "слабый повод VK (3× обычного) — не пост"
    assert trend == ["VK", "Газпром", "Норникель"], "три трендовых остаются, одна бумага — один пост"


def test_position_angles_on_synthetic_series():
    dates = pd.bdate_range("2025-01-01", periods=300)
    short = pd.Series(np.r_[np.full(279, 40_000.0), np.linspace(40_000, 265_000, 21)], index=dates)
    ns = pd.Series(np.r_[np.full(279, 350.0), np.linspace(350, 296, 21)], index=dates)
    nl = pd.Series(np.full(300, 700.0), index=dates)
    rng = np.random.RandomState(1)
    base = 2000 * np.cumprod(1 + rng.normal(0, 0.01, 298))       # обычный дневной ход ~1%
    price = pd.Series(np.r_[base, base[-1] * 0.99, base[-1] * 0.87], index=dates)
    P = {"short": {"MG": short}, "ns": {"MG": ns}, "nl": {"MG": nl}}
    got = cards.position_angles(P, "MG", "short", "шорт физлиц", "фьючерсу на акции «Магнит»", "акции «Магнит»",
                                short.values, dates, price, [], dates[-1])
    types = [a["type"] for a in got]
    assert "концентрация" in types and "повод" in types
    conc = next(a for a in got if a["type"] == "концентрация")
    assert "узкий круг" in conc["line"] and "тыс." not in conc["line"], "без числа контрактов (R02)"
    # доля: число шортистов против лонгистов, прошлый пик — индекс 150 (доля тогда выше)
    ns2 = pd.Series(np.r_[np.full(150, 300.0), [900.0], np.full(149, 300.0)], index=dates)
    ns2.iloc[-1] = 800.0
    nl2 = pd.Series(np.r_[np.full(150, 700.0), [100.0], np.full(149, 700.0)], index=dates)
    got2 = cards.position_angles({"ns": {"MX": ns2}, "nl": {"MX": nl2}}, "MX", "ns", "число физлиц в шорте",
                                 "фьючерсу на индекс", None, ns2.values, dates, None, [{"top": 150}], dates[-1])
    assert got2 and got2[0]["type"] == "доля" and "было 90%" in got2[0]["line"]
