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
    long_ = pd.Series(np.r_[np.full(279, 100_000.0), np.linspace(100_000, 241_000, 21)], index=dates)  # тоже рекорд
    net = long_ - short                                                                           # чистый шорт мал
    P = {"short": {"MG": short}, "ns": {"MG": ns}, "nl": {"MG": nl}, "long": {"MG": long_}, "net": {"MG": net}}
    got = cards.position_angles(P, "MG", "short", "шорт физлиц", "фьючерсу на акции «Магнит»", "акции «Магнит»",
                                short.values, dates, price, [], dates[-1])
    types = [a["type"] for a in got]
    assert "концентрация" in types and "повод" in types
    conc = next(a for a in got if a["type"] == "концентрация")
    assert "узкий круг" in conc["line"] and "ВТОРАЯ СТОРОНА" in conc["line"] and "спред" in conc["line"], \
        "покупки тоже на рекорде, чистая позиция мала — узкий круг может держать обе стороны"
    # доля: число шортистов против лонгистов, прошлый пик — индекс 150 (доля тогда выше)
    ns2 = pd.Series(np.r_[np.full(150, 300.0), [900.0], np.full(149, 300.0)], index=dates)
    ns2.iloc[-1] = 800.0
    nl2 = pd.Series(np.r_[np.full(150, 700.0), [100.0], np.full(149, 700.0)], index=dates)
    got2 = cards.position_angles({"ns": {"MX": ns2}, "nl": {"MX": nl2}}, "MX", "ns", "число физлиц в шорте",
                                 "фьючерсу на индекс", None, ns2.values, dates, None, [{"top": 150}], dates[-1])
    assert got2 and got2[0]["type"] == "доля"
    assert "с 30% до 53%" in got2[0]["line"] and "толпа переходит из лонга в шорт" in got2[0]["line"], "сдвиг за месяц — главное"
    assert "доля была 90%" in got2[0]["line"], "прошлый пик — фоном"
    none = cards.position_angles({"ns": {"CC": ns2}, "nl": {"CC": nl2}}, "CC", "ns", "число физлиц в шорте", "фьючерсу на какао",
                                 None, ns2.values, dates, None, [{"top": 150}], dates[-1])
    assert none == [], "доля — только у индекса и валют"



# ── запуск сделок фондов (шаг 2, 28.09): раз в месяц на новый срез ──────────────────────────────────────────
class _DB:
    def __init__(self, exists=False):
        self.exists = exists

    def execute(self, q, p=None):
        ex = self.exists

        class R:
            def first(self):
                return (1,) if ex else None
        return R()


def _ft_api(monkeypatch, month="2026-08-01"):
    monkeypatch.setattr(cards, "_ft_get", lambda path, **p: {
        "resolved_month": month, "top_accumulated": [{"asset_name": "ЛУКОЙЛ"}], "top_reduced": [{"asset_name": "X5"}]})


def test_fund_trades_month_once_and_not_after_channel_post(monkeypatch):
    _ft_api(monkeypatch)
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [])
    job = ins.fund_trades_job(pd.Timestamp("2026-09-20"), _DB())
    assert job and job["thread_key"] == "insight:fund_trades:2026-08" and job["spec"]["kind"] == "fund_trades"
    assert ins.fund_trades_job(pd.Timestamp("2026-09-20"), _DB(exists=True)) is None, "месяц уже был у завода"
    post = (pd.Timestamp("2026-09-22 12:00"), "Фокус смещается на сырьё 🔍 ◽️ Лукойл обошёл Сбербанк… #cделкифондов")
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [post])
    assert ins.fund_trades_job(pd.Timestamp("2026-09-25"), _DB()) is None, "канал уже писал о сделках фондов за август"


def test_fund_trades_old_month_is_not_news(monkeypatch):
    _ft_api(monkeypatch, month="2026-06-01")
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [])
    assert ins.fund_trades_job(pd.Timestamp("2026-09-20"), _DB()) is None


def test_fund_trades_chart_draws(tmp_path):
    card = {"kind": "fund_trades", "chart": {"type": "hbars", "title": "Сделки фондов акций за август, млрд ₽",
                                             "labels": ["Лукойл", "Татнефть", "X5 Group", "Полюс"],
                                             "values": [1.56, 0.5, -0.93, -0.46]}}
    out = tmp_path / "ft.png"
    cards.draw_chart(card, str(out))
    assert out.exists() and out.stat().st_size > 5000


# ── макро (шаг 3, 28.09): новость важности 5 без компании → «как отреагировали наши данные» ─────────────────
from signals import macro_scan  # noqa: E402


def test_macro_one_post_a_day_and_theme_once_in_three_days():
    now = pd.Timestamp("2026-09-18 12:00", tz=macro_scan.MSK)
    rows = [{"id": 1}, {"id": 2}]
    themes = {1: "санкции", 2: "геополитика"}
    done_today = [("insight:macro:санкции:10", pd.Timestamp("2026-09-18 07:00", tz="UTC"))]
    assert macro_scan.pick_event(rows, done_today, now, themes) == [], "сегодня макро уже был"
    done_yday = [("insight:macro:санкции:10", pd.Timestamp("2026-09-17 07:00", tz="UTC"))]
    assert macro_scan.pick_event(rows, done_yday, now, themes) == [{"id": 2}], "санкции были вчера — тема ждёт три дня"


def test_macro_weekend_news_is_read_on_monday_morning():
    assert macro_scan.lookback_hours(pd.Timestamp("2026-09-21 09:00", tz=macro_scan.MSK)) == 74
    assert macro_scan.lookback_hours(pd.Timestamp("2026-09-22 09:00", tz=macro_scan.MSK)) == 26


def test_macro_headline_is_clean():
    raw = "🔥⚠️🇺🇸🇷🇺#санкции #россия ТРАМП ПОДПИСАЛ ЗАКОНОПРОЕКТ ОБ **\"АДСКИХ САНКЦИЯХ\" **ПРОТИВ РОССИИ. Подробнее…"
    h = macro_scan.headline_of(raw)
    assert h.startswith("ТРАМП ПОДПИСАЛ") and "#" not in h and "**" not in h and "Подробнее" not in h


def test_macro_runs_inside_the_daytime_combo_pass():
    import inspect

    from signals import combo_scan
    src = inspect.getsource(combo_scan)
    assert "macro_scan.run_once(dry_run=a.dry_run, at=a.at)" in src and 'if a.mode == "news":' in src


def test_macro_card_has_strength_and_no_contract_counts(monkeypatch):
    days = pd.bdate_range("2026-06-01", "2026-09-17")
    rows, oi = [], []
    for d in days:
        for k, t in enumerate(pd.date_range(d + pd.Timedelta(hours=7), d + pd.Timedelta(hours=23, minutes=45),
                                            freq="5min")):
            jump = t >= pd.Timestamp("2026-09-17 07:00")
            rows.append((t, 2300 * (1 + 0.0005 * np.sin(k / 9)) * (0.99 if jump else 1)))
            oi.append((t, 226_000 * (1 + 0.001 * np.sin(k / 9)) * (0.87 if jump else 1), 0, 0, 0, 0))

    def sql(q, p):
        if "FROM candles" in q:
            return pd.DataFrame(rows if p["s"] == "IMOEXF" else [], columns=["t", "close"])
        if "FROM open_interest" in q:
            return pd.DataFrame(oi if p["s"] == "IMOEXF" else [],
                                columns=["t", "net", "pos_long", "pos_short", "nl", "ns"])
        return pd.DataFrame(columns=["trade_date", "close"])
    monkeypatch.setattr(cards, "_sql", sql)
    c = cards.macro_card("2026-09-17 06:16", "Палата представителей США проголосовала за законопроект", "",
                         "2026-09-17 23:55")
    text = cards.brief_text(c, focus=True)
    assert c["strength"] >= 2, "цена и толпа ушли сильнее обычного — черновик пройдёт порог"
    assert "контракт" not in text, "число контрактов не называем (R02) — доли и кратности"
    assert "ТИП ПОСТА: РЕАКЦИЯ НА МАКРО-НОВОСТЬ" in text and c["chart"]["type"] == "intraday"


def test_macro_chart_draws(tmp_path):
    x = pd.date_range("2026-09-17 06:00", periods=60, freq="5min")
    card = {"kind": "macro", "chart": {"type": "intraday", "title": "Фьючерс на индекс и позиции физлиц", "x": x.values,
                                       "y": np.linspace(2300, 2280, 60), "x2": x.values, "y2": np.linspace(226e3, 197e3, 60),
                                       "y_label": "фьючерс", "y2_label": "чистая позиция", "event": x[3]}}
    out = tmp_path / "m.png"
    cards.draw_chart(card, str(out))
    assert out.exists() and out.stat().st_size > 5000



def test_macro_draft_has_own_ticker_so_seasonality_does_not_block_it():
    import inspect
    src = inspect.getsource(macro_scan.run_once)
    assert '"tickers": [f"MACRO:{themes[r[\'id\']]}"]' in src and '"tickers": ["MIX"]' not in src, \
        "с «MIX» правило повторов по тикеру сравнивало бы макро с ежедневной сезонностью индекса"


# ── повод дня в тот же день (28.09): бумага сегодня ушла резко, а у толпы рекорд ─────────────────────────────
from signals import trigger_scan  # noqa: E402


def test_trigger_window_is_the_whole_trading_day():
    """Вадим 28.09: «я бы делал всё постоянно — каждый час или каждый день» — не только вечером."""
    msk = trigger_scan.MSK
    assert trigger_scan.in_window(pd.Timestamp("2026-09-15 21:00", tz=msk)) is False
    assert trigger_scan.in_window(pd.Timestamp("2026-09-15 20:40", tz=msk))
    assert trigger_scan.in_window(pd.Timestamp("2026-09-15 12:00", tz=msk)), "с полудня, а не с 18:00"
    assert trigger_scan.in_window(pd.Timestamp("2026-09-15 11:40", tz=msk)) is False, "первые два часа сессии"
    assert trigger_scan.in_window(pd.Timestamp("2026-09-19 14:00", tz=msk)) is False, "суббота"


def test_trigger_pool_is_stock_position_records_once_per_stock():
    d = pd.Timestamp("2026-09-14")
    items = [{"date": d, "family": "позиции", "type": "рекорд_или_экстремум", "score": 9, "instrument": "SS",
              "facts": {"sec": "SS", "leg": "long"}, "title": "Самолет: покупки физлиц — максимум"},
             {"date": d, "family": "позиции", "type": "рекорд_или_экстремум", "score": 8, "instrument": "SS",
              "facts": {"sec": "SS", "leg": "net"}, "title": "Самолет: чистый лонг — максимум"},
             {"date": d, "family": "позиции", "type": "рекорд_или_экстремум", "score": 7, "instrument": "Si",
              "facts": {"sec": "Si", "leg": "short"}, "title": "USD/RUB: шорт — максимум"},
             {"date": d - pd.Timedelta(days=1), "family": "позиции", "type": "рекорд_или_экстремум", "score": 20,
              "instrument": "GZ", "facts": {"sec": "GZ", "leg": "short"}, "title": "Газпром: вчерашний рекорд"}]
    pool = trigger_scan.pool_of(items, {"SS": "SMLT", "GZ": "GAZP"})
    assert [x["facts"]["leg"] for x in pool] == ["long"], "одна бумага — один раз; доллар не акция; вчерашнее — мимо"


def test_trigger_takes_strongest_live_move_only():
    d = pd.Timestamp("2026-09-14")
    pool = [{"date": d, "title": "Самолет", "facts": {"sec": "SS", "leg": "long"}},
            {"date": d, "title": "Магнит", "facts": {"sec": "MN", "leg": "short"}},
            {"date": d, "title": "VK", "facts": {"sec": "VK", "leg": "long"}}]
    angles = {"SS": [{"type": "повод", "strength": 6.0, "live": True, "line": "сегодня −13%"}],
              "MN": [{"type": "повод", "strength": 9.0, "live": False, "line": "вчера"}],
              "VK": [{"type": "повод", "strength": 3.0, "live": True, "line": "сегодня −4%"}]}
    x, spec, a = trigger_scan.best_trigger(pool, build=lambda spec, dt: {"angles": angles[spec["sec"]]})
    assert x["title"] == "Самолет" and a["live"], "вчерашний ход — дело утреннего сканера; слабый сегодняшний — мимо"


def test_live_price_respects_now_override(monkeypatch):
    got = {}

    def sql(q, p):
        got.update(p)
        return pd.DataFrame([(pd.Timestamp("2026-09-15 20:55"), 270.0)], columns=["t", "close"])
    monkeypatch.setattr(cards, "_sql", sql)
    monkeypatch.setattr(cards, "NOW", pd.Timestamp("2026-09-15 21:00"))
    price, t = cards.live_price("SMLT", pd.Timestamp("2026-09-14"))
    assert price == 270.0 and got["now"] == pd.Timestamp("2026-09-15 21:00") and got["d"] == pd.Timestamp("2026-09-15")


def test_trigger_runs_inside_the_daytime_combo_pass():
    import inspect

    from signals import combo_scan
    assert "trigger_scan.run_once(dry_run=a.dry_run, at=a.at)" in inspect.getsource(combo_scan)


# ── общий файл находок дня (28.09): утренний сканер, связки и повод дня считают находки один раз ──────────────
def test_detections_are_computed_once_per_data_day(monkeypatch, tmp_path):
    import inspect
    calls = []
    monkeypatch.setattr(ins, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(ins, "detect_window", lambda until: calls.append(until) or [{"title": "x", "score": 1.5}])
    until = pd.Timestamp("2026-09-25")
    assert ins.detections(until) == [{"title": "x", "score": 1.5}]
    assert ins.detections(until) == [{"title": "x", "score": 1.5}]
    assert len(calls) == 1, "второй раз — из файла"
    from signals import combo_scan
    assert combo_scan.detections is ins.detections, "связки и повод дня берут тот же файл"
    assert "detections(until)" in inspect.getsource(ins.run_once), "утренний сканер — тоже"


# ── рывок позиции по ходу дня (28.09, Вадим: «сканер находок — на интрадей, хотя бы каждый час») ─────────────
def _rusal_like(n=80, base=40000.0, step=500.0):
    """Ряд покупок с обычным дневным ходом около step и последним закрытием 25.09 (пятница)."""
    dates = pd.bdate_range(end="2026-09-25", periods=n)
    rng = np.random.default_rng(1)
    arr = base + np.cumsum(rng.choice([-step, step], size=n))
    return arr.astype(float), dates


def test_surge_angle_reads_the_intraday_jump(monkeypatch):
    arr, dates = _rusal_like()
    v = float(arr[-1])
    people = pd.Series(591.0, index=dates)
    snap = {"long": (v * 1.2, pd.Timestamp("2026-09-28"), "12:05"), "nl": (595.0, pd.Timestamp("2026-09-28"), "12:05")}
    monkeypatch.setattr(cards, "intraday_now", lambda sec, leg, t: snap.get(leg))
    monkeypatch.setattr(cards, "live_price", lambda *a, **k: None)
    monkeypatch.setattr(cards, "data", lambda: (_ for _ in ()).throw(RuntimeError("без базы")))
    a = cards.surge_angle("RL", "long", "покупки физлиц", "фьючерсу на акции «Русал»", "акции «Русал»", arr, dates,
                          None, dates[-1], lambda name: people)
    assert a and a["type"] == "рывок" and a["live"]
    assert a["strength"] >= cards.ANGLE_MIN["рывок"]
    assert "к 12:05" in a["line"] and "+20%" in a["line"]
    assert "+4 человека" in a["line"] and "те же люди" in a["line"], "объём +20% при людях +0,7% — узкий круг"
    assert "максимум" in a["line"], "срез выше всей истории ряда — рекорд по срезу"
    assert a["point"][0] == pd.Timestamp("2026-09-28")


def test_surge_angle_ignores_small_moves_and_expiry(monkeypatch):
    arr, dates = _rusal_like()
    v = float(arr[-1])
    people = pd.Series(591.0, index=dates)
    monkeypatch.setattr(cards, "live_price", lambda *a, **k: None)
    monkeypatch.setattr(cards, "data", lambda: (_ for _ in ()).throw(RuntimeError("без базы")))
    args = ("RL", "long", "покупки физлиц", "фьючерсу на акции «Русал»", "акции «Русал»", arr, dates, None, dates[-1],
            lambda name: people)
    monkeypatch.setattr(cards, "intraday_now", lambda sec, leg, t: (v + 900, pd.Timestamp("2026-09-28"), "12:05"))
    assert cards.surge_angle(*args) is None, "обычный дневной ход — не рывок"
    monkeypatch.setattr(cards, "intraday_now", lambda sec, leg, t: (v * 1.5, pd.Timestamp("2026-09-17"), "12:05"))
    assert cards.surge_angle(*args[:5], arr[:-6], dates[:-6], None, dates[-7], lambda name: people) is None, \
        "день экспирации — переход в следующий контракт, а не рывок"


def test_surge_candidates_rank_jumps_and_skip_low_activity():
    arr, dates = _rusal_like()
    P = {"long": pd.DataFrame({"RL": arr, "XX": arr}, index=dates),
         "short": pd.DataFrame({"RL": arr / 4, "XX": arr / 4}, index=dates)}
    d = pd.Timestamp("2026-09-28")
    snap = pd.DataFrame({"tradedate": [d, d], "tradetime": ["12:05:00", "12:05:00"],
                         "long": [arr[-1] * 1.2, arr[-1] * 1.3], "short": [arr[-1] / 4, arr[-1] / 4]},
                        index=pd.Index(["RL", "XX"], name="sectype"))
    got = trigger_scan.surge_candidates(snap, P, low={"XX"}, day=d)
    assert [(c["sec"], c["leg"]) for c in got] == [("RL", "long")], "малоактивный XX — мимо, шорт без хода — мимо"
    assert got[0]["date"] == dates[-1] and got[0]["t"] == "12:05"
    assert trigger_scan.surge_candidates(snap, P, day=pd.Timestamp("2026-09-17")) == [], "день экспирации"
    groups = {"RL": "Акции", "XX": "Сырьё"}
    assert [c["sec"] for c in trigger_scan.surge_candidates(snap, P, day=d, groups=groups)] == ["RL"], \
        "медь, какао и S&P — не наш рынок (реплей 15.09: медь обошла бы Самолёт)"
    assert trigger_scan.surge_allowed("Si", {}) and not trigger_scan.surge_allowed("CE", {"CE": "Сырьё"})


def test_trigger_picks_surge_and_skips_taken_stock():
    d = pd.Timestamp("2026-09-25")
    pool = [{"date": d, "title": "Самолет", "facts": {"sec": "SS", "leg": "long"}}]
    surges = [{"sec": "RL", "leg": "long", "k": 12.0, "pct": 0.2, "date": d, "t": "12:05"},
              {"sec": "MN", "leg": "short", "k": 20.0, "pct": 0.5, "date": d, "t": "12:05"}]
    angles = {"SS": [{"type": "повод", "strength": 4.4, "live": True, "line": "сегодня −9%"}],
              "RL": [{"type": "рывок", "strength": 12.0, "live": True, "line": "рывок по ходу дня: покупки +20%; люди"}],
              "MN": [{"type": "рывок", "strength": 20.0, "live": True, "line": "рывок по ходу дня: шорт +50%"}]}
    build = lambda spec, dt: {"angles": angles[spec["sec"]]}  # noqa: E731
    x, spec, a = trigger_scan.best_trigger(pool, surges, build=build, taken={"MN"})
    assert spec["sec"] == "RL" and a["type"] == "рывок", "12/8 сильнее 4,4/4; по Магниту сегодня уже был черновик"
    assert x["title"] == "покупки +20%" and x["instrument"] == "RL"


def test_intraday_event_is_not_a_repeat_of_unpublished_trend_draft():
    from types import SimpleNamespace

    from signals.content_ai import _repeat_of_ticker

    def db_with(*rows):
        return SimpleNamespace(execute=lambda q, p: SimpleNamespace(fetchall=lambda: list(rows)))

    def row(**kw):
        base = {"id": 3049, "created_at": pd.Timestamp("2026-09-27 08:55"), "headline": "Покупки … максимум с 26 января",
                "status": "draft_ready", "thread_key": "insight:positions:RL", "my_source": "insight",
                "my_headline": "Покупки … максимум с 26 января", "my_thread": "insight:trigger:RL:20260928"}
        return SimpleNamespace(**{**base, **kw})
    assert _repeat_of_ticker(db_with(row()), 1) is None, "рывок дня — не повтор невышедшего трендового"
    assert _repeat_of_ticker(db_with(row(status="published")), 1), "вышедший пост за три дня — повтор"
    assert _repeat_of_ticker(db_with(row(thread_key="insight:trigger:RL:20260927")), 1), "второе событие дня — повтор"
    assert _repeat_of_ticker(db_with(row(my_thread="insight:positions:RL")), 1), "обычная находка — как раньше"


def test_surge_card_has_its_own_headline_and_chart_point():
    card = {"headline": "Покупки физлиц по фьючерсу на акции «Русал» - 96 тыс. контрактов, максимум с 26 января",
            "as_of": pd.Timestamp("2026-09-25"), "spec": {"kind": "positions", "sec": "RL", "leg": "long"},
            "angles": [{"type": "рывок", "strength": 8.4, "live": True,
                        "line": "рывок по ходу дня: покупки физлиц по фьючерсу на акции «Русал» к 12:05 28 сентября +20% "
                                "к закрытию 25 сентября - в 8,4 раза больше обычного дневного хода; люди",
                        "point": (pd.Timestamp("2026-09-28"), 115651.0, "12:05")}],
            "facts": [], "price": [], "limits": ["данные дневные, на закрытие торгов 25 сентября; что было внутри дня, не видно"],
            "chart": {"type": "line2", "x": pd.DatetimeIndex(["2026-09-24", "2026-09-25"]), "y": np.array([54114.0, 96499.0]),
                      "marks": []}, "chart_note": ["на графике"]}
    c = cards.angle_card(card, "рывок")
    assert c["headline"].startswith("Рывок по ходу дня: покупки физлиц") and "к 12:05" in c["headline"]
    assert c["headline"] != card["headline"], "иначе проверка «такой заголовок уже был» снимает рывок"
    assert list(c["chart"]["y"])[-1] == 115651.0 and c["chart"]["x"][-1] == pd.Timestamp("2026-09-28")
    assert any("к 12:05" in x for x in c["limits"]) and not any(x.startswith("данные дневные") for x in c["limits"])


# ── тренд по EMA 20 вместо прошлых пиков (Вадим 05.10, шорт индекса по вечному фьючерсу) ─────────────────────────
def test_ema_matches_site_indicator():
    s = pd.Series([1.0, 2, 3, 4, 5, 6], index=pd.bdate_range("2026-01-01", periods=6))
    e = cards.ema(s, 3).values
    assert np.isnan(e[:2]).all() and e[2] == 2.0, "старт — SMA первых n точек, как frontend/src/utils/indicators.ts"
    assert e[3] == 4 * 0.5 + 2 * 0.5 and e[5] == 6 * 0.5 + e[4] * 0.5


def test_short_touch_does_not_flip_trend():
    up = np.linspace(100, 200, 60)
    arr = np.r_[up, [150, 205, 210]]         # один день под средней — касание, не разворот
    side = cards.trend_side(pd.Series(arr, index=pd.bdate_range("2025-01-01", periods=len(arr)))).values
    assert side[-1] == 1 and side[-3] == 1


def test_record_in_rising_trend_is_not_a_peak(monkeypatch):
    """Рекорд в растущем тренде: блока «что было после прошлых пиков» нет, в фактах — «продолжение, а не пик»."""
    d = pd.bdate_range("2023-01-02", periods=700)
    wave = np.r_[np.linspace(10, 100, 150), np.linspace(100, 20, 150), np.linspace(20, 110, 200), np.linspace(110, 140, 200)]
    pos = pd.Series(wave * 1000, index=d)
    px = pd.Series(np.linspace(2000, 3000, 700), index=d)
    monkeypatch.setattr(cards, "data", lambda: ({"short": {"IMOEXF": pos}}, {}, {}, {}, {}, {}, {}, None))
    monkeypatch.setattr(cards, "human_name", lambda sec: ("вечный фьючерс на индекс", "вечному фьючерсу на индекс"))
    monkeypatch.setattr(cards, "price_for", lambda sec: ("индекс Мосбиржи", px))
    monkeypatch.setattr(cards, "intraday_now", lambda *a: None)
    monkeypatch.setattr(cards, "price_lines", lambda *a: [])
    monkeypatch.setattr(cards, "other_legs", lambda *a: [])
    monkeypatch.setattr(cards, "position_angles", lambda *a: [])
    c = cards.positions_card("IMOEXF", "short", d[-1])
    assert c["after"] == [] and c["analogy"] == [], "прошлые пики найдены задним числом — к сегодняшнему не примеряем"
    assert any("продолжение тренда, а не пик" in f for f in c["facts"])
    assert any("нынешнее значение - не пик" in x for x in c["limits"])
    assert c["trend"] and c["trend"][0].startswith("главное - тренд") and "растёт с" in c["trend"][0]
    assert c["chart"]["marks"] == [(d[-1], float(pos.iloc[-1]))]


def test_focus_prefers_trend_regime_stat():
    c = {"kind": "positions", "headline": "h", "trend": ["главное - тренд…", "итого: когда такой тренд позиции "
         "разворачивался…", "итого по тренду: после 2022 года, пока шорт физлиц рос, индекс…"]}
    assert cards.focus_lines(c)[-1].startswith("опора для вывода: итого по тренду")
