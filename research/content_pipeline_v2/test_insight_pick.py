"""Выбор находок дня (реплей завода на постах канала 14–25.09, 27.09.2026): пост другой рубрики — не повтор находки,
рекорд за всё время — мимо порога «мало для читателя», рекорд одного дня в фондах.

python3 -m pytest research/content_pipeline_v2/test_insight_pick.py -q
"""
import inspect
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
from signals.insights import detect as det  # noqa: E402

# посты канала 22.09 и 18.09 — их фильтр повторов принимал за посты о позициях физлиц; в channel_posts текст обрезан
# (~500 знаков), хэштег рубрики в конце поста может не дойти — проверяем и с ним, и без него
REBALANCE = ("Покупки/продажи фондов 💼 ◽️ В сентябре у Индекса МосБиржи будет меняться состав. Пересчитали корзину. "
             "📌 Газпром. Вес в индексе 8,74% → 9,05%. Докупят на 134 млн.")
YUAN = "Толпа охладела к юаню 👨 ◽️ За неделю из юаневых фондов ушло 1,4 млрд рублей. Такого оттока не было 1,5 года."
INDEX_POS = ("Шортистов больше, чем когда-либо 📣 ◽️ Число трейдеров с короткой позицией по фьючерсу на индекс "
             "МосБиржи перевалило за 5000. #открытыепозиции")
UNTAGGED = "Толпа снова против доллара ➡️ ◽️ Шорт физлиц по фьючерсу на валюту растёт третью неделю"


def _posts(monkeypatch, *texts):
    d = pd.Timestamp("2026-09-22 12:00")
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [(d, t) for t in texts])


def test_post_of_another_rubric_is_not_a_repeat(monkeypatch):
    for tail in ("", " #cделкифондов"):
        _posts(monkeypatch, REBALANCE + tail)
        assert ins.repeat_of({"kind": "positions", "sec": "IMOEXF", "leg": "short"}, "2026-09-23") is None
    for tail in ("", " #деньгивфондах"):
        _posts(monkeypatch, YUAN + tail)
        assert ins.repeat_of({"kind": "positions", "sec": "CNYRUBF", "leg": "short"}, "2026-09-23") is None
        assert ins.repeat_of({"kind": "funds", "cat": "yuan"}, "2026-09-23"), "а для потоков в юаневые фонды — повтор"


def test_same_rubric_and_untagged_posts_still_repeat(monkeypatch):
    _posts(monkeypatch, INDEX_POS)
    assert ins.repeat_of({"kind": "positions", "sec": "IMOEXF", "leg": "ns"}, "2026-09-23")
    _posts(monkeypatch, UNTAGGED)
    assert ins.repeat_of({"kind": "positions", "sec": "USDRUBF", "leg": "long"}, "2026-09-23"), \
        "пост без хэштега рубрики — по словам, как раньше"


def test_all_time_fund_record_passes_the_significance_threshold(monkeypatch):
    day = pd.Timestamp("2026-09-11")
    items = [{"date": day, "family": "фонды", "type": "серия", "score": 8.0, "instrument": "funds:stocks",
              "facts": {"cat": "stocks"}, "title": "Фонды акций: отток 9-й месяц подряд"},
             {"date": day, "family": "фонды", "type": "рекорд_или_экстремум", "score": 7.5, "instrument": "funds:gold",
              "facts": {"cat": "gold"},
              "title": "Фонды золота: отток с начала месяца 0.7 млрд ₽ — рекорд за всё время наших данных"}]
    monkeypatch.setattr(ins, "fund_significant", lambda cat, as_of: False)
    monkeypatch.setattr(ins, "channel_posts", lambda as_of, days=ins.REPEAT_DAYS: [])
    got = ins.pick(items, log=lambda *a: None, until=day)
    assert [j["spec"].get("cat") for j in got] == ["gold"], "серия мала для читателя, рекорд за всё время — нет"


def test_one_day_fund_flow_record():
    dv = np.r_[np.random.RandomState(0).normal(0, 1e9, 400), -11e9]
    assert det.day_record(dv, len(dv) - 1)
    assert not det.day_record(dv, 100), "меньше года истории — не рекорд"
    assert not det.day_record(np.r_[dv[:-1], -0.5e9], len(dv) - 1)
    assert not det.day_record(np.r_[dv[:-1], 0.0], len(dv) - 1)
    assert det.day_record(np.r_[dv[:-1], 12e9], len(dv) - 1), "и приток"
    assert "day_record(dv, i)" in inspect.getsource(det.detect_funds)
    assert "det.day_record(dv, len(dv) - 1)" in inspect.getsource(cards.funds_card), "карточка называет рекорд дня"
