"""Связки завода на едином словаре второго мозга (27.09.2026, Вадим: «надо закрыть тему второго мозга»).

Темы новости — из Brain/news_types.py (те же правила и проверка «про наш рынок», что у мозга), шум — общим фильтром
vocab.sql_шум. У связок свои только события-триггеры: отчёт ЦБ о потоках, операции Минфина с валютой, ход индекса.

python3 -m pytest research/content_pipeline_v2/test_combos_vocab.py -q
"""
import inspect
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Brain"))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
import news_types as nt  # noqa: E402

from signals.insights import combos, data  # noqa: E402


def test_no_own_topic_classifier_left():
    assert not hasattr(combos, "RULES"), "свой классификатор тем связок убран"
    assert set(combos.ТЕМА_СВЯЗКИ) <= {т for т, *_ in nt.ТИПЫ}, "темы — только из словаря мозга"
    assert set(combos.ТЕМА_СВЯЗКИ.values()) | set(combos.ТРИГГЕРЫ) | {"отчёт_цб_потоки"} == set(combos.THEMES)
    assert set(combos.ТРИГГЕРЫ) == {"минфин_валюта", "отчёт_цб_потоки", "рынок_целиком"}


def test_news_query_takes_all_topics_and_the_common_noise_filter():
    sql, p = data.QUERIES["news_recent"]()
    assert "AS темы" in sql and "array_remove(ARRAY[" in sql, "все темы новости, а не одна главная"
    assert "cardinality(coalesce(na.hashtags" in sql, "реклама и «ВПЕРЕДИ» MarketTwits без хэштегов — шум"
    assert "мнение" in sql, "мнение аналитиков темы не даёт"
    assert "length(na.text) >=" not in sql, "короткая молния «🔥ЦБ РФ снизил ставку» — главная новость дня"
    assert all(k.startswith(("nt", "шм", "чс")) for k in p), "регэкспы — параметрами (pg8000 и «%»)"
    assert "q() if callable(q)" in inspect.getsource(data._frame)


def test_topics_map_to_combo_themes_and_one_post_feeds_several():
    assert combos.classify("Набиуллина о ставке и курсе рубля", "#дкп,#россия",
                           ["ставка и инфляция", "рубль и валюта"]) == ["ставка", "рубль"]
    assert combos.classify("Атака БПЛА на НПЗ", "#бпла", ["удары по инфраструктуре", "нефть, газ и топливо"]) == \
        ["нефть_газ"], "удар и нефть — одна тема связки, без повтора"
    assert combos.classify("Банки подняли ставки по вкладам", "#банки", ["банки и кредит"]) == [], \
        "у темы без связки отклика в данных нет"
    assert combos.classify("пусто", "", None) == [] and combos.classify("пусто", "", float("nan")) == []


def test_world_assets_only_for_gold_and_silver():
    assert combos.classify("Цена золота обновила максимум", "#золото", ["металлы и удобрения"]) == ["мировые_активы"]
    assert combos.classify("Выплавка стали в августе упала", "#сталь", ["металлы и удобрения"]) == [], \
        "ноги «мировых активов» — золото и серебро; сталь их не касается"


def test_triggers_stay_as_before():
    cb = ("Крупнейшими нетто-покупателями на вторичных биржевых торгах в августе стали розничные инвесторы — "
          "физлица купили акций на 30 млрд руб — ЦБ")
    assert "отчёт_цб_потоки" in combos.classify(cb, "#cot,#обзор", [])
    assert "отчёт_цб_потоки" not in combos.classify("ЦБ обсуждает защиту розничных инвесторов", "#cot", []), \
        "без цифр отчёта — не отчёт (#2084)"
    assert "минфин_валюта" in combos.classify("ЦЕНА ОТСЕЧЕНИЯ В БЮДЖЕТНОМ ПРАВИЛЕ — $50", "#fx,#бюджет",
                                              ["налоги и бюджет"])
    assert "рынок_целиком" in combos.classify("Индекс Мосбиржи обновил рекорд", "", [])
