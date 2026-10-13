#!/usr/bin/env python3
"""Юнит-тесты дедупа хайп-бота (signals/news_similarity).

Ловит случай 13.10.2026: цитата Пескова про план урегулирования пришла из
newssmartlab и из markettwits разными словами, в канал ушли обе. И проверяет,
что разные новости с похожими словами (Сбер/ВТБ, Brent вверх/вниз) не склеиваются.

Запуск:
    python tests/test_news_similarity.py      # печатает PASS/FAIL
    pytest tests/test_news_similarity.py       # тоже работает
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from signals import config  # noqa: E402
from signals.news_similarity import Idf, find_duplicate, stems  # noqa: E402

SMARTLAB = """Песков: Мы ничего не слышал о плане урегулирования по Украине, который якобы \
обсуждают США и Европа, включая прекращение огня по линии фронта

Читать далее
👉 https://smartlab.news/i/207232"""

MARKETTWITS = """🇺🇦#украина #геополитика
Кремль не знает об обсуждениях США и Европой плана урегулирования по Украине

Контакты по вопросу достижения энергоперемирия между РФ и Украиной продолжаются.

-- Песков"""


def _dup(a: str, b: str, idf=None):
    return find_duplicate(
        a, [("x", b)], idf or Idf([a, b]), config.HYPE_DEDUP_MIN_SCORE,
        config.HYPE_DEDUP_MIN_SHARED, config.HYPE_DEDUP_MIN_NAMES_SHARE,
    )


def test_same_quote_from_two_channels_is_duplicate():
    assert _dup(MARKETTWITS, SMARTLAB)
    assert _dup(SMARTLAB, MARKETTWITS)


def test_links_tags_and_read_more_ignored():
    s = stems(SMARTLAB + "\n#геополитика [мы в max](https://max.ru/x)")
    assert not any(x.startswith(("http", "smart", "читат", "далее", "геопо")) for x in s)


def test_same_words_different_companies_not_duplicate():
    assert not _dup("Сбербанк в сентябре увеличил чистую прибыль по РСБУ на 5%",
                    "ВТБ в сентябре увеличил чистую прибыль по МСФО на 10%")


def test_short_posts_with_few_shared_words_not_duplicate():
    assert not _dup("Brent подорожала до $70 за баррель", "Brent подешевела ниже $65 за баррель")


def test_other_statement_of_same_person_not_duplicate():
    assert not _dup("Песков: Путин пока не планирует разговор с Трампом", MARKETTWITS)


def test_same_decision_reworded_is_duplicate():
    assert _dup("ЦБ снизил ключевую ставку до 16% годовых",
                "🇷🇺#ставка\nБанк России снизил ключевую ставку до 16%")


def test_common_feed_words_weigh_less():
    """Слова, которые есть почти в каждом посте ленты, весят меньше редких."""
    a = "Новатэк запустил вторую линию завода Арктик СПГ 2, выручка вырастет в сентябре"
    b = "Новатэк сообщил: выручка в сентябре вырастет, завод работает"
    flat = Idf([a, b])
    feed = Idf([a, b] + ["Выручка компании вырастет в сентябре, новый завод"] * 50)
    sa, sb = stems(a), stems(b)
    from signals.news_similarity import similarity
    assert similarity(sa, sb, feed) < similarity(sa, sb, flat)


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError:
                failed += 1
                print(f"FAIL {name}")
    sys.exit(1 if failed else 0)
