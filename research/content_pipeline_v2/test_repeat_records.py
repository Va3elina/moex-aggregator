"""Рекорд сильнее прежнего обходит «один тикер — один пост за три дня»; устаревшая новость утром
отпадает от сюжета, а не убивает его (разбор завода 24.09).

python3 -m pytest research/content_pipeline_v2/test_repeat_records.py -q
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals import combo_scan  # noqa: E402
from signals.content_ai import _record_rank  # noqa: E402


def test_record_ranks_on_real_headlines():
    # #2490 прошёл бы: «за всё время» сильнее «рекорд с 25 декабря 2024» (#2271)
    assert _record_rank("Юаневые фонды: отток за 5 торговых дней - 2 млрд ₽, рекорд за всё время наших данных") == 2
    assert _record_rank("Юаневые фонды: отток за 5 торговых дней - 1,4 млрд ₽, рекорд с 25 декабря 2024 года") == 1
    # #2724 после #2613 — оба «исторический максимум» по Магниту: по-прежнему повтор
    assert _record_rank("Покупки физлиц по фьючерсу на акции «Магнит» - исторический максимум") == 2
    assert _record_rank("Фонды денежного рынка: приток с начала сентября - 35 млрд ₽") == 0


def test_stale_news_leaves_data_story():
    s = {"families": ["новость", "позиции", "фонды"], "is_news": True, "news": [{"t": "x"}], "score": 9.0}
    plain = combo_scan.without_news(s)
    assert plain["families"] == ["позиции", "фонды"] and not plain["is_news"] and not plain["news"]
    assert plain["score"] == 7.0
    assert combo_scan.without_news({"families": ["новость", "позиции"], "is_news": True, "news": [], "score": 8}) is None
