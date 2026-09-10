"""Аудит разметки второго мозга по имени (10.09.2026).

Главное требование Вадима: «чтобы не помешало работе нынешних агентов». Квота Routines
общая с писателем и судьёй, поэтому решение «стрелять ли» проверено здесь отдельно.
"""
import contextlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DB_URL", "postgresql+pg8000://x:y@127.0.0.1:1/x")
from signals import brain_audit_fire as F  # noqa: E402
from api import brain_core as core  # noqa: E402

MSK = timezone(timedelta(hours=3))
THU = datetime(2026, 9, 10, 2, 20, tzinfo=MSK)   # четверг, ночь
SUN = datetime(2026, 9, 13, 2, 20, tzinfo=MSK)   # воскресенье, ночь


def test_never_fires_by_day():
    assert F.решение(THU.replace(hour=14), 3000, 0, None, None)[0] == "не ночь"


def test_waits_while_writer_or_judge_has_work():
    why, _, _ = F.решение(THU, 3000, 2, None, None)
    assert why and "конвейер постов занят" in why


def test_first_pass_fires_every_night_window():
    assert F.решение(THU, 3000, 0, None, None) == (None, 0, "main")


def test_does_not_overlap_a_batch_that_has_not_come_back():
    last = THU - timedelta(minutes=30)
    assert F.решение(THU, 3000, 0, last, None)[0] == "прошлая партия ещё не вернулась"
    assert F.решение(THU, 3000, 0, last, last + timedelta(minutes=10)) == (None, 0, "main")
    # второе мнение тоже партия: пока не вернулось — основную не стреляем
    assert F.решение(THU, 3000, 0, None, None, 0, last)[0] == "прошлая партия ещё не вернулась"


def test_after_first_pass_only_on_sunday_with_resample():
    week = timedelta(days=7)
    assert F.решение(THU, 10, 0, THU - week, THU - week)[0]
    assert F.решение(SUN, 10, 0, SUN - week, SUN - week) == (None, F.WEEKLY_RESAMPLE, "main")
    # второе окно того же воскресенья — уже нет
    assert F.решение(SUN, 10, 0, SUN - timedelta(hours=1), SUN - timedelta(minutes=40))[0]


def test_second_opinion_goes_first_and_does_not_break_sunday():
    """Пока второго мнения нет, «неверно» ничего не удаляет — оно идёт первым, но копится
    до SECOND_MIN: отдельная сессия ради пары связей растянула бы первый проход вдвое."""
    assert F.решение(THU, 3000, 0, None, None, 5) == (None, 0, "main")
    assert F.решение(THU, 3000, 0, None, None, F.SECOND_MIN) == (None, 0, "second")
    # после первого прохода — остаток в воскресенье
    assert F.решение(THU, 10, 0, THU - timedelta(days=3), THU - timedelta(days=3), 5)[0]
    assert F.решение(SUN, 10, 0, SUN - timedelta(days=7), SUN - timedelta(days=7), 5) == (None, 0, "second")
    # воскресенье: второе мнение в 01:20 не съедает основную партию в 02:20
    back = SUN - timedelta(minutes=50)
    assert F.решение(SUN, 10, 0, SUN - timedelta(days=7), back, 0, SUN - timedelta(hours=1)) == \
        (None, F.WEEKLY_RESAMPLE, "main")


class _DB:
    def __init__(self):
        self.sql, self.committed = [], False

    def execute(self, stmt, params=None):
        self.sql.append((str(stmt), params or {}))
        return type("R", (), {"rowcount": 1})()

    def begin_nested(self):
        return contextlib.nullcontext()

    def commit(self):
        self.committed = True


def test_decisions_keep_valid_and_count_garbage():
    db = _DB()
    out = core.аудит_имён_решения({"партия": "p1", "решения": [
        {"id": "news:markettwits/1|company:MGNT", "вердикт": "неверно", "причина": "магнит как предмет"},
        {"id": "news:markettwits/2|company:MGNT", "вердикт": "может быть"},
        {"id": "мусор", "вердикт": "верно"},
    ]}, db=db)
    assert out == {"принято": 1, "отброшено": 2, "предложено": 0}
    assert db.committed
    вставка = [p for s, p in db.sql if "brain_edge_reviews" in s]
    assert вставка and вставка[0]["v"] == "неверно" and вставка[0]["who"] == "routine"


def _sync():
    spec = importlib.util.spec_from_file_location("brain_sync", ROOT / "Brain" / "brain_sync.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_second_opinion_is_written_next_to_the_first_and_only_once():
    db = _DB()
    out = core.аудит_имён_решения({"партия": "p2", "режим": "second", "решения": [
        {"id": "news:newssmartlab/71977|company:AFKS", "вердикт": "верно"}],
        "предложения": [{"company_id": "company:AFKS", "исключение": "sitronics"}]}, db=db)
    assert out == {"принято": 1, "отброшено": 0, "предложено": 0}, "в режиме second исключений нет"
    sql = " ".join(s for s, _ in db.sql)
    assert "second_verdict = :v" in sql and "second_verdict IS NULL" in sql
    assert "INSERT INTO brain_edge_reviews" not in sql


def test_sync_removes_an_edge_only_on_two_verdicts_or_a_human():
    s = _sync()
    имя = inspect.getsource(s.новости_по_имени)
    assert "r.second_verdict = 'неверно'" in имя and "r.human_decision = 'убрать'" in имя
    assert "AND r.verdict = 'неверно'\n" not in имя, "одного «неверно» для удаления мало"
    assert "HAVING COUNT(*) > 5" in inspect.getsource(s.новости)
    assert "таблицы_аудита(conn)" in inspect.getsource(s.main)


def test_window_shows_the_company_name_even_far_in_the_text():
    """Прогон 10.09: Совкомбанк стоял дальше 600-го символа — агент честно сказал «неясно»."""
    текст = "Анонс конференции. " + "Яндекс — одна из крупнейших компаний. " * 30 + "Совкомбанка тоже ждём."
    окно = core._окно(текст, "Совкомбанк | ПАО Совкомбанк")
    assert "Совкомбанка" in окно and len(окно) <= 601
    assert core._окно("коротко про ВТБ", "ВТБ") == "коротко про ВТБ"


def test_second_batch_is_blind():
    src = inspect.getsource(core.аудит_имён_партия)
    assert "FALSE AS повтор {_ВТОРОЕ_ЖДЁТ}" in src and "ORDER BY random()" in src
    assert "reason" not in src.split("if mode == \"second\":")[1].split("else:")[0]
