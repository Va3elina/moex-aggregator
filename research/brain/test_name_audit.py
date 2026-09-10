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
    why, _ = F.решение(THU, 3000, 2, None, None)
    assert why and "конвейер постов занят" in why


def test_first_pass_fires_every_night_window():
    assert F.решение(THU, 3000, 0, None, None) == (None, 0)


def test_does_not_overlap_a_batch_that_has_not_come_back():
    last = THU - timedelta(minutes=30)
    assert F.решение(THU, 3000, 0, last, None)[0] == "прошлая партия ещё не вернулась"
    assert F.решение(THU, 3000, 0, last, last + timedelta(minutes=10)) == (None, 0)


def test_after_first_pass_only_on_sunday_with_resample():
    assert F.решение(THU, 10, 0, THU - timedelta(days=7), THU - timedelta(days=7))[0]
    assert F.решение(SUN, 10, 0, SUN - timedelta(days=7), SUN - timedelta(days=7)) == (None, F.WEEKLY_RESAMPLE)
    # второе окно того же воскресенья — уже нет
    assert F.решение(SUN, 10, 0, SUN - timedelta(hours=1), SUN - timedelta(minutes=40))[0]


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


def test_sync_drops_rejected_edges_and_digests():
    s = _sync()
    имя = inspect.getsource(s.новости_по_имени)
    assert "brain_edge_reviews" in имя and "'неверно'" in имя
    assert "HAVING COUNT(*) > 5" in inspect.getsource(s.новости)
    assert "таблицы_аудита(conn)" in inspect.getsource(s.main)
